package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The window reads of a parser's series identity are bounded by construction. The buckets of one read hold a series
// per hash of each line's parsed labels and bucket: with a key of its own on every line (a request id) that is a
// series per line, so a read over hours of a busy service can be as large as the window's lines. The evaluation is
// split by time into chunks of evaluation steps, each read on its own (its windows (t-range, t] reach one range back),
// folded by the outer aggregation and merged with the others; no chunk's answer holds more than a byte budget, and the
// proxy holds one chunk's buckets at a time. One cheap count of the selection's rows per coarse bucket (no parser, no
// hash) sizes the chunks; the first chunk's answer gives the bytes a row costs for the rest, so series that repeat
// (few series of many lines) are read in few chunks. A selection whose rows fit the budget is read in one call, as
// before.

// identityChunkBytes is the bytes one chunk's bucket answer is sized for: a quarter of what the proxy reads from one
// stats answer (maxStatsBucketResponseBytes, and -backend-max-buffered-response-bytes when lower), so an estimate off
// by a few times still fits.
func (p *Proxy) identityChunkBytes() int64 {
	limit := maxStatsBucketResponseBytes
	if buffered := int64(p.limits().BufferedBackendBodyBytes); buffered > 0 && buffered < limit {
		limit = buffered
	}
	return max(limit/4, 1)
}

// identityBytesPerRow is the bytes a row is assumed to cost in a bucket answer before one is measured: a series of its
// own per line, with two or three stats results each.
const identityBytesPerRow = 256

// identityCountBuckets is how many buckets the row count of a request is read in, at most.
const identityCountBuckets = 256

// identityChunk is a run of evaluation steps, first and last included, as Unix nanoseconds.
type identityChunk struct{ first, last int64 }

// identityReadBytes counts the bytes of the bucket answers a chunked read takes (collectRangeMetricHitsAux adds them).
type identityReadBytes struct{ n atomic.Int64 }

type identityReadBytesKey struct{}

// addIdentityReadBytes records n bytes of a bucket answer read for ctx's chunk, when it is one.
func addIdentityReadBytes(ctx context.Context, n int) {
	if c, _ := ctx.Value(identityReadBytesKey{}).(*identityReadBytes); c != nil {
		c.n.Add(int64(n))
	}
}

// rowCounts is the row count of a selection per bucket [start + k*step, start + (k+1)*step).
type rowCounts struct {
	start, step int64
	counts      []int64
}

// within is the rows of the buckets that overlap [from, to] (an upper bound of the rows in it).
func (c rowCounts) within(from, to int64) int64 {
	if c.step <= 0 || len(c.counts) == 0 {
		return 0
	}
	lo, hi := (from-c.start)/c.step, (to-c.start)/c.step
	if lo < 0 {
		lo = 0
	}
	if top := int64(len(c.counts) - 1); hi > top {
		hi = top
	}
	var n int64
	for k := lo; k <= hi; k++ {
		n += c.counts[k]
	}
	return n
}

// identityCountQuery is the count of the selection of base (its filters, before any pipe), the cheapest read of the
// rows a window holds; a base the LogsQL parser does not read is counted whole (exact, with its parser and gate).
func identityCountQuery(base string) string {
	if q, err := logsql.Parse(base); err == nil && q.Filter != nil {
		return logsql.Query{Filter: q.Filter, Pipes: []logsql.Pipe{logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "__lvp_rows"}}}}}.String()
	}
	return base + " | stats count() as __lvp_rows"
}

// readRowCounts reads the rows of base per coarse bucket over [from, to].
func (p *Proxy) readRowCounts(ctx context.Context, base string, from, to time.Time, step time.Duration) (rowCounts, error) {
	params := url.Values{"query": {identityCountQuery(base)}}
	setSlidingStatsRangeParams(params, from, to, step)
	resp, err := p.vlPost(ctx, "/select/logsql/stats_query_range", params)
	if err != nil {
		return rowCounts{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return rowCounts{}, p.redactedBackendStatusError("stats_query_range backend", resp.StatusCode, body)
	}
	body, err := readBodyLimited(resp.Body, maxStatsBucketResponseBytes)
	if err != nil {
		return rowCounts{}, err
	}
	var answer struct {
		Data struct {
			Result []struct {
				Values [][2]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return rowCounts{}, &statsBucketsUnreadableError{"parse stats_query_range row count: " + err.Error()}
	}
	out := rowCounts{start: from.UnixNano(), step: int64(step)}
	out.counts = make([]int64, (to.UnixNano()-out.start)/out.step+1)
	for _, series := range answer.Data.Result {
		for _, point := range series.Values {
			var ts float64
			var raw string
			if json.Unmarshal(point[0], &ts) != nil || json.Unmarshal(point[1], &raw) != nil {
				continue
			}
			n, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				continue
			}
			k := (snapSlidingBucketNanos(int64(ts*1e9), from, step) - out.start) / out.step
			if k >= 0 && k < int64(len(out.counts)) {
				out.counts[k] += int64(n)
			}
		}
	}
	return out, nil
}

// planIdentityChunks splits the evaluation steps first..last (step apart) into chunks whose windows hold at most
// budget rows by counts, one step at least each; window is the range the first step reads back.
func planIdentityChunks(counts rowCounts, first, last, step, window, budget int64) []identityChunk {
	var out []identityChunk
	for a := first; a <= last; {
		b := a
		for b+step <= last && counts.within(a-window, b+step) <= budget {
			b += step
		}
		out = append(out, identityChunk{first: a, last: b})
		a = b + step
	}
	return out
}

// foldDynamicIdentityChunks serves a request marked identityAheadKey in time chunks and merges their folded answers,
// or reports false when the request is read in one call (its rows fit one chunk) and foldDynamicIdentity reads it as
// before. Each chunk is a request of its own over its steps: the window evaluator answers its windows and the outer
// aggregation folds them (chain). The chunks are read one after another, so the proxy holds one chunk's buckets.
func (p *Proxy) foldDynamicIdentityChunks(w http.ResponseWriter, r *http.Request, base string, chain []*logqlpkg.VectorAggregation, serve func(http.ResponseWriter, *http.Request) bool) bool {
	start, okStart := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, okEnd := parseLokiTimeToUnixNano(r.FormValue("end"))
	stepD, okStep := parsePositiveStepDuration(r.FormValue("step"))
	window, okWindow := dynamicWindowRange(r.FormValue("query"))
	if !okStart || !okEnd || !okStep || !okWindow || end < start || base == "" {
		return false
	}
	step := int64(stepD)
	last := start + (end-start)/step*step
	from := time.Unix(0, start).Add(-window)
	span := time.Unix(0, last).Sub(from)
	countStep := max(stepD, (span/identityCountBuckets/stepD+1)*stepD)
	counts, err := p.readRowCounts(r.Context(), base, from, time.Unix(0, last), countStep)
	if err != nil {
		p.writeErr(w, r.Context(), identityAheadReadError(r.Context(), err))
		return true
	}
	budget := p.identityChunkBytes() / identityBytesPerRow
	if counts.within(from.UnixNano(), last) <= budget {
		return false
	}
	results := map[string]*identityChunkSeries{}
	var warnings []string
	header := http.Header{}
	measured := false
	for a := start; a <= last; {
		chunk := planIdentityChunks(counts, a, last, step, int64(window), budget)[0]
		read := &identityReadBytes{}
		ctx := context.WithValue(context.WithValue(r.Context(), identityReadBytesKey{}, read), statsExtentRequestKey{}, false)
		sub := binaryAggregationRangeAt(r, chunk.first, chunk.last).WithContext(ctx)
		buf := &bufferedResponseWriter{header: make(http.Header)}
		if !foldVectorChain(p, buf, sub, chain, serve) {
			p.writeErr(w, r.Context(), fmt.Errorf("parser identity chunk not served"))
			return true
		}
		if buf.code != 0 && buf.code != http.StatusOK {
			copyHeaders(w.Header(), buf.Header())
			w.WriteHeader(buf.code)
			_, _ = w.Write(buf.body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the error layer's own answer, Content-Type copied above
			return true
		}
		if err := mergeIdentityChunk(results, &warnings, buf.body); err != nil {
			p.writeErr(w, r.Context(), err)
			return true
		}
		copyHeaders(header, buf.Header())
		if !measured {
			// The bytes a row of this selection costs in a bucket answer size the chunks left.
			if rows := counts.within(chunk.first-int64(window), chunk.last); rows > 0 {
				perRow := max(read.n.Load()/rows, 1)
				budget = max(p.identityChunkBytes()/perRow, budget)
			}
			measured = true
		}
		a = chunk.last + step
	}
	body, err := encodeIdentityChunks(results, warnings)
	if err == nil {
		body, err = capSeriesToLimit(r.Context(), body, p.resolvedMaxStatsQuerySeries(r.Context()))
	}
	if err != nil {
		p.writeErr(w, r.Context(), err)
		return true
	}
	copyHeaders(w.Header(), header)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; the merged matrix JSON
	return true
}

// identityChunkSeries is one series of the merged answer: its labels and its points in time order.
type identityChunkSeries struct {
	Metric map[string]string `json:"metric"`
	Values []json.RawMessage `json:"values"`
}

// mergeIdentityChunk appends the series of one chunk's matrix answer to results, and its warnings to warnings.
func mergeIdentityChunk(results map[string]*identityChunkSeries, warnings *[]string, body []byte) error {
	var answer struct {
		Data struct {
			ResultType string                `json:"resultType"`
			Result     []identityChunkSeries `json:"result"`
		} `json:"data"`
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || answer.Data.ResultType != "matrix" {
		return &statsBucketsUnreadableError{"parser identity chunk: not a matrix answer"}
	}
	for _, s := range answer.Data.Result {
		key := canonicalLabelsKey(s.Metric)
		if prior := results[key]; prior != nil {
			prior.Values = append(prior.Values, s.Values...)
			continue
		}
		series := s
		results[key] = &series
	}
	for _, w := range answer.Warnings {
		dup := false
		for _, seen := range *warnings {
			dup = dup || seen == w
		}
		if !dup {
			*warnings = append(*warnings, w)
		}
	}
	return nil
}

// encodeIdentityChunks writes the merged series as a Loki matrix answer, in the label order a single read answers.
func encodeIdentityChunks(results map[string]*identityChunkSeries, warnings []string) ([]byte, error) {
	out := make([]*identityChunkSeries, 0, len(results))
	for _, s := range results {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return compareLabelSets(out[i].Metric, out[j].Metric) < 0 })
	answer := map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": out}}
	if len(warnings) > 0 {
		answer["warnings"] = warnings
	}
	return json.Marshal(answer)
}
