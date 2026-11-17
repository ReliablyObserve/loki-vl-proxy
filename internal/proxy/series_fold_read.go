package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	fj "github.com/valyala/fastjson"
)

// The folded bucket reads of a parser's series identity over a range longer than the step (lower.SeriesFold).
//
// A request marked identityAheadKey reads the range function's partials per series hash and per anchored bucket, in
// the time chunks of listed_window_chunks.go. Each chunk is one stats_query_range call whose answer holds, per bucket
// and group, the partials of every series with lines in more than one bucket of the read (their hash kept), and one
// row of the series that lie wholly in that bucket, folded by VictoriaLogs: the sum (or extreme) of their values and
// their count, under an empty hash. The window evaluator merges each kept series' buckets into its windows as before,
// and adds a folded row's value to the outer aggregation of every window that holds its bucket, weighted by its count
// (hitsRangeMetricSeries, foldAccumulator.addPartial). The answer is the per-series one: a window holds all of a
// folded series' lines in the read or none, and each window lies in its chunk's read, which the subquery that tells
// the series apart is scoped to (VictoriaLogs adds the read's time range and extra filters to every subquery).
//
// The answer then holds a row per bucket and group for the folded series, not one per line: a key of its own on
// every line (a request id) costs about what the stream's series did. The buckets of these reads depend on the
// read's window (a series spans buckets inside one read, not inside another), so the stats extent cache never
// answers them.

// seriesFoldKey carries the seriesFoldRead of a request's bucket reads.
type seriesFoldKey struct{}

// seriesFoldRead is how a request's bucket reads fold the series that lie in one bucket.
type seriesFoldRead struct {
	outer logql.VectorOp
	// identity is the bucket query before the unwrap gate, which ends with the series hash.
	identity string
	field    string
	// columns are the bucket stats' results; value the math expression of a series' value in a bucket; valueColumn
	// and countColumn the results a folded row's value and count are read from (countColumn may be
	// lower.SeriesFoldCount, which the fold adds).
	columns                  []string
	stats                    []logsql.StatsFuncAlias
	value                    string
	valueColumn, countColumn string
	// stats are the bucket stats' functions (unwrapBucketAgg's, typed).
	// added says the fold adds lower.SeriesFoldCount to the bucket stats' results.
	added bool
	// singletons is how the bucket stats fold the series of one line (lower.SeriesFoldSingletons).
	singletons lower.SeriesFoldSingletons
}

// seriesFoldOff reads the series of a parser's identity one by one, as before the fold (tests compare the two).
var seriesFoldOff bool

// seriesFoldHash is the pipe that ends the identity of a parser of every key (lower.Profile IdentityAhead).
var seriesFoldHash = logsql.PipeHash{Fields: []string{dynamicSeriesHashLabel}, Result: dynamicSeriesHashLabel}.String()

// withSeriesFold marks the bucket reads of a request of a parser's identity over a range longer than the step
// (identityAhead) to fold the series that lie in one bucket, when its outer aggregation and function fold (one
// vector aggregation, the matrix fold of the request). The extent cache is off for them.
func (p *Proxy) withSeriesFold(r *http.Request, spec statsCompatSpec, manualFunc string, fold *matrixFold) *http.Request {
	if seriesFoldOff || !identityAhead(r.Context()) || fold == nil || len(fold.levels) != 1 || !spec.gated() {
		return r
	}
	chain, ok := listedIdentityChain(r.FormValue("query"))
	if !ok || len(chain) != 1 {
		return r
	}
	ungated := spec.ungated()
	if !strings.HasSuffix(ungated, seriesFoldHash) {
		return r
	}
	identity := lower.SeriesFoldIdentity(resolvedPlan(dynamicQuery(r.FormValue("query"))), p.logsqlCapabilities(), ungated)
	read := &seriesFoldRead{outer: chain[0].Op, identity: identity, field: spec.UnwrapField}
	v := translate.UnwrapValueAlias
	switch manualFunc {
	case "avg":
		read.columns, read.value, read.valueColumn, read.countColumn = []string{"__lvp_sum", "__sample_count"}, "__lvp_sum / __sample_count", "__lvp_sum", "__sample_count"
		read.stats = []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: v}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}}
		if read.outer == logql.VectorSum || read.outer == logql.VectorAvg {
			// The bucket stats add up the values of the series of one line: their sum, not their extreme.
			read.singletons = lower.SingletonsKept
		}
	case "max", "min":
		read.columns, read.value, read.valueColumn = []string{"c"}, "c", "c"
		read.stats = []logsql.StatsFuncAlias{{Func: logsql.Max{Field: v}, Alias: "c"}}
		if manualFunc == "min" {
			read.stats[0].Func = logsql.Min{Field: v}
		}
		if read.outer == logql.VectorAvg {
			read.countColumn, read.added = lower.SeriesFoldCount, true
		}
	case "stddev", "stdvar":
		read.columns = []string{deviationMeanName, "__sample_count", deviationBucketName}
		read.stats = []logsql.StatsFuncAlias{{Func: logsql.Avg{Field: v}, Alias: deviationMeanName}, {Func: logsql.Count{}, Alias: "__sample_count"},
			{Func: logsql.Stddev{Field: v}, Alias: deviationBucketName}}
		read.value, read.valueColumn, read.countColumn = deviationBucketName, deviationBucketName, "__sample_count"
		read.singletons = lower.SingletonsZero
		if manualFunc == "stdvar" {
			read.value = deviationBucketName + " * " + deviationBucketName
		}
	default:
		return r
	}
	ctx := context.WithValue(context.WithValue(r.Context(), seriesFoldKey{}, read), statsExtentRequestKey{}, false)
	return r.WithContext(ctx)
}

// seriesFoldFrom is the fold of ctx's bucket reads, nil when they read every series.
func seriesFoldFrom(ctx context.Context) *seriesFoldRead {
	read, _ := ctx.Value(seriesFoldKey{}).(*seriesFoldRead)
	return read
}

// query is the folded bucket query of the base query baseQuery and the bucket stats statsAggFunc grouped by groupBy,
// for a read from start in buckets of hitStep; ok is false when the fold does not apply (the read then reads every
// series).
func (s *seriesFoldRead) query(baseQuery string, groupBy []string, statsAggFunc string, start time.Time, hitStep time.Duration) (string, bool) {
	// The bucket stats are the ones the window evaluator asked for (statsAggFunc), written typed.
	if s == nil || !strings.HasSuffix(logsql.PipeStats{Funcs: s.stats}.String(), statsAggFunc) {
		return "", false
	}
	bucket, offset := slidingStatsGrid(start, hitStep)
	read, ok := lower.SeriesFold{Outer: s.outer, By: groupBy, Stats: s.stats, Columns: s.columns, Value: s.value, ValueColumn: s.valueColumn,
		CountColumn: s.countColumnIn(), Singletons: s.singletons, Field: s.field, Bucket: bucket, Offset: offset, From: start.UnixNano()}.Pipes(s.identity)
	if !ok {
		return "", false
	}
	var b strings.Builder
	b.Grow(len(baseQuery) + 2*len(s.identity) + 2048)
	b.WriteString(baseQuery)
	for _, p := range read.Pipes {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	return b.String(), true
}

// errSeriesFoldTooLong is a folded read whose query, as it would be sent, passes VictoriaLogs' default query length
// (-search.maxQueryLen): the read is sent again reading every series (collectRangeMetricHitsAux).
var errSeriesFoldTooLong = errors.New("folded series read longer than VictoriaLogs' query length")

// countColumnIn is the count column among the bucket stats' results (none when the fold adds it).
func (s *seriesFoldRead) countColumnIn() string {
	if s.added {
		return ""
	}
	return s.countColumn
}

// extraResults is the number of results per group the fold adds to the bucket stats'.
func (s *seriesFoldRead) extraResults() int {
	if s != nil && s.added {
		return 1
	}
	return 0
}

// slidingStatsGrid is the step and offset setSlidingStatsRangeParams sends for buckets of hitStep from start.
func slidingStatsGrid(start time.Time, hitStep time.Duration) (step, offset string) {
	if hitStep%time.Second == 0 {
		step = strconv.FormatInt(int64(hitStep/time.Second), 10) + "s"
	} else {
		step = strconv.FormatInt(int64(hitStep), 10) + "ns"
	}
	shift := ((start.UnixNano()+1)%int64(hitStep) + int64(hitStep)) % int64(hitStep)
	return step, strconv.FormatInt(-shift, 10) + "ns"
}

// foldedWindows evaluates a folded row's windows at the steps of [startNS, endNS]: per step the outer
// aggregation's merge of the values of the buckets in its window (their sum, or their extreme for max and min), and
// the series they hold (the counts' sum). A step whose window holds no bucket has no point.
func foldedWindows(entry manualSeriesSamples, merge withoutMerge, startNS, endNS, stepNS, windowNS int64, points []typedPoint, weights []float64) ([]typedPoint, []float64) {
	samples := sortedHitsSamples(entry.Samples)
	counts := sortedHitsSamples(entry.Counts)
	var sampleWindow, countWindow windowCursor
	for i, n := int64(0), stepCount(startNS, endNS, stepNS); i < n; i++ {
		tNS := startNS + i*stepNS
		lo, hi := sampleWindow.span(samples, tNS-windowNS, tNS)
		cLo, cHi := countWindow.span(counts, tNS-windowNS, tNS)
		if lo >= hi {
			continue
		}
		v := samples[lo].value
		for _, s := range samples[lo+1 : hi] {
			switch {
			case merge.max:
				v = math.Max(v, s.value)
			case merge.min:
				v = math.Min(v, s.value)
			default:
				v += s.value
			}
		}
		var w float64
		for _, c := range counts[cLo:cHi] {
			w += c.value
		}
		points = append(points, typedPoint{ts: lokiSampleSeconds(tNS), v: v})
		weights = append(weights, max(w, 1))
	}
	return points, weights
}

// readsRow reads one result of a folded bucket answer into seriesMap and reports whether it took it: the results of a
// folded row (an empty series hash) go into its entry (Folded: the value column into Samples, the count column into
// Counts, the others dropped), and the count the fold adds is dropped from every other row.
func (s *seriesFoldRead) readsRow(seriesMap map[string]manualSeriesSamples, seriesKey string, metricObj *fj.Object, metric map[string]string, name string, values []byte, start time.Time, hitStep time.Duration) bool {
	if hash := metricObj.Get(dynamicSeriesHashLabel); hash == nil || len(hash.GetStringBytes()) > 0 {
		return s.added && name == lower.SeriesFoldCount
	}
	entry := seriesMap[seriesKey]
	entry.Metric, entry.Folded = metric, true
	switch name {
	case s.valueColumn:
		entry.Samples = appendBucketSamples(entry.Samples, values, start, hitStep)
	case s.countColumn:
		entry.Counts = appendBucketSamples(entry.Counts, values, start, hitStep)
	}
	seriesMap[seriesKey] = entry
	return true
}

// seriesFoldPost answers a folded bucket read (params rewritten for the request's unwrap check, sentParams as sent)
// through one VictoriaLogs call shared by the identical reads in flight: the folded answer is the same for every
// request of a tenant scope, so the call holds one stats_query_range slot (collectRangeMetricHitsAux takes none for
// these reads) and its answer, read up to the stats body bound, is handed to each read, which strips its own unwrap
// counter from it. Any number of identical cold requests make one call; the reads sharing it are not cut short by
// one another's cancellation.
func (p *Proxy) seriesFoldPost(ctx context.Context, path string, params, sentParams url.Values) (*http.Response, error) {
	if len(params.Get("query")) > vlDefaultMaxQueryLen {
		return nil, errSeriesFoldTooLong
	}
	key := "series-fold:" + p.contextScopeFingerprint(ctx) + ":" + params.Encode()
	if full, _ := ctx.Value(levelFullChainKey{}).(bool); full {
		key += ":full-chain"
	}
	status, header, body, err := p.coalescer.DoWithGuardCtx(ctx, key, p.breaker.Allow, func(callCtx context.Context) (*http.Response, error) {
		release, err := p.takeStatsQueryRangeSlot(callCtx)
		if err != nil {
			return nil, err
		}
		defer release()
		resp, err := p.vlPostSend(callCtx, path, params, sentParams, nil, nil)
		if err != nil {
			return nil, err
		}
		// The coalescer reads the body once, up to the stats body bound.
		resp.Body = &seriesFoldBody{ReadCloser: resp.Body, left: maxStatsBucketResponseBytes}
		return resp, nil
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return nil, errors.New("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return nil, err
	}
	// The shared header is the call's: each read gets a copy it may set.
	header = header.Clone()
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
}

// seriesFoldBody is a folded answer's body as the coalescer reads it: past limit bytes the read fails with
// errBodyTooLarge, so the shared call holds at most the stats body bound.
type seriesFoldBody struct {
	io.ReadCloser
	left int64
}

func (b *seriesFoldBody) Read(p []byte) (int, error) {
	if b.left < 0 {
		return 0, errBodyTooLarge
	}
	if int64(len(p)) > b.left+1 {
		p = p[:b.left+1]
	}
	n, err := b.ReadCloser.Read(p)
	if b.left -= int64(n); b.left < 0 {
		return n, errBodyTooLarge
	}
	return n, err
}
