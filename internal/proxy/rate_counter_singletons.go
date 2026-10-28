package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// rate_counter over one-sample series (Loki v3.7.9).
//
// rate_counter is evaluated per series of its sample extractor: the stream,
// its structured metadata and every label a parser extracted, less the
// unwrapped one (pkg/logql/log/metrics_extraction.go:160-166 and :202-231,
// labels.go:664-688 GroupedLabels). A `sum` does not hand its grouping to
// rate_counter (pkg/logql/syntax/ast.go:1630-1640 canInjectVectorGrouping), so
// a parser that extracts a per-line key (a transaction or request id) makes
// every line a series of its own. A series with fewer than two samples in a
// window rates 0 (pkg/logql/range_vector.go:286-291 extrapolatedRate), and the
// step still has its point. Under `sum` or `sum by`, Loki then answers 0 at
// every step whose window holds a sample of the group, whatever the values.
//
// The proxy proves that case in VictoriaLogs, without reading rows: over the
// fetch range, the lines that carry the unwrapped label are counted next to
// the number of distinct (_stream, logfmt keys) tuples (count_uniq_hash), the
// keys parsed again from the line under a prefix of their own, so a stored
// field Loki does not extract never splits a series. Structured metadata,
// keys missing from the key sample and keys sharing a Loki label name with
// another key or with the unwrapped label are left out, which only merges
// series. So equal counts mean every Loki series holds one sample, and every
// window rates 0; a hash collision lowers the distinct count and only sends
// the request to the per-row evaluator. Not covered: a line holding one key
// twice, where VictoriaLogs and Loki may keep different values. The windows'
// presence is the bucket count of the unwrap rate route, which also reports
// Loki's conversion errors.

// rateCounterZeroFunc names the bucket matrix of rate_counter over one-sample
// series: the presence of the unwrap rate route with every value 0.
const rateCounterZeroFunc = "__rate_counter_one_sample__"

// errRateCounterProof is a failed proof read: the per-row evaluator answers.
var errRateCounterProof = errors.New("rate_counter one-sample proof failed")

// rateCounterKeysSample is how many lines the parsed-key discovery reads. Keys
// it misses only merge series, which can fail the proof, never fake it.
const rateCounterKeysSample = 1000

// rateCounterMaxKeys bounds the keys of the distinct tuple, and with them the
// query length.
const rateCounterMaxKeys = 64

// rateCounterUnderSum reports whether query is `sum` or `sum by (...)` directly
// over rate_counter of a log query with a parser stage, whose series identity
// the stats proof can bound: no stage that drops, keeps or renames labels (it
// would make Loki's series coarser than the parsed fields). ungrouped is true
// for a bare sum; label is the unwrapped label.
func rateCounterUnderSum(query string) (label string, ungrouped, ok bool) {
	expr, err := logqlpkg.Parse(stripOuterLabelReplace(strings.TrimSpace(query)))
	if err != nil {
		return "", false, false
	}
	agg, isAgg := expr.(*logqlpkg.VectorAggregation)
	if !isAgg || agg.Op != logqlpkg.VectorSum || (agg.Grouping != nil && agg.Grouping.Without) {
		return "", false, false
	}
	ra, isRange := agg.Inner.(*logqlpkg.RangeAggregation)
	if !isRange || ra.Op != logqlpkg.RangeRateCounter || ra.Grouping != nil || ra.Offset != "" {
		return "", false, false
	}
	lq, isLog := ra.Inner.(*logqlpkg.LogQuery)
	if !isLog {
		return "", false, false
	}
	parsers := 0
	for _, stage := range lq.Pipeline {
		switch st := stage.(type) {
		case *logqlpkg.ParserStage:
			// The proof re-parses the line as logfmt: one `| logfmt` that
			// extracts every key. `| json` is left out (VictoriaLogs renders an
			// array as text, Loki skips it), as are the other parsers.
			if st.Type != logqlpkg.ParserLogfmt || st.Param != "" || len(st.Fields) != 0 {
				return "", false, false
			}
			parsers++
		case *logqlpkg.UnwrapStage:
			label = st.Label
		case *logqlpkg.LineFilterStage, *logqlpkg.LabelFilterStage, *logqlpkg.DecolorizeStage:
		default:
			return "", false, false
		}
	}
	return label, agg.Grouping == nil || len(agg.Grouping.Labels) == 0, parsers == 1 && label != ""
}

// startRateCounterProof starts the one-sample proof of a sum over
// rate_counter and returns the spec and function its windows' presence is
// read with (the unwrap rate route, one series for a bare sum); other
// requests get a nil channel and their spec and function back.
func (p *Proxy) startRateCounterProof(r *http.Request, spec statsCompatSpec, manualFunc, field string, from, to time.Time) (<-chan bool, statsCompatSpec, string) {
	label, ungrouped, ok := rateCounterUnderSum(r.FormValue("query"))
	if !ok || manualFunc != "rate_counter" {
		return nil, spec, manualFunc
	}
	proof := p.proveRateCounterOneSample(r.Context(), withoutUnwrapGate(spec).BaseQuery, field, label, from, to)
	if ungrouped {
		spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit = nil, nil, true
	}
	return proof, spec, "unwrap_rate"
}

// rateCounterBucketRoute names the route and the matrix function of a bucket
// answer: the plain ones without a proof, the zero matrix once the proof
// holds; proven is false when it failed.
func rateCounterBucketRoute(proof <-chan bool, manualFunc string) (route, matrixFunc string, proven bool) {
	if proof == nil {
		return "buckets-", manualFunc, true
	}
	return "counter-one-sample-", rateCounterZeroFunc, <-proof
}

// rateCounterRawSpec is the per-row evaluator's spec of a rate_counter whose
// proof failed, as proxyManualRangeMetricRange builds it.
func rateCounterRawSpec(ctx context.Context, spec statsCompatSpec, start time.Time, step, window time.Duration) statsCompatSpec {
	_, raw, _ := unwrapManualSpecs(spec, "rate_counter")
	if window < step {
		raw.BaseQuery = withWindowPhaseFilter(ctx, raw.BaseQuery, start, step, window)
	}
	return raw
}

// rateCounterKeyPrefix names the fields of the proof's own parse of the line.
const rateCounterKeyPrefix = "__lvpk."

// proveRateCounterOneSample starts the proof that no series of the rate_counter
// holds two samples in [from, to] and returns its verdict channel. base is the
// translated log query without the unwrap gate, field the unwrapped
// VictoriaLogs field and label the unwrapped LogQL label.
func (p *Proxy) proveRateCounterOneSample(ctx context.Context, base, field, label string, from, to time.Time) <-chan bool {
	out := make(chan bool, 1)
	go func() {
		out <- p.rateCounterOneSample(ctx, base, field, label, from, to)
	}()
	return out
}

// rateCounterOneSample counts the lines that carry the unwrapped field and the
// distinct (_stream, parsed keys) tuples among them. The keys are those of the
// proof's own parse of the line (unpack_logfmt with a prefix), so stored
// fields Loki does not extract never split a series; structured metadata left
// out, a key left out of the sample, and a key whose Loki name another key or
// the unwrapped label shares (Loki keeps one of them) only merge series.
func (p *Proxy) rateCounterOneSample(ctx context.Context, base, field, label string, from, to time.Time) bool {
	lines := base + " | filter " + quoteLogsQLIdent(field) + ":*"
	keys := p.rateCounterIdentityKeys(ctx, lines, label, from, to)
	parse := ""
	tuple := []string{"_stream"}
	if len(keys) > 0 {
		quoted := make([]string, len(keys))
		for i, k := range keys {
			quoted[i] = quoteLogsQLIdent(k)
			tuple = append(tuple, quoteLogsQLIdent(rateCounterKeyPrefix+k))
		}
		parse = " | unpack_logfmt from _msg fields (" + strings.Join(quoted, ", ") + ") result_prefix " + strconv.Quote(rateCounterKeyPrefix)
	}
	row, ok := p.rateCounterStatsRow(ctx, lines+parse+" | stats count() as n, count_uniq_hash("+strings.Join(tuple, ", ")+") as u", from, to)
	if !ok {
		return false
	}
	n, errN := strconv.ParseUint(row["n"], 10, 64)
	u, errU := strconv.ParseUint(row["u"], 10, 64)
	return errN == nil && errU == nil && n == u
}

// rateCounterIdentityKeys returns the logfmt keys of a sample of the lines,
// the busiest first, less the unwrapped label and every key whose Loki label
// name (pkg/logql/log/util.go:22-39 sanitizeLabelKey) another key or the
// unwrapped label shares.
func (p *Proxy) rateCounterIdentityKeys(ctx context.Context, lines, label string, from, to time.Time) []string {
	resp, err := p.rateCounterQuery(ctx, lines+" | limit "+strconv.Itoa(rateCounterKeysSample)+" | unpack_logfmt from _msg result_prefix "+strconv.Quote(rateCounterKeyPrefix)+" | field_names", from, to)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	type named struct {
		name string
		hits uint64
	}
	var names []named
	byLoki := map[string]int{}
	scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1<<20))
	for scanner.Scan() {
		var row map[string]string
		if json.Unmarshal(scanner.Bytes(), &row) != nil {
			continue
		}
		name, ok := strings.CutPrefix(row["name"], rateCounterKeyPrefix)
		hits, _ := strconv.ParseUint(row["hits"], 10, 64)
		if !ok || name == "" || hits == 0 {
			continue
		}
		byLoki[lokiLogfmtLabel(name)]++
		names = append(names, named{name, hits})
	}
	kept := names[:0]
	for _, n := range names {
		loki := lokiLogfmtLabel(n.name)
		if byLoki[loki] == 1 && loki != label && loki+"_extracted" != label {
			kept = append(kept, n)
		}
	}
	names = kept
	sort.Slice(names, func(i, j int) bool {
		if names[i].hits != names[j].hits {
			return names[i].hits > names[j].hits
		}
		return names[i].name < names[j].name
	})
	if len(names) > rateCounterMaxKeys {
		names = names[:rateCounterMaxKeys]
	}
	keys := make([]string, 0, len(names))
	for _, n := range names {
		keys = append(keys, n.name)
	}
	return keys
}

// lokiLogfmtLabel is the label name Loki's logfmt parser gives a key
// (pkg/logql/log/util.go:22-39 sanitizeLabelKey with isPrefix).
func lokiLogfmtLabel(key string) string {
	key = strings.TrimSpace(key)
	if key != "" && key[0] >= '0' && key[0] <= '9' {
		key = "_" + key
	}
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, key)
}

// rateCounterStatsRow reads the one row of a stats query.
func (p *Proxy) rateCounterStatsRow(ctx context.Context, query string, from, to time.Time) (map[string]string, bool) {
	resp, err := p.rateCounterQuery(ctx, query, from, to)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, 64<<10)
	if err != nil {
		return nil, false
	}
	var row map[string]string
	if json.Unmarshal([]byte(strings.TrimSpace(string(body))), &row) != nil {
		return nil, false
	}
	return row, true
}

// rateCounterQuery runs query on /select/logsql/query over [from, to], the end
// included as Loki's window is.
func (p *Proxy) rateCounterQuery(ctx context.Context, query string, from, to time.Time) (*http.Response, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", formatVLTimestamp(from.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(to.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)))
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		resp.Body.Close()
		return nil, errRateCounterProof
	}
	return resp, nil
}
