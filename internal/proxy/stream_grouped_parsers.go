package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// maxStreamGroupedValues bounds the label values the stored value check of one request names: past it the request
// keeps the route that evaluates both parsers.
const maxStreamGroupedValues = 256

// serveStreamGroupedParsers answers a sum grouped by stream labels over json and logfmt parsers from the query with
// no stage from its first parser on (lower.StreamGroupedParsersAt, semantics/stream-grouped-sum-skips-parsers),
// when every line that answer counted holds every grouping label as a stream label; it reports whether it answered.
//
// Loki writes a parsed key named like a stream label as `<name>_extracted`, so the parsers cannot move a line to
// another group, and the drop after them clears their errors: the cut query counts the same lines in the same
// groups. It is read only when its labels can be stream labels, and served only when the checks hold:
//
//   - before the read, every label is a stream label of the tenant lately or pinned by the selector
//     (streamGroupedLabelsListed, no VictoriaLogs call once the tenant's names are listed), and no line of the
//     window has a stream without one of the labels the selector does not pin (one VictoriaLogs read of at most one
//     row, from the stream index): a grouping by a parsed key spends no stats read;
//   - after it, every series of the answer holds every label with a value (a line written after the first check
//     without one is counted in a series that lacks it);
//   - after it, for a name the cut query reads from the stored field (the level names the detected_level derivation
//     reads as stored, which the stream value restore leaves, semantics/stream-label-shadowed-by-ingest-json-key), no
//     line of the window has a stored field other than its stream label's value, which the answer names: structured
//     metadata or a key VictoriaLogs unpacked at ingest can hold another value under the same name, where Loki groups
//     by the stream label (one VictoriaLogs read of at most one row).
//
// Any failure, or a check that does not hold, sends the request on to the route that evaluates both parsers (the
// ordered JSON evaluator), as before. The cut query runs through the handler like any other request: VictoriaLogs
// counts it with stats, its answer and its steps are cached under its own key, and an exact repeat is the response
// cache's.
func (p *Proxy) serveStreamGroupedParsers(w http.ResponseWriter, r *http.Request, query string, isRange bool) bool {
	if !p.queryPlanLowering || p.jsonLineKeysActive() || p.labelTranslator != nil && len(p.labelTranslator.vlToLoki) != 0 {
		return false
	}
	query = strings.TrimSpace(query)
	plan := resolvedPlan(query)
	at := lower.StreamGroupedParsersAt(plan)
	if at < 0 {
		return false
	}
	expr, ok := sharedLogQLExpr(query)
	if !ok {
		return false
	}
	cut, ok := streamGroupedCutOf(expr, at)
	if !ok {
		return false
	}
	start, end, _, err := orderedJSONMetricTimes(r, isRange)
	if err != nil {
		return false // the route that answers the query reports it
	}
	from, to := start.Add(-cut.window-cut.offset), end.Add(-cut.offset)
	ctx := r.Context()
	opStart := time.Now()
	outcome := "served"
	var buffered *bufferedResponseWriter
	switch {
	case !p.streamGroupedLabelsListed(ctx, cut):
		outcome = "unlisted"
	case !p.streamGroupedRowsAbsent(ctx, cut, p.streamGroupedUnlabelled(cut), from, to):
		outcome = "unlabelled"
	default:
		buffered = p.streamGroupedRun(r, cut.query, isRange)
		values, complete := streamGroupedAnswerValues(buffered.body, cut.labels)
		switch {
		case buffered.code != 0 && buffered.code != http.StatusOK || !complete:
			outcome = "incomplete"
		case !p.streamGroupedRowsAbsent(ctx, cut, p.streamGroupedUnstored(cut, values), from, to):
			outcome = "unstored"
		case cut.ranked != "":
			// The ranking of the checked sum: the sum's answer is the response cache's now.
			if buffered = p.streamGroupedRun(r, cut.ranked, isRange); buffered.code != 0 && buffered.code != http.StatusOK {
				outcome = "ranking"
			}
		}
	}
	p.observeInternalOperation(ctx, "stream_grouped_parsers", outcome, time.Since(opStart))
	if outcome != "served" {
		return false
	}
	copyBackendHeaders(w.Header(), buffered.Header())
	setReplayContentType(w, http.StatusOK)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buffered.body)
	return true
}

// streamGroupedRun answers query through the handler, into a buffer.
func (p *Proxy) streamGroupedRun(r *http.Request, query string, isRange bool) *bufferedResponseWriter {
	buffered := &bufferedResponseWriter{header: make(http.Header)}
	child := cloneMetricQueryRequest(r, query)
	if isRange {
		p.handleQueryRange(buffered, child)
	} else {
		p.handleQuery(buffered, child)
	}
	return buffered
}

// streamGroupedCut is the query a stream grouped sum is answered from.
type streamGroupedCut struct {
	// query is the sum with no stage from the first parser on, in the canonical spelling.
	query string
	// ranked is the ranking of query, when the request ranks the sum, in the canonical spelling.
	ranked string
	// selector is the log query's stream selector.
	selector *logqlpkg.StreamSelector
	// labels are the grouping labels; pinned are the labels the selector matches with = and a value.
	labels, pinned []string
	window, offset time.Duration
}

// streamGroupedCutOf is the cut of expr, the sum lower.StreamGroupedParsersAt read from the same text, at the stage
// at.
func streamGroupedCutOf(expr logqlpkg.Expr, at int) (streamGroupedCut, bool) {
	va, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok {
		return streamGroupedCut{}, false
	}
	var rank *logqlpkg.VectorAggregation
	switch va.Op {
	case logqlpkg.VectorSort, logqlpkg.VectorSortDesc, logqlpkg.VectorTopK, logqlpkg.VectorBottomK:
		rank = va
		if va, ok = va.Inner.(*logqlpkg.VectorAggregation); !ok {
			return streamGroupedCut{}, false
		}
	}
	if va.Op != logqlpkg.VectorSum || va.Grouping == nil {
		return streamGroupedCut{}, false
	}
	ra, ok := va.Inner.(*logqlpkg.RangeAggregation)
	if !ok {
		return streamGroupedCut{}, false
	}
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok || lq.Selector == nil || at > len(lq.Pipeline) {
		return streamGroupedCut{}, false
	}
	cutLog := *lq
	cutLog.Pipeline = lq.Pipeline[:at:at]
	cutRange := *ra
	cutRange.Inner = &cutLog
	cutSum := *va
	cutSum.Inner = &cutRange
	ranked := ""
	if rank != nil {
		cutRank := *rank
		cutRank.Inner = &cutSum
		ranked = logqlpkg.CanonicalSpelling(cutRank.String())
	}
	var pinned []string
	for _, m := range lq.Selector.Matchers {
		if m.Op == logqlpkg.MatchEq && m.Value != "" {
			pinned = append(pinned, m.Name)
		}
	}
	return streamGroupedCut{
		pinned:   pinned,
		query:    logqlpkg.CanonicalSpelling(cutSum.String()),
		ranked:   ranked,
		selector: lq.Selector,
		labels:   va.Grouping.Labels,
		window:   ra.RangeDuration,
		offset:   ra.OffsetDuration,
	}, true
}

// streamGroupedLabelsListed reports whether every grouping label of cut can be a stream label: one its selector
// pins (an equality matcher with a value), or, once the tenant's stream label names of the last hour are listed
// (awaitTenantStreamLabelNames), one of them. It only spares the cut query's checks and read for a grouping by a
// parsed key: the checks decide the answer, so a stale listing, one of another window or one that failed (no names:
// the checks run) never serves a wrong one.
func (p *Proxy) streamGroupedLabelsListed(ctx context.Context, cut streamGroupedCut) bool {
	var names []string
	for _, label := range cut.labels {
		if slices.Contains(cut.pinned, label) {
			continue
		}
		if names == nil {
			if names = p.awaitTenantStreamLabelNames(ctx); len(names) == 0 {
				return true
			}
		}
		if !slices.Contains(names, label) {
			return false
		}
	}
	return true
}

// streamGroupedAnswerValues is the values each label holds in a successful matrix or vector answer, and whether
// every series holds every one of labels with a value: no counted line lacked one (Loki drops an empty label, so an
// empty value is a missing one).
func streamGroupedAnswerValues(body []byte, labels []string) (map[string][]string, bool) {
	var answer struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &answer) != nil || answer.Status != "success" ||
		answer.Data.ResultType != "matrix" && answer.Data.ResultType != "vector" {
		return nil, false
	}
	seen := make(map[string]map[string]bool, len(labels))
	for _, label := range labels {
		seen[label] = map[string]bool{}
	}
	for _, series := range answer.Data.Result {
		for _, label := range labels {
			value := series.Metric[label]
			if value == "" {
				return nil, false
			}
			seen[label][value] = true
		}
	}
	values := make(map[string][]string, len(labels))
	for label, set := range seen {
		for value := range set {
			values[label] = append(values[label], value)
		}
		sort.Strings(values[label])
	}
	return values, true
}

// streamGroupedUnlabelled matches a line whose stream lacks one of the labels of cut its selector does not pin, or
// is nil when the selector pins them all.
func (p *Proxy) streamGroupedUnlabelled(cut streamGroupedCut) logsql.FilterExpr {
	var unlabelled logsql.FilterExpr
	for _, label := range cut.labels {
		if !slices.Contains(cut.pinned, label) {
			held := logsql.StreamFilter{Matchers: []logsql.LabelMatcher{{Name: p.streamGroupedName(label), Op: "=~", Value: ".+"}}}
			unlabelled = orFilter(unlabelled, logsql.NotExpr{Expr: held})
		}
	}
	return unlabelled
}

// streamGroupedUnstored matches a line, for a label of cut the cut query reads from the stored field, whose stream
// value is not one of the answer's values or whose stored field differs from it, or is nil when the cut query reads
// every label from the stream (the stream value restore) or the answer is empty. Past maxStreamGroupedValues values
// it matches every line: the request keeps the evaluator.
func (p *Proxy) streamGroupedUnstored(cut streamGroupedCut, values map[string][]string) logsql.FilterExpr {
	var unstored logsql.FilterExpr
	n := 0
	for _, label := range cut.labels {
		if p.restorableName(label) || len(values[label]) == 0 {
			continue
		}
		name := p.streamGroupedName(label)
		var held logsql.FilterExpr
		for _, value := range values[label] {
			if n++; n > maxStreamGroupedValues {
				return logsql.Wildcard{}
			}
			one := logsql.AndExpr{
				Left:  logsql.StreamFilter{Matchers: []logsql.LabelMatcher{{Name: name, Op: "=", Value: value}}},
				Right: logsql.FieldFilter{Field: name, Op: logsql.FieldOpExact, Value: value},
			}
			held = orFilter(held, logsql.Group{Expr: one})
		}
		unstored = orFilter(unstored, logsql.NotExpr{Expr: logsql.Group{Expr: held}})
	}
	return unstored
}

// streamGroupedName is the stored name of a grouping label.
func (p *Proxy) streamGroupedName(label string) string {
	if p.labelTranslator != nil {
		return p.labelTranslator.ToVL(label)
	}
	return label
}

// streamGroupedRowsAbsent reports whether no line of the cut's selector in [from, to] matches filter (true for a
// nil one): one VictoriaLogs read of at most one row's time. A failure, or a selector whose translation is not one
// filter, answers false.
func (p *Proxy) streamGroupedRowsAbsent(ctx context.Context, cut streamGroupedCut, filter logsql.FilterExpr, from, to time.Time) bool {
	if filter == nil {
		return true
	}
	base, err := p.streamGroupedSelectorFilter(cut)
	if err != nil || base == nil {
		return false
	}
	check := logsql.Query{
		Filter: logsql.AndExpr{Left: logsql.Group{Expr: base}, Right: logsql.Group{Expr: filter}},
		Pipes:  []logsql.Pipe{logsql.PipeFields{Labels: []string{"_time"}}, logsql.PipeLimit{N: 1}},
	}
	text := check.String()
	if len(text) > vlDefaultMaxQueryLen {
		return false
	}
	params := url.Values{"query": {text}, "start": {from.UTC().Format(time.RFC3339Nano)}, "end": {to.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	return err == nil && resp.StatusCode < 400 && len(strings.TrimSpace(string(body))) == 0
}

// streamGroupedSelectorFilter is the VictoriaLogs filter of the rows the cut's stream selector reads, lowered from
// its typed plan with the read path's label spelling and stream fields (lower.PlanLogFilter, as a delete reads it).
func (p *Proxy) streamGroupedSelectorFilter(cut streamGroupedCut) (logsql.FilterExpr, error) {
	plan, err := queryplan.Build(&logqlpkg.LogQuery{Selector: cut.selector}, queryplan.Inputs{})
	if err != nil {
		return nil, err
	}
	queryplan.Resolve(plan)
	p.configMu.RLock()
	labelFn, streamFields := p.labelTranslator.ToVL, p.streamFieldsMap
	p.configMu.RUnlock()
	prof := p.labelTranslator.withAliases(loweringProfile(labelFn, streamFields, p.logsqlCapabilities(), false))
	return lower.PlanLogFilter(plan, prof)
}

// orFilter is left OR right, or right alone when left is nil.
func orFilter(left, right logsql.FilterExpr) logsql.FilterExpr {
	if left == nil {
		return right
	}
	return logsql.OrExpr{Left: left, Right: right}
}
