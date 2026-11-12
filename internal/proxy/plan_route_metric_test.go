package proxy

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The route parity prediction of a metric query from its plan: lower.Route decides the route (the VictoriaLogs
// endpoint, how the windows are read, the level probe), and metricRoute writes the requests and the route steps the
// handlers record for it, under the route parity request profile and stub answer. A request is predicted by its
// shape (routeCallShape): the endpoint, every parameter, the query's filters (the text before its first pipe) and
// its route pipes (the window phase filter and the stats pipes on). The pipes between them are not compared: the
// handlers build them from the lowering's text (the level chain the presence probe masks, the scratch-field delete
// they keep or drop, the stream identity of a bare range aggregation), and the corpus shadow
// (internal/querycorpus TestPlanShadow) holds the lowering's own text to the translator byte for byte.

// routeLoweringProfile is the lowering profile of the route parity proxy (the options its translator gets).
var routeLoweringProfile = sync.OnceValue(func() lower.Profile {
	p, err := New(Config{BackendURL: "http://vl.invalid", Cache: cache.New(time.Minute, 10), LogLevel: "error"})
	if err != nil {
		panic(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	caps := p.logsqlCapabilities()
	caps.NoDetectedLevelBodyScan = !p.detectedLevelBodyScan
	return loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, nil), p.streamFieldsMap, caps, false)
})

// routeLabelFns are the label translations of the route parity proxy, to Loki's names and back: the pushdown of a
// without clause reads both (translator.PushDownWithout).
var routeLabelFns = sync.OnceValues(func() (toVL, toLoki translator.LabelTranslateFunc) {
	p, err := New(Config{BackendURL: "http://vl.invalid", Cache: cache.New(time.Minute, 10), LogLevel: "error"})
	if err != nil {
		panic(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	return p.labelTranslator.ToVL, p.labelTranslator.ToLoki
})

// routeTimeout is the timeout every VictoriaLogs request of the route parity proxy carries.
const routeTimeout = "120000ms"

func routeTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// routeLevelProbeShape is the query shape of the detected_level presence probe.
const routeLevelProbeShape = "level presence probe"

// routeShapeChain marks a query whose base holds the derived detected_level chain.
const routeShapeChain = "[level chain] "

var routeShapeSpace = regexp.MustCompile(`\s+`)

// routeQueryShape is the route part of a VictoriaLogs query: its filters, then the presence probe, or the pipes
// from the window phase filter or the first stats pipe on, whitespace folded, after a mark for the derived level
// chain in the pipes between.
func routeQueryShape(q string) string {
	if strings.Contains(q, " as p0,") {
		return routeShapeFilters(q) + routeLevelProbeShape
	}
	filters := routeShapeFilters(q)
	at := strings.Index(q, "| stats ")
	if i := strings.Index(q, "| math ((_time - "); i >= 0 && (at < 0 || i < at) {
		at = i
	}
	shape := "rows"
	if at >= 0 {
		shape = routeShapeSpace.ReplaceAllString(strings.TrimSpace(q[at:]), " ")
	}
	if strings.Contains(q, translate.RateDetectedLevelChain()) || strings.Contains(q, "as detected_level") {
		shape = routeShapeChain + shape
	}
	return filters + shape
}

// routeShapeFilters is the filters of q, before its first pipe, whitespace folded: the lowering's filters as the
// handlers send them.
func routeShapeFilters(q string) string {
	if i := translator.FirstTopLevelPipe(q); i >= 0 {
		q = q[:i]
	}
	return routeShapeSpace.ReplaceAllString(strings.TrimSpace(q), " ") + " … "
}

// routeCallShape is a recorded VictoriaLogs request with its query replaced by routeQueryShape.
func routeCallShape(call string) string {
	path, raw, _ := strings.Cut(call, "?")
	form, err := url.ParseQuery(raw)
	if err != nil {
		return call
	}
	form.Set("query", routeQueryShape(form.Get("query")))
	return path + "?" + form.Encode()
}

func routeCallShapes(calls []string) []string {
	out := make([]string, len(calls))
	for i, c := range calls {
		out[i] = routeCallShape(c)
	}
	return uniqueSorted(out)
}

// metricRoute predicts the route of a range or vector aggregation root.
func metricRoute(q string, p *queryplan.Plan, rp routeProfile, data bool) (routeRecord, error) {
	return predictMetricRoute(q, p, rp, data, nil)
}

// predictMetricRoute is metricRoute with the route lower.Route decided changed by mutate first (nil changes
// nothing): the mutation test changes one decision to show the harness sees it.
func predictMetricRoute(q string, p *queryplan.Plan, rp routeProfile, data bool, mutate func(*lower.MetricRoute)) (routeRecord, error) {
	rangeDur := firstRange(p.Root)
	// The handlers read a range aggregation at the root by its spelling: one spelled otherwise ("rate (", a
	// parenthesised query) takes another route there (a misreading the plan does not copy).
	if ra, ok := p.Root.(*queryplan.RangeAgg); ok && !strings.HasPrefix(strings.TrimSpace(q), string(ra.Op)+"(") {
		if _, err := lower.Plan(p, routeLoweringProfile()); err != nil {
			return routeRecord{}, err
		}
		return routeRecord{}, &lower.NotLoweredError{Construct: "RangeAgg route spelling"}
	}
	// The handlers send the string translator's filter on a label called _time, _stream, _stream_id or _msg
	// (semantics/translator-writes-reserved-field-names): the plan's stream filter is the fix, not their request.
	if lower.ReadsReservedField(p, routeLoweringProfile()) {
		if _, err := lower.Plan(p, routeLoweringProfile()); err != nil {
			return routeRecord{}, err
		}
		return routeRecord{}, &lower.NotLoweredError{Construct: lower.Construct(p.Root) + " route reserved field"}
	}
	req := lower.RouteRequest{Instant: rp.instant}
	var start, end time.Time
	sent := time.Duration(0)
	if !rp.instant {
		end = routeEnd
		start = end.Add(-rp.window(rangeDur))
		// The step as runRoute sends it, then as Loki evaluates it: in whole milliseconds.
		sent, _ = parsePositiveStepDuration(strconv.FormatFloat(rp.step(rangeDur).Seconds(), 'f', -1, 64))
		req.Step, req.Span = sent.Truncate(time.Millisecond), end.Sub(start)
	}
	route, err := lower.Route(p, routeLoweringProfile(), req)
	if err != nil {
		return routeRecord{}, err
	}
	if mutate != nil {
		mutate(&route)
	}
	// Loki's resolution limit, checked before anything else.
	if !rp.instant && req.Span/sent > lokiMaxPointsPerSeries {
		return routeRecord{Status: 400, queryShape: true}, nil
	}
	out := routeRecord{Status: 200, queryShape: true}
	steps := []string{"operation prefer_working_parser bypass", "operation translate_query translated"}
	if rp.instant {
		t := routeEnd
		text, err := routeStatsQuery(q, route, "", time.Time{}, 0)
		if err != nil {
			return routeRecord{}, err
		}
		form := url.Values{
			"query":   {routeQueryShape(text)},
			"start":   {routeTime(t.Add(-route.Range + 1))},
			"end":     {routeTime(t.Add(1))},
			"time":    {strconv.FormatInt(t.Unix(), 10)},
			"timeout": {routeTimeout},
		}
		out.Calls = []string{"/select/logsql/stats_query?" + form.Encode()}
		outcome := "no_results"
		if data {
			outcome = routeLabelsOutcome(p)
		}
		out.Steps = uniqueSorted(append(steps, "operation translate_stats_response_labels "+outcome))
		return out, nil
	}
	steps = append(steps, "operation query_steps_eligibility disabled")
	params := url.Values{"timeout": {routeTimeout}}
	var fetchStart time.Time
	var phase string
	switch route.Window {
	case lower.WindowTumbling:
		// Lines after the last whole window belong to no sample.
		end = start.Add(end.Sub(start) / route.Range * route.Range)
		fetchStart = start.Add(-route.Range)
		setSlidingStatsRangeParams(params, fetchStart, end, route.Bucket)
		fetchStart = fetchStart.Add(1)
		params.Set("start", routeTime(fetchStart))
		if data {
			steps = append(steps, "operation trim_translate_stats_qr "+routeTrimOutcome(p, route, start, req))
		}
	case lower.WindowBuckets:
		fetchStart = start.Add(-route.Range)
		setSlidingStatsRangeParams(params, fetchStart, end, route.Bucket)
	case lower.WindowPhase:
		if route.StepBuckets {
			// Lines after the last whole step belong to no sample.
			end = start.Add(req.Span / req.Step * req.Step)
		}
		fetchStart = start.Add(-route.Bucket)
		setSlidingStatsRangeParams(params, fetchStart, end, route.Bucket)
		phase = "phase"
		if route.StepBuckets {
			fetchStart = fetchStart.Add(1)
			params.Set("start", routeTime(fetchStart))
			if data {
				steps = append(steps, "operation trim_translate_stats_qr "+routeTrimOutcome(p, route, start, req))
			}
		}
	}
	text, err := routeStatsQuery(q, route, phase, start, req.Step)
	if err != nil {
		return routeRecord{}, err
	}
	params.Set("query", routeQueryShape(text))
	out.Calls = append(out.Calls, "/select/logsql/stats_query_range?"+params.Encode())
	if route.LevelProbe {
		probe := url.Values{"query": {routeShapeFilters(route.Text) + routeLevelProbeShape}, "start": {routeTime(fetchStart)}, "end": {routeTime(end.Add(2))}, "timeout": {routeTimeout}}
		out.Calls = append(out.Calls, "/select/logsql/query?"+probe.Encode())
	}
	out.Calls, out.Steps = uniqueSorted(out.Calls), uniqueSorted(steps)
	return out, nil
}

// routeStatsQuery is the stats request of route for the query q, as far as its shape goes: the lowered text with the
// stream identity grouping of a range aggregation at the root or of a without clause, and on a windowed route the
// window pipes (the phase filter) and the bucket stats of the range function, grouped like the lowered query's first
// stats pipe. A without clause VictoriaLogs folds (lower.FoldPushed, lower.FoldCut) is written by the handlers'
// builders (translator.PushDownWithout, translator.WithoutStreamCut), which refuse a query the route should not have
// pushed down: that is an error, a mismatch for the harness.
func routeStatsQuery(q string, route lower.MetricRoute, phase string, start time.Time, step time.Duration) (string, error) {
	text, err := routeStatsText(q, route, phase, start, step)
	if err != nil || !route.Counted {
		return text, err
	}
	// The VictoriaLogs client flags the rows the conversion rejects in every stats pipe that follows the gate
	// (the handlers' unwrapCountingRewrite); that it does is the route's.
	counted, _, _, _, _, ok := unwrapCountingRewrite(text, func(string, string) (bool, bool) { return true, true })
	if !ok {
		return "", fmt.Errorf("route counts the conversion errors of %q; the handlers' rewrite does not", q)
	}
	return counted, nil
}

// routeStatsText is routeStatsQuery before the client's rewrite.
func routeStatsText(q string, route lower.MetricRoute, phase string, start time.Time, step time.Duration) (string, error) {
	text := route.Text
	if route.Identity && !strings.Contains(text, "| stats by (") {
		// The grouping text is the handlers' builder; that the series keep their identity is the route's.
		text = addStatsByStreamClause(text, nil)
	}
	toVL, toLoki := routeLabelFns()
	switch route.Window {
	case lower.WindowInstant:
		if route.Ungrouped {
			text += ", count() as " + emptyInputGuardAlias
		}
		return text, nil
	case lower.WindowTumbling:
		if route.Fold == lower.FoldPushed {
			pushed, ok := translator.PushDownWithout(q, text, toVL, toLoki)
			if !ok {
				return "", fmt.Errorf("route pushes the without of %q down; the handlers' builder does not", q)
			}
			return pushed, nil
		}
		return text, nil
	}
	at := strings.Index(text, "| stats ")
	by := ""
	if rest := text[at+len("| stats "):]; strings.HasPrefix(rest, "by (") && !route.SumAll {
		by = rest[:strings.Index(rest, ")")+2]
	}
	if route.GroupPushed {
		// The extreme per bucket and group of the outer labels: the grouping is the last stats pipe's, with no series.
		by = ""
		if rest := text[strings.LastIndex(text, "| stats ")+len("| stats "):]; strings.HasPrefix(rest, "by (") {
			by = rest[:strings.Index(rest, ")")+2]
		}
	}
	agg, folded := "count() as c", "sum(c) as c"
	if route.Counted {
		// An unwrap function keeps its stats on a window up to the step; a longer one merges the partial aggregate
		// of each bucket (unwrapBucketAgg).
		if route.Window == lower.WindowBuckets {
			agg = unwrapBucketAgg(strings.TrimSuffix(string(route.Op), "_over_time"))
		} else {
			agg, by = text[at+len("| stats "):], ""
		}
	}
	if route.Op == logql.RangeBytesOverTime || route.Op == logql.RangeBytesRate {
		agg, folded = "sum_len(_msg) as c, count() as __sample_count", "sum(c) as c, sum(__sample_count) as __sample_count"
	}
	base := strings.TrimRight(text[:at], " ")
	if phase != "" {
		// Where the filter goes is the handlers' text placement (withWindowPhaseFilter); that it goes is the route's.
		base = withWindowPhaseFilter(context.Background(), base, start, step, route.Range)
	}
	out := base + " | stats " + by + agg
	if route.Fold == lower.FoldCut {
		cut, ok := translator.WithoutStreamCut(q, toVL, toLoki)
		if !ok {
			return "", fmt.Errorf("route cuts the without of %q out of the stream; the handlers' builder does not", q)
		}
		out += " " + cut + "| stats " + by + folded
	}
	return out, nil
}

// routeLabelsOutcome is how the instant stats answer's labels are rewritten (translate_stats_response_labels) when
// the stub answers its series ({app, level}).
func routeLabelsOutcome(p *queryplan.Plan) string {
	if v, ok := p.Root.(*queryplan.VectorAgg); ok && v.By != nil && !v.By.Without && len(v.By.Labels) == 1 && v.By.Labels[0] == "level" {
		return "noop"
	}
	return "translated"
}

// routeTrimOutcome is how a tumbling answer's axis is trimmed and folded (trim_translate_stats_qr) when the stub
// answers its one sample, 30s before the request's end: a sample after the last whole step the request holds is
// dropped, which leaves the fold nothing to relabel.
func routeTrimOutcome(p *queryplan.Plan, route lower.MetricRoute, start time.Time, req lower.RouteRequest) string {
	step := route.Range
	if route.Window == lower.WindowPhase {
		step = req.Step
	}
	lastWhole := start.Add(req.Span / step * step)
	switch {
	case routeEnd.Add(-30 * time.Second).After(lastWhole):
		return "ok"
	case routeLabelsOutcome(p) == "noop":
		return "noop"
	}
	return "folded"
}

// routeWithoutRows are up to n corpus rows with a without clause for each way the handlers fold one under some request
// profile (the proxy folds the rows, VictoriaLogs folds the range, a sliding window reads the cut buckets).
func routeWithoutRows(t *testing.T, n int) []routeRow {
	t.Helper()
	var out []routeRow
	count := map[lower.WithoutFold]int{}
	for _, row := range routeRows(t, filepath.Join("..", ".."), 0) {
		kinds := map[lower.WithoutFold]bool{}
		for _, rp := range routeProfiles() {
			req := lower.RouteRequest{Instant: rp.instant}
			if !rp.instant {
				req.Step, req.Span = rp.step(row.rangeDur), rp.window(row.rangeDur)
			}
			if route, err := lower.Route(row.plan, routeLoweringProfile(), req); err == nil && route.Fold != lower.FoldNone {
				kinds[route.Fold] = true
			}
		}
		take := false
		for k := range kinds {
			take = take || count[k] < n
		}
		if !take {
			continue
		}
		for k := range kinds {
			count[k]++
		}
		out = append(out, row)
	}
	for _, k := range []lower.WithoutFold{lower.FoldRows, lower.FoldPushed, lower.FoldCut} {
		if count[k] == 0 {
			t.Fatalf("no corpus row is folded the way %d", k)
		}
	}
	return out
}

// Each decision of the route rule is held by the harness: a route with one decision changed mispredicts every run
// of the native rows it changes, and the unchanged rule predicts them all.
//
// conformance: architecture-syntax-package-layering
func TestMetricRouteCatchesAWrongDecision(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	var rows []routeRow
	for _, row := range routeRows(t, filepath.Join("..", ".."), 0) {
		if _, err := lower.Route(row.plan, routeLoweringProfile(), lower.RouteRequest{Instant: true}); err == nil {
			rows = append(rows, row)
		}
		if len(rows) == 25 {
			break
		}
	}
	if len(rows) < 25 {
		t.Fatalf("%d native metric rows, want 25", len(rows))
	}
	rows = append(rows, routeWithoutRows(t, 6)...)
	rows = append(rows, routeRangeFunctionRows(t, 8)...)
	rows = append(rows, routeVectorUnwrapRows(t, 6)...)
	native := func(mutate func(*lower.MetricRoute)) routePrediction {
		return func(q string, p *queryplan.Plan, rp routeProfile, data bool, _ routeRecord) (routeRecord, error) {
			return predictMetricRoute(q, p, rp, data, mutate)
		}
	}
	sec, problems := runRouteParity(t, rows, native(nil))
	if sec.Native == 0 || sec.Mismatches != 0 {
		t.Fatalf("the rule: %+v\n%s", sec, joinFirstRoute(problems, 5))
	}
	for _, c := range []struct {
		name   string
		mutate func(*lower.MetricRoute)
	}{
		{"tumbling read as buckets", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowTumbling {
				r.Window, r.Bucket = lower.WindowBuckets, r.Range
			}
		}},
		{"buckets read as tumbling", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowBuckets {
				r.Window, r.Bucket = lower.WindowTumbling, r.Range
			}
		}},
		{"phase read as buckets", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowPhase {
				r.Window = lower.WindowBuckets
			}
		}},
		{"no level probe", func(r *lower.MetricRoute) {
			if r.Window != lower.WindowInstant {
				r.LevelProbe = false
			}
		}},
		{"no stream identity", func(r *lower.MetricRoute) { r.Identity = false }},
		{"guard flipped", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowInstant {
				r.Ungrouped = !r.Ungrouped
			}
		}},
		{"pushed fold read as the proxy's", func(r *lower.MetricRoute) {
			if r.Fold == lower.FoldPushed {
				r.Fold = lower.FoldRows
			}
		}},
		{"proxy fold read as pushed", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowTumbling && r.Fold == lower.FoldRows {
				r.Fold = lower.FoldPushed
			}
		}},
		{"cut buckets read as the proxy's fold", func(r *lower.MetricRoute) {
			if r.Fold == lower.FoldCut {
				r.Fold = lower.FoldRows
			}
		}},
		{"proxy fold read as cut buckets", func(r *lower.MetricRoute) {
			if (r.Window == lower.WindowBuckets || r.Window == lower.WindowPhase) && r.Fold == lower.FoldRows {
				r.Fold = lower.FoldCut
			}
		}},
		{"absent read as a stream series", func(r *lower.MetricRoute) {
			if r.Op == logql.RangeAbsentOverTime {
				r.Ungrouped, r.Identity, r.LevelProbe = false, true, true
			}
		}},
		{"conversion flag dropped", func(r *lower.MetricRoute) { r.Counted = false }},
		{"step buckets read as the proxy's phase", func(r *lower.MetricRoute) { r.StepBuckets = false }},
		{"count phase read as step buckets", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowPhase && !r.Counted {
				r.StepBuckets = true
			}
		}},
		{"grouped unwrap read as the stream's series", func(r *lower.MetricRoute) {
			if r.Counted && !r.Identity {
				r.Identity, r.LevelProbe = true, true
			}
		}},
		{"group pushdown dropped", func(r *lower.MetricRoute) { r.GroupPushed = false }},
		{"group pushdown on a read per series", func(r *lower.MetricRoute) {
			if r.Counted && r.Window == lower.WindowBuckets && (r.Op == logql.RangeMaxOverTime || r.Op == logql.RangeMinOverTime) {
				r.GroupPushed = true
			}
		}},
		{"sum of all streams read for a non-additive function", func(r *lower.MetricRoute) {
			if r.Counted && r.Op != logql.RangeSumOverTime {
				r.SumAll = true
			}
		}},
		{"sum of all streams flipped", func(r *lower.MetricRoute) {
			if r.Window == lower.WindowBuckets || r.Window == lower.WindowPhase {
				r.SumAll = !r.SumAll
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			// The unchanged rule predicts every run, so a run is mispredicted exactly when its prediction changed.
			changed := 0
			for _, row := range rows {
				for _, rp := range routeProfiles() {
					for _, data := range []bool{true, false} {
						want, err := predictMetricRoute(row.q, row.plan, rp, data, nil)
						if err != nil {
							continue
						}
						if got, err := predictMetricRoute(row.q, row.plan, rp, data, c.mutate); err != nil || !got.equal(want) {
							changed++
						}
					}
				}
			}
			bad, _ := runRouteParity(t, rows, native(c.mutate))
			if changed == 0 || bad.Mismatches != changed {
				t.Fatalf("a changed decision went unseen: %d predictions changed, %d runs mispredicted (%+v)", changed, bad.Mismatches, bad)
			}
		})
	}
}

// The without clauses the corpus does not hold: each is held to the handlers' route on every request profile, and the
// fold the rule decides is pinned per profile (instant, step = range, step < range, step > range, 24h chunk): n none,
// r the proxy folds the rows, p VictoriaLogs folds the range (pushed), c a sliding window reads the cut buckets.
//
// conformance: architecture-syntax-package-layering
func TestMetricRouteWithoutEdges(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, c := range []struct{ q, folds string }{
		// the pushdown and the cut: plain labels, any of the five operators, count and byte functions
		{`sum without (pod) (count_over_time({app="uw"}[1m]))`, "rpccp"},
		{`sum without (pod) (bytes_over_time({app="uw"}[1m]))`, "rpccp"},
		{`avg without (pod) (count_over_time({app="uw"}[1m]))`, "rprrp"},
		{`count without (pod) (rate({app="uw"}[1m]))`, "rprrp"},
		{`sum(count_over_time({app="uw"}[1m])) without (pod, zone)`, "rpccp"},
		{`sum without (Pod1, pod) (count_over_time({app="uw"}[30s]))`, "rpccc"},
		// a rate keeps the per-stream route on a sliding window; so does a bytes rate
		{`sum without (pod) (rate({app="uw"}[1m]))`, "rprrp"},
		{`sum without (pod) (bytes_rate({app="uw"}[1m]))`, "rprrp"},
		// filters: a line filter keeps the rule; a label filter ends the cut (not the push); a pattern filter and a
		// line that names _msg or an unpack pipe end the push (not the cut)
		{`sum without (pod) (count_over_time({app="uw"} |~ "a.*b" [1m]))`, "rpccp"},
		{`sum without (pod) (count_over_time({app="uw"} != "a" or "b" [1m]))`, "rpccp"},
		{`sum without (pod) (count_over_time({app="uw"} |= ip("1.2.3.4") [1m]))`, "rpccp"},
		{`sum without (pod) (count_over_time({app="uw"} | env="b" [1m]))`, "rprrp"},
		{`avg without (pod) (count_over_time({app="uw"} | env=~"b.*" |= "x" [1m]))`, "rprrp"},
		{`sum without (pod) (count_over_time({app="uw"} |> "<_> x <_>" [1m]))`, "rrccr"},
		{`sum without (pod) (count_over_time({app="uw"} !> "<_> x <_>" [1m]))`, "rrccr"},
		{`sum without (pod) (count_over_time({app="uw"} |= "_msg" [1m]))`, "rrccr"},
		{`sum without (pod) (count_over_time({app="uw"} |= "unpack_json" [1m]))`, "rrccr"},
		{`sum without (pod) (count_over_time({app="uw", env="_msg"}[1m]))`, "rrccr"},
		{`sum without (pod) (count_over_time({app="uw"} |= "sum by (x) (" [1m]))`, "rpccp"},
		// labels the cut cannot name: derived, with an underscore, or a stream key
		{`sum without (service_name) (count_over_time({app="uw"}[1m]))`, "rrrrr"},
		{`sum without (detected_level) (count_over_time({app="uw"}[1m]))`, "rrrrr"},
		{`sum without (level) (rate({app="uw"}[1m]))`, "rrrrr"},
		{`sum without (pod_name) (count_over_time({app="uw"}[1m]))`, "rrrrr"},
		{`sum without (pod, k8s_pod) (count_over_time({app="uw"}[1m]))`, "rrrrr"},
		{`sum without (_stream) (count_over_time({app="uw"}[1m]))`, "rrrrr"},
		// levels above a without: by columns push down, derived labels and a second without do not, and only a single
		// sum cuts a sliding window
		{`sum by (zone) (sum without (pod) (count_over_time({app="uw"}[1m])))`, "rprrp"},
		{`sum(sum without (pod) (count_over_time({app="uw"}[1m])))`, "rprrp"},
		{`min by () (sum without (node) (count_over_time({app="uw"}[1m])))`, "rprrp"},
		{`sum by (zone) (sum by (pod) (sum without (node) (count_over_time({app="uw"}[1m]))))`, "rprrp"},
		{`sum by (level) (sum without (pod) (count_over_time({app="uw"}[1m])))`, "rrrrr"},
		{`sum by (service_name) (sum without (pod) (count_over_time({app="uw"}[1m])))`, "rrrrr"},
		{`sum by (detected_level) (sum without (pod) (count_over_time({app="uw"}[1m])))`, "rrrrr"},
		{`sum without (zone) (sum without (pod) (count_over_time({app="uw"}[1m])))`, "rrrrr"},
		{`sum without (zone) (sum by (pod) (count_over_time({app="uw"}[1m])))`, "rrrrr"},
		{`sum by (zone) (sum without (node) (sum by (pod) (count_over_time({app="uw"}[1m]))))`, "rrrrr"},
		// no without clause
		{`sum by (pod) (count_over_time({app="uw"}[1m]))`, "nnnnn"},
	} {
		t.Run(c.q, func(t *testing.T) {
			expr, msg := logql.ValidateLogQLExpr(c.q)
			if msg != "" || expr == nil {
				t.Fatalf("%q: %s", c.q, msg)
			}
			plan, err := queryplan.Build(expr, queryplan.Inputs{})
			if err != nil {
				t.Fatal(err)
			}
			queryplan.Resolve(plan)
			row := routeRow{q: c.q, plan: plan, construct: lower.Construct(plan.Root), rangeDur: firstRange(plan.Root)}
			folds := ""
			for _, rp := range routeProfiles() {
				req := lower.RouteRequest{Instant: rp.instant}
				if !rp.instant {
					req.Step, req.Span = rp.step(row.rangeDur), rp.window(row.rangeDur)
				}
				route, err := lower.Route(plan, routeLoweringProfile(), req)
				if err != nil {
					t.Fatalf("%s: %v", rp.name, err)
				}
				folds += string("nrpc"[route.Fold])
			}
			if folds != c.folds {
				t.Errorf("folds %s, want %s", folds, c.folds)
			}
			if sec, problems := runRouteParity(t, []routeRow{row}, nativeRoute); sec.Mismatches != 0 || sec.Native != sec.Runs {
				t.Fatalf("%+v\n%s", sec, joinFirstRoute(problems, 4))
			}
		})
	}
}

// routeRowOf is the plan-corpus row of one query.
func routeRowOf(t *testing.T, q string) routeRow {
	t.Helper()
	expr, msg := logql.ValidateLogQLExpr(q)
	if msg != "" || expr == nil {
		t.Fatalf("%q: %s", q, msg)
	}
	plan, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	queryplan.Resolve(plan)
	return routeRow{q: q, plan: plan, construct: lower.Construct(plan.Root), rangeDur: firstRange(plan.Root)}
}

// routeRangeFunctionRows are up to n corpus rows of each kind of range function the rule decides beyond the line
// counts and byte sums: absent_over_time, a function over an unwrapped label, and one with a grouping.
func routeRangeFunctionRows(t *testing.T, n int) []routeRow {
	t.Helper()
	var out []routeRow
	count := map[string]int{}
	for _, row := range routeRows(t, filepath.Join("..", ".."), 0) {
		ra, ok := row.plan.Root.(*queryplan.RangeAgg)
		if !ok {
			continue
		}
		kind := "unwrap"
		switch {
		case ra.Op == logql.RangeAbsentOverTime:
			kind = "absent"
		case ra.By != nil:
			kind = "unwrap by"
		}
		if _, err := lower.Route(row.plan, routeLoweringProfile(), lower.RouteRequest{Instant: true}); err != nil || count[kind] == n || kind == "unwrap" && ra.Op != logql.RangeSumOverTime && count[kind] >= n/2 {
			continue
		}
		count[kind]++
		out = append(out, row)
	}
	for _, k := range []string{"absent", "unwrap", "unwrap by"} {
		if count[k] == 0 {
			t.Fatalf("no corpus row of kind %s", k)
		}
	}
	return out
}

// routeVectorUnwrapRows are up to n corpus rows of each kind of vector aggregation over a function of an unwrapped
// label: the extreme pushed into the buckets, the sum of all streams, the sum of an extreme or an average (which keeps
// the series), a count, minimum, maximum or average per series, a sum by labels, and a without clause.
func routeVectorUnwrapRows(t *testing.T, n int) []routeRow {
	t.Helper()
	var out []routeRow
	count := map[string]int{}
	for _, row := range routeRows(t, filepath.Join("..", ".."), 0) {
		if _, ok := row.plan.Root.(*queryplan.VectorAgg); !ok {
			continue
		}
		kinds := map[string]bool{}
		for _, rp := range routeProfiles() {
			req := lower.RouteRequest{Instant: rp.instant}
			if !rp.instant {
				req.Step, req.Span = rp.step(row.rangeDur), rp.window(row.rangeDur)
			}
			route, err := lower.Route(row.plan, routeLoweringProfile(), req)
			if err != nil || !route.Counted {
				continue
			}
			v := row.plan.Root.(*queryplan.VectorAgg)
			switch {
			case route.GroupPushed:
				kinds["pushed"] = true
			case route.SumAll && route.Window != lower.WindowInstant:
				kinds["sum of all"] = true
			case route.Fold != lower.FoldNone:
				kinds["without"] = true
			case v.Op == logql.VectorSum && route.Op == logql.RangeSumOverTime:
				kinds["sum by"] = true
			case v.Op == logql.VectorSum:
				kinds["sum of an extreme"] = true
			default:
				kinds["per series"] = true
			}
		}
		take := false
		for k := range kinds {
			take = take || count[k] < n
		}
		if !take {
			continue
		}
		for k := range kinds {
			count[k]++
		}
		out = append(out, row)
	}
	for _, k := range []string{"pushed", "sum of all", "without", "sum by", "sum of an extreme", "per series"} {
		if count[k] == 0 {
			t.Fatalf("no corpus row of kind %s", k)
		}
	}
	return out
}

// The range functions the corpus does not hold: each is held to the handlers' route on every request profile, or,
// where the rule delegates it, to the name of its delegation and to no mismatch. The conversions, the filters before
// the unwrap, the groupings, the ranges that divide the span and the ones that do not.
//
// conformance: architecture-syntax-package-layering
func TestMetricRouteRangeFunctionEdges(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, c := range []struct{ q, delegated string }{
		// absent_over_time: the filters the rule takes, a range that divides the span and one that does not
		{q: `absent_over_time({app="ra"}[5m])`},
		{q: `absent_over_time({app="ra"}[7m])`},
		{q: `absent_over_time({app="ra"} |= "x" [1m])`},
		{q: `absent_over_time({app="ra"} != "x" |~ "a.*b" [90s])`},
		{q: `absent_over_time({app="ra", env=~"p.*"} | zone="z" [1m])`},
		{q: `absent_over_time({app="ra"} |> "<_> x" [1m])`},
		{q: `absent_over_time({app="ra"} | detected_level="error" [1m])`, delegated: "RangeAgg route level filter"},
		{q: `absent_over_time({app="ra"} | json [1m])`, delegated: "RangeAgg route pipeline"},
		{q: `absent_over_time({app="ra"} | unwrap n [1m])`, delegated: "RangeAgg route unwrap absent"},
		{q: `absent_over_time({app="ra"}[1m] offset 5m)`, delegated: "RangeAgg route offset"},
		// the four functions over an unwrapped label, with every conversion
		{q: `sum_over_time({app="ra"} | unwrap n [1m])`},
		{q: `sum_over_time({app="ra"} | unwrap n [7m])`},
		{q: `sum_over_time({app="ra"} | unwrap duration(n) [1m])`},
		{q: `sum_over_time({app="ra"} | unwrap duration_seconds(n) [1m])`},
		{q: `sum_over_time({app="ra"} | unwrap bytes(n) [1m])`},
		{q: `avg_over_time({app="ra"} | unwrap n [1m])`},
		{q: `avg_over_time({app="ra"} | unwrap bytes(n) [3m])`},
		{q: `min_over_time({app="ra"} | unwrap duration(n) [45s])`},
		{q: `max_over_time({app="ra"} | unwrap n [1500ms])`},
		{q: `max_over_time({app="ra"} | unwrap bytes(n) [5m])`},
		// the label the unwrap names: dotted, underscored, a name the profile translates, one the proxy derives
		{q: `sum_over_time({app="ra"} | unwrap http.bytes [1m])`},
		{q: `sum_over_time({app="ra"} | unwrap k8s_pod_cpu [1m])`},
		{q: `max_over_time({app="ra"} | unwrap level [1m])`},
		{q: `max_over_time({app="ra"} | unwrap service_name [1m])`},
		// filters before the unwrap: lines, a stream label, a line filter on the value's name
		{q: `sum_over_time({app="ra"} |= "x" | unwrap n [1m])`},
		{q: `sum_over_time({app="ra"} | env="prod" | unwrap n [1m])`},
		{q: `sum_over_time({app="ra"} |= "n" != "bad" |~ "a.+" | unwrap n [1m])`},
		{q: `sum_over_time({app="ra"} | status >= 500 | unwrap n [1m])`},
		{q: `sum_over_time({app="ra"} | env=~"p.*" | env!="pp" | unwrap n [1m])`},
		{q: `sum_over_time({app="ra"} | detected_level="error" | unwrap n [1m])`, delegated: "RangeAgg LogSelect level filter unwrap"},
		// a stage after the unwrap, a parser, an offset
		{q: `sum_over_time({app="ra"} | unwrap n | n > 5 [1m])`, delegated: "RangeAgg route unwrap filter"},
		{q: `sum_over_time({app="ra"} | unwrap n | __error__="" [1m])`, delegated: "RangeAgg route unwrap filter"},
		{q: `sum_over_time({app="ra"} | unwrap n | env="prod" [1m])`, delegated: "RangeAgg route unwrap filter"},
		{q: `sum_over_time({app="ra"} | unwrap duration(n) | __error__!="" [1m])`, delegated: "RangeAgg route unwrap filter"},
		{q: `sum_over_time({app="ra"} | logfmt | unwrap n [1m])`, delegated: "RangeAgg route unwrap pipeline"},
		{q: `sum_over_time({app="ra"} | unwrap n [1m] offset 5m)`, delegated: "RangeAgg route offset"},
		// the groupings: plain labels, none, several, dotted, translated; derived labels stay delegated
		{q: `max_over_time({app="ra"} | unwrap n [1m]) by (pod)`},
		{q: `avg_over_time({app="ra"} | unwrap n [1m]) by (pod, zone)`},
		{q: `avg_over_time({app="ra"} | unwrap n [7m]) by ()`},
		{q: `max_over_time({app="ra"} | unwrap n [30s]) by (k8s.pod.name)`},
		{q: `min_over_time({app="ra"} | unwrap bytes(n) [1m]) by (app)`},
		{q: `min_over_time({app="ra"} | unwrap duration(n) [1m]) by (pod_name)`},
		{q: `avg_over_time({app="ra"} | unwrap n [1m]) by (level)`, delegated: "RangeAgg route unwrap grouping"},
		{q: `min_over_time({app="ra"} | unwrap n [1m]) by (detected_level)`, delegated: "RangeAgg route unwrap grouping"},
		{q: `max_over_time({app="ra"} | unwrap n [1m]) by (service_name, pod)`, delegated: "RangeAgg route unwrap grouping"},
		// the functions read another way stay delegated by name
		{q: `quantile_over_time(0.5, {app="ra"} | unwrap n [1m])`, delegated: "RangeAgg route unwrap quantile"},
		{q: `stddev_over_time({app="ra"} | unwrap n [1m])`, delegated: "RangeAgg route unwrap stddev"},
		{q: `first_over_time({app="ra"} | unwrap n [1m])`, delegated: "RangeAgg route unwrap first"},
		{q: `last_over_time({app="ra"} | unwrap n [1m]) by (pod)`, delegated: "RangeAgg route unwrap last"},
		{q: `rate_counter({app="ra"} | unwrap n [1m])`, delegated: "RangeAgg route unwrap rate_counter"},
		{q: `rate({app="ra"} | unwrap n [1m])`, delegated: "RangeAgg route unwrap rate"},
		{q: `sum by (pod) (absent_over_time({app="ra"}[1m]))`, delegated: "VectorAgg route absent operand"},
	} {
		t.Run(c.q, func(t *testing.T) {
			row := routeRowOf(t, c.q)
			sec, problems := runRouteParity(t, []routeRow{row}, nativeRoute)
			if sec.Mismatches != 0 {
				t.Fatalf("%+v\n%s", sec, joinFirstRoute(problems, 4))
			}
			switch {
			case c.delegated == "" && sec.Native != sec.Runs:
				t.Fatalf("routed %d of %d runs, delegated %v", sec.Native, sec.Runs, sec.DelegatedByConstruct)
			case c.delegated != "" && (sec.Native != 0 || sec.DelegatedByConstruct[c.delegated] != sec.Runs):
				t.Fatalf("want every run delegated as %q: %+v", c.delegated, sec.DelegatedByConstruct)
			}
		})
	}
}

// The vector aggregations over a function of an unwrapped label: each is held to the handlers' route on every request
// profile, or, where the rule delegates it, to the name of its delegation and to no mismatch. pushed says the extreme
// of the group is taken in VictoriaLogs on a sliding window (the handlers' group pushdown), not per series first.
//
// conformance: architecture-syntax-package-layering
func TestMetricRouteVectorUnwrapEdges(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, c := range []struct {
		q         string
		delegated string
		pushed    bool
	}{
		// every operator over every function, whole and sliding ranges, a range that does not divide the span
		{q: `sum(sum_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum(avg_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum(min_over_time({app="va"} | unwrap n [7m]))`},
		{q: `sum(max_over_time({app="va"} | unwrap n [90s]))`},
		{q: `avg(sum_over_time({app="va"} | unwrap n [1m]))`},
		{q: `avg(avg_over_time({app="va"} | unwrap n [7m]))`},
		{q: `count(max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `count by (pod) (min_over_time({app="va"} | unwrap n [90s]))`},
		{q: `min(sum_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max by (pod) (avg_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max(min_over_time({app="va"} | unwrap n [1m]))`},
		{q: `min by (pod) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum by (pod, zone) (sum_over_time({app="va"} | unwrap n [1m]))`},
		{q: `avg by (pod) (max_over_time({app="va"} | unwrap n [7m]))`},
		{q: `sum by () (sum_over_time({app="va"} | unwrap n [1500ms]))`},
		{q: `avg by () (min_over_time({app="va"} | unwrap n [1m]))`},
		// the sum of the lines of every stream, or of each stream's own window
		{q: `sum(sum_over_time({app="va"} | unwrap bytes(n) [1m]))`},
		{q: `sum(sum_over_time({app="va"} | unwrap n [7m])) without (pod)`},
		{q: `sum(avg_over_time({app="va"} | unwrap n [7m])) by ()`},
		// the group pushdown: the extreme of the same function, by labels the stats keep as written, or none
		{q: `max by (pod) (max_over_time({app="va"} | unwrap n [1m]))`, pushed: true},
		{q: `max by (pod, zone) (max_over_time({app="va"} | unwrap n [7m]))`, pushed: true},
		{q: `max(max_over_time({app="va"} | unwrap duration(n) [1m]))`, pushed: true},
		{q: `max by () (max_over_time({app="va"} | unwrap n [90s]))`, pushed: true},
		{q: `min by (pod) (min_over_time({app="va"} | unwrap bytes(n) [1m]))`, pushed: true},
		{q: `min(min_over_time({app="va"} | unwrap n [7m]))`, pushed: true},
		{q: `max(max_over_time({app="va"} |= "x" | env="b" | unwrap n [1m]))`, pushed: true},
		{q: `max by (k8s.pod.name) (max_over_time({app="va"} | unwrap n [1m]))`, pushed: true},
		{q: `max by (pod_name) (max_over_time({app="va"} | unwrap n [1m]))`, pushed: true},
		{q: `max by (service_name) (max_over_time({app="va"} | unwrap n [1m]))`, pushed: true},
		{q: `max by (app) (max_over_time({app="va"} | unwrap n [1m]))`, pushed: true},
		{q: `max(max_over_time({service_name="va"} | unwrap n [1m])) by (pod)`, pushed: true},
		// and what keeps the read per series: the level, a label repeated, the stream or the line, a without clause, a
		// pair that is not the extreme of the same function
		{q: `max by (level) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max by (detected_level, pod) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max by (pod, pod) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max by (pod, k8s.pod.name) (max_over_time({app="va"} | unwrap n [1m]))`, pushed: true},
		{q: `max by (_stream) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max without (pod) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `min by (pod) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max by (pod) (min_over_time({app="va"} | unwrap n [1m]))`},
		{q: `max by (pod) (avg_over_time({app="va"} | unwrap n [1m]))`},
		{q: `avg by (pod) (avg_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum by (pod) (sum_over_time({app="va"} | unwrap n [1m]))`},
		// a without clause: the proxy folds the rows of the stream, every window
		{q: `sum without (pod) (sum_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum without (pod, zone) (avg_over_time({app="va"} | unwrap n [7m]))`},
		{q: `avg without (pod) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `count without (pod) (min_over_time({app="va"} | unwrap n [90s]))`},
		{q: `min(max_over_time({app="va"} | unwrap n [1m])) without (pod)`},
		{q: `max without (app) (max_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum without (level) (sum_over_time({app="va"} | unwrap n [1m]))`},
		// the conversions, the unwrapped label's name, the filters before the stage
		{q: `sum by (pod) (sum_over_time({app="va"} | unwrap duration(n) [1m]))`},
		{q: `avg(avg_over_time({app="va"} | unwrap duration_seconds(n) [1m]))`},
		{q: `max by (pod) (max_over_time({app="va"} | unwrap bytes(http.bytes) [1m]))`, pushed: true},
		{q: `sum(sum_over_time({app="va"} | unwrap level [1m]))`},
		{q: `max by (pod) (max_over_time({app="va"} | unwrap service_name [1m]))`, pushed: true},
		{q: `sum by (pod) (sum_over_time({app="va"} |= "x" != "bad" |~ "a.+" | unwrap n [1m]))`},
		{q: `avg by (pod) (avg_over_time({app="va", env=~"p.*"} | zone="z" | unwrap n [1m]))`},
		{q: `sum by (pod) (sum_over_time({app="va"} | status >= 500 | unwrap n [1m]))`},
		{q: `sum by (pod) (sum_over_time({app="va"} |> "<_> x" | unwrap n [1m]))`},
		// grouped by a label the proxy derives
		{q: `sum by (service_name) (avg_over_time({app="va"} | unwrap n [1m]))`},
		{q: `avg by (level) (sum_over_time({app="va"} | unwrap n [1m]))`},
		{q: `sum by (detected_level) (sum_over_time({app="va"} | unwrap n [1m]))`},
		// delegated by name: a parser, keep or drop ahead of the stage, a filter after it, an offset, a nest, a range
		// grouping, a level filter, a range with no bucket grid
		{q: `sum(sum_over_time({app="va"} | json | unwrap n [1m]))`, delegated: "VectorAgg route unwrap pipeline"},
		{q: `max by (pod) (max_over_time({app="va"} | logfmt | unwrap n [1m]))`, delegated: "VectorAgg route unwrap pipeline"},
		{q: `sum(sum_over_time({app="va"} | keep app, n | unwrap n [1m]))`, delegated: "VectorAgg route unwrap pipeline"},
		{q: `avg(avg_over_time({app="va"} | drop pod | unwrap n [1m]))`, delegated: "VectorAgg route unwrap pipeline"},
		{q: `sum(sum_over_time({app="va"} | unwrap n | n > 5 [1m]))`, delegated: "VectorAgg route unwrap filter"},
		{q: `max(max_over_time({app="va"} | unwrap n | __error__="" [1m]))`, delegated: "VectorAgg route unwrap filter"},
		{q: `sum(sum_over_time({app="va"} | unwrap n [1m] offset 5m))`, delegated: "VectorAgg route offset"},
		{q: `sum(sum by (pod) (sum_over_time({app="va"} | unwrap n [1m])))`, delegated: "VectorAgg nested unwrap"},
		{q: `max by (pod) (max_over_time({app="va"} | unwrap n [1m]) by (zone))`, delegated: "VectorAgg grouping"},
		{q: `sum(sum_over_time({app="va"} | detected_level="error" | unwrap n [1m]))`, delegated: "VectorAgg LogSelect level filter unwrap"},
		{q: `sum by (_msg) (sum_over_time({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap grouping"},
		{q: `min by (pod, _msg) (min_over_time({app="va"} | unwrap n [7m]))`, delegated: "VectorAgg route unwrap grouping"},
		{q: `max by (_stream, _msg) (avg_over_time({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap grouping"},
		// the functions read another way, and absent_over_time
		{q: `sum(quantile_over_time(0.5, {app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap quantile"},
		{q: `max(first_over_time({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap first"},
		{q: `max(last_over_time({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap last"},
		{q: `sum(rate({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap rate"},
		{q: `sum(rate_counter({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route unwrap rate_counter"},
		{q: `sum(absent_over_time({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route absent operand"},
		{q: `stddev(sum_over_time({app="va"} | unwrap n [1m]))`, delegated: "VectorAgg route evaluated level"},
	} {
		t.Run(c.q, func(t *testing.T) {
			row := routeRowOf(t, c.q)
			sec, problems := runRouteParity(t, []routeRow{row}, nativeRoute)
			if sec.Mismatches != 0 {
				t.Fatalf("%+v\n%s", sec, joinFirstRoute(problems, 4))
			}
			switch {
			case c.delegated == "" && sec.Native != sec.Runs:
				t.Fatalf("routed %d of %d runs, delegated %v", sec.Native, sec.Runs, sec.DelegatedByConstruct)
			case c.delegated != "" && (sec.Native != 0 || sec.DelegatedByConstruct[c.delegated] != sec.Runs):
				t.Fatalf("want every run delegated as %q: %+v", c.delegated, sec.DelegatedByConstruct)
			}
			if c.delegated != "" {
				return
			}
			// The group pushdown is the sliding windows' (a step under the range): the decision is pinned there.
			for _, rp := range routeProfiles() {
				req := lower.RouteRequest{Instant: rp.instant}
				if !rp.instant {
					req.Step, req.Span = rp.step(row.rangeDur), rp.window(row.rangeDur)
				}
				route, err := lower.Route(row.plan, routeLoweringProfile(), req)
				if err != nil {
					t.Fatalf("%s: %v", rp.name, err)
				}
				if want := c.pushed && route.Window == lower.WindowBuckets; route.GroupPushed != want {
					t.Errorf("%s: pushed %v on window %d, want %v", rp.name, route.GroupPushed, route.Window, want)
				}
			}
		})
	}
}
