package proxy

import (
	"context"
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
		form := url.Values{
			"query":   {routeQueryShape(routeStatsQuery(route, "", time.Time{}, 0))},
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
			steps = append(steps, "operation trim_translate_stats_qr "+routeTrimOutcome(p, route, req.Span))
		}
	case lower.WindowBuckets:
		fetchStart = start.Add(-route.Range)
		setSlidingStatsRangeParams(params, fetchStart, end, route.Bucket)
	case lower.WindowPhase:
		fetchStart = start.Add(-route.Bucket)
		setSlidingStatsRangeParams(params, fetchStart, end, route.Bucket)
		phase = "phase"
	}
	params.Set("query", routeQueryShape(routeStatsQuery(route, phase, start, req.Step)))
	out.Calls = append(out.Calls, "/select/logsql/stats_query_range?"+params.Encode())
	if route.LevelProbe {
		probe := url.Values{"query": {routeShapeFilters(route.Text) + routeLevelProbeShape}, "start": {routeTime(fetchStart)}, "end": {routeTime(end.Add(2))}, "timeout": {routeTimeout}}
		out.Calls = append(out.Calls, "/select/logsql/query?"+probe.Encode())
	}
	out.Calls, out.Steps = uniqueSorted(out.Calls), uniqueSorted(steps)
	return out, nil
}

// routeStatsQuery is the stats request of route, as far as its shape goes: the lowered text with the stream
// identity grouping of a range aggregation at the root, and on a windowed route the window pipes (the phase filter)
// and the bucket stats of the range function, grouped like the lowered query's first stats pipe.
func routeStatsQuery(route lower.MetricRoute, phase string, start time.Time, step time.Duration) string {
	text := route.Text
	if route.Identity && !strings.Contains(text, "| stats by (") {
		// The grouping text is the handlers' builder; that the series keep their identity is the route's.
		text = addStatsByStreamClause(text, nil)
	}
	switch route.Window {
	case lower.WindowInstant:
		if route.Ungrouped {
			text += ", count() as " + emptyInputGuardAlias
		}
		return text
	case lower.WindowTumbling:
		return text
	}
	at := strings.Index(text, "| stats ")
	by := ""
	if rest := text[at+len("| stats "):]; strings.HasPrefix(rest, "by (") && !route.SumAll {
		by = rest[:strings.Index(rest, ")")+2]
	}
	agg := "count() as c"
	if route.Op == logql.RangeBytesOverTime || route.Op == logql.RangeBytesRate {
		agg = "sum_len(_msg) as c, count() as __sample_count"
	}
	base := strings.TrimRight(text[:at], " ")
	if phase != "" {
		// Where the filter goes is the handlers' text placement (withWindowPhaseFilter); that it goes is the route's.
		base = withWindowPhaseFilter(context.Background(), base, start, step, route.Range)
	}
	return base + " | stats " + by + agg
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
// answers its one sample.
func routeTrimOutcome(p *queryplan.Plan, route lower.MetricRoute, span time.Duration) string {
	switch {
	case route.Range > span:
		return "ok"
	case routeLabelsOutcome(p) == "noop":
		return "noop"
	}
	return "folded"
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
