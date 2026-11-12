package lower

import (
	"slices"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// The route rule of a metric query: which VictoriaLogs endpoint answers a range or vector aggregation and how the
// windows (t-range, t] of its samples are read from VictoriaLogs' stats, as the query and query_range handlers
// choose it today from the query text and the request. The plan decides it from typed nodes and the request alone;
// internal/proxy TestPlanRouteParity holds the decision to the handlers on the plan corpus under every request
// profile. A shape whose route the rule does not decide yet answers a NotLoweredError named "<kind> route <what>",
// so its handler route stays delegated.

// RouteRequest is what the handlers' route reads from the request besides the query.
type RouteRequest struct {
	// Instant is a query request; the rest are query_range requests.
	Instant bool
	// Step is the evaluation step of a range request, and Span its end minus its start.
	Step, Span time.Duration
}

// Window is how VictoriaLogs' stats answer the windows of a range metric.
type Window uint8

const (
	// WindowInstant is one window ending at the evaluation time: one stats_query.
	WindowInstant Window = iota + 1
	// WindowTumbling is a range equal to the step: one stats_query_range bucket per window, the request start
	// moved back one range and every bucket relabelled to the evaluation time that ends it.
	WindowTumbling
	// WindowBuckets is a range longer than the step: stats_query_range buckets of gcd(step, range) from one range
	// before the start, each window summed from the buckets it covers.
	WindowBuckets
	// WindowPhase is a range shorter than the step: the lines outside every window are dropped before the stats
	// (a filter on the line's phase in the step), one bucket per step from one step before the start.
	WindowPhase
)

// heavyRouteBuckets is the most buckets a stats_query_range route reads (Loki's 11,000 points per series): with
// more, or with no bucket grid at all, the handlers evaluate the windows another way (raw rows, two bucket grids).
const heavyRouteBuckets = 11_000

// MetricRoute is the route of a range or vector aggregation.
type MetricRoute struct {
	Window Window
	// Op and Range are the range aggregation's.
	Op    logql.RangeOp
	Range time.Duration
	// Bucket is the stats_query_range step of a range request: the step for WindowTumbling and WindowPhase,
	// gcd(step, range) for WindowBuckets.
	Bucket time.Duration
	// Identity is a range aggregation at the root, or a without clause over stats that group by nothing: Loki keeps
	// every stream's series (the proxy folds them under a without), so VictoriaLogs groups by the stream, its level
	// and the detected_level Loki derives.
	Identity bool
	// LevelProbe is a request holding the derived detected_level chain: on a range request the handlers first
	// probe which level fields the window holds, so the chain reads only those.
	LevelProbe bool
	// Fold is how the handlers fold the series of a without clause.
	Fold WithoutFold
	// Ungrouped is a lowered query whose last stats pipe groups by nothing: VictoriaLogs answers it with one row
	// even over no input, so the instant route counts the input rows beside it (Loki answers no series then).
	Ungrouped bool
	// SumAll is an ungrouped sum right over the range aggregation: its series add up whatever their identity, so
	// a windowed route reads the lines of every stream as one series.
	SumAll bool
	// StepBuckets is a range shorter than the step answered from VictoriaLogs' own step buckets (an unwrap function
	// with its window phase filter): the request start moves one nanosecond and the answer is trimmed and relabelled
	// like a tumbling window's. A count or a rate with such a range is evaluated by the proxy (WindowPhase alone).
	StepBuckets bool
	// Counted is a range aggregation over an unwrapped label: the VictoriaLogs client flags the rows whose value the
	// conversion rejects, so every stats pipe also groups by the flag (the proxy takes that group out of the answer
	// and reads Loki's conversion error from it).
	Counted bool
	// Text is the lowered stats query the route sends (the plan's text, before the route's own pipes).
	Text string
}

// routeOps are the vector aggregations whose stats pipes VictoriaLogs evaluates on every route: the levels the
// proxy evaluates itself (stddev, stdvar, the rankings) have routes of their own.
var routeOps = map[logql.VectorOp]bool{
	logql.VectorSum: true, logql.VectorAvg: true, logql.VectorMin: true, logql.VectorMax: true, logql.VectorCount: true,
}

// routeRangeOps are the range functions the stats routes answer from line counts and byte sums.
var routeRangeOps = map[logql.RangeOp]bool{
	logql.RangeCountOverTime: true, logql.RangeRate: true, logql.RangeBytesOverTime: true, logql.RangeBytesRate: true,
}

// routeUnwrapOps are the functions over an unwrapped label whose stats the rule decides: the partial aggregates
// VictoriaLogs keeps per bucket (the sum, the extremes, the average from a sum and a count) that merge into a longer
// window. The others (quantile, stddev, stdvar, first, last, rate_counter) are read another way and stay delegated.
var routeUnwrapOps = map[logql.RangeOp]bool{
	logql.RangeSumOverTime: true, logql.RangeAvgOverTime: true, logql.RangeMinOverTime: true, logql.RangeMaxOverTime: true,
}

// Route decides the route of a plan whose root is a range aggregation, or vector aggregations over one, for req.
// A plan outside the rule keeps the lowering's own delegation when the lowering does not write it.
func Route(p *queryplan.Plan, prof Profile, req RouteRequest) (MetricRoute, error) {
	// The kind names a delegation; Construct formats it, so the two kinds the rule routes are spelled out.
	var kind string
	switch p.Root.(type) {
	case *queryplan.RangeAgg:
		kind = "RangeAgg"
	case *queryplan.VectorAgg:
		kind = "VectorAgg"
	default:
		kind = Construct(p.Root)
	}
	rng, err := routeRange(p.Root, kind)
	if err != nil {
		if _, lerr := Plan(p, prof); lerr != nil {
			return MetricRoute{}, lerr
		}
		return MetricRoute{}, err
	}
	r := MetricRoute{Op: rng.Op, Range: rng.Range}
	// A without clause writes no grouping of its own: a query whose stats group by nothing keeps the stream's
	// identity, as a range aggregation at the root does, for the proxy to fold the series.
	unkeyed := false
	var built builtVector
	switch v := p.Root.(type) {
	case *queryplan.RangeAgg:
		op, err := rangeAgg(v, prof)
		if err != nil {
			return MetricRoute{}, err
		}
		q, ok := op.(physical.VLQuery)
		if !ok {
			return MetricRoute{}, routeError(kind, "operator")
		}
		r.Text = q.Text
		if v.Op == logql.RangeAbsentOverTime {
			// No series to keep: one ungrouped count says whether the window holds a line.
			r.Ungrouped = true
		} else {
			// The series keep their stream's identity, whose grouping holds the derived detected_level.
			r.Identity, r.LevelProbe = v.By == nil, v.By == nil
			// A grouping by no label leaves the stats ungrouped.
			r.Ungrouped = v.By != nil && len(v.By.Labels) == 0
		}
		r.Counted = routeUnwrapOps[v.Op]
	case *queryplan.VectorAgg:
		b, err := routeVector(v, rng, prof)
		if err != nil {
			return MetricRoute{}, err
		}
		built = b
		chain, _ := b.derives()
		r.Text, r.LevelProbe = b.text(prof, b.stats), chain
		if last, ok := b.stats[len(b.stats)-1].(logsql.PipeStats); ok {
			r.Ungrouped = len(last.By) == 0
		}
		r.SumAll = v.Op == logql.VectorSum && (v.By == nil || len(v.By.Labels) == 0) && v.In == queryplan.Node(rng)
		unkeyed = !groupsByAny(b.stats)
	}
	r.Window, r.Bucket, err = routeWindow(kind, r.Range, req)
	if err != nil {
		return r, err
	}
	r.StepBuckets = r.Counted && r.Window == WindowPhase
	if v, ok := p.Root.(*queryplan.VectorAgg); ok {
		if r.Fold = routeFold(v, rng, r.Window, built, prof); r.Fold != FoldNone && unkeyed {
			r.Identity, r.LevelProbe, r.Ungrouped = true, true, false
		}
	}
	return r, nil
}

// routeVector is the lowering of the vector aggregations va over rng up to their stats (vectorAgg).
func routeVector(va *queryplan.VectorAgg, rng *queryplan.RangeAgg, prof Profile) (builtVector, error) {
	if _, nested := va.In.(*queryplan.VectorAgg); nested {
		return buildNestedVector(va, prof)
	}
	return buildVectorAgg(va, rng, rng.In, prof)
}

// routeWindow is how the route reads the windows of a range rng for req, and the stats_query_range step, or why the
// rule does not route them.
func routeWindow(kind string, rng time.Duration, req RouteRequest) (Window, time.Duration, error) {
	var w Window
	var bucket time.Duration
	switch {
	case req.Instant:
		return WindowInstant, 0, nil
	case req.Step <= 0:
		return 0, 0, routeError(kind, "step")
	case rng == req.Step:
		return WindowTumbling, req.Step, nil
	case rng < req.Step:
		w, bucket = WindowPhase, req.Step
	default:
		w, bucket = WindowBuckets, gcd(req.Step, rng)
	}
	if bucket < time.Millisecond || (req.Span+rng)/bucket > heavyRouteBuckets {
		return 0, 0, routeError(kind, "no bucket grid")
	}
	return w, bucket, nil
}

// routeRange is the range aggregation under the vector aggregations of n, or the reason the rule does not route n.
func routeRange(n queryplan.Node, kind string) (*queryplan.RangeAgg, error) {
	vector := false
	for {
		switch v := n.(type) {
		case *queryplan.VectorAgg:
			if !routeOps[v.Op] {
				return nil, routeError(kind, "evaluated level")
			}
			n, vector = v.In, true
			continue
		case *queryplan.RangeAgg:
			if err := routeFunction(v, kind, vector); err != nil {
				return nil, err
			}
			return v, nil
		}
		return nil, routeError(kind, "operand")
	}
}

// routeFunction is why the rule does not route the range aggregation v (under vector aggregations when vector), or
// nil: the function, its offset and grouping, and the stages of its log query.
func routeFunction(v *queryplan.RangeAgg, kind string, vector bool) error {
	// The vector aggregations over absent_over_time and the unwrap functions have routes of their own.
	if vector && v.Op == logql.RangeAbsentOverTime {
		return routeError(kind, "absent operand")
	}
	if vector && routeUnwrapOps[v.Op] {
		return routeError(kind, "unwrap operand")
	}
	unwrapped := slices.ContainsFunc(v.In.Stages, func(st queryplan.Stage) bool { _, ok := st.(*queryplan.Unwrap); return ok })
	switch {
	case unwrapped && !routeUnwrapOps[v.Op]:
		return routeError(kind, unwrapName(v.Op))
	case !unwrapped && !routeRangeOps[v.Op] && v.Op != logql.RangeAbsentOverTime:
		return routeError(kind, "function")
	case v.HasOffset:
		return routeError(kind, "offset")
	case v.By != nil && !unwrapped:
		return routeError(kind, "grouping")
	case v.By != nil && !routeGroups(v.By):
		return routeError(kind, "unwrap grouping")
	}
	afterUnwrap := false
	for _, st := range v.In.Stages {
		switch s := st.(type) {
		case *queryplan.LineFilter:
		case *queryplan.Unwrap:
			afterUnwrap = true
			continue
		case *queryplan.LabelFilter:
			// A filter on detected_level writes the level chain the handlers probe the window for first
			// (DetectedLevelProbeBase over its prefilter): not decided by the rule yet.
			if isLevelLeaf(s.Expr) {
				return routeError(kind, "level filter")
			}
		default:
			// A parser ahead of the unwrap derives the value (and the series) another way.
			if unwrapped {
				return routeError(kind, "unwrap pipeline")
			}
			return routeError(kind, "pipeline")
		}
		// The rows a filter after the unwrap drops may be the ones whose value the conversion rejects: the
		// handlers then read the conversion error from the surviving rows (not decided by the rule yet).
		if afterUnwrap {
			return routeError(kind, "unwrap filter")
		}
	}
	return nil
}

// unwrapName is the delegation of a function over an unwrapped label the rule does not decide: "unwrap quantile",
// "unwrap rate", "unwrap absent".
func unwrapName(op logql.RangeOp) string {
	return "unwrap " + strings.TrimSuffix(string(op), "_over_time")
}

// routeGroups reports whether the grouping of an unwrap function is labels the stats group by as they stand: not
// a `without`, and no label the proxy derives per row (the series would then hold the derived level chain).
func routeGroups(g *logql.Grouping) bool {
	return !g.Without && !slices.ContainsFunc(g.Labels, func(name string) bool { return !groupLabel(name) })
}

func routeError(kind, what string) error { return &NotLoweredError{Construct: kind + " route " + what} }

func gcd(a, b time.Duration) time.Duration {
	for b > 0 {
		a, b = b, a%b
	}
	return a
}
