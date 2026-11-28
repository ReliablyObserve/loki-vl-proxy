package proxy

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"
)

// The read grid of a range request: what the window evaluator read for it, so the error layer's log line says which
// step and bucket grid made a read pass its bound (issue #1795). The client text stays Loki's; the operator sees that
// the step, not the range, sized the read.

type readGridNoteKey struct{}

// readGridNote is the bucket grid a request's window evaluator read: its route (the stats buckets of
// gcd(step, window), or the recurrence's step-sized window grids), the request's step and window, the bucket of the
// reads and how many of them one series spans.
type readGridNote struct {
	mu                   sync.Mutex
	set                  bool
	route                string
	step, window, bucket time.Duration
	buckets              int64
}

// withReadGridNote gives the request a note its window evaluator fills.
func withReadGridNote(ctx context.Context) context.Context {
	return context.WithValue(ctx, readGridNoteKey{}, &readGridNote{})
}

// noteReadGrid records the request's bucket grid; a later note (the route the evaluator went on to) replaces it.
func noteReadGrid(ctx context.Context, route string, step, window, bucket time.Duration, buckets int64) {
	n, _ := ctx.Value(readGridNoteKey{}).(*readGridNote)
	if n == nil {
		return
	}
	n.mu.Lock()
	n.set, n.route, n.step, n.window, n.bucket, n.buckets = true, route, step, window, bucket, buckets
	n.mu.Unlock()
}

// readRouteStepIndependent names a failed read whose cost a coarser step does not lower (one step's window, the
// series of a window): the step layer does not retry it.
const readRouteStepIndependent = "step-independent"

// noteReadRoute names the route the request's evaluator went on to over the grid it noted (raw: the raw rows of the
// window, whose cost the step does not change).
func noteReadRoute(ctx context.Context, route string) {
	n, _ := ctx.Value(readGridNoteKey{}).(*readGridNote)
	if n == nil {
		return
	}
	n.mu.Lock()
	if n.set {
		n.route = route
	}
	n.mu.Unlock()
}

// readGridBuckets is the buckets of one series over [start, end] and the window before it.
func readGridBuckets(start, end time.Time, window, bucket time.Duration) int64 {
	if bucket <= 0 {
		return 0
	}
	return int64((end.Sub(start) + window) / bucket)
}

// readGridAttrs appends the request's bucket grid to a log line's attributes, when its evaluator chose one.
func readGridAttrs(ctx context.Context, attrs []slog.Attr) []slog.Attr {
	if ctx == nil {
		return attrs
	}
	n, _ := ctx.Value(readGridNoteKey{}).(*readGridNote)
	if n == nil {
		return attrs
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.set {
		return attrs
	}
	return append(attrs, slog.String("read_route", n.route), slog.Duration("query_step", n.step), slog.Duration("range_window", n.window),
		slog.Duration("read_bucket", n.bucket), slog.Int64("read_buckets_per_series", n.buckets))
}

// readGrid is a copy of a request's read grid note, kept with a failure the step layer may retry (step_adapt.go).
type readGrid struct {
	route                string
	step, window, bucket time.Duration
	buckets              int64
}

// readGridOf is the read grid the request's window evaluator noted, if it noted one.
func readGridOf(ctx context.Context) (readGrid, bool) {
	if ctx == nil {
		return readGrid{}, false
	}
	n, _ := ctx.Value(readGridNoteKey{}).(*readGridNote)
	if n == nil {
		return readGrid{}, false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.set {
		return readGrid{}, false
	}
	return readGrid{route: n.route, step: n.step, window: n.window, bucket: n.bucket, buckets: n.buckets}, true
}

// The effective step of a range metric request whose read passed a proxy-internal bound (step_adapt.go).
//
// The cost of a read is its buckets per series. The bucket reads (stats_query_range and the listed chunks) read
// buckets of gcd(step, window) over the window before the range and the range, (range + window) / gcd; a window
// shorter than the step reads one bucket per step. Past heavyQueryBucketThreshold buckets the evaluators read raw
// rows instead (range_metric_compat.go), whose cost does not fall with the step: the step that brings every window of
// the query back under the bound is the one that changes the read. A read that failed within the bound (a bucket
// read or the window grids of a recurrence over the bytes the proxy buffers) needs fewer buckets of its own kind:
// at most its buckets times the budget share (-range-metric-step-budget-bytes of -backend-max-buffered-response-bytes,
// one half by default), so the next read holds the budget with the margin the estimate needs.
//
// The rule (owner 2026-11-28, "close to loki": as close to Loki's answer as the limits allow, its point density
// included):
//   - the effective step is the smallest whole multiple k*step of the requested step that fits, when it is at most
//     stepMultipleDensity times the smallest step of any whole number of milliseconds that fits: its times are times
//     of Loki's answer at the requested step and its values Loki's values at them;
//   - otherwise (a requested step that shares no large factor with the window, 53 ms over [1h]: the smallest
//     multiple that fits is 19.08 s, 5 points over 80 s, where 360 ms fits with 223) it is that smallest step: the
//     answer is Loki's answer to the same query at that step.
//
// Times are anchored as Loki anchors a request at the effective step: at its multiples inside the requested range, as
// Loki's step alignment (align_queries_with_step) does and as its interval splitting (split_queries_by_interval, on by
// default) floors the start to the step. Each value is Loki's at its time, because the range window of every range
// aggregation stays what the query says.

// stepMultipleDensity is how many times coarser than the finest fitting step the smallest fitting multiple of the
// requested step may be and still be chosen.
const stepMultipleDensity = 2

// stepCandidateInput is what the choice of an effective step reads.
type stepCandidateInput struct {
	// step is the requested step (whole milliseconds); prev the step the failed read used (0: the request as sent).
	step, prev time.Duration
	// startNs and endNs are the requested bounds; onGrid evaluates the effective request at multiples of its step.
	startNs, endNs int64
	onGrid         bool
	// windows are the range windows of the query's range aggregations.
	windows []time.Duration
	// floor and ceiling bound the effective step (0: none).
	floor, ceiling time.Duration
	// maxBuckets is the bucket bound of a read (at most heavyQueryBucketThreshold).
	maxBuckets int64
	// failed are the read grids noted by the failed reads, and share the budget share of a read that failed within
	// its bound (0 < share < 1).
	failed []readGrid
	share  float64
	// halve: the failure counted the answer (its samples or bytes), not a read, so a step that holds every window
	// within the bound still fails it with all its points: the next step holds at most share of them.
	halve bool
}

// effectiveBounds are the bounds of the request at the effective step eff.
func effectiveBounds(in stepCandidateInput, eff time.Duration) (startNs, endNs int64, ok bool) {
	e := int64(eff)
	if e <= 0 || in.endNs < in.startNs {
		return 0, 0, false
	}
	if in.onGrid {
		startNs = ceilDiv(in.startNs, e) * e
		endNs = floorDiv(in.endNs, e) * e
	} else {
		startNs = in.startNs
		endNs = in.startNs + (in.endNs-in.startNs)/e*e
	}
	// Two points at least: a range shorter than its step is a query-split residual to Logs Drilldown
	// (isQuerySplitResidual), and one point says nothing a coarser grid could not.
	return startNs, endNs, endNs-startNs >= e
}

func ceilDiv(a, b int64) int64 { return -floorDiv(-a, b) }

// bucketsAt is the buckets per series of a bucket read of one window at the effective step eff.
func bucketsAt(in stepCandidateInput, eff, window time.Duration) int64 {
	startNs, endNs, ok := effectiveBounds(in, eff)
	if !ok {
		return 0
	}
	span := time.Duration(endNs - startNs)
	if window < eff {
		return int64(span/eff) + 1
	}
	bucket, ok := slidingStatsBucket(eff, window)
	if !ok {
		return math.MaxInt64
	}
	return int64((span + window) / bucket)
}

// pointsAt is the points per series at the effective step eff.
func pointsAt(in stepCandidateInput, eff time.Duration) int64 {
	startNs, endNs, ok := effectiveBounds(in, eff)
	if !ok {
		return 0
	}
	return (endNs-startNs)/int64(eff) + 1
}

// gridBucketsAt is the buckets per series of the read a failed read noted, at the effective step eff: the window
// grids of a recurrence read one bucket per step, the bucket reads the gcd grid.
func gridBucketsAt(in stepCandidateInput, eff time.Duration, g readGrid) int64 {
	if g.route == "window-grids" {
		return pointsAt(in, eff)
	}
	return bucketsAt(in, eff, g.window)
}

// effectiveStep is the step the request is served at (the rule above). ok is false when no step within the floor,
// the ceiling and the range fits, or none is known to change the failed read.
func effectiveStep(in stepCandidateInput) (time.Duration, bool) {
	if in.step < time.Millisecond || in.step%time.Millisecond != 0 || in.endNs <= in.startNs || len(in.windows) == 0 {
		return 0, false
	}
	maxBuckets := in.maxBuckets
	if maxBuckets <= 0 || maxBuckets > heavyQueryBucketThreshold {
		maxBuckets = heavyQueryBucketThreshold
	}
	share := in.share
	if share <= 0 || share >= 1 {
		share = 0.5
	}
	// The noted reads that failed within the bound, with the buckets the next one may hold.
	type shrink struct {
		grid readGrid
		most int64
	}
	var shrinks []shrink
	for _, g := range in.failed {
		if g.route == readRouteStepIndependent {
			return 0, false
		}
		if g.route == "raw" {
			// Raw rows read within the bucket bound were read for another reason than the step (a counter
			// proof, a parser error check): no coarser step changes them. Past the bound, the step that brings the
			// windows back within it moves the read to the buckets.
			if g.buckets <= heavyQueryBucketThreshold {
				return 0, false
			}
			continue
		}
		if g.buckets > 0 && g.buckets <= heavyQueryBucketThreshold {
			shrinks = append(shrinks, shrink{g, int64(math.Ceil(float64(g.buckets) * share))})
		}
	}
	prev := in.prev
	if prev < in.step {
		prev = in.step
	}
	fitsAt := func(eff time.Duration) bool {
		for _, w := range in.windows {
			if b := bucketsAt(in, eff, w); b == 0 || b > maxBuckets {
				return false
			}
		}
		return true
	}
	bucketBound := len(shrinks) == 0 && !fitsAt(prev)
	halveFrom := int64(0)
	if len(shrinks) == 0 && !bucketBound {
		if !in.halve {
			// A read that failed with every window within the bound and no grid a coarser step shrinks (raw rows
			// read for another reason, a read the model does not see): no step is known to change it.
			return 0, false
		}
		halveFrom = int64(math.Ceil(float64(pointsAt(in, prev)) * share))
	}
	fits := func(eff time.Duration) bool {
		if in.floor > 0 && eff < in.floor || in.ceiling > 0 && eff > in.ceiling || !fitsAt(eff) {
			return false
		}
		if halveFrom > 0 && pointsAt(in, eff) > halveFrom {
			return false
		}
		for _, s := range shrinks {
			if b := gridBucketsAt(in, eff, s.grid); b == 0 || b > s.most {
				return false
			}
		}
		return true
	}
	span := time.Duration(in.endNs - in.startNs)
	// The multiples of the requested step, or of an earlier effective step off its grid.
	inc := in.step
	if prev%in.step != 0 {
		inc = prev
	}
	multiple := time.Duration(0)
	for eff := prev + inc; eff <= span; eff += inc {
		if fits(eff) {
			multiple = eff
			break
		}
	}
	if !bucketBound || prev != in.step {
		// The bucket bound alone decides how far apart Loki's grid and the finest fitting step are: a read that
		// shrinks, or a later retry, stays on the multiples.
		return multiple, multiple > 0
	}
	// The finest step of whole milliseconds that fits; no finer than the multiple bound allows is worth the scan.
	limit := span
	if multiple > 0 {
		limit = multiple
	}
	for eff := prev + time.Millisecond; eff < limit; eff += time.Millisecond {
		if fits(eff) {
			if multiple > 0 && multiple <= stepMultipleDensity*eff {
				return multiple, true
			}
			return eff, true
		}
	}
	return multiple, multiple > 0
}
