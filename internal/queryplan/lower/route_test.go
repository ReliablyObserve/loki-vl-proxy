package lower

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// routeTestEnd is the end of the requests the offset rows move: in the past, with room both ways.
var routeTestEnd = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// The earliest time an offset may move a bound to, and the latest for a 5m range on an instant request (a second and
// the range stay below int64 nanoseconds), as times.
var (
	// routeBoundsRangeCeiling is the latest end a range request of step 5m over a 5m range may have after an offset of
	// -1h: 15 minutes (the request time bounds), the step and the range short of int64 nanoseconds, less the hour the offset adds.
	routeBoundsRangeCeiling = time.Unix(0, math.MaxInt64-int64(15*time.Minute)-int64(10*time.Minute)-int64(time.Hour))
	routeBoundsFloor        = time.Unix(0, 1e18)
	routeBoundsCeiling      = time.Unix(0, math.MaxInt64-int64(15*time.Minute)-int64(5*time.Minute))
)

// The route rule decides, from the plan and the request, how the handlers read a range metric's windows from
// VictoriaLogs: one stats_query for an instant request, one bucket per window when the range equals the step, the
// buckets of gcd(step, range) when it is longer, a phase filter and a bucket per step when it is shorter, and no
// stats grid at all past Loki's 11,000 points. Route parity (internal/proxy TestPlanRouteParity) holds the rule
// to the handlers on the corpus; this table pins the decisions and the delegations.
//
// conformance: architecture-syntax-package-layering
func TestRouteDecidesTheWindows(t *testing.T) {
	hour := time.Hour
	for _, c := range []struct {
		q         string
		req       RouteRequest
		window    Window
		bucket    time.Duration
		identity  bool
		probe     bool
		ungrouped bool
		sumAll    bool
		counted   bool
		stepBkts  bool
		pushed    bool
		fold      WithoutFold
		offset    time.Duration
		construct string
		textHolds string
		textLacks string
	}{
		{q: `rate({app="a"}[5m])`, req: RouteRequest{Instant: true}, window: WindowInstant, identity: true, probe: true},
		{q: `rate({app="a"}[5m])`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true},
		{q: `rate({app="a"}[5m])`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true},
		{q: `rate({app="a"}[5m])`, req: RouteRequest{Step: 2 * time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true},
		{q: `rate({app="a"}[5m])`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, identity: true, probe: true},
		{q: `bytes_over_time({app="a"} |= "x" [90s])`, req: RouteRequest{Step: 18 * time.Second, Span: hour}, window: WindowBuckets, bucket: 18 * time.Second, identity: true, probe: true},
		{q: `count_over_time({app="a"} | level="error" [1m])`, req: RouteRequest{Instant: true}, window: WindowInstant, identity: true, probe: true},
		// sum by a label: no stream identity and no level chain, so no level probe.
		{q: `sum by (app) (count_over_time({app="a"}[1m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowTumbling, bucket: time.Minute, textLacks: translate.RateDetectedLevelChain()},
		// grouped by detected_level: the chain derives it, and a range request probes the level fields first.
		{q: `sum by (detected_level) (count_over_time({app="a"}[1m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowTumbling, bucket: time.Minute, probe: true, textHolds: translate.RateDetectedLevelChain()},
		// an ungrouped sum right over the range adds every stream up; its last stats pipe groups by nothing.
		{q: `sum(rate({app="a"}[5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, probe: true, ungrouped: true, sumAll: true},
		{q: `sum by () (count_over_time({app="a"}[5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true, sumAll: true},
		{q: `max(count_over_time({app="a"}[5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, probe: true, ungrouped: true},
		{q: `sum(sum by (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true},
		{q: `sum by (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant},
		// a grouping the inner level does not hold leaves the last stats pipe without one.
		{q: `min by (zone) (avg by (pod) (rate({app="uw"} [1m])))`, req: RouteRequest{Instant: true}, window: WindowInstant, probe: true, ungrouped: true},
		// no bucket grid: a gcd under a millisecond, more buckets than Loki's points.
		{q: `rate({app="a"}[1001ms])`, req: RouteRequest{Step: time.Second, Span: hour}, construct: "RangeAgg route no bucket grid"},
		{q: `sum by (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: time.Second, Span: 24 * hour}, construct: "VectorAgg route no bucket grid"},
		{q: `count_over_time({app="a"}[1s])`, req: RouteRequest{Step: 10 * time.Second, Span: 200 * 24 * hour}, construct: "RangeAgg route no bucket grid"},
		{q: `count_over_time({app="a"}[1m])`, req: RouteRequest{Span: hour}, construct: "RangeAgg route step"},
		// a without clause: the stats group by nothing, so the series keep their stream's identity for the proxy to fold
		// (and the level chain they derive is probed first); a range equal to the step is folded by VictoriaLogs, a
		// sliding window of one sum over a count reads the cut buckets, anything else the proxy folds.
		{q: `sum without (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, identity: true, probe: true, fold: FoldRows},
		{q: `sum without (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldPushed},
		{q: `sum without (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, fold: FoldCut},
		{q: `sum without (pod) (bytes_over_time({app="a"}[1m]))`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, identity: true, probe: true, fold: FoldCut},
		// a rate, count, min, max and avg already group by the series: no identity added, no guard
		{q: `sum without (pod) (rate({app="a"}[5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, probe: true, fold: FoldRows},
		{q: `sum without (pod) (rate({app="a"}[5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, probe: true, fold: FoldPushed},
		{q: `sum without (pod) (rate({app="a"}[5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, probe: true, fold: FoldRows},
		{q: `avg without (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, probe: true, fold: FoldRows},
		{q: `max(count_over_time({app="a"}[5m])) without (pod)`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, probe: true, fold: FoldPushed},
		// levels above the without: by columns are pushed down, and the nest folds in the proxy on a sliding window
		{q: `sum by (zone) (sum without (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldPushed},
		{q: `sum by (zone) (sum without (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, fold: FoldRows},
		{q: `sum by (level) (sum without (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldRows},
		{q: `sum by (service_name) (sum without (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldRows},
		{q: `sum without (zone) (sum without (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldRows},
		// a without over a grouped level reads the grouped series: no identity to add
		{q: `sum without (zone) (sum by (pod) (count_over_time({app="a"}[5m])))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, fold: FoldRows},
		// labels the cut cannot name stay with the proxy: derived, or with an underscore
		{q: `sum without (level) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldRows},
		{q: `sum without (pod_name) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldRows},
		{q: `sum without (pod, service_name) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, fold: FoldRows},
		// a pattern filter ends the push (the line is read as _msg) and a label filter the cut
		{q: `sum without (pod) (count_over_time({app="a"} |> "<_> x" [5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldRows},
		{q: `sum without (pod) (count_over_time({app="a"} |> "<_> x" [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, fold: FoldCut},
		{q: `sum without (pod) (count_over_time({app="a"} | env="b" [5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, fold: FoldPushed},
		{q: `sum without (pod) (count_over_time({app="a"} | env="b" [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, fold: FoldRows},
		// absent_over_time keeps no series: one ungrouped count says whether the window holds a line, with no stream
		// identity and no level probe, on every window.
		{q: `absent_over_time({app="a"}[5m])`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true},
		{q: `absent_over_time({app="a"} |= "x" [5m])`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, ungrouped: true},
		{q: `absent_over_time({app="a"} | env="b" [5m])`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, ungrouped: true},
		{q: `absent_over_time({app="a"}[1m])`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, ungrouped: true},
		// a function over an unwrapped label keeps the stream's series like a count and counts the rows its conversion
		// rejects; a range up to the step is read from VictoriaLogs' own step buckets (a shorter one under a phase
		// filter), a longer one merges the partial aggregate of each bucket.
		{q: `sum_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, window: WindowInstant, identity: true, probe: true, counted: true},
		{q: `max_over_time({app="a"} | unwrap bytes(n) [5m])`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, counted: true},
		{q: `avg_over_time({app="a"} | unwrap duration(n) [5m])`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, counted: true},
		{q: `min_over_time({app="a"} |= "x" | env="b" | unwrap duration_seconds(n) [1m])`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, identity: true, probe: true, counted: true, stepBkts: true},
		// stddev merges a longer window from the mean, count and deviation of each bucket; a shorter one is read
		// from the step buckets under the phase filter, like the other partial aggregates
		{q: `stddev_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, window: WindowInstant, identity: true, probe: true, counted: true},
		{q: `stddev_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, counted: true},
		{q: `stddev_over_time({app="a"} | unwrap n [5m]) by (pod)`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, counted: true},
		{q: `stddev_over_time({app="a"} | unwrap n [1m])`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, identity: true, probe: true, counted: true, stepBkts: true},
		// a count over the same short range is not read from step buckets
		{q: `count_over_time({app="a"}[1m])`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, identity: true, probe: true},
		// a grouping by labels the stats keep as they are: no stream identity, no level chain, no probe
		{q: `avg_over_time({app="a"} | unwrap n [5m]) by (pod)`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, counted: true},
		{q: `max_over_time({app="a"} | unwrap n [5m]) by ()`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true, counted: true},
		{q: `max_over_time({app="a"} | unwrap n [1m]) by (pod, zone)`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, counted: true, stepBkts: true},
		// a vector aggregation over a function of an unwrapped label: the stats group by the clause's labels, or by the
		// series first (the stream and its level, so the chain is probed) for count, min, max and avg; the conversion
		// flag counts on every window.
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, counted: true},
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, counted: true},
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, counted: true},
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap bytes(n) [5m]))`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, counted: true, stepBkts: true},
		{q: `count by (pod) (min_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, probe: true, counted: true},
		{q: `avg by (pod, zone) (avg_over_time({app="a"} |= "x" | env="b" | unwrap duration(n) [5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, probe: true, counted: true},
		// an ungrouped aggregation: the last stats pipe groups by nothing, and the guard counts the input beside it
		{q: `avg(avg_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true, counted: true},
		{q: `sum by () (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true, counted: true},
		// the sum of the lines of every stream is the sum of their sums, and of nothing else: a sliding window of the sum
		// of a sum reads the lines as one series, the sum of an average, a minimum or a maximum keeps the series
		{q: `sum(sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, ungrouped: true, sumAll: true, counted: true},
		{q: `sum(sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, ungrouped: true, sumAll: true, counted: true},
		{q: `sum(avg_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, ungrouped: true, counted: true},
		{q: `sum by () (min_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, ungrouped: true, counted: true},
		// the extreme of the same function by labels (or none) on a sliding window is taken per group in VictoriaLogs:
		// no series identity; any other pair, a level, a repeated label or a without clause reads the series first
		{q: `max by (pod) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, probe: true, counted: true, pushed: true},
		{q: `min(min_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, ungrouped: true, counted: true, pushed: true},
		{q: `max by (pod) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, probe: true, counted: true},
		{q: `max by (pod) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, probe: true, counted: true},
		{q: `min by (pod) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, probe: true, counted: true},
		{q: `max by (level) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, probe: true, counted: true},
		{q: `max by (pod, pod) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, probe: true, counted: true},
		{q: `max(max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, ungrouped: true, counted: true, stepBkts: true},
		// a without clause: the stats keep a row per stream for the proxy to fold, on every window; the pushdown that cuts
		// the stream text reads the stats of counts and rates only
		{q: `sum without (pod) (sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, window: WindowInstant, counted: true, fold: FoldRows},
		{q: `max without (pod) (max_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour}, window: WindowTumbling, bucket: 5 * time.Minute, counted: true, fold: FoldRows},
		{q: `avg without (pod) (min_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: time.Minute, Span: hour}, window: WindowBuckets, bucket: time.Minute, counted: true, fold: FoldRows},
		{q: `sum without (pod) (avg_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Step: 15 * time.Minute, Span: hour}, window: WindowPhase, bucket: 15 * time.Minute, counted: true, stepBkts: true, fold: FoldRows},
		// an offset: the handlers strip the clause and read the windows that much earlier (later when negative), so the
		// route is the one without it on every window kind; the request's end bounds how far it may move them
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Instant: true, End: routeTestEnd}, window: WindowInstant, identity: true, probe: true, offset: hour},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Step: 5 * time.Minute, Span: hour, End: routeTestEnd}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, offset: hour},
		{q: `rate({app="a"}[5m] offset 7m)`, req: RouteRequest{Step: time.Minute, Span: hour, End: routeTestEnd}, window: WindowBuckets, bucket: time.Minute, identity: true, probe: true, offset: 7 * time.Minute},
		{q: `rate({app="a"}[5m] offset -1h)`, req: RouteRequest{Step: 15 * time.Minute, Span: hour, End: routeTestEnd}, window: WindowPhase, bucket: 15 * time.Minute, identity: true, probe: true, offset: -hour},
		{q: `count_over_time({app="a"}[5m] offset 1500ms)`, req: RouteRequest{Instant: true, End: routeTestEnd}, window: WindowInstant, identity: true, probe: true, offset: 1500 * time.Millisecond},
		{q: `sum_over_time({app="a"} | unwrap n [5m] offset 1h)`, req: RouteRequest{Instant: true, End: routeTestEnd}, window: WindowInstant, identity: true, probe: true, counted: true, offset: hour},
		{q: `max_over_time({app="a"} | unwrap n [1m] offset 1h) by (pod)`, req: RouteRequest{Step: 15 * time.Minute, Span: hour, End: routeTestEnd}, window: WindowPhase, bucket: 15 * time.Minute, counted: true, stepBkts: true, offset: hour},
		{q: `absent_over_time({app="a"}[5m] offset 1h)`, req: RouteRequest{Instant: true, End: routeTestEnd}, window: WindowInstant, ungrouped: true, offset: hour},
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n [5m] offset 1h))`, req: RouteRequest{Instant: true, End: routeTestEnd}, window: WindowInstant, counted: true, offset: hour},
		{q: `max by (pod) (max_over_time({app="a"} | unwrap n [5m] offset -1h))`, req: RouteRequest{Step: time.Minute, Span: hour, End: routeTestEnd}, window: WindowBuckets, bucket: time.Minute, probe: true, counted: true, pushed: true, offset: -hour},
		{q: `sum(count_over_time({app="a"}[5m] offset 1h))`, req: RouteRequest{Step: time.Minute, Span: hour, End: routeTestEnd}, window: WindowBuckets, bucket: time.Minute, ungrouped: true, sumAll: true, offset: hour},
		{q: `sum without (pod) (rate({app="a"}[5m] offset 1h))`, req: RouteRequest{Step: 5 * time.Minute, Span: hour, End: routeTestEnd}, window: WindowTumbling, bucket: 5 * time.Minute, probe: true, fold: FoldPushed, offset: hour},
		// a zero offset moves nothing, so no end is asked for
		{q: `rate({app="a"}[5m] offset 0s)`, req: RouteRequest{Instant: true}, window: WindowInstant, identity: true, probe: true},
		// an offset that leaves the times the handlers read back as written: no end, an end before 2001, a start moved to
		// before it (instant and range), an end moved past int64 nanoseconds (a second is left for the bounds' own moves)
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Instant: true}, construct: "RangeAgg route offset bounds"},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Instant: true, End: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}, construct: "RangeAgg route offset bounds"},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Instant: true, End: routeBoundsFloor.Add(time.Hour - time.Nanosecond)}, construct: "RangeAgg route offset bounds"},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Instant: true, End: routeBoundsFloor.Add(time.Hour)}, window: WindowInstant, identity: true, probe: true, offset: hour},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Step: 5 * time.Minute, Span: 30 * time.Minute, End: routeBoundsFloor.Add(90 * time.Minute)}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, offset: hour},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Step: 5 * time.Minute, Span: 30 * time.Minute, End: routeBoundsFloor.Add(90*time.Minute - time.Nanosecond)}, construct: "RangeAgg route offset bounds"},
		{q: `rate({app="a"}[5m] offset 1h)`, req: RouteRequest{Step: 5 * time.Minute, Span: -time.Minute, End: routeTestEnd}, construct: "RangeAgg route offset bounds"},
		{q: `rate({app="a"}[5m] offset -1h)`, req: RouteRequest{Instant: true, End: routeBoundsCeiling.Add(-time.Hour)}, window: WindowInstant, identity: true, probe: true, offset: -hour},
		{q: `rate({app="a"}[5m] offset -1h)`, req: RouteRequest{Instant: true, End: routeBoundsCeiling.Add(-time.Hour + time.Nanosecond)}, construct: "RangeAgg route offset bounds"},
		{q: `rate({app="a"}[5m] offset -1h)`, req: RouteRequest{Step: 5 * time.Minute, Span: time.Hour, End: routeBoundsRangeCeiling}, window: WindowTumbling, bucket: 5 * time.Minute, identity: true, probe: true, offset: -hour},
		{q: `rate({app="a"}[5m] offset -1h)`, req: RouteRequest{Step: 5 * time.Minute, Span: time.Hour, End: routeBoundsRangeCeiling.Add(time.Nanosecond)}, construct: "RangeAgg route offset bounds"},
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n [5m] offset 1h))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route offset bounds"},
		// the lowering's own delegation keeps its name under an offset
		{q: `rate({app="a"} | json [5m] offset 1h)`, req: RouteRequest{Instant: true, End: routeTestEnd}, construct: "RangeAgg route pipeline"},
		{q: `sum(rate({app="a"} | json [5m] offset 1h))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route pipeline"},
		// outside the rule: each named after what it holds.
		{q: `sum without (pod) (rate({app="a"} | json [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route pipeline"},
		{q: `stddev(rate({app="a"}[5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route evaluated level"},
		{q: `count_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg unwrap count_over_time"},
		{q: `rate({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap rate"},
		{q: `quantile_over_time(0.9, {app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap quantile"},
		{q: `stdvar_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap stdvar"},
		{q: `first_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap first"},
		{q: `last_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap last"},
		{q: `rate_counter({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap rate_counter"},
		{q: `absent_over_time({app="a"} | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap absent"},
		{q: `sum_over_time({app="a"} | unwrap n | n > 5 [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap filter"},
		{q: `sum_over_time({app="a"} | unwrap n | __error__="" [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap filter"},
		{q: `sum_over_time({app="a"} | json | unwrap n [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap pipeline"},
		{q: `avg_over_time({app="a"} | unwrap n [5m]) by (level)`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap grouping"},
		{q: `avg_over_time({app="a"} | unwrap n [5m]) by (detected_level, pod)`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap grouping"},
		{q: `avg_over_time({app="a"} | unwrap n [5m]) by (service_name)`, req: RouteRequest{Instant: true}, construct: "RangeAgg route unwrap grouping"},
		{q: `rate({app="a"}[5m]) by (pod)`, req: RouteRequest{Instant: true}, construct: "RangeAgg grouping"},
		{q: `sum_over_time({app="a"} | unwrap n [1001ms])`, req: RouteRequest{Step: time.Second, Span: hour}, construct: "RangeAgg route no bucket grid"},
		{q: `absent_over_time({app="a"}[1001ms])`, req: RouteRequest{Step: time.Second, Span: hour}, construct: "RangeAgg route no bucket grid"},
		{q: `sum by (pod) (absent_over_time({app="a"}[5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route absent operand"},
		{q: `sum(avg_over_time({app="a"} | json | unwrap n [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route unwrap pipeline"},
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n | n > 5 [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route unwrap filter"},
		{q: `sum(sum by (pod) (sum_over_time({app="a"} | unwrap n [5m])))`, req: RouteRequest{Instant: true}, construct: "VectorAgg nested unwrap"},
		{q: `sum(sum_over_time({app="a"} | unwrap n [1001ms]))`, req: RouteRequest{Step: time.Second, Span: hour}, construct: "VectorAgg route no bucket grid"},
		{q: `stddev(sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route evaluated level"},
		{q: `sum by (_msg) (sum_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route unwrap grouping"},
		{q: `sum(rate({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route unwrap rate"},
		{q: `max(first_over_time({app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route unwrap first"},
		{q: `sum(quantile_over_time(0.5, {app="a"} | unwrap n [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route unwrap quantile"},
		{q: `rate({app="a"} | json [5m])`, req: RouteRequest{Instant: true}, construct: "RangeAgg route pipeline"},
		{q: `sum(count_over_time({app="a"}[5m]) > 1)`, req: RouteRequest{Instant: true}, construct: "VectorAgg operand"},
		{q: `sum by (level) (rate({app="a"} | json | detected_level="x" [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg " + levelAfterStageConstruct},
		{q: `sum by (level) (rate({app="a"} | detected_level="x" [5m]))`, req: RouteRequest{Instant: true}, construct: "VectorAgg route level filter"},
		{q: `count_over_time({app="a"} |= "y" | detected_level!="info" [1m])`, req: RouteRequest{Step: time.Minute, Span: hour}, construct: "RangeAgg route level filter"},
	} {
		t.Run(c.q, func(t *testing.T) {
			r, err := Route(planOf(t, c.q), Profile{}, c.req)
			if c.construct != "" {
				var nl *NotLoweredError
				if !errors.As(err, &nl) || nl.Construct != c.construct {
					t.Fatalf("Route = %+v, %v; want a delegation named %q", r, err, c.construct)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.Fold != c.fold || r.Window != c.window || r.Bucket != c.bucket || r.Identity != c.identity || r.LevelProbe != c.probe || r.Ungrouped != c.ungrouped || r.SumAll != c.sumAll || r.Counted != c.counted || r.StepBuckets != c.stepBkts || r.GroupPushed != c.pushed || r.Offset != c.offset {
				t.Fatalf("Route = window %d bucket %v identity %v probe %v ungrouped %v sumAll %v counted %v stepBuckets %v pushed %v fold %d offset %v", r.Window, r.Bucket, r.Identity, r.LevelProbe, r.Ungrouped, r.SumAll, r.Counted, r.StepBuckets, r.GroupPushed, r.Fold, r.Offset)
			}
			if c.textHolds != "" && !strings.Contains(r.Text, c.textHolds) || c.textLacks != "" && strings.Contains(r.Text, c.textLacks) {
				t.Fatalf("text %q", r.Text)
			}
		})
	}
}

// The route's text is the lowering's: Route writes the query Plan writes for every corpus row it routes, so the
// route the handlers will read and the request they send come from one lowering.
//
// conformance: architecture-syntax-package-layering
func TestRouteTextIsTheLowering(t *testing.T) {
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		t.Fatal(err)
	}
	routed := 0
	for _, q := range qs {
		p := routePlan(q)
		if p == nil {
			continue
		}
		for _, req := range []RouteRequest{{Instant: true, End: routeTestEnd}, {Step: time.Minute, Span: time.Hour, End: routeTestEnd}} {
			r, err := Route(p, Profile{}, req)
			if err != nil {
				// A delegation of the lowering's keeps its name.
				if _, perr := Plan(p, Profile{}); perr != nil && err.Error() != perr.Error() {
					t.Fatalf("%q: Route %v, Plan %v", q, err, perr)
				}
				continue
			}
			routed++
			op, err := Plan(p, Profile{})
			if err != nil {
				t.Fatalf("%q: routed, but Plan: %v", q, err)
			}
			// A without clause is a fold of the proxy over the query's rows.
			if f, ok := op.(physical.Fold); ok {
				op = f.In
			}
			var text string
			switch v := op.(type) {
			case physical.VLQuery:
				text = v.Text
			case physical.VLStats:
				text = v.Text
			}
			if text != r.Text {
				t.Fatalf("%q:\n route %q\n plan  %q", q, r.Text, text)
			}
		}
	}
	if routed == 0 {
		t.Fatal("no corpus row routed")
	}
}

// FuzzRoute: the route rule never panics and either routes a plan, with a window that fits the request and a fold
// that fits the window, or names the delegation.
func FuzzRoute(f *testing.F) {
	f.Add(`sum by (app) (rate({app="a"} |= "x" [5m]))`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`count_over_time({app="a"}[90s])`, int64(18*time.Second), int64(24*time.Hour), int64(1767225600e9), false)
	f.Add(`max(sum by (pod) (bytes_over_time({app="a"}[1m])))`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`sum without (pod) (rate({app="a"}[5m]))`, int64(time.Millisecond), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`sum without (pod, zone) (count_over_time({app="a"} |= "x" [5m]))`, int64(5*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`sum by (zone) (avg without (pod) (bytes_over_time({app="a"} | env="b" [1m])))`, int64(time.Minute), int64(24*time.Hour), int64(1767225600e9), false)
	f.Add(`max(sum without (pod_name, level) (count_over_time({app="a"} |> "<_> x" [90s])))`, int64(30*time.Second), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`avg(sum(count_over_time({app="a"}[1m])) without (pod)) without (svc)`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`sum_over_time({app="a"} | unwrap bytes(n) [1m])`, int64(15*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`avg_over_time({app="a"} |= "x" | env="b" | unwrap duration(n) [90s]) by (pod)`, int64(time.Minute), int64(24*time.Hour), int64(1767225600e9), false)
	f.Add(`max_over_time({app="a"} | unwrap n [5m]) by ()`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`min_over_time({app="a"} | unwrap n | __error__="" [5m])`, int64(5*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`count_over_time({unwrap="unwrap"} |= "unwrap" [5m])`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`absent_over_time({app="a"} != "x" [90s])`, int64(30*time.Second), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`absent_over_time({app="a"}[5m] offset 1h)`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`sum(avg_over_time({app="a"} | unwrap n [5m]))`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`max by (pod) (max_over_time({app="a"} | unwrap n [5m]))`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`min(min_over_time({app="a"} |= "x" | unwrap duration(n) [90s]))`, int64(30*time.Second), int64(24*time.Hour), int64(1767225600e9), false)
	f.Add(`max by (pod, pod) (max_over_time({app="a"} | unwrap n [5m]))`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`max by (level) (max_over_time({app="a"} | unwrap n [5m]))`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`sum(sum_over_time({app="a"} | unwrap bytes(n) [1m]))`, int64(15*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`avg without (pod) (min_over_time({app="a"} | unwrap n [5m]))`, int64(5*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`count by (pod) (sum_over_time({app="a"} | unwrap n | __error__="" [5m]))`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`sum(sum by (pod) (avg_over_time({app="a"} | unwrap n [5m])))`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`rate({app="a"}[5m] offset 1h)`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`sum by (pod) (sum_over_time({app="a"} | unwrap n [5m] offset -1h))`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`max by (pod) (max_over_time({app="a"} | unwrap n [1m] offset 90s))`, int64(time.Minute), int64(24*time.Hour), int64(1767225600e9), false)
	f.Add(`sum without (pod) (count_over_time({app="a"}[5m] offset 1500ms))`, int64(5*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`count_over_time({app="a"}[5m] offset 175200h)`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`count_over_time({app="a"}[5m] offset -175200h)`, int64(0), int64(0), int64(math.MaxInt64-3600e9), true)
	f.Add(`absent_over_time({app="a"}[5m] offset 0s)`, int64(0), int64(0), int64(0), true)
	// binary operations: each aggregation operand is routed as the query it is, under one request
	f.Add(`sum by (level) (count_over_time({app="a"}[5m])) > 0`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`rate({app="a"}[5m]) / rate({app="b"}[1m])`, int64(5*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`2 * rate({app="a"}[5m])`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`sum by (app) (rate({app="a"}[5m])) + on() vector(0)`, int64(15*time.Minute), int64(24*time.Hour), int64(1767225600e9), false)
	f.Add(`rate({app="a"}[5m]) - bytes_rate({app="a"}[1m])`, int64(5*time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`rate({app="a"}[5m] offset 1h) / rate({app="b"}[5m] offset 1h)`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`rate({app="a"}[5m] offset 1h) / rate({app="b"}[5m])`, int64(0), int64(0), int64(1767225600e9), true)
	f.Add(`count_over_time({app="a"}[5m] offset -175200h) % 2`, int64(0), int64(0), int64(math.MaxInt64-3600e9), true)
	f.Add(`sum(sum_over_time({app="a"} | unwrap n [5m])) / sum(rate({app="a"}[5m]))`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Add(`sum without (pod) (count_over_time({app="a"}[1m])) / 2`, int64(time.Minute), int64(time.Hour), int64(1767225600e9), false)
	f.Fuzz(func(t *testing.T, q string, step, span, end int64, instant bool) {
		if len(q) > 512 {
			return
		}
		p := routeQueryPlan(q)
		if p == nil {
			return
		}
		req := RouteRequest{Instant: instant, Step: time.Duration(step), Span: time.Duration(span), End: time.Unix(0, end)}
		got, err := RouteQuery(p, Profile{}, req)
		if err != nil {
			if !errors.Is(err, ErrNotLowered) {
				t.Fatalf("%q: %v is not a delegation", q, err)
			}
			return
		}
		bin, isBin := p.Root.(*queryplan.BinOp)
		if isBin != got.Binary {
			t.Fatalf("%q: binary %v", q, got.Binary)
		}
		if !isBin {
			checkRouted(t, q, p, got.Operands()[0], req)
			return
		}
		// A routed binary operation is one the lowering writes, and holds one route per aggregation operand, which
		// is the route of the operand alone; operands that share a probe read the same windows.
		if _, err := Plan(p, Profile{}); err != nil {
			t.Fatalf("%q: routed, but Plan: %v", q, err)
		}
		if k := p.Sem.OffsetKind; k == queryplan.OffsetMixed || k == queryplan.OffsetDistinct {
			t.Fatalf("%q: routed with offsets of kind %d", q, k)
		}
		ops := got.Operands()
		i := 0
		for _, n := range [2]queryplan.Node{bin.L, bin.R} {
			switch n.(type) {
			case *queryplan.RangeAgg, *queryplan.VectorAgg:
			default:
				continue
			}
			operand := *p
			operand.Root = n
			alone, err := Route(&operand, Profile{}, req)
			if err != nil || i >= len(ops) || ops[i] != alone {
				t.Fatalf("%q: operand %d routed as %+v, alone %+v, %v", q, i, ops, alone, err)
			}
			checkRouted(t, q, &operand, ops[i], req)
			i++
		}
		if i != len(ops) || len(ops) == 0 {
			t.Fatalf("%q: %d routes for %d aggregation operands", q, len(ops), i)
		}
		if len(ops) == 2 && !instant && ops[0].LevelProbe && ops[1].LevelProbe && !sameWindows(ops[0], ops[1]) && bytes.Equal(operandSelector(bin.L), operandSelector(bin.R)) {
			t.Fatalf("%q: operands with one selector probe different windows", q)
		}
	})
}

// checkRouted holds a route of the aggregation p (at the root of the query, or an operand of a binary operation) to the
// request req: a window that fits the request, a fold that fits the window, and an offset that keeps the moved bounds
// where the handlers read them back.
func checkRouted(t *testing.T, q string, p *queryplan.Plan, r MetricRoute, req RouteRequest) {
	t.Helper()
	instant := req.Instant
	switch {
	case instant && r.Window != WindowInstant:
		t.Fatalf("%q: instant routed as %d", q, r.Window)
	case !instant && (r.Bucket < time.Millisecond || r.Window == WindowInstant):
		t.Fatalf("%q: range routed as %d with bucket %v", q, r.Window, r.Bucket)
	case !instant && (r.Window == WindowTumbling) != (r.Range == req.Step):
		t.Fatalf("%q: range %v step %v routed as %d", q, r.Range, req.Step, r.Window)
	case r.Offset != rangeOf(p).Offset:
		t.Fatalf("%q: offset %v", q, r.Offset)
	case r.Offset != 0 && !offsetFits(r.Offset, r.Range, req):
		t.Fatalf("%q: offset %v routed with end %v, span %v, instant %v", q, r.Offset, req.End, req.Span, instant)
	case r.Text == "":
		t.Fatalf("%q: routed without a query", q)
	case r.Counted != unwrapsLabel(p):
		t.Fatalf("%q: counted %v", q, r.Counted)
	case r.StepBuckets && (!r.Counted || r.Window != WindowPhase):
		t.Fatalf("%q: step buckets on window %d, counted %v", q, r.Window, r.Counted)
	case r.Counted && r.Op != logql.RangeSumOverTime && r.Op != logql.RangeAvgOverTime && r.Op != logql.RangeMinOverTime && r.Op != logql.RangeMaxOverTime:
		t.Fatalf("%q: counted %s", q, r.Op)
	case r.GroupPushed && (!r.Counted || r.Window != WindowBuckets || r.Op != logql.RangeMaxOverTime && r.Op != logql.RangeMinOverTime):
		t.Fatalf("%q: group pushed on window %d, counted %v, %s", q, r.Window, r.Counted, r.Op)
	case r.SumAll && r.Counted && r.Op != logql.RangeSumOverTime:
		t.Fatalf("%q: sum of all streams over %s", q, r.Op)
	case r.Fold != FoldNone && !strings.Contains(strings.ToLower(q), "without"):
		t.Fatalf("%q: folded as %d without a clause", q, r.Fold)
	case r.Fold == FoldPushed && r.Window != WindowTumbling, r.Fold == FoldCut && r.Window != WindowBuckets && r.Window != WindowPhase:
		t.Fatalf("%q: fold %d on window %d", q, r.Fold, r.Window)
	}
}

// rangeOf is the range aggregation of the plan, or an empty one.
func rangeOf(p *queryplan.Plan) *queryplan.RangeAgg {
	for n := p.Root; ; {
		switch v := n.(type) {
		case *queryplan.VectorAgg:
			n = v.In
		case *queryplan.RangeAgg:
			return v
		default:
			return &queryplan.RangeAgg{}
		}
	}
}

// offsetFits reports whether the bounds of req moved back by off stay within the times the handlers read back as
// written: from September 2001 (1e18 nanoseconds) to a second, a range and a step short of int64 nanoseconds, in big
// integers so that no step of the check can wrap.
func offsetFits(off, rng time.Duration, req RouteRequest) bool {
	end := new(big.Int).SetInt64(req.End.UnixNano())
	start := new(big.Int).Set(end)
	if !req.Instant {
		start.Sub(start, big.NewInt(int64(req.Span)))
	}
	d := big.NewInt(int64(off))
	start.Sub(start, d)
	end.Sub(end, d)
	reach := big.NewInt(int64(rng))
	if !req.Instant {
		reach.Add(reach, big.NewInt(int64(req.Step)))
	}
	latest := new(big.Int).Sub(big.NewInt(math.MaxInt64-int64(15*time.Minute)), reach)
	return start.Cmp(big.NewInt(1e18)) >= 0 && end.Cmp(latest) <= 0
}

// unwrapsLabel reports whether the range aggregation of the plan unwraps a label.
func unwrapsLabel(p *queryplan.Plan) bool {
	for n := p.Root; ; {
		switch v := n.(type) {
		case *queryplan.VectorAgg:
			n = v.In
		case *queryplan.RangeAgg:
			return unwrapStage(v.In) != nil
		default:
			return false
		}
	}
}

// routePlan is the resolved plan of a query the route rule may be asked about, nil for another.
func routePlan(q string) *queryplan.Plan {
	expr, msg := logql.ValidateLogQLExpr(q)
	if msg != "" || expr == nil {
		return nil
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	queryplan.Resolve(p)
	switch p.Root.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg:
		return p
	}
	return nil
}
