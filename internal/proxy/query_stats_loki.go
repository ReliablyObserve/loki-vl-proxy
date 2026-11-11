package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Loki's accounting of a query, emulated (the default -query-stats-mode=loki;
// the query-stats-loki-emulation decision of the query-stats-envelope
// registry item). Every rule below was read off Loki 3.7.9's answers on the
// test stack (recorded in the decision), not from Loki's source:
//
//   - Loki splits a range query at the split interval (1h, Loki's default
//     split_queries_by_interval), aligned to multiples of it since the epoch,
//     after aligning start and end down to the step. A metric query runs
//     every piece: splits = the pieces when there are more than one, else 0;
//     its results-cache hit reports the pieces (at least 1); shards = the
//     pieces on a miss (one shard a piece at the test stack's volume).
//   - An instant query splits its range into pieces of the split interval,
//     not aligned: splits = ceil(range / 1h) when over 1, else 0; shards the
//     pieces.
//   - A log query runs its pieces from the newest (backward) or the oldest
//     (forward) and stops after the piece in which its limit was filled; each
//     piece it runs is read whole (its lines and bytes processed are every
//     line of the pieces it ran); splits = the pieces it ran when the query
//     was split, else 0; shards = the pieces it ran.
//   - volume_range: splits = the step buckets, aligned to the step since the
//     epoch, the window touches; volume: 1.

// lokiSplitInterval is Loki's default split_queries_by_interval.
const lokiSplitInterval = int64(time.Hour)

// The stats modes (-query-stats-mode).
const (
	QueryStatsModeLoki         = "loki"
	QueryStatsModeVictoriaLogs = "victorialogs"
)

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// lokiPieces is how many intervals (aligned to the interval since the
// epoch) the window [start, end] touches; an end on a boundary does not
// open the next interval.
func lokiPieces(start, end, interval int64) int64 {
	if interval <= 0 || end <= start {
		return 1
	}
	return floorDiv(end-1, interval) - floorDiv(start, interval) + 1
}

// lokiMetricRangePieces is the pieces of a metric query_range: start and end
// aligned down to the step, then split at the split interval.
func lokiMetricRangePieces(start, end, step int64) int64 {
	if step > 0 {
		start -= start - floorDiv(start, step)*step
		end -= end - floorDiv(end, step)*step
	}
	return lokiPieces(start, end, lokiSplitInterval)
}

// lokiInstantPieces is the pieces of an instant query's range.
func lokiInstantPieces(rangeDur time.Duration) int64 {
	if rangeDur <= 0 {
		return 1
	}
	return (int64(rangeDur) + lokiSplitInterval - 1) / lokiSplitInterval
}

// splitsOf is Loki's splits for a query split into pieces: 0 unsplit.
func splitsOf(pieces int64) int64 {
	if pieces > 1 {
		return pieces
	}
	return 0
}

// lokiLogRun is the part of a log query's window Loki reads: the pieces it
// ran, [from, to], ran of the window's pieces.
type lokiLogRun struct {
	from, to    int64
	ran, pieces int64
}

// lokiLogQueryRun is the part of [start, end] a log query reads when its
// answer holds entries (the oldest at tMin, the newest at tMax): every piece
// while the limit is not filled, else the pieces up to the one holding the
// last entry it returned.
func lokiLogQueryRun(start, end, limit int64, backward bool, entries, tMin, tMax int64, hasTS bool) lokiLogRun {
	n := lokiPieces(start, end, lokiSplitInterval)
	run := lokiLogRun{from: start, to: end, ran: n, pieces: n}
	if n == 1 || limit <= 0 || entries < limit || !hasTS {
		return run
	}
	if backward {
		run.from = max(start, floorDiv(tMin, lokiSplitInterval)*lokiSplitInterval)
		run.ran = lokiPieces(run.from, end, lokiSplitInterval)
	} else {
		run.to = (floorDiv(tMax, lokiSplitInterval) + 1) * lokiSplitInterval
		if run.to > end {
			run.to = end
		}
		run.ran = lokiPieces(start, run.to, lokiSplitInterval)
	}
	return run
}

// statsRequest is what the stats layer reads of the request: the window,
// step, limit and direction of a query_range, the time of an instant query.
type statsRequest struct {
	start, end, step int64 // ns
	limit            int64
	backward         bool
	ok               bool
}

func readStatsRequest(r *http.Request, route uint8) statsRequest {
	var q statsRequest
	if route == statsRouteInstant {
		t, ok := parseLokiTimeToUnixNano(r.FormValue("time"))
		if !ok {
			t = time.Now().UnixNano()
		}
		q.start, q.end, q.ok = t, t, true
		return q
	}
	start, okS := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, okE := parseLokiTimeToUnixNano(r.FormValue("end"))
	q.start, q.end, q.ok = start, end, okS && okE && end >= start
	q.step = statsStepNanos(r.FormValue("step"))
	q.limit = 100 // Loki's default limit
	if raw := strings.TrimSpace(r.FormValue("limit")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			q.limit = n
		}
	}
	q.backward = !strings.EqualFold(r.FormValue("direction"), "forward")
	return q
}

// statsStepNanos reads a step: whole seconds (Grafana's usual form) without
// allocating, anything else as the query handlers read it; 0 when unread.
func statsStepNanos(raw string) int64 {
	if raw != "" && len(raw) <= 12 && strings.Trim(raw, "0123456789") == "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err == nil && n > 0 && n < 1<<33 {
			return n * int64(time.Second)
		}
	}
	if step, ok := parseStepToNanos(raw); ok {
		return step
	}
	return 0
}

// request is the request as the stats layer reads it, read on first use
// (a cache hit that replays its computation never needs it).
func (w *queryStatsWriter) request() statsRequest {
	if !w.sreqRead {
		w.sreq, w.sreqRead = readStatsRequest(w.req, w.route), true
	}
	return w.sreq
}

// maxRangeOf is the longest range selector of a query (0 for none).
func maxRangeOf(expr logqlpkg.Expr) time.Duration {
	var longest time.Duration
	var visit func(logqlpkg.Expr)
	visit = func(e logqlpkg.Expr) {
		switch n := e.(type) {
		case *logqlpkg.RangeAggregation:
			longest = max(longest, n.RangeDuration)
		case *logqlpkg.VectorAggregation:
			visit(n.Inner)
		case *logqlpkg.BinOpExpr:
			visit(n.Left)
			visit(n.Right)
		}
	}
	visit(expr)
	return longest
}

// parseTS19 reads a 19-digit nanosecond timestamp.
func parseTS19(b [19]byte) int64 {
	var v int64
	for _, c := range b {
		v = v*10 + int64(c-'0')
	}
	return v
}

// lokiLogRun is the run of this writer's log answer.
func (w *queryStatsWriter) lokiLogRun() lokiLogRun {
	q := w.request()
	tMin, tMax := parseTS19(w.entries.tsMin), parseTS19(w.entries.tsMax)
	return lokiLogQueryRun(q.start, q.end, q.limit, q.backward, w.entries.n, tMin, tMax, w.entries.hasTS)
}

// lokiLogSummary is a log answer's summary in Loki's accounting: the lines
// of the pieces Loki would have read, from the panel's volume answer when
// the proxy holds one (totals, derived for the run's part of the window),
// else estimated from the answer itself: its entries spread over the time
// they cover, scaled to the run (exact when the limit was not filled: every
// matching line was returned). Bytes are those lines times the returned
// lines' mean size.
func (w *queryStatsWriter) lokiLogSummary(totals *statsTotals, filtered bool) (statsSummary, lokiLogRun) {
	run := w.lokiLogRun()
	q, entries := w.request(), w.entries.n
	post := entries
	if entries > 0 && q.limit > 0 && entries >= q.limit && w.entries.hasTS {
		covered := q.end - parseTS19(w.entries.tsMin)
		if !q.backward {
			covered = parseTS19(w.entries.tsMax) - q.start
		}
		if covered > 0 && run.to > run.from {
			post = max(entries, int64(float64(entries)*float64(run.to-run.from)/float64(covered)))
		}
	}
	var s statsSummary
	s.postFilter, s.lines = post, post
	if totals.hasPostFilter {
		s.postFilter = totals.postFilter
		s.lines = s.postFilter
	}
	if totals.hasLines && (filtered || !totals.hasPostFilter) {
		s.lines = totals.lines
	}
	s.lines = max(s.lines, s.postFilter)
	if entries > 0 {
		s.bytes = int64(float64(w.entries.lineBytes) / float64(entries) * float64(s.lines))
	}
	return s, run
}

// lokiReport fills rep with Loki's accounting of a computed answer.
func (w *queryStatsWriter) lokiReport(rep *statsReport, totals *statsTotals, logs bool) {
	q := w.request()
	switch {
	case logs && w.route == statsRouteRange && q.ok:
		filtered := true
		if shape, ok := totalsShapeOf(w.req.Context(), w.req.FormValue("query")); ok {
			filtered = shape.filtered
		}
		s, run := w.lokiLogSummary(totals, filtered)
		rep.summary = s
		rep.shards = run.ran
		if run.pieces > 1 {
			rep.splits = run.ran
		}
	case w.route == statsRouteRange:
		rep.summary = processedSummary(&w.rt.flow, totals, false, w.entries.n, 0)
		n := lokiMetricRangePieces(q.start, q.end, q.step)
		rep.splits, rep.shards = splitsOf(n), n
	case w.route == statsRouteInstant:
		rep.summary = processedSummary(&w.rt.flow, totals, logs, w.entries.n, w.entries.lineBytes)
		n := lokiInstantPieces(w.instantRange())
		rep.splits, rep.shards = splitsOf(n), n
	}
}

// instantRange is the longest range of the instant query.
func (w *queryStatsWriter) instantRange() time.Duration {
	expr, ok := sharedLogQLExpr(strings.TrimSpace(w.req.FormValue("query")))
	if !ok {
		return 0
	}
	return maxRangeOf(expr)
}

// lokiHitReport fills rep for a response-cache hit in Loki's accounting.
func (w *queryStatsWriter) lokiHitReport(rep *statsReport, logs bool) {
	switch {
	case w.replayed.exec > 0: // an answer Loki computes on every request
		r := w.replayed
		rep.summary = statsSummary{bytes: r.bytes, lines: r.lines, postFilter: r.postFilter, rateOver: r.exec}
		rep.splits, rep.shards, rep.querierExec = r.splits, r.shards, r.querierExec
	case w.route == statsRouteRange && !logs: // Loki's results-cache hit
		if q := w.request(); q.ok {
			rep.splits = max(lokiMetricRangePieces(q.start, q.end, q.step), 1)
		}
	}
}

// lokiVolumeSplits is Loki's splits of a volume answer.
func (w *queryStatsWriter) lokiVolumeSplits() int64 {
	q := w.request()
	if q.step <= 0 || !q.ok || q.end == q.start {
		return 1
	}
	return lokiPieces(q.start, q.end, q.step)
}

// startExactMetricBytes starts, next to a metric query, the VictoriaLogs
// volume request whose answer is the query's bytes processed in Loki's
// meaning: the line bytes of its stream selector over the window the query
// reads, (start - range, end] (-query-stats-exact-metric-bytes). The answer
// arrives on the writer's exactBytes (-1 when it cannot be read); the
// request ends with the client's.
func (p *Proxy) startExactMetricBytes(r *http.Request, sw *queryStatsWriter) {
	shape, ok := totalsShapeOf(r.Context(), r.FormValue("query"))
	q := sw.request()
	if !ok || shape.op == "" || shape.op == logqlpkg.RangeBytesOverTime || shape.op == logqlpkg.RangeBytesRate || !q.ok {
		return
	}
	req := volumeRequest{
		query:   (&logqlpkg.LogQuery{Selector: shape.expr.Selector}).String(),
		startNs: q.start - int64(shape.rangeDur),
		endNs:   q.end,
		targets: []string{"service_name"},
		limit:   exactMetricBytesSeriesLimit,
	}
	ch := make(chan int64, 1)
	sw.exactBytes = ch
	// Its own flow: its cache reads and VictoriaLogs call are not the
	// query's (a volume segment it finds must not read as part of the
	// query's answer served from a cache).
	ctx := context.WithValue(r.Context(), requestTelemetryKey, (*requestTelemetry)(nil))
	go func() {
		total := int64(-1)
		if result, err := p.computeVolume(ctx, req); err == nil {
			if sum, ok := volumeVectorSum(result); ok {
				total = sum
			}
		}
		ch <- total
	}()
}

// exactMetricBytesSeriesLimit bounds the series of the exact bytes request:
// one per service_name of the selector.
const exactMetricBytesSeriesLimit = 100000

// volumeVectorSum sums an instant volume answer's values.
func volumeVectorSum(result map[string]interface{}) (int64, bool) {
	data, _ := result["data"].(map[string]interface{})
	var items []map[string]interface{}
	switch v := data["result"].(type) {
	case []map[string]interface{}:
		items = v
	case []interface{}:
		for _, item := range v {
			m, ok := item.(map[string]interface{})
			if !ok {
				return 0, false
			}
			items = append(items, m)
		}
	default:
		return 0, false
	}
	var sum int64
	for _, m := range items {
		pair, ok := m["value"].([]interface{})
		if !ok || len(pair) != 2 {
			return 0, false
		}
		str, _ := pair[1].(string)
		v, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return 0, false
		}
		sum += v
	}
	return sum, true
}
