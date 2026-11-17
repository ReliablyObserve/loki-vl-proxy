package proxy

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
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
	value                    string
	valueColumn, countColumn string
	// added says the fold adds lower.SeriesFoldCount to the bucket stats' results.
	added bool
}

// seriesFoldOff reads the series of a parser's identity one by one, as before the fold (tests compare the two).
var seriesFoldOff bool

// seriesFoldHash is the pipe that ends the identity of a parser of every key (lower.Profile IdentityAhead).
var seriesFoldHash = logsql.PipeHash{Fields: []string{dynamicSeriesHashLabel}, Result: dynamicSeriesHashLabel}.String()

// withSeriesFold marks the bucket reads of a request of a parser's identity over a range longer than the step
// (identityAhead) to fold the series that lie in one bucket, when its outer aggregation and function fold (one
// vector aggregation, the matrix fold of the request). The extent cache is off for them.
func withSeriesFold(r *http.Request, spec statsCompatSpec, manualFunc string, fold *matrixFold) *http.Request {
	if seriesFoldOff || !identityAhead(r.Context()) || fold == nil || len(fold.levels) != 1 || !spec.gated() {
		return r
	}
	chain, ok := listedIdentityChain(r.FormValue("query"))
	if !ok || len(chain) != 1 {
		return r
	}
	identity := spec.ungated()
	if !strings.HasSuffix(identity, seriesFoldHash) {
		return r
	}
	read := &seriesFoldRead{outer: chain[0].Op, identity: identity, field: spec.UnwrapField}
	switch manualFunc {
	case "avg":
		read.columns, read.value, read.valueColumn, read.countColumn = []string{"__lvp_sum", "__sample_count"}, "__lvp_sum / __sample_count", "__lvp_sum", "__sample_count"
	case "max", "min":
		read.columns, read.value, read.valueColumn = []string{"c"}, "c", "c"
		if read.outer == logql.VectorAvg {
			read.countColumn, read.added = lower.SeriesFoldCount, true
		}
	case "stddev", "stdvar":
		read.columns = []string{deviationMeanName, "__sample_count", deviationBucketName}
		read.value, read.valueColumn, read.countColumn = deviationBucketName, deviationBucketName, "__sample_count"
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

// query is the bucket query statsQuery (the base query, the bucket stats pipe grouped by groupBy) with the fold
// written after it, for a read from start in buckets of hitStep; ok is false when the fold does not apply or the query
// would pass VictoriaLogs' default query length (the read then reads every series).
func (s *seriesFoldRead) query(statsQuery string, groupBy []string, start time.Time, hitStep time.Duration) (string, bool) {
	if s == nil {
		return statsQuery, false
	}
	bucket, offset := slidingStatsGrid(start, hitStep)
	pipes, ok := lower.SeriesFold{Outer: s.outer, By: groupBy, Columns: s.columns, Value: s.value, ValueColumn: s.valueColumn,
		CountColumn: s.countColumnIn(), Field: s.field, Bucket: bucket, Offset: offset}.Pipes(s.identity)
	if !ok {
		return statsQuery, false
	}
	var b strings.Builder
	b.Grow(len(statsQuery) + len(s.identity) + 1024)
	b.WriteString(statsQuery)
	for _, p := range pipes {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	if b.Len() > vlDefaultMaxQueryLen {
		return statsQuery, false
	}
	return b.String(), true
}

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
