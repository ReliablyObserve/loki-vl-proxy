package proxy

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// matrixFoldCase is one generated bucket matrix and the vector aggregations
// around it.
type matrixFoldCase struct {
	fn                string
	series            map[string]manualSeriesSamples
	start, end        time.Time
	step, window      time.Duration
	levels            []foldLevel
	levelText, source string
}

var (
	matrixFoldFuncs = []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate", "unwrap_rate", "sum", "max", "min", "avg"}
	matrixFoldOps   = []string{"count", "min", "max", "avg", "sum"}
	matrixFoldBy    = [][]string{nil, {}, {"zone"}, {"pod", "zone"}, {"baz"}, {"level"}, {"detected_level", "zone"}}
)

// generateMatrixFoldCase draws series with uneven buckets (some empty, some
// zero-valued, some on window edges), a window longer than, equal to and shorter
// than the step, a start on and off the step, and one to three levels.
func generateMatrixFoldCase(rnd *rand.Rand) matrixFoldCase {
	c := matrixFoldCase{fn: matrixFoldFuncs[rnd.Intn(len(matrixFoldFuncs))]}
	steps := []time.Duration{5 * time.Second, 30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 150 * time.Second}
	c.step = steps[rnd.Intn(len(steps))]
	c.window = windows[rnd.Intn(len(windows))]
	if rnd.Intn(4) == 0 {
		c.window = c.step
	}
	c.start = time.Unix(1_800_000_000+int64(rnd.Intn(600)), 0)
	c.end = c.start.Add(time.Duration(rnd.Intn(40)) * c.step)
	if rnd.Intn(8) == 0 {
		c.end = c.start.Add(-c.step) // an empty range
	}
	bucket := c.step
	if rnd.Intn(2) == 0 {
		bucket = time.Duration(1+rnd.Intn(30)) * time.Second
	}
	pods, zones := []string{"a", "b", "c", "d", "e", "f"}, []string{"x", "y"}
	levels := []string{"", "info", "error"}
	count := rnd.Intn(14)
	c.series = map[string]manualSeriesSamples{}
	bytesFn := strings.HasPrefix(c.fn, "bytes")
	for i := 0; i < count; i++ {
		metric := map[string]string{"pod": pods[rnd.Intn(len(pods))], "zone": zones[rnd.Intn(len(zones))]}
		if lvl := levels[rnd.Intn(len(levels))]; lvl != "" {
			metric["level"], metric["detected_level"] = lvl, lvl
		}
		entry := manualSeriesSamples{Metric: metric}
		first := c.start.Add(-c.window).UnixNano()
		span := int64(c.end.Sub(c.start) + c.window + c.step)
		buckets := rnd.Intn(25)
		at := first
		for j := 0; j < buckets && span > 0; j++ {
			at = first + (rnd.Int63n(span)/int64(bucket))*int64(bucket)
			value := float64(1 + rnd.Intn(9))
			switch rnd.Intn(5) {
			case 0:
				value = 0
			case 1:
				value = float64(rnd.Intn(1000)) / 7
			}
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: at, value: value})
		}
		// ascending, distinct bucket labels
		sortedSamples := entry.Samples[:0]
		seen := map[int64]bool{}
		for _, sample := range entry.Samples {
			if !seen[sample.ts] {
				seen[sample.ts] = true
				sortedSamples = append(sortedSamples, sample)
			}
		}
		for a := 1; a < len(sortedSamples); a++ {
			for b := a; b > 0 && sortedSamples[b].ts < sortedSamples[b-1].ts; b-- {
				sortedSamples[b], sortedSamples[b-1] = sortedSamples[b-1], sortedSamples[b]
			}
		}
		entry.Samples = sortedSamples
		if bytesFn || c.fn == "unwrap_rate" {
			entry.PresentBuckets = []int64{}
			for _, sample := range entry.Samples {
				entry.PresentBuckets = append(entry.PresentBuckets, sample.ts)
			}
		}
		if c.fn == "avg" {
			for _, sample := range entry.Samples {
				entry.Counts = append(entry.Counts, rangeMetricSample{ts: sample.ts, value: float64(1 + rnd.Intn(4))})
			}
		}
		c.series[fmt.Sprintf("%s|%d", canonicalLabelsKey(metric), i)] = entry
	}
	depth := 1 + rnd.Intn(3)
	var text []string
	for i := 0; i < depth; i++ {
		op := matrixFoldOps[rnd.Intn(len(matrixFoldOps))]
		by := matrixFoldBy[rnd.Intn(len(matrixFoldBy))]
		if rnd.Intn(6) == 0 {
			c.levels = append(c.levels, withoutFoldLevel([]string{"pod"}, op))
			text = append(text, op+" without (pod)")
			continue
		}
		c.levels = append(c.levels, byFoldLevel(by, op))
		text = append(text, fmt.Sprintf("%s by %v", op, by))
	}
	c.levelText = strings.Join(text, " > ")
	c.source = fmt.Sprintf("%s step=%v window=%v range=%v..%v series=%d", c.fn, c.step, c.window, c.start.Unix(), c.end.Unix(), len(c.series))
	return c
}

// check builds the case both ways and fails unless the bytes are the same: the
// fold the builder applies as it goes, and the fold of the encoded matrix that
// foldVectorAggregations applied before. It returns the sizes of the two.
func (c matrixFoldCase) check(t testing.TB) (matrixBytes, foldedBytes int) {
	t.Helper()
	// Both builds read the same series: the builder sorts and shifts nothing in place.
	matrix, err := buildHitsRangeMetricMatrix(c.fn, c.series, c.start, c.end, c.step, c.window, 1<<30)
	if err != nil {
		t.Fatalf("%s: %v", c.source, err)
	}
	want, ok := foldLevels(matrix, c.levels...)
	if !ok {
		t.Fatalf("%s: the typed fold does not read the matrix", c.source)
	}
	fold := &matrixFold{levels: c.levels}
	got, err := buildHitsRangeMetricMatrixFolded(c.fn, c.series, c.start, c.end, c.step, c.window, 1<<30, fold)
	if err != nil {
		t.Fatalf("%s: %v", c.source, err)
	}
	if !fold.folded && !bytes.Contains(matrix, []byte(`"result":[]`)) {
		// only an empty matrix may be left to the caller's fold
		if c.end.Before(c.start) {
			return len(matrix), len(got)
		}
		t.Fatalf("%s %s: the builder did not fold a matrix with series: %s", c.source, c.levelText, matrix)
	}
	if fold.folded && !bytes.Equal(got, want) {
		t.Fatalf("%s %s:\n got %s\nwant %s", c.source, c.levelText, got, want)
	}
	return len(matrix), len(got)
}

// TestMatrixFoldSinkMatchesFoldedMatrix: over generated series, windows, steps
// and chains of count, min, max, avg and sum (by, without, no grouping, a label
// no series has), the builder that folds as it goes answers the bytes the fold
// of the encoded matrix answers.
//
// conformance: semantics/series-level-aggregation-bounded-memory, loki_api_v1_query_range
func TestMatrixFoldSinkMatchesFoldedMatrix(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261104))
	folded := 0
	for i := 0; i < 1500; i++ {
		c := generateMatrixFoldCase(rnd)
		matrixBytes, foldedBytes := c.check(t)
		if foldedBytes > 0 && matrixBytes > 0 {
			folded++
		}
	}
	if folded < 800 {
		t.Fatalf("only %d of 1500 generated cases built a matrix", folded)
	}
}

// FuzzMatrixFoldSink holds the folding builder to the fold of the encoded
// matrix on whatever the seed draws.
func FuzzMatrixFoldSink(f *testing.F) {
	for _, seed := range []int64{1, 2, 3, 20261104, 1 << 40} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) {
		generateMatrixFoldCase(rand.New(rand.NewSource(seed))).check(t)
	})
}

// TestMatrixFoldSinkBuffersGroupsNotStreams: the matrix the builder encodes
// before the fold holds every stream at every step, the folded builder only the
// groups. 2800 streams in 6 groups over a 48h range at a 120s step, the shape of
// B2 (bench/ab aggregation-over-binary), through the 64 MiB default bound.
//
// conformance: semantics/series-level-aggregation-bounded-memory
func TestMatrixFoldSinkBuffersGroupsNotStreams(t *testing.T) {
	// A single-threaded size check over 2,800 streams: slow under the race
	// detector and nothing for it to check; the test job runs it in full.
	skipSimulationUnderRace(t)
	start := time.Unix(1_800_000_000, 0)
	step, window := 120*time.Second, 5*time.Minute
	end := start.Add(48 * time.Hour)
	series := map[string]manualSeriesSamples{}
	for i := 0; i < 2800; i++ {
		entry := manualSeriesSamples{Metric: map[string]string{"service_name": fmt.Sprintf("svc-%d", i%6), "pod": fmt.Sprintf("pod-%04d", i), "level": "info", "detected_level": "info"}}
		for at := start.Add(-window); !at.After(end); at = at.Add(2 * time.Minute) {
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: at.UnixNano(), value: float64(1 + i%5)})
		}
		series[canonicalLabelsKey(entry.Metric)] = entry
	}
	levels := []foldLevel{byFoldLevel([]string{"service_name"}, "count")}
	matrix, err := buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	_, err = buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window, 64<<20)
	fold := &matrixFold{levels: levels}
	folded, foldedErr := buildHitsRangeMetricMatrixFolded("count_over_time", series, start, end, step, window, 64<<20, fold)
	if foldedErr != nil || !fold.folded {
		t.Fatalf("folded build: %v folded=%v", foldedErr, fold.folded)
	}
	t.Logf("per-stream matrix %d bytes (over the 64 MiB bound: %v), folded answer %d bytes", len(matrix), err != nil, len(folded))
	if err == nil {
		t.Fatalf("the fixture must pass the default bound unfolded, got %d bytes", len(matrix))
	}
	if len(folded)*50 > len(matrix) {
		t.Fatalf("folded answer %d bytes is not far below the %d bytes of the per-stream matrix", len(folded), len(matrix))
	}
	want, _ := foldLevels(matrix, levels...)
	if !bytes.Equal(folded, want) {
		t.Fatal("folded answer differs from the fold of the matrix")
	}
}

// TestSeriesLevelAggregationStaysWithinResponseBudget: count, min, max and avg
// of a range function on a sliding window answer within a response bound the
// per-stream matrix passes, like Loki, which holds the groups of a step only.
// Every answer is Loki's (reference in vector_agg_series_differential_test.go);
// with the fold in the builder off, the same requests answer the 500 naming
// the flag, which shows the bound bites.
//
// conformance: semantics/series-level-aggregation-bounded-memory, semantics/non-sum-vector-aggregation-unreduced, loki_api_v1_query_range
func TestSeriesLevelAggregationStaysWithinResponseBudget(t *testing.T) {
	var cases [][]vaStage
	for _, op := range []string{"count", "min", "max", "avg"} {
		cases = append(cases, []vaStage{{op: op}}, []vaStage{{op: op, by: []string{"zone"}}})
	}
	cases = append(cases, []vaStage{{op: "max", by: []string{"pod", "zone"}}, {op: "sum", by: []string{"zone"}}})
	const budget = 1100
	over := 0
	for _, stages := range cases {
		for _, fn := range []string{"count_over_time", "rate"} {
			t.Run(vaScenario{stages: stages, fn: fn, window: time.Minute}.query(), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(stages))*131 + int64(len(fn))))
				for i := 0; i < 8; i++ {
					s := generateVectorAggScenario(rnd, fn, stages, false)
					s.window, s.version, s.budget = 5*time.Minute, "v1.52.0", budget
					s.end = s.start + 7*int64(s.step) // eight evaluation steps
					runVectorAggScenario(t, fmt.Sprintf("#%d", i), s)

					// The same request without the fold in the builder.
					matrixFoldOff.Store(true)
					defer matrixFoldOff.Store(false)
					emu := &vlemu.Emulator{T: t, Rows: s.rows, Version: s.version, Field: "amount"}
					p, closeBackend := newVectorAggProxyBudget(t, emu, budget)
					target := fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&step=%d", url.QueryEscape(s.query()), s.start/1e9, s.end/1e9, int64(s.step/time.Second))
					res := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
					closeBackend()
					matrixFoldOff.Store(false)
					if res.Code == 500 && strings.Contains(res.Body.String(), "ResourceExhausted") {
						over++
					}
				}
			})
		}
	}
	if over == 0 {
		t.Fatal("no request exceeded the bound without the fold in the builder: the budget proves nothing")
	}
}

// TestAggregationOverBinaryRateCountRatioSlidingWindowWithinBudget is B2 of
// bench/ab (aggregation-over-binary): `sum by (service_name) (rate(...[5m]) /
// count_over_time(...[5m]))` at a 120s step. The ratio is 1/300 for every
// stream with a line in the window, so the sum is the number of active streams
// over 300 (Loki evaluates the binary per stream and sums per step,
// pkg/logql/evaluator.go newBinOpStepEvaluator, VectorAggEvaluator.Next). The
// proxy reads that as `count by (service_name)` of the per-stream counts, and
// 120 streams over four hours passed a 128 KiB response bound as a matrix of
// streams x steps before the count; they now answer within it, with Loki's
// values, and 500 with the fold in the builder off.
//
// conformance: semantics/series-level-aggregation-bounded-memory, semantics/aggregation-over-binary-dropped, loki_api_v1_query_range
func TestAggregationOverBinaryRateCountRatioSlidingWindowWithinBudget(t *testing.T) {
	const (
		step   = 120 * time.Second
		window = 300 * time.Second
		budget = 128 << 10
	)
	end := binaryAggregationAt
	start := end - int64(4*time.Hour)
	var rows []vlemu.Row
	for svc := 0; svc < 4; svc++ {
		for pod := 0; pod < 30; pod++ {
			stream := fmt.Sprintf(`{env="production",service_name="s%d",pod="p%d"}`, svc, pod)
			// A stream is quiet for the first half hour of every 3h, so the active
			// count moves between steps; its lines are 70s apart, off the step grid.
			for at := start; at <= end; at += 70e9 {
				if (at-start)%int64(3*time.Hour) < int64(30*time.Minute) && pod%2 == 0 {
					continue
				}
				rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1})
			}
		}
	}
	want := map[string]map[int64]float64{}
	for ts := start; ts <= end; ts += int64(step) {
		active := map[string]map[string]bool{}
		for _, row := range rows {
			if row.TS > ts-int64(window) && row.TS <= ts {
				svc := refStreamLabels(row.Stream)["service_name"]
				if active[svc] == nil {
					active[svc] = map[string]bool{}
				}
				active[svc][row.Stream] = true
			}
		}
		for svc, streams := range active {
			key := fmt.Sprintf(`{"service_name":%q}`, svc)
			if want[key] == nil {
				want[key] = map[int64]float64{}
			}
			want[key][ts/1e9] = float64(len(streams)) / window.Seconds()
		}
	}
	query := `sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`
	configure := func(cfg *Config) { cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = budget }
	got, _ := binaryAggregationAnswerFor(t, &vlemu.Emulator{T: t, Rows: rows, Version: "v1.52.0"}, "0", query, start, end, step, configure)
	compareBinaryAggregation(t, query, got, want)

	matrixFoldOff.Store(true)
	defer matrixFoldOff.Store(false)
	backend := httptest.NewServer((&vlemu.Emulator{T: t, Rows: rows, Version: "v1.52.0"}).Handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: budget}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	target := fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&step=%d", url.QueryEscape(query), start/1e9, end/1e9, int64(step/time.Second))
	if res := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusInternalServerError || !strings.Contains(res.Body.String(), "ResourceExhausted") {
		t.Fatalf("without the fold in the builder the %d byte bound should be passed: %d %s", budget, res.Code, res.Body)
	}
}

// TestSeriesLevelAggregationRepeatedAndMovedWindows sends a folded aggregation to
// one proxy with its response cache and settled-steps cache on: the request, the
// same request again, and windows moved by steps and back. A reused answer is
// already folded and must never be folded again (a second count of a count
// answers 1); every answer is the reference's.
//
// conformance: semantics/series-level-aggregation-bounded-memory, loki_api_v1_query_range
func TestSeriesLevelAggregationRepeatedAndMovedWindows(t *testing.T) {
	const (
		step   = 120 * time.Second
		window = 300 * time.Second
	)
	end0 := time.Now().Add(-3 * time.Hour).Truncate(step).UnixNano()
	first := end0 - int64(3*time.Hour)
	var rows []vlemu.Row
	for svc := 0; svc < 3; svc++ {
		for pod := 0; pod < 6; pod++ {
			stream := fmt.Sprintf(`{env="production",service_name="s%d",pod="p%d"}`, svc, pod)
			for at := first + int64(pod)*1e9; at <= end0+20*int64(step); at += int64(40+10*svc) * 1e9 {
				rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1})
			}
		}
	}
	lines := func(ts int64) map[string]map[string]int { // service -> stream -> lines in (ts-window, ts]
		out := map[string]map[string]int{}
		for _, row := range rows {
			if row.TS > ts-int64(window) && row.TS <= ts {
				svc := refStreamLabels(row.Stream)["service_name"]
				if out[svc] == nil {
					out[svc] = map[string]int{}
				}
				out[svc][row.Stream]++
			}
		}
		return out
	}
	shapes := []struct {
		query string
		value func(streams map[string]int) float64
	}{
		{`sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`, func(s map[string]int) float64 { return float64(len(s)) / window.Seconds() }},
		{`count by (service_name) (count_over_time({env="production"}[5m]))`, func(s map[string]int) float64 { return float64(len(s)) }},
		{`avg by (service_name) (rate({env="production"}[5m]))`, func(s map[string]int) float64 {
			total := 0
			for _, n := range s {
				total += n
			}
			return float64(total) / window.Seconds() / float64(len(s))
		}},
	}
	for _, shape := range shapes {
		t.Run(shape.query, func(t *testing.T) {
			emu := &vlemu.Emulator{T: t, Rows: rows, Version: "v1.52.0"}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			c := cache.NewWithMaxBytes(time.Minute, 100000, 64<<20)
			defer c.Close()
			p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
				QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour, QueryRangeFreshness: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			span := int64(40) * int64(step)
			for _, move := range []int64{0, 0, 1, 1, 5, -3, 0} {
				end := end0 + move*int64(step)
				start := end - span
				want := map[string]map[int64]float64{}
				for ts := start; ts <= end; ts += int64(step) {
					for svc, streams := range lines(ts) {
						key := fmt.Sprintf(`{"service_name":%q}`, svc)
						if want[key] == nil {
							want[key] = map[int64]float64{}
						}
						want[key][ts/1e9] = shape.value(streams)
					}
				}
				got, _ := binaryAggregationAnswerVia(t, p, "0", shape.query, start, end, step)
				compareBinaryAggregation(t, fmt.Sprintf("%s window moved %d steps", shape.query, move), got, want)
			}
			// The repeats and moved windows were served from the caches, in part.
			if calls := len(emu.EndpointCalls("/select/logsql/stats_query_range")); calls >= 7 {
				t.Fatalf("%d VictoriaLogs stats calls for 7 requests: no cache answered any of them", calls)
			}
		})
	}
}
