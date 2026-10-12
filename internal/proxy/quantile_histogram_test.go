package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"
)

// lokiQuantile is Loki v3.7.7's Quantile (pkg/logql/range_vector.go:465-487)
// over plain values, kept apart from the proxy's implementation.
func lokiQuantile(q float64, values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := float64(len(sorted))
	rank := q * (n - 1)
	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)
	weight := rank - math.Floor(rank)
	return sorted[int(lowerIndex)]*(1-weight) + sorted[int(upperIndex)]*weight
}

// sameQuantile compares within the last bits a fused multiply-add may change.
func sameQuantile(got, want float64) bool {
	if math.IsNaN(want) || math.IsInf(want, 0) {
		return math.IsNaN(got) == math.IsNaN(want) && (math.IsNaN(got) || got == want)
	}
	return math.Abs(got-want) <= 1e-12*math.Max(1, math.Abs(want))
}

// conformance: semantics/unwrap-quantile-response-limit
//
// The proxy's quantile over value counts answers Loki's Quantile, edges
// included: one sample, two, the phi bounds, repeated values (a count above
// one), no sample (NaN), phi outside [0, 1].
func TestQuantileOfCountsEdges(t *testing.T) {
	type vc struct{ value, count float64 }
	for _, tc := range []struct {
		name   string
		counts []vc
		phis   []float64
		want   []float64
	}{
		{"n=1", []vc{{7, 1}}, []float64{0, 0.5, 0.95, 1}, []float64{7, 7, 7, 7}},
		{"n=2", []vc{{10, 1}, {20, 1}}, []float64{0, 0.5, 0.95, 1}, []float64{10, 15, 19.5, 20}},
		{"n=4 Loki interpolates where VictoriaLogs picks int(phi*n)", []vc{{4, 1}, {1, 1}, {3, 1}, {2, 1}}, []float64{0.5}, []float64{2.5}},
		{"repeated values", []vc{{5, 3}, {1, 1}}, []float64{0, 0.25, 0.5, 1}, []float64{1, 4, 5, 5}},
		{"repeated values across ranks", []vc{{1, 2}, {9, 2}}, []float64{0.5, 0.95}, []float64{5, 9}},
		{"negative and fractional values", []vc{{-1.5, 1}, {0.25, 2}, {3, 1}}, []float64{0.5}, []float64{0.25}},
		{"phi<0", []vc{{1, 1}, {2, 1}}, []float64{-0.1}, []float64{math.Inf(-1)}},
		{"phi>1", []vc{{1, 1}, {2, 1}}, []float64{1.1}, []float64{math.Inf(+1)}},
		{"empty", nil, []float64{0.5, -1, 2}, []float64{math.NaN(), math.NaN(), math.NaN()}},
		{"one infinite sample", []vc{{math.Inf(+1), 1}}, []float64{0.5}, []float64{math.NaN()}},
	} {
		for i, phi := range tc.phis {
			var counts []quantileSample
			var values []float64
			for _, c := range tc.counts {
				counts = append(counts, quantileSample{value: c.value, count: c.count})
				for k := 0; k < int(c.count); k++ {
					values = append(values, c.value)
				}
			}
			got := quantileOfCounts(phi, counts)
			if !sameQuantile(got, tc.want[i]) || !sameQuantile(got, lokiQuantile(phi, values)) {
				t.Errorf("%s phi=%v: got %v, want %v (Loki reference %v)", tc.name, phi, got, tc.want[i], lokiQuantile(phi, values))
			}
		}
	}
}

// Buckets are labelled by their left edge: the window (t-range, t] is the
// buckets labelled in [t-range, t). A tumbling window is one bucket, a sliding
// one merges several, and a window without counts is absent.
func TestQuantileWindowMergesBuckets(t *testing.T) {
	m := int64(time.Minute)
	samples := []quantileSample{
		{ts: 0, value: 1, count: 1}, {ts: 0, value: 3, count: 1},
		{ts: m, value: 2, count: 2},
		{ts: 3 * m, value: 10, count: 1},
	}
	var scratch []quantileSample
	for _, tc := range []struct {
		name       string
		start, end int64
		want       float64
		ok         bool
	}{
		{"tumbling first", 0, m, 2, true},            // {1, 3}
		{"tumbling second", m, 2 * m, 2, true},       // {2, 2}
		{"sliding two buckets", 0, 2 * m, 2, true},   // {1, 2, 2, 3}
		{"sliding over a gap", m, 4 * m, 2, true},    // {2, 2, 10}
		{"empty bucket", 2 * m, 3 * m, 0, false},     // nothing labelled 2m
		{"edges excluded", -m, 0, 0, false},          // label 0 is the next window
		{"sliding three buckets", 0, 4 * m, 2, true}, // {1, 2, 2, 3, 10}: rank 2
	} {
		got, ok := quantileWindow(0.5, samples, tc.start, tc.end, &scratch)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("%s: got %v %v, want %v %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// conformance: semantics/unwrap-quantile-response-limit
//
// Differential: the real handler over VictoriaLogs' value counts (vlEmulator:
// stats_query_range buckets with the offset argument, stats_query for instant
// queries) against a Loki reference over the generated rows: the window
// (t-range, t], the series of the grouping, Loki's Quantile. Tumbling, sliding
// and shorter-than-step windows, by (level), by () and the ungrouped per-stream
// form, query_range and query. VictoriaLogs is never asked for raw rows.
func TestQuantileHistogramDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	base := time.Unix(1700001000, 0).UTC()
	levels := []string{"info", "warn", "error"}
	type row struct {
		ts         int64
		pod, level string
		value      float64
	}
	var rows []row
	for i := 0; i < 900; i++ {
		v := float64(rng.Intn(40)) // repeats
		if rng.Intn(3) == 0 {
			v = math.Round(rng.ExpFloat64()*1e6) / 1e3 // distinct floats
		}
		rows = append(rows, row{
			ts:    base.Add(-15*time.Minute + time.Duration(rng.Int63n(int64(35*time.Minute)))).UnixNano(),
			pod:   fmt.Sprintf("p%d", rng.Intn(4)),
			level: levels[rng.Intn(len(levels))],
			value: v,
		})
	}
	// A row exactly on a window edge belongs to the window it ends.
	rows = append(rows, row{ts: base.UnixNano(), pod: "p0", level: "info", value: 1000})
	emu := &vlEmulator{t: t, field: "lat"}
	for _, r := range rows {
		emu.rows = append(emu.rows, emuRow{ts: r.ts, stream: fmt.Sprintf(`{app="api",pod=%q,level=%q}`, r.pod, r.level), value: r.value})
	}
	srv := httptest.NewServer(emu.handler())
	defer srv.Close()
	p := newSlidingTestProxy(t, srv.URL)

	seriesOf := map[string]func(row) string{
		"by (level)": func(r row) string { return "level=" + r.level },
		"by ()":      func(row) string { return "" },
		"":           func(r row) string { return "pod=" + r.pod + ",level=" + r.level },
	}
	keyOf := map[string]func(map[string]string) string{
		"by (level)": func(m map[string]string) string { return "level=" + m["level"] },
		"by ()":      func(map[string]string) string { return "" },
		"":           func(m map[string]string) string { return "pod=" + m["pod"] + ",level=" + m["level"] },
	}
	// An ungrouped quantile after a parser keeps Loki's series of every parsed
	// label on the raw evaluator; the per-stream form is the one without.
	pipeline := map[string]string{
		"by (level)": `{app="api"} | json | unwrap lat`,
		"by ()":      `{app="api"} | json | unwrap lat`,
		"":           `{app="api"} | unwrap lat`,
	}
	reference := func(grouping string, phi float64, t time.Time, window time.Duration) map[string]float64 {
		values := map[string][]float64{}
		for _, r := range rows {
			if r.ts > t.Add(-window).UnixNano() && r.ts <= t.UnixNano() {
				key := seriesOf[grouping](r)
				values[key] = append(values[key], r.value)
			}
		}
		out := map[string]float64{}
		for key, vs := range values {
			out[key] = lokiQuantile(phi, vs)
		}
		return out
	}
	for _, phi := range []float64{0, 0.5, 0.95, 0.99, 1} {
		for grouping := range seriesOf {
			for _, shape := range []struct {
				window, step time.Duration
			}{
				{time.Minute, time.Minute},          // tumbling
				{5 * time.Minute, time.Minute},      // sliding
				{150 * time.Second, time.Minute},    // sliding, gcd buckets of 30s
				{time.Minute, 5 * time.Minute},      // shorter than the step
				{2 * time.Minute, 90 * time.Second}, // sliding over a fractional ratio
			} {
				name := fmt.Sprintf("phi=%v/%q/range=%v/step=%v", phi, grouping, shape.window, shape.step)
				query := fmt.Sprintf(`quantile_over_time(%v, %s [%ds]) %s`, phi, pipeline[grouping], int(shape.window.Seconds()), grouping)
				start, end := base.Add(-10*time.Minute), base.Add(10*time.Minute)
				params := url.Values{"query": {query}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}, "step": {strconv.Itoa(int(shape.step.Seconds()))}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusOK {
					t.Fatalf("%s: %d %s", name, rec.Code, rec.Body.String())
				}
				var resp struct {
					Data struct {
						Result []struct {
							Metric map[string]string `json:"metric"`
							Values [][]any           `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				got := map[int64]map[string]float64{}
				for _, s := range resp.Data.Result {
					for _, point := range s.Values {
						ts := int64(point[0].(float64))
						v, _ := strconv.ParseFloat(point[1].(string), 64)
						if got[ts] == nil {
							got[ts] = map[string]float64{}
						}
						got[ts][keyOf[grouping](s.Metric)] = v
					}
				}
				for ts := start; !ts.After(end); ts = ts.Add(shape.step) {
					want := reference(grouping, phi, ts, shape.window)
					if len(got[ts.Unix()]) != len(want) {
						t.Fatalf("%s t=%v: series %v, want %v", name, ts, got[ts.Unix()], want)
					}
					for key, w := range want {
						if g, ok := got[ts.Unix()][key]; !ok || !sameQuantile(g, w) {
							t.Fatalf("%s t=%v %s: got %v, want %v", name, ts, key, g, w)
						}
					}
				}
			}
			// Instant query at an evaluation time off the step grid.
			evalTS := base.Add(37*time.Second + 123*time.Millisecond)
			query := fmt.Sprintf(`quantile_over_time(%v, %s [3m]) %s`, phi, pipeline[grouping], grouping)
			params := url.Values{"query": {query}, "time": {evalTS.Format(time.RFC3339Nano)}}
			rec := httptest.NewRecorder()
			p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("instant %q: %d %s", query, rec.Code, rec.Body.String())
			}
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Value  []any             `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			want := reference(grouping, phi, evalTS, 3*time.Minute)
			if len(resp.Data.Result) != len(want) {
				t.Fatalf("instant %q: %s, want %v", query, rec.Body.String(), want)
			}
			for _, s := range resp.Data.Result {
				v, _ := strconv.ParseFloat(s.Value[1].(string), 64)
				if w, ok := want[keyOf[grouping](s.Metric)]; !ok || !sameQuantile(v, w) {
					t.Fatalf("instant %q %v: got %v, want %v", query, s.Metric, v, w)
				}
			}
		}
	}
	if q := emu.endpointCalls("/select/logsql/query"); len(q) != 0 {
		t.Fatalf("quantile read raw rows: %d queries", len(q))
	}
}

// VictoriaLogs before v1.45 has no stats_query_range offset argument: buckets
// sit on multiples of their size. A range query whose first window starts on
// that grid still reads the value counts; one off the grid falls back to the
// raw evaluator (as every bucketed metric does there), and an instant query
// reads stats_query, which needs no buckets, on every version.
func TestQuantileHistogramWithoutStatsRangeOffset(t *testing.T) {
	base := time.Unix(1700001000, 0).UTC() // a multiple of 1m
	emu := &vlEmulator{t: t, field: "lat"}
	for i, v := range []float64{5, 1, 9, 3, 7, 2} {
		emu.rows = append(emu.rows, emuRow{ts: base.Add(-time.Duration(i*9+1) * time.Second).UnixNano(), stream: `{app="api"}`, value: v})
	}
	srv := httptest.NewServer(emu.handler())
	defer srv.Close()
	p := newGapTestProxy(t, srv.URL)
	p.storeBackendVersion("v1.40.0", "v1.40.0")
	const query = `quantile_over_time(0.5, {app="api"} | unwrap lat [1m]) by ()`
	for _, tc := range []struct {
		name  string
		start time.Time
		raw   bool
	}{
		{"aligned", base, false},
		{"unaligned", base.Add(-500 * time.Millisecond), true},
	} {
		before := len(emu.endpointCalls("/select/logsql/query"))
		got := unwrapRangeValues(t, p, query, tc.start, tc.start, time.Minute)
		var window []float64
		for _, r := range emu.rows {
			if r.ts > tc.start.Add(-time.Minute).UnixNano() && r.ts <= tc.start.UnixNano() {
				window = append(window, r.value)
			}
		}
		if want := strconv.FormatFloat(lokiQuantile(0.5, window), 'f', -1, 64); len(got) != 1 || got[0] != want {
			t.Fatalf("%s: got %v, want [%s]", tc.name, got, want)
		}
		if raw := len(emu.endpointCalls("/select/logsql/query")) > before; raw != tc.raw {
			t.Fatalf("%s: raw rows read %v, want %v", tc.name, raw, tc.raw)
		}
	}
	params := url.Values{"query": {query}, "time": {base.Add(-500 * time.Millisecond).Format(time.RFC3339Nano)}}
	rec := httptest.NewRecorder()
	before := len(emu.endpointCalls("/select/logsql/query"))
	p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
	if rec.Code != http.StatusOK || len(emu.endpointCalls("/select/logsql/stats_query")) == 0 || len(emu.endpointCalls("/select/logsql/query")) != before {
		t.Fatalf("instant: %d %s", rec.Code, rec.Body.String())
	}
}

// conformance: semantics/unwrap-quantile-response-limit
//
// Differential: a vector aggregation over quantile_over_time folds the
// quantile of every series (stream) per step (Loki's VectorAggEvaluator), while
// the range aggregation's own by (...) takes one quantile over the group's
// samples. The translation groups both alike; the proxy must not: `sum by
// (level) (quantile_over_time(...))` adds per-stream quantiles. Ungrouped sum,
// avg, min, max and count, grouped ones, and both levels at once, after a
// parser and without, on query_range and query, against the Loki reference.
func TestQuantileHistogramOuterAggregationDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	base := time.Unix(1700001000, 0).UTC()
	type row struct {
		ts         int64
		pod, level string
		value      float64
	}
	var rows []row
	emu := &vlEmulator{t: t, field: "lat"}
	for i := 0; i < 600; i++ {
		r := row{
			ts:    base.Add(-12*time.Minute + time.Duration(rng.Int63n(int64(24*time.Minute)))).UnixNano(),
			pod:   fmt.Sprintf("p%d", rng.Intn(3)),
			level: []string{"info", "error"}[rng.Intn(2)],
			value: float64(rng.Intn(50)),
		}
		rows = append(rows, r)
		emu.rows = append(emu.rows, emuRow{ts: r.ts, stream: fmt.Sprintf(`{app="api",pod=%q,level=%q}`, r.pod, r.level), value: r.value})
	}
	srv := httptest.NewServer(emu.handler())
	defer srv.Close()
	p := newSlidingTestProxy(t, srv.URL)

	merge := func(op string, vs []float64) float64 {
		acc := vs[0]
		for i, v := range vs[1:] {
			switch op {
			case "sum":
				acc += v
			case "min":
				acc = math.Min(acc, v)
			case "max":
				acc = math.Max(acc, v)
			case "avg":
				acc += (v - acc) / float64(i+2)
			}
		}
		if op == "count" {
			return float64(len(vs))
		}
		return acc
	}
	for _, tc := range []struct {
		query string // %s: the quantile_over_time expression
		// series is the row's series key for the quantile, group its key in the answer.
		series, group func(r row) string
		op            string
		answerKey     func(m map[string]string) string
	}{
		{`sum(%s)`, func(r row) string { return r.pod + r.level }, func(row) string { return "" }, "sum", func(map[string]string) string { return "" }},
		{`sum by () (%s)`, func(r row) string { return r.pod + r.level }, func(row) string { return "" }, "sum", func(map[string]string) string { return "" }},
		{`max(%s)`, func(r row) string { return r.pod + r.level }, func(row) string { return "" }, "max", func(map[string]string) string { return "" }},
		{`min(%s)`, func(r row) string { return r.pod + r.level }, func(row) string { return "" }, "min", func(map[string]string) string { return "" }},
		{`count(%s)`, func(r row) string { return r.pod + r.level }, func(row) string { return "" }, "count", func(map[string]string) string { return "" }},
		{`sum by (level) (%s)`, func(r row) string { return r.pod + r.level }, func(r row) string { return r.level }, "sum", func(m map[string]string) string { return m["level"] }},
		{`avg by (level) (%s)`, func(r row) string { return r.pod + r.level }, func(r row) string { return r.level }, "avg", func(m map[string]string) string { return m["level"] }},
		{`max by (pod) (%s)`, func(r row) string { return r.pod + r.level }, func(r row) string { return r.pod }, "max", func(m map[string]string) string { return m["pod"] }},
		{`sum by (level) (%s by (level, pod))`, func(r row) string { return r.pod + r.level }, func(r row) string { return r.level }, "sum", func(m map[string]string) string { return m["level"] }},
		{`sum(%s by (level))`, func(r row) string { return r.level }, func(row) string { return "" }, "sum", func(map[string]string) string { return "" }},
		// The outer without belongs to the without grouping's own route; the
		// values must still be Loki's per-stream fold.
		{`sum without (pod) (%s)`, func(r row) string { return r.pod + r.level }, func(r row) string { return r.level }, "sum", func(m map[string]string) string { return m["level"] }},
	} {
		for _, pipeline := range []string{`{app="api"} | unwrap lat`, `{app="api"} | json | unwrap lat`} {
			reference := func(at time.Time, window time.Duration) map[string]float64 {
				perSeries := map[string][]float64{}
				groupOf := map[string]string{}
				for _, r := range rows {
					if r.ts > at.Add(-window).UnixNano() && r.ts <= at.UnixNano() {
						perSeries[tc.series(r)] = append(perSeries[tc.series(r)], r.value)
						groupOf[tc.series(r)] = tc.group(r)
					}
				}
				perGroup := map[string][]float64{}
				keys := make([]string, 0, len(perSeries))
				for key := range perSeries {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					perGroup[groupOf[key]] = append(perGroup[groupOf[key]], lokiQuantile(0.9, perSeries[key]))
				}
				out := map[string]float64{}
				for g, vs := range perGroup {
					out[g] = merge(tc.op, vs)
				}
				return out
			}
			for _, shape := range []struct{ window, step time.Duration }{{time.Minute, time.Minute}, {3 * time.Minute, time.Minute}} {
				query := fmt.Sprintf(tc.query, fmt.Sprintf(`quantile_over_time(0.9, %s [%ds])`, pipeline, int(shape.window.Seconds())))
				start, end := base.Add(-8*time.Minute), base.Add(8*time.Minute)
				params := url.Values{"query": {query}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}, "step": {strconv.Itoa(int(shape.step.Seconds()))}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusOK {
					t.Fatalf("%s: %d %s", query, rec.Code, rec.Body.String())
				}
				var resp struct {
					Data struct {
						Result []struct {
							Metric map[string]string `json:"metric"`
							Values [][]any           `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				got := map[int64]map[string]float64{}
				for _, s := range resp.Data.Result {
					for _, point := range s.Values {
						ts := int64(point[0].(float64))
						v, _ := strconv.ParseFloat(point[1].(string), 64)
						if got[ts] == nil {
							got[ts] = map[string]float64{}
						}
						got[ts][tc.answerKey(s.Metric)] = v
					}
				}
				for at := start; !at.After(end); at = at.Add(shape.step) {
					want := reference(at, shape.window)
					if len(got[at.Unix()]) != len(want) {
						t.Fatalf("%s t=%v: got %v, want %v", query, at, got[at.Unix()], want)
					}
					for key, w := range want {
						if g, ok := got[at.Unix()][key]; !ok || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
							t.Fatalf("%s t=%v %q: got %v, want %v", query, at, key, g, w)
						}
					}
				}
			}
			query := fmt.Sprintf(tc.query, fmt.Sprintf(`quantile_over_time(0.9, %s [2m])`, pipeline))
			evalTS := base.Add(17 * time.Second)
			params := url.Values{"query": {query}, "time": {evalTS.Format(time.RFC3339Nano)}}
			rec := httptest.NewRecorder()
			p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Value  []any             `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); rec.Code != http.StatusOK || err != nil {
				t.Fatalf("instant %s: %d %s", query, rec.Code, rec.Body.String())
			}
			want := reference(evalTS, 2*time.Minute)
			if len(resp.Data.Result) != len(want) {
				t.Fatalf("instant %s: %s, want %v", query, rec.Body.String(), want)
			}
			for _, s := range resp.Data.Result {
				v, _ := strconv.ParseFloat(s.Value[1].(string), 64)
				if w, ok := want[tc.answerKey(s.Metric)]; !ok || math.Abs(v-w) > 1e-9*math.Max(1, math.Abs(w)) {
					t.Fatalf("instant %s %v: got %v, want %v", query, s.Metric, v, w)
				}
			}
		}
	}
	if q := emu.endpointCalls("/select/logsql/query"); len(q) != 0 {
		t.Fatalf("quantile read raw rows: %d queries", len(q))
	}
}
