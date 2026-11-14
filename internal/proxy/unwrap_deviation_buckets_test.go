package proxy

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The window of stddev_over_time and stdvar_over_time with a range longer than the step is merged from per-bucket
// partials (deviationBucketStats: the mean, the sample count and the population standard deviation of each bucket,
// as VictoriaLogs computes them): these tests hold the merge to the population variance of the window's samples
// taken directly, and the pairing of VictoriaLogs' stats results to the bucket and group each belongs to.

// bucketPartial is what VictoriaLogs answers for the samples of one bucket: their mean, count and population
// standard deviation (lib/logstorage statsAvg, statsCount, statsStddev).
func bucketPartial(samples []float64) (mean, n, dev float64) {
	n = float64(len(samples))
	for _, v := range samples {
		mean += v
	}
	mean /= n
	var q float64
	for _, v := range samples {
		q += (v - mean) * (v - mean)
	}
	return mean, n, math.Sqrt(q / n)
}

// directVariance is the population variance of the samples, two-pass.
func directVariance(samples []float64) float64 {
	var mean float64
	for _, v := range samples {
		mean += v
	}
	mean /= float64(len(samples))
	var q float64
	for _, v := range samples {
		q += (v - mean) * (v - mean)
	}
	return q / float64(len(samples))
}

// partialsOf is the buckets' partials as the window evaluator reads them.
func partialsOf(buckets [][]float64) (means, counts, devs []rangeMetricSample) {
	for i, b := range buckets {
		m, n, d := bucketPartial(b)
		ts := int64(i) * int64(time.Minute)
		means = append(means, rangeMetricSample{ts: ts, value: m})
		counts = append(counts, rangeMetricSample{ts: ts, value: n})
		devs = append(devs, rangeMetricSample{ts: ts, value: d})
	}
	return means, counts, devs
}

func closeTo(got, want float64) bool {
	if math.IsNaN(want) {
		return math.IsNaN(got)
	}
	if math.IsInf(want, 0) {
		return got == want
	}
	return math.Abs(got-want) <= 1e-9*math.Max(1, math.Max(math.Abs(got), math.Abs(want)))
}

// conformance: semantics/stddev-stdvar-over-time-range-window
func TestMergeDeviationBucketsIsTheWindowsVariance(t *testing.T) {
	for _, c := range []struct {
		name    string
		buckets [][]float64
		want    float64 // NaN: the window is NaN
	}{
		{name: "one bucket, one sample", buckets: [][]float64{{42}}},
		{name: "one bucket", buckets: [][]float64{{1, 2, 3, 4}}},
		{name: "buckets of one sample each", buckets: [][]float64{{3}, {0.25}, {-2}, {1e3}, {42}}},
		{name: "uneven buckets", buckets: [][]float64{{1}, {5, 6, 7, 8, 9}, {100, 101}}},
		{name: "equal samples", buckets: [][]float64{{7, 7}, {7}, {7, 7, 7}}},
		{name: "large mean, small spread", buckets: [][]float64{{1e9 + 1, 1e9 + 2}, {1e9 + 3}, {1e9 + 4, 1e9 + 5}}},
		{name: "negative and fractional", buckets: [][]float64{{-1.5, -0.25}, {0.125, 2.75}, {-3}}},
		{name: "an infinity", buckets: [][]float64{{1, 2}, {math.Inf(1)}}, want: math.NaN()},
		{name: "a NaN", buckets: [][]float64{{1, 2}, {math.NaN(), 3}}, want: math.NaN()},
	} {
		t.Run(c.name, func(t *testing.T) {
			means, counts, devs := partialsOf(c.buckets)
			var all []float64
			for _, b := range c.buckets {
				all = append(all, b...)
			}
			want := c.want
			if !math.IsNaN(want) {
				want = directVariance(all)
			}
			if got := mergeDeviationBuckets(means, counts, devs); !closeTo(got, want) {
				t.Fatalf("variance %v, want %v", got, want)
			}
			if got := mergeUnwrapBuckets("stdvar", means, counts, devs); !closeTo(got, want) {
				t.Fatalf("stdvar %v, want %v", got, want)
			}
			if got := mergeUnwrapBuckets("stddev", means, counts, devs); !closeTo(got, math.Sqrt(want)) {
				t.Fatalf("stddev %v, want %v", got, math.Sqrt(want))
			}
		})
	}
	t.Run("partials that do not line up are NaN", func(t *testing.T) {
		means, counts, devs := partialsOf([][]float64{{1, 2}, {3}})
		for _, c := range [][3][]rangeMetricSample{
			{means[:1], counts, devs}, {means, counts[:1], devs}, {means, counts, devs[:1]}, {nil, nil, nil},
		} {
			if got := mergeDeviationBuckets(c[0], c[1], c[2]); !math.IsNaN(got) {
				t.Fatalf("got %v, want NaN", got)
			}
		}
	})
	t.Run("the other functions keep their merge", func(t *testing.T) {
		b := []rangeMetricSample{{ts: 0, value: 2}, {ts: 1, value: 5}}
		c := []rangeMetricSample{{ts: 0, value: 1}, {ts: 1, value: 2}}
		for fn, want := range map[string]float64{"sum": 7, "max": 5, "min": 2, "avg": 7.0 / 3} {
			if got := mergeUnwrapBuckets(fn, b, c, nil); !closeTo(got, want) {
				t.Fatalf("%s %v, want %v", fn, got, want)
			}
		}
	})
}

// statsResultMetric is a stats_query_range result's metric object as VictoriaLogs writes it.
func statsResultMetric(t *testing.T, raw string) *fj.Object {
	t.Helper()
	v, err := fj.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	o, err := v.Object()
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// bucketValues writes the points of one stats result: a value per bucket start (seconds from start).
func bucketValues(start time.Time, points map[int]float64) []byte {
	var b strings.Builder
	b.WriteByte('[')
	first := true
	for i := 0; i < 64; i++ {
		v, ok := points[i]
		if !ok {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString("[" + strconv.FormatInt(start.Add(time.Duration(i)*time.Minute).Unix(), 10) + `,"` + strconv.FormatFloat(v, 'g', -1, 64) + `"]`)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

// conformance: semantics/stddev-stdvar-over-time-range-window
func TestDeviationPartialsPairByGroupAndBucket(t *testing.T) {
	start := time.Unix(1_767_222_000, 0)
	// Two VictoriaLogs groups of one Loki series (the unwrap gate's value kinds), and a series of its own; the
	// results come in any order, the deviation before the mean.
	type result struct {
		metric string
		points map[int]float64
	}
	groupA := []float64{1, 2, 3}  // bucket 0
	groupB := []float64{10, 20}   // bucket 0, the other group
	groupA1 := []float64{4, 4, 7} // bucket 1
	mA, nA, dA := bucketPartial(groupA)
	mB, nB, dB := bucketPartial(groupB)
	mA1, nA1, dA1 := bucketPartial(groupA1)
	results := []result{
		{`{"__name__":"__lvp_sd","app":"api","__lvp_bad":"0"}`, map[int]float64{0: dA, 1: dA1}},
		{`{"__name__":"__lvp_avg","app":"api","__lvp_bad":"2"}`, map[int]float64{0: mB}},
		{`{"__name__":"__sample_count","app":"api","__lvp_bad":"0"}`, map[int]float64{0: nA, 1: nA1}},
		{`{"__name__":"__lvp_avg","app":"api","__lvp_bad":"0"}`, map[int]float64{1: mA1, 0: mA}},
		{`{"__name__":"__sample_count","app":"api","__lvp_bad":"2"}`, map[int]float64{0: nB}},
		{`{"__name__":"__lvp_sd","app":"api","__lvp_bad":"2"}`, map[int]float64{0: dB}},
		{`{"__name__":"__sample_count","app":"web","__lvp_bad":"0"}`, map[int]float64{3: 1}},
		{`{"__name__":"__lvp_avg","app":"web","__lvp_bad":"0"}`, map[int]float64{3: 5}},
		// A bucket whose deviation is missing: NaN, never no sample.
		{`{"__name__":"__lvp_sd","app":"web","__lvp_bad":"0"}`, map[int]float64{}},
	}
	var d deviationPartials
	for _, r := range results {
		o := statsResultMetric(t, r.metric)
		app := string(o.Get("app").GetStringBytes())
		d.add("app="+app, statsGroupID(o), map[string]string{"app": app}, string(o.Get("__name__").GetStringBytes()), bucketValues(start, r.points), start, time.Minute)
	}
	series := map[string]manualSeriesSamples{}
	var none *deviationPartials
	none.fold(series) // no deviation stats: nothing to fold
	d.fold(series)
	api, web := series["app=api"], series["app=web"]
	if len(api.Samples) != 2 || len(api.Counts) != 2 || len(api.Deviations) != 2 || len(api.PresentBuckets) != 2 {
		t.Fatalf("api: %+v", api)
	}
	if got, want := mergeDeviationBuckets(api.Samples[:1], api.Counts[:1], api.Deviations[:1]), directVariance(append(append([]float64{}, groupA...), groupB...)); !closeTo(got, want) {
		t.Fatalf("bucket 0 of two groups: variance %v, want %v", got, want)
	}
	all := append(append(append([]float64{}, groupA...), groupB...), groupA1...)
	if got, want := mergeDeviationBuckets(api.Samples, api.Counts, api.Deviations), directVariance(all); !closeTo(got, want) {
		t.Fatalf("window of both buckets: variance %v, want %v", got, want)
	}
	if api.Metric["app"] != "api" || api.Samples[0].ts >= api.Samples[1].ts {
		t.Fatalf("api series %+v", api)
	}
	if len(web.Samples) != 1 || !math.IsNaN(web.Deviations[0].value) || web.Counts[0].value != 1 {
		t.Fatalf("web: a bucket without its deviation must be NaN: %+v", web)
	}
}

// FuzzMergeDeviationBuckets: the samples of a window split into buckets any way merge to the window's population
// variance taken directly, within float rounding, and never panic on partials that do not line up.
//
// conformance: semantics/stddev-stdvar-over-time-range-window
func FuzzMergeDeviationBuckets(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, uint8(3), int16(0))
	f.Add([]byte{0, 0, 0, 255}, uint8(1), int16(1000))
	f.Add([]byte{9}, uint8(7), int16(-30000))
	f.Add([]byte{}, uint8(0), int16(5))
	f.Fuzz(func(t *testing.T, raw []byte, split uint8, offset int16) {
		if len(raw) > 512 {
			raw = raw[:512]
		}
		var buckets [][]float64
		var all []float64
		size := int(split%7) + 1
		for i := 0; i < len(raw); i += size {
			var b []float64
			for _, x := range raw[i:min(i+size, len(raw))] {
				// A spread of a few hundred around an offset up to 3e7: exact in float64, so only the order of the
				// sums differs between the merge and the direct variance.
				v := float64(offset)*1000 + float64(x) - 128 + float64(x%4)/4
				b = append(b, v)
				all = append(all, v)
			}
			buckets = append(buckets, b)
		}
		means, counts, devs := partialsOf(buckets)
		got := mergeDeviationBuckets(means, counts, devs)
		if len(all) == 0 {
			if !math.IsNaN(got) {
				t.Fatalf("no bucket: %v, want NaN", got)
			}
			return
		}
		want := directVariance(all)
		if math.Abs(got-want) > 1e-9*math.Max(1, want) {
			t.Fatalf("variance %v, want %v (buckets %v)", got, want, buckets)
		}
		if len(means) > 1 {
			if got := mergeDeviationBuckets(means[1:], counts, devs); !math.IsNaN(got) {
				t.Fatalf("partials that do not line up: %v, want NaN", got)
			}
		}
	})
}

// The window evaluator of stddev and stdvar over time leaves an aggregation over a range aggregation with its own by
// on the step buckets: the folds take no grouped range aggregation (semantics/aggregation-over-range-by-not-applied).
// unwrapWindowForm reports it beside the forms the evaluator applies after the window.
//
// conformance: semantics/aggregation-over-range-by-not-applied, semantics/stddev-stdvar-over-time-range-window
func TestUnwrapWindowFormRangeBy(t *testing.T) {
	for _, c := range []struct {
		q             string
		form, rangeBy bool
	}{
		{`max(stddev_over_time({a="b"} | json | unwrap n [5m]) by (pod))`, true, true},
		{`sum by (app) (stdvar_over_time({a="b"} | unwrap n [5m]) by (app, pod))`, true, true},
		{`label_replace(max(stddev_over_time({a="b"} | unwrap n [5m]) by (pod)), "x", "$1", "pod", "(.*)")`, true, true},
		{`max(sum by (app) (stddev_over_time({a="b"} | unwrap n [5m]) by (app, pod)))`, true, true},
		{`stddev_over_time({a="b"} | unwrap n [5m]) by (pod)`, true, false},
		{`max by (pod) (stddev_over_time({a="b"} | unwrap n [5m]))`, true, false},
		{`topk(1, stddev_over_time({a="b"} | unwrap n [5m]) by (pod))`, true, false},
		{`sum(sum(stddev_over_time({a="b"} | unwrap n [5m]) by (pod)))`, false, true},
		{`max(stddev_over_time({a="b"} | unwrap n [5m]) by (pod)) / 2`, false, false},
		{`not a query`, false, false},
		{``, false, false},
	} {
		if form, rangeBy := unwrapWindowForm(c.q); form != c.form || rangeBy != c.rangeBy {
			t.Errorf("%s: form %v rangeBy %v, want %v %v", c.q, form, rangeBy, c.form, c.rangeBy)
		}
	}
}

// FuzzUnwrapWindowForm: any text terminates without a panic, and a query reported with a grouped range aggregation
// under aggregations parses to a vector aggregation (under label_replace).
//
// conformance: semantics/aggregation-over-range-by-not-applied
func FuzzUnwrapWindowForm(f *testing.F) {
	for _, seed := range []string{
		`max(stddev_over_time({a="b"} | json | unwrap n [5m]) by (pod))`,
		`sum by (app) (stdvar_over_time({a="b"} | unwrap n [5m]) by (app, pod))`,
		`stddev_over_time({a="b"} | unwrap n [5m]) by (pod)`,
		`max(((stddev_over_time({a="b"} | unwrap n [5m]) by (pod))))`,
		`topk(2, avg without (pod) (avg_over_time({a="b"} | unwrap n [5m])))`,
		`label_replace(sum(rate({a="b"}[5m]) by (x)), "y", "$1", "x", "(.*)")`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 4096 {
			return
		}
		if _, rangeBy := unwrapWindowForm(q); !rangeBy {
			return
		}
		expr, err := parseLogQLExpr(noFlowCtx, stripOuterLabelReplace(strings.TrimSpace(q)))
		if err != nil {
			t.Fatalf("%q reported but does not parse: %v", q, err)
		}
		if _, ok := expr.(*logqlpkg.VectorAggregation); !ok {
			t.Fatalf("%q reported but is %T", q, expr)
		}
	})
}
