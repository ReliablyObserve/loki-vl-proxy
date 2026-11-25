package metrics

import (
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: observability-metrics-histograms-and-labels
func TestDefaultDurationBuckets(t *testing.T) {
	want := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 45, 60, 90, 120}
	got := DefaultDurationBuckets()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults = %v, want %v", got, want)
	}
	got[0] = 99
	if DefaultDurationBuckets()[0] != 0.001 {
		t.Fatal("DefaultDurationBuckets shares its slice")
	}
	// The earlier bounds stay, so recorded series and queries on le="10" keep working.
	if !reflect.DeepEqual(want[:len(defaultBuckets)], defaultBuckets) {
		t.Fatalf("the duration bounds dropped an earlier one: %v", defaultBuckets)
	}
	if parsed, err := ParseDurationBuckets(boundsText(want)); err != nil || !reflect.DeepEqual(parsed, want) {
		t.Fatalf("the defaults do not pass their own validation: %v %v", parsed, err)
	}
}

func boundsText(b []float64) string {
	s := make([]string, len(b))
	for i, v := range b {
		s[i] = strconv.FormatFloat(v, 'g', -1, 64)
	}
	return strings.Join(s, ",")
}

// conformance: observability-metrics-histograms-and-labels
func TestParseDurationBuckets(t *testing.T) {
	ascending := func(n int) string {
		b := make([]float64, n)
		for i := range b {
			b[i] = float64(i + 1)
		}
		return boundsText(b)
	}
	for _, tc := range []struct {
		name, raw string
		want      []float64
		errPart   string
	}{
		{name: "empty", raw: "", want: DefaultDurationBuckets()},
		{name: "blank", raw: "  ", want: DefaultDurationBuckets()},
		{name: "one", raw: "60", want: []float64{60}},
		{name: "spaces and forms", raw: " 0.005, 1 ,6e1,120.5 ", want: []float64{0.005, 1, 60, 120.5}},
		{name: "most", raw: ascending(maxDurationBuckets), want: mustBounds(ascending(maxDurationBuckets))},
		{name: "too many", raw: ascending(maxDurationBuckets + 1), errPart: "at most 32"},
		{name: "huge list", raw: strings.Repeat(",", 1<<20), errPart: "at most 32"},
		{name: "zero", raw: "0,1", errPart: "above zero"},
		{name: "negative", raw: "-1,1", errPart: "above zero"},
		{name: "nan", raw: "1,NaN", errPart: "finite"},
		{name: "plus inf", raw: "1,+Inf", errPart: "finite"},
		{name: "out of range", raw: "1,1e999", errPart: "not a number"},
		{name: "descending", raw: "10,5", errPart: "ascending"},
		{name: "repeated", raw: "5,5", errPart: "ascending"},
		{name: "same value twice spelled apart", raw: "60,6e1", errPart: "ascending"},
		{name: "text", raw: "1,ten", errPart: "not a number"},
		{name: "duration unit", raw: "30s", errPart: "not a number"},
		{name: "trailing comma", raw: "1,2,", errPart: "not a number"},
		{name: "empty entry", raw: "1,,2", errPart: "not a number"},
		{name: "semicolons", raw: "1;2", errPart: "not a number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDurationBuckets(tc.raw)
			if tc.errPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errPart) {
					t.Fatalf("error = %v, want one holding %q", err, tc.errPart)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

func mustBounds(raw string) []float64 {
	var out []float64
	for _, p := range strings.Split(raw, ",") {
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			panic(err)
		}
		out = append(out, v)
	}
	return out
}

func FuzzParseDurationBuckets(f *testing.F) {
	for _, seed := range []string{"", "1,2,3", "0.001,120", "NaN", "1,,2", "-0", "1e308,1e309", "0x1p4"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseDurationBuckets(raw)
		if err != nil {
			return
		}
		if len(got) == 0 || len(got) > maxDurationBuckets {
			t.Fatalf("%q: %d bounds accepted", raw, len(got))
		}
		for i, v := range got {
			if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || (i > 0 && v <= got[i-1]) {
				t.Fatalf("%q: accepted bounds %v", raw, got)
			}
		}
	})
}

// conformance: observability-metrics-histograms-and-labels
func TestHistogramObserveBounds(t *testing.T) {
	for _, tc := range []struct {
		v    float64
		want []int64 // cumulative counts for the bounds 1, 10, 60
	}{
		{v: -1, want: []int64{1, 1, 1}},
		{v: 0, want: []int64{1, 1, 1}},
		{v: 1, want: []int64{1, 1, 1}},
		{v: math.Nextafter(1, 2), want: []int64{0, 1, 1}},
		{v: 10, want: []int64{0, 1, 1}},
		{v: 60, want: []int64{0, 0, 1}},
		{v: 60.001, want: []int64{0, 0, 0}},
		{v: math.Inf(1), want: []int64{0, 0, 0}},
		{v: math.NaN(), want: []int64{0, 0, 0}},
	} {
		h := newHistogramWithBuckets([]float64{1, 10, 60})
		h.observe(tc.v)
		if got := h.snapshot().counts; !slices.Equal(got, tc.want) || h.count != 1 {
			t.Errorf("observe(%v): counts %v count %d, want %v and 1", tc.v, got, h.count, tc.want)
		}
	}
}

func metricsText(m *Metrics) string {
	rec := httptest.NewRecorder()
	m.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// bucketBounds maps each histogram family of an exposition to the le values of its first series.
func bucketBounds(body string) map[string][]string {
	bounds, series := map[string][]string{}, map[string]string{}
	for _, line := range strings.Split(body, "\n") {
		name, rest, ok := strings.Cut(line, "_bucket{")
		if !ok {
			continue
		}
		labels, le, _ := strings.Cut(rest, `le="`)
		le, _, _ = strings.Cut(le, `"`)
		if first, seen := series[name]; seen && first != labels {
			continue
		}
		series[name] = labels
		bounds[name] = append(bounds[name], le)
	}
	return bounds
}

func recordEveryHistogram(m *Metrics, d time.Duration) {
	m.RecordRequestWithRoute("query_range", "", 200, d)
	m.RecordBackendDurationWithRoute("select_logsql_query", "/select/logsql/query", d)
	m.RecordTenantRequestWithRoute("team-a", "query_range", "", 200, d)
	m.RecordClientIdentityWithRoute("grafana", "query_range", "", d, 1)
	m.RecordClientQueryLengthWithRoute("grafana", "query_range", "", 40)
	m.RecordInternalOperation("translate_query", "translated", d)
	m.RecordRequestFlow("query_range", "/loki/api/v1/query_range", RequestFlow{VLCalls: 1, VL: d})
	m.RecordCacheLookup("l1_memory", "hit", time.Millisecond)
	m.RecordQueryRangeWindowFetchDuration(d)
	m.RecordQueryRangeWindowMergeDuration(d)
	m.RecordQueryRangeWindowPrefilterDuration(d)
	m.RecordQueryRangeWindowCount(3)
	m.RecordUpstreamCallsPerRequest("query_range", 2)
}

var durationFamilies = []string{
	"loki_vl_proxy_request_duration_seconds",
	"loki_vl_proxy_backend_duration_seconds",
	"loki_vl_proxy_tenant_request_duration_seconds",
	"loki_vl_proxy_client_request_duration_seconds",
	"loki_vl_proxy_request_phase_seconds",
	"loki_vl_proxy_internal_operation_duration_seconds",
	"loki_vl_proxy_window_fetch_seconds",
	"loki_vl_proxy_window_merge_seconds",
	"loki_vl_proxy_window_prefilter_duration_seconds",
}

// conformance: observability-metrics-histograms-and-labels
func TestDurationHistogramsExposeTheBuckets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		buckets []float64
		want    []float64
	}{
		{name: "defaults", want: DefaultDurationBuckets()},
		{name: "configured", buckets: []float64{0.5, 30, 300}, want: []float64{0.5, 30, 300}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetricsWithDurationBuckets(256, 256, true, tc.buckets)
			recordEveryHistogram(m, 19*time.Second)
			bounds := bucketBounds(metricsText(m))
			text := func(b []float64) []string { return append(strings.Split(boundsText(b), ","), "+Inf") }
			for _, family := range durationFamilies {
				if want := text(tc.want); !slices.Equal(bounds[family], want) {
					t.Errorf("%s: le = %v, want %v", family, bounds[family], want)
				}
			}
			// Histograms that do not time a request keep their own bounds whatever is configured.
			for family, want := range map[string][]float64{
				"loki_vl_proxy_client_query_length_chars":  defaultBuckets,
				"loki_vl_proxy_window_count":               defaultBuckets,
				"loki_vl_proxy_upstream_calls_per_request": countBuckets,
				"loki_vl_proxy_request_fanout":             flowFanoutBuckets,
				"loki_vl_proxy_cache_lookup_seconds":       cacheLookupBuckets,
			} {
				got := bounds[family]
				if len(got) != len(want)+1 {
					t.Errorf("%s: le = %v, want its own %d bounds", family, got, len(want))
				}
			}
		})
	}
}

// The incident's requests (12 s, 19 s, 29 s and a 60 s VictoriaLogs timeout) must fall into different buckets.
// conformance: observability-metrics-histograms-and-labels
func TestSlowRequestsAreToldApart(t *testing.T) {
	m := NewMetricsWithOptions(256, 256, false)
	for _, d := range []time.Duration{12 * time.Second, 19 * time.Second, 29 * time.Second, 59999 * time.Millisecond, 130 * time.Second} {
		m.RecordRequestWithRoute("query_range", "", 200, d)
		m.RecordBackendDurationWithRoute("select_logsql_query", "/select/logsql/query", d)
	}
	body := metricsText(m)
	for le, count := range map[string]int{"10": 0, "15": 1, "20": 2, "30": 3, "45": 3, "60": 4, "90": 4, "120": 4, "+Inf": 5} {
		for _, series := range []string{
			`loki_vl_proxy_request_duration_seconds_bucket{system="loki",direction="downstream",endpoint="query_range",route="/loki/api/v1/query_range",le="`,
			`loki_vl_proxy_backend_duration_seconds_bucket{system="vl",direction="upstream",endpoint="select_logsql_query",route="/select/logsql/query",le="`,
		} {
			if line := series + le + `"} ` + strconv.Itoa(count) + "\n"; !strings.Contains(body, line) {
				t.Errorf("missing %s", line)
			}
		}
	}
}

// Many observations read back as cumulative buckets on /metrics and as per-bucket counts on OTLP.
func TestHistogramCumulativeAndPerBucketCounts(t *testing.T) {
	h := newHistogramWithBuckets([]float64{1, 10, 60})
	for _, v := range []float64{0.5, 1, 2, 30, 30, 61, 1000} {
		h.observe(v)
	}
	if got := h.snapshot().counts; !slices.Equal(got, []int64{2, 3, 5}) {
		t.Fatalf("cumulative counts %v", got)
	}
	if got := h.snapshot().counts; !slices.Equal(got, []int64{2, 3, 5}) {
		t.Fatalf("a snapshot changed the histogram: %v", got)
	}
	dp := NewOTLPPusher(OTLPConfig{}, NewMetrics()).histogramDP(h, 1)
	if got := dp["bucketCounts"]; !reflect.DeepEqual(got, []int64{2, 1, 2, 2}) {
		t.Fatalf("per-bucket counts %v", got)
	}
}

func TestWriteBucketLines(t *testing.T) {
	var sb strings.Builder
	writeBucketLines(&sb, "x_seconds", `a="b"`, histogramSnapshot{count: 4, buckets: []float64{0.00001, 2.5, 1e21}, counts: []int64{0, 2, 3}})
	want := "x_seconds_bucket{a=\"b\",le=\"1e-05\"} 0\nx_seconds_bucket{a=\"b\",le=\"2.5\"} 2\n" +
		"x_seconds_bucket{a=\"b\",le=\"1e+21\"} 3\nx_seconds_bucket{a=\"b\",le=\"+Inf\"} 4\n"
	if sb.String() != want {
		t.Fatalf("got %q, want %q", sb.String(), want)
	}
	sb.Reset()
	writeHistogramSeries(&sb, "y_seconds", "", histogramSnapshot{sum: 1.5, count: 1, buckets: []float64{2}, counts: []int64{1}})
	if want := "y_seconds_bucket{le=\"2\"} 1\ny_seconds_bucket{le=\"+Inf\"} 1\ny_seconds_sum 1.5\ny_seconds_count 1\n"; sb.String() != want {
		t.Fatalf("without labels: got %q, want %q", sb.String(), want)
	}
}

// conformance: observability-metrics-histograms-and-labels
func TestOTLPDurationHistogramsUseTheBuckets(t *testing.T) {
	m := NewMetricsWithDurationBuckets(256, 256, false, []float64{30, 60})
	m.RecordBackendDurationWithRoute("select_logsql_query", "/select/logsql/query", 45*time.Second)
	p := NewOTLPPusher(OTLPConfig{}, m)
	dp := p.histogramDP(m.backendDurations[joinMetricKey("select_logsql_query", "/select/logsql/query")], 1)
	if !reflect.DeepEqual(dp["explicitBounds"], []float64{30, 60}) || !reflect.DeepEqual(dp["bucketCounts"], []int64{0, 1, 0}) {
		t.Fatalf("bounds %v counts %v", dp["explicitBounds"], dp["bucketCounts"])
	}
}
