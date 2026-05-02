package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newOpt6Proxy(t testing.TB, backendURL string, streamFields []string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             backendURL,
		Cache:                  cache.New(0, 0),
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
		StreamFields:           streamFields,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

func TestOPT6_BuildSlidingWindowSums_CountOverTime(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9   // 1m
	rangeNs := int64(180) * 1e9 // 3m

	buckets := map[string]map[int64]int64{
		`app="svc"`: {
			t0:            10,
			t0 + stepNs:   20,
			t0 + 2*stepNs: 30,
			t0 + 3*stepNs: 40,
			t0 + 4*stepNs: 50,
		},
	}
	labelSets := map[string]map[string]string{
		`app="svc"`: {"app": "svc"},
	}

	evalStart := t0 + 3*stepNs
	evalEnd := t0 + 4*stepNs

	series := buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalEnd, stepNs, rangeNs, false)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	// Eval at t0+3m: buckets in [t0, t0+3m) → t0, t0+1m, t0+2m = 10+20+30=60
	// Eval at t0+4m: buckets in [t0+1m, t0+4m) → 20+30+40=90
	want := []struct {
		ts    int64
		value float64
	}{
		{evalStart, 60},
		{evalEnd, 90},
	}
	if len(series[0].samples) != len(want) {
		t.Fatalf("expected %d samples, got %d", len(want), len(series[0].samples))
	}
	for i, w := range want {
		if series[0].samples[i].tsNanos != w.ts {
			t.Errorf("sample[%d].ts=%d want %d", i, series[0].samples[i].tsNanos, w.ts)
		}
		if math.Abs(series[0].samples[i].value-w.value) > 1e-9 {
			t.Errorf("sample[%d].value=%g want %g", i, series[0].samples[i].value, w.value)
		}
	}
}

func TestOPT6_BuildSlidingWindowSums_Rate(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(300) * 1e9 // 5m = 300s

	buckets := map[string]map[int64]int64{
		`app="api"`: {t0: 300},
	}
	labelSets := map[string]map[string]string{
		`app="api"`: {"app": "api"},
	}

	evalStart := t0 + 5*stepNs
	series := buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalStart, stepNs, rangeNs, true)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	// rate = 300 count / 300s window = 1.0
	wantRate := 1.0
	if math.Abs(series[0].samples[0].value-wantRate) > 1e-9 {
		t.Errorf("rate=%g want %g", series[0].samples[0].value, wantRate)
	}
}

func TestOPT6_BuildSlidingWindowSums_EmptyBuckets(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(180) * 1e9

	series := buildSlidingWindowSumsFromHits(
		map[string]map[int64]int64{},
		map[string]map[string]string{},
		t0, t0+2*stepNs, stepNs, rangeNs, false,
	)
	if len(series) != 0 {
		t.Errorf("expected 0 series for empty data, got %d", len(series))
	}
}

func TestOPT6_BuildSlidingWindowSums_AbsentPoints(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(60) * 1e9

	// Only one bucket has data at t0; eval at t0+2*stepNs is outside its window.
	buckets := map[string]map[int64]int64{
		`app="a"`: {t0: 5},
	}
	labelSets := map[string]map[string]string{
		`app="a"`: {"app": "a"},
	}

	// eval at t0+stepNs: window [t0, t0+stepNs) → bucket t0 included → sum=5
	// eval at t0+2*stepNs: window [t0+stepNs, t0+2*stepNs) → bucket t0 excluded → sum=0 → absent
	series := buildSlidingWindowSumsFromHits(buckets, labelSets, t0+stepNs, t0+2*stepNs, stepNs, rangeNs, false)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	if len(series[0].samples) != 1 {
		t.Errorf("expected 1 sample (absent point omitted), got %d", len(series[0].samples))
	}
	if series[0].samples[0].value != 5 {
		t.Errorf("expected value 5, got %g", series[0].samples[0].value)
	}
}

func TestOPT6_BuildSlidingWindowSums_MultiStream(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(60) * 1e9

	buckets := map[string]map[int64]int64{
		`app="a"`: {t0: 5},
		`app="b"`: {t0: 15},
	}
	labelSets := map[string]map[string]string{
		`app="a"`: {"app": "a"},
		`app="b"`: {"app": "b"},
	}

	series := buildSlidingWindowSumsFromHits(buckets, labelSets, t0+stepNs, t0+stepNs, stepNs, rangeNs, false)
	if len(series) != 2 {
		t.Errorf("expected 2 series, got %d", len(series))
	}
}

func TestOPT6_SlidingWindowGate_UsesHitsEndpoint(t *testing.T) {
	hitsCalled := false
	queryCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalled = true
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"hits":[]}`)
		case "/select/logsql/query":
			queryCalled = true
			http.Error(w, "should not reach slow path", 500)
		default:
			http.Error(w, "unexpected: "+r.URL.Path, 404)
		}
	}))
	defer srv.Close()

	// With StreamFields set, declaredLabelFields is populated → hits gate activates.
	p := newOpt6Proxy(t, srv.URL, []string{"app", "namespace"})

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+url.Values{
		"query": {`rate({namespace="prod"} | json [5m])`},
		"start": {"1746100000000000000"},
		"end":   {"1746103600000000000"}, // 1h window
		"step":  {"60000000000"},         // 1m step in nanoseconds → sliding: 5m > 1m
	}.Encode(), nil)
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)

	if queryCalled {
		t.Error("slow full-fetch path was used — hits gate did not activate")
	}
	if !hitsCalled {
		t.Error("hits endpoint was NOT called — sliding window optimisation is inactive")
	}
}

func TestOPT6_SlidingWindowGate_FallsBackWithoutDeclaredFields(t *testing.T) {
	queryCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			t.Error("hits must not be called when declaredLabelFields is nil")
			w.WriteHeader(500)
		case "/select/logsql/query":
			queryCalled = true
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, "") // empty — no entries
		default:
			http.Error(w, "unexpected: "+r.URL.Path, 404)
		}
	}))
	defer srv.Close()

	// No StreamFields → declaredLabelFields is nil → fallback to full-fetch.
	p := newOpt6Proxy(t, srv.URL, nil)

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+url.Values{
		"query": {`rate({namespace="prod"} | json [5m])`},
		"start": {"1746100000000000000"},
		"end":   {"1746103600000000000"},
		"step":  {"60000000000"},
	}.Encode(), nil)
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)

	if !queryCalled {
		t.Error("expected fallback to full-fetch path when declaredLabelFields is nil")
	}
}

func TestOPT6_FetchBareParserMetricSeriesViaHits_Integration(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t0 := now.Add(-3 * time.Minute)

	makeTS := func(offset time.Duration) string {
		return fmt.Sprintf("%d", t0.Add(offset).UnixNano())
	}

	hitsResp := map[string]interface{}{
		"hits": []map[string]interface{}{
			{
				"fields":     map[string]string{"app": "api-gw"},
				"timestamps": []string{makeTS(0), makeTS(time.Minute), makeTS(2 * time.Minute)},
				"values":     []int{10, 20, 30},
			},
			{
				"fields":     map[string]string{"app": "auth"},
				"timestamps": []string{makeTS(0), makeTS(time.Minute)},
				"values":     []int{5, 5},
			},
		},
	}
	hitsJSON, _ := json.Marshal(hitsResp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			http.Error(w, "unexpected: "+r.URL.Path, 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(hitsJSON)
	}))
	defer srv.Close()

	p := newOpt6Proxy(t, srv.URL, []string{"app", "namespace"})

	spec := bareParserMetricCompatSpec{
		funcName:        "count_over_time",
		baseQuery:       `{namespace="prod"} | json`,
		rangeWindow:     3 * time.Minute,
		rangeWindowExpr: "3m",
	}

	evalStart := t0.UnixNano()
	evalEnd := now.UnixNano()
	stepNs := int64(60) * int64(time.Second)

	series, err := p.fetchBareParserMetricSeriesViaHits(t.Context(), spec, evalStart, evalEnd, stepNs)
	if err != nil {
		t.Fatalf("fetchBareParserMetricSeriesViaHits: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 series (api-gw, auth), got %d", len(series))
	}
	for _, s := range series {
		if len(s.samples) == 0 {
			t.Errorf("series %v has no samples", s.metric)
		}
	}
}
