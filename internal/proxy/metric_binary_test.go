package proxy

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestParseTopKWrapper(t *testing.T) {
	tests := []struct {
		name     string
		logql    string
		wantK    int
		wantDesc bool
		wantOK   bool
	}{
		{"topk basic", `topk(5, sum by (app) (rate({app="nginx"}[5m])))`, 5, true, true},
		{"bottomk basic", `bottomk(3, sum by (app) (count_over_time({app="nginx"}[5m])))`, 3, false, true},
		{"topk k=1", `topk(1, rate({app="nginx"}[5m]))`, 1, true, true},
		{"not topk — sum", `sum by (app) (rate({app="nginx"}[5m]))`, 0, false, false},
		{"not topk — rate bare", `rate({app="nginx"}[5m])`, 0, false, false},
		{"not topk — binary op", `topk(5, rate({a="b"}[5m])) / 2`, 0, false, false},
		{"topk k=0 invalid", `topk(0, rate({a="b"}[5m]))`, 0, false, false},
		{"empty string", ``, 0, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotK, gotDesc, gotOK := parseTopKWrapper(tc.logql)
			if gotOK != tc.wantOK {
				t.Errorf("parseTopKWrapper(%q) ok=%v, want %v", tc.logql, gotOK, tc.wantOK)
				return
			}
			if !tc.wantOK {
				return
			}
			if gotK != tc.wantK {
				t.Errorf("parseTopKWrapper(%q) k=%d, want %d", tc.logql, gotK, tc.wantK)
			}
			if gotDesc != tc.wantDesc {
				t.Errorf("parseTopKWrapper(%q) descending=%v, want %v", tc.logql, gotDesc, tc.wantDesc)
			}
		})
	}
}

func TestApplyTopKToMatrix(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"high"},"values":[[1700000300,"10.0"],[1700000600,"12.0"]]},` +
		`{"metric":{"app":"mid"},"values":[[1700000300,"5.0"],[1700000600,"6.0"]]},` +
		`{"metric":{"app":"low"},"values":[[1700000300,"1.0"],[1700000600,"2.0"]]}` +
		`]}}`)

	// topk(2) → keep "high" and "mid" (max 12 and 6), drop "low" (max 2).
	got := applyTopKToMatrix(input, 2, true)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("topk(2): got %d series, want 2", len(resp.Data.Result))
	}
	apps := map[string]bool{}
	for _, s := range resp.Data.Result {
		apps[s.Metric["app"]] = true
	}
	if !apps["high"] || !apps["mid"] {
		t.Errorf("topk(2): expected high+mid, got %v", apps)
	}

	// bottomk(1) → keep "low" (max 2), drop others.
	got2 := applyTopKToMatrix(input, 1, false)
	var resp2 struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got2, &resp2); err != nil {
		t.Fatalf("unmarshal bottomk: %v", err)
	}
	if len(resp2.Data.Result) != 1 {
		t.Fatalf("bottomk(1): got %d series, want 1", len(resp2.Data.Result))
	}
	if resp2.Data.Result[0].Metric["app"] != "low" {
		t.Errorf("bottomk(1): expected low, got %v", resp2.Data.Result[0].Metric)
	}

	// k >= number of series → return all.
	got3 := applyTopKToMatrix(input, 10, true)
	var resp3 struct {
		Data struct {
			Result []struct{} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got3, &resp3); err != nil {
		t.Fatalf("unmarshal k>=n: %v", err)
	}
	if len(resp3.Data.Result) != 3 {
		t.Fatalf("topk(10) of 3: got %d series, want 3", len(resp3.Data.Result))
	}
}

func TestApplyTopKToVector(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"app":"a"},"value":[1700000300,"10.0"]},` +
		`{"metric":{"app":"b"},"value":[1700000300,"5.0"]},` +
		`{"metric":{"app":"c"},"value":[1700000300,"1.0"]}` +
		`]}}`)

	got := applyTopKToVector(input, 2, true)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("topk(2): got %d, want 2", len(resp.Data.Result))
	}
}

func TestQueryRange_TopKFiltersToKSeries(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
				`{"metric":{"app":"high"},"values":[[1700000300,"10.0"],[1700000600,"12.0"]]},`+
				`{"metric":{"app":"mid"},"values":[[1700000300,"5.0"],[1700000600,"6.0"]]},`+
				`{"metric":{"app":"low"},"values":[[1700000300,"1.0"],[1700000600,"2.0"]]}]}}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
	params := url.Values{}
	params.Set("query", `topk(2, sum by (app) (rate({app=~".+"}[5m])))`)
	// One populated evaluation window: no all-zero buckets with tied winners.
	// Changing winners across steps are covered by TestTopK_RangeWinnersChangeAtEachStep.
	params.Set("start", "1700000600")
	params.Set("end", "1700000600")
	params.Set("step", "300")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("topk(2) got %d series, want 2", len(resp.Data.Result))
	}
	apps := map[string]bool{}
	for _, s := range resp.Data.Result {
		apps[s.Metric["app"]] = true
	}
	if !apps["high"] || !apps["mid"] {
		t.Errorf("topk(2) kept %v, want high+mid", apps)
	}
}

func TestQueryRange_BottomKFiltersToKSeries(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
				`{"metric":{"app":"high"},"values":[[1700000300,"10.0"]]},`+
				`{"metric":{"app":"low"},"values":[[1700000300,"1.0"]]}]}}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
	params := url.Values{}
	params.Set("query", `bottomk(1, sum by (app) (count_over_time({app=~".+"}[5m])))`)
	params.Set("start", "1700000600")
	params.Set("end", "1700000600")
	params.Set("step", "300")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("bottomk(1) got %d series, want 1", len(resp.Data.Result))
	}
	if resp.Data.Result[0].Metric["app"] != "low" {
		t.Errorf("bottomk(1) got app=%s, want low", resp.Data.Result[0].Metric["app"])
	}
}

// TestAddUnderscorefallbackByLabels covers the by() clause augmentation and
// the guard paths to prevent panic when the pattern is absent.
func TestAddUnderscorefallbackByLabels(t *testing.T) {
	newUnderscoreProxy := func(t *testing.T) *Proxy {
		t.Helper()
		p, err := New(Config{
			BackendURL: "http://127.0.0.1:9999",
			Cache:      cache.New(60e9, 100),
			LogLevel:   "error",
			LabelStyle: LabelStyleUnderscores,
		})
		if err != nil {
			t.Fatalf("create proxy: %v", err)
		}
		return p
	}

	t.Run("injects underscore fallback for dotted OTel label", func(t *testing.T) {
		p := newUnderscoreProxy(t)
		query := `app:="svc" | stats by (service.name, level) count() as c`
		got := p.addUnderscorefallbackByLabels(query, []string{"service_name", "level"})
		// service_name → service.name (dotted): underscore fallback added.
		// level has no dot translation: no fallback.
		if !strings.Contains(got, "service_name") {
			t.Fatalf("expected underscore fallback to include service_name, got: %q", got)
		}
		if !strings.Contains(got, "| stats by (") {
			t.Fatalf("expected stats by clause to be present, got: %q", got)
		}
	})

	t.Run("no stats by clause returns query unchanged", func(t *testing.T) {
		p := newUnderscoreProxy(t)
		input := `app:="svc" | unpack_json | filter status:>500`
		got := p.addUnderscorefallbackByLabels(input, []string{"service_name"})
		if got != input {
			t.Fatalf("expected unchanged query when no '| stats by (' present, got %q", got)
		}
	})

	t.Run("passthrough translator returns query unchanged", func(t *testing.T) {
		p, _ := New(Config{
			BackendURL: "http://127.0.0.1:9999",
			Cache:      cache.New(60e9, 100),
			LogLevel:   "error",
		})
		input := `app:="svc" | stats by (service.name) count()`
		got := p.addUnderscorefallbackByLabels(input, []string{"service_name"})
		if got != input {
			t.Fatalf("expected passthrough proxy to leave query unchanged, got %q", got)
		}
	})

	t.Run("empty origGroupBy derives fallbacks from the by clause", func(t *testing.T) {
		p := newUnderscoreProxy(t)
		input := `app:="svc" | stats by (service.name) count()`
		want := `app:="svc" | stats by (service.name, service_name) count()`
		if got := p.addUnderscorefallbackByLabels(input, nil); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	t.Run("every stats pipe keeps the fallback", func(t *testing.T) {
		// rate() translates to an inner count and an outer sum; grouping only the
		// outer pipe by service.name collapsed Loki-push streams into "".
		p := newUnderscoreProxy(t)
		input := `namespace:="data" | stats by (service.name) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by ("service.name") sum(__lvp_rate)`
		want := `namespace:="data" | stats by (service.name, service_name) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by ("service.name", service_name) sum(__lvp_rate)`
		if got := p.addUnderscorefallbackByLabels(input, []string{"service_name"}); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if got := p.addUnderscorefallbackByLabels(want, []string{"service_name"}); got != want {
			t.Fatalf("not idempotent: %q", got)
		}
	})

	t.Run("unclosed by clause guard returns query unchanged", func(t *testing.T) {
		// Malformed query: '| stats by (' without closing ')' — guard prevents
		// an out-of-bounds splice.
		p := newUnderscoreProxy(t)
		input := `app:="svc" | stats by (service.name`
		got := p.addUnderscorefallbackByLabels(input, []string{"service_name"})
		if got != input {
			t.Fatalf("expected unclosed by-clause guard to return query unchanged, got %q", got)
		}
	})
}

// Instant `sum by (service_name)` must group Loki-push rows (stream field
// service_name) and OTel rows (field service.name) under one Loki label, as the
// range path does. Without the fallback every Loki-push stream grouped as "".
func TestProxyStatsQueryGroupsByUnderscoreFallback(t *testing.T) {
	var backendQuery atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stats_query" {
			http.NotFound(w, r)
			return
		}
		_ = r.ParseForm()
		backendQuery.Store(r.Form.Get("query"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"service.name":"","service_name":"cache-redis"},"value":[1789450000,"25"]},` +
			`{"metric":{"service.name":"api-gateway","service_name":""},"value":[1789450000,"60"]}]}}`))
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60e9, 100), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	params := url.Values{"query": {`sum by (service_name) (count_over_time({env="production"}[1h]))`}, "time": {"1789450000"}}
	w := httptest.NewRecorder()
	p.handleQuery(w, withOrgID(httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)))
	if q, _ := backendQuery.Load().(string); !strings.Contains(q, "service_name") {
		t.Fatalf("backend stats query lacks the service_name fallback: %q", q)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(w.Body.Bytes(), &resp); err != nil || w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	got := map[string]bool{}
	for _, series := range resp.Data.Result {
		got[series.Metric["service_name"]] = true
	}
	if len(got) != 2 || !got["cache-redis"] || !got["api-gateway"] {
		t.Fatalf("service_name groups=%v body=%s", got, w.Body)
	}
}

// Loki returns the label the query grouped by: `by (level)` yields level,
// `by (detected_level)` yields detected_level and `by (level, detected_level)`
// yields both. VictoriaLogs answers all three with its level field.
func TestStatsResponseKeepsRequestedLevelLabel(t *testing.T) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:9999", Cache: cache.New(60e9, 100), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"level":"info","service.name":"api"},"value":[1789450000,"5"]}]}}`)
	cases := []struct {
		query string
		want  map[string]string
	}{
		{`sum by (service_name, level) (count_over_time({app="a"}[5m]))`, map[string]string{"service_name": "api", "level": "info"}},
		{`topk(3, sum by (level, service_name) (count_over_time({app="a"}[5m])))`, map[string]string{"service_name": "api", "level": "info"}},
		{`sum by (service_name, detected_level) (count_over_time({app="a"}[5m]))`, map[string]string{"service_name": "api", "detected_level": "info"}},
		{`sum by (service_name, level, detected_level) (count_over_time({app="a"}[5m]))`, map[string]string{"service_name": "api", "level": "info", "detected_level": "info"}},
	}
	for _, tc := range cases {
		out := p.translateStatsResponseLabelsWithContext(context.Background(), body, tc.query)
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := stdjson.Unmarshal(out, &resp); err != nil || len(resp.Data.Result) != 1 {
			t.Fatalf("%s: body=%s err=%v", tc.query, out, err)
		}
		if got := resp.Data.Result[0].Metric; fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Fatalf("%s: metric=%v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestStatsQueryRangeResponseKeepsRequestedLevelLabel(t *testing.T) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:9999", Cache: cache.New(60e9, 100), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[1789450000,"5"]]}]}}`)
	for query, want := range map[string]string{
		`sum by (level) (count_over_time({app="a"}[1m]))`:          `{"level":"info"}`,
		`sum by (detected_level) (count_over_time({app="a"}[1m]))`: `{"detected_level":"info"}`,
	} {
		out := p.trimAndTranslateStatsQRFJ(context.Background(), body, nil, query)
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := stdjson.Unmarshal(out, &resp); err != nil || len(resp.Data.Result) != 1 {
			t.Fatalf("%s: body=%s err=%v", query, out, err)
		}
		got, _ := stdjson.Marshal(resp.Data.Result[0].Metric)
		if string(got) != want {
			t.Fatalf("%s: metric=%s, want %s", query, got, want)
		}
	}
}
