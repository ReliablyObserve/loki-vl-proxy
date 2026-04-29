package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestExtendStatsQueryRangeEnd_ReturnsSecs guards that extendStatsQueryRangeEnd
// always emits a Unix-seconds string. VL's stats_query_range rejects nanosecond
// timestamps silently, causing empty or wrong results.
func TestExtendStatsQueryRangeEnd_ReturnsSecs(t *testing.T) {
	cases := []struct {
		name    string
		endRaw  string
		stepRaw string
		want    string
	}{
		{
			name:    "unix seconds + 60s step",
			endRaw:  "1745904000",
			stepRaw: "60",
			want:    "1745904060",
		},
		{
			name:    "unix seconds + 5m step",
			endRaw:  "1745904000",
			stepRaw: "300",
			want:    "1745904300",
		},
		{
			name:    "nanosecond input normalised to seconds output",
			endRaw:  strconv.FormatInt(1745904000*int64(time.Second), 10),
			stepRaw: "60",
			want:    "1745904060",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extendStatsQueryRangeEnd(tc.endRaw, tc.stepRaw)
			if !ok {
				t.Fatal("expected ok=true")
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
			// Guard: seconds strings are at most 12 digits; nanoseconds are 19.
			if len(got) > 12 {
				t.Errorf("result %q looks like nanoseconds — VL expects Unix seconds", got)
			}
		})
	}
}

// TestExtendStatsQueryRangeEnd_NeverNanoseconds is a property test: for any
// realistic eval time, the output must stay under 13 digits.
func TestExtendStatsQueryRangeEnd_NeverNanoseconds(t *testing.T) {
	endRaw := strconv.FormatInt(time.Now().Unix(), 10)
	got, ok := extendStatsQueryRangeEnd(endRaw, "60")
	if !ok {
		t.Fatal("expected ok=true for current unix time")
	}
	if len(got) > 12 {
		t.Errorf("extendStatsQueryRangeEnd(%q, 60s) = %q — looks like nanoseconds", endRaw, got)
	}
}

// TestBuildStatsQueryRangeParams_EndIsSeconds checks that the end param sent to
// VL's stats_query_range is in Unix seconds, not nanoseconds.
func TestBuildStatsQueryRangeParams_EndIsSeconds(t *testing.T) {
	params := buildStatsQueryRangeParams(
		`app:=api | stats count()`,
		"1745900000",
		"1745903600",
		"60",
	)
	endVal := params.Get("end")
	if endVal == "" {
		t.Fatal("expected end param to be set")
	}
	if len(endVal) > 12 {
		t.Errorf("end param %q looks like nanoseconds — VL expects Unix seconds", endVal)
	}
	startVal := params.Get("start")
	if startVal != "" && len(startVal) > 12 {
		t.Errorf("start param %q looks like nanoseconds — VL expects Unix seconds", startVal)
	}
}

// TestProxyStatsQuery_SendsSecondBoundsToVL verifies that proxyStatsQuery
// sends start/end in seconds to VL when the original LogQL contains a range window.
// The root bug: without this fix, VL received nanosecond timestamps and scanned
// all historical data (O(all_time) instead of O(window)).
func TestProxyStatsQuery_SendsSecondBoundsToVL(t *testing.T) {
	var capturedStart, capturedEnd string

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stats_query") {
			_ = r.ParseForm()
			capturedStart = r.FormValue("start")
			capturedEnd = r.FormValue("end")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	evalTimeUnix := int64(1745904000)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	q := req.URL.Query()
	q.Set("query", `count_over_time({app="api"}[5m])`)
	q.Set("time", strconv.FormatInt(evalTimeUnix, 10))
	req.URL.RawQuery = q.Encode()

	rec := httptest.NewRecorder()
	// Call proxyStatsQuery directly with a pre-translated VL query
	p.proxyStatsQuery(rec, req, `app:=api | stats count()`)

	if capturedStart == "" {
		t.Fatal("expected start to be sent to VL for windowed LogQL query")
	}
	// Both must be in seconds (≤12 digits), never 19-digit nanoseconds
	if len(capturedStart) > 12 {
		t.Errorf("start param %q looks like nanoseconds — VL expects Unix seconds", capturedStart)
	}
	if len(capturedEnd) > 12 {
		t.Errorf("end param %q looks like nanoseconds — VL expects Unix seconds", capturedEnd)
	}

	// end = eval time, start = eval time minus 5m window
	wantEnd := strconv.FormatInt(evalTimeUnix, 10)
	wantStart := strconv.FormatInt(evalTimeUnix-300, 10)

	if capturedEnd != wantEnd {
		t.Errorf("end: got %q, want %q", capturedEnd, wantEnd)
	}
	if capturedStart != wantStart {
		t.Errorf("start: got %q, want %q", capturedStart, wantStart)
	}
}

// TestProxyStatsQuery_NoStartEndWhenNoWindow checks that proxyStatsQuery does
// not inject start/end when the LogQL query has no range window (non-windowed
// vector query). Adding spurious bounds would break those queries.
func TestProxyStatsQuery_NoStartEndWhenNoWindow(t *testing.T) {
	var capturedStart, capturedEnd string

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stats_query") {
			_ = r.ParseForm()
			capturedStart = r.FormValue("start")
			capturedEnd = r.FormValue("end")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	q := req.URL.Query()
	q.Set("query", `{app="api"}`) // no range window — not a range metric query
	q.Set("time", "1745904000")
	req.URL.RawQuery = q.Encode()

	rec := httptest.NewRecorder()
	p.proxyStatsQuery(rec, req, `app:=api | stats count()`)

	if capturedStart != "" || capturedEnd != "" {
		t.Errorf("expected no start/end for non-windowed query, got start=%q end=%q",
			capturedStart, capturedEnd)
	}
}

// TestTranslateStatsResponseLabels_LevelPreservedWithStream is a regression test
// for the hadStream bug. When VL returns _stream alongside a level field, both
// level (genuine stream label) and detected_level (synthetic) must coexist.
// Previously, level was incorrectly deleted whenever detected_level was synthesised,
// causing count_over_time grouped by level to collapse cross-product series.
func TestTranslateStatsResponseLabels_LevelPreservedWithStream(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)

	// Metric result where VL returns _stream (all stream labels) AND explicit level
	body := []byte(`{"results":[{"metric":{"_stream":"{app=\"api\",level=\"warn\"}","level":"warn"}}]}`)
	got := p.translateStatsResponseLabelsWithContext(
		context.Background(), body,
		`sum by (level) (count_over_time({app="api"}[5m]))`,
	)

	var resp struct {
		Results []struct {
			Metric map[string]interface{} `json:"metric"`
		} `json:"results"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Results) == 0 {
		t.Fatal("expected at least one result")
	}
	metric := resp.Results[0].Metric

	// level MUST be preserved — it's a genuine stream label expanded from _stream
	if _, ok := metric["level"]; !ok {
		t.Errorf("level must be preserved when _stream is present; got %#v", metric)
	}
	// detected_level must also be synthesised from level
	if metric["detected_level"] == "" {
		t.Errorf("detected_level must be synthesised; got %#v", metric)
	}
}

// TestTranslateStatsResponseLabels_LevelRemovedWithoutStream checks the complementary
// case: when _stream is absent (VL returned only explicit by-group keys), level must
// be replaced by detected_level for Loki compatibility.
func TestTranslateStatsResponseLabels_LevelRemovedWithoutStream(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)

	body := []byte(`{"results":[{"metric":{"level":"error"}}]}`)
	got := p.translateStatsResponseLabelsWithContext(
		context.Background(), body,
		`sum by (level) (count_over_time({app="api"}[5m]))`,
	)

	var resp struct {
		Results []struct {
			Metric map[string]interface{} `json:"metric"`
		} `json:"results"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	metric := resp.Results[0].Metric

	// Without _stream, level came from VL grouping and must be replaced by detected_level
	if _, ok := metric["level"]; ok {
		t.Errorf("level must be removed when _stream is absent; got %#v", metric)
	}
	if metric["detected_level"] == "" {
		t.Errorf("detected_level must be synthesised from level; got %#v", metric)
	}
}

// --- /admin/cache/flush endpoint ---

func newAdminTestProxy(t *testing.T, backendURL, adminToken string) (*Proxy, *http.ServeMux) {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     backendURL,
		Cache:          c,
		LogLevel:       "error",
		AdminAuthToken: adminToken,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

// TestHandleCacheFlush_MethodNotAllowed guards that GET is rejected.
func TestHandleCacheFlush_MethodNotAllowed(t *testing.T) {
	_, mux := newAdminTestProxy(t, "http://unused", "")

	req := httptest.NewRequest(http.MethodGet, "/admin/cache/flush", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET, got %d", rec.Code)
	}
}

// TestHandleCacheFlush_RequiresAuth guards that a configured admin token is enforced.
func TestHandleCacheFlush_RequiresAuth(t *testing.T) {
	_, mux := newAdminTestProxy(t, "http://unused", "secret-token")

	req := httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth header, got %d", rec.Code)
	}
}

// TestHandleCacheFlush_RejectsWrongToken guards wrong Bearer tokens are rejected.
func TestHandleCacheFlush_RejectsWrongToken(t *testing.T) {
	_, mux := newAdminTestProxy(t, "http://unused", "correct-token")

	req := httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong token, got %d", rec.Code)
	}
}

// TestHandleCacheFlush_SuccessReturnsJSON checks the happy path returns JSON status=ok.
func TestHandleCacheFlush_SuccessReturnsJSON(t *testing.T) {
	_, mux := newAdminTestProxy(t, "http://unused", "secret-token")

	req := httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("expected JSON content-type, got %q", ct)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("expected valid JSON response: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", result["status"])
	}
}

// TestHandleCacheFlush_PurgesCacheEntries confirms that cache entries are gone
// after a successful flush, so benchmarks that call the endpoint get a cold start.
func TestHandleCacheFlush_PurgesCacheEntries(t *testing.T) {
	p, mux := newAdminTestProxy(t, "http://unused", "")

	// Seed a cache entry
	if p.cache != nil {
		p.cache.Set("flush-test-key", []byte("flush-test-value"))
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	// Entry must be gone
	if p.cache != nil {
		if _, ok := p.cache.Get("flush-test-key"); ok {
			t.Error("expected cache entry to be purged after flush")
		}
	}
}

// TestHandleCacheFlush_XAdminTokenHeader checks the X-Admin-Token header alternative.
func TestHandleCacheFlush_XAdminTokenHeader(t *testing.T) {
	_, mux := newAdminTestProxy(t, "http://unused", "secret-token")

	req := httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil)
	req.Header.Set("X-Admin-Token", "secret-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 with X-Admin-Token header, got %d", rec.Code)
	}
}
