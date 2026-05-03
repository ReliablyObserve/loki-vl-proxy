package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestColdRouter_Route_HotOnly(t *testing.T) {
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
	}

	now := time.Now()
	start := now.Add(-1 * time.Hour).UnixNano()
	end := now.UnixNano()

	if d := cr.Route(start, end); d != RouteHotOnly {
		t.Errorf("recent query should route hot, got %s", d)
	}
}

func TestColdRouter_Route_ColdOnly(t *testing.T) {
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
	}

	now := time.Now()
	start := now.Add(-30 * 24 * time.Hour).UnixNano()
	end := now.Add(-20 * 24 * time.Hour).UnixNano()

	if d := cr.Route(start, end); d != RouteColdOnly {
		t.Errorf("old query should route cold, got %s", d)
	}
}

func TestColdRouter_Route_Both(t *testing.T) {
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
	}

	now := time.Now()
	start := now.Add(-10 * 24 * time.Hour).UnixNano()
	end := now.UnixNano()

	if d := cr.Route(start, end); d != RouteBoth {
		t.Errorf("spanning query should route both, got %s", d)
	}
}

func TestColdRouter_Route_NilRouter(t *testing.T) {
	var cr *ColdRouter
	if d := cr.Route(0, time.Now().UnixNano()); d != RouteHotOnly {
		t.Errorf("nil router should route hot, got %s", d)
	}
}

func TestColdRouter_Route_EmptyManifest(t *testing.T) {
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
		manifest: &ManifestRange{TotalFiles: 0},
	}

	now := time.Now()
	start := now.Add(-30 * 24 * time.Hour).UnixNano()
	end := now.Add(-20 * 24 * time.Hour).UnixNano()

	if d := cr.Route(start, end); d != RouteHotOnly {
		t.Errorf("empty manifest should route hot, got %s", d)
	}
}

func TestColdRouter_Route_ManifestMax(t *testing.T) {
	now := time.Now()
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
		manifest: &ManifestRange{
			TotalFiles: 100,
			MaxTime:    now.Add(-15 * 24 * time.Hour).UnixNano(),
		},
	}

	start := now.Add(-10 * 24 * time.Hour).UnixNano()
	end := now.Add(-8 * 24 * time.Hour).UnixNano()

	if d := cr.Route(start, end); d != RouteHotOnly {
		t.Errorf("query after manifest max should route hot, got %s", d)
	}
}

func TestNewColdRouter_Disabled(t *testing.T) {
	cr, err := NewColdRouter(ColdBackendConfig{Enabled: false}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cr != nil {
		t.Error("disabled config should return nil router")
	}
}

func TestNewColdRouter_EmptyURL(t *testing.T) {
	cr, err := NewColdRouter(ColdBackendConfig{Enabled: true, URL: ""}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cr != nil {
		t.Error("empty URL should return nil router")
	}
}

func TestColdRouter_RefreshManifest(t *testing.T) {
	manifest := ManifestRange{
		MinTime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		MaxTime:    time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC).UnixNano(),
		MinDate:    "2026-01-01",
		MaxDate:    "2026-04-30",
		TotalFiles: 2920,
		TotalBytes: 146000000000,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/manifest/range" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(manifest)
	}))
	defer srv.Close()

	cr := &ColdRouter{
		coldBackend: mustParseURL(srv.URL),
		client:      srv.Client(),
		logger:      testSlogger(),
	}

	cr.refreshManifest(t.Context())

	got := cr.ManifestRange()
	if got == nil {
		t.Fatal("expected manifest after refresh")
	}
	if got.TotalFiles != 2920 {
		t.Errorf("total files = %d, want 2920", got.TotalFiles)
	}
	if got.MinDate != "2026-01-01" {
		t.Errorf("min date = %s, want 2026-01-01", got.MinDate)
	}
}

func TestMergeNDJSON(t *testing.T) {
	hot := bytes.NewBufferString(`{"line":"hot1"}` + "\n" + `{"line":"hot2"}` + "\n")
	cold := bytes.NewBufferString(`{"line":"cold1"}` + "\n")

	merged, err := io.ReadAll(MergeNDJSON(hot, cold))
	if err != nil {
		t.Fatal(err)
	}

	s := string(merged)
	if !bytes.Contains(merged, []byte("cold1")) {
		t.Errorf("missing cold data in merged: %s", s)
	}
	if !bytes.Contains(merged, []byte("hot1")) {
		t.Errorf("missing hot data in merged: %s", s)
	}
}

func TestMergeValuesJSON(t *testing.T) {
	hot := []byte(`{"values":[{"value":"svc-a","hits":10},{"value":"svc-b","hits":5}]}`)
	cold := []byte(`{"values":[{"value":"svc-a","hits":20},{"value":"svc-c","hits":3}]}`)

	result, err := MergeValuesJSON(hot, cold)
	if err != nil {
		t.Fatal(err)
	}

	var resp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatal(err)
	}

	valMap := make(map[string]int64)
	for _, v := range resp.Values {
		valMap[v.Value] = v.Hits
	}

	if valMap["svc-a"] != 30 {
		t.Errorf("svc-a hits = %d, want 30", valMap["svc-a"])
	}
	if valMap["svc-b"] != 5 {
		t.Errorf("svc-b hits = %d, want 5", valMap["svc-b"])
	}
	if valMap["svc-c"] != 3 {
		t.Errorf("svc-c hits = %d, want 3", valMap["svc-c"])
	}
}

func TestParseFlexTimestamp(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1714521600", 1714521600_000_000_000},
		{"1714521600000", 1714521600_000_000_000},
		{"1714521600000000", 1714521600_000_000_000},
		{"1714521600000000000", 1714521600_000_000_000},
	}
	for _, tt := range tests {
		got, err := parseFlexTimestamp(tt.input)
		if err != nil {
			t.Errorf("parseFlexTimestamp(%q) error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseFlexTimestamp(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseFlexTimestamp_RFC3339(t *testing.T) {
	got, err := parseFlexTimestamp("2026-05-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	if got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestRouteDecision_String(t *testing.T) {
	if RouteHotOnly.String() != "hot" {
		t.Error("RouteHotOnly.String()")
	}
	if RouteColdOnly.String() != "cold" {
		t.Error("RouteColdOnly.String()")
	}
	if RouteBoth.String() != "both" {
		t.Error("RouteBoth.String()")
	}
}

func TestColdRouter_HasManifest(t *testing.T) {
	var cr *ColdRouter
	if cr.HasManifest() {
		t.Error("nil router should not have manifest")
	}

	cr = &ColdRouter{}
	if cr.HasManifest() {
		t.Error("empty router should not have manifest")
	}

	cr.manifest = &ManifestRange{TotalFiles: 1}
	if !cr.HasManifest() {
		t.Error("router with manifest should have manifest")
	}
}

func TestProxyColdRouterIntegration(t *testing.T) {
	manifest := ManifestRange{
		MinTime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		MaxTime:    time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC).UnixNano(),
		TotalFiles: 100,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/manifest/range" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(manifest)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	p, err := New(Config{
		BackendURL: srv.URL,
		ColdBackend: ColdBackendConfig{
			Enabled:  true,
			URL:      srv.URL,
			Boundary: 7 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.ColdRouter() == nil {
		t.Fatal("expected cold router to be initialized")
	}
}

func TestProxyColdRouterDisabled(t *testing.T) {
	p, err := New(Config{
		BackendURL: "http://localhost:9428",
		ColdBackend: ColdBackendConfig{
			Enabled: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.ColdRouter() != nil {
		t.Error("expected nil cold router when disabled")
	}
}

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func testSlogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
