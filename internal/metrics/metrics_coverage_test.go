package metrics

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type comparableHandler struct {
	called *bool
}

func (h comparableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.called != nil {
		*h.called = true
	}
	w.WriteHeader(http.StatusNoContent)
}

func TestMetricsWrapHandler_TracksInflightRequests(t *testing.T) {
	m := NewMetrics()
	handler := m.WrapHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := m.activeRequests.Load(); got != 1 {
			t.Fatalf("expected active request gauge 1 during handler execution, got %d", got)
		}
		w.WriteHeader(http.StatusAccepted)
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("unexpected status %d", recorder.Code)
	}
	if got := m.activeRequests.Load(); got != 0 {
		t.Fatalf("expected active request gauge reset to 0, got %d", got)
	}
	if got := m.WrapHandler(nil); got != nil {
		t.Fatalf("expected nil handler to remain nil, got %#v", got)
	}

	var nilMetrics *Metrics
	called := false
	next := comparableHandler{called: &called}
	got := nilMetrics.WrapHandler(next)
	recorder = httptest.NewRecorder()
	got.ServeHTTP(recorder, req)
	if !called || recorder.Code != http.StatusNoContent {
		t.Fatal("expected nil metrics wrapper to pass through to original handler")
	}
}

func TestMetricsConnectionHelpers_AdditionalCoverage(t *testing.T) {
	m := NewMetrics()

	m.RecordActiveRequest(-2)
	if got := m.activeRequests.Load(); got != 0 {
		t.Fatalf("expected active requests to clamp at 0, got %d", got)
	}

	connA, connB := net.Pipe()
	defer func() { _ = connA.Close() }()
	defer func() { _ = connB.Close() }()

	hook := m.ConnStateHook()
	if hook == nil {
		t.Fatal("expected non-nil conn state hook")
	}
	hook(connA, http.StateNew)
	hook(connA, http.StateNew)
	hook(connA, http.StateHijacked)

	if got := m.connectionTransitionCounter("custom").Add(1); got != 1 {
		t.Fatalf("expected dynamic transition counter to initialize, got %d", got)
	}
	if got := m.connectionStateGauge("custom").Add(2); got != 2 {
		t.Fatalf("expected dynamic connection gauge to initialize, got %d", got)
	}
	m.RecordHTTPConnectionRotation("")
	m.RecordHTTPConnectionRotation("manual")

	if got := connStateLabel(http.ConnState(99)); got != "unknown" {
		t.Fatalf("expected unknown conn state label, got %q", got)
	}
	if isLiveConnState("custom") {
		t.Fatal("expected custom state not to be treated as live")
	}
}

func TestMetricsAdditionalRecorders(t *testing.T) {
	m := NewMetricsWithLimits(1, 1)

	if got := m.canonicalTenantLabel("tenant-a"); got != "tenant-a" {
		t.Fatalf("expected first tenant to be retained, got %q", got)
	}
	if got := m.canonicalTenantLabel("tenant-b"); got != overflowMetricLabel {
		t.Fatalf("expected second tenant to overflow, got %q", got)
	}

	m.RecordClientInflight("client-a", 1)
	m.RecordClientInflight("client-a", -3)
	if got := m.clientInflight["client-a"].Load(); got != 0 {
		t.Fatalf("expected inflight to clamp at 0, got %d", got)
	}

	m.RecordUpstreamCallsPerRequestWithRoute("custom", "/custom/route", -5)
	m.RecordQueryRangeWindowFetchDuration(2 * time.Millisecond)
	m.RecordQueryRangeWindowMergeDuration(3 * time.Millisecond)
	m.RecordQueryRangeWindowCount(0)
	m.RecordQueryRangeWindowCount(4)
	m.RecordQueryRangeWindowPrefilterDuration(5 * time.Millisecond)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler(recorder, req)
	body := recorder.Body.String()

	for _, needle := range []string{
		`loki_vl_proxy_upstream_calls_per_request_count{system="loki",direction="downstream",endpoint="custom",route="/custom/route"} 1`,
		`loki_vl_proxy_window_fetch_seconds_count 1`,
		`loki_vl_proxy_window_merge_seconds_count 1`,
		`loki_vl_proxy_window_count_count 1`,
		`loki_vl_proxy_window_prefilter_duration_seconds_count 1`,
	} {
		if !strings.Contains(body, needle) {
			t.Fatalf("expected metrics output to contain %q", needle)
		}
	}
}
