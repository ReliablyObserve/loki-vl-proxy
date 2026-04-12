package metrics

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetrics_Handler_Output(t *testing.T) {
	m := NewMetrics()
	m.RecordRequest("labels", 200, 5*time.Millisecond)
	m.RecordRequest("query_range", 500, 100*time.Millisecond)
	m.RecordCacheHit()
	m.RecordCacheMiss()
	m.RecordTranslation()
	m.RecordTranslationError()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()

	// Content-Type
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", ct)
	}

	// Request counters
	if !strings.Contains(body, "loki_vl_proxy_requests_total") {
		t.Error("missing loki_vl_proxy_requests_total")
	}
	if !strings.Contains(body, `endpoint="labels"`) {
		t.Error("missing labels endpoint in metrics")
	}
	if !strings.Contains(body, `status="200"`) {
		t.Error("missing status=200 in metrics")
	}

	// Histogram
	if !strings.Contains(body, "loki_vl_proxy_request_duration_seconds_bucket") {
		t.Error("missing duration histogram buckets")
	}
	if !strings.Contains(body, "loki_vl_proxy_request_duration_seconds_sum") {
		t.Error("missing duration histogram sum")
	}
	if !strings.Contains(body, "loki_vl_proxy_request_duration_seconds_count") {
		t.Error("missing duration histogram count")
	}

	// Cache
	if !strings.Contains(body, "loki_vl_proxy_cache_hits_total 1") {
		t.Error("expected cache_hits_total 1")
	}
	if !strings.Contains(body, "loki_vl_proxy_cache_misses_total 1") {
		t.Error("expected cache_misses_total 1")
	}

	// Translations
	if !strings.Contains(body, "loki_vl_proxy_translations_total 1") {
		t.Error("expected translations_total 1")
	}
	if !strings.Contains(body, "loki_vl_proxy_translation_errors_total 1") {
		t.Error("expected translation_errors_total 1")
	}

	// Uptime
	if !strings.Contains(body, "loki_vl_proxy_uptime_seconds") {
		t.Error("missing uptime metric")
	}
}

func TestMetrics_Handler_EmptyState(t *testing.T) {
	m := NewMetrics()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "loki_vl_proxy_cache_hits_total 0") {
		t.Error("expected zero cache hits")
	}
}

func TestMetrics_RecordTranslationError(t *testing.T) {
	m := NewMetrics()
	m.RecordTranslationError()
	m.RecordTranslationError()
	if m.translationErrors.Load() != 2 {
		t.Errorf("expected 2, got %d", m.translationErrors.Load())
	}
}

func TestResolveClientID_IgnoresUntrustedProxyHeadersByDefault(t *testing.T) {
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("X-Grafana-User", "grafana-user")
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("backend-user:secret")))
	req.RemoteAddr = "198.51.100.20:1234"

	got := ResolveClientID(req, false)
	if got != "198.51.100.20" {
		t.Fatalf("expected remote client identity when proxy headers are untrusted, got %q", got)
	}
}

func TestResolveClientID_UsesTrustedGrafanaUser(t *testing.T) {
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("X-Grafana-User", "grafana-user")
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.RemoteAddr = "198.51.100.20:1234"

	got := ResolveClientID(req, true)
	if got != "grafana-user" {
		t.Fatalf("expected trusted grafana user to win, got %q", got)
	}
}

func TestResolveClientID_UsesTrustedForwardedUserHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("X-Forwarded-User", "idp-user@example.com")
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("backend-user:secret")))
	req.RemoteAddr = "198.51.100.20:1234"

	got := ResolveClientID(req, true)
	if got != "idp-user@example.com" {
		t.Fatalf("expected trusted forwarded user header to win, got %q", got)
	}
}

func TestMetrics_Handler_ExportsClientCentricBreakdowns(t *testing.T) {
	m := NewMetrics()
	m.RecordClientIdentity("grafana-user", "query_range", 20*time.Millisecond, 512)
	m.RecordClientStatus("grafana-user", "query_range", http.StatusTooManyRequests)
	m.RecordClientInflight("grafana-user", 1)
	m.RecordClientQueryLength("grafana-user", "query_range", len(`{app="api"}`))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	for _, metric := range []string{
		"loki_vl_proxy_client_status_total",
		"loki_vl_proxy_client_inflight_requests",
		"loki_vl_proxy_client_query_length_chars_bucket",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("expected %s in metrics output", metric)
		}
	}
	if !strings.Contains(body, `client="grafana-user"`) {
		t.Fatal("expected client label in client-centric metrics")
	}
	if !strings.Contains(body, `status="429"`) {
		t.Fatal("expected per-client status metric for 429")
	}
}

func TestMetrics_Handler_BoundsTenantAndClientCardinality(t *testing.T) {
	m := NewMetricsWithLimits(1, 1)
	m.RecordTenantRequest("team-a", "query_range", 200, 10*time.Millisecond)
	m.RecordTenantRequest("team-b", "query_range", 200, 10*time.Millisecond)
	m.RecordClientIdentity("client-a", "query_range", 10*time.Millisecond, 10)
	m.RecordClientIdentity("client-b", "query_range", 10*time.Millisecond, 10)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	if !strings.Contains(body, `tenant="team-a"`) {
		t.Fatal("expected first tenant label to be retained")
	}
	if !strings.Contains(body, `tenant="__overflow__"`) {
		t.Fatal("expected overflow tenant bucket in metrics output")
	}
	if strings.Contains(body, `tenant="team-b"`) {
		t.Fatal("expected second tenant to be folded into overflow bucket")
	}
	if !strings.Contains(body, `client="client-a"`) {
		t.Fatal("expected first client label to be retained")
	}
	if !strings.Contains(body, `client="__overflow__"`) {
		t.Fatal("expected overflow client bucket in metrics output")
	}
	if strings.Contains(body, `client="client-b"`) {
		t.Fatal("expected second client to be folded into overflow bucket")
	}
}

func TestResolveClientContext_Branches(t *testing.T) {
	t.Run("trusted forwarded for", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/metrics", nil)
		req.Header.Set("X-Forwarded-For", "203.0.113.10, 198.51.100.20")
		req.RemoteAddr = "198.51.100.20:1234"

		clientID, source := ResolveClientContext(req, true)
		if clientID != "203.0.113.10" || source != "forwarded_for" {
			t.Fatalf("unexpected context: %q %q", clientID, source)
		}
	})

	t.Run("tenant preferred when proxy headers untrusted", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/metrics", nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a")
		req.Header.Set("X-Forwarded-For", "203.0.113.10")
		req.RemoteAddr = "198.51.100.20:1234"

		clientID, source := ResolveClientContext(req, false)
		if clientID != "tenant-a" || source != "tenant" {
			t.Fatalf("unexpected context: %q %q", clientID, source)
		}
	})

	t.Run("remote addr fallback", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/metrics", nil)
		req.RemoteAddr = "198.51.100.20:1234"

		clientID, source := ResolveClientContext(req, false)
		if clientID != "198.51.100.20" || source != "remote_addr" {
			t.Fatalf("unexpected context: %q %q", clientID, source)
		}
	})
}

func TestResolveAuthContext_BasicAuthUser(t *testing.T) {
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("backend-user:secret")))

	authUser, authSource := ResolveAuthContext(req)
	if authUser != "backend-user" || authSource != "basic_auth" {
		t.Fatalf("unexpected auth context: %q %q", authUser, authSource)
	}
}

func TestSanitizeMetricIdentity(t *testing.T) {
	if got := sanitizeMetricIdentity("", "__fallback__"); got != "__fallback__" {
		t.Fatalf("expected fallback, got %q", got)
	}
	if got := sanitizeMetricIdentity("tenant:a", "__fallback__"); got != "tenant_a" {
		t.Fatalf("expected colon replacement, got %q", got)
	}
	long := strings.Repeat("a", 80)
	if got := sanitizeMetricIdentity(long, "__fallback__"); len(got) != 64 {
		t.Fatalf("expected truncation to 64 chars, got %d", len(got))
	}
}

func TestMetrics_RecordersAndHandler_ExposeAdditionalMetrics(t *testing.T) {
	m := NewMetricsWithLimits(0, 0)
	m.SetCircuitBreakerFunc(func() string { return "half-open" })
	m.RecordTenantRequest("team-a", "query_range", 200, 15*time.Millisecond)
	m.RecordClientError("query_range", "bad_query")
	m.RecordTupleMode("grafana_default_2tuple")
	m.RecordTupleMode("grafana_default_2tuple")
	m.RecordEndpointCacheHit("labels")
	m.RecordEndpointCacheMiss("labels")
	m.RecordBackendDuration("query_range", 25*time.Millisecond)
	m.RecordCoalesced()
	m.RecordCoalescedSaved()
	m.RecordQueryRangeWindowCacheHit()
	m.RecordQueryRangeWindowCacheMiss()
	m.RecordQueryRangeWindowFetchDuration(20 * time.Millisecond)
	m.RecordQueryRangeWindowMergeDuration(5 * time.Millisecond)
	m.RecordQueryRangeWindowCount(3)
	m.RecordQueryRangeWindowPrefilterAttempt()
	m.RecordQueryRangeWindowPrefilterError()
	m.RecordQueryRangeWindowPrefilterOutcome(2, 1)
	m.RecordQueryRangeWindowPrefilterDuration(3 * time.Millisecond)
	m.RecordQueryRangeAdaptiveState(4, 1400*time.Millisecond, 0.03)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)
	body := w.Body.String()

	for _, snippet := range []string{
		`loki_vl_proxy_tenant_requests_total{tenant="team-a",endpoint="query_range",status="200"} 1`,
		`loki_vl_proxy_client_errors_total{endpoint="query_range",reason="bad_query"} 1`,
		`loki_vl_proxy_cache_hits_by_endpoint{endpoint="labels"} 1`,
		`loki_vl_proxy_cache_misses_by_endpoint{endpoint="labels"} 1`,
		`loki_vl_proxy_backend_duration_seconds_count{endpoint="query_range"} 1`,
		`loki_vl_proxy_coalesced_total 1`,
		`loki_vl_proxy_coalesced_saved_total 1`,
		`loki_vl_proxy_window_cache_hit_total 1`,
		`loki_vl_proxy_window_cache_miss_total 1`,
		`loki_vl_proxy_window_fetch_seconds_count 1`,
		`loki_vl_proxy_window_merge_seconds_count 1`,
		`loki_vl_proxy_window_count_count 1`,
		`loki_vl_proxy_window_prefilter_attempt_total 1`,
		`loki_vl_proxy_window_prefilter_error_total 1`,
		`loki_vl_proxy_window_prefilter_kept_total 2`,
		`loki_vl_proxy_window_prefilter_skipped_total 1`,
		`loki_vl_proxy_window_prefilter_duration_seconds_count 1`,
		`loki_vl_proxy_window_adaptive_parallel_current 4`,
		`loki_vl_proxy_window_adaptive_latency_ewma_seconds 1.4`,
		`loki_vl_proxy_window_adaptive_error_ewma 0.03`,
		`loki_vl_proxy_response_tuple_mode_total{mode="grafana_default_2tuple"} 2`,
		`loki_vl_proxy_circuit_breaker_state 2`,
	} {
		if !strings.Contains(body, snippet) {
			t.Fatalf("expected metrics output to contain %q\nbody:\n%s", snippet, body)
		}
	}
}

func TestNewMetricsWithLimits_Defaults(t *testing.T) {
	m := NewMetricsWithLimits(0, 0)
	if m.maxTenantLabels != defaultMaxTenantLabels {
		t.Fatalf("expected default tenant limit, got %d", m.maxTenantLabels)
	}
	if m.maxClientLabels != defaultMaxClientLabels {
		t.Fatalf("expected default client limit, got %d", m.maxClientLabels)
	}
}
