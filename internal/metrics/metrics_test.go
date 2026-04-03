package metrics

import (
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
