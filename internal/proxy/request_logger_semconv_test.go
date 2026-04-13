package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

func TestRequestLogger_UsesEnduserSemanticFields(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{
		log:                      slog.New(slog.NewJSONHandler(&buf, nil)),
		metrics:                  metrics.NewMetrics(),
		metricsTrustProxyHeaders: true,
	}

	h := p.requestLogger("query_range", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D", nil)
	req.RemoteAddr = "10.0.0.10:1234"
	req.Header.Set("X-Grafana-User", "alice@example.com")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("invalid request log json: %v", err)
	}

	if payload["enduser.id"] != "alice@example.com" {
		t.Fatalf("expected enduser.id, got %#v", payload["enduser.id"])
	}
	if payload["enduser.name"] != "alice@example.com" {
		t.Fatalf("expected enduser.name, got %#v", payload["enduser.name"])
	}
	if payload["enduser.source"] != "grafana_user" {
		t.Fatalf("expected enduser.source grafana_user, got %#v", payload["enduser.source"])
	}
	if _, ok := payload["user.id"]; ok {
		t.Fatalf("did not expect user.id field, got %#v", payload["user.id"])
	}
	if _, ok := payload["user.name"]; ok {
		t.Fatalf("did not expect user.name field, got %#v", payload["user.name"])
	}
}
