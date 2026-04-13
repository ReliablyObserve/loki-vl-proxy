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

	h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D", nil)
	req.RemoteAddr = "10.0.0.10:1234"
	req.Header.Set("X-Grafana-User", "alice@example.com")
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.10")
	req.Header.Set("User-Agent", "Grafana/12")
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
	if payload["client.address"] != "203.0.113.9" {
		t.Fatalf("expected client.address from forwarded-for, got %#v", payload["client.address"])
	}
	if payload["network.peer.address"] != "10.0.0.10" {
		t.Fatalf("expected network.peer.address without port, got %#v", payload["network.peer.address"])
	}
	if payload["user_agent.original"] != "Grafana/12" {
		t.Fatalf("expected user_agent.original, got %#v", payload["user_agent.original"])
	}
	if _, ok := payload["user.id"]; ok {
		t.Fatalf("did not expect user.id field, got %#v", payload["user.id"])
	}
	if _, ok := payload["user.name"]; ok {
		t.Fatalf("did not expect user.name field, got %#v", payload["user.name"])
	}
	if _, ok := payload["event.duration_ms"]; ok {
		t.Fatalf("did not expect legacy event.duration_ms field, got %#v", payload["event.duration_ms"])
	}
}
