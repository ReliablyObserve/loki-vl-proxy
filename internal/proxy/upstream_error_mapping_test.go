package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestStatusFromUpstreamErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "nil", err: nil, want: http.StatusBadGateway},
		{name: "context canceled", err: context.Canceled, want: 499},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: http.StatusGatewayTimeout},
		{name: "net timeout", err: timeoutErr{}, want: http.StatusGatewayTimeout},
		{name: "wrapped canceled", err: errors.New("upstream: context canceled"), want: 499},
		{name: "wrapped timeout", err: errors.New("upstream deadline exceeded"), want: http.StatusGatewayTimeout},
		{name: "generic", err: errors.New("connection refused"), want: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := statusFromUpstreamErr(tt.err); got != tt.want {
				t.Fatalf("statusFromUpstreamErr(%v)=%d want=%d", tt.err, got, tt.want)
			}
		})
	}
}

func TestHandleLabels_MapsCanceledContextTo499(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"values":["service.name","level"]}`))
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	p.handleLabels(rec, req)

	if rec.Code != 499 {
		t.Fatalf("expected 499 for canceled request context, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp["errorType"] != "canceled" {
		t.Fatalf("expected errorType=canceled, got %v", resp["errorType"])
	}
}

func TestHandleLabels_MapsUpstreamTimeoutTo504(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		w.Write([]byte(`{"values":["service.name","level"]}`))
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	p.client.Timeout = 20 * time.Millisecond

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	rec := httptest.NewRecorder()
	p.handleLabels(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 for upstream timeout, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp["errorType"] != "timeout" {
		t.Fatalf("expected errorType=timeout, got %v", resp["errorType"])
	}
}
