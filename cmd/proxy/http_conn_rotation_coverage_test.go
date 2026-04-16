package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPConnRotator_HelperCoverage(t *testing.T) {
	if rotator := newHTTPConnRotator(httpConnRotationConfig{}, nil, nil); rotator != nil {
		t.Fatal("expected zero-value rotation config not to create a rotator")
	}

	rotator := newHTTPConnRotator(httpConnRotationConfig{
		maxAge:         -time.Second,
		maxAgeJitter:   -time.Second,
		maxRequests:    3,
		overloadMaxAge: -time.Second,
	}, nil, nil)
	if rotator == nil {
		t.Fatal("expected non-zero config to create a rotator")
	}
	if rotator.cfg.maxAge != 0 || rotator.cfg.maxAgeJitter != 0 || rotator.cfg.overloadMaxAge != 0 {
		t.Fatalf("expected negative durations to clamp to zero, got %+v", rotator.cfg)
	}

	if got := rotator.Wrap(nil); got != nil {
		t.Fatalf("expected Wrap(nil) to return nil, got %#v", got)
	}

	var nilRotator *httpConnRotator
	if got := nilRotator.ConnContextHook(); got != nil {
		t.Fatal("expected nil rotator conn-context hook to be nil")
	}

	ageRotator := newHTTPConnRotator(httpConnRotationConfig{
		maxAge:       time.Minute,
		maxAgeJitter: time.Second,
		maxRequests:  10,
	}, nil, func() bool { return false })
	if ageRotator == nil {
		t.Fatal("expected age rotator")
	}

	connA, connB := net.Pipe()
	defer func() { _ = connA.Close() }()
	defer func() { _ = connB.Close() }()

	ctx := ageRotator.ConnContextHook()(context.Background(), connA)
	state, _ := ctx.Value(httpConnStateContextKey{}).(*httpConnState)
	if state == nil {
		t.Fatal("expected connection state in context")
	}
	if state.maxAge < 59*time.Second || state.maxAge > 61*time.Second {
		t.Fatalf("unexpected jittered maxAge %s", state.maxAge)
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.test/loki/api/v1/query_range", nil).WithContext(ctx)
	req.ProtoMajor = 1
	state.acceptedAt = time.Now().Add(-2 * time.Minute)
	if reason := ageRotator.recordAndDecide(req, state, time.Now()); reason != "age" {
		t.Fatalf("expected age-based rotation, got %q", reason)
	}

	req.Header.Set("Upgrade", "websocket")
	if reason := ageRotator.recordAndDecide(req, state, time.Now()); reason != "" {
		t.Fatalf("expected websocket upgrade to bypass rotation, got %q", reason)
	}

	if got := jitterDuration(5*time.Second, 0, connA, 1); got != 5*time.Second {
		t.Fatalf("expected no-jitter duration to equal base, got %s", got)
	}
}
