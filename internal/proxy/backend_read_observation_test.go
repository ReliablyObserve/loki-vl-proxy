package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
)

func TestBackendBodyReadProductionObservation(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	var log bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	body := p.observeBackendBody(context.Background(), io.NopCloser(strings.NewReader("fixture")), "/select/logsql/query", url.Values{"start": {"1700000400000000000"}, "end": {"1700000460000000000"}, "query": {"secret-selector"}}, time.Now())
	got, err := io.ReadAll(body)
	if err != nil || string(got) != "fixture" {
		t.Fatal("observer changed bytes", err)
	}
	if err = body.Close(); err != nil {
		t.Fatal(err)
	}
	if err = body.Close(); err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	if err = json.Unmarshal(log.Bytes(), &event); err != nil {
		t.Fatal("missing or duplicate completion event", err, log.String())
	}
	if event["decoded_bytes"] != float64(7) || event["read_span_seconds"] != float64(60) || event["outcome"] != "eof" {
		t.Fatalf("unusable body observation: %s", log.String())
	}
	if strings.Contains(log.String(), "secret-selector") {
		t.Fatal("query leaked into observation")
	}
	w := httptest.NewRecorder()
	p.metrics.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(w.Body.String(), `loki_vl_proxy_internal_operation_total{operation="backend_body_read",outcome="eof"} 1`) {
		t.Fatal("missing completion metric")
	}
	if !strings.Contains(w.Body.String(), `loki_vl_proxy_backend_read_bytes_total{kind="raw"} 7`) {
		t.Fatal("missing raw body-byte metric")
	}
}

func TestBackendBodyReadObservationPreservesErrors(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	for _, outcome := range []string{"closed", "error", "canceled", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			var log bytes.Buffer
			p.log = slog.New(slog.NewJSONHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if outcome == "timeout" {
				var release context.CancelFunc
				ctx, release = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer release()
			}
			var reader io.Reader = strings.NewReader("fixture")
			boom := errors.New("controlled failure")
			if outcome == "error" {
				reader = iotest.ErrReader(boom)
			}
			body := p.observeBackendBody(ctx, io.NopCloser(reader), "/select/logsql/query", nil, time.Now())
			if outcome == "canceled" {
				cancel()
			}
			if outcome == "error" {
				if _, err := io.ReadAll(body); !errors.Is(err, boom) {
					t.Fatal("read error changed", err)
				}
			}
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
			var event map[string]any
			if err := json.Unmarshal(log.Bytes(), &event); err != nil {
				t.Fatal(err)
			}
			if event["outcome"] != outcome {
				t.Fatalf("wrong event: %s", log.String())
			}
			if _, ok := event["read_span_seconds"]; ok {
				t.Fatal("fabricated span for unspecified bounds")
			}
		})
	}
}

func TestBackendBodyReadObservationPreservesResponseCap(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	ctx := withLabelValuesResponseCap(context.Background(), 512<<20)
	body := p.observeBackendBody(ctx, p.guardBackendBody(ctx, io.NopCloser(strings.NewReader("x")), "/select/logsql/field_values", nil, time.Now()), "/select/logsql/field_values", nil, time.Now())
	defer body.Close()
	if cap, ok := body.(interface{ BodyLimit() int64 }); !ok || cap.BodyLimit() != 512<<20 {
		t.Fatal("observer hid body cap from coalescer")
	}
}

type observedTestBody struct {
	read  func([]byte) (int, error)
	close func() error
}

func (b observedTestBody) Read(p []byte) (int, error) { return b.read(p) }
func (b observedTestBody) Close() error               { return b.close() }

func TestBackendBodyReadObservationTerminalErrorsAndClose(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	var log bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	readErr, closeErr := errors.New("private read detail"), errors.New("private close detail")
	var closes atomic.Int64
	body := p.observeBackendBody(context.Background(), observedTestBody{
		read:  func(b []byte) (int, error) { return copy(b, "abc"), readErr },
		close: func() error { closes.Add(1); return closeErr },
	}, "/select/logsql/query", nil, time.Now())
	buf := make([]byte, 10)
	if n, err := body.Read(buf); n != 3 || !errors.Is(err, readErr) || string(buf[:n]) != "abc" {
		t.Fatal("terminal read changed", n, err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if err := body.Close(); !errors.Is(err, closeErr) {
				t.Error("close error changed", err)
			}
		})
	}
	wg.Wait()
	if closes.Load() != 1 {
		t.Fatal("underlying close repeated")
	}
	var event map[string]any
	if err := json.Unmarshal(log.Bytes(), &event); err != nil || event["decoded_bytes"] != float64(3) || event["outcome"] != "error" {
		t.Fatal("bad error observation", err, log.String())
	}
	if strings.Contains(log.String(), "private") {
		t.Fatal("error detail leaked")
	}
	if cap, ok := body.(interface{ BodyLimit() int64 }); !ok || cap.BodyLimit() != 0 {
		t.Fatal("invented response cap")
	}
}

func TestBackendBodyReadObservationCloseInterruptsRead(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	reader, writer := io.Pipe()
	defer writer.Close()
	body := p.observeBackendBody(context.Background(), reader, "/select/logsql/query", nil, time.Now())
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(body); done <- err }()
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Read")
	}
}
