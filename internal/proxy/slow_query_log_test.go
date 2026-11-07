package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// slowLogProxy is a proxy whose VictoriaLogs answers each query after delay, logging JSON into the returned buffer.
func slowLogProxy(t *testing.T, delay time.Duration) (*Proxy, *bytes.Buffer) {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		_, _ = io.WriteString(w, `{"_time":"2026-11-07T10:00:00Z","_msg":"x"}`+"\n")
	}))
	t.Cleanup(backend.Close)
	p := newTestProxy(t, backend.URL)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	p.slowQueryThreshold, p.slowUpstreamThreshold, p.slowQueryLogRawQueries = 30*time.Millisecond, 15*time.Millisecond, true
	return p, &buf
}

// slowLogHandler rewrites the request's LogQL, runs a proxy-side operation and sends one VictoriaLogs query.
func slowLogHandler(p *Proxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		setRequestQuery(r, `sum(count_over_time({app="api"}[1m]))`)
		p.observeInternalOperation(r.Context(), "patterns_state", "read", 3*time.Millisecond)
		resp, err := p.vlGet(r.Context(), "/select/logsql/query", url.Values{"query": {`app:="api" | stats count() c`}, "start": {"1"}, "end": {"2"}})
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		w.WriteHeader(http.StatusOK)
	}
}

// slowLogLines returns the log lines with the given message.
func slowLogLines(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func serveSlowLog(p *Proxy, endpoint, route string, h http.HandlerFunc) {
	req := httptest.NewRequest(http.MethodGet, route+"?query="+url.QueryEscape(`count_over_time({app="api"}[1m])`), nil)
	p.requestLogger(endpoint, route, h).ServeHTTP(httptest.NewRecorder(), req)
}

// A request over -slow-query-threshold logs one slow_request with its LogQL and its steps, in order and with
// their offsets: the LogQL rewrite, the proxy-side operation, and the VictoriaLogs call with its LogsQL, window,
// status, bytes and time; the VictoriaLogs call over -slow-upstream-threshold logs its own line; both are
// counted by system and route.
//
// conformance: observability-slow-query-log
func TestSlowQueryLogCarriesTheQueriesAndTheSteps(t *testing.T) {
	p, buf := slowLogProxy(t, 40*time.Millisecond)
	serveSlowLog(p, "query_range", "/loki/api/v1/query_range", slowLogHandler(p))
	slow := slowLogLines(t, buf, "slow_request")
	if len(slow) != 1 {
		t.Fatalf("slow_request lines: %d\n%s", len(slow), buf)
	}
	line := slow[0]
	if line["loki.query"] != `count_over_time({app="api"}[1m])` || line["level"] != "WARN" {
		t.Fatalf("slow_request %v", line)
	}
	steps, _ := line["proxy.steps"].([]any)
	var kinds []string
	for _, s := range steps {
		step := s.(map[string]any)
		kinds = append(kinds, step["kind"].(string))
		if step["kind"] == "upstream" && (step["query"] != `app:="api" | stats count() c` || step["window"] != "start=1 end=2" || step["status"] != float64(200) || step["bytes"].(float64) <= 0 || step["duration_ms"].(float64) < 40) {
			t.Fatalf("upstream step %v", step)
		}
		if step["kind"] == "rewrite" && (step["from"] != `count_over_time({app="api"}[1m])` || step["to"] != `sum(count_over_time({app="api"}[1m]))`) {
			t.Fatalf("rewrite step %v", step)
		}
	}
	if strings.Join(kinds, ",") != "rewrite,operation,upstream" {
		t.Fatalf("steps %v, want rewrite, operation, upstream", kinds)
	}
	up := slowLogLines(t, buf, "slow_upstream_request")
	if len(up) != 1 || up[0]["vl.query"] != `app:="api" | stats count() c` || up[0]["http.parent_route"] != "/loki/api/v1/query_range" || up[0]["proxy.background"] != nil {
		t.Fatalf("slow_upstream_request %v", up)
	}
	body := metricsBody(p)
	for _, want := range []string{
		`loki_vl_proxy_slow_requests_total{system="loki",route="/loki/api/v1/query_range"} 1`,
		`loki_vl_proxy_slow_requests_total{system="vl",route="/select/logsql/query"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %s", want)
		}
	}
}

// Under the thresholds nothing is logged or counted as slow; a live tail is never a slow request; with
// -slow-query-log-raw-queries=false the queries are their hashes.
//
// conformance: observability-slow-query-log
func TestSlowQueryLogThresholdsTailAndRedaction(t *testing.T) {
	p, buf := slowLogProxy(t, 0)
	p.slowQueryThreshold, p.slowUpstreamThreshold = time.Hour, time.Hour
	serveSlowLog(p, "query_range", "/loki/api/v1/query_range", slowLogHandler(p))
	if n := len(slowLogLines(t, buf, "slow_request")) + len(slowLogLines(t, buf, "slow_upstream_request")); n != 0 || strings.Contains(metricsBody(p), "loki_vl_proxy_slow_requests_total{") {
		t.Fatalf("under the thresholds: %d slow lines\n%s", n, buf)
	}

	p, buf = slowLogProxy(t, 40*time.Millisecond)
	serveSlowLog(p, "tail", "/loki/api/v1/tail", slowLogHandler(p))
	if n := len(slowLogLines(t, buf, "slow_request")); n != 0 {
		t.Fatalf("a live tail logged %d slow_request lines", n)
	}

	p, buf = slowLogProxy(t, 40*time.Millisecond)
	p.slowQueryLogRawQueries = false
	serveSlowLog(p, "query_range", "/loki/api/v1/query_range", slowLogHandler(p))
	slow := slowLogLines(t, buf, "slow_request")
	if len(slow) != 1 || !strings.HasPrefix(slow[0]["loki.query"].(string), "sha256:") || strings.Contains(buf.String(), `app:=\"api\"`) {
		t.Fatalf("redacted slow line %v\n%s", slow, buf)
	}
}

// A request's trace keeps at most slowTraceMaxSteps steps and counts the rest; a VictoriaLogs call that outlives
// its request (a background build) adds no step to it, and its slow line says it is background work.
//
// conformance: observability-slow-query-log
func TestSlowQueryLogTraceBoundsAndBackgroundWork(t *testing.T) {
	p, buf := slowLogProxy(t, 40*time.Millisecond)
	done := make(chan struct{})
	serveSlowLog(p, "query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < slowTraceMaxSteps+5; i++ {
			p.observeInternalOperation(r.Context(), "op", "x", 0)
		}
		ctx := r.Context()
		go func() {
			defer close(done)
			time.Sleep(60 * time.Millisecond) // after the request's line
			resp, err := p.vlGet(ctx, "/select/logsql/query", url.Values{"query": {"*"}})
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
		time.Sleep(35 * time.Millisecond)
	})
	<-done
	slow := slowLogLines(t, buf, "slow_request")
	if len(slow) != 1 || len(slow[0]["proxy.steps"].([]any)) != slowTraceMaxSteps || slow[0]["proxy.steps_dropped"] != float64(5) {
		t.Fatalf("bounded trace: %v", slow)
	}
	up := slowLogLines(t, buf, "slow_upstream_request")
	if len(up) != 1 || up[0]["proxy.background"] != true {
		t.Fatalf("background call's slow line %v", up)
	}
}
