package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
)

func TestGrafanaStatsFailure_PreservesFailures(t *testing.T) {
	p := newCompatTestProxy(t, "http://unused")
	for _, tc := range []struct {
		name   string
		err    error
		status int
		text   string
	}{
		{"backend_unavailable", p.redactedBackendStatusError("", 503, []byte("private backend unavailable")), 500, lokiErrInternal},
		{"connection_refused", &url.Error{Op: "Get", URL: "http://backend.private:3100/?query=private-selector", Err: syscall.ECONNREFUSED}, 500, lokiErrInternal},
		{"service_disappeared", &net.DNSError{Err: "no such host", Name: "backend.private", IsNotFound: true}, 500, lokiErrInternal},
		{"deadline", context.DeadlineExceeded, 504, lokiErrDeadlineExceeded},
		{"wrapped_deadline", fmt.Errorf("private backend: %w", context.DeadlineExceeded), 504, lokiErrDeadlineExceeded},
		{"cancelled", context.Canceled, 499, lokiErrClientCanceled},
		{"queue_full", p.redactedBackendStatusError("", 429, []byte("private backend queue full")), 429, lokiTooManyOutstandingRequests},
		{"query_rejected", p.redactedBackendStatusError("", 400, []byte(vlParseErrorBody)), 400, lokiErrParse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			p.writeGrafanaStatsFailure(w, tc.err)
			var body struct {
				Status, Error string
				Warnings      []string
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status || body.Status != "error" || body.Error != tc.text || len(body.Warnings) != 0 {
				t.Fatalf("failure was hidden or misclassified: status=%d body=%s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "private") || w.Header().Get(drilldownPartialUpstreamStatusHeader) != "" {
				t.Fatalf("failure exposed backend details: headers=%v body=%s", w.Header(), w.Body.String())
			}
		})
	}
}

func TestResponseCache_DoesNotStoreDrilldownPartialFailure(t *testing.T) {
	p := newCompatTestProxy(t, "http://unused")
	calls := 0
	h := p.compatCacheMiddleware("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			// Retain the cache guard for legacy partial-failure envelopes, even
			// though backend failures now use the normal error response path.
			w.Header().Set(drilldownPartialUpstreamStatusHeader, "503")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","warnings":["backend unavailable"],"data":{"resultType":"matrix","result":[]}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[1,"1"]]}]}}`))
	})
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=sum%28count_over_time%28%7Bapp%3D%22a%22%7D%5B5s%5D%29%29&start=1&end=2&step=1", nil))
		return w
	}
	first, second := request(), request()
	if calls != 2 || first.Header().Get(drilldownPartialUpstreamStatusHeader) == "" || second.Header().Get(drilldownPartialUpstreamStatusHeader) != "" || !json.Valid(second.Body.Bytes()) || second.Body.String() == first.Body.String() {
		t.Fatalf("partial failure was cached: calls=%d first=%s second=%s", calls, first.Body.String(), second.Body.String())
	}
}

func TestGrafanaStatsFailure_DoesNotTurnCancellationIntoSuccess(t *testing.T) {
	p := newCompatTestProxy(t, "http://unused")
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"cancelled", context.Canceled, 499},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			p.writeGrafanaStatsFailure(w, tc.err)
			if w.Code != tc.want || w.Header().Get(drilldownPartialUpstreamStatusHeader) != "" {
				t.Fatalf("stats response: status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}

func TestResponseCache_PrimaryExactWindows(t *testing.T) {
	calls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprintf(w, "{\"_time\":\"2026-01-01T00:00:01Z\",\"_msg\":%q,\"_stream\":\"{app=\\\"a\\\"}\"}\n", r.FormValue("start"))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	first := doCompatProxyRequest(p, "/loki/api/v1/query_range?query=%7Bapp%3D%22a%22%7D&start=1767225600&end=1767225602", nil)
	second := doCompatProxyRequest(p, "/loki/api/v1/query_range?query=%7Bapp%3D%22a%22%7D&start=1767225603&end=1767225604", nil)
	if first.Code != 200 || second.Code != 200 || calls != 2 || first.Body.String() == second.Body.String() {
		t.Fatalf("exact windows collide: calls=%d first=%s second=%s", calls, first.Body, second.Body)
	}
}

func TestResponseCache_PrimaryIncludesSemanticParameters(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	base := httptest.NewRequest("GET", "/loki/api/v1/query_range?start=1&end=10", nil)
	key := p.queryRangeCacheKey(base, `{app="a"}`)
	for _, field := range []string{"start", "end", "step", "limit", "direction", "interval", "since", "time"} {
		r := httptest.NewRequest("GET", "/loki/api/v1/query_range?start=1&end=10", nil)
		q := r.URL.Query()
		q.Set(field, "2")
		r.URL.RawQuery = q.Encode()
		if p.queryRangeCacheKey(r, `{app="a"}`) == key {
			t.Errorf("key omits %s", field)
		}
	}
}

func TestResponseCache_CompatTimeCollision(t *testing.T) {
	p := newCompatTestProxy(t, "http://unused")
	calls := 0
	h := p.compatCacheMiddleware("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]string{"start": r.FormValue("start"), "end": r.FormValue("end")})
	})
	request := func(start, end string) string {
		q := url.Values{"query": {`{app="a"}`}, "start": {start}, "end": {end}}
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil))
		return w.Body.String()
	}
	a := request("2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z")
	b := request("2026-01-01T00:00:03Z", "2026-01-01T00:00:04Z")
	if calls != 2 || a == b {
		t.Fatalf("response contract collision: calls=%d a=%s b=%s", calls, a, b)
	}
}

func TestResponseCache_CompatResponseContractCollision(t *testing.T) {
	p := newCompatTestProxy(t, "http://unused")
	p.emitStructuredMetadata = true
	calls := 0
	h := p.compatCacheMiddleware("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]string{"tuple": p.tupleModeCacheKey(r)})
	})
	request := func(flag string) string {
		r := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=x", nil)
		r.Header.Set("X-Loki-Response-Encoding-Flags", flag)
		w := httptest.NewRecorder()
		h(w, r)
		return w.Body.String()
	}
	a, b := request(""), request("categorize-labels")
	if calls != 2 || a == b {
		t.Fatalf("response contract collision: %d %s %s", calls, a, b)
	}
}
