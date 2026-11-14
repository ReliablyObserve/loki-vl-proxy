package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// limitStubVL answers /select/logsql/query with as many rows as the call's
// limit asks for, up to rows, and records the limits and the calls it saw.
type limitStubVL struct {
	mu     sync.Mutex
	rows   int
	calls  int
	limits []string
}

func (s *limitStubVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	s.mu.Lock()
	s.calls++
	if r.URL.Path == "/select/logsql/query" {
		s.limits = append(s.limits, r.Form.Get("limit"))
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/select/logsql/query":
		n := s.rows
		if limit, err := strconv.Atoi(r.Form.Get("limit")); err == nil && limit < n {
			n = limit
		}
		var b strings.Builder
		base := time.Now().Add(-time.Minute)
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":"line %d","app":"api"}`+"\n",
				base.Add(-time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
		}
		_, _ = w.Write([]byte(b.String()))
	case "/select/logsql/hits":
		_, _ = w.Write([]byte(`{"hits":[]}`))
	default:
		_, _ = w.Write([]byte(`{"values":[]}`))
	}
}

func (s *limitStubVL) snapshot() (calls int, limits []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, append([]string(nil), s.limits...)
}

func defaultLimitTestProxy(t *testing.T, backendURL string, maxLines, maxEntries int) http.Handler {
	t.Helper()
	cfg := Config{BackendURL: backendURL, Cache: cache.New(time.Minute, 1000), CompatCache: cache.New(time.Minute, 100), LogLevel: "error", MaxLines: maxLines}
	if maxEntries > 0 {
		cfg.TenantDefaultLimits = map[string]any{"max_entries_limit_per_query": float64(maxEntries)}
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return mux
}

func streamEntryCount(t *testing.T, body []byte) int {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if resp.Data.ResultType != "streams" {
		t.Fatalf("resultType %q, want streams: %s", resp.Data.ResultType, body)
	}
	n := 0
	for _, s := range resp.Data.Result {
		n += len(s.Values)
	}
	return n
}

// TestLogQueryWithoutLimitReturnsLokisDefault: a range log query that sends no
// limit gets Loki's default of 100 entries. The recorded
// Loki v3.7 answers to `{namespace="prod"} | logfmt --strict` and
// `{app="api-gateway"}` over an hour with no limit hold exactly 100 entries
// (the proxy answered 1000). An explicit limit and -max-lines still apply, and
// a repeat is served from the cache.
// conformance: semantics/log-query-default-limit
func TestLogQueryWithoutLimitReturnsLokisDefault(t *testing.T) {
	now := statsTestNow()
	window := url.Values{"start": {fmt.Sprint(now.Add(-time.Hour).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}}
	cases := []struct {
		name     string
		path     string
		params   url.Values
		maxLines int
		// maxEntries is the tenants' max_entries_limit_per_query (0: the default).
		maxEntries int
		want       int
	}{
		{name: "query_range no limit", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}}, want: 100},
		{name: "query_range no limit with a parser", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"} | logfmt --strict`}, "step": {"60"}}, want: 100},
		{name: "query_range no limit forward", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}, "direction": {"forward"}}, want: 100},
		{name: "query_range explicit limit", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}, "limit": {"250"}}, want: 250},
		{name: "query_range explicit limit 1", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}, "limit": {"1"}}, want: 1},
		{name: "query_range no limit, -max-lines 1000", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}}, maxLines: 1000, want: 300},
		{name: "query_range no limit, -max-lines 40", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}}, maxLines: 40, want: 40},
		// A tenant whose max_entries_limit_per_query is below the default keeps
		// getting that many entries, as before.
		{name: "query_range no limit, max_entries_limit_per_query 30", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}}, maxEntries: 30, want: 30},
		{name: "query_range no limit, max_entries_limit_per_query 500", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{app="api"}`}}, maxEntries: 500, want: 100},
		{name: "metric query_range no limit", path: "/loki/api/v1/query_range", params: url.Values{"query": {`count_over_time({app="api"}[1m])`}, "step": {"60"}}, want: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &limitStubVL{rows: 300}
			vl := httptest.NewServer(stub)
			defer vl.Close()
			h := defaultLimitTestProxy(t, vl.URL, tc.maxLines, tc.maxEntries)
			params := url.Values{}
			for k, v := range tc.params {
				params[k] = v
			}
			if tc.path == "/loki/api/v1/query_range" {
				for k, v := range window {
					params[k] = v
				}
			}
			body := serveQueryStats(t, h, tc.path, params, nil)
			if tc.want < 0 {
				// A metric query reads no entry limit: it is answered as a matrix.
				if !strings.Contains(string(body), `"resultType":"matrix"`) {
					t.Fatalf("metric query: %s, want a matrix", body)
				}
				return
			}
			if got := streamEntryCount(t, body); got != tc.want {
				t.Fatalf("%d entries, want %d", got, tc.want)
			}
			calls, limits := stub.snapshot()
			if len(limits) == 0 {
				t.Fatalf("no VictoriaLogs log query was made")
			}
			wantLimit := tc.params.Get("limit")
			if wantLimit == "" {
				wantLimit = "100"
				if tc.maxLines > 0 {
					wantLimit = strconv.Itoa(tc.maxLines)
				}
				if tc.maxEntries > 0 && tc.maxEntries < mustAtoi(t, wantLimit) {
					wantLimit = strconv.Itoa(tc.maxEntries)
				}
			}
			for _, l := range limits {
				if n, err := strconv.Atoi(l); err != nil || n > mustAtoi(t, wantLimit) {
					t.Errorf("VictoriaLogs log query limit %q, want at most %s (all calls: %q)", l, wantLimit, limits)
				}
			}
			// The repeat is a cache hit: no VictoriaLogs call.
			again := serveQueryStats(t, h, tc.path, params, nil)
			if got := streamEntryCount(t, again); got != tc.want {
				t.Fatalf("repeat: %d entries, want %d", got, tc.want)
			}
			if after, _ := stub.snapshot(); after != calls {
				t.Errorf("repeat made %d VictoriaLogs calls, want 0", after-calls)
			}
		})
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
