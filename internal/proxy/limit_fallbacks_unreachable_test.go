package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestLogQueryLimitReachesEveryReaderAsAPositiveInteger proves that the log
// query code reads a limit that is always a positive integer, so it keeps no
// fallback for an absent, empty, zero, negative or unparseable one. Each route
// to the readers (the hot read, the windowed read, the cold read, the hot and
// cold read, and the multi-tenant fan-out) is driven through the registered
// handler with every spelling of the limit. A spelling Loki rejects is answered
// with 400 and reaches no backend; every other one reaches the backends as a
// positive integer no larger than the answer's limit.
// conformance: semantics/log-query-default-limit
func TestLogQueryLimitReachesEveryReaderAsAPositiveInteger(t *testing.T) {
	now := time.Now()
	ns := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).UnixNano(), 10) }
	windows := map[string][2]string{
		"1h":            {ns(-time.Hour), ns(0)},
		"cold only":     {ns(-30 * 24 * time.Hour), ns(-20 * 24 * time.Hour)},
		"hot and cold":  {ns(-10 * 24 * time.Hour), ns(-time.Hour)},
		"sent no start": {"", ""},
	}
	routes := []struct {
		name     string
		window   string
		windowed bool
		cold     bool
		tenant   string
	}{
		{name: "hot", window: "1h"},
		{name: "windowed", window: "1h", windowed: true},
		{name: "cold only", window: "cold only", cold: true},
		{name: "hot and cold", window: "hot and cold", cold: true},
		{name: "hot and cold, windowed", window: "hot and cold", cold: true, windowed: true},
		{name: "no start or end", window: "sent no start"},
		{name: "multi-tenant fan-out", window: "1h", tenant: "0|1"},
	}
	// Each spelling of the limit parameter: want is the limit the backends see
	// under the default limit of 100 (0: the request is rejected as Loki does).
	spellings := []struct {
		name  string
		query string
		want  int
	}{
		{"absent", "", 100},
		{"empty", "&limit=", 100},
		{"empty then a value", "&limit=&limit=7", 100},
		{"a value then empty", "&limit=7&limit=", 7},
		{"value", "&limit=7", 7},
		{"plus sign", "&limit=%2B7", 7},
		{"leading zeros", "&limit=0007", 7},
		{"above the tenant cap", "&limit=5000", 0}, // Loki rejects it: "max entries limit per query exceeded"
		{"at the tenant cap", "&limit=250", 250},
		{"zero", "&limit=0", 0},
		{"negative", "&limit=-1", 0},
		{"not a number", "&limit=abc", 0},
		{"fraction", "&limit=1.5", 0},
		{"space", "&limit=%205", 0},
		{"beyond int", "&limit=99999999999999999999999", 0},
		{"zero then a value", "&limit=0&limit=7", 0},
	}
	for _, route := range routes {
		for _, direction := range []string{"backward", "forward"} {
			for _, sp := range spellings {
				name := fmt.Sprintf("%s/%s/%s", route.name, direction, sp.name)
				t.Run(name, func(t *testing.T) {
					hot := &limitStubVL{rows: 300}
					hotSrv := httptest.NewServer(hot)
					defer hotSrv.Close()
					cfg := Config{
						BackendURL:                 hotSrv.URL,
						Cache:                      cache.New(time.Minute, 1000),
						CompatCache:                cache.New(time.Minute, 100),
						LogLevel:                   "error",
						QueryRangeWindowingEnabled: route.windowed,
						QueryRangeSplitInterval:    15 * time.Minute,
						TenantDefaultLimits:        map[string]any{"max_entries_limit_per_query": float64(250)},
					}
					cold := &limitStubVL{rows: 300}
					if route.cold {
						coldSrv := httptest.NewServer(cold)
						defer coldSrv.Close()
						cfg.ColdBackend = ColdBackendConfig{Enabled: true, URL: coldSrv.URL, Boundary: 7 * 24 * time.Hour}
					}
					p, err := New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = p.Shutdown(t.Context()) }()
					mux := http.NewServeMux()
					p.RegisterRoutes(mux)

					w := windows[route.window]
					q := url.Values{"query": {`{app="api"}`}, "direction": {direction}}
					raw := q.Encode()
					if w[0] != "" {
						raw += "&start=" + w[0] + "&end=" + w[1]
					}
					raw += sp.query
					req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+raw, nil)
					req.Header.Set("X-Scope-OrgID", map[bool]string{true: route.tenant, false: "0"}[route.tenant != ""])
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)

					hotCalls, hotLimits := hot.snapshot()
					coldCalls, coldLimits := cold.snapshot()
					if sp.want == 0 {
						if rec.Code != http.StatusBadRequest {
							t.Fatalf("status %d, want 400: %.300s", rec.Code, rec.Body.String())
						}
						if hotCalls != 0 || coldCalls != 0 {
							t.Fatalf("a rejected limit reached a backend: hot %d calls, cold %d calls", hotCalls, coldCalls)
						}
						return
					}
					if rec.Code != http.StatusOK {
						t.Fatalf("status %d, want 200: %.300s", rec.Code, rec.Body.String())
					}
					limits := append(hotLimits, coldLimits...)
					if len(limits) == 0 {
						t.Fatalf("no backend call read a limit")
					}
					for _, got := range limits {
						n, err := strconv.Atoi(got)
						if err != nil || n <= 0 || n > sp.want {
							t.Fatalf("a backend call carried limit %q, want a positive integer of at most %d (all: hot %v, cold %v)", got, sp.want, hotLimits, coldLimits)
						}
					}
				})
			}
		}
	}
}

// BenchmarkSanitizeLimit is the limit step every log read pays: the cap of the
// limit the route already validated.
func BenchmarkSanitizeLimit(b *testing.B) {
	for _, limit := range []string{"100", "5000"} {
		b.Run(limit, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = sanitizeLimit(limit, 1000)
			}
		})
	}
}
