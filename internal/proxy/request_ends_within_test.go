package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// An instant query ends at its evaluation time, Loki's time parameter; a range
// query ends at end (or to). Loki reads time on the instant endpoint only and
// end only on the others, so each side ignores the other's parameter.
//
// conformance: data-probing-and-freshness, loki_api_v1_query, loki_api_v1_query_range
func TestRequestEndsWithinReadsTheEndpointsEndParameter(t *testing.T) {
	const window = 2 * time.Minute
	now := time.Now()
	ns := func(ago time.Duration) string { return strconv.FormatInt(now.Add(-ago).UnixNano(), 10) }
	sec := func(ago time.Duration) string { return strconv.FormatInt(now.Add(-ago).Unix(), 10) }
	for _, tc := range []struct {
		name     string
		endpoint string
		params   url.Values
		aligned  string // the end the client sent before step alignment, when set
		want     bool
	}{
		{"instant past time", "query", url.Values{"time": {ns(30 * time.Minute)}}, "", false},
		{"instant past time in seconds", "query", url.Values{"time": {sec(30 * time.Minute)}}, "", false},
		{"instant past time in RFC3339", "query", url.Values{"time": {now.Add(-30 * time.Minute).UTC().Format(time.RFC3339)}}, "", false},
		{"instant no time is now", "query", url.Values{}, "", true},
		{"instant empty time is now", "query", url.Values{"time": {" "}}, "", true},
		{"instant future time is clamped to now", "query", url.Values{"time": {ns(-time.Hour)}}, "", true},
		{"instant time inside the window", "query", url.Values{"time": {ns(time.Minute)}}, "", true},
		{"instant time at the window edge", "query", url.Values{"time": {ns(window - time.Second)}}, "", true},
		{"instant time just past the window", "query", url.Values{"time": {ns(window + time.Second)}}, "", false},
		{"instant ignores a stray past end", "query", url.Values{"end": {ns(30 * time.Minute)}}, "", true},
		{"instant ignores a stray recent end", "query", url.Values{"time": {ns(30 * time.Minute)}, "end": {ns(0)}}, "", false},
		{"instant unparsable time", "query", url.Values{"time": {"yesterday"}}, "", false},
		{"range stray past time and no end is now", "query_range", url.Values{"time": {ns(30 * time.Minute)}}, "", true},
		{"range past end", "query_range", url.Values{"end": {ns(30 * time.Minute)}, "time": {ns(0)}}, "", false},
		{"range recent end", "query_range", url.Values{"end": {ns(time.Minute)}}, "", true},
		{"range to", "query_range", url.Values{"to": {ns(30 * time.Minute)}}, "", false},
		{"range step aligned keeps the client's end", "query_range", url.Values{"end": {ns(30 * time.Minute)}}, ns(time.Minute), true},
		{"range step aligned past client end", "query_range", url.Values{"end": {ns(0)}}, ns(30 * time.Minute), false},
		{"labels stray past time and no end is now", "labels", url.Values{"time": {ns(30 * time.Minute)}}, "", true},
		{"labels past end", "labels", url.Values{"end": {ns(30 * time.Minute)}}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/x?"+tc.params.Encode(), nil)
			if tc.aligned != "" {
				r = r.WithContext(context.WithValue(r.Context(), stepAlignedKey{}, tc.aligned))
			}
			if got := requestEndsWithin(r, tc.endpoint, window); got != tc.want {
				t.Fatalf("requestEndsWithin(%s, %v) = %v, want %v", tc.endpoint, tc.params, got, tc.want)
			}
		})
	}
	if requestEndsWithin(nil, "query", window) || requestEndsWithin(httptest.NewRequest(http.MethodGet, "/x", nil), "query", 0) {
		t.Fatal("no request or no window is never near now")
	}
}

// An instant query evaluated at a fixed past time is not near now: a repeat
// after max-staleness is the cached answer and asks VictoriaLogs nothing. An
// instant query without time is evaluated at now, so its repeat after
// max-staleness is evaluated again.
//
// conformance: data-probing-and-freshness, loki_api_v1_query
func TestInstantQueryAtAPastTimeIsReusedAfterMaxStaleness(t *testing.T) {
	var served atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/select/logsql/stats_query") {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"3"]}]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	defer backend.Close()
	c := cache.NewWithMaxBytes(time.Hour, 1024, 8<<20)
	defer c.Close()
	edge := cache.NewWithMaxBytes(time.Hour, 1024, 8<<20)
	defer edge.Close()
	p, err := New(Config{
		BackendURL:                    backend.URL,
		Cache:                         c,
		CompatCache:                   edge,
		LogLevel:                      "error",
		RecentTailRefreshEnabled:      true,
		RecentTailRefreshWindow:       2 * time.Minute,
		RecentTailRefreshMaxStaleness: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	origins := &vlCallOrigins{inner: p.client.Transport}
	p.client.Transport = origins
	headers := map[string]string{"X-Scope-OrgID": "0"}
	instant := func(params url.Values) string { return "/loki/api/v1/query?" + params.Encode() }
	query := `sum(count_over_time({app="api"}[5m]))`
	past := url.Values{"query": {query}, "time": {strconv.FormatInt(time.Now().Add(-30*time.Minute).UnixNano(), 10)}}
	nowRelative := url.Values{"query": {query}}
	for _, tc := range []struct {
		name   string
		target string
		reused bool
	}{
		{"past time", instant(past), true},
		{"no time", instant(nowRelative), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cold := doCompatProxyRequest(p, tc.target, withHeader(headers, vlCallOriginHeader, "cold-"+tc.name))
			if cold.Code != http.StatusOK || !strings.Contains(cold.Body.String(), `"3"`) || origins.made("cold-"+tc.name) == 0 {
				t.Fatalf("cold: status=%d calls=%d body=%s", cold.Code, origins.made("cold-"+tc.name), cold.Body)
			}
			// Older than max-staleness on the cache clock, well inside the TTL.
			restore := cache.AdvanceClockForTesting(3 * time.Second)
			defer restore()
			repeat := doCompatProxyRequest(p, tc.target, withHeader(headers, vlCallOriginHeader, "repeat-"+tc.name))
			// A reused answer is the cold one byte for byte; one evaluated again
			// carries the new evaluation time.
			if repeat.Code != http.StatusOK || !strings.Contains(repeat.Body.String(), `"3"`) || (tc.reused && repeat.Body.String() != cold.Body.String()) {
				t.Fatalf("repeat: status=%d body=%s want %s", repeat.Code, repeat.Body, cold.Body)
			}
			if calls := origins.made("repeat-" + tc.name); (calls == 0) != tc.reused {
				t.Fatalf("repeat made %d VictoriaLogs calls, reused=%v wanted", calls, tc.reused)
			}
		})
	}
	if untagged := origins.made(""); untagged != 0 {
		t.Fatalf("%d VictoriaLogs calls were made for no client request", untagged)
	}
}

// BenchmarkCompatCacheFreshnessCheck times the near-now check of a cache hit
// for an instant query at a past time and a range query ending now.
func BenchmarkCompatCacheFreshnessCheck(b *testing.B) {
	p := &Proxy{recentTailRefreshEnabled: true, recentTailRefreshWindow: 2 * time.Minute, recentTailRefreshMaxStaleness: 2 * time.Second}
	past := strconv.FormatInt(time.Now().Add(-30*time.Minute).UnixNano(), 10)
	for _, tc := range []struct{ name, endpoint, target string }{
		{"instant", "query", "/loki/api/v1/query?query=x&time=" + past},
		{"range", "query_range", "/loki/api/v1/query_range?query=x&end=" + strconv.FormatInt(time.Now().UnixNano(), 10)},
	} {
		r := httptest.NewRequest(http.MethodGet, tc.target, nil)
		_ = r.ParseForm()
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.compatCacheShouldBypassForFreshness(tc.endpoint, r, 5*time.Minute, 4*time.Minute, 10*time.Second)
			}
		})
		b.Run(tc.name+"/young", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.compatCacheShouldBypassForFreshness(tc.endpoint, r, 5*time.Minute, 5*time.Minute-time.Second, time.Second)
			}
		})
	}
}
