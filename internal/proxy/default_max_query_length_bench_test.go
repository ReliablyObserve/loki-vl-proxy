package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkTenantLimitsMiddleware_MaxQueryLength measures the limits
// middleware on a request inside the limit, with max_query_length off (the
// old flag default) and at Loki's 721h (the shipped default). The next handler
// reads the request's form, as every route handler does, so form parsing the
// middleware does is not counted twice.
func BenchmarkTenantLimitsMiddleware_MaxQueryLength(b *testing.B) {
	end := time.Unix(1794570675, 0)
	start := end.Add(-time.Hour)
	requests := []struct {
		endpoint, path string
		params         url.Values
	}{
		{"query_range", "/loki/api/v1/query_range", url.Values{"query": {`sum by (level) (rate({app="api-gateway"} | json | status>=500 [5m]))`}, "step": {"60"}}},
		{"labels", "/loki/api/v1/labels", url.Values{}},
	}
	noop := func(_ http.ResponseWriter, r *http.Request) {
		_, _ = r.FormValue("start"), r.FormValue("end")
	}
	for _, length := range []struct {
		name string
		d    time.Duration
	}{{"off", 0}, {"721h", 721 * time.Hour}} {
		c := cache.New(time.Minute, 100)
		p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error", DefaultMaxQueryLength: length.d})
		if err != nil {
			b.Fatal(err)
		}
		for _, rq := range requests {
			params := url.Values{}
			for k, v := range rq.params {
				params[k] = v
			}
			params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
			params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
			target := rq.path + "?" + params.Encode()
			handler := p.tenantLimitsMiddleware(rq.endpoint, noop)
			b.Run(rq.endpoint+"/"+length.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					handler(nil, httptest.NewRequest(http.MethodGet, target, nil))
				}
			})
		}
		c.Close()
	}
}
