package proxy

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// benchQueryRequest measures one request of query against the sliding fake VictoriaLogs (8 apps, three hours of
// lines): query_range over the hour before end at step 15s, or an instant query at end when instant is set. cold is a
// first request on an empty cache, repeat the identical request again. It reports the VictoriaLogs calls per request.
func benchQueryRequest(b *testing.B, query string, instant bool, mode string) {
	end := time.Unix(1700003600, 0).UTC()
	rng := rand.New(rand.NewSource(1))
	var lines []slidingFixtureLine
	for i, app := range []string{"api-gateway", "web", "db-main", "cache", "queue", "auth", "mail", "search"} {
		pace := time.Duration(2+i) * time.Second
		for ts := end.Add(-3 * time.Hour); ts.Before(end); ts = ts.Add(pace + time.Duration(rng.Intn(500))*time.Millisecond) {
			lines = append(lines, slidingFixtureLine{ts: ts.UnixNano(), app: app, msg: "x"})
		}
	}
	srv, fake := newSlidingFakeVL(b, lines)
	shimSrv, _ := newExtentShim(b, srv.Config.Handler)
	newProxy := func() *Proxy {
		c := cache.NewWithMaxBytes(time.Minute, 100000, 256<<20)
		p, err := New(Config{BackendURL: shimSrv.URL, Cache: c, LogLevel: "error", QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour})
		if err != nil {
			b.Fatal(err)
		}
		p.storeBackendVersion("v1.50.0", "v1.50.0")
		b.Cleanup(func() { _ = p.Shutdown(context.Background()); c.Close() })
		return p
	}
	ask := func(p *Proxy) {
		v := url.Values{"query": {query}}
		target := "/loki/api/v1/query_range?"
		if instant {
			target = "/loki/api/v1/query?"
			v.Set("time", strconv.FormatInt(end.UnixNano(), 10))
		} else {
			v.Set("start", strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10))
			v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
			v.Set("step", "15")
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target+v.Encode(), nil)
		if instant {
			p.handleQuery(rec, req)
		} else {
			p.handleQueryRange(rec, req)
		}
		p.statsExtentStores.wait()
		if rec.Code != http.StatusOK {
			b.Fatalf("HTTP %d: %s", rec.Code, rec.Body.String())
		}
	}
	calls := func() int {
		c, raw := fake.snapshot()
		return len(c) + raw
	}
	p := newProxy()
	if mode == "repeat" {
		ask(p)
	}
	before := calls()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if mode == "cold" {
			b.StopTimer()
			p = newProxy()
			b.StartTimer()
		}
		ask(p)
	}
	b.StopTimer()
	b.ReportMetric(float64(calls()-before)/float64(b.N), "vl-calls/op")
}

const (
	queryRequestPlainQuery  = `sum by (app) (count_over_time({env="production"}[5m]))`
	queryRequestRankQuery   = `topk(3, count_over_time({env="production"}[15s]) * 2)`
	queryRequestStddevQuery = `stddev(sum by (app) (count_over_time({env="production"}[15s])) * 2)`
)

func BenchmarkQueryRequestRangeCold(b *testing.B) {
	benchQueryRequest(b, queryRequestPlainQuery, false, "cold")
}
func BenchmarkQueryRequestRangeRepeat(b *testing.B) {
	benchQueryRequest(b, queryRequestPlainQuery, false, "repeat")
}
func BenchmarkQueryRequestInstantCold(b *testing.B) {
	benchQueryRequest(b, queryRequestPlainQuery, true, "cold")
}
func BenchmarkQueryRequestInstantRepeat(b *testing.B) {
	benchQueryRequest(b, queryRequestPlainQuery, true, "repeat")
}
func BenchmarkQueryRequestRankBinaryCold(b *testing.B) {
	benchQueryRequest(b, queryRequestRankQuery, false, "cold")
}
func BenchmarkQueryRequestRankBinaryRepeat(b *testing.B) {
	benchQueryRequest(b, queryRequestRankQuery, false, "repeat")
}
func BenchmarkQueryRequestStddevBinaryCold(b *testing.B) {
	benchQueryRequest(b, queryRequestStddevQuery, false, "cold")
}
func BenchmarkQueryRequestStddevBinaryRepeat(b *testing.B) {
	benchQueryRequest(b, queryRequestStddevQuery, false, "repeat")
}
