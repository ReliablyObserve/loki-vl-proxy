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

// The L3 request of the label_replace A/B set (topk over a label_replace over a
// grouped count, 5m range, 15s step, one hour), against a fake VictoriaLogs.
// cold is a first request on an empty cache; repeat the identical request again;
// moved the same request one minute further back each time, over a stretch the
// cache holds after its first pass. vl-calls/op is what VictoriaLogs was asked
// per request.
const rankedTopKBenchQuery = `topk(3, label_replace(sum by (app) (count_over_time({env="production"}[5m])), "app_short", "$1", "app", "(.*)-.*"))`

func benchRankedTopK(b *testing.B, mode string) {
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
	ask := func(p *Proxy, shift time.Duration) {
		e := end.Add(-shift)
		v := url.Values{"query": {rankedTopKBenchQuery}, "start": {strconv.FormatInt(e.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(e.UnixNano(), 10)}, "step": {"15"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil)
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, req)
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
	if mode != "cold" {
		ask(p, 0)
		for i := 1; i <= 60; i++ {
			ask(p, time.Duration(i)*time.Minute)
		}
	}
	before := calls()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch mode {
		case "cold":
			b.StopTimer()
			p = newProxy()
			b.StartTimer()
			ask(p, 0)
		case "repeat":
			ask(p, 0)
		case "moved":
			ask(p, time.Duration(1+i%60)*time.Minute)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(calls()-before)/float64(b.N), "vl-calls/op")
}

func BenchmarkRankedTopKCold(b *testing.B)   { benchRankedTopK(b, "cold") }
func BenchmarkRankedTopKRepeat(b *testing.B) { benchRankedTopK(b, "repeat") }
func BenchmarkRankedTopKMoved(b *testing.B)  { benchRankedTopK(b, "moved") }
