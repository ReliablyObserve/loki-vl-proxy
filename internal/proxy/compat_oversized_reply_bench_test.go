package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// The edge cache middleware around a handler answering a 4 MB metric reply,
// larger than one entry of the default edge cache (2.56 MiB): Miss is a new
// request every time (the reply is computed and stored), Repeat the same
// request again (stored replies are served without the handler).
func BenchmarkCompatOversizedReply(b *testing.B) {
	body := append([]byte(`{"status":"success","data":{"resultType":"matrix","result":[`), bytes.Repeat([]byte(`{"metric":{"pod":"api-0001"},"values":[[1700000000,"1"]]},`), 4<<20/56)...)
	body = append(body[:len(body)-1], []byte(`]}}`)...)
	for _, mode := range []string{"Miss", "Repeat"} {
		b.Run(mode, func(b *testing.B) {
			primary := cache.NewWithMaxBytes(time.Hour, 1<<20, 256<<20)
			edge := cache.NewWithMaxBytes(time.Hour, 1<<20, 25<<20+600<<10)
			b.Cleanup(primary.Close)
			b.Cleanup(edge.Close)
			p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: primary, CompatCache: edge, LogLevel: "error"})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			handler := p.compatCacheMiddleware("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			})
			target := func(i int) string {
				return "/loki/api/v1/query_range?" + url.Values{"query": {`sum by (pod) (count_over_time({app="api"}[5m]))`},
					"start": {strconv.Itoa(1700000000 + i)}, "end": {strconv.Itoa(1700003600 + i)}, "step": {"60"}}.Encode()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n := i
				if mode == "Repeat" {
					n = 0
				}
				r := httptest.NewRequest(http.MethodGet, target(n), nil)
				r.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				handler(rec, r)
				if rec.Code != http.StatusOK || rec.Body.Len() != len(body) {
					b.Fatalf("status %d, %d bytes", rec.Code, rec.Body.Len())
				}
			}
		})
	}
}

// A Drilldown breakdown by pod over the last hour, about 60 KB, through the
// routes with an edge cache whose entry holds 6.5 KB. Repeat asks the same
// request again, which the edge cache alone could not keep; Miss moves the
// window a second each time, so every request is computed and stored.
func BenchmarkOversizedBreakdown(b *testing.B) {
	now := time.Now().Truncate(time.Second)
	var rows []vlemu.Row
	for i := 0; i < 650; i++ {
		rows = append(rows, vlemu.Row{TS: now.Add(-65*time.Minute + time.Duration(i)*6*time.Second).UnixNano(),
			Stream: fmt.Sprintf(`{app="api-gateway",env="production",pod="api-%04d"}`, i%300)})
	}
	for _, mode := range []string{"Miss", "Repeat"} {
		b.Run(mode, func(b *testing.B) {
			srv := httptest.NewServer((&vlemu.Emulator{T: b, Rows: rows, Field: "duration_ms", Version: "v1.50.0"}).Handler())
			b.Cleanup(srv.Close)
			primary := cache.NewWithMaxBytes(5*time.Second, 100000, 256<<20)
			edge := cache.NewWithMaxBytes(time.Hour, 100000, 64<<10)
			b.Cleanup(primary.Close)
			b.Cleanup(edge.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: primary, CompatCache: edge, LogLevel: "error", MaxStatsQuerySeries: 1_000_000, QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			end := now.Add(-5 * time.Minute)
			target := func(shift int) string {
				e := end.Add(-time.Duration(shift) * time.Second)
				return "/loki/api/v1/query_range?" + url.Values{"query": {`sum by (pod) (count_over_time({env="production"}[5m]))`},
					"start": {strconv.FormatInt(e.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(e.UnixNano(), 10)}, "step": {"60"}}.Encode()
			}
			headers := map[string]string{"X-Scope-OrgID": "0"}
			if rec := doCompatProxyRequest(p, target(0), headers); rec.Code != http.StatusOK || rec.Body.Len() <= edge.MaxEntrySizeBytes() {
				b.Fatalf("cold: status %d, %d bytes", rec.Code, rec.Body.Len())
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				shift := 0
				if mode == "Miss" {
					shift = i + 1
				}
				if rec := doCompatProxyRequest(p, target(shift), headers); rec.Code != http.StatusOK {
					b.Fatalf("status %d", rec.Code)
				}
			}
		})
	}
}
