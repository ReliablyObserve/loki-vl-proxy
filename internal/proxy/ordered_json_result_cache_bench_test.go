package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Exercise the dispatch boundary as well as the cache: a cold native request
// must not pay another retained response copy when it declines the JSON route.
func BenchmarkOrderedJSONResultCache(b *testing.B) {
	for _, pipeline := range []struct{ name, stage, tags string }{
		{"native", "", ""},
		{"json", "| json | drop __error__", ""},
		{"drilldown_json", "| json | drop __error__", "Source=grafana-lokiexplore-app"},
	} {
		for _, mode := range []string{"cold", "warm"} {
			b.Run(mode+"/"+pipeline.name, func(b *testing.B) {
				base := time.Unix(1700000400, 0).UTC()
				var rows []jsonVolumeLine
				for i := 0; i < 240; i++ {
					rows = append(rows, jsonVolumeFixtureLine(base.Add(time.Duration(i)*time.Second), i))
				}
				backend, _ := newJSONVolumeFakeVL(b, rows)
				c := cache.NewWithMaxBytes(time.Hour, 128, 8<<20)
				b.Cleanup(c.Close)
				p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				p.storeBackendVersion("v1.52.0", "v1.52.0")
				query := `sum by (level) (count_over_time({app="api"} ` + pipeline.stage + ` [1m]))`
				params := url.Values{"query": {query}, "start": {strconv.FormatInt(base.Add(time.Minute).Unix(), 10)}, "end": {strconv.FormatInt(base.Add(3*time.Minute).Unix(), 10)}, "step": {"60"}}
				target := "/loki/api/v1/query_range?" + params.Encode()
				newRequest := func() *http.Request {
					r := httptest.NewRequest(http.MethodGet, target, nil)
					if pipeline.tags != "" {
						r.Header.Set("X-Query-Tags", pipeline.tags)
					}
					return r
				}
				key := p.queryRangeCacheKey(newRequest(), query)
				request := func() []byte {
					w := httptest.NewRecorder()
					p.handleQueryRange(w, newRequest())
					if w.Code != http.StatusOK || w.Body.Len() == 0 {
						b.Fatalf("status=%d body=%s", w.Code, w.Body)
					}
					return w.Body.Bytes()
				}
				var initial struct {
					Data struct {
						ResultType string
						Result     []json.RawMessage
					}
				}
				body := request()
				if json.Unmarshal(body, &initial) != nil || initial.Data.ResultType != "matrix" || len(initial.Data.Result) == 0 {
					b.Fatalf("benchmark must answer a nonempty matrix: %s", body)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode == "cold" {
						b.StopTimer()
						c.Invalidate(key)
						b.StartTimer()
					}
					request()
				}
			})
		}
	}
}
