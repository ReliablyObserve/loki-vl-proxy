package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkVLRefusalSuccessPath measures successful requests on a proxy that knows its VictoriaLogs release (so
// every VictoriaLogs call goes through the refusal prediction's screen): a log query_range miss and a metric
// query_range miss (stats_query_range), each with a new window per iteration, against a stub VictoriaLogs. The
// prediction must not make the success path slower.
func BenchmarkVLRefusalSuccessPath(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/stats_query_range"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[1700000060,"3"]]}]}}`))
		case strings.HasPrefix(r.URL.Path, "/select/"):
			_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer backend.Close()
	p, err := New(Config{
		BackendURL:  backend.URL,
		Cache:       cache.New(60*time.Second, 100000),
		CompatCache: cache.New(60*time.Second, 100000),
		LogLevel:    "error",
		ErrorCache:  ErrorCacheConfig{TransientTTL: time.Second, DataTTL: 5 * time.Second, RefusalTTL: 5 * time.Minute},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(b.Context()) })
	p.storeBackendVersion("bench", "v1.53.0")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	run := func(b *testing.B, query string, step bool, from int) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			start := from + i*60
			target := "/loki/api/v1/query_range?query=" + url.QueryEscape(query) + "&start=" + strconv.Itoa(start) + "&end=" + strconv.Itoa(start+3600)
			if step {
				target += "&step=60"
			} else {
				target += "&limit=100"
			}
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
		}
	}
	b.Run("log_miss", func(b *testing.B) { run(b, `{app="api"} |= "hello"`, false, 1700000000) })
	b.Run("metric_miss", func(b *testing.B) {
		run(b, `sum by (level) (count_over_time({app="api"}[1m]))`, true, 1700000000)
	})
	// Another query's refusal is held for every window: each miss also reads the held-query set.
	holdOther(p)
	b.Run("metric_miss_while_held", func(b *testing.B) {
		run(b, `sum by (level) (count_over_time({app="api"}[1m]))`, true, 1600000000)
	})
}

// holdOther marks another query's refusal as held for every window for an hour.
func holdOther(p *Proxy) {
	until := cache.Now().Add(time.Hour).UnixNano()
	p.refusalQueries.add("query_range", `{app="other"}`, until, 4096)
	raiseTo(&p.refusalSeenUntil, until)
}
