package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkRangeMetricFittingMiss times a range metric query_range that fits its read bounds through the shipped mux,
// on a response cache miss (every iteration a window of its own), against a VictoriaLogs stub: the path the step
// layer (step_adapt.go) wraps and must not slow. It uses only what main has, so the same file times main.
func BenchmarkRangeMetricFittingMiss(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/stats_query_range") {
			start := r.Form.Get("start")
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"nginx"},"values":[[`+start+`,"3"]]}]}}`)
			return
		}
		_, _ = io.WriteString(w, `{"values":[]}`)
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 1000), CompatCache: cache.New(time.Minute, 1000), LogLevel: "error", AlignQueriesWithStep: true})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(b.Context()) }()
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	query := url.QueryEscape(`sum by (app) (count_over_time({app="nginx"}[1m]))`)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	serve := func(i int) {
		end := base.Add(-time.Duration(i) * 48 * time.Hour)
		target := "/loki/api/v1/query_range?query=" + query + "&step=60&start=" + strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10) +
			"&end=" + strconv.FormatInt(end.UnixNano(), 10)
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("%d %s", rec.Code, rec.Body)
		}
	}
	serve(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serve(i + 1)
	}
}
