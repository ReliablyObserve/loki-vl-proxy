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
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// BenchmarkRangeMetricFittingMiss times a range metric query_range that fits its read bounds through the shipped mux,
// on a response cache miss (every iteration a window of its own), against the VictoriaLogs emulator: the path the
// step layer (step_adapt.go) wraps and must not slow. It uses only what main has, so the same file times main.
func BenchmarkRangeMetricFittingMiss(b *testing.B) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var rows []vlemu.Row
	for i := 0; i < 2000; i++ {
		rows = append(rows, vlemu.Row{TS: base.Add(-time.Duration(i) * 7 * time.Second).UnixNano(), Stream: fmt.Sprintf(`{app="nginx",pod="p%d"}`, i%5), Value: 1, Msg: "line"})
	}
	emu := &vlemu.Emulator{Rows: rows, Field: "amount", Version: "v1.54.0", AlignStatsRange: true}
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 1000), CompatCache: cache.New(time.Minute, 1000), LogLevel: "error",
		AlignQueriesWithStep: true, BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(b.Context()) }()
	_ = p.ValidateBackendVersionCompatibility(b.Context())
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	query := url.QueryEscape(`sum by (pod) (count_over_time({app="nginx"}[1m]))`)
	serve := func(i int) {
		end := base.Add(-time.Duration(i%200) * time.Minute)
		target := "/loki/api/v1/query_range?query=" + query + "&step=60&start=" + strconv.FormatInt(end.Add(-time.Hour).UnixNano()+int64(i/200), 10) +
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
