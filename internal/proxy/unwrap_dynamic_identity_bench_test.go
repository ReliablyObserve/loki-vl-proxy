package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkDynamicIdentityRoutes measures sum, sum by and max by over a function taken per series after a json
// parser with no keep (lower.DynamicUnwrapIdentity) through the real handlers, against a stub VictoriaLogs that
// answers a row query with 2,000 rows over the window and a stats query with a small answer: the proxy's work on each
// route the shapes take, instant, a range equal to the step and a range longer than it. unique has one series per
// line (the one-sample proof holds); dup has 40 series of 50 lines each (the per-series evaluators answer).
func BenchmarkDynamicIdentityRoutes(b *testing.B) {
	b.Run("unique", func(b *testing.B) { benchDynamicIdentityRoutes(b, 2000) })
	b.Run("dup", func(b *testing.B) { benchDynamicIdentityRoutes(b, 40) })
}

func benchDynamicIdentityRoutes(b *testing.B, series int) {
	const rows = 2000
	end := time.Unix(1_800_000_600, 0)
	var raw bytes.Buffer
	for i := 0; i < rows; i++ {
		ts := end.Add(-time.Duration(rows-i) * 900 * time.Millisecond).UTC().Format(time.RFC3339Nano)
		fmt.Fprintf(&raw, `{"_time":%q,"_stream":"{app=\"x\",pod=\"p%d\"}","app":"x","_msg":"{\"n\":%d,\"req\":\"r%d\"}","n":"%d","req":"r%d","__lvp_sid":"%d","__lvp_v":"%d","__lvp_bad":"0"}`+"\n",
			ts, i%4, i%500, i, i%500, i, 1000+i%series, i%500)
	}
	rowBody := raw.Bytes()
	const (
		rangeBody   = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"x","__lvp_bad":"0","__lvp_v":"3"},"values":[[1799999700,"3"],[1800000000,"4"],[1800000300,"5"],[1800000600,"6"]]}]}}`
		instantBody = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"x","__lvp_bad":"0","__lvp_v":"3"},"value":[1800000600,"3"]}]}}`
	)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			_ = r.ParseForm()
			if strings.Contains(r.Form.Get("query"), "count_uniq_hash(") {
				// The one-sample proof: the rows and the distinct series hashes.
				_, _ = fmt.Fprintf(w, `{"n":"%d","u":"%d"}`, rows, series)
				return
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write(rowBody)
		case "/select/logsql/stats_query":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(instantBody))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(rangeBody))
		}
	}))
	defer backend.Close()
	c := cache.New(time.Millisecond, 10)
	defer c.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for _, tc := range []struct{ name, endpoint, query, step string }{
		{"sum_by_avg_instant", "query", `sum by (app) (avg_over_time({app="x"} | json | unwrap n [5m]))`, "300"},
		{"sum_stddev_instant", "query", `sum(stddev_over_time({app="x"} | json | unwrap n [5m]))`, "300"},
		{"sum_by_quantile_instant", "query", `sum by (app) (quantile_over_time(0.9, {app="x"} | json | unwrap n [5m]))`, "300"},
		{"sum_by_avg_tumbling", "query_range", `sum by (app) (avg_over_time({app="x"} | json | unwrap n [5m]))`, "300"},
		{"max_by_stdvar_tumbling", "query_range", `max by (app) (stdvar_over_time({app="x"} | json | unwrap n [5m]))`, "300"},
		{"sum_by_max_longer_range", "query_range", `sum by (app) (max_over_time({app="x"} | json | unwrap n [5m]))`, "60"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// A selector of its own per iteration, so no response cache answers it.
				query := strings.Replace(tc.query, `{app="x"}`, `{app="x`+strconv.Itoa(i)+`"}`, 1)
				params := url.Values{"query": {query}, "start": {"1800000000"}, "end": {"1800000600"}, "step": {tc.step}, "time": {"1800000600"}}
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+tc.endpoint+"?"+params.Encode(), nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
			}
		})
	}
}
