package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestBinaryEvaluationBudget_LokiLimitAnswer drives binary expressions over
// their operator-set evaluation budgets on every binary route (a top-level
// join, a join under an aggregation, a comparison with a scalar, a set
// operation; range and instant) and asserts the limit answer the limits
// registry documents: 400 bad_data, as Loki answers a query over one of its
// limits (logqlmodel.ErrLimit, pkg/util/server/error.go:110-118, v3.7.9), with
// that class's text and no flag in it. The answer is the error layer's, so the
// client reads Loki's body shape.
// conformance: semantics/error-layer-proxy-limit-status, operator-configurable-limits, status-400
func TestBinaryEvaluationBudget_LokiLimitAnswer(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	ts := now.Add(-30 * time.Minute).Unix()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/stats_query_range"):
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[%d,"1"],[%d,"2"]]},{"metric":{"level":"error"},"values":[[%d,"2"]]}]}}`, ts, ts+60, ts)
		case strings.HasSuffix(r.URL.Path, "/stats_query"):
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"level":"info"},"value":[%d,"1"]},{"metric":{"level":"error"},"value":[%d,"2"]}]}}`, ts, ts)
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	defer srv.Close()
	const agg = `sum by (level) (count_over_time({app="api"}[1m]))`
	queries := []string{
		agg + ` / ` + agg,
		`sum(` + agg + ` / ` + agg + `)`,
		agg + ` > 1`,
		agg + ` and ` + agg,
	}
	for _, budget := range []struct {
		name string
		set  func(*ExecutionLimitsConfig)
		want string
	}{
		{"arrays", func(l *ExecutionLimitsConfig) { l.BinaryMetricMaxArrays = 1 }, "limit reached while evaluating the query"},
		{"operand_bytes", func(l *ExecutionLimitsConfig) { l.BinaryMetricMaxOperandBytes = 16 }, "limit reached while evaluating the query"},
	} {
		c := cache.New(time.Minute, 100)
		cfg := Config{BackendURL: srv.URL, Cache: c, LogLevel: "error"}
		budget.set(&cfg.ExecutionLimits)
		p, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		for _, q := range queries {
			for _, mode := range []string{"query_range", "query"} {
				t.Run(budget.name+"/"+mode+"/"+q, func(t *testing.T) {
					v := url.Values{"query": {q}, "start": {fmt.Sprint(now.Add(-time.Hour).Unix())}, "end": {fmt.Sprint(now.Unix())}, "time": {fmt.Sprint(now.Unix())}, "step": {"60"}}
					w := httptest.NewRecorder()
					shippedChain(mux).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+mode+"?"+v.Encode(), nil))
					if w.Code != http.StatusBadRequest || w.Body.String() != budget.want {
						t.Fatalf("got %d %q, want 400 %q", w.Code, w.Body.String(), budget.want)
					}
					readErrorBody(t, w.Header(), w.Body.Bytes())
				})
			}
		}
		c.Close()
		_ = p.Shutdown(t.Context())
	}
}
