package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// A request that folds its series afterwards reads under the raw-row budget,
// but the busiest-values ranking of a grouped bucket query is bound by the
// tenant's -max-stats-query-series: with the budget above that limit the
// ranking still answers Loki's series limit error of the tenant's limit, not the
// row-limit text.
//
// conformance: semantics/unwrap-sum-without-collapses, limits/series-limit-error
func TestTopValueRankingIsBoundByTheTenantLimitWhenFolding(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stats_query" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"app":"a"},"value":[1,"9"]},{"metric":{"app":"b"},"value":[1,"8"]},{"metric":{"app":"c"},"value":[1,"7"]}]}}`))
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", MaxStatsQuerySeries: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), foldedSumKey{}, true)
	ctx = context.WithValue(ctx, seriesLimitScopeKey{}, &seriesLimitScope{foldBudget: 1000})
	end := time.Unix(1_800_000_000, 0)
	_, err = p.collectTopValueRangeMetricHits(ctx, statsCompatSpec{BaseQuery: `app:~"x.*"`, GroupBy: []string{"app"}}, "count() as c", end.Add(-time.Hour), end, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "maximum number of series (2)") || strings.Contains(err.Error(), "-manual-range-metric-row-limit") {
		t.Fatalf("got %v, want Loki's series limit error of the tenant's limit (2)", err)
	}
}
