package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins which `without` queries the proxy folds from a row per stream
// (semantics/unwrap-sum-without-collapses). Only a sum, min, max, avg or count
// without that is the outermost aggregation is: its fold is Loki's
// (TestSumWithoutDifferential). Any other query that holds a `without` clause
// (topk and bottomk without, a without under another aggregation) is answered as
// it was before the fold existed: the answers below are the ones the proxy gave
// then, byte for byte, not Loki's: the shapes whose answers differ from Loki's are
// open rows of the registry (semantics/without-*).

// withoutScopeAnswer runs query through the real handler against the emulated
// VictoriaLogs and returns the answer as "status: series ..." with the series
// sorted, each as its labels and values.
func withoutScopeAnswer(t *testing.T, rows []emuRow, query string, instant bool, seriesCap int) string {
	t.Helper()
	emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: seriesCap})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := int64(1_800_000_000)
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if instant {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(base, 10))
	} else {
		params.Set("start", strconv.FormatInt(base-120, 10))
		params.Set("end", strconv.FormatInt(base, 10))
		params.Set("step", "60")
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	var resp struct {
		Error string `json:"error"`
		Data  struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v: %s", query, err, res.Body)
	}
	if res.Code != 200 {
		if i := strings.Index(resp.Error, "maximum number of series ("); i >= 0 {
			return fmt.Sprintf("%d series limit (%s)", res.Code, resp.Error[i+len("maximum number of series ("):strings.Index(resp.Error, ")")])
		}
		return fmt.Sprintf("%d %s", res.Code, resp.Error)
	}
	var series []string
	for _, s := range resp.Data.Result {
		keys := make([]string, 0, len(s.Metric))
		for k, v := range s.Metric {
			keys = append(keys, k+"="+v)
		}
		sort.Strings(keys)
		points := s.Values
		if s.Value != nil {
			points = [][]any{s.Value}
		}
		var vals []string
		for _, pt := range points {
			v, _ := strconv.ParseFloat(pt[1].(string), 64)
			vals = append(vals, strconv.FormatFloat(v, 'g', 6, 64))
		}
		series = append(series, "{"+strings.Join(keys, ",")+"}="+strings.Join(vals, ","))
	}
	sort.Strings(series)
	return "200 " + strings.Join(series, " ")
}

func withoutScopeRows() []emuRow {
	base := int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := 0; i < 40; i++ {
		stream := fmt.Sprintf(`{app="uw",ns=%q,pod=%q}`, []string{"x", "y"}[(i/4)%2], []string{"a", "b", "c", "d"}[i%4])
		rows = append(rows, emuRow{ts: base - int64(i)*5e9 - 3e8, stream: stream, value: float64(i + 1), msg: fmt.Sprintf("amount=%v", i+1)})
	}
	return rows
}

// conformance: semantics/unwrap-sum-without-collapses, limits/series-limit-error
func TestWithoutFoldIsLimitedToTheOperatorsItProves(t *testing.T) {
	// Each answer is [query_range, query_range with -max-stats-query-series=1,
	// query, query with the limit of 1].
	for _, tc := range []struct {
		query string
		want  [4]string
	}{
		{`topk without (pod) (1, max_over_time({app="uw"} | unwrap amount [5m]))`,
			[4]string{`200 {app=uw,ns=x,service_name=uw}=12 {app=uw,ns=y,service_name=uw}=24`, `400 series limit (1)`, `200 {app=uw,ns=y,service_name=uw}=40`, `200 {app=uw,ns=y,service_name=uw}=40`}},
		{`bottomk without (pod) (1, max_over_time({app="uw"} | unwrap amount [5m]))`,
			[4]string{`200 {app=uw,ns=x,service_name=uw}=17 {app=uw,ns=y,service_name=uw}=5`, `400 series limit (1)`, `200 {app=uw,ns=x,service_name=uw}=33`, `200 {app=uw,ns=x,service_name=uw}=33`}},
		{`topk without (pod) (1, rate({app="uw"}[5m]))`,
			[4]string{`200 {app=uw,detected_level=unknown,ns=x,service_name=uw}=2,5 {app=uw,detected_level=unknown,ns=y,service_name=uw}=4`, `400 series limit (1)`, `200 {app=uw,ns=x,service_name=uw}=0.0166667`, `200 {app=uw,ns=x,service_name=uw}=0.0166667`}},
		{`max(sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m])))`,
			[4]string{`200 {}=164,68`, `200 {}=164,68`, `200 {}=292`, `200 {}=292`}},
		{`sum by (ns) (sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m])))`,
			[4]string{`200 {}=164,68`, `200 {}=164,68`, `200 {}=292`, `200 {}=292`}},
		{`sum by (ns) (sum without (pod) (rate({app="uw"}[5m])))`,
			[4]string{`200 {app=uw,detected_level=unknown,ns=x,service_name=uw}=8,12,20 {app=uw,detected_level=unknown,ns=y,service_name=uw}=8,16,20`, `400 series limit (1)`, `200 {}=0.133333`, `200 {}=0.133333`}},
		{`max(sum without (pod) (rate({app="uw"}[5m])))`,
			[4]string{`200 {app=uw,detected_level=unknown,ns=x,service_name=uw}=2,3,5 {app=uw,detected_level=unknown,ns=y,service_name=uw}=2,4,5`, `400 series limit (1)`, `200 {}=0.133333`, `200 {}=0.133333`}},
		{`topk(2, max(sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m]))))`,
			[4]string{`200 {}=164,68`, `200 {}=164,68`, `200 {}=292`, `200 {}=292`}},
		{`sort(max(sum without (pod) (rate({app="uw"}[5m]))))`,
			[4]string{`200 {app=uw,detected_level=unknown,ns=x,service_name=uw}=8,12,20 {app=uw,detected_level=unknown,ns=y,service_name=uw}=8,16,20`, `400 series limit (1)`, `200 {}=0.133333`, `200 {}=0.133333`}},
		// The shapes the fold answers (sum, min, max, avg, count without, the
		// outermost aggregation, also under a ranking): a series per remaining
		// label set, limited on the folded answer.
		{`max without (pod) (max_over_time({app="uw"} | unwrap amount [5m]))`,
			[4]string{`200 {app=uw,ns=x,service_name=uw}=20,12 {app=uw,ns=y,service_name=uw}=24,8`, `400 series limit (1)`, `200 {app=uw,ns=x,service_name=uw}=36 {app=uw,ns=y,service_name=uw}=40`, `400 series limit (1)`}},
		{`topk(5, max without (pod) (max_over_time({app="uw"} | unwrap amount [5m])))`,
			[4]string{`200 {app=uw,ns=x,service_name=uw}=20,12 {app=uw,ns=y,service_name=uw}=24,8`, `400 series limit (1)`, `200 {app=uw,ns=x,service_name=uw}=36 {app=uw,ns=y,service_name=uw}=40`, `400 series limit (1)`}},
	} {
		for i, name := range []string{"query_range", "query_range/limit 1", "query", "query/limit 1"} {
			t.Run(fmt.Sprintf("%s/%s", tc.query, name), func(t *testing.T) {
				limit := 500
				if i%2 == 1 {
					limit = 1
				}
				if got := withoutScopeAnswer(t, withoutScopeRows(), tc.query, i >= 2, limit); got != tc.want[i] {
					t.Fatalf("%s:\n got %s\nwant %s", tc.query, got, tc.want[i])
				}
			})
		}
	}
}
