package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkSumWithout_Query drives the query and query_range handlers with the
// shapes of the unwrap-sum-without A/B set and a few controls, against a stub
// VictoriaLogs. The stub answers every stats request with the same 500 streams
// (one series per stats-by group of the query it was sent, a single series when
// that pipe has no `by`), so the benchmark measures what the proxy does with an
// answer, not what a translation makes VictoriaLogs compute. It uses only
// functions that exist on main, so it runs on both trees for benchstat.
var (
	sumWithoutByRe    = regexp.MustCompile(`stats by \(([^)]*)\)`)
	sumWithoutAliasRe = regexp.MustCompile(`as ([A-Za-z_][A-Za-z_0-9]*)`)
)

func sumWithoutStub(streams, points int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			q := r.FormValue("query")
			by := []string{}
			if m := sumWithoutByRe.FindAllStringSubmatch(q, -1); len(m) > 0 {
				for _, f := range strings.Split(m[len(m)-1][1], ",") {
					if f = strings.TrimSpace(f); f != "" {
						by = append(by, f)
					}
				}
			}
			alias := "x"
			if m := sumWithoutAliasRe.FindAllStringSubmatch(q, -1); len(m) > 0 {
				alias = m[len(m)-1][1]
			}
			ranged := r.URL.Path == "/select/logsql/stats_query_range"
			start, _ := strconv.ParseFloat(r.FormValue("start"), 64)
			if start == 0 {
				if t, err := time.Parse(time.RFC3339Nano, r.FormValue("start")); err == nil {
					start = float64(t.Unix())
				}
			}
			n := streams
			if len(by) == 0 {
				n = 1
			}
			result := make([]map[string]any, 0, n)
			for i := 0; i < n; i++ {
				metric := map[string]string{"__name__": alias}
				for _, f := range by {
					if f == "_stream" {
						metric[f] = fmt.Sprintf(`{app="payment-service",pod="pod-%d"}`, i)
					} else {
						metric[f] = "v" + strconv.Itoa(i%3)
					}
				}
				e := map[string]any{"metric": metric}
				if ranged {
					vals := make([][]any, 0, points)
					for p := 0; p < points; p++ {
						vals = append(vals, []any{start + float64(p*300), strconv.Itoa(i + p)})
					}
					e["values"] = vals
				} else {
					e["value"] = []any{start, strconv.Itoa(i)}
				}
				result = append(result, e)
			}
			rt := "vector"
			if ranged {
				rt = "matrix"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": rt, "result": result}})
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
}

func BenchmarkSumWithout_Query(b *testing.B) {
	backend := httptest.NewServer(sumWithoutStub(500, 24))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Millisecond, 10), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	cases := []struct {
		name, query string
		instant     bool
	}{
		{"W1_sum_without_max_over_time", `sum without (pod) (max_over_time({app="payment-service"} | logfmt | unwrap amount [5m]))`, false},
		{"W3_sum_without_rate", `sum without (pod) (rate({app="payment-service"} | logfmt | unwrap amount [5m]))`, false},
		{"W5_instant_sum_without", `sum without (pod) (max_over_time({app="payment-service"} | logfmt | unwrap amount [5m]))`, true},
		{"W7_topk_sum_without", `topk(5, sum without (pod) (max_over_time({app="payment-service"} | logfmt | unwrap amount [5m])))`, false},
		{"C1_count_by_app", `sum by (app) (count_over_time({env="production"}[5m]))`, false},
		{"C2_rate_ungrouped", `sum(rate({env="production"}[5m]))`, false},
		{"C3_label_replace_instant", `label_replace(sum by (app) (count_over_time({env="production"}[5m])), "svc", "$1", "app", "(.*)")`, true},
		{"C4_binary", `sum(rate({env="production"}[5m])) / sum(rate({env="production"}[1h]))`, false},
	}
	end := time.Now().Truncate(time.Minute)
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			mk := func(shift int) string {
				e := end.Add(-time.Duration(shift) * time.Minute)
				params := url.Values{"query": {c.query}}
				target := "/loki/api/v1/query_range?"
				if c.instant {
					target = "/loki/api/v1/query?"
					params.Set("time", strconv.FormatInt(e.Unix(), 10))
				} else {
					params.Set("start", strconv.FormatInt(e.Add(-2*time.Hour).Unix(), 10))
					params.Set("end", strconv.FormatInt(e.Unix(), 10))
					params.Set("step", "300")
				}
				return target + params.Encode()
			}
			urls := make([]string, 512)
			for i := range urls {
				urls[i] = mk(i)
			}
			res := doCompatProxyRequest(p, urls[0], map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				b.Fatalf("%d %s", res.Code, res.Body)
			}
			var parsed struct {
				Data struct {
					Result []json.RawMessage `json:"result"`
				} `json:"data"`
			}
			_ = json.Unmarshal(res.Body.Bytes(), &parsed)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doCompatProxyRequest(p, urls[i%len(urls)], map[string]string{"X-Scope-OrgID": "0"})
			}
			b.StopTimer()
			b.Logf("series in the answer: %d", len(parsed.Data.Result))
		})
	}
}
