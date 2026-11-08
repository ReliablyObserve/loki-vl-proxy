package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// A binary operation is evaluated one operand at a time from the printed text
// of that operand. The printer used to cut a label filter literal the scanner
// reads in pieces (`1h5m` as `1h 5m`, `1e3` as `1e 3`) and to print a large
// scalar with an exponent (`1e+06`), so the operand did not parse again and the
// client got a 400 for a query Loki answers. Both the range and the instant
// handler evaluate the operands this way.
// conformance: semantics/label-filter-expression-syntax
func TestBinaryOperandKeepsLiterals(t *testing.T) {
	type tc struct {
		name, query string
		// sent is a text each operand with the label filter sends to VictoriaLogs.
		sent string
	}
	var cases []tc
	for _, literal := range []string{"1h5m", "1e3", "1h30m45s", "0x10", "1.5e3"} {
		cases = append(cases, tc{
			name:  "label filter " + literal,
			query: `sum(count_over_time({app="api"} | json | latency > ` + literal + ` [5m])) / 2`,
			sent:  ":>" + literal,
		})
	}
	cases = append(cases, tc{
		name:  "large scalar in a nested operand",
		query: `sum(count_over_time({app="api"} | json | latency > 5 [5m])) * 1000000 / 2`,
		sent:  ":>5",
	})

	for _, endpoint := range []string{"query_range", "query"} {
		for _, c := range cases {
			t.Run(endpoint+"/"+c.name, func(t *testing.T) {
				var mu sync.Mutex
				var sent []string
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/health" {
						return
					}
					_ = r.ParseForm()
					mu.Lock()
					sent = append(sent, r.FormValue("query"))
					mu.Unlock()
					if strings.HasSuffix(r.URL.Path, "/stats_query_range") {
						_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[]}]}}`))
						return
					}
					_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
				}))
				defer backend.Close()

				p := newGapTestProxy(t, backend.URL)
				start := time.Unix(1700000000, 0)
				params := url.Values{"query": {c.query}}
				rec := httptest.NewRecorder()
				if endpoint == "query_range" {
					params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
					params.Set("end", strconv.FormatInt(start.Add(10*time.Minute).UnixNano(), 10))
					params.Set("step", "60")
					p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				} else {
					params.Set("time", strconv.FormatInt(start.UnixNano(), 10))
					p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
				}
				if rec.Code != http.StatusOK {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
				mu.Lock()
				defer mu.Unlock()
				found := false
				for _, q := range sent {
					if strings.Contains(q, "latency") {
						found = true
						if !strings.Contains(q, c.sent) {
							t.Errorf("VictoriaLogs query %q does not carry the comparison %q", q, c.sent)
						}
					}
				}
				if !found {
					t.Fatalf("no query with the label filter reached VictoriaLogs: %q", sent)
				}
			})
		}
	}
}
