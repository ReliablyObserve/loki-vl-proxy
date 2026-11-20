package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestNumericEqualityVictoriaLogsCost measures what serving a numeric equality after a parser costs VictoriaLogs
// (v1.54.0 and v1.49.0, its own query handlers through the serve oracle), over json rows of one stream spread over
// 24 hours:
//   - the pushdown: the range pair alone (the first served text) against the pair with Loki's syntax guard and the
//     regexp of the exact spellings (the served text), a count over the last 1h, 6h and 24h, and over no rows (an
//     empty selector: the parse cost of the regexps alone);
//   - the trial of a proxy fallback (not served): the raw rows a `!=` would have to read for the proxy to find the
//     present but empty json values VictoriaLogs stores as no field (every row without the field), and the rows a
//     duration of several units would have to read (every row whose value spells several units), each read through
//     /select/logsql/query and decided in Go, over the same windows.
//
// It records nothing; LVP_NUMEQ_VLCOST=1 runs it.
func TestNumericEqualityVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_NUMEQ_VLCOST") != "1" {
		t.Skip("a measurement: LVP_NUMEQ_VLCOST=1 runs it")
	}
	// Optional saved queries from the build being compared. Unlike the range
	// pair diagnostic, this comparison requires identical counts before timing.
	var before map[string]map[string]string
	if path := os.Getenv("LVP_NUMEQ_BASELINE_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &before); err != nil {
			t.Fatal(err)
		}
	}
	const n = 600_000
	const tenant = 11
	first := metricEnd.Add(-24 * time.Hour)
	every := 24 * time.Hour / time.Duration(n)
	statuses := []string{`"200"`, `200`, `"404"`, `200.0`, `"1kb"`, `""`, ``, `"abc"`, `2e2`, `500`}
	durations := []string{`"5s"`, `"5000ms"`, `"1m30s"`, `"5000000us"`, `"4s1000ms"`, `""`, `"2m"`, `"90s"`}
	rows := make([]oraclerun.ServeRow, 0, n)
	for i := 0; i < n; i++ {
		var fields []string
		if s := statuses[i%len(statuses)]; s != `` {
			fields = append(fields, `"status":`+s)
		}
		fields = append(fields, `"d":`+durations[(i/3)%len(durations)], `"size":"`+[]string{"1KB", "1 kb", "1000", "2KB"}[(i/7)%4]+`"`, `"id":"r`+strconv.Itoa(i)+`"`)
		rows = append(rows, oraclerun.ServeRow{TS: first.Add(time.Duration(i) * every).UnixNano(),
			Stream: [][2]string{{"app", "nq"}}, Fields: [][2]string{{"_msg", "{" + strings.Join(fields, ",") + "}"}}})
	}
	// Each query and the range pair alone, the first served text.
	queries := []struct{ q, pair string }{
		{`{app="nq"} | json | status = 200`, `(status:>=200 status:<=200)`},
		{`{app="nq"} | json | status != 200`, `(status:* -(status:>=200 status:<=200))`},
		{`{app="nq"} | json | d = 5s`, `(d:>=5000000000 d:<=5000000000)`},
		{`{app="nq"} | json | size = 1KB`, `(size:>=1000 size:<=1000)`},
	}
	for _, prof := range diffProfiles() {
		caps := logsql.CapabilitiesFor(prof.version)
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: tenant, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		stop := sync.OnceFunc(srv.Stop)
		t.Cleanup(stop)
		post := func(path string, form url.Values) (time.Duration, []byte) {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("AccountID", strconv.Itoa(tenant))
			req.Header.Set("ProjectID", "0")
			t0 := time.Now()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: %d %s", form.Get("query"), resp.StatusCode, body)
			}
			return time.Since(t0), body
		}
		count := func(text string, window time.Duration) (time.Duration, string) {
			d, body := post("/select/logsql/stats_query", url.Values{"query": {text + " | stats count() as c"},
				"start": {metricEnd.Add(-window).Format(time.RFC3339Nano)}, "end": {metricEnd.Format(time.RFC3339Nano)}, "time": {metricEnd.Format(time.RFC3339)}})
			c, err := numericCostCount(body)
			if err != nil {
				t.Fatalf("invalid count answer: %v: %s", err, body)
			}
			return d, c
		}
		const rounds = 9
		median := func(f func() time.Duration) time.Duration {
			f()
			ds := make([]time.Duration, rounds)
			for i := range ds {
				ds[i] = f()
			}
			sort.Slice(ds, func(a, b int) bool { return ds[a] < ds[b] })
			return ds[rounds/2]
		}
		var b strings.Builder
		windows := []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}
		for _, qp := range queries {
			q := qp.q
			canon := logql.CanonicalizeLogRanges(q)
			plan, err := planOfText(canon)
			if err != nil {
				t.Fatal(err)
			}
			op, err := lower.PlanQuery(canon, plan, lower.Profile{Caps: caps})
			if err != nil {
				t.Fatal(err)
			}
			served := op.(physical.VLQuery).Text
			pairOnly := `app:="nq" | unpack_json | filter ` + qp.pair
			for _, w := range windows {
				if before != nil {
					old := before[prof.version][q]
					if old == "" {
						t.Fatalf("baseline missing %s on %s", q, prof.version)
					}
					_, oldCount := count(old, w)
					_, newCount := count(served, w)
					if oldCount != newCount {
						t.Fatalf("count changed for %s: %s -> %s", q, oldCount, newCount)
					}
					var oldTimes, newTimes []time.Duration
					for round := 0; round < rounds; round++ {
						texts := []string{old, served}
						if round%2 != 0 {
							texts[0], texts[1] = texts[1], texts[0]
						}
						for i, text := range texts {
							d, gotCount := count(text, w)
							if gotCount != oldCount {
								t.Fatalf("unstable count for %s: %s -> %s", q, oldCount, gotCount)
							}
							if i == round%2 {
								oldTimes = append(oldTimes, d)
							} else {
								newTimes = append(newTimes, d)
							}
						}
					}
					sort.Slice(oldTimes, func(i, j int) bool { return oldTimes[i] < oldTimes[j] })
					sort.Slice(newTimes, func(i, j int) bool { return newTimes[i] < newTimes[j] })
					d0, d1 := oldTimes[rounds/2], newTimes[rounds/2]
					fmt.Fprintf(&b, "\n  interleaved %-36s %4s baseline p50 %9s candidate p50 %9s (%+6.1f%%), count %s", q, w,
						d0.Round(time.Microsecond), d1.Round(time.Microsecond), 100*(float64(d1)-float64(d0))/float64(d0), newCount)
				}
				var c0, c1 string
				d0 := median(func() time.Duration { d, c := count(pairOnly, w); c0 = c; return d })
				d1 := median(func() time.Duration { d, c := count(served, w); c1 = c; return d })
				fmt.Fprintf(&b, "\n  %-36s %4s  pair only p50 %9s  served p50 %9s  (%+6.1f%%)  counts %s / %s", q, w,
					d0.Round(time.Microsecond), d1.Round(time.Microsecond), 100*(float64(d1)-float64(d0))/float64(d0), c0, c1)
			}
			empty := strings.Replace(served, `app:="nq"`, `app:="none-xyz"`, 1)
			emptyPair := strings.Replace(pairOnly, `app:="nq"`, `app:="none-xyz"`, 1)
			e0 := median(func() time.Duration { d, _ := count(emptyPair, time.Hour); return d })
			e1 := median(func() time.Duration { d, _ := count(empty, time.Hour); return d })
			fmt.Fprintf(&b, "\n  %-36s empty selector: pair only p50 %9s  served p50 %9s", q, e0.Round(time.Microsecond), e1.Round(time.Microsecond))
		}
		// The proxy fallback trial: the candidate rows and their decision in Go.
		for _, c := range []struct{ name, filter, key string }{
			{"!= on json: rows without the field", `app:="nq" | unpack_json | filter -status:*`, "status"},
			{"several-unit durations", `app:="nq" | unpack_json | filter d:~"^[-+]?(?:(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:ns|us|µs|μs|ms|s|m|h)){2,}$"`, "d"},
		} {
			for _, w := range windows {
				var read, kept int
				d := median(func() time.Duration {
					t0 := time.Now()
					_, body := post("/select/logsql/query", url.Values{"query": {c.filter + " | fields _msg"},
						"start": {metricEnd.Add(-w).Format(time.RFC3339Nano)}, "end": {metricEnd.Format(time.RFC3339Nano)}})
					read, kept = 0, 0
					sc := bufio.NewScanner(strings.NewReader(string(body)))
					sc.Buffer(make([]byte, 1<<20), 1<<20)
					for sc.Scan() {
						var row struct {
							Msg string `json:"_msg"`
						}
						if json.Unmarshal(sc.Bytes(), &row) != nil {
							continue
						}
						read++
						var line map[string]any
						if json.Unmarshal([]byte(row.Msg), &line) == nil {
							if v, ok := line[c.key]; ok && (c.key != "status" || v == "") {
								kept++
							}
						}
					}
					return time.Since(t0)
				})
				fmt.Fprintf(&b, "\n  fallback %-36s %4s  raw rows read %7d, kept %7d, p50 %9s", c.name, w, read, kept, d.Round(time.Microsecond))
			}
		}
		t.Logf("VictoriaLogs %s, %d json rows over 24h, %d runs each:%s", prof.version, n, rounds, b.String())
		stop()
	}
}

// numericCostCount reads the Prometheus vector returned by stats_query. Refuse
// malformed, failed or partial answers instead of timing them as useful work.
func numericCostCount(body []byte) (string, error) {
	var answer struct {
		Status   string   `json:"status"`
		Warnings []string `json:"warnings"`
		Data     struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", err
	}
	if answer.Status != "success" || answer.Data.ResultType != "vector" || len(answer.Warnings) != 0 || answer.Data.Result == nil || len(answer.Data.Result) > 1 {
		return "", fmt.Errorf("expected one complete count vector")
	}
	if len(answer.Data.Result) == 0 {
		return "0", nil
	}
	v := answer.Data.Result[0].Value
	if len(v) != 2 {
		return "", fmt.Errorf("expected a timestamp and count")
	}
	var count string
	if err := json.Unmarshal(v[1], &count); err != nil {
		return "", err
	}
	if _, err := strconv.ParseUint(count, 10, 64); err != nil {
		return "", err
	}
	return count, nil
}

// conformance: semantics/label-filter-numeric-equality
func TestNumericCostCount(t *testing.T) {
	for _, c := range []struct{ name, body, want string }{
		{"count", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1250"]}]}}`, "1250"},
		{"empty", `{"status":"success","data":{"resultType":"vector","result":[]}}`, "0"},
		{"error", `{"status":"error","error":"failed"}`, ""},
		{"partial", `{"status":"success","warnings":["partial"],"data":{"resultType":"vector","result":[]}}`, ""},
		{"malformed", `{"status":`, ""},
		{"missing", `{}`, ""},
		{"missing result", `{"status":"success","data":{"resultType":"vector"}}`, ""},
		{"null result", `{"status":"success","data":{"resultType":"vector","result":null}}`, ""},
		{"nan", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"NaN"]}]}}`, ""},
		{"negative", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"-1"]}]}}`, ""},
		{"missing sample", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1]}]}}`, ""},
		{"unquoted count", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,2]}]}}`, ""},
		{"multiple", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"2"]},{"value":[1,"3"]}]}}`, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := numericCostCount([]byte(c.body))
			if got != c.want || (err != nil) != (c.want == "") {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}
