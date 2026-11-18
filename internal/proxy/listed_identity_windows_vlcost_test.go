package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestListedIdentityWindowsVictoriaLogsCost measures what reading an extraction list's series identity on a range
// longer than the step costs VictoriaLogs (v1.54.0 and v1.49.0, its own query handlers through the serve oracle): a
// query_range of an hour at a step of a minute over a range of five, through the real handler on a fresh proxy (every
// cache cold), with the windows of the identity (head) and without them (listedIdentityWindowsOff: the stream's
// series and each line's step bucket, as origin/main reads them), interleaved runs in turn. It reports the median and
// the 90th percentile of the time VictoriaLogs spends answering the request's stats queries, their count and the
// answer's size. It records nothing; LVP_XLIST_WINDOWS_VLCOST=1 runs it.
func TestListedIdentityWindowsVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_XLIST_WINDOWS_VLCOST") != "1" {
		t.Skip("a measurement: LVP_XLIST_WINDOWS_VLCOST=1 runs it")
	}
	t.Cleanup(func() { listedIdentityWindowsOff = false })
	const n = 600_000
	end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first := end.Add(-2 * time.Hour)
	every := 2 * time.Hour / time.Duration(n)
	routes := []string{"/x", "/y", "/z"}
	set := oraclerun.ServeDataSet{}
	for i := 0; i < n; i++ {
		s := i % 8
		line := "amount=" + strconv.Itoa((i*7919)%500+1) + " id=r" + strconv.Itoa(i) + " user=u" + strconv.Itoa(i%2000) + " msg=paid"
		if i%5 != 0 {
			line += " route=" + routes[(i/8)%3]
		}
		set.Rows = append(set.Rows, oraclerun.ServeRow{TS: first.Add(time.Duration(i) * every).UnixNano(),
			Stream: [][2]string{{"app", "uw"}, {"ns", []string{"x", "y"}[s%2]}, {"pod", string(rune('a' + s))}},
			Fields: [][2]string{{"_msg", line}}})
	}
	queries := []string{
		`sum(stdvar_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum(stdvar_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`sum(avg_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`max(max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		// A listed label of 2,000 values: 2,000 series, whose step buckets the window evaluator holds before the fold
		// (bounded by -manual-range-metric-row-limit).
		`sum(max_over_time({app="uw"} | logfmt amount, user | unwrap amount [5m]))`,
	}
	if only := os.Getenv("LVP_XLIST_WINDOWS_QUERY"); only != "" {
		queries = []string{only}
	}
	step := "60"
	if s := os.Getenv("LVP_XLIST_WINDOWS_STEP"); s != "" {
		step = s
	}
	form := func(q string) string {
		return url.Values{"query": {q}, "start": {end.Add(-time.Hour).Format(time.RFC3339)}, "end": {end.Format(time.RFC3339)}, "step": {step}}.Encode()
	}
	for _, v := range []struct{ version, modfile string }{{"v1.54.0", ""}, {"v1.49.0", "go.v1.49.0.mod"}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{set})
		if err != nil {
			t.Fatal(err)
		}
		inner := levelSweepVLFront(v.version, srv.URL)
		var mu sync.Mutex
		var spent time.Duration
		calls := 0
		front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t0 := time.Now()
			inner.ServeHTTP(w, r)
			if strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query") {
				mu.Lock()
				spent += time.Since(t0)
				calls++
				mu.Unlock()
			}
		}))
		run := func(q string, off bool) (time.Duration, int, int, int) {
			listedIdentityWindowsOff = off
			p, err := New(Config{BackendURL: front.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			spent, calls = 0, 0
			mu.Unlock()
			res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+form(q), map[string]string{"X-Scope-OrgID": "0"})
			_ = p.Shutdown(context.Background())
			if res.Code != http.StatusOK {
				t.Logf("%s (windows off %v): %d %.300s", q, off, res.Code, res.Body.String())
			}
			mu.Lock()
			defer mu.Unlock()
			return spent, calls, res.Body.Len(), res.Code
		}
		const rounds = 11
		var b strings.Builder
		for _, q := range queries {
			run(q, true)
			run(q, false)
			var base, head []time.Duration
			var baseCalls, headCalls, baseSize, headSize, baseCode, headCode int
			for i := 0; i < rounds; i++ {
				for k := 0; k < 2; k++ {
					off := (i+k)%2 == 0
					d, c, sz, code := run(q, off)
					if off {
						base, baseCalls, baseSize, baseCode = append(base, d), c, sz, code
					} else {
						head, headCalls, headSize, headCode = append(head, d), c, sz, code
					}
				}
			}
			sort.Slice(base, func(i, j int) bool { return base[i] < base[j] })
			sort.Slice(head, func(i, j int) bool { return head[i] < head[j] })
			fmt.Fprintf(&b, "\n  %-92s base %d p50 %9s p90 %9s (%d calls, %6d B)  head %d p50 %9s p90 %9s (%d calls, %6d B)  p50 %+6.1f%%", q,
				baseCode, base[rounds/2].Round(time.Microsecond), base[rounds*9/10].Round(time.Microsecond), baseCalls, baseSize,
				headCode, head[rounds/2].Round(time.Microsecond), head[rounds*9/10].Round(time.Microsecond), headCalls, headSize,
				100*(float64(head[rounds/2])-float64(base[rounds/2]))/float64(base[rounds/2]))
		}
		t.Logf("VictoriaLogs %s, %d rows, query_range 1h step %ss range 5m, %d runs each, interleaved, VictoriaLogs time of the stats calls:%s", v.version, n, step, rounds, b.String())
		front.Close()
		srv.Stop()
	}
}

// TestListedWindowChunksVictoriaLogsCost measures the time chunks of an extraction list's windows against VictoriaLogs
// (v1.54.0 and v1.49.0, its own query handlers through the serve oracle): a query_range of six hours over a range of
// five minutes, after a list naming a label of 2,000 values (user) and one of a value per line (id: one series per
// line), through the real handler on a fresh proxy per request (every cache cold), read in one read
// (listedWindowChunksOff: the status, the VictoriaLogs time and calls one read takes) and in chunks under the default
// bound. Each mode runs five times in turn; it reports the status, the median VictoriaLogs time of the stats calls,
// their count and the answer's size, and whether the two modes answer the same values. It records nothing;
// LVP_XLIST_CHUNKS_VLCOST=1 runs it.
func TestListedWindowChunksVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_XLIST_CHUNKS_VLCOST") != "1" {
		t.Skip("a measurement: LVP_XLIST_CHUNKS_VLCOST=1 runs it")
	}
	t.Cleanup(func() { listedWindowChunksOff = false })
	n := 600_000
	if s, err := strconv.Atoi(os.Getenv("LVP_XLIST_CHUNKS_ROWS")); err == nil && s > 0 {
		n = s
	}
	end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	span := 6*time.Hour + 10*time.Minute
	first := end.Add(-span)
	every := span / time.Duration(n)
	set := oraclerun.ServeDataSet{}
	for i := 0; i < n; i++ {
		s := i % 8
		line := "amount=" + strconv.Itoa((i*7919)%500+1) + " id=r" + strconv.Itoa(i) + " user=u" + strconv.Itoa((i*104729)%2000) + " msg=paid"
		set.Rows = append(set.Rows, oraclerun.ServeRow{TS: first.Add(time.Duration(i) * every).UnixNano(),
			Stream: [][2]string{{"app", "uw"}, {"ns", []string{"x", "y"}[s%2]}, {"pod", string(rune('a' + s))}},
			Fields: [][2]string{{"_msg", line}}})
	}
	queries := []string{
		`sum(max_over_time({app="uw"} | logfmt amount, user | unwrap amount [5m]))`,
		`sum(max_over_time({app="uw"} | logfmt amount, id | unwrap amount [5m]))`,
		`avg by (ns) (stddev_over_time({app="uw"} | logfmt amount, id | unwrap amount [5m]))`,
	}
	if only := os.Getenv("LVP_XLIST_CHUNKS_QUERY"); only != "" {
		queries = []string{only}
	}
	steps := []string{"60", "15"}
	if only := os.Getenv("LVP_XLIST_CHUNKS_STEP"); only != "" {
		steps = []string{only}
	}
	for _, v := range []struct{ version, modfile string }{{"v1.54.0", ""}, {"v1.49.0", "go.v1.49.0.mod"}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{set})
		if err != nil {
			t.Fatal(err)
		}
		inner := levelSweepVLFront(v.version, srv.URL)
		var mu sync.Mutex
		var spent time.Duration
		calls := 0
		front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t0 := time.Now()
			inner.ServeHTTP(w, r)
			if strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query") {
				mu.Lock()
				spent += time.Since(t0)
				calls++
				mu.Unlock()
			}
		}))
		run := func(q, step string, off bool) (time.Duration, int, string, int) {
			listedWindowChunksOff = off
			if os.Getenv("LVP_XLIST_CHUNKS_TRACE") == "1" {
				listedWindowChunksObserve = func(first, last int64, m statsReadMeter) {
					t.Logf("chunk steps %d-%d: over %v, %d series, %d bytes", first, last, m.over, m.series, m.bytes)
				}
				defer func() { listedWindowChunksObserve = nil }()
			}
			p, err := New(Config{BackendURL: front.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			spent, calls = 0, 0
			mu.Unlock()
			form := url.Values{"query": {q}, "start": {end.Add(-6 * time.Hour).Format(time.RFC3339)}, "end": {end.Format(time.RFC3339)}, "step": {step}}.Encode()
			res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+form, map[string]string{"X-Scope-OrgID": "0"})
			_ = p.Shutdown(context.Background())
			mu.Lock()
			defer mu.Unlock()
			return spent, calls, res.Body.String(), res.Code
		}
		result := func(body string) string {
			if i := strings.Index(body, `"stats":`); i > 0 {
				return body[:i]
			}
			return body
		}
		const rounds = 5
		var b strings.Builder
		for _, step := range steps {
			for _, q := range queries {
				var times [2][]time.Duration
				var nCalls, codes, sizes [2]int
				var bodies [2]string
				for i := 0; i < rounds; i++ {
					for k := 0; k < 2; k++ {
						mode := (i + k) % 2
						d, c, body, code := run(q, step, mode == 0)
						times[mode] = append(times[mode], d)
						nCalls[mode], codes[mode], sizes[mode], bodies[mode] = c, code, len(body), result(body)
					}
				}
				for m := range times {
					sort.Slice(times[m], func(i, j int) bool { return times[m][i] < times[m][j] })
				}
				fmt.Fprintf(&b, "\n  step %3ss %-88s one read: %d %8s %3d calls %7d B | chunks: %d %8s %3d calls %7d B | same values %v", step, q,
					codes[0], times[0][rounds/2].Round(time.Millisecond), nCalls[0], sizes[0],
					codes[1], times[1][rounds/2].Round(time.Millisecond), nCalls[1], sizes[1], codes[0] == 200 && bodies[0] == bodies[1])
			}
		}
		t.Logf("VictoriaLogs %s, %d rows over 6h10m, query_range 6h, range 5m, %d runs each, VictoriaLogs time of the stats calls (p50):%s", v.version, n, rounds, b.String())
		front.Close()
		srv.Stop()
	}
}
