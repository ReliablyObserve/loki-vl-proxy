package querycorpus_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestExtractionListIdentityVictoriaLogsCost measures what the extraction list's series identity costs VictoriaLogs
// (v1.53.0 and v1.49.0, its own query handlers through the serve oracle): the stats query the string translator
// writes, which origin/main sends (a series per line, or the clause alone), and the one the lowering writes (a value
// per series of the stream, its stored level fields and the listed labels, then the aggregation), over logfmt rows of
// eight streams, interleaved runs, the median and the 90th percentile of each and the answer's size. It records nothing; LVP_XLIST_VLCOST=1
// runs it.
func TestExtractionListIdentityVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_XLIST_VLCOST") != "1" {
		t.Skip("a measurement: LVP_XLIST_VLCOST=1 runs it")
	}
	const n = 600_000
	const tenant = 9
	first := metricEnd.Add(-2 * time.Hour)
	every := 2 * time.Hour / time.Duration(n)
	routes := []string{"/x", "/y", "/z"}
	rows := make([]oraclerun.ServeRow, 0, n)
	for i := 0; i < n; i++ {
		s := i % 8
		line := "amount=" + strconv.Itoa((i*7919)%500+1) + " id=r" + strconv.Itoa(i) + " msg=paid"
		if i%5 != 0 {
			line += " route=" + routes[(i/8)%3]
		}
		rows = append(rows, oraclerun.ServeRow{TS: first.Add(time.Duration(i) * every).UnixNano(),
			Stream: [][2]string{{"app", "uw"}, {"ns", []string{"x", "y"}[s%2]}, {"pod", string(rune('a' + s))}},
			Fields: [][2]string{{"_msg", line}}})
	}
	queries := []string{
		`sum(max_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`max(stddev_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`min(min_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`sum by (route) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
	}
	label := translator.WithStreamLabels(proxy.NewLabelTranslator(proxy.LabelStyleUnderscores, nil).ToVL, nil)
	for _, prof := range diffProfiles() {
		caps := logsql.CapabilitiesFor(prof.version)
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: tenant, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		run := func(q string) (time.Duration, int) {
			form := url.Values{"query": {q}, "time": {metricEnd.Format(time.RFC3339)}}
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/select/logsql/stats_query", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("AccountID", strconv.Itoa(tenant))
			req.Header.Set("ProjectID", "0")
			t0 := time.Now()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: %d %s", q, resp.StatusCode, body)
			}
			return time.Since(t0), len(body)
		}
		// interleaved runs old and new rounds times each, alternating which goes first, after one warm-up run of each:
		// the median and the 90th percentile of each, and the answer's size.
		const rounds = 21
		interleaved := func(old, new string) (o, n [2]time.Duration, ob, nb int) {
			run(old)
			run(new)
			var od, nd []time.Duration
			for i := 0; i < rounds; i++ {
				if i%2 == 0 {
					d, b := run(old)
					od, ob = append(od, d), b
					d, b = run(new)
					nd, nb = append(nd, d), b
				} else {
					d, b := run(new)
					nd, nb = append(nd, d), b
					d, b = run(old)
					od, ob = append(od, d), b
				}
			}
			sort.Slice(od, func(i, j int) bool { return od[i] < od[j] })
			sort.Slice(nd, func(i, j int) bool { return nd[i] < nd[j] })
			p90 := rounds * 9 / 10
			return [2]time.Duration{od[rounds/2], od[p90]}, [2]time.Duration{nd[rounds/2], nd[p90]}, ob, nb
		}
		var b strings.Builder
		for _, q := range queries {
			canon := logql.CanonicalizeLogRanges(q)
			old, err := translator.TranslateLogQLWithCapabilities(canon, label, nil, caps)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := planOfText(canon)
			if err != nil {
				t.Fatal(err)
			}
			op, err := lower.PlanQuery(canon, plan, lower.Profile{Label: label, Caps: caps})
			if err != nil {
				t.Fatal(err)
			}
			text := op.(physical.VLQuery).Text
			o, nw, ob, nb := interleaved(old, text)
			fmt.Fprintf(&b, "\n  %-90s base p50 %9s p90 %9s (%5d B)  head p50 %9s p90 %9s (%5d B)  p50 %+6.1f%%", q,
				o[0].Round(time.Microsecond), o[1].Round(time.Microsecond), ob, nw[0].Round(time.Microsecond), nw[1].Round(time.Microsecond), nb,
				100*(float64(nw[0])-float64(o[0]))/float64(o[0]))
		}
		t.Logf("VictoriaLogs %s, %d rows, stats_query at the end, %d runs each, interleaved:%s", prof.version, n, rounds, b.String())
		srv.Stop()
	}
}
