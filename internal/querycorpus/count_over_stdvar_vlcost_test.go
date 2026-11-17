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

// TestCountOverStdvarVictoriaLogsCost measures what counting the series of a count by labels over stdvar_over_time
// costs VictoriaLogs (v1.53.0 and v1.49.0, its own query handlers through the serve oracle): the stats query the
// string translator writes (the deviation of each group's pooled samples, which origin/main squares) against the one
// the lowering writes (the deviation of each series, then their count per group), over logfmt rows of eight streams,
// interleaved runs in turn, the median and the 90th percentile of each and the answer's size. It records nothing;
// LVP_COUNT_STDVAR_VLCOST=1 runs it.
func TestCountOverStdvarVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_COUNT_STDVAR_VLCOST") != "1" {
		t.Skip("a measurement: LVP_COUNT_STDVAR_VLCOST=1 runs it")
	}
	const n = 600_000
	const tenant = 9
	first := metricEnd.Add(-2 * time.Hour)
	every := 2 * time.Hour / time.Duration(n)
	rows := make([]oraclerun.ServeRow, 0, n)
	for i := 0; i < n; i++ {
		s := i % 8
		amount := strconv.Itoa((i*7919)%500 + 1)
		line := "amount=" + amount + " id=r" + strconv.Itoa(i) + " msg=paid"
		// The amount is a field of the row too, which the query without a parser reads.
		rows = append(rows, oraclerun.ServeRow{TS: first.Add(time.Duration(i) * every).UnixNano(),
			Stream: [][2]string{{"app", "uw"}, {"ns", []string{"x", "y"}[s%2]}, {"pod", string(rune('a' + s))}},
			Fields: [][2]string{{"_msg", line}, {"amount", amount}}})
	}
	queries := []string{
		`count by (ns) (stdvar_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`count by (ns) (stdvar_over_time({app="uw"} | unwrap amount [5m]))`,
		`count by (ns, pod) (stdvar_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		// count over stddev_over_time, which the string translator wrote as the lowering writes count over stdvar.
		`count by (ns) (stddev_over_time({app="uw"} | unwrap amount [5m]))`,
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
		const rounds = 21
		var b strings.Builder
		for _, q := range queries {
			canon := logql.CanonicalizeLogRanges(q)
			old, err := translator.TranslateLogQLWithCapabilities(canon, label, nil, caps)
			if err != nil {
				t.Fatal(err)
			}
			// The translator's text is a power of the deviation: VictoriaLogs runs its left operand.
			old = strings.TrimSuffix(strings.TrimPrefix(old, "__binary__:^:"), "|||2")
			plan, err := planOfText(canon)
			if err != nil {
				t.Fatal(err)
			}
			op, err := lower.PlanQuery(canon, plan, lower.Profile{Label: label, Caps: caps, StreamIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			texts := []string{old, op.(physical.VLQuery).Text}
			ds := make([][]time.Duration, len(texts))
			size := make([]int, len(texts))
			for _, text := range texts {
				run(text)
			}
			for i := 0; i < rounds; i++ {
				for k := range texts {
					j := (i + k) % len(texts)
					d, sz := run(texts[j])
					ds[j], size[j] = append(ds[j], d), sz
				}
			}
			var p50, p90 [2]time.Duration
			for j, d := range ds {
				sort.Slice(d, func(a, c int) bool { return d[a] < d[c] })
				p50[j], p90[j] = d[rounds/2], d[rounds*9/10]
			}
			fmt.Fprintf(&b, "\n  %-90s base p50 %9s p90 %9s (%5d B)  head p50 %9s p90 %9s (%5d B)  p50 %+6.1f%%", q,
				p50[0].Round(time.Microsecond), p90[0].Round(time.Microsecond), size[0], p50[1].Round(time.Microsecond),
				p90[1].Round(time.Microsecond), size[1], 100*(float64(p50[1])-float64(p50[0]))/float64(p50[0]))
		}
		t.Logf("VictoriaLogs %s, %d rows, stats_query at the end, %d runs each, interleaved:%s", prof.version, n, rounds, b.String())
		srv.Stop()
	}
}
