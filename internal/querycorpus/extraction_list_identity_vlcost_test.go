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
// writes (a series per line) and the one the lowering writes (the series hash of the stream and the listed labels),
// over logfmt rows of eight streams, median of repeated runs, interleaved. It records nothing; LVP_XLIST_VLCOST=1
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
		median := func(q string) (time.Duration, int) {
			var ds []time.Duration
			size := 0
			run(q)
			for i := 0; i < 11; i++ {
				d, b := run(q)
				ds, size = append(ds, d), b
			}
			sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
			return ds[len(ds)/2], size
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
			o1, ob := median(old)
			n1, nb := median(text)
			o2, _ := median(old)
			n2, _ := median(text)
			o, nw := (o1+o2)/2, (n1+n2)/2
			fmt.Fprintf(&b, "\n  %-90s per line %9s (%5d B)  identity %9s (%5d B)  %+6.1f%%", q, o.Round(time.Microsecond), ob, nw.Round(time.Microsecond), nb, 100*(float64(nw)-float64(o))/float64(o))
		}
		t.Logf("VictoriaLogs %s, %d rows, stats_query at the end, median of 11 runs, twice interleaved:%s", prof.version, n, b.String())
		srv.Stop()
	}
}
