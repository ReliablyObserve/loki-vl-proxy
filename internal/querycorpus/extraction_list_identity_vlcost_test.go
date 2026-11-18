package querycorpus_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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
// (v1.54.0 and v1.49.0, its own query handlers through the serve oracle): the stats query the string translator
// writes (a series per line, or the clause alone), the one the lowering writes (a value per series of the stream, its
// stored level fields and the listed labels, then the aggregation; one stats pipe over the rows for the maximum of
// maxima and the minimum of minima) and, when LVP_XLIST_VLCOST_BASE names a JSON object of query to text (another
// build's lowered texts, which LVP_XLIST_VLCOST_DUMP writes), that build's, over logfmt rows of eight streams,
// interleaved runs in turn, the median and the 90th percentile of each and the answer's size. Exact forms of the
// identity that group otherwise are measured beside the lowered text of `sum by (ns) (max_over_time(...))`. It records
// nothing; LVP_XLIST_VLCOST=1 runs it.
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
		`max(max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`max by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`min by (ns) (min_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`max by (ns) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`min(min_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
	}
	// Exact forms of the grouped identity of sum by (ns) over max_over_time, beside the lowered text: _stream_id for
	// _stream, and ns read back from the stream of each series instead of grouped by.
	sumMax := `sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`
	variants := map[string][][2]string{sumMax: {
		{"_stream_id", "| stats by (ns, _stream, level, detected_level, route) max(__lvp_v) as __lvp_inner => | stats by (ns, _stream_id, level, detected_level, route) max(__lvp_v) as __lvp_inner"},
		{"ns from _stream", "| stats by (ns, _stream, level, detected_level, route) max(__lvp_v) as __lvp_inner => | stats by (_stream, level, detected_level, route) max(__lvp_v) as __lvp_inner | extract 'ns=\"<ns>\"' from _stream"},
	}}
	base := map[string]string{}
	if path := os.Getenv("LVP_XLIST_VLCOST_BASE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			t.Fatal(err)
		}
	}
	dump := map[string]string{}
	label := translator.WithStreamLabels(proxy.NewLabelTranslator(proxy.LabelStyleUnderscores, nil).ToVL, nil)
	for _, prof := range diffProfiles() {
		caps := logsql.CapabilitiesFor(prof.version)
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: tenant, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		last := map[string][]byte{}
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
			last[q] = body
			return time.Since(t0), len(body)
		}
		// answer is the answer last read for q as sorted rows, to hold an exact form to the lowered text's answer.
		answer := func(q string) string {
			var resp struct {
				Data struct {
					Result []json.RawMessage `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(last[q], &resp); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			rows := make([]string, len(resp.Data.Result))
			for i, r := range resp.Data.Result {
				rows[i] = string(r)
			}
			sort.Strings(rows)
			return strings.Join(rows, "\n")
		}
		// interleaved runs each text rounds times in turn, the first of a turn moving by one each round, after one
		// warm-up run of each: the median and the 90th percentile of each, and the answer's size.
		const rounds = 21
		interleaved := func(texts []string) (p50, p90 []time.Duration, size []int) {
			ds := make([][]time.Duration, len(texts))
			size = make([]int, len(texts))
			for _, q := range texts {
				run(q)
			}
			for i := 0; i < rounds; i++ {
				for k := range texts {
					j := (i + k) % len(texts)
					d, b := run(texts[j])
					ds[j], size[j] = append(ds[j], d), b
				}
			}
			for _, d := range ds {
				sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
				p50, p90 = append(p50, d[rounds/2]), append(p90, d[rounds*9/10])
			}
			return p50, p90, size
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
			if prof.version == diffProfiles()[0].version {
				dump[q] = text
			}
			names, texts := []string{"translator"}, []string{old}
			if bt, ok := base[q]; ok {
				names, texts = append(names, "base"), append(texts, bt)
			}
			names, texts = append(names, "head"), append(texts, text)
			for _, v := range variants[q] {
				from, to, _ := strings.Cut(v[1], " => ")
				if !strings.Contains(text, from) {
					t.Fatalf("%s: %s holds no %s", q, text, from)
				}
				names, texts = append(names, v[0]), append(texts, strings.Replace(text, from, to, 1))
			}
			p50, p90, size := interleaved(texts)
			head := slices.Index(names, "head")
			for i := head + 1; i < len(texts); i++ {
				if answer(texts[i]) != answer(text) {
					t.Errorf("%s: the %s form answers otherwise than the lowered text", q, names[i])
				}
			}
			fmt.Fprintf(&b, "\n  %s", q)
			for i, name := range names {
				fmt.Fprintf(&b, "\n    %-16s p50 %9s p90 %9s (%5d B)  p50 vs translator %+6.1f%%  vs head %+6.1f%%", name,
					p50[i].Round(time.Microsecond), p90[i].Round(time.Microsecond), size[i],
					100*(float64(p50[i])-float64(p50[0]))/float64(p50[0]), 100*(float64(p50[i])-float64(p50[head]))/float64(p50[head]))
			}
		}
		t.Logf("VictoriaLogs %s, %d rows, stats_query at the end, %d runs each, interleaved:%s", prof.version, n, rounds, b.String())
		srv.Stop()
	}
	if path := os.Getenv("LVP_XLIST_VLCOST_DUMP"); path != "" {
		raw, err := json.Marshal(dump)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
