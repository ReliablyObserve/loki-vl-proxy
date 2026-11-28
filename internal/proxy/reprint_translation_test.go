package proxy

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The proxy translates the printed text of a parsed query in several places (a
// binary operation is evaluated one operand at a time, a parser stage is
// removed and the query printed again). Printing must not change what the
// translator makes of a query: every query of the benchmark shapes and the
// semantics matrix, and the literals the scanner cuts in pieces, translate the
// same after a parse and a print. For an owned mixed sort, compare the two
// served answers and the reads its operand actually sends to VictoriaLogs.
// conformance: semantics/label-filter-expression-syntax, semantics/binary-operand-literals-reprint, semantics/two-parsers-keep-one
func TestReprintedQueryTranslatesLikeTheOriginal(t *testing.T) {
	var queries []string
	seen := map[string]bool{}
	add := func(q string) {
		if !seen[q] && strings.Contains(q, "{") {
			seen[q] = true
			queries = append(queries, q)
		}
	}
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		for _, s := range jsonStrings(t, path) {
			add(s)
		}
	}
	for _, literal := range []string{"1h5m", "1h30m45s", "1e3", "1.5e3", "0x10", "10KB", "1.5s", "100ms", "1000000"} {
		for _, format := range []string{
			`{app="api"} | json | latency > %s`,
			`{app="api"} | json | level="error" or latency <= %s`,
			`sum(rate({app="api"} | json | latency > %s [5m]))`,
			`sum(rate({app="api"} | json | latency > %s [5m])) / 2`,
		} {
			add(strings.ReplaceAll(format, "%s", literal))
		}
	}

	var mixedSorts [][2]string
	var translated, differ int
	var differing []string
	for _, q := range queries {
		expr, err := logqlpkg.Parse(q)
		if err != nil {
			continue
		}
		printed := expr.String()
		if _, err := logqlpkg.Parse(printed); err != nil {
			t.Errorf("query %q printed as %q, which does not parse: %v", q, printed, err)
		} else if a, b := filterLeaves(q), filterLeaves(printed); strings.Join(a, "\n") != strings.Join(b, "\n") {
			t.Errorf("query %q printed as %q: label filters %q become %q", q, printed, a, b)
		}
		// These sorts evaluate an ordered range or recursively dispatch a binary
		// operand. The whole sort never goes through the string translator.
		if agg, ok := expr.(*logqlpkg.VectorAggregation); ok &&
			(agg.Op == logqlpkg.VectorSort || agg.Op == logqlpkg.VectorSortDesc) && containsMixedParserMetric(agg) {
			mixedSorts = append(mixedSorts, [2]string{q, printed})
			continue
		}
		want, wantErr := translator.TranslateLogQL(q)
		if wantErr != nil {
			continue
		}
		translated++
		got, gotErr := translator.TranslateLogQL(printed)
		if gotErr != nil || got != want {
			differ++
			differing = append(differing, q)
			if !reprintTranslationDiffers[q] {
				t.Errorf("query %q printed as %q:\n  original translates to %q\n  printed translates to %q (err %v)", q, printed, want, got, gotErr)
			}
		}
	}
	checkReprintedMixedSorts(t, mixedSorts)
	for q := range reprintTranslationDiffers {
		if !slices.Contains(differing, q) {
			t.Errorf("query %q no longer differs: remove it from reprintTranslationDiffers", q)
		}
	}
	if translated < 100 {
		t.Fatalf("only %d queries translated: the query files were not read", translated)
	}
	t.Logf("%d queries, %d translated, %d differ after a parse and a print", len(queries), translated, differ)
}

// Compare the physical read, complete Loki reply and series order for each
// spelling on independent cold handlers. Feed both spellings to the engine.
func checkReprintedMixedSorts(t *testing.T, queries [][2]string) {
	t.Helper()
	if len(queries) == 0 {
		t.Fatal("no owned mixed sorts in the query corpus")
	}
	at := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i, line := range []string{`{}`, `{"user_id":"a"}`, `user_id=z`, `user_id=z`} {
		rows = append(rows, pushdownRow{ts: at.Add(time.Duration(i) * time.Millisecond), msg: line, stream: map[string]string{"app": "api-gateway", "env": "production"}})
	}
	metadata := map[string]string{"detected_level": "error", "region": "west"}
	rows = append(rows, pushdownRow{ts: at, msg: "tenant_id=bad", stream: map[string]string{"app": "matrix-mixed-error"}, loki: metadata, vl: metadata})
	set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows)}
	for _, spellings := range queries {
		for _, step := range []time.Duration{0, 30 * time.Second} {
			for index, spelling := range spellings {
				set.Requests = append(set.Requests, lokiMetricRequest(fmt.Sprintf("%s/%v/%d", spellings[0], step, index), spelling, at.UnixNano(), at.Add(30*time.Second).UnixNano(), step))
			}
		}
	}
	answers := lokiEngineAnswers(t, "reprint_mixed_rank", []oraclerun.EngineDataSet{set})
	backend, fake := newPushdownFakeVL(t, rows, func(s string) string { return s })
	fake.projectRaw = true
	var original []byte
	var originalReads []fakeRawQuery
	for i, req := range set.Requests {
		endpoint := "query_range"
		if req.Instant {
			endpoint = "query"
		}
		fake.mu.Lock()
		raw, stats := fake.raw, len(fake.stats)
		fake.mu.Unlock()
		got := doCompatProxyRequest(newTestProxy(t, backend.URL), "/loki/api/v1/"+endpoint+"?"+req.Form, nil)
		fake.mu.Lock()
		reads := slices.Clone(fake.rawQueries[raw:])
		buckets := len(fake.stats) - stats
		fake.mu.Unlock()
		if len(reads) == 0 || buckets != 0 {
			t.Fatalf("%s: missing ordered raw execution: reads %v, stats %d", req.ID, reads, buckets)
		}
		for _, read := range reads {
			if strings.HasSuffix(read.query, "| sort by (_time desc)") {
				t.Fatalf("%s: unexpected parser discovery probe %s", req.ID, read.query)
			}
		}
		want := answers[req.ID]
		if got.Code != want.Status {
			t.Fatalf("%s: status %d, Loki %d: %s", req.ID, got.Code, want.Status, got.Body)
		}
		switch got.Code {
		case 400:
			if strings.TrimSpace(got.Body.String()) != strings.TrimSpace(want.Error) {
				t.Fatalf("%s: proxy %q, Loki %q", req.ID, got.Body, want.Error)
			}
		case 200:
			if err := recordedParserDifference(recordedParserCase{Status: 200, Response: want.Body}, 200, got.Body.Bytes()); err != nil {
				t.Fatalf("%s: %v", req.ID, err)
			}
			if !slices.Equal(recordedMixedSeriesOrder(t, got.Body.Bytes()), recordedMixedSeriesOrder(t, want.Body)) {
				t.Fatalf("%s: wrong series order: %s / %s", req.ID, got.Body, want.Body)
			}
		default:
			t.Fatalf("%s: unusable Loki status %d", req.ID, want.Status)
		}
		if i%2 == 0 {
			original, originalReads = got.Body.Bytes(), reads
		} else if !bytes.Equal(original, got.Body.Bytes()) || !slices.Equal(originalReads, reads) {
			t.Fatalf("%s: reprint changed response or physical reads: %s %v / %s %v", req.ID, original, originalReads, got.Body, reads)
		}
	}
}

// filterLeaves is the label filters of a query, one line each.
func filterLeaves(query string) []string {
	var out []string
	var walk func(*logqlpkg.LabelFilterExpr)
	walk = func(e *logqlpkg.LabelFilterExpr) {
		if e == nil {
			return
		}
		if e.IsLeaf() {
			out = append(out, e.Name+" "+e.Cmp+" "+e.Value+" "+e.Kind.String())
			return
		}
		walk(e.Left)
		walk(e.Right)
	}
	for _, st := range labelFilterStagesOf(query) {
		walk(st.Expr)
	}
	return out
}

// reprintTranslationDiffers are the queries whose printed text still translates
// differently, none of them because of a literal: the string translator reads a
// parenthesized selector (`(...)[5m]`), the scalar of `sum(2 * ...)` and the
// `topk (5,` spacing as the text of a line phrase, so any change of the text
// changes the phrase. The list may only shrink.
var reprintTranslationDiffers = map[string]bool{
	"bottomk(1, sum by (app) (count_over_time({namespace=\"prod\",env=\"production\"}[5m])))":                                                         true,
	"bottomk(3, sum by (app) (count_over_time({env=\"production\"}[5m])))":                                                                            true,
	"sort_desc(sum by (app) (count_over_time({namespace=\"prod\",env=\"production\"}[5m])))":                                                          true,
	"sort(sum by (app) (count_over_time({env=\"production\"}[5m])))":                                                                                  true,
	"sort(sum by (app) (count_over_time({namespace=\"prod\",env=\"production\"}[5m])))":                                                               true,
	"sort(sum(rate({env=\"production\", app=\"api-gateway\"} | unpack | keep level [1h])))":                                                           true,
	"sum by (app) (bytes_over_time(({env=\"production\"} |~ \"level=(debug|info)\")[5m]))":                                                            true,
	"sum by (app) (rate({env=\"production\"}[5m] | json | drop __error__))":                                                                           true,
	"sum by (level) (count_over_time(({env=\"production\"} | json | drop __error__)[1m] offset 5m))":                                                  true,
	"sum by (level) (count_over_time(({env=\"production\"} | json | drop __error__)[1m]))":                                                            true,
	"sum(2 * count_over_time({service_name=\"api-gateway\"}[5m]))":                                                                                    true,
	"sum(count_over_time((({env=\"production\"} |= \"failed\")[5m])))":                                                                                true,
	"sum(count_over_time(({env=\"production\"} |= \"failed\")[5m]))":                                                                                  true,
	"sum(rate(({env=\"production\"} |= \"failed\")[5m]))":                                                                                             true,
	"topk(2, sum by (app) (count_over_time({namespace=\"prod\",env=\"production\",app=~\"api-gateway|worker|payment-service\"}[5m])))":                true,
	"topk(3, label_replace(sum by (service_name) (count_over_time({env=\"production\"}[5m])), \"svc_short\", \"$1\", \"service_name\", \"(.*)-.*\"))": true,
	"topk(3, sum by (app) (count_over_time({env=\"production\"}[5m])))":                                                                               true,
	"topk(5, sum by (app) (rate({env=\"production\"}[5m])))":                                                                                          true,
	"topk(5, sum by (service_name) (count_over_time({env=\"production\"} | detected_level=\"error\" [5m])))":                                          true,
	"topk(5, sum without (pod) (max_over_time({app=\"payment-service\"} | logfmt amount | unwrap amount [5m])))":                                      true,
}
