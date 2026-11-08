package proxy

import (
	"slices"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The proxy translates the printed text of a parsed query in several places (a
// binary operation is evaluated one operand at a time, a parser stage is
// removed and the query printed again). Printing must not change what the
// translator makes of a query: every query of the benchmark shapes and the
// semantics matrix, and the literals the scanner cuts in pieces, translate the
// same after a parse and a print.
// conformance: semantics/label-filter-expression-syntax
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
