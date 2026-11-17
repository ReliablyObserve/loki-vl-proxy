package querycorpus_test

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankspace"
)

// A tab, a line feed, a carriage return or a run of them where a space stands between two tokens is the same query to
// Loki (TestBlankAfterKeywordMatchesLoki), so the VictoriaLogs oracle's typed-selector pair of every blank spelling is
// the pair of the spaced spelling: the plan's text of the query as sent, against the translator's text of the spelling
// it reads as Loki does. The string translator copies the blank after a parser keyword or a stage name into a pipe
// VictoriaLogs rejects (semantics/translator-reads-only-space-between-tokens); a pair made on that text names a typed
// filter that differs where only the translator is wrong, and fails the PR quality gate for every corpus row of
// TestBlankAfterKeywordRequestsEqualSpaced. A spelling the plan leaves to the translator (a field list or an unwrap
// spelled with a blank, which lower.PlanQuery turns away) makes no pair, as the proxy sends no plan text for it.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestTypedPairOfBlankSpellingIsTheSpacedOnes(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	pairs := 0
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		for _, tpl := range blankspace.Templates() {
			ref := strings.ReplaceAll(tpl, blankspace.Mark, " ")
			wantTyped, wantBuilt, wantOK := typedQueryPair(ref, v)
			for _, blank := range blankspace.Skipped {
				q := strings.ReplaceAll(tpl, blankspace.Mark, blank)
				typed, built, ok := typedQueryPair(q, v)
				if !ok && wantOK && !planLeavesToTranslator(q, v) {
					t.Errorf("%s %q: no pair, where the plan lowers it and the spaced spelling makes one", v, q)
					continue
				}
				if !ok {
					continue
				}
				if !wantOK || typed != wantTyped || built != wantBuilt {
					t.Errorf("%s %q: pair %v\n typed %q\n built %q\nthe spaced spelling's: pair %v\n typed %q\n built %q",
						v, q, ok, typed, built, wantOK, wantTyped, wantBuilt)
				}
				pairs++
			}
		}
	}
	if pairs < 500 {
		t.Fatalf("%d pairs made: the grid is too small", pairs)
	}
	t.Logf("%d blank spellings paired as the spaced spelling", pairs)
}

// planLeavesToTranslator reports whether the plan turns q away (lower.PlanQuery fails) for VictoriaLogs version v.
func planLeavesToTranslator(q, v string) bool {
	expr, err := logql.Parse(q)
	if err != nil {
		return true
	}
	plan, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return true
	}
	queryplan.Resolve(plan)
	_, err = lower.PlanQuery(q, plan, lower.Profile{Caps: logsql.CapabilitiesFor(strings.TrimPrefix(v, "v")), Label: func(s string) string { return s }})
	return err != nil
}

// spelledAsTranslatorReads writes the blanks Loki skips outside quoted strings as one space and a blank-only grouping
// as `by()`, and leaves quoted text alone.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestSpelledAsTranslatorReads(t *testing.T) {
	for in, want := range map[string]string{
		"{A=\"0\"} | json\r\nA0":                            `{A="0"} | json A0`,
		"{A=\"0\"}\t|\tline_format\t\"{{.a}}\"":             `{A="0"} | line_format "{{.a}}"`,
		"{A=\"0\"} |= \"a\tb\"":                             "{A=\"0\"} |= \"a\tb\"",
		"{A=\"0\"} |= `a\n\tb`":                             "{A=\"0\"} |= `a\n\tb`",
		"sum by (\t) (rate({A=\"0\"}[1m]))":                 `sum by () (rate({A="0"}[1m]))`,
		`{A="0"} | json A0`:                                 `{A="0"} | json A0`,
		"{A=\"0\"} |= ip\t(\"1.2.3.4\")":                    `{A="0"} |= ip("1.2.3.4")`,
		"{A=\"0\"} != IP  (\"::1\") or ip\r\n(\"1.2.3.4\")": `{A="0"} != IP("::1") or ip("1.2.3.4")`,
		`{A="0"} |= "ip (x)" |= zip ("a")`:                  `{A="0"} |= "ip (x)" |= zip ("a")`,
	} {
		if got := spelledAsTranslatorReads(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

// An ip() line filter with a blank Loki skips before its parenthesis (`|= ip<TAB>("1.2.3.4")`, `|= ip  (…)`) is
// Loki's query without the blank (recorded Loki answers, internal/logql testdata/keyword-blank-loki.jsonl.gz), and the
// plan serves it so. The string translator reads the call only with no blank (`|= ip ("1.2.3.4")` becomes the word
// filter "ip" and a phrase VictoriaLogs rejects), so the pair of every such spelling the recording accepts is the pair of
// the spelling without the blank. Found by the PR quality gate's typed-selector check on the rows
// TestKeywordBlankRequestsEqualSpaced puts in the query corpus.
//
// conformance: semantics/translator-reads-only-space-between-tokens, semantics/blank-before-call-parenthesis-read-unlike-loki
func TestTypedPairOfIPCallBlankIsTheUnspacedOnes(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	fh, err := os.Open(filepath.Join("..", "logql", "testdata", "keyword-blank-loki.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	pairs := 0
	for dec := json.NewDecoder(zr); ; {
		var row struct {
			Q, Ref string
			Accept bool
		}
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if !row.Accept || !strings.Contains(row.Ref, "|= ip(") && !strings.Contains(row.Ref, "!= ip(") {
			continue
		}
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			wantTyped, wantBuilt, wantOK := typedQueryPair(row.Ref, v)
			typed, built, ok := typedQueryPair(row.Q, v)
			if !wantOK || ok != wantOK || typed != wantTyped || built != wantBuilt {
				t.Errorf("%s %q: pair %v\n typed %q\n built %q\nthe spelling without the blank's: pair %v\n typed %q\n built %q",
					v, row.Q, ok, typed, built, wantOK, wantTyped, wantBuilt)
			}
			pairs++
		}
	}
	if pairs < 14 {
		t.Fatalf("%d ip() line filter spellings paired: the recording holds fewer than expected", pairs)
	}
}

// The typed-selector check needs no base corpus: with none (the CI cache of the base commit's corpus missed), a typed
// filter that differs from the string translator's still fails the gate, and a stale listed divergence still does,
// while the base-versus-PR checks find nothing. Before, the check ran only when a base was found, so the blank spellings
// of TestBlankAfterKeywordRequestsEqualSpaced passed every run that missed the cache and failed every run that hit it.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestOracleRegressionsWithoutBase(t *testing.T) {
	head := map[string]map[string]oracleVerdict{
		"golden":         {"g1": {Accept: false}},
		"case-accept":    {"c1": {Accept: true}},
		"typed-selector": {"t1": {Accept: true, Ours: true, SameMeaning: boolp(true)}, "t2": {Accept: false, Ours: true}},
	}
	got := oracleRegressions("v1.53.0", nil, head)
	if strings.Join(got.TypedSelectorsDiffer, ",") != "t2" || len(got.GoldensRejected)+len(got.GoldensMeaningLost)+len(got.CasesLost) != 0 || !got.any() {
		t.Errorf("regressions without a base = %+v", got)
	}
	if fails := reportFailures(Report{OracleRegressions: []OracleRegressions{got}}); len(fails) != 1 || !strings.Contains(fails[0], "1 stream selectors whose typed filter") {
		t.Errorf("failures = %q", fails)
	}
}
