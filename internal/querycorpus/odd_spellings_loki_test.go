package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// oddSpellingsAnswers are Loki's answers (the Loki oracle, test/oracle/loki, check and process commands) on the
// spellings behind the entries of testdata/known-typed-divergences.json that are about what Loki reads, and on the
// label names the query plan quotes.
const oddSpellingsAnswers = "testdata/odd-spellings-loki.jsonl"

type oddAnswer struct {
	Kind   string            `json:"kind"` // check or process
	Query  string            `json:"q"`
	Labels map[string]string `json:"labels,omitempty"`
	Accept bool              `json:"accept,omitempty"` // check
	Keep   bool              `json:"keep,omitempty"`   // process
}

// unicodeBlankQueries are queries with a blank that is no ASCII one between two tokens.
func unicodeBlankQueries() []string {
	var qs []string
	for _, w := range []string{" ", "\u0085", " ", "　"} {
		qs = append(qs,
			"rate"+w+`({a="b"}[5m])`, `sum by (a) (count_over_time`+w+`({a="b"}[5m]))`,
			`{a="b"} | line_format`+w+`"x"`, `{a="b"} |`+w+`json`, `{a="b"} |=`+w+`"x"`, `{a="b"}`+w+`| json`)
	}
	return qs
}

// quotedExoticQueries are label filters that name a label VictoriaLogs' lexer does not join into a word.
func quotedExoticQueries() []string {
	var qs []string
	for _, n := range exoticNamesForLoki {
		qs = append(qs, `{a="b"} | x = "y" and `+n+` = "z"`, `{a="b"} | (`+n+` = "z")`, `{a="b"} | json | x = "y" and `+n+` > 5`)
	}
	return qs
}

var exoticNamesForLoki = []string{"、", ";", "@", "³", "Ⅰ", "𝛁", "😀", "ा", "\u200b", "·", "☃", "»"}

// signedBlankValues are the values the signed number comparison is run on.
var signedBlankValues = []string{"-6", "-1", "-0.6", "-0.4", "0", "0.4", "1", "5"}

func oddSpellingQueries() (checks []string, signed [][2]string) {
	checks = append(checks, unicodeBlankQueries()...)
	checks = append(checks, quotedExoticQueries()...)
	for _, c := range []string{">", ">=", "<", "<=", "==", "!="} {
		for _, pair := range [][2]string{{"- .5", "-.5"}, {"- 5", "-5"}, {"+ 5", "+5"}, {"-  0.5", "-0.5"}} {
			signed = append(signed, [2]string{`{app="x"} | a ` + c + ` ` + pair[0], `{app="x"} | a ` + c + ` ` + pair[1]})
		}
	}
	return checks, signed
}

func recordOddSpellings(t *testing.T) {
	t.Helper()
	checks, signed := oddSpellingQueries()
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiFactsDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in := filepath.Join(work, "in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for i, q := range checks {
		if err := enc.Encode(oracleInput{ID: strconv.Itoa(i), Kind: "odd", Query: q, Ours: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(work, "out.jsonl")
	if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	verdicts := readVerdictFile(t, out)
	var rows []oddAnswer
	for i, q := range checks {
		rows = append(rows, oddAnswer{Kind: "check", Query: q, Accept: verdicts[strconv.Itoa(i)].Accept})
	}
	var cases []oraclerun.ProcessCase
	for _, pair := range signed {
		for _, q := range pair {
			for _, v := range signedBlankValues {
				cases = append(cases, oraclerun.ProcessCase{Query: q, Labels: map[string]string{"app": "x", "a": v}, Line: "l"})
			}
		}
	}
	answers, err := oraclerun.LokiProcess(lokiFactsDir, cases)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cases {
		if answers[i].Error != "" {
			t.Fatalf("%s: Loki rejects it: %s", c.Query, answers[i].Error)
		}
		rows = append(rows, oddAnswer{Kind: "process", Query: c.Query, Labels: c.Labels, Keep: answers[i].Keep})
	}
	o, err := os.Create(oddSpellingsAnswers)
	if err != nil {
		t.Fatal(err)
	}
	oenc := json.NewEncoder(o)
	oenc.SetEscapeHTML(false)
	for _, r := range rows {
		if err := oenc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
}

func readOddSpellings(t *testing.T) []oddAnswer {
	t.Helper()
	f, err := os.Open(oddSpellingsAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var rows []oddAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r oddAnswer
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// planText is the query plan's text for q with the identity label function, "" when the plan leaves it to the
// string translator.
func planText(t *testing.T, q string) string {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		return ""
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return ""
	}
	queryplan.Resolve(p)
	op, err := lower.PlanQuery(q, p, lower.Profile{Caps: logsql.CapabilitiesFor("1.53.0"), Label: func(s string) string { return s }})
	if err != nil {
		return ""
	}
	if vq, ok := op.(physical.VLQuery); ok {
		return vq.Text
	}
	return ""
}

// TestOddSpellingsMatchLoki holds what the entries of known-typed-divergences.json and the plan's quoting rest on to
// Loki's answers:
//   - Loki reads a blank that is no ASCII one (U+00A0, U+0085, U+2003, U+3000) as part of a name, so it answers 400
//     for every query that has one between two tokens, where the proxy's parser reads it as white space
//     (semantics/parser-reads-unicode-blank-as-white-space);
//   - Loki reads `a > - .5` as it reads `a > -.5`: the same lines are kept for every operator and value, and the plan
//     writes the same text for both, where the translator copies the blank into a number VictoriaLogs rejects
//     (semantics/translator-writes-signed-number-with-blank);
//   - Loki accepts a label filter that names a label VictoriaLogs cannot read bare, and the plan quotes it
//     (semantics/translator-writes-exotic-field-names-bare).
//
// LVP_RUN_ORACLES=1 records the answers again.
//
// conformance: semantics/parser-reads-unicode-blank-as-white-space, semantics/translator-writes-signed-number-with-blank, semantics/translator-writes-exotic-field-names-bare
func TestOddSpellingsMatchLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordOddSpellings(t)
	}
	rows := readOddSpellings(t)
	checks, signed := oddSpellingQueries()
	byQuery := map[string]oddAnswer{}
	kept := map[string]bool{}
	for _, r := range rows {
		switch r.Kind {
		case "check":
			byQuery[r.Query] = r
		case "process":
			kept[r.Query+"\x00"+r.Labels["a"]] = r.Keep
		}
	}
	if len(byQuery) != len(checks) || len(kept) != len(signed)*2*len(signedBlankValues) {
		t.Fatalf("%d check and %d process answers recorded: record them again with LVP_RUN_ORACLES=1", len(byQuery), len(kept))
	}
	for _, q := range unicodeBlankQueries() {
		if byQuery[q].Accept {
			t.Errorf("Loki accepts %q", q)
		}
		if logql.ParseAndValidate(q) != nil {
			t.Errorf("%q: the proxy's parser now rejects it as Loki does: remove its entries from known-typed-divergences.json", q)
		}
	}
	for _, q := range quotedExoticQueries() {
		if !byQuery[q].Accept {
			t.Errorf("Loki rejects %q", q)
		}
		if text := planText(t, q); text != "" {
			quoted := false
			for _, n := range exoticNamesForLoki {
				quoted = quoted || strings.Contains(text, logsql.QuoteFieldName(n)+":")
			}
			if !quoted {
				t.Errorf("%q: the plan's text %s names no exotic label quoted", q, text)
			}
		}
	}
	for _, pair := range signed {
		for _, v := range signedBlankValues {
			if a, b := kept[pair[0]+"\x00"+v], kept[pair[1]+"\x00"+v]; a != b {
				t.Errorf("Loki keeps a=%s differently: %q (%v) and %q (%v)", v, pair[0], a, pair[1], b)
			}
		}
		if a, b := planText(t, pair[0]), planText(t, pair[1]); a != b {
			t.Errorf("the plan writes %q and %q differently:\n %s\n %s", pair[0], pair[1], a, b)
		}
	}
}
