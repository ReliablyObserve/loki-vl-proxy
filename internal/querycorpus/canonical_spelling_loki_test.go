package querycorpus_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

const canonicalSpellingAnswers = "testdata/canonical-spelling-loki.jsonl.gz"

// canonicalSpellingAnswer is what Loki's parser says about a query and the text the request rewrite writes for it
// (the oracle's check command with the respelling as the printed form).
type canonicalSpellingAnswer struct {
	Q         string `json:"q"`
	Respelled string `json:"respelled"`
	Accept    bool   `json:"accept"`
	Error     string `json:"error,omitempty"`
	Same      bool   `json:"same,omitempty"`
}

// canonicalSpellingBlanks are the blanks Loki's lexer skips, alone and in runs.
var canonicalSpellingBlanks = []string{" ", "\t", "\n", "\r", "  ", "\r\n", " \t\n"}

// canonicalSpellingGrid is a grid of metric queries spelled with each blank where the request rewrite removes it
// (between a call's name and its parenthesis, right inside the parenthesis) and with parentheses it removes (around a
// call, around parentheses, around a binary operation no operator reads).
func canonicalSpellingGrid() []string {
	var out []string
	for _, tpl := range []string{
		`rate%({app="x"}[5m])`,
		`count_over_time%({app="x"} |= "a" [5m])`,
		`bytes_rate%({app="x"}[5m] offset 1h)`,
		`sum_over_time%({app="x"} | unwrap n [5m])`,
		`quantile_over_time%(%0.5, {app="x"} | unwrap n [5m]%)`,
		`max_over_time%({app="x"} | unwrap n [5m]) by (a)`,
		`absent_over_time%({app="x"}[5m])`,
		`sum%(rate({app="x"}[5m]))`,
		`sum by (a) (%rate({app="x"}[5m])%)`,
		`avg%(count_over_time({app="x"}[5m])) without (a)`,
		`topk%(%2, sum by (a) (rate({app="x"}[5m]))%)`,
		`bottomk%(2, rate({app="x"}[5m]))`,
		`sort_desc%(sum by (a) (rate({app="x"}[5m])))`,
		`stddev%(rate({app="x"}[5m]))`,
		`rate({app="x"}[5m]) + vector%(%1%)`,
		`vector%(%2%) * sum by (a) (rate({app="x"}[5m]))`,
		`label_replace%(%rate({app="x"}[5m]), "a", "$1", "b", "(.*)"%)`,
		`min(max%(count(rate({app="x"}[1m])) without (rack))) by (svc)`,
	} {
		for _, b := range canonicalSpellingBlanks {
			out = append(out, strings.ReplaceAll(tpl, "%", b))
		}
	}
	for _, tpl := range []string{
		`«rate({app="x"}[5m])»`,
		`«sum by (a) (rate({app="x"}[5m]))»`,
		`sum(«rate({app="x"}[5m])»)`,
		`«rate({app="x"}[5m]) + 1»`,
		`sum(«rate({app="x"}[5m]) * 2»)`,
		`topk(2, «sum by (a) (rate({app="x"}[5m])) / 2»)`,
		`«sum(rate({app="x"}[5m]))» / «sum(rate({app="y"}[5m]))»`,
		`rate({app="x"}[5m]) > bool «vector(1)»`,
		`rate({app="x"}[5m]) and«rate({app="y"}[5m])»`,
		`label_replace(«rate({app="x"}[5m])», "a", "$1", "b", "(.*)")`,
		`«vector (2) + sum by (a) (rate({app="x"}[5m]))» * 2`,
	} {
		for _, open := range []string{"(", "((", "( ", "(\n(", "(\t"} {
			closing := map[string]string{"(": ")", "((": "))", "( ": " )", "(\n(": ")\n)", "(\t": "\t)"}[open]
			out = append(out, strings.NewReplacer("«", open, "»", closing).Replace(tpl))
		}
	}
	return out
}

// canonicalSpellingQueries are the queries the respelling is held to Loki for: every plan-corpus row the request
// rewrite respells, and the grid.
func canonicalSpellingQueries(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, q := range planCorpus(t) {
		if proxy.RequestQueryText(q) != logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(q))) {
			out = append(out, q)
		}
	}
	return append(out, canonicalSpellingGrid()...)
}

func recordCanonicalSpellingAnswers(t *testing.T, queries []string) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(filepath.Join("..", "..", "test", "oracle", "loki"), "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for i, q := range queries {
		if err := enc.Encode(map[string]any{"id": strconv.Itoa(i), "kind": "client", "q": q, "ours": true, "printed": proxy.RequestQueryText(q)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	r, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var b bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
	zw.ModTime, zw.Name = zw.ModTime.UTC(), ""
	w := json.NewEncoder(zw)
	w.SetEscapeHTML(false)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	i := 0
	for sc.Scan() {
		var v struct {
			Accept      bool   `json:"accept"`
			Error       string `json:"error"`
			SameMeaning *bool  `json:"same_meaning"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		a := canonicalSpellingAnswer{Q: queries[i], Respelled: proxy.RequestQueryText(queries[i]), Accept: v.Accept, Error: v.Error, Same: v.SameMeaning != nil && *v.SameMeaning}
		if err := w.Encode(a); err != nil {
			t.Fatal(err)
		}
		i++
	}
	if i != len(queries) {
		t.Fatalf("Loki answered %d of %d queries", i, len(queries))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(canonicalSpellingAnswers, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readCanonicalSpellingAnswers(t *testing.T) []canonicalSpellingAnswer {
	t.Helper()
	f, err := os.Open(canonicalSpellingAnswers)
	if err != nil {
		t.Fatalf("%v: record Loki's answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []canonicalSpellingAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a canonicalSpellingAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

// The request rewrite's spelling (proxy.RequestQueryText, logql.CanonicalSpelling) is the query to Loki: Loki's
// parser accepts every query the rewrite respells, plan-corpus rows and a grid of blanks and parentheses, and reads
// the respelled text as the same expression (the Loki oracle's check command, the respelling as the printed form);
// the proxy's validation accepts both. Every other corpus row keeps its text, so the respelling round-trips through
// Loki's parser on the whole corpus. Loki's answers are recorded (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/translator-reads-blank-before-range-call-as-text, semantics/translator-reads-parenthesized-metric-as-text, semantics/vector-call-blank-before-parenthesis-as-query
func TestCanonicalSpellingMatchesLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	queries := canonicalSpellingQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordCanonicalSpellingAnswers(t, queries)
	}
	answers := readCanonicalSpellingAnswers(t)
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(queries))
	}
	respelled := 0
	for i, a := range answers {
		if a.Q != queries[i] || a.Respelled != proxy.RequestQueryText(a.Q) {
			t.Fatalf("answer %d is for %q respelled %q, not %q respelled %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, a.Respelled, queries[i], proxy.RequestQueryText(queries[i]))
		}
		if !a.Accept {
			t.Errorf("%q: Loki rejects it (%s)", a.Q, a.Error)
			continue
		}
		if !a.Same {
			t.Errorf("%q: Loki reads another query than its respelling %q", a.Q, a.Respelled)
		}
		if msg := logql.ValidateLogQL(a.Q); msg != "" {
			t.Errorf("%q: the proxy rejects what Loki accepts: %s", a.Q, msg)
		}
		if msg := logql.ValidateLogQL(a.Respelled); msg != "" {
			t.Errorf("%q: the proxy rejects the respelling %q: %s", a.Q, a.Respelled, msg)
		}
		if a.Respelled != a.Q {
			respelled++
		}
	}
	if respelled < len(canonicalSpellingGrid()) {
		t.Errorf("%d of %d queries respelled: the grid holds only spellings the rewrite changes", respelled, len(answers))
	}
	t.Logf("%d queries (%d corpus rows): Loki reads each as its respelling", len(answers), len(answers)-len(canonicalSpellingGrid()))
}
