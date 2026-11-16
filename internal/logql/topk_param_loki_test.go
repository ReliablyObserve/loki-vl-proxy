package logql

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// topkParamAnswers holds what Loki's parser answers for every query of topkParamQueries.
const topkParamAnswers = "testdata/topk-param-loki.jsonl"

// topkParamFirsts are first arguments of topk and bottomk that are not a number followed by a comma: a negative or
// signed number, a binary operation of literals, a parenthesised literal, a metric or log expression, a minus before
// something that is not a number, and the lexer corners `-0` and `-0x1`. `+1` and `+ 1` are written too: Loki reads a
// leading plus as part of the literal and the proxy does not yet (semantics/unary-plus-literal-rejected), so their
// rows are left out of the comparison and counted apart.
var topkParamFirsts = []string{
	"-1", "--1", "- 1", "-1.5", "-1e2", "-inf", "- -1", "-(1)", "-vector(1)", `-{app="a"}`, "-0", "-0x1",
	"1 + 2", "0 / 0", "1 / 0", "1 % 0", "1 and 2", "1 > 0", "1 or 2", "1 ^ 2", "1 == bool 2", "1 + on (a) 2",
	"-1 + 2", "-1 * vector(1)", "(1)", "(-1)", "vector(1)", "1 2",
	`rate({app="a"}[5m])`, `1 + rate({app="a"}[5m])`, `sum(rate({app="a"}[5m]))`, `{app="a"}`, `{app="a"} | json`, `({app="a"})`,
	"+1", "+ 1",
}

// topkParamInner is the expression the operation aggregates in the grid.
var topkParamInner = []string{`sum by (level) (count_over_time({app="a"}[5m]))`, `rate({app="a"}[5m])`}

// topkParamWrappers are expressions around the operation that Loki keeps its parse error through.
var topkParamWrappers = []string{
	"sum(%s)", "sort(%s)", "sort_desc(%s)", "max by (level) (%s)", "avg without (pod) (%s)", "count(%s)",
	`label_replace(%s, "x", "$1", "level", "(.*)")`, `sum by (level) (label_replace(%s, "x", "$1", "level", "(.*)"))`,
	"topk(2, %s)", "bottomk(2, %s)", "%s * 2", "2 * %s", "%s or vector(1)", "%s and on (level) %s", "(%s)",
	"sum(sum by (level) (%s))",
}

// topkParamQueries is the grid: every first argument in every spelling of topk and bottomk (a grouping before or
// after, a space before the parenthesis, a missing inner expression, an unterminated call), the operations with a
// valid parameter and with invalid numbers, and the first arguments of the gap inside every wrapper.
func topkParamQueries() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	for _, op := range []string{"topk", "bottomk"} {
		for _, k := range topkParamFirsts {
			for _, inner := range topkParamInner {
				add(op + "(" + k + ", " + inner + ")")
				add(op + " (" + k + ", " + inner + ")")
				add(op + "(" + k + "," + inner + ")")
				add(op + "(" + k + " , " + inner + ")")
				add(op + " by (level) (" + k + ", " + inner + ")")
				add(op + "(" + k + ", " + inner + ") by (level)")
				add(op + "(" + k + ", " + inner)
			}
			add(op + "(" + k + ")")
			add(op + "(" + k + ") by (a)")
			add(op + "(" + k)
			add(op + "(" + k + ",")
		}
		for _, k := range []string{"1", "2", "0", "00", "2.5", "1e1", "0x1"} {
			add(op + "(" + k + `, rate({app="a"}[5m]))`)
			add(op + "(" + k + " , " + `rate({app="a"}[5m]))`)
		}
		add(op + "(1, 2)")
		add(op + "(1, -1)")
		add(op + "(1, vector(1))")
		add(op + `(1, rate({app="a"}[5m]) , 2)`)
		add(op + "(-")
	}
	for _, w := range topkParamWrappers {
		for _, op := range []string{"topk", "bottomk"} {
			for _, k := range []string{"-1", "1 + 2", `rate({app="a"}[5m])`, `{app="a"}`} {
				q := op + "(" + k + ", " + topkParamInner[0] + ")"
				add(strings.ReplaceAll(w, "%s", q))
			}
		}
	}
	return out
}

// unaryPlusFirst reports a query whose first topk or bottomk argument starts with a plus: the proxy rejects the leading
// plus Loki reads (semantics/unary-plus-literal-rejected).
func unaryPlusFirst(q string) bool {
	return strings.Contains(q, "(+1") || strings.Contains(q, "(+ 1")
}

// recordTopkParamAnswers runs Loki's parser (the oracle's check command) over the queries and writes its answers.
func recordTopkParamAnswers(t *testing.T, queries []string) {
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
	for i, q := range queries {
		if err := enc.Encode(map[string]any{"id": fmt.Sprint(i), "kind": "client", "q": q, "ours": true}); err != nil {
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
	var b strings.Builder
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	i := 0
	for sc.Scan() {
		var v struct {
			Accept bool   `json:"accept"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		line, err := json.Marshal(numLitAnswer{Q: queries[i], Accept: v.Accept, Error: v.Error})
		if err != nil {
			t.Fatal(err)
		}
		b.Write(line)
		b.WriteByte('\n')
		i++
	}
	if i != len(queries) {
		t.Fatalf("Loki answered %d of %d queries", i, len(queries))
	}
	if err := os.WriteFile(topkParamAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readTopkParamAnswers(t *testing.T, queries []string) []numLitAnswer {
	t.Helper()
	f, err := os.Open(topkParamAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var answers []numLitAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a numLitAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(queries))
	}
	return answers
}

// A topk or bottomk whose first argument is not a number followed by a comma is answered with Loki's error: Loki
// reads that argument as the expression the operation aggregates, so `topk(-1, …)` fails at the comma (`syntax
// error: unexpected ,`), as `topk(1 + 2, …)`, `topk(rate(…), 2)` and `topk({…}, 2)` do, `topk(-1)` and
// `topk(rate(…))` lack their parameter, and the error stays through every wrapper. Loki's answers are recorded with
// the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/topk-first-argument-error-text
func TestTopkFirstArgumentMatchesLoki(t *testing.T) {
	queries := topkParamQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordTopkParamAnswers(t, queries)
	}
	answers := readTopkParamAnswers(t, queries)
	var accepted, rejected, plus int
	for i, a := range answers {
		if a.Q != queries[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, queries[i])
		}
		if err := terminationError(a.Q); err != nil {
			t.Fatal(err)
		}
		if a.Accept {
			accepted++
		} else {
			rejected++
		}
		if unaryPlusFirst(a.Q) {
			plus++
			continue
		}
		got := ValidateLogQL(a.Q)
		if (got == "") != a.Accept || !a.Accept && got != a.Error {
			t.Errorf("%q\n  proxy: %q\n  Loki:  accept=%v %q", a.Q, got, a.Accept, a.Error)
		}
	}
	t.Logf("%d queries: Loki accepts %d and rejects %d; %d with a leading plus left to semantics/unary-plus-literal-rejected", len(answers), accepted, rejected, plus)
	if rejected < 500 || accepted < 10 || plus == 0 {
		t.Fatalf("grid too small: %d accepted, %d rejected, %d with a leading plus", accepted, rejected, plus)
	}
}

// BenchmarkValidateTopkFirstArgument: the validation of a topk with a valid parameter (unchanged path) and of the
// first arguments Loki reads as an expression.
func BenchmarkValidateTopkFirstArgument(b *testing.B) {
	for _, c := range []struct{ name, q string }{
		{"valid", `topk(10, sum by (app) (rate({app="api"}[5m])))`},
		{"negative", `topk(-1, sum by (app) (rate({app="api"}[5m])))`},
		{"binary", `topk(1 + 2, sum by (app) (rate({app="api"}[5m])))`},
		{"metric_first", `topk(rate({app="api"}[5m]), 2)`},
		{"wrapped", `sort(topk(-1, sum by (app) (rate({app="api"}[5m]))))`},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ValidateLogQL(c.q)
			}
		})
	}
}
