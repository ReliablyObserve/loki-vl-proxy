package logql

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// errorOrderAnswers holds what Loki's parser and pipeline build (the Loki oracle's check command) answer for every
// query of errorOrderQueries.
const errorOrderAnswers = "testdata/error-order-loki.jsonl.gz"

// errorOrderRow is one query of the error-order grid: the check it carries (carrier), the place of the second
// error the query holds (context), and Loki's recorded answer.
type errorOrderRow struct {
	Carrier string `json:"carrier"`
	Context string `json:"context"`
	Q       string `json:"q"`
	Accept  bool   `json:"accept"`
	Error   string `json:"error,omitempty"`
}

// errorCarrier is a check Loki makes while it reads or builds a query, written as the smallest query that fails it.
// A log carrier is a pipeline stage written after a selector, a selector carrier a matcher inside the selector, a
// metric carrier a whole metric expression.
type errorCarrier struct {
	name, kind, text string
}

var errorOrderCarriers = []errorCarrier{
	// Pipeline stages.
	{"pattern-capture", "log", `| pattern "<_>"`},
	{"regexp-parser", "log", `| regexp "x"`},
	{"label-filter-regex", "log", `| c=~"("`},
	{"drop-regex", "log", `| drop c=~"("`},
	{"keep-regex", "log", `| keep c=~"("`},
	{"line-filter-regex", "log", `|~ "("`},
	{"line-filter-pattern", "log", `|> "<_><_>"`},
	{"line-format-template", "log", `| line_format "{{.a"`},
	{"label-format-template", "log", `| label_format x="{{.a"`},
	{"label-format-error-label", "log", `| label_format __error__="x"`},
	{"json-expression", "log", `| json x="a["`},
	{"logfmt-expression", "log", `| logfmt x="a["`},
	{"ip-label-filter", "log", `| addr=ip("zzz")`},
	// Selector matchers.
	{"selector-regex", "selector", `a=~"(b"`},
	{"selector-regex-repeat", "selector", `a=~"b{1001}"`},
	{"empty-compatible-matcher", "selector", `a=~".*"`},
	// Metric expressions.
	{"unsupported-parameter-sum", "metric", `sum(1, rate({a="b"}[5m]))`},
	{"unsupported-parameter-sort", "metric", `sort(1, rate({a="b"}[5m]))`},
	{"approx-topk-grouping", "metric", `approx_topk by (a) (2, rate({a="b"}[5m]))`},
	{"range-grouping-not-allowed", "metric", `count_over_time({a="b"}[5m]) by (a)`},
	{"range-without-unwrap", "metric", `sum_over_time({a="b"}[5m])`},
	{"range-with-unwrap", "metric", `count_over_time({a="b"} | unwrap c [5m])`},
	{"binary-log-leg", "metric", `{a="b"} + rate({a="b"}[5m])`},
	{"set-literal-leg", "metric", `1 and rate({a="b"}[5m])`},
	{"label-replace-regex", "metric", `label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(")`},
	{"label-replace-destination", "metric", `label_replace(rate({a="b"}[5m]), "1d", "$1", "a", "(.*)")`},
	{"topk-zero", "metric", `topk(0, rate({a="b"}[5m]))`},
	{"topk-fraction", "metric", `topk(1.5, rate({a="b"}[5m]))`},
	{"sort-grouping", "metric", `sort by (a) (rate({a="b"}[5m]))`},
	{"quantile-without-parameter", "metric", `quantile_over_time({a="b"} | unwrap c [5m])`},
}

// errorContext places a second error (or none) around a carrier: a syntax error at the very next token or further
// right, a lexer error before or after, a syntax error before, a second check before or after, and the carrier
// inside a metric expression that is not terminated.
type errorContext struct {
	name     string
	pre, suf string
}

var logContexts = []errorContext{
	{"alone", "", ""},
	{"syntax-next-paren", "", " )"},
	{"syntax-next-string", "", ` "x"`},
	{"syntax-end-after-pipe", "", " |"},
	{"syntax-far", "", " | unpack a"},
	{"syntax-farther", "", ` |= "x" | a=="b"`},
	{"lexer-after", "", " | c > 1PB"},
	{"lexer-before", " | c > 1PB", ""},
	{"escape-after", "", ` |= "\q"`},
	{"escape-before", ` |= "\q"`, ""},
	{"syntax-before", " | unpack a", ""},
	{"check-before", ` | regexp "y"`, ""},
	{"check-after", "", ` | regexp "y"`},
	{"metric-unterminated", "count_over_time(", " [5m]"},
	{"metric-next", "count_over_time(", " [5m]) )"},
	{"metric-far", "sum(count_over_time(", ` [5m])) + rate({a="b"} | unpack a [5m])`},
	{"metric-right-unterminated", `rate({a="b"}[5m]) + count_over_time(`, " [5m]"},
	{"metric-tree-grouping", "count_over_time(", " [5m]) by (a)"},
	{"metric-tree-without-unwrap", "sum_over_time(", " [5m])"},
	{"metric-tree-sort-grouping", "sort by (a) (count_over_time(", " [5m]))"},
}

var selectorContexts = []errorContext{
	{"alone", "", ""},
	{"syntax-in-selector-next", "", ` "x"`},
	{"syntax-in-selector-later", "", `, c==}`},
	{"syntax-next-paren", "", "} )"},
	{"syntax-far", "", "} | unpack a"},
	{"syntax-farther", "", `} |= "x" | a=="b"`},
	{"lexer-after", "", "} | c > 1PB"},
	{"escape-after", "", `} |= "\q"`},
	{"escape-before-in-selector", `c="\q", `, ""},
	{"syntax-before-in-selector", `c=="d", `, ""},
	{"regex-before-in-selector", `c=~"(", `, ""},
	{"check-after", "", `} | pattern "<_>"`},
	{"metric-unterminated", "count_over_time({", "}[5m]"},
	{"metric-far", "rate({", `}[5m]) + rate({a="b"} | unpack a [5m])`},
}

var metricContexts = []errorContext{
	{"alone", "", ""},
	{"syntax-next-paren", "", " )"},
	{"syntax-next-range", "", " [5m]"},
	{"syntax-far", "", ` + rate({a="b"} | unpack a [5m])`},
	{"lexer-after", "", ` + rate({a="b"}[1ns])`},
	{"lexer-before", `rate({a="b"}[1ns]) + `, ""},
	{"escape-after", "", ` + rate({a="\q"}[5m])`},
	{"escape-before", `rate({a="\q"}[5m]) + `, ""},
	{"syntax-before", `rate({a="b"} | unpack a [5m]) + `, ""},
	{"check-before", `topk(0, rate({a="b"}[5m])) + `, ""},
	{"check-after", "", ` + topk(0, rate({a="b"}[5m]))`},
	{"unterminated", "", ""},
	{"wrapped-unterminated", "sum(", ""},
}

// errorWrappers are the expressions Loki keeps a query's error through, each around a metric expression that holds
// the carrier: a vector aggregation, sort, topk, label_replace, arithmetic on either side and the set operations.
var errorWrappers = []struct{ name, text string }{
	{"sum", "sum(%s)"}, {"sort", "sort(%s)"}, {"topk", "topk(2, %s)"},
	{"label-replace", `label_replace(%s, "x", "$1", "a", "(.*)")`},
	{"times-right", "%s * 2"}, {"times-left", "2 * %s"},
	{"and", `%s and on () count_over_time({a="b"}[5m])`}, {"or-right", `count_over_time({a="b"}[5m]) or %s`},
	{"unless", `%s unless count_over_time({a="b"}[5m])`},
}

// carrierMetric is a metric expression that holds the carrier: the metric carrier itself, a log carrier's stage in a
// count_over_time, a selector carrier's matcher in a count_over_time's selector.
func carrierMetric(c errorCarrier) string {
	switch c.kind {
	case "log":
		// A line filter after the stage reads its line, so Loki builds a line_format there too.
		return `count_over_time({a="b"} ` + c.text + ` |= "x" [5m])`
	case "selector":
		return "count_over_time({" + c.text + "}[5m])"
	}
	return c.text
}

// paramOps are the operations whose first argument the parameter grid writes; paramValues the first arguments.
var (
	paramOps = []string{"sum", "avg", "min", "max", "count", "stddev", "stdvar", "sort", "sort_desc", "topk", "bottomk",
		"quantile_over_time"}
	paramValues = []string{"1", "-1", "--1", "- 1", "1 + 2", "(1)", "1.5", "0", `"x"`, "-1.5", "1e1", "0x1", "inf",
		"vector(1)", "+1", `{a="b"}`}
)

// errorOrderQueries is the grid: every carrier in every context of its kind, and the parameter grid (every
// operation with every first argument, bare, grouped and with a space before the parenthesis, and the missing
// parameter of quantile_over_time).
func errorOrderQueries() []errorOrderRow {
	seen := map[string]bool{}
	var out []errorOrderRow
	add := func(carrier, context, q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, errorOrderRow{Carrier: carrier, Context: context, Q: q})
		}
	}
	for _, c := range errorOrderCarriers {
		switch c.kind {
		case "log":
			for _, x := range logContexts {
				if strings.HasPrefix(x.name, "check-") && c.name == "regexp-parser" {
					x.pre = strings.ReplaceAll(x.pre, `regexp "y"`, `pattern "<_>"`)
					x.suf = strings.ReplaceAll(x.suf, `regexp "y"`, `pattern "<_>"`)
				}
				if strings.HasPrefix(x.name, "metric") {
					lq := `{a="b"} ` + c.text
					add(c.name, x.name, x.pre+lq+x.suf)
					continue
				}
				add(c.name, x.name, `{a="b"}`+x.pre+" "+c.text+x.suf)
			}
		case "selector":
			for _, x := range selectorContexts {
				switch {
				case strings.HasPrefix(x.name, "metric"):
					add(c.name, x.name, x.pre+c.text+x.suf)
				case x.name == "alone":
					add(c.name, x.name, "{"+c.text+"}")
				case strings.HasPrefix(x.name, "syntax-in-selector"):
					add(c.name, x.name, "{"+c.text+x.suf+"}")
				case strings.HasSuffix(x.name, "-in-selector"):
					add(c.name, x.name, "{"+x.pre+c.text+"}")
				default:
					add(c.name, x.name, "{"+c.text+x.suf)
				}
			}
		case "metric":
			for _, x := range metricContexts {
				q := c.text
				if strings.HasSuffix(x.name, "unterminated") {
					q = strings.TrimSuffix(q, ")")
					if x.name == "unterminated" && q == c.text {
						continue
					}
				}
				if c.name == "topk-zero" && strings.HasPrefix(x.name, "check-") {
					x.pre = strings.ReplaceAll(x.pre, `topk(0, rate({a="b"}[5m]))`, `sum(1, rate({a="b"}[5m]))`)
					x.suf = strings.ReplaceAll(x.suf, `topk(0, rate({a="b"}[5m]))`, `sum(1, rate({a="b"}[5m]))`)
				}
				add(c.name, x.name, x.pre+q+x.suf)
			}
		}
	}
	for _, c := range errorOrderCarriers {
		for _, w := range errorWrappers {
			q := strings.ReplaceAll(w.text, "%s", carrierMetric(c))
			add(c.name, "wrap-"+w.name, q)
			add(c.name, "wrap-"+w.name+"-syntax-after", q+" )")
		}
	}
	for _, op := range paramOps {
		inner := `rate({a="b"}[5m])`
		if op == "quantile_over_time" {
			inner = `{a="b"} | unwrap c [5m]`
		}
		for _, v := range paramValues {
			add("parameter-"+op, "bare", op+"("+v+", "+inner+")")
			add("parameter-"+op, "space", op+" ("+v+", "+inner+")")
			if op == "quantile_over_time" {
				add("parameter-"+op, "grouped", op+"("+v+", "+inner+") by (a)")
			} else {
				add("parameter-"+op, "grouped", op+" by (a) ("+v+", "+inner+")")
			}
		}
		add("parameter-"+op, "missing", op+"("+inner+")")
		add("parameter-"+op, "missing-unterminated", op+"("+inner)
		add("parameter-"+op, "only", op+"(1)")
	}
	return out
}

// recordErrorOrderAnswers runs Loki's parser and pipeline build (the oracle's check command) over the grid and
// writes its answers.
func recordErrorOrderAnswers(t *testing.T, rows []errorOrderRow) {
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
	for i, r := range rows {
		if err := enc.Encode(map[string]any{"id": fmt.Sprint(i), "kind": "client", "q": r.Q, "ours": true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	enc = json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	sc := bufio.NewScanner(bytes.NewReader(raw))
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
		r := rows[i]
		r.Accept, r.Error = v.Accept, v.Error
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
		i++
	}
	if i != len(rows) {
		t.Fatalf("Loki answered %d of %d queries", i, len(rows))
	}
	var zipped bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&zipped, gzip.BestCompression)
	if _, err := zw.Write(b.Bytes()); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(errorOrderAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// readErrorOrderAnswers reads Loki's recorded answers for the grid, in its order.
func readErrorOrderAnswers(t *testing.T, rows []errorOrderRow) []errorOrderRow {
	t.Helper()
	f, err := os.Open(errorOrderAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []errorOrderRow
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a errorOrderRow
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	if len(answers) != len(rows) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(rows))
	}
	for i, a := range answers {
		if a.Q != rows[i].Q || a.Carrier != rows[i].Carrier || a.Context != rows[i].Context {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, rows[i].Q)
		}
	}
	return answers
}

// TestErrorOrderRecorded records the grid when LVP_RUN_ORACLES=1 and checks that the recording matches the grid.
func TestErrorOrderRecorded(t *testing.T) {
	rows := errorOrderQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordErrorOrderAnswers(t, rows)
	}
	readErrorOrderAnswers(t, rows)
}

// errorPhase is when Loki raises a carrier's error, as the recorded grid shows it.
type errorPhase string

const (
	// phaseAction: while it parses, as the stage or matcher is read. It wins over any syntax or lexer error after
	// it, and over a lexer error before it that leaves the parse going (`1PB`); it loses to a syntax error before it,
	// to an error before it that ends the parse (an invalid escape), and to an earlier action error.
	phaseAction errorPhase = "action"
	// phaseTree: once the whole query has parsed, over the tree. It loses to every syntax, lexer and action error,
	// and among the tree checks the one Loki's walk meets first answers.
	phaseTree errorPhase = "tree"
	// phaseBuild: when Loki builds the pipelines, after the tree checks. It loses to every other error.
	phaseBuild errorPhase = "build"
)

// carrierPhases is the phase of every carrier of the grid.
var carrierPhases = map[string]errorPhase{
	"pattern-capture": phaseAction, "regexp-parser": phaseAction, "label-filter-regex": phaseAction,
	"drop-regex": phaseAction, "keep-regex": phaseAction, "selector-regex": phaseAction,
	"selector-regex-repeat": phaseAction,

	"line-filter-regex": phaseBuild, "line-filter-pattern": phaseBuild, "line-format-template": phaseBuild,
	"label-format-template": phaseBuild, "label-format-error-label": phaseBuild, "json-expression": phaseBuild,
	"logfmt-expression": phaseBuild, "ip-label-filter": phaseBuild,

	"empty-compatible-matcher": phaseTree, "unsupported-parameter-sum": phaseTree,
	"unsupported-parameter-sort": phaseTree, "approx-topk-grouping": phaseTree,
	"range-grouping-not-allowed": phaseTree, "range-without-unwrap": phaseTree, "range-with-unwrap": phaseTree,
	"binary-log-leg": phaseTree, "set-literal-leg": phaseTree, "label-replace-regex": phaseTree,
	"topk-zero": phaseTree, "topk-fraction": phaseTree, "sort-grouping": phaseTree,
	"quantile-without-parameter": phaseTree,
}

// carrierWins reports whether a carrier of phase raised in context answers, by the rules of errorPhase. In the
// check-before and check-after contexts the other check is an action for a pipeline or selector carrier and a tree
// check for a metric carrier.
func carrierWins(phase errorPhase, kind, context string) bool {
	if w, ok := strings.CutPrefix(context, "wrap-"); ok {
		// Every wrapper keeps the carrier's error; a syntax error after the whole expression answers ahead of the
		// tree checks and the build, never ahead of an action.
		return !strings.HasSuffix(w, "-syntax-after") || phase == phaseAction
	}
	switch phase {
	case phaseAction:
		switch context {
		case "syntax-before", "escape-before", "escape-before-in-selector", "check-before",
			"syntax-before-in-selector", "regex-before-in-selector":
			return false
		}
		return true
	case phaseTree:
		return context == "alone" || context == "check-after" && kind == "metric"
	}
	return context == "alone"
}

// Loki raises a query's errors in phases, and the first phase that fails answers: the actions its parser runs
// while it reads the query (a pattern or regexp parser that does not compile, a regex in a selector, a label
// filter, drop or keep), then the errors of its lexer, then the syntax error, then the checks over the parsed tree
// (a grouping a range aggregation does not allow, an unwrap missing or not allowed, a log query or a literal as a
// binary operand, a label_replace regex, a vector operation parameter, sort with a grouping, the empty-compatible
// selector), then the pipeline build (line filter regex and pattern, templates, json and logfmt expressions, ip(),
// a label_format of __error__). Every row of the recorded grid is held to that rule, and the parameter of a
// vector operation is a number token: a sign or an expression before the comma is a syntax error, a number an
// operation without a parameter refuses is `unsupported parameter`, and quantile_over_time's is read by the
// syntax `NUMBER or { or (` at the parameter's position.
//
// conformance: semantics/loki-error-phases
func TestLokiErrorPhasesRecorded(t *testing.T) {
	rows := readErrorOrderAnswers(t, errorOrderQueries())
	alone := map[string]string{}
	for _, r := range rows {
		if r.Context == "alone" {
			alone[r.Carrier] = r.Error
		}
	}
	checked := 0
	for _, r := range rows {
		phase, ok := carrierPhases[r.Carrier]
		if !ok {
			continue
		}
		kind := carrierKind(r.Carrier)
		if r.Context == "alone" {
			if r.Accept {
				// label_replace with a destination Prometheus accepts: a control.
				continue
			}
			checked++
			continue
		}
		if alone[r.Carrier] == "" {
			continue
		}
		checked++
		if r.Context == "wrap-times-left" && (r.Carrier == "binary-log-leg" || r.Carrier == "set-literal-leg") {
			// `2 * a + b` and `2 * 1 and b` bind the multiplication first: the leg Loki names is another one.
			continue
		}
		// A wrapped line filter carrier is reported with the line filter after it, as one run of filters.
		text := strings.ReplaceAll(r.Error, ` |= "x"`, "")
		if got, want := !r.Accept && text == alone[r.Carrier], carrierWins(phase, kind, r.Context); got != want {
			t.Errorf("%s (%s) in %s: Loki answers %q; the %s rule says the carrier's error %s",
				r.Carrier, phase, r.Context, r.Error, phase, map[bool]string{true: "answers", false: "does not answer"}[want])
		}
	}
	for _, r := range rows {
		op, ok := strings.CutPrefix(r.Carrier, "parameter-")
		if !ok || r.Context != "bare" {
			continue
		}
		checked++
		v := strings.TrimSuffix(strings.TrimPrefix(r.Q, op+"("), ", "+strings.SplitN(r.Q, ", ", 2)[1])
		number := v == "1" || v == "0" || v == "1.5" || v == "1e1" || v == "0x1"
		switch {
		case op == "quantile_over_time":
			// The parameter is a NUMBER read as a float; `(` opens the range expression, a selector is the range
			// expression without its range, and an expression after the number wants the comma.
			want, ok := map[string]string{
				"0x1":     `parse error : invalid parameter for operation quantile_over_time: strconv.ParseFloat: parsing "0x1": invalid syntax`,
				"1 + 2":   "parse error at line 1, col 22: syntax error: unexpected +, expecting ,",
				"(1)":     "parse error at line 1, col 21: syntax error: unexpected NUMBER, expecting { or (",
				`{a="b"}`: "parse error at line 1, col 27: syntax error: unexpected ,",
			}[v]
			switch {
			case ok:
				if r.Error != want {
					t.Errorf("%s: Loki answers %q, not %q", r.Q, r.Error, want)
				}
			case number:
				if !r.Accept {
					t.Errorf("%s: Loki answers %q", r.Q, r.Error)
				}
			case !strings.HasPrefix(r.Error, "parse error at line 1, col 20: syntax error: unexpected ") ||
				!strings.HasSuffix(r.Error, "expecting NUMBER or { or ("):
				t.Errorf("%s: Loki answers %q, not a syntax error at the parameter", r.Q, r.Error)
			}
		case number && (op == "topk" || op == "bottomk"):
			if (v == "1") != r.Accept {
				t.Errorf("%s: Loki answers %v %q", r.Q, r.Accept, r.Error)
			}
		case number:
			if want := "parse error : unsupported parameter for operation " + op + "(" + v + ","; r.Error != want {
				t.Errorf("%s: Loki answers %q, not %q", r.Q, r.Error, want)
			}
		case v == "vector(1)" || v == `{a="b"}` || v == "(1)" || v == "1 + 2" || v == "-1" || v == "- 1" || v == "-1.5" || v == "+1":
			if !strings.Contains(r.Error, "syntax error: unexpected ,") {
				t.Errorf("%s: Loki answers %q, not a syntax error at the comma", r.Q, r.Error)
			}
		default:
			if !strings.Contains(r.Error, "syntax error: unexpected ") {
				t.Errorf("%s: Loki answers %q, not a syntax error", r.Q, r.Error)
			}
		}
	}
	if checked < 600 {
		t.Fatalf("only %d rows checked", checked)
	}
}

// carrierKind is the kind of the grid carrier named name.
func carrierKind(name string) string {
	for _, c := range errorOrderCarriers {
		if c.name == name {
			return c.kind
		}
	}
	return ""
}
