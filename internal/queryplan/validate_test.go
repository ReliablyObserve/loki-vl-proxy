package queryplan

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// validateAnswer is the handlers' validation of a query (proxy.validateQueryTree): the parser and its semantic pass,
// then the tree checks. "" is an accepted query.
func validateAnswer(q string) string {
	expr, msg := logql.ValidateLogQLExpr(q)
	if msg != "" || expr == nil {
		return msg
	}
	if err := Validate(expr); err != nil {
		return err.Error()
	}
	return ""
}

const (
	sortGrouping  = "parse error : sort and sort_desc doesn't allow grouping by "
	quantileParam = "parse error : parameter required for operation quantile_over_time"
	labelReplace  = "parse error : syntax error: unexpected ,"
)

func setLiteral(leg, op, value string) string {
	return "parse error : unexpected literal for " + leg + " leg of logical/set binary operation (" + op + "): " + value
}

// validateCases are the tree checks, each with the neighbours Loki accepts: the answer is the handlers' whole
// validation, and every query is in the grid whose Loki answers are recorded (TestValidateMatchesLoki).
var validateCases = []struct {
	q, want string
}{
	// sort and sort_desc refuse labels to group by, wherever they stand; an empty list is no grouping.
	{`sort by (a) (rate({a="b"}[5m]))`, sortGrouping},
	{`sort_desc by (a) (rate({a="b"}[5m]))`, sortGrouping},
	{`sort without (a) (rate({a="b"}[5m]))`, sortGrouping},
	{`sort (rate({a="b"}[5m])) by (a)`, sortGrouping},
	{`sort(vector(1)) by (a)`, sortGrouping},
	{`sort by (a) (vector(1))`, sortGrouping},
	{`sort by (a) (1)`, sortGrouping},
	{`sort by (a) (rate({a="b"}[5m]) + 1)`, sortGrouping},
	{`sort_desc by (a) (sort by (b) (rate({a="b"}[5m])))`, sortGrouping},
	{`sum by (a) (sort by (b) (rate({a="b"}[5m])))`, sortGrouping},
	{`label_replace(sort by (a) (rate({a="b"}[5m])), "d", "$1", "a", "(.*)")`, sortGrouping},
	{`sort by (a) (label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)"))`, sortGrouping},
	{`sort by (a) (quantile_over_time({a="b"} | unwrap c [5m]))`, sortGrouping},
	{`sort_desc without () (rate({a="b"}[5m]))`, ""},
	{`sort by () (rate({a="b"}[5m]))`, ""},
	{`sort(rate({a="b"}[5m]))`, ""},
	{`sort_desc(sum by (a) (rate({a="b"}[5m])))`, ""},
	{`sum by (a) (rate({a="b"}[5m]))`, ""},
	{`topk by (a) (2, rate({a="b"}[5m]))`, ""},

	// quantile_over_time needs its quantile; any value is accepted (above 1 too).
	{`quantile_over_time({a="b"} | unwrap c [5m])`, quantileParam},
	{`quantile_over_time({a="b"} | json | unwrap c [5m]) by (a)`, quantileParam},
	{`rate({a="b"}[5m]) and quantile_over_time({a="b"} | unwrap c [5m])`, quantileParam},
	{`rate({a="b"}[5m]) and 1 + quantile_over_time({a="b"} | unwrap c [5m])`, quantileParam},
	{`quantile_over_time({a="b"} | unwrap c [5m]) + sort by (a) (rate({a="b"}[5m]))`, quantileParam},
	{`sort by (a) (rate({a="b"}[5m])) + quantile_over_time({a="b"} | unwrap c [5m])`, sortGrouping},
	{`quantile_over_time(0.5, {a="b"} | unwrap c [5m])`, ""},
	{`quantile_over_time(1.5, {a="b"} | unwrap c [5m])`, ""},
	{`quantile_over_time(0.5, {a="b"} | unwrap c [5m]) by (a)`, ""},
	{`avg_over_time({a="b"} | unwrap c [5m])`, ""},

	// A logical/set operation refuses an operand that folds to a number, left leg first, before its operands' own
	// checks; vector() is no number.
	{`rate({a="b"}[5m]) and 1 + 2`, setLiteral("right", "and", "3.000000")},
	{`1 + 2 and rate({a="b"}[5m])`, setLiteral("left", "and", "3.000000")},
	{`rate({a="b"}[5m]) or (1 + 2) * 3`, setLiteral("right", "or", "9.000000")},
	{`(1 + 2) unless rate({a="b"}[5m])`, setLiteral("left", "unless", "3.000000")},
	{`1 * 2 or sum(rate({a="b"}[5m]))`, setLiteral("left", "or", "2.000000")},
	{`vector(1) and 1 + 2`, setLiteral("right", "and", "3.000000")},
	{`rate({a="b"}[5m]) and (1 + 2)`, setLiteral("right", "and", "3.000000")},
	{`rate({a="b"}[5m]) and on (a) 1 + 2`, setLiteral("right", "and", "3.000000")},
	{`(rate({a="b"}[5m])) and ignoring () 1 + 2`, setLiteral("right", "and", "3.000000")},
	{`sum(rate({a="b"}[5m])) and 1 * 2`, setLiteral("right", "and", "2.000000")},
	{`1 + 2 and 3 + 4`, setLiteral("left", "and", "3.000000")},
	{`rate({a="b"}[5m]) and 1 + on (a) 2`, setLiteral("right", "and", "3.000000")},
	{`rate({a="b"}[5m]) and 1 + ignoring (a) 2`, setLiteral("right", "and", "3.000000")},
	{`rate({a="b"}[5m]) and 1 * on (a) group_left 2`, setLiteral("right", "and", "2.000000")},
	{`rate({a="b"}[5m]) and 1 == bool 1`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and 1 > 2`, setLiteral("right", "and", "0.000000")},
	{`rate({a="b"}[5m]) and 3 > 2`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and 2 > 3`, setLiteral("right", "and", "0.000000")},
	{`rate({a="b"}[5m]) and 3 > bool 2`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and 1 != 1`, setLiteral("right", "and", "0.000000")},
	{`rate({a="b"}[5m]) and 2 >= 2`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and 2 <= 1`, setLiteral("right", "and", "0.000000")},
	{`rate({a="b"}[5m]) and 1 < 2`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and 1 == 1`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and 1 / 0`, setLiteral("right", "and", "NaN")},
	{`rate({a="b"}[5m]) and -1 / 0`, setLiteral("right", "and", "NaN")},
	{`rate({a="b"}[5m]) and 0 / 0`, setLiteral("right", "and", "NaN")},
	{`rate({a="b"}[5m]) and 5 % 3`, setLiteral("right", "and", "2.000000")},
	{`rate({a="b"}[5m]) and 5 % 0`, setLiteral("right", "and", "NaN")},
	{`rate({a="b"}[5m]) and 2 ^ 3`, setLiteral("right", "and", "8.000000")},
	{`rate({a="b"}[5m]) and -2 ^ 2`, setLiteral("right", "and", "4.000000")},
	{`rate({a="b"}[5m]) and 2 ^ 3 ^ 2`, setLiteral("right", "and", "512.000000")},
	{`rate({a="b"}[5m]) and 1e300 * 1e300`, setLiteral("right", "and", "+Inf")},
	{`rate({a="b"}[5m]) and 0.1 + 0.2`, setLiteral("right", "and", "0.300000")},
	{`rate({a="b"}[5m]) and 1e3`, setLiteral("right", "and", "1000.000000")},
	{`rate({a="b"}[5m]) and (1 + (2 and 3))`, setLiteral("left", "and", "2.000000")},
	{`(1 + 2) + (3 and 4)`, setLiteral("left", "and", "3.000000")},
	{`(1 and 2) or (3 and 4)`, setLiteral("left", "and", "1.000000")},
	{`quantile_over_time({a="b"} | unwrap c [5m]) + 1 and 2`, setLiteral("right", "and", "2.000000")},
	{`sort by (a) (rate({a="b"}[5m])) and 1`, setLiteral("right", "and", "1.000000")},
	{`1 and sort by (a) (rate({a="b"}[5m]))`, setLiteral("left", "and", "1.000000")},
	{`quantile_over_time({a="b"} | unwrap c [5m]) and 1`, setLiteral("right", "and", "1.000000")},
	{`label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)") and 1`, setLiteral("right", "and", "1.000000")},
	{`rate({a="b"}[5m]) and vector(1)`, ""},
	{`rate({a="b"}[5m]) and (vector(1) + 2)`, ""},
	{`rate({a="b"}[5m]) and rate({a="b"}[1m])`, ""},
	{`rate({a="b"}[5m]) + 1 + 2`, ""},
	{`1 + 2`, ""},
	{`1 > 2`, ""},
	{`rate({a="b"}[5m]) > bool rate({a="b"}[5m])`, ""},
	{`rate({a="b"}[5m]) + on (a) group_left (b) 1`, ""},
	{`vector(1) + on (a) vector(2)`, ""},

	// label_replace takes a metric expression: Loki's grammar stops at the comma after a log query (at a position
	// the tree does not hold).
	{`label_replace({a="b"}, "d", "x", "a", "(.*)")`, labelReplace},
	{`label_replace({a="b"} | json, "d", "$1", "a", ".+")`, labelReplace},
	{"label_replace({a=\"b\"} |= \"x\" | logfmt | line_format `{{.c}}`, \"d\", \"$1\", \"a\", \"(.*)\")", labelReplace},
	// A comma after a label filter joins another filter: the parser reports Loki's syntax error at the next string.
	{`label_replace({a="b"} |= "x" | logfmt | c="d", "d", "$1", "a", "(.*)")`, `parse error at line 1, col 48: syntax error: unexpected STRING, expecting IDENTIFIER or (`},
	{`sum(label_replace({a="b"}, "d", "x", "a", "(.*)"))`, labelReplace},
	{`label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)")`, ""},
	{`label_replace(1 + 1, "d", "$1", "a", "(.*)")`, ""},
	{`label_replace(vector(1), "d", "$1", "a", "(.*)")`, ""},
	{`label_replace(rate({a="b"}[5m]), "1d", "$1", "a", "(.*)")`, ""},
}

// TestValidate holds the handlers' validation to the table, with Loki's status for every rejection.
//
// conformance: semantics/sort-grouping-accepted, semantics/quantile-over-time-parameter-required, semantics/set-operation-folded-literal-operand, semantics/label-replace-over-log-selector-accepted
func TestValidate(t *testing.T) {
	for _, tc := range validateCases {
		if got := validateAnswer(tc.q); got != tc.want {
			t.Errorf("%s:\n got  %q\n want %q", tc.q, got, tc.want)
		}
		expr, msg := logql.ValidateLogQLExpr(tc.q)
		if msg != "" {
			continue
		}
		if err := Validate(expr); err != nil {
			if answer := lokierr.Parse(0, 0, err.Detail); answer.Status() != http.StatusBadRequest || answer.Text() != err.Error() {
				t.Errorf("%s: %d %q, Loki answers 400 %q", tc.q, answer.Status(), answer.Text(), err.Error())
			}
		}
	}
}

// TestValidateAllocatesNothingOnAccept: the checks run on every query the cache has not seen; a query Loki accepts
// costs no allocation.
func TestValidateAllocatesNothingOnAccept(t *testing.T) {
	for _, tc := range validateCases {
		if tc.want != "" {
			continue
		}
		expr, msg := logql.ValidateLogQLExpr(tc.q)
		if msg != "" || expr == nil {
			t.Fatalf("%s: %q", tc.q, msg)
		}
		if n := testing.AllocsPerRun(20, func() { _ = Validate(expr) }); n != 0 {
			t.Errorf("%s: %v allocations", tc.q, n)
		}
	}
}

// validateLokiAnswers are Loki's answers to the table's queries, recorded with the Loki oracle (test/oracle/loki
// check: Loki's own parser and validation); LVP_RUN_ORACLES=1 records them again.
var validateLokiAnswers = filepath.Join("testdata", "validate_loki_answers.jsonl")

type validateLokiAnswer struct {
	Query  string `json:"q"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// lokiPosition is the position Loki writes into a syntax error.
var lokiPosition = regexp.MustCompile(`^parse error at line \d+, col \d+: `)

// validateKnownOrder are queries with two faults where Loki reports the tree check and the proxy the parser's own
// check, which runs first: Loki checks each node before its operands, the parser's semantic pass knows nothing of
// the tree checks. Both reject with Loki's status.
var validateKnownOrder = map[string]bool{
	`1 + 2 and 3`:                       true, // Loki folds the left leg first; the parser sees the literal on the right
	`1 + 2 and -1`:                      true,
	`sort by (a) (rate({a=~".*"}[5m]))`: true, // the selector's empty-compatible matchers are checked by the parser first
	`quantile_over_time({a=~".*"} | unwrap c [5m])`: true,
	`quantile_over_time({a="b"} [5m])`:              true, // the parser reports the missing unwrap first
}

// TestValidateMatchesLoki holds the handlers' validation to Loki's recorded answers for every query of the table
// and the known orders: the same verdict and Loki's text, or, for label_replace over a log query, Loki's syntax
// error without its position.
//
// conformance: semantics/sort-grouping-accepted, semantics/quantile-over-time-parameter-required, semantics/set-operation-folded-literal-operand, semantics/label-replace-over-log-selector-accepted
func TestValidateMatchesLoki(t *testing.T) {
	grid := make([]string, 0, len(validateCases)+len(validateKnownOrder))
	for _, tc := range validateCases {
		grid = append(grid, tc.q)
	}
	for q := range validateKnownOrder {
		grid = append(grid, q)
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordValidateAnswers(t, grid)
	}
	answers := readValidateAnswers(t)
	for _, q := range grid {
		loki, ok := answers[q]
		if !ok {
			t.Errorf("%s: no recorded Loki answer: record them with LVP_RUN_ORACLES=1", q)
			continue
		}
		got := validateAnswer(q)
		if loki.Accept {
			if got != "" {
				t.Errorf("%s: Loki accepts, the proxy answers %q", q, got)
			}
			continue
		}
		if got == "" {
			t.Errorf("%s: Loki answers %q, the proxy accepts", q, loki.Error)
			continue
		}
		want := loki.Error
		if loc := lokiPosition.FindStringIndex(want); loc != nil && got == labelReplace {
			want = "parse error : " + want[loc[1]:]
		}
		if validateKnownOrder[q] {
			if got == want {
				t.Errorf("%s: the known order difference is gone (%q): remove it", q, got)
			}
			continue
		}
		if got != want {
			t.Errorf("%s:\n proxy %q\n Loki  %q", q, got, want)
		}
	}
}

func recordValidateAnswers(t *testing.T, grid []string) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiOracleDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	var b strings.Builder
	for i, q := range grid {
		line, _ := json.Marshal(map[string]any{"id": fmt.Sprint(i), "kind": "validate", "q": q})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	b.Reset()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var v struct {
			ID     string `json:"id"`
			Accept bool   `json:"accept"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		i, err := strconv.Atoi(v.ID)
		if err != nil || i < 0 || i >= len(grid) {
			t.Fatalf("oracle answer for an unknown id %q", v.ID)
		}
		line, _ := json.Marshal(validateLokiAnswer{Query: grid[i], Accept: v.Accept, Error: v.Error})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(validateLokiAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readValidateAnswers(t *testing.T) map[string]validateLokiAnswer {
	t.Helper()
	f, err := os.Open(validateLokiAnswers)
	if err != nil {
		t.Fatalf("%v: record the answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	out := map[string]validateLokiAnswer{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a validateLokiAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out[a.Query] = a
	}
	return out
}

// TestFoldLiteral: the numbers Loki folds an operand of a logical/set operation to, the special values included.
func TestFoldLiteral(t *testing.T) {
	for _, tc := range []struct {
		op   string
		l, r float64
		want float64
	}{
		{"+", 1, 2, 3}, {"-", 1, 2, -1}, {"*", 2, 3, 6}, {"/", 1, 2, 0.5}, {"/", 1, 0, math.NaN()}, {"/", -1, 0, math.NaN()},
		{"%", 5, 3, 2}, {"%", -5, 3, -2}, {"%", 5, 0, math.NaN()}, {"^", 2, 10, 1024}, {"==", 1, 1, 1}, {"!=", 1, 1, 0},
		{">", 3, 2, 1}, {"<", 3, 2, 0}, {">=", 2, 2, 1}, {"<=", 3, 2, 0},
	} {
		got, ok := foldOperation(tc.op, tc.l, tc.r)
		if !ok || (got != tc.want && !(math.IsNaN(got) && math.IsNaN(tc.want))) {
			t.Errorf("%v %s %v = %v, %v; want %v", tc.l, tc.op, tc.r, got, ok, tc.want)
		}
	}
	if _, ok := foldOperation("and", 1, 2); ok {
		t.Error("a set operation folds")
	}
	for value, want := range map[float64]string{math.NaN(): "NaN", math.Inf(1): "+Inf", math.Inf(-1): "-Inf", -0.5: "-0.500000"} {
		if got := setOperationLiteral("left", "or", value).Error(); got != setLiteral("left", "or", want) {
			t.Errorf("%v: %q", value, got)
		}
	}
}

// FuzzValidate: for any query the parser and its semantic pass accept, the tree checks do not panic, give the same
// answer twice, answer Loki's 400, and accept every query of the table Loki accepts.
func FuzzValidate(f *testing.F) {
	seedQueries(f)
	for _, tc := range validateCases {
		f.Add(tc.q)
	}
	accepted := map[string]bool{}
	for _, tc := range validateCases {
		if tc.want == "" {
			accepted[tc.q] = true
		}
	}
	f.Fuzz(func(t *testing.T, q string) {
		expr, msg := logql.ValidateLogQLExpr(q)
		if msg != "" || expr == nil {
			return
		}
		first := Validate(expr)
		second := Validate(expr)
		if (first == nil) != (second == nil) || (first != nil && *first != *second) {
			t.Fatalf("%q: %v, then %v", q, first, second)
		}
		if first == nil {
			return
		}
		if answer := lokierr.Parse(0, 0, first.Detail); answer.Status() != http.StatusBadRequest || first.Detail == "" || !strings.HasPrefix(answer.Text(), "parse error : ") {
			t.Fatalf("%q: %d %q", q, answer.Status(), answer.Text())
		}
		if accepted[q] {
			t.Fatalf("%q: Loki accepts it, the proxy answers %q", q, first.Error())
		}
	})
}
