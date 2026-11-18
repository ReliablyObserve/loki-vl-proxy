package logql

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// nestLabelReplace wraps q in depth label_replace calls, each with tail as its four string arguments.
func nestLabelReplace(q, tail string, depth int) string {
	var b strings.Builder
	b.Grow(depth*(len("label_replace(")+len(tail)) + len(q))
	for i := 0; i < depth; i++ {
		b.WriteString("label_replace(")
	}
	b.WriteString(q)
	for i := 0; i < depth; i++ {
		b.WriteString(tail)
	}
	return b.String()
}

const labelReplaceTail = `, "dst", "$1", "src", "(.*)")`

// nestedCanonicalBases are innermost expressions for the nested canonicalization rows: alternative log range forms
// (rewritten), the plain form (kept), keyword spellings, and a binary operation and an aggregation around them.
var nestedCanonicalBases = []string{
	`sum(rate(({app="api"} |= "x")[1m]))`,
	`rate(({app="api"} | json)[5m] offset 1m)`,
	`sum by (app) (count_over_time(({app="api"} |= "x" | logfmt)[1m]))`,
	`sum(rate({app="api"}[1m]))`,
	`SUM(RATE(({app="api"} |= "x")[1m]))`,
	`sum(rate(({app="api"})[1m])) / sum(rate(({app="web"} != "y")[1m]))`,
	`label_replace(rate(({app="api"} |= "x")[1m]), "a", "$1", "b", "(.*)") + vector(1)`,
	`topk(3, sum by (pod) (bytes_over_time(({app="api"} |~ "a|b")[5m])))`,
	`sum(rate(({app="api"} |= "x")[1m])) # a comment`,
}

func nestedCanonicalRows() []string {
	var rows []string
	for _, base := range nestedCanonicalBases {
		for _, depth := range []int{0, 1, 2, 3, 5, 8, 12} {
			rows = append(rows, nestLabelReplace(base, labelReplaceTail, depth))
		}
	}
	// Calls side by side, nested in each other, and a call whose first argument holds another call's text.
	rows = append(rows,
		`label_replace(label_replace(rate(({a="b"} |= "x")[1m]), "a", "", "a", ""), "b", "", "b", "") + label_replace(rate(({a="c"})[1m]), "c", "", "c", "")`,
		`sum(label_replace(sum(label_replace(rate(({a="b"})[1m]), "a", "", "a", "")), "b", "", "b", ""))`,
		`label_replace(vector(1), "a", "", "a", "")`,
		`label_replace(rate(({a="b"} |= "label_replace(")[1m]), "a", "", "a", "")`,
	)
	return rows
}

// nestedCanonicalGolden holds CanonicalizeLogRanges' answers for nestedCanonicalRows, recorded before the
// canonicalization read each call's first argument once instead of twice.
const nestedCanonicalGolden = "testdata/canonicalize_nested_golden.json"

// Canonicalizing nested calls keeps its answers: every row writes the recorded text.
//
// conformance: semantics/nested-calls-linear-work
func TestCanonicalizeLogRangesNestedGolden(t *testing.T) {
	rows := nestedCanonicalRows()
	got := make(map[string]string, len(rows))
	for _, q := range rows {
		got[q] = CanonicalizeLogRanges(q)
	}
	path := filepath.FromSlash(nestedCanonicalGolden)
	if os.Getenv("LVP_CANONICAL_NESTED_GOLDEN_UPDATE") == "1" {
		b, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]string
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(got) {
		t.Fatalf("golden holds %d rows, the test %d", len(want), len(got))
	}
	for q, w := range want {
		if got[q] != w {
			t.Errorf("CanonicalizeLogRanges(%q)\n got %q\nwant %q", q, got[q], w)
		}
	}
}

// Canonicalizing nested calls reads each call's first argument once: the allocations grow linearly with the depth
// (reading it twice doubled the work with each level), and a thousand levels of the shortest call canonicalize
// within a generous bound.
//
// conformance: semantics/nested-calls-linear-work
func TestCanonicalizeLogRangesNestedDepthCost(t *testing.T) {
	base := `rate(({app="api"} |= "x")[1m])`
	const shortTail = `,"a","","a","")`
	allocs := func(depth int) float64 {
		q := nestLabelReplace(base, shortTail, depth)
		return testing.AllocsPerRun(3, func() { CanonicalizeLogRanges(q) })
	}
	for _, depth := range []int{16, 64, 256} {
		one, two := allocs(depth), allocs(2*depth)
		if two > 2.5*one {
			t.Errorf("depth %d: %.0f allocations, depth %d: %.0f: more than linear", depth, one, 2*depth, two)
		}
	}
	// A comment, a character literal and a line break between tokens keep the work linear.
	for _, tail := range []string{"\n# a comment", " + vector('a')", "\n"} {
		q := nestLabelReplace(base, shortTail, 2000) + tail
		start := time.Now()
		CanonicalizeLogRanges(q)
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("depth 2000 with %q: canonicalized in %v", tail, elapsed)
		}
	}
	const depth = 1000
	q := nestLabelReplace(base, shortTail, depth)
	start := time.Now()
	out := CanonicalizeLogRanges(q)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("depth %d (%d bytes): canonicalized in %v", depth, len(q), elapsed)
	}
	if want := nestLabelReplace(`rate({app="api"} |= "x" [1m])`, shortTail, depth); out != want {
		t.Errorf("depth %d: the innermost range is not written in the plain form", depth)
	}
}

// Canonicalizing a call writes its first argument as canonicalizing that argument alone does: wrapping any
// expression in label_replace calls changes nothing but the wrapping.
func FuzzCanonicalizeNestedLabelReplace(f *testing.F) {
	for _, base := range nestedCanonicalBases {
		f.Add(base, uint8(3))
	}
	f.Add(`rate(({a="b"} |= "x")[1m] offset 5m)`, uint8(9))
	f.Add(`sum by (a) (rate(({a="b"} | json | x="y")[1m]))`, uint8(1))
	f.Fuzz(func(t *testing.T, q string, depth uint8) {
		d := int(depth % 16)
		if len(q) > 512 || FirstArgumentLen(q) != len(q) || !balancedOutsideStrings(q) || strings.ContainsAny(q, "#\x00") {
			// The first argument would end early, or a comment or NUL would run into the wrapping.
			return
		}
		inner := CanonicalizeLogRanges(q)
		got := CanonicalizeLogRanges(nestLabelReplace(q, labelReplaceTail, d))
		if want := nestLabelReplace(inner, labelReplaceTail, d); got != want {
			t.Fatalf("depth %d: CanonicalizeLogRanges(wrapped %q)\n got %q\nwant %q", d, q, got, want)
		}
	})
}

// balancedOutsideStrings reports whether q's brackets outside strings balance, so FirstArgumentLen and the
// parser's parenthesis count end a call at the same place.
func balancedOutsideStrings(q string) bool {
	depth := 0
	var quote byte
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			if depth == 0 {
				return false
			}
			depth--
		}
	}
	return depth == 0 && quote == 0
}

// nestedDepthShape is a query nested to a depth: the shortest label_replace call around an alternative log
// range, a longer call around the plain form, and calls whose arguments Loki refuses at the innermost level.
type nestedDepthShape struct {
	name, base, tail string
}

func (s nestedDepthShape) query(depth int) string { return nestLabelReplace(s.base, s.tail, depth) }

var nestedDepthShapes = []nestedDepthShape{
	{"short-alternative", `rate(({app="api"} |= "x")[1m])`, `,"a","","a","")`},
	{"long-plain", `sum by (app) (rate({app="api"}[1m]))`, labelReplaceTail},
	{"bad-regex-innermost", `label_replace(rate({app="api"}[1m]),"a","","a","(")`, `,"a","","a","")`},
	{"three-arguments-innermost", `label_replace(rate({app="api"}[1m]),"a","","a")`, `,"a","","a","")`},
}

// nestedDepthSweep is the depths a shape is recorded at: powers of two up to the deepest nesting whose query
// stays under Loki's input size limit.
func nestedDepthSweep(s nestedDepthShape) []int {
	deepest := (MaxInputSize - 1 - len(s.base)) / (len("label_replace(") + len(s.tail))
	var depths []int
	for d := 1; d < deepest; d *= 2 {
		depths = append(depths, d)
	}
	return append(depths, deepest)
}

// nestedCallsLokiVerdicts are Loki's answers for the nested rows and the depth sweep, recorded with the Loki
// oracle (test/oracle/loki check): Loki accepts nesting at every depth under its input size limit, so the proxy
// refuses no query for its depth.
const nestedCallsLokiVerdicts = "testdata/nested_calls_loki_verdicts.json"

type nestedVerdict struct {
	Query  string `json:"query,omitempty"`
	Shape  string `json:"shape,omitempty"`
	Depth  int    `json:"depth,omitempty"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// nestedValidationMaxDepth is the deepest sweep row validated here: validating deeper nesting is checked where its
// cost is.
const nestedValidationMaxDepth = 64

// recordNestedCallsLokiVerdicts runs Loki's parser (the oracle's check command) over the nested rows and the depth
// sweep and writes its verdicts (LVP_RUN_ORACLES=1).
func recordNestedCallsLokiVerdicts(t *testing.T) {
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
	var inputs []nestedVerdict
	for _, q := range nestedCanonicalRows() {
		inputs = append(inputs, nestedVerdict{Query: q})
	}
	for _, s := range nestedDepthShapes {
		for _, d := range nestedDepthSweep(s) {
			inputs = append(inputs, nestedVerdict{Shape: s.name, Depth: d})
		}
	}
	shapes := map[string]nestedDepthShape{}
	for _, s := range nestedDepthShapes {
		shapes[s.name] = s
	}
	for i, v := range inputs {
		q := v.Query
		if v.Shape != "" {
			q = shapes[v.Shape].query(v.Depth)
		}
		if err := enc.Encode(map[string]any{"id": fmt.Sprint(i), "kind": "metric", "q": q}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Loki   string          `json:"loki"`
		Rows   []nestedVerdict `json:"rows"`
		Shapes []nestedVerdict `json:"shapes"`
	}
	rec.Loki = nestedCallsLokiVersion
	for i, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var v struct {
			Accept bool   `json:"accept"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatal(err)
		}
		in := inputs[i]
		in.Accept, in.Error = v.Accept, v.Error
		if in.Shape == "" {
			rec.Rows = append(rec.Rows, in)
		} else {
			rec.Shapes = append(rec.Shapes, in)
		}
	}
	sort.Slice(rec.Rows, func(i, j int) bool { return rec.Rows[i].Query < rec.Rows[j].Query })
	sort.Slice(rec.Shapes, func(i, j int) bool {
		a, b := rec.Shapes[i], rec.Shapes[j]
		return a.Shape < b.Shape || a.Shape == b.Shape && a.Depth < b.Depth
	})
	data, err := json.MarshalIndent(rec, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.FromSlash(nestedCallsLokiVerdicts), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// nestedCallsLokiVersion names the Loki the verdicts were recorded with (test/oracle/loki's pin).
const nestedCallsLokiVersion = "v3.7.9 parser, test/oracle/loki check"

// The proxy accepts and refuses nested calls as Loki does, with Loki's error text, at every recorded depth. Loki's
// verdicts are recorded with the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/nested-calls-linear-work
func TestNestedCallsMatchRecordedLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordNestedCallsLokiVerdicts(t)
	}
	b, err := os.ReadFile(filepath.FromSlash(nestedCallsLokiVerdicts))
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Rows   []nestedVerdict `json:"rows"`
		Shapes []nestedVerdict `json:"shapes"`
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	shapes := map[string]nestedDepthShape{}
	for _, s := range nestedDepthShapes {
		shapes[s.name] = s
	}
	if len(rec.Rows) != len(nestedCanonicalRows()) || len(rec.Shapes) == 0 {
		t.Fatalf("%d recorded rows, %d in the test", len(rec.Rows), len(nestedCanonicalRows()))
	}
	check := func(q string, v nestedVerdict) {
		msg := ValidateLogQL(q)
		if (msg == "") != v.Accept {
			t.Errorf("%.120q: proxy %q, Loki accept=%v %q", q, msg, v.Accept, v.Error)
		}
		if !v.Accept && msg != v.Error {
			t.Errorf("%.120q: proxy %q, Loki %q", q, msg, v.Error)
		}
	}
	for _, v := range rec.Rows {
		check(v.Query, v)
	}
	for _, v := range rec.Shapes {
		s, ok := shapes[v.Shape]
		if !ok {
			t.Fatalf("unknown shape %q", v.Shape)
		}
		if v.Depth <= nestedValidationMaxDepth {
			check(s.query(v.Depth), v)
		}
	}
}

// BenchmarkCanonicalizeLogRanges is the canonicalization of queries a dashboard sends (one call around an
// alternative range, the plain form, two levels) and of nested calls.
func BenchmarkCanonicalizeLogRanges(b *testing.B) {
	cases := []struct{ name, q string }{
		{"plain", `sum by (app) (rate({app="api"}[1m]))`},
		{"alternative", `sum by (app) (rate(({app="api"} |= "x")[1m]))`},
		{"label_replace-1", nestLabelReplace(`sum by (app) (rate(({app="api"} |= "x")[1m]))`, labelReplaceTail, 1)},
		{"label_replace-2", nestLabelReplace(`sum by (app) (rate(({app="api"} |= "x")[1m]))`, labelReplaceTail, 2)},
		{"label_replace-plain-1", nestLabelReplace(`sum by (app) (rate({app="api"}[1m]))`, labelReplaceTail, 1)},
		{"nested-8", nestLabelReplace(`rate(({app="api"} |= "x")[1m])`, labelReplaceTail, 8)},
		{"nested-16", nestLabelReplace(`rate(({app="api"} |= "x")[1m])`, labelReplaceTail, 16)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CanonicalizeLogRanges(c.q)
			}
		})
	}
}

// Reading a call's first argument in place writes what canonicalizing the argument on its own writes, for any
// query: the two ways are compared on the nested rows and the fuzz corpus.
func FuzzCanonicalizeInPlaceMatchesPerArgument(f *testing.F) {
	for _, q := range nestedCanonicalRows() {
		f.Add(q)
	}
	for _, q := range terminationSeeds() {
		f.Add(q)
	}
	f.Add(`label_replace(rate(({a="b"} |= "x")[1m]) # c, "a", "", "a", "")` + "\n" + `, "b", "", "b", "")`)
	f.Add(`label_replace(rate(({a="b"} |= 'x')[1m]), "a", "", "a", "")`)
	f.Add(`approx_topk(2, rate(({a="b"})[1m]))`)
	f.Add(nestLabelReplace(`rate(({a="b"} |= "x")[1m])`, `,"a","","a","")`, 3) + "\n# a comment")
	f.Add(nestLabelReplace(`rate(({a="b"} |= "x")[1m])`, `,"a","","a","")`, 3) + "\n# a (comment, \"x\"")
	f.Add(`label_replace(rate(({a="b"} |= "x")[1m]) # c` + "\n" + `, "a", "", "a", "")`)
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 2048 || strings.Count(q, "(") > 64 {
			return // the per-argument way costs the square of the depth or more
		}
		if got, want := CanonicalizeLogRanges(q), canonicalizeLogRanges(q, true); got != want {
			t.Fatalf("CanonicalizeLogRanges(%q)\n in place     %q\n per argument %q", q, got, want)
		}
	})
}

// The nested rows canonicalize in place as on their own.
//
// conformance: semantics/nested-calls-linear-work
func TestCanonicalizeInPlaceMatchesPerArgument(t *testing.T) {
	for _, q := range append(nestedCanonicalRows(), terminationSeeds()...) {
		if got, want := CanonicalizeLogRanges(q), canonicalizeLogRanges(q, true); got != want {
			t.Errorf("CanonicalizeLogRanges(%q)\n in place     %q\n per argument %q", q, got, want)
		}
	}
}

// plainArgumentText is true only for text whose strings and brackets FirstArgumentLen reads as the parser does.
//
// conformance: semantics/nested-calls-linear-work
func TestPlainArgumentText(t *testing.T) {
	for _, c := range []struct {
		q     string
		plain bool
	}{
		{`label_replace(rate({a="b"}[1m]), "a", "", "a", "")`, true},
		{"rate({a=\"b\"}[1m])\n# a comment", true},
		{"rate({a=\"b\"}[1m])\n# a (comment", false},
		{"rate({a=\"b\"}[1m])\n# a comment, \"quoted\"", false},
		{`rate({a="b"}[1m]) + vector('a')`, true},
		{`rate({a="b"}[1m]) + vector('(')`, false},
		{`rate({a="b"}[1m]) + vector('a)`, false},
		{"rate({a=\"b\"}[1m]) + vector('a\n')", false},
		{"rate({a=\"b\nc\"}[1m])", false},
		{"rate({a=`b\nc`}[1m])", true},
		{`rate({a="b\"("}[1m])`, true},
		{`rate({a="b"}[1m]])`, false},
		{`rate({a="b")[1m]}`, false},
		{`rate({a="b"}[1m]`, false},
		{`rate({a="b`, false},
	} {
		if got := plainArgumentText(c.q); got != c.plain {
			t.Errorf("plainArgumentText(%q) = %v, want %v", c.q, got, c.plain)
		}
	}
}

// A query whose text Loki's lexer refuses is canonicalized the way before (each argument on its own).
//
// conformance: semantics/nested-calls-linear-work
func TestCanonicalizeLexerErrorReadsArgumentsOnTheirOwn(t *testing.T) {
	for _, q := range []string{
		"label_replace(rate(({a=\"b\"} |= \"x\x00\")[1m]), \"a\", \"\", \"a\", \"\")",
		"label_replace(rate(({a=\"b\"} |= \"x\")[1m]), \"a\", \"\", \"a\", \"\") > 1e",
	} {
		if got, want := CanonicalizeLogRanges(q), canonicalizeLogRanges(q, true); got != want {
			t.Errorf("CanonicalizeLogRanges(%q)\n in place     %q\n per argument %q", q, got, want)
		}
	}
}
