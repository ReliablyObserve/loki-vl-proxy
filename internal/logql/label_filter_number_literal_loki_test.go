package logql

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// numLitAnswers holds what Loki's parser answers for every query of numLitQueries.
const numLitAnswers = "testdata/label_filter_number_literals_loki.jsonl.gz"

// numLitAnswer is one recorded answer of the Loki oracle.
type numLitAnswer struct {
	Q      string `json:"q"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// numLitValues is every value text the grid writes after a comparison operator of a label filter: the prefixes
// of number literals (0x 0X 0o 0O 0b 0B and a leading 0) with valid and invalid digits and underscores, exponents
// (decimal and hexadecimal floats), decimals, signs, every bytes unit in both cases, every duration unit and
// compound, trailing junk and whitespace of every kind, and control characters inside a literal.
func numLitValues() []string {
	seen := map[string]bool{}
	var out []string
	add := func(vs ...string) {
		for _, v := range vs {
			if !seen[v] {
				seen[v] = true
				out = append(out, v)
			}
		}
	}
	for _, prefix := range []string{"0x", "0X", "0o", "0O", "0b", "0B"} {
		for _, body := range []string{"", "0", "1", "7", "8", "9", "a", "F", "g", "z", "_", "_1", "1_", "1__1", "1_1", "01", "1.", "1.1", ".1", "1e3", "1p3", "1m", "1s", "1b", "1B", "1kb", "1KB"} {
			add(prefix + body)
		}
	}
	add("0", "00", "01", "05", "07", "08", "09", "007", "0_1", "0__1", "0_", "1_0", "1__0", "_1", "1_", "_", "1_000", "1_000_000", "0_8", "1_.5", "1._5", "1.5_", "1_e3", "1e_3", "1e3_")
	for _, e := range []string{"e", "E", "p", "P"} {
		for _, rest := range []string{"", "+", "-", "3", "+3", "-3", "03", "+_3", "_3", "3_", "3.5", "3e3", "e3", "+-3", "3m", "3B", "3kb"} {
			if (rest[:min(1, len(rest))] == "+" || rest[:min(1, len(rest))] == "-") && (e == "e" || e == "E") {
				// 0x1e+3 is the hexadecimal number 0x1e plus 3: a binary operation on the query, which the proxy
				// reads on its own (and Loki rejects the query for), not a literal.
				add("1"+e+rest, "0"+e+rest, "1.5"+e+rest, "1."+e+rest, "."+e+rest, "0b1"+e+rest, "0o7"+e+rest)
				continue
			}
			add("1"+e+rest, "0"+e+rest, "1.5"+e+rest, "1."+e+rest, "."+e+rest, "0x1"+e+rest, "0x1.8"+e+rest, "0x"+e+rest, "0x.8"+e+rest, "0b1"+e+rest, "0o7"+e+rest)
		}
	}
	add(".5", "5.", "0.5", "00.5", "1.2.3", "..5", "5..", ".", "..", ".e", "0.", ".0", "1.5.", "-.5", "+.5", "-5.", "1_000.5", "0x1.8", "0x.8", "0b1.1", "0o7.7")
	add("-1", "+1", "--1", "+-1", "-+1", "-", "+", "-0x1", "+0b1", "-0o7", "-1e3", "-5m", "+5B", "-0x", "-1e", "-0b", "- 1", "+ 1", "-.", "-_1", "-1_")
	units := []string{"B", "kB", "KB", "KiB", "MB", "MiB", "GB", "GiB", "TB", "TiB", "PB", "PiB", "EB", "EiB", "k", "K", "m", "M", "g", "G", "t", "T", "p", "P", "e", "E", "kib", "mib", "gib", "tib", "pib", "eib", "Kb", "kIB", "b", "bB", "Bb", "ZB", "YB", "zb", "yb", "KBB", "Ki", "Mi", "iB"}
	for _, u := range units {
		for _, n := range []string{"0", "1", "1.5", "5.", ".5", "0x1", "1e3", "1_0"} {
			add(n+u, n+strings.ToLower(u), n+strings.ToUpper(u))
		}
	}
	for _, u := range []string{"ns", "us", "µs", "μs", "ms", "s", "m", "h", "d", "w", "y", "NS", "US", "MS", "S", "M", "H", "D", "W", "Y", "Ns", "mS", "sec", "min", "hr", "q", "n", "u", "µ"} {
		for _, n := range []string{"0", "1", "1.5", "5.", ".5", "1_0", "0x1", "1e3"} {
			add(n + u)
		}
	}
	add("1h30m", "1h 30m", "1m1h", "1h30", "30m1", "1.5h30m", "1h1h", "1h30m15s", "1h30mx", "1h30m5B", "5m5B", "5B5m", "1h.5m", "1h5.m", "1h-5m", "1d12h", "1w1d", "1y1w1d1h1m1s1ms1us1ns", "1s1", "1m1_0s", "1H30M", "2h45m30s500ms")
	add("5x", "5Bx", "5mx", "5smx", "5kBx", "5B5", "5kB5", "5 B", "5 m", "5_B", "5_m", "5B_", "5m_", "5Bi", "5é", "5B)", "5m)", "0B)", "0x)", "1e)")
	for _, base := range []string{"5", "5m", "5B", "0B", "0x1", "0x", "1e3", "1e", "5x", "0", "0.5", "0o7", "0b1"} {
		for _, c := range []string{"\v", "\f", "\r", "\n", "\t", " ", "\u00a0", "\u0085", "\u2003", "\x00", "\x01", "\x1f", "\x7f", "\u200b", "\ufeff"} {
			add(base+c, base+c+c)
		}
	}
	add("\v5", "\f5", "\u00a05", "5\v5", "5\v5m", "5\v5B", "5m\v5m", "0\vB", "0\v", "0B\v", "0b\v", "0B\v1", "0x\v", "0x\v1", "0x1\v", "1e\v", "1e\v3", "1\ve3", "5\f", "5m\f", "5B\f", "5B\f ", "5B \v")
	sort.Strings(out)
	return out
}

// numLitSpacings are the texts that can sit between a comparison operator and its value: Loki reads a 0b or 0B
// literal after white space unlike one that follows the operator.
var numLitSpacings = []string{"", " ", "  ", "\t", "\n", "\r", " \n", "#c\n", "#c\n "}

// numLitQueries is the grid: every value after every comparison operator alone, and a subset of the values inside
// every combination of two filters, in parentheses and followed by another stage.
func numLitQueries() []string {
	ops := []string{"=", "==", "!=", ">", ">=", "<", "<="}
	values := numLitValues()
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	for _, v := range values {
		for _, op := range ops {
			add(`{app="a"} | A` + op + v)
		}
	}
	for _, v := range values {
		for _, sp := range numLitSpacings[1:] {
			if strings.HasPrefix(v, "0") || len(v)%5 == 0 {
				add(`{app="a"} | A<` + sp + v)
				add(`{app="a"} | A =` + sp + v)
			}
		}
	}
	subset := []string{"5", "05", "0x1", "0x", "0X", "0B", "0b", "0B\v", "0b1", "0B1", "1e", "1e3", "1e+", "5m", "5B", "1b", "0kb", "0KB", "1h30m", "5x", "5Bx", "1__0", "_1", "1_", "0x_1", ".5", "5.", "-1", "+1", "5\v", "5m\f", "5B\r", "5 ", "0o7", "1_0"}
	for _, v := range subset {
		for _, op := range []string{"<", "=", "!="} {
			f := "A" + op + v
			g := "B>1"
			for _, shape := range []string{
				f + " and " + g,
				g + " and " + f,
				f + " or " + g,
				g + " or " + f,
				f + ", " + g,
				g + ", " + f,
				"(" + f + ")",
				"(" + f + " and " + g + ") or C=1",
				f + " " + g,
			} {
				add(`{app="a"} | ` + shape)
				add(`{app="a"} | json | ` + shape + ` | line_format "x"`)
			}
		}
	}
	for _, q := range lokiLexerQueries() {
		add(q)
	}
	return out
}

// recordNumLitAnswers runs Loki's parser (the oracle's check command) over the queries and writes its answers.
func recordNumLitAnswers(t *testing.T, queries []string) {
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
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	var zipped bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&zipped, gzip.BestCompression)
	if _, err := zw.Write([]byte(b.String())); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(numLitAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Label filter number, duration and bytes literals are read as Loki reads them: the proxy accepts and rejects
// exactly what Loki's parser accepts and rejects, with Loki's error text. Loki's answers are recorded with the
// Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/label-filter-number-literal-scan
func TestLabelFilterNumberLiteralsMatchLoki(t *testing.T) {
	queries := numLitQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordNumLitAnswers(t, queries)
	}
	f, err := os.Open(numLitAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []numLitAnswer
	sc := bufio.NewScanner(zr)
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
	var accepted, rejected, diffs int
	for i, a := range answers {
		if a.Q != queries[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, queries[i])
		}
		got := ValidateLogQL(a.Q)
		if a.Accept {
			accepted++
		} else {
			rejected++
		}
		if (got == "") != a.Accept || !a.Accept && got != a.Error {
			diffs++
			if diffs <= 40 {
				t.Errorf("%q\n  proxy: %q\n  Loki:  accept=%v %q", a.Q, got, a.Accept, a.Error)
			}
		}
	}
	t.Logf("%d queries: Loki accepts %d and rejects %d; %d differ", len(answers), accepted, rejected, diffs)
	if diffs > 40 {
		t.Errorf("%d queries differ from Loki", diffs)
	}
	if accepted < 500 || rejected < 500 {
		t.Fatalf("grid too small: %d accepted, %d rejected", accepted, rejected)
	}
}

// The value and kind a label filter reads from an accepted literal, and the stage that starts with a dotted name
// (the first word of the text is not cut at its dots).
//
// conformance: semantics/label-filter-number-literal-scan
func TestLabelFilterLiteralKinds(t *testing.T) {
	for _, tc := range []struct {
		text, value string
		kind        LabelFilterValueKind
	}{
		{"a<0x1", "0x1", LabelFilterValueNumber},
		{"a<0b1", "0b1", LabelFilterValueNumber},
		{"a<1_000", "1_000", LabelFilterValueNumber},
		{"a<1e3", "1e3", LabelFilterValueNumber},
		{"a<.5", ".5", LabelFilterValueNumber},
		{"a<-5", "-5", LabelFilterValueNumber},
		{"a< - 5", "-5", LabelFilterValueNumber},
		{"a<+0x1p3", "+0x1p3", LabelFilterValueNumber},
		{"a<5m", "5m", LabelFilterValueDuration},
		{"a<-5m", "-5m", LabelFilterValueDuration},
		{"a<1d12h", "1d12h", LabelFilterValueDuration},
		{"a<1h30m", "1h30m", LabelFilterValueDuration},
		{"a<5\u00b5s", "5\u00b5s", LabelFilterValueDuration},
		{"a<5k", "5k", LabelFilterValueBytes},
		{"a<5KiB", "5KiB", LabelFilterValueBytes},
		{"a>=1.5GB", "1.5GB", LabelFilterValueBytes},
		{"k8s.namespace.name=`x`", "x", LabelFilterValueString},
	} {
		expr, err := ParseLabelFilterExpr(tc.text)
		if err != nil {
			t.Errorf("%q: %v", tc.text, err)
			continue
		}
		if expr.Value != tc.value || expr.Kind != tc.kind {
			t.Errorf("%q: value %q kind %v, want %q %v", tc.text, expr.Value, expr.Kind, tc.value, tc.kind)
		}
	}
	// The input that made the label filter fuzz target fail: the control character is not part of the number, and
	// the rejected literal never renders as a filter that no longer parses.
	for _, text := range []string{"A<0B\v", "a<0x", "a<1e", "a<1b", "a<0kb", "a<5x", "a<1h30", "a<1__0", "a<5\v", "a<5 \v", "\x99=0B\f#0", "\x99=0B\f", "\x99=0B#0"} {
		if _, err := ParseLabelFilterExpr(text); err == nil {
			t.Errorf("%q is accepted", text)
		}
	}
}

// A '[' right after a literal is a word of its own that the lexer must step over, never an empty word.
func TestLabelFilterBracketAfterLiteralTerminates(t *testing.T) {
	for _, text := range []string{"a<5[", "a<5[1m]", "a<5m[", "a<[", "[", "a<5 ["} {
		if _, err := ParseLabelFilterExpr(text); err == nil {
			t.Errorf("%q is accepted", text)
		}
	}
}
