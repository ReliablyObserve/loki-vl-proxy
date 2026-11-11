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
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// unicodeRuneAnswers holds what Loki's parser answers for every query of unicodeRuneQueries.
const unicodeRuneAnswers = "testdata/unicode_runes_loki.jsonl.gz"

// runeMark stands for the character under test in a runeTemplates entry.
const runeMark = "¤"

// runeSamples are the characters of the grid by Unicode category: the edges of the blocks and a few between, ASCII
// punctuation Loki's lexer returns as a name, and the format, private and unassigned characters.
var runeSamples = []struct {
	category string
	runes    []rune
}{
	{"Nd", []rune{0x660, 0x669, 0x6F3, 0x966, 0x9E6, 0xFF10, 0xFF13, 0x1D7CE, 0x1D7D1, 0x1D7FF}},
	{"No", []rune{0xB2, 0xB3, 0xB9, 0xBC, 0x2153, 0x2460, 0x2776, 0x3220, 0x1F100}},
	{"Nl", []rune{0x16EE, 0x2160, 0x2163, 0x2188, 0x3007, 0x3021}},
	{"L", []rune{0xAA, 0xB5, 0xC0, 0xDF, 0xE9, 0x1C5, 0x2B0, 0x3A9, 0x3BC, 0x416, 0x4E2D, 0x1F00, 0x10000, 0x1D538}},
	{"Mn", []rune{0x300, 0x301, 0x93C, 0x20D0, 0xFE0F}},
	{"Mc", []rune{0x903, 0x93E}},
	{"Me", []rune{0x488, 0x20DD}},
	{"Sc", []rune{0xA2, 0xA3, 0x20AC, 0x20BF}},
	{"Sm", []rune{0xB1, 0xD7, 0x2211, 0x2260, 0x1D6C1}},
	{"Sk", []rune{0xA8, 0xAF, 0x2C2, 0x1F3FB}},
	{"So", []rune{0xA9, 0xAE, 0x2603, 0x2764, 0x1F600}},
	{"Pc", []rune{0x203F, 0x2040, 0xFF3F}},
	{"Pd", []rune{0x2010, 0x2013, 0x2014, 0x30A0}},
	{"Ps Pe Pi Pf", []rune{0xAB, 0xBB, 0x2018, 0x2019, 0x300C, 0x300D}},
	{"Po", []rune{0xA1, 0xBF, 0xB7, 0x2022, 0x3001}},
	{"Zs", []rune{0xA0, 0x1680, 0x2000, 0x2003, 0x200A, 0x202F, 0x205F, 0x3000}},
	{"Zl Zp", []rune{0x2028, 0x2029}},
	{"Cc", []rune{0x1, 0x1F, 0x7F, 0x80, 0x85, 0x9F}},
	{"Cf", []rune{0xAD, 0x200B, 0x200C, 0x200D, 0x200E, 0x2060, 0xFEFF, 0xE0001}},
	{"Co", []rune{0xE000, 0xF8FF, 0xF0000}},
	{"Cn", []rune{0x378, 0xFFFE, 0x10FFFF}},
	{"replacement", []rune{0xFFFD}},
	{"ASCII", []rune{'$', '&', ';', '?', '@', '\\', '~', '^', '%', '!', ':', '\''}},
}

// runeTemplates are the queries, with runeMark in one position each.
var runeTemplates = []string{
	// a label name in a stream selector
	`{¤="a"}`, `{a¤="b"}`, `{a="b",¤="c"}`, `{¤a="b"}`, `{a="b", a¤b="c"}`, `{a="b"¤}`, `{a=¤"b"}`, `{a=~¤".*"}`,
	// a label name in a label filter and the words around it
	`{a="b"} | ¤ = "x"`, `{a="b"} | ¤`, `{a="b"} | x ¤`, `{a="b"} | x¤`, `{a="b"} | ¤x = "y"`, `{a="b"} | x¤y = "z"`, `{a="b"} | x ¤ y`,
	`{a="b"} | json | ¤ = "x"`, `{a="b"} | x = "y" and ¤ = "z"`, `{a="b"} | x = "y" or x¤ = "z"`, `{a="b"} | (¤ = "z")`,
	// a number, a duration and a bytes size in a label filter
	`{a="b"} | x=¤`, `{a="b"} | x = ¤`, `{a="b"} | x > ¤`, `{a="b"} | x > ¤1`, `{a="b"} | x > 1¤`, `{a="b"} | x >= 1¤`, `{a="b"} | x == ¤1`,
	`{a="b"} | json | x == ¤1`, `{a="b"} | x < 1.5¤`, `{a="b"} | x < 1¤.5`, `{a="b"} | x < 5¤m`, `{a="b"} | x < 5m¤`, `{a="b"} | x < 5¤B`,
	`{a="b"} | x < 5B¤`, `{a="b"} | x < 5k¤B`, `{a="b"} | x < 1h¤30m`, `{a="b"} | x < 0x¤`, `{a="b"} | x < 0b¤`, `{a="b"} | x < 1e¤`,
	`{a="b"} | x < 1e3¤`, `{a="b"} | x < 1_¤`, `{a="b"} | x < .¤`, `{a="b"} | x < -¤`, `{a="b"} | x < -¤5`, `{a="b"} | x < +¤5`,
	`{a="b"} | x > 1 and y > ¤2`, `{a="b"} | x > (¤)`, `{a="b"} | x > 5¤ and y = "z"`, `{a="b"} | x = ip(¤)`,
	// a number or a name in a binary operation, a vector and function arguments
	`1 +¤1`, `1 + ¤`, `¤1 + 1`, `1¤ + 1`, `1 + 1¤`, `1 ¤+ 1`, `1.5¤`, `0x¤`, `sum(rate({a="b"}[5m])) > ¤`, `sum(rate({a="b"}[5m])) > 1¤`,
	`sum(rate({a="b"}[5m])) > bool ¤`, `sum(rate({a="b"}[5m])) > ¤1`, `rate({a="b"}[5m]) / ¤`, `rate({a="b"}[5m]) / 2¤`,
	`vector(¤1)`, `vector(1¤)`, `vector(¤)`, `topk(¤5, rate({a="b"}[5m]))`, `topk(5¤, rate({a="b"}[5m]))`, `topk(¤, rate({a="b"}[5m]))`,
	`quantile_over_time(¤0.9, {a="b"} | unwrap x [5m])`, `quantile_over_time(0.9¤, {a="b"} | unwrap x [5m])`,
	`label_replace(rate({a="b"}[5m]), "a¤", "b", "c", "d")`, `label_replace(rate({a="b"}[5m]), ¤, "b", "c", "d")`,
	// a range, an offset and the lists
	`rate({a="b"}[¤5m])`, `rate({a="b"}[5¤m])`, `rate({a="b"}[5m¤])`, `rate({a="b"}[¤])`, `rate({a="b"}[5m]¤)`, `rate({a="b"}¤[5m])`,
	`rate({a="b"}[5m] offset ¤5m)`, `rate({a="b"}[5m] offset 5¤m)`, `rate({a="b"}[5m] offset 5m¤)`, `rate({a="b"}[5m] offset ¤)`, `rate({a="b"}[5m] offset -¤5m)`,
	`sum by (¤) (rate({a="b"}[5m]))`, `sum by (a¤) (rate({a="b"}[5m]))`, `sum by (¤a) (rate({a="b"}[5m]))`, `sum by (a,¤) (rate({a="b"}[5m]))`,
	`sum without (¤) (rate({a="b"}[5m]))`, `sum(rate({a="b"}[5m])) by (¤)`, `sum¤(rate({a="b"}[5m]))`, `sum by¤(a) (rate({a="b"}[5m]))`,
	`rate({a="b"}[5m]) / on(¤) rate({a="b"}[5m])`, `rate({a="b"}[5m]) / on(a) group_left(¤) rate({a="b"}[5m])`, `rate¤({a="b"}[5m])`,
	// stage names, keywords and their arguments
	`{a="b"} | ¤json`, `{a="b"} | js¤on`, `{a="b"} | json¤`, `{a="b"} | logfmt¤`, `{a="b"} | line_format¤"x"`, `{a="b"} | line_format ¤"x"`,
	`{a="b"} | unwrap ¤x`, `{a="b"} | unwrap x¤`, `{a="b"} | unwrap bytes(¤x)`, `{a="b"} | drop ¤`, `{a="b"} | drop a¤`, `{a="b"} | drop a,¤`,
	`{a="b"} | keep ¤a`, `{a="b"} | label_format ¤=a`, `{a="b"} | label_format a=¤b`, `{a="b"} | label_format a=b¤`, `{a="b"} | regexp ¤"(?P<a>.)"`,
	`{a="b"} | pattern ¤"<a>"`, `{a="b"} | decolorize¤`, `{a="b"} | unpack¤`, `{a="b"} | json a¤="b"`, `{a="b"} | json ¤a="b"`, `{a="b"} | json a=¤"b"`,
	`{a="b"} |= ¤"x"`, `{a="b"} |= "x"¤`, `{a="b"} |~ ¤`, `{a="b"} |= ip¤("1.2.3.4")`, `{a="b"} | a=ip(¤"1.2.3.4")`, `{a="b"} |= "x" or ¤"y"`,
	`{a="b"} |¤= "x"`, `{a="b"} !¤= "x"`, `{a="b"} |> ¤"<_>"`, `{a="b"} | distinct ¤`, `{a="b"} | distinct a,¤b`,
	// a value in a string and a backquoted string, where every character is a byte of the value
	`{a="¤"}`, `{a="x¤y"}`, "{a=`¤`}", "{a=`x¤y`}", `{a="b"} |= "¤"`, "{a=\"b\"} |= `¤`", `{a="b"} |~ "x¤"`, `{a="b"} | x = "¤"`, "{a=\"b\"} | x = `¤`",
	`{a="b"} | line_format "¤"`, `{a="b"} | line_format "{{.a}}¤"`, `{a="b"} | label_format a="¤"`, `{a="b"} | json a="¤"`, `{a="b"} | regexp "(?P<a>¤)"`,
	`{a="b"} | pattern "<a>¤"`, `{a="b"} | drop a="¤"`, `{a="b"} | x > 1 | line_format "¤"`, `label_replace(rate({a="b"}[5m]), "a", "¤", "c", "d")`,
	// a comment
	`{a="b"} #¤`, "{a=\"b\"} # ¤\n|= \"x\"", "{a=\"b\"} #¤\n| json", `rate({a="b"}[5m]) # ¤`, "rate({a=\"b\"}[5m] # ¤\n)", "#¤\n{a=\"b\"}",
}

// unicodeRuneQueries is the grid: every sample in every template, and a few runs of the same character and of two
// categories in the positions where a name, a number and a unit meet.
func unicodeRuneQueries() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	for _, tpl := range runeTemplates {
		for _, cat := range runeSamples {
			for _, r := range cat.runes {
				add(strings.ReplaceAll(tpl, runeMark, string(r)))
			}
		}
	}
	for _, tpl := range []string{`{a="b"} | x ¤`, `{a="b"} | x > ¤1`, `{a="b"} | x < 5¤m`, `{a="b"}¤`, `{¤="a"}`, `rate({a="b"}[¤5m])`} {
		for _, pair := range [][2]rune{{0x663, 0x663}, {0x663, 'a'}, {'a', 0x663}, {0x663, 0xB2}, {0x2163, 0x663}, {0x301, 0x663}, {0x663, 0x301}, {0x200D, 0x663}, {0xFF13, 0xFF14}, {0x1D7D1, 0x1D7D2}, {0xB5, 0x663}} {
			add(strings.ReplaceAll(tpl, runeMark, string(pair[0])+string(pair[1])))
		}
	}
	return out
}

type runeAnswer struct {
	Q      string `json:"q"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// recordRuneAnswers runs Loki's parser (the oracle's check command) over the queries and writes its answers.
func recordRuneAnswers(t *testing.T, queries []string) {
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
		line, err := json.Marshal(runeAnswer{Q: queries[i], Accept: v.Accept, Error: v.Error})
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
	if err := os.WriteFile(unicodeRuneAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readRuneAnswers(t *testing.T) []runeAnswer {
	t.Helper()
	f, err := os.Open(unicodeRuneAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []runeAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a runeAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	return answers
}

// validateWithin runs ValidateLogQL on q and fails the test when it does not return within a second. A parser that
// does not return burns a core for the life of the process, so the check is the first to run on a query.
func validateWithin(t *testing.T, q string) string {
	t.Helper()
	done := make(chan string, 1)
	go func() { done <- ValidateLogQL(q) }()
	select {
	case msg := <-done:
		return msg
	case <-time.After(5 * time.Second):
		t.Fatalf("ValidateLogQL(%q) does not return", q)
		return ""
	}
}

// asciiTwin replaces every non-ASCII character of q by an ASCII one of the same lexical class for Go's text/scanner:
// 'z' for a letter, '7' for a decimal digit, '$' for the rest (a character that starts no token).
func asciiTwin(q string) string {
	var b strings.Builder
	for _, r := range q {
		switch {
		case r < utf8.RuneSelf:
			b.WriteRune(r)
		case unicode.IsLetter(r):
			b.WriteByte('z')
		case unicode.Is(unicode.Nd, r):
			b.WriteByte('7')
		default:
			b.WriteByte('$')
		}
	}
	return b.String()
}

// hasWideSpace reports white space that is no ASCII white space (U+0085, U+00A0, U+2003, ...): the scanner skips it
// where Loki's lexer does not (the white space fix, semantics/logql-loki-whitespace).
func hasWideSpace(q string) bool {
	for _, r := range q {
		if r >= utf8.RuneSelf && unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// textClass names why the proxy's message differs from Loki's where both reject: white space the scanner still skips
// (step "logql-loki-whitespace"), a quote (Loki's lexer reads 'x' as a character literal and reports its own errors,
// which the query scanner does not), a '!' (the proxy reads it as the start of !=), or another ASCII symbol (&, $, @,
// ;, ?, \\, ~: the scanner reports it as an error where Loki takes it as a name).
func textClass(q string) string {
	switch {
	case hasWideSpace(q):
		return "white space"
	case strings.Contains(q, "'"):
		return "character literal"
	case strings.Contains(q, "!"):
		return "bang"
	}
	return "ASCII symbol"
}

// Characters of every Unicode category, in every position of a query, are read as Loki's lexer reads them. Loki's
// scanner is Go's text/scanner: a name starts with a letter or '_' and goes on with letters and digits, a number
// starts with an ASCII digit, and any other character starts no token and is returned as a name of its own, which
// the grammar accepts only where a name may stand. The proxy never accepts what Loki rejects because of a
// character: a query it accepts and Loki rejects must be accepted with the ASCII twin of its characters as well (a
// stage that is a bare name, which the proxy passes to the backend), or hold white space the scanner still skips.
// It rejects with Loki's text where the parser's messages are Loki's, and it turns away a character that starts no
// token where Loki takes it as a label name (the translation to LogsQL cannot write such a name). Loki's answers
// are recorded with the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/logql-unicode-runes
func TestUnicodeRunesMatchLoki(t *testing.T) {
	queries := unicodeRuneQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordRuneAnswers(t, queries)
	}
	answers := readRuneAnswers(t)
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(queries))
	}
	var accepted, rejected, diffs, bareStage, wideSpace, nameLimit, textDiffs, same int
	textByClass := map[string]int{}
	for i, a := range answers {
		if a.Q != queries[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, queries[i])
		}
		got := validateWithin(t, a.Q)
		if a.Accept {
			accepted++
		} else {
			rejected++
		}
		switch {
		case got == "" && !a.Accept:
			switch {
			case hasWideSpace(a.Q):
				wideSpace++
			case validateWithin(t, asciiTwin(a.Q)) == "":
				bareStage++
			default:
				diffs++
				if diffs <= 40 {
					t.Errorf("%q\n  proxy: accepted (its ASCII twin %q is rejected)\n  Loki:  %q", a.Q, asciiTwin(a.Q), a.Error)
				}
			}
		case got != "" && a.Accept:
			nameLimit++
		case got != "" && got != a.Error:
			textDiffs++
			textByClass[textClass(a.Q)]++
		default:
			same++
		}
	}
	t.Logf("%d queries: Loki accepts %d and rejects %d; same answer %d; proxy accepts what Loki rejects: %d on white space, %d on a bare-name stage, %d other; proxy rejects what Loki accepts: %d; rejected by both with other text: %d",
		len(answers), accepted, rejected, same, wideSpace, bareStage, diffs, nameLimit, textDiffs)
	t.Logf("rejected by both with other text, by class: %v", textByClass)
	if diffs > 0 {
		t.Errorf("%d queries are accepted by the proxy and rejected by Loki because of a character", diffs)
	}
	// The limits may only shrink: the white space fix and the stage fix remove the first two, the characters the
	// scanner still reports as errors the text classes.
	if wideSpace > 649 || bareStage > 272 || nameLimit > 1463 {
		t.Errorf("the limits grew: white space %d, bare-name stage %d, name %d", wideSpace, bareStage, nameLimit)
	}
	for class, bound := range map[string]int{"white space": 143, "ASCII symbol": 38, "bang": 13, "character literal": 7} {
		if textByClass[class] > bound {
			t.Errorf("%d queries differ in text for %s, the limit is %d", textByClass[class], class, bound)
		}
	}
	if textDiffs > 201 {
		t.Errorf("%d queries differ in text, the limit is 201", textDiffs)
	}
	if accepted < 1000 || rejected < 1000 {
		t.Fatalf("grid too small: %d accepted, %d rejected", accepted, rejected)
	}
}
