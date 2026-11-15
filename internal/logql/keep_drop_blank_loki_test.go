package logql

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// keepDropBlankAnswers holds what Loki's parser answers for every query of keepDropBlankQueries.
const keepDropBlankAnswers = "testdata/keep-drop-blank-loki.jsonl.gz"

// keepDropBlankAnswer is one recorded answer of the Loki oracle.
type keepDropBlankAnswer struct {
	Q      string `json:"q"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// keepDropBlankChars is every character Go reads as white space: the four Loki's lexer skips (space, tab, line
// feed, carriage return) and the others, which Loki reads as a name of their own (a vertical tab, a form feed,
// U+0085, U+00A0, U+1680, U+2000 to U+200A, U+2028, U+2029, U+202F, U+205F, U+3000).
func keepDropBlankChars() []string {
	var out []string
	for _, c := range whitespaceChars() {
		if r, _ := utf8.DecodeRuneInString(c); unicode.IsSpace(r) {
			out = append(out, c)
		}
	}
	return out
}

// keepDropBlankFollowers are what follows the keyword, with C for the character under test: the character alone,
// twice, beside another one Loki reads as a name, before and after a label name, a matcher and a comma, before a
// comment and a line feed. A name written against the keyword (`| keepa`) is another difference, left out.
var keepDropBlankFollowers = []string{
	"C", " C", "C ", "CC", "C\v", "C\u00a0", "C\t", "Ca", "C a", " C a", "C, a", "C,a", "C ,a", " a, C", " a,C", " a C",
	"C, a=\"x\"", " a=\"x\", C", "C,C", "C, \u00a0", "C,", "C #c\n", "C\n",
}

// keepDropBlankPositions are the places a drop or keep stage stands, with L for the stage.
var keepDropBlankPositions = []string{
	`{app="a"} | L`,
	`count_over_time({app="a"} | L [1m])`,
	`{app="a"} | L | json`,
	`{app="a"} | json | L`,
	`rate({app="a"} | L[1m])`,
	`sum by (a) (bytes_rate({app="a"} | json | L [5m]))`,
	`count(rate({app="a"} | L [5m]))`,
}

// keepDropBlankQueries is the grid: keep and drop, with each follower, with every character in the first two
// positions and the characters Loki reads as a name in the others; then the printed form of every query the proxy
// accepts, so that Loki's answer to what the proxy prints is recorded too.
func keepDropBlankQueries() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	few := []string{" ", "\t", "\v", "\f", "\u0085", "\u00a0", "\u2028", "\u3000"}
	for i, pos := range keepDropBlankPositions {
		chars := few
		if i < 2 {
			chars = keepDropBlankChars()
		}
		for _, kw := range []string{"keep", "drop"} {
			for _, f := range keepDropBlankFollowers {
				for _, c := range chars {
					add(strings.ReplaceAll(pos, "L", kw+strings.ReplaceAll(f, "C", c)))
				}
			}
		}
	}
	base := len(out)
	for _, q := range out[:base] {
		if e, err := Parse(q); err == nil {
			add(e.String())
		}
	}
	return out
}

// recordKeepDropBlankAnswers runs Loki's parser (the oracle's check command) over the queries and writes its answers.
func recordKeepDropBlankAnswers(t *testing.T, queries []string) {
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
	var b bytes.Buffer
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
		line, err := json.Marshal(keepDropBlankAnswer{Q: queries[i], Accept: v.Accept, Error: v.Error})
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
	var zipped bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&zipped, gzip.BestCompression)
	if _, err := zw.Write(b.Bytes()); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(keepDropBlankAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// readKeepDropBlankAnswers reads Loki's recorded answers, checked against the grid's queries in their order.
func readKeepDropBlankAnswers(t *testing.T, queries []string) []keepDropBlankAnswer {
	t.Helper()
	f, err := os.Open(keepDropBlankAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []keepDropBlankAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a keepDropBlankAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(queries))
	}
	for i, a := range answers {
		if a.Q != queries[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, queries[i])
		}
	}
	return answers
}

// A drop or keep followed by a character Loki's lexer does not skip reads it as Loki does: the character alone is a
// label name no stream has, which Loki accepts (`| keep\v`, `| drop\u00a0 | json`, `rate({...} | keep\f[1m])`), and
// the character beside another name is `unexpected IDENTIFIER`. The proxy accepts and rejects exactly what Loki does,
// with Loki's error text, and every query it accepts prints to a text that Loki accepts too and that parses back to
// the same tree: a list of only such characters prints as one of them (`| keep \v`), not `| keep `, which neither
// reads.
// Loki's answers are recorded with the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/keep-drop-blank-name-prints-back, semantics/parser-reads-unicode-blank-as-white-space
func TestKeepDropBlankMatchesLoki(t *testing.T) {
	// The grid's spellings stay out of the query corpus: the string translator reads a keep of no name as a phrase,
	// which the corpus would count against every typed selector of the grid (the corpus row of the registry case
	// stands for them).
	t.Cleanup(querycorpus.PauseRecording())
	queries := keepDropBlankQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordKeepDropBlankAnswers(t, queries)
	}
	answers := readKeepDropBlankAnswers(t, queries)
	accept := make(map[string]bool, len(answers))
	for _, a := range answers {
		accept[a.Q] = a.Accept
	}
	var accepted, rejected, diffs, blankLists int
	for _, a := range answers {
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
			continue
		}
		if !a.Accept {
			continue
		}
		first, err := Parse(a.Q)
		if err != nil {
			t.Errorf("%q: Loki and the validation accept it, the parser does not: %v", a.Q, err)
			continue
		}
		printed := first.String()
		second, err := Parse(printed)
		switch ok, recorded := accept[printed]; {
		case err != nil:
			t.Errorf("%q printed as %q, which does not parse: %v", a.Q, printed, err)
		case second.String() != printed:
			t.Errorf("%q printed as %q then %q: not stable", a.Q, printed, second.String())
		case !sameTree(first, second):
			t.Errorf("%q printed as %q, which parses to a different tree", a.Q, printed)
		case !recorded:
			t.Errorf("%q printed as %q, which has no recorded answer: record them again with LVP_RUN_ORACLES=1", a.Q, printed)
		case !ok:
			t.Errorf("%q printed as %q, which Loki rejects", a.Q, printed)
		}
		walkTree(reflect.ValueOf(&first), func(node any) {
			switch st := node.(type) {
			case *KeepStage:
				if len(st.Labels)+len(st.Matchers) == 0 {
					blankLists++
				}
			case *DropStage:
				if len(st.Labels)+len(st.Matchers) == 0 {
					blankLists++
				}
			}
		})
	}
	t.Logf("%d queries: Loki accepts %d and rejects %d; %d differ; %d accepted stages list only a blank", len(answers), accepted, rejected, diffs, blankLists)
	if diffs > 0 {
		t.Errorf("%d queries differ from Loki", diffs)
	}
	if accepted < 500 || rejected < 500 || blankLists < 200 {
		t.Fatalf("grid too small: %d accepted, %d rejected, %d blank lists", accepted, rejected, blankLists)
	}
}

// A list of only names Loki reads as labels no stream has prints as one of them, a vertical tab, whichever was
// written: any of them means the same to Loki. A list that names a label or holds a matcher prints only those.
//
// conformance: semantics/keep-drop-blank-name-prints-back
func TestKeepDropBlankPrintsBack(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, tc := range []struct{ in, want string }{
		{"{a=\"b\"} | keep\v", "{a=\"b\"} | keep \v"},
		{"{a=\"b\"} | drop\f", "{a=\"b\"} | drop \v"},
		{"{a=\"b\"} | keep \u00a0 | json", "{a=\"b\"} | keep \v | json"},
		{"{a=\"b\"} | keep\u2028,\v", "{a=\"b\"} | keep \v"},
		{"rate({a=\"b\"} | keep\u0085[1m])", "rate({a=\"b\"} | keep \v[1m])"},
		{"{a=\"b\"} | keep\v, a", "{a=\"b\"} | keep a"},
		{"{a=\"b\"} | keep a,\v", "{a=\"b\"} | keep a"},
		{"{a=\"b\"} | drop\u3000, a=\"x\"", "{a=\"b\"} | drop a=\"x\""},
		{"{a=\"b\"} | keep a", "{a=\"b\"} | keep a"},
	} {
		e, err := Parse(tc.in)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if got := e.String(); got != tc.want {
			t.Errorf("%q printed as %q, want %q", tc.in, got, tc.want)
		}
	}
}
