package logql

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankspace"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

const blankAfterKeywordAnswers = "testdata/blank-after-keyword-loki.jsonl.gz"

// blankAnswer is what Loki's parser says about one spelling: whether it accepts it and, when it does, whether it reads
// the same expression as the spelling with one space (the oracle's check command with the spaced spelling as the
// printed form).
type blankAnswer struct {
	Q      string `json:"q"`
	Spaced string `json:"spaced"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
	Same   bool   `json:"same,omitempty"`
}

// blankAfterKeywordGrid is every template of blankspace.Templates with a space, with each blank Loki skips and with
// each character it rejects in the place of the mark. Each entry carries the spaced spelling it is compared with.
func blankAfterKeywordGrid() (queries, spaced []string, skipped []bool) {
	for _, tpl := range blankspace.Templates() {
		sp := strings.ReplaceAll(tpl, blankspace.Mark, " ")
		for _, b := range append(append([]string{" "}, blankspace.Skipped...), blankspace.Rejected...) {
			queries = append(queries, strings.ReplaceAll(tpl, blankspace.Mark, b))
			spaced = append(spaced, sp)
			skipped = append(skipped, b == " " || slices.Contains(blankspace.Skipped, b))
		}
	}
	return queries, spaced, skipped
}

func recordBlankAfterKeywordAnswers(t *testing.T, queries, spaced []string) {
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
		if err := enc.Encode(map[string]any{"id": fmt.Sprint(i), "kind": "client", "q": q, "ours": true, "printed": spaced[i]}); err != nil {
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
			Accept      bool   `json:"accept"`
			Error       string `json:"error"`
			SameMeaning *bool  `json:"same_meaning"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		line, err := json.Marshal(blankAnswer{Q: queries[i], Spaced: spaced[i], Accept: v.Accept, Error: v.Error, Same: v.SameMeaning != nil && *v.SameMeaning})
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
	if _, err := zw.Write([]byte(b.String())); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(blankAfterKeywordAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readBlankAfterKeywordAnswers(t *testing.T) []blankAnswer {
	t.Helper()
	f, err := os.Open(blankAfterKeywordAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []blankAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a blankAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	return answers
}

// A tab, a line feed, a carriage return or a run of them where a space stands after a parser keyword (json, logfmt,
// regexp, pattern, unpack), a stage name (label_format, line_format, drop, keep, decolorize, unwrap and its
// conversions), a grouping keyword or a binary operator is the same query to Loki's parser, and the proxy's parser reads
// the same tree (the same printed form up to the blanks it keeps as written). A vertical tab, a form feed, U+0085, U+00A0 or a control byte in that place is
// Loki's parse error, with Loki's text. Loki's answers are recorded with the Loki oracle (LVP_RUN_ORACLES=1 records them
// again); the grid of whitespace_loki.jsonl.gz holds the same positions for every character.
//
// conformance: semantics/translator-reads-only-space-between-tokens, semantics/parser-reads-unicode-blank-as-white-space
func TestBlankAfterKeywordMatchesLoki(t *testing.T) {
	queries, spaced, skipped := blankAfterKeywordGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordBlankAfterKeywordAnswers(t, queries, spaced)
	}
	answers := readBlankAfterKeywordAnswers(t)
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(queries))
	}
	var accepted, rejected int
	for i, a := range answers {
		if a.Q != queries[i] || a.Spaced != spaced[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, queries[i])
		}
		if a.Accept != skipped[i] {
			t.Errorf("%q: Loki accepts=%v %q; a blank Loki skips is accepted and any other character rejected", a.Q, a.Accept, a.Error)
		}
		msg := ValidateLogQL(a.Q)
		if a.Accept {
			accepted++
			if !a.Same {
				t.Errorf("%q: Loki reads another query than %q", a.Q, a.Spaced)
			}
			if msg != "" {
				t.Errorf("%q: the proxy rejects what Loki accepts: %s", a.Q, msg)
				continue
			}
			got, _ := Parse(a.Q)
			want, _ := Parse(a.Spaced)
			// The printed form keeps the blanks of a json extraction list and of label_replace's arguments as written.
			if got == nil || want == nil || blankspace.Collapse(got.String()) != want.String() {
				t.Errorf("%q: the proxy reads\n  %v\n but the spelling with a space reads\n  %v", a.Q, got, want)
			}
			continue
		}
		rejected++
		if msg != a.Error {
			t.Errorf("%q:\n proxy %q\n Loki  %q", a.Q, msg, a.Error)
		}
	}
	if accepted == 0 || rejected == 0 {
		t.Fatalf("%d accepted, %d rejected: the grid must hold both", accepted, rejected)
	}
	t.Logf("%d queries: Loki accepts %d (the same query as the spaced spelling) and rejects %d", len(answers), accepted, rejected)
}
