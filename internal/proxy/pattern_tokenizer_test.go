package proxy

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"slices"
	"testing"
)

// patternTokenizerAnswers holds the patterns tokenizer's answers for hand-picked and random lines, recorded from
// the tokenizer that served /loki/api/v1/patterns before it was rewritten. Each row is a line, whether the
// tokenizer accepted it and, when it did, the tokens as [offset, length] in the line, the spaces after each token
// and the joined pattern.
const patternTokenizerAnswers = "testdata/pattern_tokenizer_answers.jsonl.gz"

type patternTokenizerAnswer struct {
	Line        string   `json:"line"`
	OK          bool     `json:"ok"`
	Tokens      [][2]int `json:"tokens"`
	SpacesAfter []int    `json:"spaces_after"`
	Join        string   `json:"join"`
}

func readPatternTokenizerAnswers(tb testing.TB) []patternTokenizerAnswer {
	tb.Helper()
	fh, err := os.Open(patternTokenizerAnswers)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		tb.Fatal(err)
	}
	var out []patternTokenizerAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		var row patternTokenizerAnswer
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			tb.Fatal(err)
		}
		out = append(out, row)
	}
	if err := sc.Err(); err != nil {
		tb.Fatal(err)
	}
	return out
}

// conformance: loki_api_v1_patterns
func TestPatternTokenizerKeepsTheRecordedAnswers(t *testing.T) {
	tok := newPatternLineTokenizer()
	rows, accepted := 0, 0
	for _, want := range readPatternTokenizerAnswers(t) {
		rows++
		tokens, spacesAfter, ok := tok.Tokenize(want.Line)
		if ok != want.OK {
			t.Errorf("Tokenize(%q) accepted = %v, recorded %v", want.Line, ok, want.OK)
			continue
		}
		if !ok {
			continue
		}
		accepted++
		wantTokens := make([]string, len(want.Tokens))
		for i, span := range want.Tokens {
			wantTokens[i] = want.Line[span[0] : span[0]+span[1]]
		}
		if !slices.Equal(tokens, wantTokens) || !slices.Equal(spacesAfter, want.SpacesAfter) {
			t.Errorf("Tokenize(%q) = %q %v, recorded %q %v", want.Line, tokens, spacesAfter, wantTokens, want.SpacesAfter)
			continue
		}
		if got := tok.Join(tokens, spacesAfter); got != want.Join {
			t.Errorf("Join of %q = %q, recorded %q", want.Line, got, want.Join)
		}
	}
	if rows < 1000 || accepted < 900 {
		t.Fatalf("read %d rows, %d accepted: the recorded answers are incomplete", rows, accepted)
	}
}

// BenchmarkPatternTokenizer tokenizes and joins every recorded line once per iteration.
func BenchmarkPatternTokenizer(b *testing.B) {
	rows := readPatternTokenizerAnswers(b)
	tok := newPatternLineTokenizer()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, row := range rows {
			if tokens, spacesAfter, ok := tok.Tokenize(row.Line); ok {
				_ = tok.Join(tokens, spacesAfter)
			}
		}
	}
}

// FuzzPatternTokenizer checks that the tokenizer accounts for every byte of a line: the tokens, in order, are the
// line with the spaces that ended a token taken out.
//
// conformance: loki_api_v1_patterns
func FuzzPatternTokenizer(f *testing.F) {
	for _, seed := range []string{
		"level=info msg=\"request done\" status=200 duration=12ms",
		"GET /api/v1/query?x=1&y=2 HTTP/1.1 200",
		" leading space and «guillemets» … here",
		"emoji 🙂 here, and 日本語 テスト ログ",
		"a b c d e f g h i j k l m n o p q r s t u v w x y z a b c d e f g h i j k l m n o p q r s t u v w x y z",
		"",
	} {
		f.Add(seed)
	}
	tok := newPatternLineTokenizer()
	f.Fuzz(func(t *testing.T, line string) {
		tokens, spacesAfter, ok := tok.Tokenize(line)
		if !ok {
			if tokens != nil || spacesAfter != nil {
				t.Fatalf("Tokenize(%q) rejected the line but returned %q %v", line, tokens, spacesAfter)
			}
			return
		}
		if len(tokens) < patternMinTokens || len(tokens) > patternMaxTokens {
			t.Fatalf("Tokenize(%q) returned %d tokens, outside [%d, %d]", line, len(tokens), patternMinTokens, patternMaxTokens)
		}
		rest, prev, spaces := line, -1, spacesAfter
		for i, token := range tokens {
			if token == "" {
				t.Fatalf("Tokenize(%q) token %d is empty", line, i)
			}
			for len(spaces) > 0 && spaces[0] == i-1 {
				if rest == "" || rest[0] != ' ' {
					t.Fatalf("Tokenize(%q) records a space after token %d that the line does not have", line, i-1)
				}
				rest, spaces = rest[1:], spaces[1:]
			}
			if len(rest) < len(token) || rest[:len(token)] != token {
				t.Fatalf("Tokenize(%q) token %d %q is not the next part of the line %q", line, i, token, rest)
			}
			rest, prev = rest[len(token):], i
		}
		for len(spaces) > 0 && spaces[0] == prev && rest != "" && rest[0] == ' ' {
			rest, spaces = rest[1:], spaces[1:]
		}
		if rest != "" || len(spaces) != 0 {
			t.Fatalf("Tokenize(%q) left %q of the line and spaces %v unaccounted for", line, rest, spaces)
		}
		_ = tok.Join(tokens, spacesAfter)
	})
}
