package logql

import (
	"testing"
)

func TestScanner_BasicTokens(t *testing.T) {
	tests := []struct {
		input  string
		tokens []TokType
	}{
		{
			`{app="nginx"}`,
			[]TokType{TokLBrace, TokIdent, TokEq, TokString, TokRBrace, TokEOF},
		},
		{
			`{app!="prod"}`,
			[]TokType{TokLBrace, TokIdent, TokNeq, TokString, TokRBrace, TokEOF},
		},
		{
			`{app=~"api.*"}`,
			[]TokType{TokLBrace, TokIdent, TokReMatch, TokString, TokRBrace, TokEOF},
		},
		{
			`{app!~"debug.*"}`,
			[]TokType{TokLBrace, TokIdent, TokReNotMatch, TokString, TokRBrace, TokEOF},
		},
		{
			`|= "error"`,
			[]TokType{TokPipeEq, TokString, TokEOF},
		},
		{
			`!= "debug"`,
			[]TokType{TokBangEq, TokString, TokEOF},
		},
		{
			`|~ "err.*"`,
			[]TokType{TokPipeTilde, TokString, TokEOF},
		},
		{
			`!~ "ok.*"`,
			[]TokType{TokBangTilde, TokString, TokEOF},
		},
		{
			`|> "GET <_> HTTP"`,
			[]TokType{TokPipeGt, TokString, TokEOF},
		},
		{
			`!> "GET <_> HTTP"`,
			[]TokType{TokBangGt, TokString, TokEOF},
		},
		{
			`| json`,
			[]TokType{TokPipe, TokIdent, TokEOF},
		},
		{
			`[5m]`,
			[]TokType{TokLBracket, TokDuration, TokRBracket, TokEOF},
		},
		{
			`rate({app="api"}[5m])`,
			[]TokType{TokIdent, TokLParen, TokLBrace, TokIdent, TokEq, TokString, TokRBrace, TokLBracket, TokDuration, TokRBracket, TokRParen, TokEOF},
		},
		{
			`sum by (app, env) (rate({app="api"}[5m]))`,
			[]TokType{
				TokIdent, TokBy, TokLParen, TokIdent, TokComma, TokIdent, TokRParen,
				TokLParen, TokIdent, TokLParen, TokLBrace, TokIdent, TokEq, TokString,
				TokRBrace, TokLBracket, TokDuration, TokRBracket, TokRParen, TokRParen,
				TokEOF,
			},
		},
		{
			`without (host)`,
			[]TokType{TokWithout, TokLParen, TokIdent, TokRParen, TokEOF},
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			sc := newScanner(tc.input)
			for i, want := range tc.tokens {
				got := sc.next()
				if got.Typ != want {
					t.Errorf("token[%d]: got %v (%q), want %v", i, got.Typ, got.Val, want)
				}
			}
		})
	}
}

func TestScanner_StringValues(t *testing.T) {
	sc := newScanner(`{app="nginx"} |= "error"`)
	toks := scanAll(sc)
	// Verify specific string token values
	found := false
	for _, tok := range toks {
		if tok.Typ == TokString && tok.Val == "nginx" {
			found = true
		}
	}
	if !found {
		t.Error("expected string token with value 'nginx'")
	}
}

func TestScanner_DurationValues(t *testing.T) {
	sc := newScanner(`[5m]`)
	_ = sc.next() // [
	dur := sc.next()
	if dur.Typ != TokDuration {
		t.Fatalf("expected TokDuration, got %v", dur.Typ)
	}
	if dur.Val != "5m" {
		t.Errorf("expected '5m', got %q", dur.Val)
	}
}

func TestScanner_BacktickString(t *testing.T) {
	sc := newScanner("| regexp `(?P<lvl>\\w+)`")
	_ = sc.next() // |
	_ = sc.next() // regexp
	tok := sc.next()
	if tok.Typ != TokRawString {
		t.Fatalf("expected TokRawString, got %v", tok.Typ)
	}
	if tok.Val != `(?P<lvl>\w+)` {
		t.Errorf("unexpected value: %q", tok.Val)
	}
}

func TestScanner_Keywords(t *testing.T) {
	keywords := []struct {
		input string
		typ   TokType
	}{
		{"by", TokBy},
		{"without", TokWithout},
		{"sum", TokIdent},    // not keyword-locked, parsed as ident
		{"rate", TokIdent},
		{"json", TokIdent},
	}
	for _, kw := range keywords {
		sc := newScanner(kw.input)
		tok := sc.next()
		if tok.Typ != kw.typ {
			t.Errorf("input %q: got %v, want %v", kw.input, tok.Typ, kw.typ)
		}
	}
}

// scanAll drains all tokens from a scanner.
func scanAll(sc *scanner) []Token {
	var toks []Token
	for {
		t := sc.next()
		toks = append(toks, t)
		if t.Typ == TokEOF {
			break
		}
	}
	return toks
}
