package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestScannerTokenSequences(t *testing.T) {
	tests := []struct {
		input string
		want  []logsql.Token
	}{
		{
			`app:="nginx" AND error`,
			[]logsql.Token{
				{logsql.TokIdent, "app"},
				{logsql.TokColonEq, ":="},
				{logsql.TokString, "nginx"},
				{logsql.TokAnd, "AND"},
				{logsql.TokIdent, "error"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`status:>=500`,
			[]logsql.Token{
				{logsql.TokIdent, "status"},
				{logsql.TokColonGTE, ":>="},
				{logsql.TokIdent, "500"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`NOT level:~"err.*"`,
			[]logsql.Token{
				{logsql.TokNot, "NOT"},
				{logsql.TokIdent, "level"},
				{logsql.TokColonTilde, ":~"},
				{logsql.TokString, "err.*"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`* | unpack_json`,
			[]logsql.Token{
				{logsql.TokStar, "*"},
				{logsql.TokPipe, "|"},
				{logsql.TokIdent, "unpack_json"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`{app="nginx", env="prod"}`,
			[]logsql.Token{
				{logsql.TokLBrace, "{"},
				{logsql.TokIdent, "app"},
				{logsql.TokEq, "="},
				{logsql.TokString, "nginx"},
				{logsql.TokComma, ","},
				{logsql.TokIdent, "env"},
				{logsql.TokEq, "="},
				{logsql.TokString, "prod"},
				{logsql.TokRBrace, "}"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`_time:5m`,
			[]logsql.Token{
				{logsql.TokIdent, "_time"},
				{logsql.TokColon, ":"},
				{logsql.TokIdent, "5m"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`latency:<=200`,
			[]logsql.Token{
				{logsql.TokIdent, "latency"},
				{logsql.TokColonLTE, ":<="},
				{logsql.TokIdent, "200"},
				{logsql.TokEOF, ""},
			},
		},
		{
			`error OR warn`,
			[]logsql.Token{
				{logsql.TokIdent, "error"},
				{logsql.TokOr, "OR"},
				{logsql.TokIdent, "warn"},
				{logsql.TokEOF, ""},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			sc := logsql.NewScanner(tc.input)
			for i, want := range tc.want {
				got := sc.Next()
				if got.Typ != want.Typ {
					t.Errorf("token[%d]: type got %d, want %d (val=%q)", i, got.Typ, want.Typ, got.Val)
				}
				if want.Val != "" && got.Val != want.Val {
					t.Errorf("token[%d]: val got %q, want %q", i, got.Val, want.Val)
				}
			}
		})
	}
}

func TestScannerQuotedString(t *testing.T) {
	sc := logsql.NewScanner(`"hello \"world\""`)
	tok := sc.Next()
	if tok.Typ != logsql.TokString {
		t.Fatalf("expected TokString, got %d", tok.Typ)
	}
	want := `hello "world"`
	if tok.Val != want {
		t.Errorf("got %q, want %q", tok.Val, want)
	}
}

func TestScannerRawString(t *testing.T) {
	sc := logsql.NewScanner("`(?P<level>\\w+)`")
	tok := sc.Next()
	if tok.Typ != logsql.TokRawString {
		t.Fatalf("expected TokRawString, got %d", tok.Typ)
	}
	if tok.Val != `(?P<level>\w+)` {
		t.Errorf("got %q", tok.Val)
	}
}

func TestScannerEOF(t *testing.T) {
	sc := logsql.NewScanner("")
	tok := sc.Next()
	if tok.Typ != logsql.TokEOF {
		t.Errorf("expected EOF, got %d", tok.Typ)
	}
	// Second call must also return EOF
	tok = sc.Next()
	if tok.Typ != logsql.TokEOF {
		t.Errorf("expected EOF on second call, got %d", tok.Typ)
	}
}
