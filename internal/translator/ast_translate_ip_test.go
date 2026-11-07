package translator

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestIPCallAndLiteralTranslateDistinctly(t *testing.T) {
	call, err := logql.ParseLogQuery(`{app="a"} |= ip("10.0.0.1")`)
	if err != nil {
		t.Fatal(err)
	}
	literal, err := logql.ParseLogQuery(`{app="a"} |= "ip(10.0.0.1)"`)
	if err != nil {
		t.Fatal(err)
	}
	callQL, callErr := TranslateAST(call, ASTTranslateOptions{})
	literalQL, literalErr := TranslateAST(literal, ASTTranslateOptions{})
	if callErr != nil || literalErr != nil || callQL == literalQL {
		t.Fatalf("IP call translated as literal: call=%s literal=%s errors=%v/%v", callQL, literalQL, callErr, literalErr)
	}
}
