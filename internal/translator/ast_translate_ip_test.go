package translator

import "testing"

// An ip() line filter and the same text as a quoted literal are different queries: the string translator writes
// them differently.
func TestIPCallAndLiteralTranslateDistinctly(t *testing.T) {
	callQL, callErr := TranslateLogQL(`{app="a"} |= ip("10.0.0.1")`)
	literalQL, literalErr := TranslateLogQL(`{app="a"} |= "ip(10.0.0.1)"`)
	if callErr != nil || literalErr != nil || callQL == literalQL {
		t.Fatalf("IP call translated as literal: call=%s literal=%s errors=%v/%v", callQL, literalQL, callErr, literalErr)
	}
}
