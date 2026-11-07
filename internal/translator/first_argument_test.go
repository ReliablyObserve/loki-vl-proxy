package translator

import "testing"

func TestSplitFuncFirstArgSkipsStringsAndBrackets(t *testing.T) {
	if first, rest, ok := splitFuncFirstArg(`rate({a=") , ("}[5m]), "d", "e")`); !ok || first != `rate({a=") , ("}[5m])` || rest != `"d", "e")` {
		t.Errorf("splitFuncFirstArg = %q, %q, %v", first, rest, ok)
	}
	if _, _, ok := splitFuncFirstArg(`unclosed(`); ok {
		t.Error("splitFuncFirstArg accepted an unclosed call")
	}
}
