package translate

import (
	"strconv"
	"strings"
	"testing"
)

// splitUnwrapGateReference is SplitUnwrapGate as it was before the range filter check in front: every conversion's
// gate is written and compared.
func splitUnwrapGateReference(query string) (base, field string, ok bool) {
	idx := strings.LastIndex(query, ` | filter "`)
	if idx < 0 {
		return query, "", false
	}
	m := unwrapGateMarkerRE.FindStringSubmatch(query[idx:])
	if m == nil {
		return query, "", false
	}
	field, err := strconv.Unquote(m[1])
	if err != nil {
		return query, "", false
	}
	for _, conv := range []string{"", "duration", "bytes"} {
		if query[idx:] == UnwrapGateFor(field, conv) {
			return query[:idx], field, true
		}
	}
	return query, "", false
}

// SplitUnwrapGate splits a query as the reference does: the check of the range filter that ends a gate only turns
// away earlier the queries no gate ends, over gates of any field and conversion, with or without pipes after them,
// cut short, and arbitrary text.
func FuzzSplitUnwrapGate(f *testing.F) {
	for _, field := range []string{"n", "a-b", `we"ird`, "dotted.field", ""} {
		for conv := 0; conv < 3; conv++ {
			f.Add(`{app="x"}`, field, uint8(conv), "", 0)
			f.Add(`{app="x"}`, field, uint8(conv), " | hash(_msg) as h", 0)
			f.Add(`{app="x"}`, field, uint8(conv), "", 7)
		}
	}
	f.Add(`app:="a" | filter "n":~"x" | filter __lvp_v:<=inf`, "", uint8(3), "", 0)
	f.Fuzz(func(t *testing.T, head, field string, conv uint8, tail string, cut int) {
		query := head
		if conv < 3 {
			query += UnwrapGateFor(field, []string{"", "duration", "bytes"}[conv])
		}
		query += tail
		if cut > 0 && cut < len(query) {
			query = query[:len(query)-cut]
		}
		gotBase, gotField, gotOK := SplitUnwrapGate(query)
		wantBase, wantField, wantOK := splitUnwrapGateReference(query)
		if gotBase != wantBase || gotField != wantField || gotOK != wantOK {
			t.Fatalf("SplitUnwrapGate(%q) = %q, %q, %v; the reference %q, %q, %v", query, gotBase, gotField, gotOK, wantBase, wantField, wantOK)
		}
	})
}
