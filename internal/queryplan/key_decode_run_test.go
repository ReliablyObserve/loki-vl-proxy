package queryplan

import "testing"

// Every corpus query's key decodes to the fields of its plan, node by node, under different inputs.
func TestKeyDecodesToPlan(t *testing.T) {
	texts, exprs := corpus(t)
	inputs := []Inputs{
		{Kind: KindLogs},
		{Kind: KindRange, StreamLabels: []string{"a", "b\x00c"}, Exposure: Exposure{LokiProfile: true, DottedNames: true}},
		{Kind: KindInstant, StreamLabels: []string{""}},
	}
	for i, e := range exprs {
		for _, in := range inputs {
			p, err := Build(e, in)
			if err != nil {
				t.Fatalf("%q: %v", texts[i], err)
			}
			checkKey(t, p, e, texts[i])
		}
	}
	t.Logf("%d corpus keys decoded under %d input sets", len(exprs), len(inputs))
}

// No proper prefix of a key is a key, and no key with a byte appended is: a key ends where its plan ends.
func TestKeyPrefixFree(t *testing.T) {
	_, exprs := corpus(t)
	checked := 0
	for i := 0; i < len(exprs); i += 5 {
		p, err := Build(exprs[i], Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		for n := 0; n < len(p.Key); n++ {
			if _, _, err := decodeKey(p.Key[:n]); err == nil {
				t.Fatalf("a %d byte prefix of a %d byte key decodes", n, len(p.Key))
			}
		}
		if _, _, err := decodeKey(append(append([]byte(nil), p.Key...), 0)); err == nil {
			t.Fatalf("a key with a trailing byte decodes")
		}
		checked++
	}
	t.Logf("%d keys checked at every prefix", checked)
}
