package proxy

import "testing"

// The label name of an extracted key, as Loki's json and logfmt parsers name it.
func TestOrderedJSONLabelNameEdges(t *testing.T) {
	for _, c := range []struct {
		key   string
		first bool
		want  string
	}{
		{"level", true, "level"},
		{"status_code", true, "status_code"},
		{"9lives", true, "_9lives"},
		{"9lives", false, "9lives"}, // a nested part after the first keeps its digit
		{"http.method", true, "http_method"},
		{"a-b c", true, "a_b_c"},
		{"  padded  ", true, "padded"},
		{"ünï", true, "_n_"}, // not ASCII: replaced, one underscore per character
		{"", true, ""},
		{"   ", true, ""},
		{"_", true, "_"},
		{"0", true, "_0"},
		{"A1_b2", true, "A1_b2"},
	} {
		if got := orderedJSONLabelName(c.key, c.first); got != c.want {
			t.Errorf("orderedJSONLabelName(%q, %v) = %q, want %q", c.key, c.first, got, c.want)
		}
	}
}
