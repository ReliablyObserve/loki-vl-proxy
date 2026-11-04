package translator

import "testing"

// The inner stats of a fold over a range aggregation whose pipeline ends in
// a bare | keep group by the kept labels: Loki deletes every other label
// (pkg/logql/log/keep_labels.go:15-45), so the streams that keep the same
// labels are one series.
// conformance: semantics/keep-leaves-labels-loki-drops
func TestTrailingKeepGrouping(t *testing.T) {
	for _, tc := range []struct {
		query, want string
		ok          bool
	}{
		{`{app="x"} | json | keep foo`, "foo", true},
		{`{app="x"} | keep a, b`, "a, b", true},
		{`{app="x"} | json | keep a, __error__, __error_details__`, "a", true},
		{`{app="x"} | json | keep __error__`, "", true},
		{`{app="x"} | json | keep a |= "x" | a="1" | line_format "{{.a}}" | decolorize`, "a", true},
		{`{app="x"} | keep a, b | json | keep b`, "b", true},
		{`{app="x"} |= "keep" != "z"`, "", false},
		{`{app="x"} | json`, "", false},
		{`{app="x"} | keep level="error"`, "", false},
		{`{app="x"} | keep a | json`, "", false},
		{`{app="x"} | keep a | logfmt`, "", false},
		{`{app="x"} | keep a | label_format b="c"`, "", false},
		{`{app="x"} | keep a | drop a`, "", false},
		{`{app="x"} | keep a | unwrap n`, "", false},
	} {
		got, ok := trailingKeepGrouping(tc.query)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: %q %v, want %q %v", tc.query, got, ok, tc.want, tc.ok)
		}
	}
	for _, tc := range []struct{ query, want string }{
		{`count(rate({app="x"} | json | keep foo [5m]))`, `stats by (foo) count() as __lvp_inner | stats count()`},
		{`count(count_over_time({app="x"} | json | keep foo, pod [5m]))`, `stats by (foo, pod) count() as __lvp_inner | stats count()`},
		{`count(rate({app="x"} | json | keep __error__ [5m]))`, `stats by () count() as __lvp_inner | stats count()`},
		{`count(rate({app="x"} | json [5m]))`, `stats by (_stream) count() as __lvp_inner | stats count()`},
	} {
		got, err := TranslateLogQL(tc.query)
		if err != nil || len(got) < len(tc.want) || got[len(got)-len(tc.want):] != tc.want {
			t.Errorf("%s\n=> %s (%v)\nwant suffix %s", tc.query, got, err, tc.want)
		}
	}
}
