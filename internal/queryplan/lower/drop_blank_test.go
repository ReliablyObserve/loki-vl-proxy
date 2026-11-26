package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// A drop blanks the fields a later stage reads, where its matchers match; a level field is put back after the last
// stage. The answers against Loki are TestCompat_KeepNamesAndMatchersLikeLoki (test/e2e-compat).
//
// conformance: semantics/drop-matcher-later-stage-reads-dropped-value
func TestDropReadByLaterStage(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []string
		none  bool
	}{
		{query: `{app="a"} | drop code="5" | code="5"`, want: []string{`| format if (code:="5") "" as code`}},
		{query: `{app="a"} | drop level="w" | level=""`, want: []string{
			`| format if (level:="w") "<level>" as __lvp_drop_0_0_level | format if (level:="w") "" as level | filter`,
			`as level | delete __lvp_drop_0_0_level`}},
		{query: `{app="a"} | drop level | level="w"`, want: []string{`| format "" as level`}},
		{query: `{app="a"} | drop code="5", code!="1" | code="5"`, want: []string{`(code:="5" OR `}},
		{query: `{app="a"} | drop level="w" | drop level="e" | level!=""`, want: []string{`__lvp_drop_0_0_level`, `__lvp_drop_1_0_level`}},
		{query: `{app="a"} | drop code="5" | keep code="1"`, want: []string{`"" as code`}},
		{query: `sum(count_over_time({app="a"} | drop level="w" | level="w" [5m]))`, want: []string{`format if (level:="w") "" as level`}},
		// Nothing reads the name, or the plan leaves the field to its own reading.
		{query: `{app="a"} | drop level="w"`, none: true},
		{query: `{app="a"} | drop code="5" | other="1"`, none: true},
		{query: `{app="a"} | drop code | code="5"`, none: true},
		{query: `{app="a"} | drop detected_level="e" | detected_level="e"`, none: true},
		{query: `{app="a"} | drop service_name="a" | service_name="a"`, none: true},
		{query: `{app="a"} | drop level="w" | json | level="w"`, none: true},
		{query: `{app="a"} | drop level="w" | detected_level="w" | level="w"`, none: true},
	} {
		expr, err := logql.Parse(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		queryplan.Resolve(p)
		if got := WritesDropBlank(p); got == tc.none {
			t.Errorf("%s: WritesDropBlank = %v", tc.query, got)
		}
		text, err := planText(tc.query, p)
		if err != nil {
			t.Logf("%s: %v", tc.query, err)
			continue
		}
		if tc.none && (strings.Contains(text, `"" as `) || strings.Contains(text, dropScratch)) {
			t.Errorf("%s: blanks in %s", tc.query, text)
		}
		for _, w := range tc.want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: %s lacks %s", tc.query, text, w)
			}
		}
	}
}
