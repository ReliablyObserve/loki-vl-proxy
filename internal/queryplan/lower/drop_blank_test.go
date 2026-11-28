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
		// rewritten is a filter the plan writes reading the stored value, with no blanking pipe.
		rewritten bool
	}{
		// Several stages read the name, or a later one may: the field is blanked.
		{query: `{app="a"} | drop code="5" | code="5" | code!="1"`, want: []string{`| format if (code:="5") "" as code`}},
		{query: `{app="a"} | drop level="w" | level="" | level!="x"`, want: []string{
			`| format if (level:="w") "<level>" as __lvp_drop_0_0_level | format if (level:="w") "" as level | filter`,
			`as level | delete __lvp_drop_0_0_level`}},
		{query: `{app="a"} | drop level | level="w"`, want: []string{`| format "" as level`}},
		{query: `{app="a"} | drop level="w", level | level="x"`, want: []string{`| format "" as level`}},
		{query: `{app="a"} | drop code="5", code!="1" | code="5" | code="6"`, want: []string{`(code:="5" OR `}},
		{query: `{app="a"} | drop level="w" | drop level="e" | level!=""`, want: []string{`__lvp_drop_0_0_level`, `filter level:!"" AND NOT level:="e" |`}},
		{query: `{app="a"} | drop code="5" | keep code="1"`, want: []string{`"" as code`}},
		{query: `{app="a"} | drop code="5" | label_format x="{{.code}}" | code="5"`, want: []string{`"" as code`}},
		{query: `{app="a"} | drop code="5" | code > 5`, want: []string{`"" as code`}},
		// Behind a parser the filter may read an alias field or a stream label's stored copy: the field is blanked.
		{query: `{app="a"} | json | drop code="5" | code="7"`, want: []string{`| format if (code:="5") "" as code`}},
		{query: `sum(count_over_time({app="a"} | drop level="w" | level="w" [5m]))`, want: []string{`app:="a" level:="w" AND NOT level:="w" | stats`}, rewritten: true},
		// The empty value passes the filter: a metric query groups by the blanked field.
		{query: `sum(count_over_time({app="a"} | drop level="w" | level!="x" [5m]))`, want: []string{`format if (level:="w") "" as level`}},
		// The only later reader is one string comparison: it reads the stored value (dropHoistedFilter), as F(x) AND
		// NOT R when the empty value fails F, as R OR F(x) when it passes, and as F(x) alone when R spares F's value.
		{query: `{app="a"} | drop code="5" | code="5"`, want: []string{`app:="a" code:="5" AND NOT code:="5"`}, rewritten: true},
		{query: `{app="a"} | drop level="w" | level=""`, want: []string{`app:="a" (level:="w" OR -level:*)`}, rewritten: true},
		{query: `{app="a"} | drop code="5" | code!="7"`, none: true, want: []string{`app:="a" -code:="7"`}},
		{query: `{app="a"} | drop code="5" | code!="5"`, want: []string{`app:="a" (code:="5" OR -code:="5")`}, rewritten: true},
		{query: `{app="a"} | drop code="5", code="6" | code=~"[0-4]"`, none: true, want: []string{`app:="a" code:~`}},
		{query: `{app="a"} | drop code=~"5.*" | code!="7"`, want: []string{`app:="a" (code:~"^(?s:5.*)$" OR -code:="7")`}, rewritten: true},
		{query: `{app="a"} | drop code="5", code!="1" | code="5"`, want: []string{`code:="5" AND NOT (code:="5" OR -code:="1")`}, rewritten: true},
		{query: `{app="a"} | drop code=~"5.." |= "x" | code=~"4.."`, want: []string{`code:~"^(?-s:4..)$" AND NOT code:~"^(?s:5..)$"`}, rewritten: true},
		{query: `{app="a"} | drop level="warn" | level="nope"`, none: true, want: []string{`app:="a" level:="nope"`}},
		{query: `{app="a"} | drop level="warn", level="info" | decolorize | level="nope"`, none: true, want: []string{`app:="a" | decolorize | filter level:="nope"`}},
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
		if (tc.none || tc.rewritten) && (strings.Contains(text, `"" as `) || strings.Contains(text, dropScratch)) {
			t.Errorf("%s: blanks in %s", tc.query, text)
		}
		for _, w := range tc.want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: %s lacks %s", tc.query, text, w)
			}
		}
	}
}
