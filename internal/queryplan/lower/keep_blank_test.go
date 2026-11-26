package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A keep with matchers writes the fields it names in written order; the matcher-named fields a later stage reads are
// blanked where no matcher matches, and a keep nothing reads after writes no blanking pipe. The answers against Loki
// are TestPlanMatchesLokiOnLogQueries over the rows of the case.
//
// conformance: semantics/keep-names-and-matchers
func TestKeepNamesAndMatchersText(t *testing.T) {
	for _, tc := range []struct {
		query string
		blank int
		want  []string
	}{
		{query: `{app="a"} | keep level="e", app`, want: []string{"| fields _time, _msg, _stream, level, app"}},
		{query: `{app="a"} | keep app, level="e"`, want: []string{"| fields _time, _msg, _stream, app, level"}},
		{query: `{app="a"} | keep level="e", app | json`, want: []string{"| unpack_json"}},
		{query: `{app="a"} | keep level="e", app | level="w"`, blank: 1, want: []string{`| format if (NOT level:="e") "" as level`}},
		{query: `{app="a"} | keep level="e", level="w", app | level="w"`, blank: 1, want: []string{`NOT level:="e" AND NOT level:="w"`}},
		{query: `{app="a"} | keep level!="e", app | line_format "{{.level}}"`, blank: 1, want: []string{"as level"}},
		{query: `{app="a"} | keep level="e", level | level="w"`},
		{query: `{app="a"} | keep level="e" | drop level="w"`, blank: 1},
		{query: `{app="a",level="e"} | keep level="e", app | level="w"`},
		{query: `{app="a",level="e"} | keep level="w", app | level="w"`, blank: 1},
		{query: `{app="a",level="e"} | json | keep level="e", app | level="w"`, blank: 1},
		{query: `rate({app="a"} | keep level="e" [5m])`, blank: 1},
		{query: `rate({app="a",level="e"} | keep level=~"e|w" [5m])`},
		{query: `{app="a"} | keep level="e" | env="p"`},
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
		if got := WritesKeepBlank(p); got != (tc.blank > 0) {
			t.Errorf("%s: WritesKeepBlank = %v, want %v", tc.query, got, tc.blank > 0)
		}
		text, err := planText(tc.query, p)
		if err != nil {
			// A stage the plan leaves to the translator: the case's shapes name which.
			t.Logf("%s: %v", tc.query, err)
			continue
		}
		if got := strings.Count(text, `| format if (NOT `); got != tc.blank {
			t.Errorf("%s: %d blanking pipes in %s, want %d", tc.query, got, text, tc.blank)
		}
		for _, w := range tc.want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: %s lacks %s", tc.query, text, w)
			}
		}
	}
}

// planText is the plan's LogsQL for a log or metric query.
func planText(q string, p *queryplan.Plan) (string, error) {
	if _, ok := p.Root.(*queryplan.LogSelect); ok {
		return PlanLogText(p, Profile{})
	}
	op, err := PlanQuery(q, p, Profile{})
	if err != nil {
		return "", err
	}
	return translator.EncodePhysical(op), nil
}
