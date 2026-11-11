package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A label Loki accepts and VictoriaLogs reads as a keyword (not, in, contains_all, options, and, or under a label
// function that drops a leading underscore) is filtered on as the field it is: the lowering quotes the name, where the
// string translator writes it bare and VictoriaLogs rejects the query ("missing whitespace or ':' between ..." for
// not, "the in must be put in quotes" for in). Names VictoriaLogs reads bare (range, exact, i, offset, ...) stay
// as they were. The quoted texts are in the VictoriaLogs verdicts of internal/logsql
// (testdata/keyword-field-names-vl.jsonl), accepted by v1.49.0 and v1.53.0, and the translator's are rejected there.
//
// conformance: semantics/translator-writes-keyword-field-names-bare
func TestPlanQuotesKeywordFieldNames(t *testing.T) {
	profiles := parserProfiles()
	none, dots := profiles[0], profiles[3]
	for _, c := range []struct {
		q       string
		prof    parserProfile
		want    string
		changed bool // the translator writes the name bare
	}{
		{`{not="a"}`, none, `"not":="a"`, true},
		{`{app="a"} | in="x"`, none, `app:="a" "in":="x"`, true},
		{`{app="a"} | contains_all>5`, none, `app:="a" "contains_all":>5`, true},
		{`{app="a"} | Not="x" or b="1"`, none, `app:="a" ("Not":="x" or b:="1")`, true},
		{`{app="a"} | options>5`, none, `app:="a" "options":>5`, true},
		{`{app="a"} | _or="x"`, dots, `app:="a" "or":="x"`, true},
		{`{app="a"} | AAAAAA>7KB and _OR=ip("0")`, dots, `app:="a" (AAAAAA:>7KB and "OR":~".*")`, true},
		{`{app="a"} | json | not="x"`, none, `app:="a" | unpack_json | filter "not":="x"`, true},
		{`{app="a"} | json x="not" | x="1"`, none, `app:="a" | unpack_json | copy "not" as x | filter "not":="1"`, true},
		{`{app="a"} | json x="OR" | x>=1`, none, `app:="a" | unpack_json | copy "OR" as x | filter "OR":>=1`, true},
		// Names VictoriaLogs reads bare, and a name that only starts or ends with a keyword, stay as they are.
		{`{app="a"} | range="x" | exact="y"`, none, `app:="a" range:="x" exact:="y"`, false},
		{`{app="a"} | not_x="x" | x_in="y"`, none, `app:="a" not_x:="x" x_in:="y"`, false},
		{`{app="a"} | _or="x"`, none, `app:="a" _or:="x"`, false},
		{`{app="a"} | json | offset2="x"`, none, `app:="a" | unpack_json | filter offset2:="x"`, false},
	} {
		expr := planOf(t, c.q)
		op, err := Plan(expr, Profile{Caps: c.prof.caps, Label: c.prof.label, StreamFields: c.prof.stream})
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		got := op.(physical.VLQuery).Text
		if got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.q, got, c.want)
		}
		legacy, err := translator.TranslateLogQLWithCapabilities(c.q, c.prof.label, c.prof.stream, c.prof.caps)
		if err != nil {
			t.Errorf("%s: translator: %v", c.q, err)
			continue
		}
		if (legacy != got) != c.changed {
			t.Errorf("%s: translator %s, lowering %s: differ = %v, want %v", c.q, legacy, got, legacy != got, c.changed)
		}
		if made := QuoteKeywordFieldText(legacy); made != got {
			t.Errorf("%s: the translator's text made right is %s, the lowering writes %s", c.q, made, got)
		}
		if _, err := logsql.Parse(got); err != nil {
			t.Errorf("%s: the proxy's parser does not read %s: %v", c.q, got, err)
		}
	}
}
