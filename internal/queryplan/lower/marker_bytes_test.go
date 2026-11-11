package lower

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// translatorReadsStageMarker reports a query whose label filter value the string translator reads as syntax: it
// brackets the names a `<name>_extracted` filter reads with a \x02 byte in the text of that filter's stage and later
// takes the first pair of \x02 bytes of every stage that holds none for such brackets, cutting the bytes between them
// out of the stage. A label filter value that holds two \x02 bytes (`o="\x02\x00\x02"`, `o="a\x02b\x02c"`), or two
// values of one stage that hold one each, in a stage with no `_extracted` filter of its own, in a query with a json or
// logfmt parser and an `_extracted` filter elsewhere, is cut that way: the translator writes the filter on the value without those bytes (`o:=""`), copies the
// stream where the bytes were instead of where the `_extracted` filter reads it, and may write a query VictoriaLogs
// rejects. Loki and the query plan read the value as written (semantics/translator-reads-marker-bytes-in-quoted-value).
// The value is the decoded one, so the query may spell it with an escape, a raw byte or backquotes. A single \x02
// byte, and the \x00 and \x01 bytes the translator also uses, are read as text on every shape the grid found.
func translatorReadsStageMarker(q string) bool {
	expr, err := logql.Parse(q)
	if err != nil {
		return false
	}
	lq, ok := expr.(*logql.LogQuery)
	if !ok {
		return false
	}
	var stages [][]*logql.LabelFilterExpr
	extracted, parsed := false, false
	for _, st := range lq.Pipeline {
		if _, ok := st.(*logql.ParserStage); ok {
			parsed = true
		}
		lf, ok := st.(*logql.LabelFilterStage)
		if !ok || lf.Expr == nil {
			continue
		}
		var leaves []*logql.LabelFilterExpr
		collectLeaves(lf.Expr, &leaves)
		stages = append(stages, leaves)
		for _, l := range leaves {
			extracted = extracted || strings.HasSuffix(l.Name, "_extracted")
		}
	}
	if !extracted || !parsed {
		return false
	}
	for _, leaves := range stages {
		marks, own := 0, false
		for _, l := range leaves {
			own = own || strings.HasSuffix(l.Name, "_extracted")
			if l.Kind == logql.LabelFilterValueString {
				marks += strings.Count(l.Value, "\x02")
			}
		}
		if !own && marks >= 2 {
			return true
		}
	}
	return false
}

func collectLeaves(e *logql.LabelFilterExpr, out *[]*logql.LabelFilterExpr) {
	if e == nil {
		return
	}
	if e.IsLeaf() {
		*out = append(*out, e)
		return
	}
	collectLeaves(e.Left, out)
	collectLeaves(e.Right, out)
}

// conformance: architecture-syntax-package-layering
func TestTranslatorReadsStageMarker(t *testing.T) {
	const ext = ` | json | K_extracted!~"p|q"`
	for q, want := range map[string]bool{
		`{app="a"} | o="\x02\x00\x02"` + ext:                           true,
		`{app="a"} | o="\x02\x02"` + ext:                               true,
		`{app="a"} | o="a\x02b\x02c" | json | K_extracted="x"`:         true,
		`{app="a"} | o=` + "`\x02\x00\x02`" + ext:                      true, // a raw byte in backquotes
		`{app="a"} | o="\u0002a\U00000002"` + ext:                      true, // the same bytes spelled otherwise
		`{app="a"} | json | K_extracted="x" | o="\x02\x00\x02"`:        true, // behind the filter that reads the name
		`{app="a"} | logfmt | K_extracted="x" | o="\x02" and z="\x02"`: true, // one byte in each of two values
		`{app="a"} | o="\x02" and z="\x02"` + ext:                      true,
		`{app="a"} | o="\x02"` + ext:                                   false, // one byte
		`{app="a"} | o="\x02" | z="\x02"` + ext:                        false, // one byte in each of two stages
		`{app="a"} | o="\x00\x01\x00"` + ext:                           false, // the other bytes the translator uses
		`{app="a"} | json | o="\x02\x02"`:                              false, // no _extracted filter
		`{app="a"} | o="\x02\x02"`:                                     false, // filter-only
		`{app="a"} | json | K_extracted="x" and o="\x02\x02"`:          false, // the stage's own marker comes first
		`{app="a"} | json | o="\x02\x02" or K_extracted="x"`:           false,
		`{app="a"} |= "\x02\x02" | json | K_extracted="x"`:             false, // a line filter
		`{app="a"} | o="\\x02\\x02"` + ext:                             false, // a backslash and the text x02
		`rate({app="a"} | o="\x02\x02"` + ext + `[5m])`:                false, // not a log query
		`not a query`: false,
	} {
		if got := translatorReadsStageMarker(q); got != want {
			t.Errorf("%q: got %v, want %v", q, got, want)
		}
	}
}

// vlEqualsFilter reads the filter on field o that a lowered query holds in front of its first unpack, as
// VictoriaLogs reads it: the exact value (an empty one matches a missing field) and whether it is negated.
func vlEqualsFilter(t *testing.T, text string) (value string, negate bool) {
	t.Helper()
	q, err := logsql.Parse(text)
	if err != nil {
		t.Fatalf("%q does not parse: %v", text, err)
	}
	type hit struct {
		f   logsql.FieldFilter
		neg bool
	}
	var found []hit
	var walk func(f logsql.FilterExpr, neg bool)
	walk = func(f logsql.FilterExpr, neg bool) {
		switch f := f.(type) {
		case logsql.AllOf:
			for _, e := range f {
				walk(e, neg)
			}
		case logsql.AndExpr:
			walk(f.Left, neg)
			walk(f.Right, neg)
		case logsql.Exclude:
			walk(f.Expr, !neg)
		case logsql.FieldFilter:
			if f.Field == "o" {
				found = append(found, hit{f, neg != f.Negate})
			}
		}
	}
	walk(q.Filter, false)
	for _, p := range q.Pipes {
		if pf, ok := p.(logsql.PipeFilter); ok {
			walk(pf.Expr, false)
			continue
		}
		if _, ok := p.(logsql.PipeCopy); ok {
			continue
		}
		break // the first unpack: what follows reads the parsed keys
	}
	if len(found) != 1 {
		t.Fatalf("%q: %d filters on o in front of the first unpack, want 1", text, len(found))
	}
	f, negate := found[0].f, found[0].neg
	if f.Op != logsql.FieldOpExact {
		t.Fatalf("%q: the model does not read the filter %v", text, f)
	}
	return f.Value, negate
}

type markerBytesCase struct {
	Query  string            `json:"q"`
	Labels map[string]string `json:"labels"`
	Keep   bool              `json:"keep"`
}

const markerBytesAnswers = "testdata/marker-bytes-label-value-loki.json"

// markerBytesValues are label filter values the translator cuts (translatorReadsStageMarker), and rows that tell the
// value, an empty value, a missing label and another value apart.
var markerBytesValues = []string{"\x02\x00\x02", "\x02\x02", "a\x02b\x02c", "\x02a\x02"}

func markerBytesCases() []markerBytesCase {
	var out []markerBytesCase
	for _, v := range markerBytesValues {
		for _, op := range []string{"=", "!="} {
			for _, row := range []map[string]string{{"o": v}, {"o": ""}, {}, {"o": v + "x"}} {
				labels := map[string]string{"app": "a"}
				for k, s := range row {
					labels[k] = s
				}
				out = append(out, markerBytesCase{Query: `{app="a"} | o` + op + strconv.Quote(v), Labels: labels})
			}
		}
	}
	return out
}

// A label filter on a value that holds the translator's \x02 marker byte twice keeps the rows Loki keeps when the
// query plan lowers it, and the string translator's text keeps others. Loki's answers are recorded with the Loki
// oracle (test/oracle/loki process; LVP_RUN_ORACLES=1 records them again); a model of how VictoriaLogs reads the
// filter in front of the first unpack (an exact value, an empty value matching a missing field, the pipe that
// negates it) is run over each recorded row against the filter each side writes. The translator's text is held to
// the defect: when it keeps the rows Loki keeps, the case is fixed.
//
// conformance: semantics/translator-reads-marker-bytes-in-quoted-value
func TestMarkerBytesInLabelValueMeanWhatLokiMeans(t *testing.T) {
	cases := markerBytesCases()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMarkerBytesAnswers(t, cases)
	}
	b, err := os.ReadFile(markerBytesAnswers)
	if err != nil {
		t.Fatal(err)
	}
	var recorded []markerBytesCase
	if err := json.Unmarshal(b, &recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded) != len(cases) {
		t.Fatalf("%d recorded answers for %d cases: LVP_RUN_ORACLES=1 records them again", len(recorded), len(cases))
	}
	translatorDiffers := 0
	for _, c := range recorded {
		// Loki asked for the filter alone; the proxy gets the same filter in front of a parser and a filter that
		// reads a parsed key, the shape the translator cuts.
		q := c.Query + ` | json | K_extracted!~"p|q"`
		if !translatorReadsStageMarker(q) {
			t.Fatalf("%s: the recogniser does not see the defect", q)
		}
		op, err := Plan(planOf(t, q), Profile{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		legacy, err := translator.TranslateLogQL(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		holds := func(text string) bool {
			v, neg := vlEqualsFilter(t, text)
			return (c.Labels["o"] == v) != neg
		}
		if got := holds(op.(physical.VLQuery).Text); got != c.Keep {
			t.Errorf("%s labels %q: the plan keeps %v, Loki %v", q, c.Labels, got, c.Keep)
		}
		if holds(legacy) != c.Keep {
			translatorDiffers++
		}
	}
	t.Logf("%d recorded Loki answers: the plan agrees on all, the string translator keeps other rows on %d", len(recorded), translatorDiffers)
	if translatorDiffers == 0 {
		t.Errorf("the string translator keeps the rows Loki keeps on all %d rows: the case is fixed, drop the recogniser", len(recorded))
	}
}

func recordMarkerBytesAnswers(t *testing.T, cases []markerBytesCase) {
	t.Helper()
	pc := make([]oraclerun.ProcessCase, len(cases))
	for i, c := range cases {
		pc[i] = oraclerun.ProcessCase{Query: c.Query, Labels: c.Labels, Line: "x"}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "..", "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range answers {
		if a.Error != "" {
			t.Fatalf("Loki rejects %s: %s", cases[i].Query, a.Error)
		}
		cases[i].Keep = a.Keep
	}
	b, err := json.MarshalIndent(cases, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(markerBytesAnswers, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
