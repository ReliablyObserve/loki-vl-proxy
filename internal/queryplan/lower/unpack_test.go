package lower

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The `| unpack` parser against Loki and VictoriaLogs. The string translator writes `| unpack` as `| unpack_json`.
// For each family of stages after an unpack, the families below are run over lines of every kind (a packed line,
// one without `_entry`, an empty or numeric `_entry`, values that are not strings, a line that is not an object, an
// empty line): Loki's pipeline (the Loki oracle, process) says which lines the stages keep and what each kept line
// is, and VictoriaLogs (the VictoriaLogs oracle, store: the lines stored as the Loki push API stores them, in
// v1.49.0 and v1.53.0) runs the translator's text. The lowering writes a family natively only when the two keep the
// same lines; a family where they differ, or where the line is read, is delegated under the name of what it reads.
// The answers are recorded; LVP_RUN_ORACLES=1 records them again (the VictoriaLogs oracle downloads VictoriaLogs;
// set GOMODCACHE to a fresh directory when the shared cache holds other copies).

const unpackAnswers = "testdata/unpack-oracles.json"

// unpackRowLabel reads the row label of a stored stream (`{app="s",row="3"}`).
var unpackRowLabel = regexp.MustCompile(`row="(\d+)"`)

var (
	unpackVersions = []string{"v1.49.0", "v1.53.0"}
	// unpackLines are the lines of the grid, one stream each (app="s").
	unpackLines = []string{
		`{"a":"1","b":"x","_entry":"hello world"}`,
		`{"a":"1","b":"x"}`,
		`{"a":"1","_entry":""}`,
		`{"a":1,"b":true,"c":null,"d":{"e":"f"},"_entry":"L"}`,
		`not json`,
		`{"a":"1","_entry":5}`,
		`{"a":"1","_entry":{"x":"y"}}`,
		`{"_entry":"only"}`,
		`{"a":"1","n":{"_entry":"z"}}`,
		`{}`,
		``,
		`{"a":"1","_entry":"say \"hi\" {x}"}`,
		`{"a":"2","_entry":"hello again"}`,
		`{"a":"hello","_entry":"bye"}`,
		`{"a":"1","_entry":"{\"z\":\"2\"}"}`,
	}
)

// unpackFamily is a log query over an unpack and the way its range aggregations are lowered: lowered when the
// translator's text keeps the lines Loki keeps, else delegated as construct (a prefix of the aggregation's name).
type unpackFamily struct {
	// query is the log query whose lines the stages keep.
	query string
	// construct is the name a range aggregation over it is delegated under, "" when it is lowered.
	construct string
	// differs says what the translator's text gets wrong on the grid: "" nothing (the lowered families), "rows"
	// lines it keeps or drops unlike Loki, "line" the kept line.
	differs string
}

var unpackFamilies = []unpackFamily{
	{`{app="s"} | unpack`, "", ""},
	{`{app="s"} | unpack | drop a`, "", ""},
	{`{app="s"} | unpack | drop a, b`, "", ""},
	{`{app="s"} | unpack | drop a="1"`, "", ""},
	{`{app="s"} | unpack | keep a`, "", ""},
	{`{app="s"} | unpack | keep a, b`, "", ""},
	{`{app="s"} | unpack | a="1"`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack | a!="1"`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack | a=~"1|2"`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack | a=""`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack | a>=1`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack | __error__=""`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack | __error__!=""`, unpackLabelReadConstruct, "rows"},
	{`{app="s"} | unpack |= "hello"`, unpackLineReadConstruct, "rows"},
	{`{app="s"} | unpack != "hello"`, unpackLineReadConstruct, "rows"},
	{`{app="s"} | unpack |~ "hel+o"`, unpackLineReadConstruct, "rows"},
	{`{app="s"} | unpack | json | z="2"`, unpackOrderConstruct, "rows"},
	{`{app="s"} | unpack | line_format "{{.a}}"`, unpackLineReadConstruct, "line"},
	{`{app="s"} | unpack | decolorize`, unpackLineReadConstruct, "line"},
	{`{app="s"} | line_format "x" | unpack`, "", ""},
}

// unpackAnswer is the recorded answer for one family: per line, Loki's verdict and VictoriaLogs' under each version.
type unpackAnswer struct {
	Query string            `json:"q"`
	Text  string            `json:"text"`
	Loki  []unpackLokiLine  `json:"loki"`
	VL    map[string][]bool `json:"vl_kept"`
	// VLLine is the kept line VictoriaLogs returns (the same in both versions) and "" for a dropped one.
	VLLine []string `json:"vl_line"`
}

type unpackLokiLine struct {
	Keep   bool              `json:"keep"`
	Line   string            `json:"line,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// unpackTranslatorText is the string translator's text for q under the plain profile.
func unpackTranslatorText(t *testing.T, q, version string) string {
	t.Helper()
	s, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, logsql.CapabilitiesFor(version))
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return s
}

func recordUnpackAnswers(t *testing.T) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	var pc []oraclerun.ProcessCase
	for _, f := range unpackFamilies {
		for _, l := range unpackLines {
			pc = append(pc, oraclerun.ProcessCase{Query: f.query, Labels: map[string]string{"app": "s"}, Line: l})
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]oraclerun.StoreRow, len(unpackLines))
	for i, l := range unpackLines {
		rows[i] = oraclerun.StoreRow{Stream: [][2]string{{"app", "s"}, {"row", strconv.Itoa(i)}}, Fields: [][2]string{{"_msg", l}}}
	}
	answers := make([]unpackAnswer, len(unpackFamilies))
	for i, f := range unpackFamilies {
		a := unpackAnswer{Query: f.query, Text: unpackTranslatorText(t, f.query, "v1.53.0"), VL: map[string][]bool{}}
		for j := range unpackLines {
			l := loki[i*len(unpackLines)+j]
			if l.Error != "" {
				t.Fatalf("%s: Loki rejects it: %s", f.query, l.Error)
			}
			a.Loki = append(a.Loki, unpackLokiLine{Keep: l.Selected && l.Keep, Line: l.Line, Labels: l.Labels})
		}
		answers[i] = a
	}
	for _, v := range unpackVersions {
		modfile := ""
		if v != unpackVersions[len(unpackVersions)-1] {
			modfile = "go." + v + ".mod"
		}
		texts := make([]string, len(unpackFamilies))
		for i, f := range unpackFamilies {
			texts[i] = unpackTranslatorText(t, f.query, v) + " | fields _stream, _msg"
		}
		got, err := oraclerun.VLStore(filepath.Join(root, "test", "oracle", "vl"), modfile, rows, texts)
		if err != nil {
			t.Fatal(err)
		}
		for i, g := range got {
			if g.Error != "" {
				t.Fatalf("%s: VictoriaLogs rejects %s: %s", v, texts[i], g.Error)
			}
			kept := make([]bool, len(unpackLines))
			lines := make([]string, len(unpackLines))
			for _, r := range g.Rows {
				row, msg := -1, ""
				for _, f := range r {
					if n, ok := strings.CutPrefix(f, "_stream="); ok {
						if m := unpackRowLabel.FindStringSubmatch(n); m != nil {
							row, _ = strconv.Atoi(m[1])
						}
					}
					if m, ok := strings.CutPrefix(f, "_msg="); ok {
						msg = m
					}
				}
				kept[row], lines[row] = true, msg
			}
			answers[i].VL[v] = kept
			if v == unpackVersions[len(unpackVersions)-1] {
				answers[i].VLLine = lines
			}
		}
	}
	b, err := json.MarshalIndent(answers, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unpackAnswers, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Loki and VictoriaLogs on the lines of the grid: the families the lowering writes keep the lines Loki keeps, in
// both VictoriaLogs versions; the families it delegates differ on at least one line (so the delegation is needed),
// and name what they read.
//
// conformance: semantics/translator-writes-unpack-as-unpack-json
func TestUnpackFamiliesMatchLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordUnpackAnswers(t)
	}
	b, err := os.ReadFile(unpackAnswers)
	if err != nil {
		t.Fatal(err)
	}
	var answers []unpackAnswer
	if err := json.Unmarshal(b, &answers); err != nil {
		t.Fatal(err)
	}
	if len(answers) != len(unpackFamilies) {
		t.Fatalf("%d recorded answers for %d families: LVP_RUN_ORACLES=1 records them again", len(answers), len(unpackFamilies))
	}
	differing := 0
	for i, f := range unpackFamilies {
		a := answers[i]
		if a.Query != f.query || len(a.Loki) != len(unpackLines) {
			t.Fatalf("answer %d is for %s: LVP_RUN_ORACLES=1 records them again", i, a.Query)
		}
		if got := unpackTranslatorText(t, f.query, "v1.53.0"); got != a.Text {
			t.Errorf("%s: the translator writes %s, recorded for %s", f.query, got, a.Text)
		}
		rows, line := 0, 0
		for j, l := range a.Loki {
			for _, v := range unpackVersions {
				if a.VL[v][j] != l.Keep {
					rows++
					break
				}
			}
			// The line is read where both keep it.
			if l.Keep && a.VL[unpackVersions[len(unpackVersions)-1]][j] && a.VLLine[j] != l.Line {
				line++
			}
		}
		switch f.differs {
		case "":
			if rows != 0 {
				t.Errorf("%s: the translator's text keeps other lines than Loki on %d lines", f.query, rows)
			}
		case "rows":
			if rows == 0 {
				t.Errorf("%s: the translator's text keeps the lines Loki keeps on the whole grid: it needs no delegation", f.query)
			}
		case "line":
			if rows == 0 && line == 0 {
				t.Errorf("%s: the translator's text gives the lines Loki gives on the whole grid: it needs no delegation", f.query)
			}
		}
		differing += line
		// The lowering follows the recorded verdict: a count over the family is lowered exactly when the translator's
		// text keeps Loki's lines and reads neither the labels nor the line, and else is delegated under the name.
		_, err := unpackPlan(t, `count_over_time(`+f.query+` [5m])`, Profile{Caps: logsql.CapabilitiesFor("v1.53.0")})
		var nl *NotLoweredError
		switch {
		case f.construct == "" && err != nil:
			t.Errorf("%s: the family keeps Loki's lines, but a count over it is not lowered: %v", f.query, err)
		case f.construct != "" && !errors.As(err, &nl):
			t.Errorf("%s: the family differs from Loki (%s), but a count over it is lowered", f.query, f.differs)
		case f.construct != "" && nl.Construct != "RangeAgg LogSelect "+f.construct:
			t.Errorf("%s: a count over it is delegated as %q, want %q", f.query, nl.Construct, "RangeAgg LogSelect "+f.construct)
		}
	}
	// A log query returns the line, which Loki replaces by `_entry` on a packed line: the translator's text keeps
	// the packed one, so a log query over an unpack is delegated (unpackLineConstruct).
	if differing == 0 {
		t.Error("no family returns a line unlike Loki's: the grid has no packed line")
	}
}

// unpackPlan lowers q under the plain profile of VictoriaLogs v1.53.0.
func unpackPlan(t *testing.T, q string, prof Profile) (physical.Op, error) {
	t.Helper()
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	queryplan.Resolve(p)
	return PlanQuery(q, p, prof)
}

// A plain range aggregation over an unpack is lowered to the text of the string translator, which is Loki's answer
// for it (TestUnpackFamiliesMatchLoki); every other shape is delegated under the name of what it reads, and a log
// query under the line the unpack replaces.
//
// conformance: semantics/translator-writes-unpack-as-unpack-json
func TestUnpackLowering(t *testing.T) {
	v53 := Profile{Caps: logsql.CapabilitiesFor("v1.53.0")}
	// lowered: the translator's text, with its unpack_json.
	for _, q := range []string{
		`count_over_time({app="x"} | unpack [5m])`,
		`count_over_time({app="x"} | unpack | drop a [5m])`,
		`count_over_time({app="x"} | unpack | drop a, b [5m])`,
		`count_over_time({app="x"} | unpack | drop a="1" [5m])`,
		`count_over_time({app="x"} | unpack | drop __error__ [5m])`,
		`count_over_time({app="x"} | unpack | keep a [5m])`,
		`count_over_time({app="x"} | unpack | keep a, b [5m])`,
		`count_over_time({app="x"} | unpack | drop a | keep b [5m])`,
		`count_over_time({app="x"} |= "k" | unpack [5m])`,
		`count_over_time({app="x"} | l="a" | unpack [5m])`,
		`count_over_time({app="x"} | unpack [5m] offset 1h)`,
		`absent_over_time({app="x"} | unpack [5m])`,
		`rate({app="x"} | unpack [5m])`,
		`rate({app="x"} | unpack | drop a [5m])`,
	} {
		op, err := unpackPlan(t, q, v53)
		if err != nil {
			t.Errorf("%s: not lowered: %v", q, err)
			continue
		}
		text := op.(physical.VLQuery).Text
		legacy, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), nil, nil, v53.Caps)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if text != legacy {
			t.Errorf("%s:\n lowered %s\n  string %s", q, text, legacy)
		}
		if !strings.Contains(text, " | unpack_json") {
			t.Errorf("%s: lowered %s, want the unpack_json pipe", q, text)
		}
	}
	// delegated, by construct.
	for q, construct := range map[string]string{
		`{app="x"} | unpack`:                                              unpackLineConstruct,
		`{app="x"} | unpack | drop a`:                                     unpackLineConstruct,
		`{app="x"} |= "k" | unpack | a="1"`:                               unpackLineConstruct,
		`count_over_time({app="x"} | unpack | a="1" [5m])`:                "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`count_over_time({app="x"} | unpack | a=~"1|2" [5m])`:             "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`count_over_time({app="x"} | unpack | __error__="" [5m])`:         "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`count_over_time({app="x"} | unpack | label_format b=a [5m])`:     "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`count_over_time({app="x"} | unpack |= "k" [5m])`:                 "RangeAgg LogSelect " + unpackLineReadConstruct,
		`count_over_time({app="x"} | unpack | line_format "{{.a}}" [5m])`: "RangeAgg LogSelect " + unpackLineReadConstruct,
		`count_over_time({app="x"} | unpack | decolorize [5m])`:           "RangeAgg LogSelect " + unpackLineReadConstruct,
		`count_over_time({app="x"} | unpack | json [5m])`:                 "RangeAgg LogSelect " + unpackOrderConstruct,
		`count_over_time({app="x"} | json | unpack [5m])`:                 "RangeAgg LogSelect " + unpackOrderConstruct,
		`count_over_time({app="x"} | unpack | logfmt [5m])`:               "RangeAgg LogSelect " + unpackOrderConstruct,
		`bytes_over_time({app="x"} | unpack [5m])`:                        "RangeAgg LogSelect " + unpackLineReadConstruct,
		`bytes_rate({app="x"} | unpack [5m])`:                             "RangeAgg LogSelect " + unpackLineReadConstruct,
		`max_over_time({app="x"} | unpack | unwrap x [5m])`:               "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`max_over_time({app="x"} | unpack | k="a" | unwrap x [5m])`:       "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`sum_over_time({app="x"} | unpack | unwrap bytes(x) [5m])`:        "RangeAgg LogSelect " + unpackLabelReadConstruct,
		`sum(count_over_time({app="x"} | unpack [5m]))`:                   "VectorAgg " + unpackParserConstruct,
		`sum by (a) (rate({app="x"} | unpack [5m]))`:                      "VectorAgg " + unpackParserConstruct,
		`sum(max_over_time({app="x"} | unpack | unwrap x [5m]))`:          "VectorAgg " + unpackParserConstruct,
		`sum(bytes_over_time({app="x"} | unpack [5m]))`:                   "VectorAgg " + unpackParserConstruct,
	} {
		_, err := unpackPlan(t, q, v53)
		var nl *NotLoweredError
		if !errors.As(err, &nl) {
			t.Errorf("%s: want not lowered: %s, got %v", q, construct, err)
			continue
		}
		if nl.Construct != construct {
			t.Errorf("%s: delegated as %q, want %q", q, nl.Construct, construct)
		}
	}
}

// FuzzUnpackLowering holds the lowering of a query with an unpack to the contract of this file on any selector,
// stage before and after the unpack and function: a range aggregation the lowering writes is a count, a rate or an
// absence check whose stages after the unpack are drops and keeps and whose only parser is the unpack, written as
// the string translator writes it; a log query, and every other shape, is left to the translator.
//
// conformance: semantics/translator-writes-unpack-as-unpack-json
func FuzzUnpackLowering(f *testing.F) {
	f.Add("a", "x", uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("k8s.pod", "é\n", uint8(3), uint8(4), uint8(7), uint8(2))
	f.Add("app", "[5m]", uint8(1), uint8(9), uint8(8), uint8(1))
	f.Add("a", "x", uint8(2), uint8(12), uint8(9), uint8(4))
	f.Add("a", "x", uint8(0), uint8(20), uint8(5), uint8(0))
	f.Fuzz(func(t *testing.T, name, val string, fn, before, after, log uint8) {
		for _, s := range []string{name, val} {
			if translatorReadsAsSyntax(s) {
				return
			}
		}
		ident := "x"
		for _, r := range name {
			if r >= 'a' && r <= 'z' {
				ident = string(r) + "_n"
				break
			}
		}
		pre := []string{``, ` |= ` + strconv.Quote(val), ` != ` + strconv.Quote(val), ` | ` + ident + `=` + strconv.Quote(val), ` | drop ` + ident, ` | line_format "x"`}[before%6]
		post := []string{
			``, ` | drop ` + ident, ` | keep ` + ident, ` | drop ` + ident + `, b`, ` | drop ` + ident + `=` + strconv.Quote(val),
			` | ` + ident + `=` + strconv.Quote(val), ` |= ` + strconv.Quote(val), ` | json`, ` | line_format "{{.a}}"`, ` | decolorize`,
			` | label_format b=a`, ` | drop ` + ident + ` | ` + ident + `=~"x"`,
		}[after%12]
		stages := pre + ` | unpack` + post
		var q string
		switch log % 5 {
		case 0:
			q = `{app="a"}` + stages
		case 1:
			q = `max_over_time({app="a"}` + stages + ` | unwrap ` + ident + ` [5m])`
		default:
			fn := []string{"count_over_time", "rate", "absent_over_time", "bytes_over_time", "bytes_rate"}[fn%5]
			q = fn + `({app="a"}` + stages + ` [5m])`
		}
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			prof := Profile{Caps: logsql.CapabilitiesFor(v)}
			op, err := unpackPlan(t, q, prof)
			if err != nil {
				var nl *NotLoweredError
				if !errors.As(err, &nl) {
					t.Fatalf("%s: %v", q, err)
				}
				continue
			}
			if log%5 == 0 || log%5 == 1 {
				t.Fatalf("%s: lowered, a log query and an unwrapped value over an unpack are delegated", q)
			}
			legacy, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), nil, nil, prof.Caps)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if text := op.(physical.VLQuery).Text; text != legacy {
				t.Fatalf("%s:\n lowered %s\n  string %s", q, text, legacy)
			}
			switch fn % 5 {
			case 3, 4:
				t.Fatalf("%s: the bytes of a line an unpack replaced are lowered", q)
			}
			if after%12 > 4 {
				t.Fatalf("%s: a stage after the unpack that reads the labels or the line is lowered", q)
			}
		}
	})
}

var unpackBenchQueries = []string{
	`count_over_time({app="x"} | unpack [5m])`,
	`count_over_time({app="x"} | unpack | drop a [5m])`,
	`rate({app="x"} | unpack | keep a, b [5m])`,
	`absent_over_time({app="x"} |= "k" | unpack [5m])`,
}

// The string translator on the unpack aggregations the lowering writes: text to LogsQL text.
func BenchmarkUnpackRangeTranslator(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(unpackBenchQueries[i%len(unpackBenchQueries)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The lowering of the same aggregations, from the resolved plan.
func BenchmarkUnpackRangeLowered(b *testing.B) {
	plans := make([]*queryplan.Plan, len(unpackBenchQueries))
	for i, q := range unpackBenchQueries {
		plans[i] = rangeAggPlan(q)
		if plans[i] == nil {
			b.Fatal(q)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := PlanQuery(unpackBenchQueries[i%len(unpackBenchQueries)], plans[i%len(plans)], Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}
