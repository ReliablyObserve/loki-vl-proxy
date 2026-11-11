package lower

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
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

// reservedFieldStreams are the streams of the reserved field grid: every one is app="r", a row label naming it, and
// one stream label called after a field VictoriaLogs keeps for itself (_time, _stream, _stream_id, _msg) or after
// one of them in another case, with values a comparison reads (numbers, a duration, a size, an address) and others.
// Each stream has one line, pushed the way the Loki push API hands it to VictoriaLogs (stream labels, then _msg).
var reservedFieldStreams = []struct {
	label, value, line string
}{
	{"", "", "x"},
	{"_time", "st", "l"},
	{"_time", "10", "l"},
	{"_time", "3", "l"},
	{"_time", "7s", "l"},
	{"_time", "10.0.0.1", "l"},
	{"_time", "a\nb", "l"},
	{"_time", "2KB", "l"},
	{"_stream", "ss", "l"},
	{"_stream_id", "si", "l"},
	{"_msg", "x", "y"},
	{"_TIME", "up", "l"},
	{"_TIME", "11", "l"},
	{"_Stream", "S", "l"},
	{"_STREAM_ID", "SI", "l"},
}

// reservedFieldQueries are the stream selector matchers and label filters on those names, written by the lowering
// as stream filters, comparisons of the tag's value, and quoted fields.
var reservedFieldQueries = []string{
	`{app="r", _time="st"}`, `{app="r", _time!="st"}`, `{app="r", _time=~"s.*"}`, `{app="r", _time!~"s.*"}`,
	`{app="r", _time=""}`, `{app="r", _time!=""}`, `{app="r", _time=~"a.b"}`, `{app="r", _time=~"1.|3"}`,
	`{app="r", _stream="ss"}`, `{app="r", _stream_id="si"}`, `{app="r", _stream_id!="si"}`,
	`{app="r", _msg="x"}`, `{app="r", _msg!="x"}`, `{app="r", _msg=~".+"}`,
	`{app="r", _TIME="up"}`, `{app="r", _TIME!="up"}`, `{app="r", _Stream="S"}`, `{app="r", _STREAM_ID=~"S.*"}`,
	`{app="r"} | _time="st"`, `{app="r"} | _time!="st"`, `{app="r"} | _time=~"s.*"`, `{app="r"} | _time=~"s"`,
	`{app="r"} | _time!~"s.*"`, `{app="r"} | _time=""`, `{app="r"} | _time=~""`, `{app="r"} | _time!~""`,
	`{app="r"} | _time=~"a.b"`, `{app="r"} | _time=~"(?s)a.b"`, `{app="r"} | _time>5`, `{app="r"} | _time<5`,
	`{app="r"} | _time>=5s`, `{app="r"} | _time<5KB`, `{app="r"} | _time=ip("10.0.0.1")`,
	`{app="r"} | _time!=ip("10.0.0.1")`, `{app="r"} | _time>5 or _stream="ss"`, `{app="r"} | _time>5 and _time<20`,
	`{app="r"} | _time==10`, `{app="r"} | _time!=10`, `{app="r"} | _time=~"1.*" | _time>5`, `{app="r"} | _time>5 | _time<11`,
	`{app="r"} | _stream="ss"`, `{app="r"} | _stream_id!="si"`, `{app="r"} | _msg="x"`, `{app="r"} | _msg!="x"`,
	`{app="r"} | _TIME="up"`, `{app="r"} | _TIME>5`, `{app="r"} | _Stream="S"`,
	`{app="r"} | _STREAM_ID="SI"`, `{app="r"} | _TIME=~"u.*" or _time="st"`,
}

const reservedFieldAnswers = "testdata/reserved-field-labels.jsonl"

// reservedFieldAnswer is the recorded answer for one query: the rows (stream indexes) Loki keeps, those it keeps
// with an __error__ label (a comparison on a value that is not a number, which VictoriaLogs' comparison drops for
// every label alike: semantics/label-filter-numeric-error-label), and for each VictoriaLogs version the text the
// lowering wrote and the rows VictoriaLogs returned for it. Before is the string translator's text, which the
// proxy served before, with VictoriaLogs' (v1.53.0) answer to it: the evidence of what changed.
type reservedFieldAnswer struct {
	Query     string                      `json:"q"`
	Loki      []int                       `json:"loki"`
	LokiError []int                       `json:"loki_error,omitempty"`
	VL        map[string]reservedVLAnswer `json:"vl"`
	Before    reservedVLAnswer            `json:"before"`
}

type reservedVLAnswer struct {
	Text  string `json:"text"`
	Rows  []int  `json:"rows"`
	Error string `json:"error,omitempty"`
}

var reservedFieldVersions = []string{"v1.49.0", "v1.53.0"}

func reservedStreamLabels(i int) map[string]string {
	l := map[string]string{"app": "r", "row": strconv.Itoa(i)}
	if s := reservedFieldStreams[i]; s.label != "" {
		l[s.label] = s.value
	}
	return l
}

// reservedLowered is the lowering's text for q under the plain profile of a VictoriaLogs version.
func reservedLowered(t *testing.T, q, version string) string {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	queryplan.Resolve(p)
	op, err := Plan(p, Profile{Caps: logsql.CapabilitiesFor(version)})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return op.(physical.VLQuery).Text
}

// A label called _time, _stream, _stream_id or _msg is filtered the way VictoriaLogs keeps it, and the client gets
// Loki's answer: for every stream of the grid, Loki's pipeline (the Loki oracle, process, with the selector's
// matchers) keeps the stream's line exactly when VictoriaLogs (the VictoriaLogs oracle, store: the rows stored
// as the Loki push API stores them, in v1.49.0 and v1.53.0) returns it for the lowering's text. The string
// translator's text for each is rejected by VictoriaLogs or read as its own filter (`_time:>=5s` is a time range,
// `_msg:="x"` the line). The answers are recorded; LVP_RUN_ORACLES=1 records them again (the VictoriaLogs oracle
// downloads VictoriaLogs; set GOMODCACHE to a fresh directory when the shared cache holds other copies).
//
// conformance: semantics/translator-writes-reserved-field-names
func TestReservedFieldLabelsMatchLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordReservedFieldAnswers(t)
	}
	answers := readReservedFieldAnswers(t)
	if len(answers) != len(reservedFieldQueries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(reservedFieldQueries))
	}
	for i, q := range reservedFieldQueries {
		a := answers[i]
		if a.Query != q {
			t.Fatalf("answer %d is for %s, not %s: record them again with LVP_RUN_ORACLES=1", i, a.Query, q)
		}
		for _, v := range reservedFieldVersions {
			got := a.VL[v]
			if text := reservedLowered(t, q, v); text != got.Text {
				t.Errorf("%s: the lowering writes %s, the answer was recorded for %s: record them again with LVP_RUN_ORACLES=1", q, text, got.Text)
				continue
			}
			if got.Error != "" {
				t.Errorf("%s: VictoriaLogs %s rejects %s: %s", q, v, got.Text, got.Error)
				continue
			}
			var vl []int
			for _, r := range got.Rows {
				if !slices.Contains(a.LokiError, r) {
					vl = append(vl, r)
				}
			}
			if !slices.Equal(vl, a.Loki) {
				t.Errorf("%s: Loki keeps rows %v, VictoriaLogs %s returns %v for %s", q, a.Loki, v, got.Rows, got.Text)
			}
		}
	}
}

func readReservedFieldAnswers(t *testing.T) []reservedFieldAnswer {
	t.Helper()
	f, err := os.Open(reservedFieldAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []reservedFieldAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a reservedFieldAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func recordReservedFieldAnswers(t *testing.T) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	var pc []oraclerun.ProcessCase
	for _, q := range reservedFieldQueries {
		for i, s := range reservedFieldStreams {
			pc = append(pc, oraclerun.ProcessCase{Query: q, Labels: reservedStreamLabels(i), Line: s.line})
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]oraclerun.StoreRow, len(reservedFieldStreams))
	for i, s := range reservedFieldStreams {
		rows[i].Stream = [][2]string{{"app", "r"}, {"row", strconv.Itoa(i)}}
		if s.label != "" {
			rows[i].Stream = append(rows[i].Stream, [2]string{s.label, s.value})
		}
		rows[i].Fields = [][2]string{{"_msg", s.line}}
	}
	answers := make([]reservedFieldAnswer, len(reservedFieldQueries))
	for i, q := range reservedFieldQueries {
		answers[i] = reservedFieldAnswer{Query: q, Loki: []int{}, VL: map[string]reservedVLAnswer{}}
		for j := range reservedFieldStreams {
			l := loki[i*len(reservedFieldStreams)+j]
			if l.Error != "" {
				t.Fatalf("%s: Loki rejects it: %s", q, l.Error)
			}
			switch {
			case !l.Selected || !l.Keep:
			case l.Labels["__error__"] != "":
				answers[i].LokiError = append(answers[i].LokiError, j)
			default:
				answers[i].Loki = append(answers[i].Loki, j)
			}
		}
	}
	for _, v := range reservedFieldVersions {
		modfile := ""
		if v != reservedFieldVersions[len(reservedFieldVersions)-1] {
			modfile = "go." + v + ".mod"
		}
		texts := make([]string, 0, 2*len(reservedFieldQueries))
		for _, q := range reservedFieldQueries {
			texts = append(texts, reservedLowered(t, q, v))
		}
		last := v == reservedFieldVersions[len(reservedFieldVersions)-1]
		if last {
			for _, q := range reservedFieldQueries {
				legacy, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, logsql.CapabilitiesFor(v))
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				texts = append(texts, legacy)
			}
		}
		got, err := oraclerun.VLStore(filepath.Join(root, "test", "oracle", "vl"), modfile, rows, texts)
		if err != nil {
			t.Fatal(err)
		}
		for i, g := range got {
			a := reservedVLAnswer{Text: texts[i], Rows: []int{}, Error: g.Error}
			for _, r := range g.Rows {
				for _, f := range r {
					if n, ok := strings.CutPrefix(f, "row="); ok {
						k, _ := strconv.Atoi(n)
						a.Rows = append(a.Rows, k)
					}
				}
			}
			sort.Ints(a.Rows)
			if i < len(reservedFieldQueries) {
				answers[i].VL[v] = a
			} else {
				answers[i-len(reservedFieldQueries)].Before = a
			}
		}
	}
	var b strings.Builder
	for _, a := range answers {
		line, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(reservedFieldAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The string translator writes each of these names as a field filter: VictoriaLogs rejects the text, or reads it as
// its time, stream or line filter. The lowering's text differs for every query of the grid, and
// ReadsReservedField names the ones that read the tag (the mixed-case names are quoted fields, the translator's
// text made right by QuoteKeywordFieldText).
//
// conformance: semantics/translator-writes-reserved-field-names
func TestReadsReservedField(t *testing.T) {
	for _, q := range reservedFieldQueries {
		legacy, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, logsql.CapabilitiesFor("v1.53.0"))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		lowered := reservedLowered(t, q, "v1.53.0")
		if legacy == lowered {
			t.Errorf("%s: the translator writes the lowering's text %s", q, lowered)
		}
		reads := ReadsReservedField(planOf(t, q), Profile{})
		if made := QuoteKeywordFieldText(legacy); (made != lowered) != reads {
			t.Errorf("%s: ReadsReservedField = %v, the translator's text made right %s, the lowering %s", q, reads, made, lowered)
		}
	}
	for q, want := range map[string]bool{
		`{app="r"} | json | _time="x"`:                          false, // delegated
		`{app="r"} | drop app | _time="x"`:                      false,
		`sum(count_over_time({app="r", _stream="x"}[1m]))`:      true,
		`rate({app="r"} | _msg="x" [1m]) / rate({app="r"}[1m])`: true,
		`{app="r"} | time="x"`:                                  false,
	} {
		if got := ReadsReservedField(planOf(t, q), Profile{}); got != want {
			t.Errorf("%s: ReadsReservedField = %v, want %v", q, got, want)
		}
	}
	// The label function decides: under one that writes dots for underscores, _time is the field time.
	dots := func(s string) string { return strings.ReplaceAll(s, "_", ".") }
	if ReadsReservedField(planOf(t, `{app="r", _time="a"}`), Profile{Label: dots}) {
		t.Error("_time under a dots label function is the field time")
	}
}

// A label filter on a reserved name after a stage that can define, rename or remove the label is left to the
// string translator.
//
// conformance: semantics/translator-writes-reserved-field-names
func TestReservedFieldAfterAStageIsDelegated(t *testing.T) {
	for _, q := range []string{
		`{app="r"} | json | _time="x"`, `{app="r"} | logfmt | _stream>5`, `{app="r"} | regexp "(?P<_msg>.+)" | _msg="x"`,
		`{app="r"} | drop app | _stream_id="x"`, `{app="r"} | keep app | _time="x"`, `{app="r"} | label_format _time=app | _time="x"`,
	} {
		_, err := Plan(planOf(t, q), Profile{Caps: logsql.CapabilitiesFor("v1.53.0")})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != reservedAfterStageConstruct {
			t.Errorf("%s: %v, want %s", q, err, reservedAfterStageConstruct)
		}
	}
}

// The cost of filtering a reserved field as its tag, against the same query on a plain label (x): a matcher or a
// label filter on a string at most 2 more (the stream filter's matcher, or the quoted name of another case; 5 in an
// or tree that has both), and a comparison at most 16 more (the extract, extract and delete pipes ahead of it, the
// guard and the final delete; the pipes and the guard are built once per name). The string translator's text for these is
// VictoriaLogs' parse error or another filter, so it is no reference here; with a label function (the tenant's
// stream labels) the translator allocates hundreds of times on each and the lowering under 60.
//
// conformance: semantics/translator-writes-reserved-field-names
func TestReservedFieldLoweringAllocations(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	lowered := func(q string) float64 {
		return testing.AllocsPerRun(5, func() {
			expr, _ := logql.Parse(q)
			pl, _ := queryplan.Build(expr, queryplan.Inputs{})
			queryplan.Resolve(pl)
			_, _ = Plan(pl, Profile{})
		})
	}
	plain := strings.NewReplacer("_time", "x", "_stream_id", "x", "_stream", "x", "_msg", "x", "_TIME", "x", "_Stream", "x", "_STREAM_ID", "x")
	for _, q := range reservedFieldQueries {
		bound := 2.0
		switch {
		case strings.ContainsAny(q[strings.IndexByte(q, '}'):], "<>") || strings.Contains(q, "ip(") || strings.Contains(q, "==") || strings.Contains(q, "!=1"):
			bound = 16
		case strings.Contains(q, " or "):
			bound = 5
		}
		if got, base := lowered(q), lowered(plain.Replace(q)); got > base+bound {
			t.Errorf("%s: %.0f allocations, %.0f for a plain label (bound +%.0f)", q, got, base, bound)
		}
	}
}
