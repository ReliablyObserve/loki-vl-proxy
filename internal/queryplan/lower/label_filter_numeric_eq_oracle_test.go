package lower

import (
	"bufio"
	"encoding/json"
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

// The numeric equality oracle: for each query, the lines Loki's pipeline keeps (the Loki oracle, process, over the
// query run on every line of its value list) and the lines VictoriaLogs returns for the text the lowering writes and
// for the text the string translator writes (the VictoriaLogs oracle, store: the lines stored as the Loki push API
// hands them over, on v1.49.0 and v1.53.0). The answers are recorded; LVP_RUN_ORACLES=1 records them again (with a
// module cache whose VictoriaLogs checksum matches, see test/oraclerun).

// numericEqualitySet is a parser and a kind of value: the lines of one app, each holding one value of the list.
type numericEqualitySet struct {
	app, parser, field string
	values             []string
	// lines are the lines when a set spells the key differently from line to line (values then names each line);
	// stream are the stream labels the app has besides its name.
	lines  []string
	stream map[string]string
}

// numericNumberValues are the values of a number: the spellings of 200, 200.5 and -3, the spellings Go reads that
// VictoriaLogs does not and the reverse, text, units, and the empty value. A value written "#v" is a JSON number
// token (no quotes) in a json line, a plain value in a logfmt one.
var numericNumberValues = []string{
	"200", "200.0", "200.", "2e2", "2E2", "+200", "0200", "200.00", "2.0e2", "0.2e3", ".2e3", "2_00", "2.00e+2", "00200.000",
	"199.9", "200.1", "1e2", "-200", "0", "-0", "0.0", "200.5", "200.50", "2.005e2", "-3", "-3.0", "-03", "3", "+3",
	"0x5", "0xC8", "0x1.9p7", "0b11001000", "0o310", "1_0e1", "abc", "200abc", " 200", "200 ", "200,0", "inf", "NaN", "1e400",
	"५", "5KB", "5s", "", "#200", "#200.0", "#2e2", "#-3",
	// the units VictoriaLogs reads as numbers in one value space
	"1KB", "1K", "1000B", "1µs", "1000ns", "1s", "1GB", "0s", "1000", "1000.0",
}

// numericDurationValues are the values of a duration: Go's spellings of 5s, 1m30s, 1.5s and 100ms, the units
// VictoriaLogs reads differently, numbers without a unit, and text.
var numericDurationValues = []string{
	"5s", "5000ms", "5.0s", "5.000s", "05s", "+5s", "-5s", "5m", "5", "5.", "5000000000", "5000000000ns", "5000000µs", "5000000us",
	"5000000μs", "5 s", "5S", "5s ", "1m30s", "90s", "1.5m", "90000ms", "1m30.0s", "0h1m30s", "1m29.9999s", "1m30.5s", "1m 30s",
	"1.5s", "1500ms", "1s500ms", "1.50s", "100ms", "0.1s", "100000µs", "100000us", "abc", "", "0", "0s", "1d", "24h",
	// the sizes and numbers VictoriaLogs reads as nanoseconds in one value space
	"1GB", "1G", "1000000000", "1000000000ns", "1KB", "100000000", "1.5",
}

// numericBytesValues are the values of a bytes size: spellings of 1KB, 1.5MiB, 10B and 2K, the cases and spaces
// Loki's size parser reads and VictoriaLogs does not, bare numbers, and text.
var numericBytesValues = []string{
	"1KB", "1000", "1000B", "1 KB", "1kb", "1Kb", "1kB", "1K", "1k", "1.0KB", "1.000KB", "001KB", "1000.0", "0.001MB", "1KB ",
	"1KiB", "1024", "1MB", "1e3", "1,000", "1_000", "0x3e8", "1000b", "1000bytes", "+1KB", "-1KB", "abc", "", "0",
	"1.5MiB", "1572864", "1536KiB", "1.5 MiB", "1.50MiB", "1.5MB", "10B", "10", "10b", "10.0B", "2K", "2KB", "2000", "2 KB",
	// the durations VictoriaLogs reads as numbers in one value space
	"1µs", "1000ns", "1s", "10ns", "0s",
}

// numericEqualityKeyLines are the lines of the sets that spell a key three ways (http_status, http.status and
// http-status, which Loki reads as the label http_status) and leave it out, each with a few values.
func numericEqualityKeyLines(parser string) (values, lines []string) {
	for _, key := range []string{"http_status", "http.status", "http-status"} {
		for _, v := range []string{"200", "200.0", "404", "abc"} {
			values = append(values, key+"="+v)
			if parser == "json" {
				b, _ := json.Marshal(map[string]string{key: v})
				lines = append(lines, string(b))
			} else {
				lines = append(lines, key+"="+v)
			}
		}
	}
	return values, lines
}

// withoutJSONTokens is the values without the JSON number tokens, which a logfmt line spells like the strings.
func withoutJSONTokens(values []string) []string {
	var out []string
	for _, v := range values {
		if !strings.HasPrefix(v, "#") {
			out = append(out, v)
		}
	}
	return out
}

func numericEqualitySets() []numericEqualitySet {
	jv, jl := numericEqualityKeyLines("json")
	fv, fl := numericEqualityKeyLines("logfmt")
	podValues := []string{"2", "2.0", "3", "abc"}
	podLines := func(parser string) (lines []string) {
		for _, v := range podValues {
			if parser == "json" {
				lines = append(lines, `{"pod":"`+v+`"}`)
			} else {
				lines = append(lines, "pod="+v)
			}
		}
		return lines
	}
	return []numericEqualitySet{
		{app: "jn", parser: "json", field: "m", values: numericNumberValues},
		{app: "fn", parser: "logfmt", field: "m", values: withoutJSONTokens(numericNumberValues)},
		{app: "jd", parser: "json", field: "d", values: numericDurationValues},
		{app: "fd", parser: "logfmt", field: "d", values: numericDurationValues},
		{app: "jb", parser: "json", field: "b", values: numericBytesValues},
		{app: "fb", parser: "logfmt", field: "b", values: numericBytesValues},
		{app: "jk", parser: "json", field: "http_status", values: jv, lines: jl},
		{app: "fk", parser: "logfmt", field: "http_status", values: fv, lines: fl},
		// pod is a stream label of the first app: the parsed pod is read as pod_extracted there, and nowhere in the second
		{app: "je", parser: "json", field: "pod_extracted", values: podValues, lines: podLines("json"), stream: map[string]string{"pod": "p"}},
		{app: "jf", parser: "json", field: "pod_extracted", values: podValues, lines: podLines("json")},
		{app: "fe", parser: "logfmt", field: "pod_extracted", values: podValues, lines: podLines("logfmt"), stream: map[string]string{"pod": "p"}},
	}
}

// numericEqualityLiterals are the literals each field is compared with.
var numericEqualityLiterals = map[string][]string{
	"m": {"200", "200.5", "-3", "2e2", "0", "1000"},
	"d": {"5s", "1m30s", "1.5s", "100ms", "1s"},
	"b": {"1KB", "1.5MiB", "10B", "2K"},

	"http_status": {"200"}, "pod_extracted": {"2"},
}

// numericEqualityLine is the line of a set that holds value; a value that is "" and the line without the key are
// the last two lines (the latter is index len(values)).
func numericEqualityLine(s numericEqualitySet, value string) string {
	if s.parser == "json" {
		if raw, ok := strings.CutPrefix(value, "#"); ok {
			return `{"` + s.field + `":` + raw + `}`
		}
		b, _ := json.Marshal(map[string]string{s.field: value})
		return string(b)
	}
	if value == "" || strings.ContainsAny(value, " \"=\\\t") || strings.ContainsFunc(value, func(r rune) bool { return r > 126 }) {
		if value == "" {
			return s.field + "="
		}
		return s.field + "=" + strconv.Quote(value)
	}
	return s.field + "=" + value
}

func numericEqualityLines(s numericEqualitySet) []string {
	if s.lines != nil {
		return append(append([]string{}, s.lines...), numericEqualityMissingLine(s))
	}
	lines := make([]string, 0, len(s.values)+1)
	for _, v := range s.values {
		lines = append(lines, numericEqualityLine(s, v))
	}
	return append(lines, numericEqualityMissingLine(s))
}

// numericEqualityMissingLine is a line without the key the set's queries read.
func numericEqualityMissingLine(s numericEqualitySet) string {
	if s.parser == "json" {
		return `{"other":"1"}`
	}
	return "other=1"
}

// numericEqualityQuery is one query of the grid: the operator, the set it runs over and its text.
type numericEqualityQuery struct {
	Q   string `json:"q"`
	Set string `json:"set"`
	Op  string `json:"op"`
}

func numericEqualityQueries() []numericEqualityQuery {
	var out []numericEqualityQuery
	for _, s := range numericEqualitySets() {
		for _, lit := range numericEqualityLiterals[s.field] {
			for _, op := range []string{"==", "!=", "="} {
				if op == "=" && lit != numericEqualityLiterals[s.field][0] {
					continue
				}
				out = append(out, numericEqualityQuery{Q: `{app="` + s.app + `"} | ` + s.parser + ` | ` + s.field + ` ` + op + ` ` + lit, Set: s.app, Op: op})
			}
		}
	}
	return out
}

// numericEqualityAnswer is the recorded answer for one query: the lines (by index) Loki keeps and the ones it keeps
// with an __error__ label, and for each VictoriaLogs version the text of each side with the lines it returns.
type numericEqualityAnswer struct {
	numericEqualityQuery
	Loki    []int                            `json:"loki"`
	LokiErr []int                            `json:"loki_err,omitempty"`
	VL      map[string]numericEqualitySentVL `json:"vl"`
}

type numericEqualitySentVL struct {
	Plan     string `json:"plan"`
	PlanRows []int  `json:"plan_rows"`
	// PlanClone is whether the handler's clones of the plan's text (print and parse again) all read it back, so the
	// text cannot restart VictoriaLogs (the oracle's roundtrip command).
	PlanClone      bool   `json:"plan_clone"`
	Translator     string `json:"translator"`
	TranslatorRows []int  `json:"translator_rows"`
}

const numericEqualityAnswers = "testdata/numeric-equality-after-parser.jsonl"

// planOfTB is planOf for a test or a benchmark.
func planOfTB(t testing.TB, q string) *queryplan.Plan {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	queryplan.Resolve(p)
	return p
}

// numericEqualitySentTexts are the texts the proxy would send for q under the plain profile of a version: the
// lowering's and the translator's.
func numericEqualitySentTexts(t testing.TB, q, version string) (plan, legacy string) {
	t.Helper()
	caps := logsql.CapabilitiesFor(version)
	op, err := PlanQuery(q, planOfTB(t, q), Profile{Caps: caps})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	plan = op.(physical.VLQuery).Text
	text, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, caps)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if op, err := translator.DecodePhysical(text); err == nil {
		if v, ok := op.(physical.VLQuery); ok {
			text = v.Text
		}
	}
	return plan, text
}

// numericEqualityKind is the kind of value a field holds in the grid: number, duration or bytes.
func numericEqualityKind(field string) string {
	switch field {
	case "d":
		return "duration"
	case "b":
		return "bytes"
	}
	return "number"
}

// numericEqualityDifferences are the values the lowering's text keeps or drops unlike Loki, with the reason. A value
// is listed once, whichever literal or operator shows it. The answers are held to this list both ways: a difference
// that is not listed fails, and so does a listed value that no longer differs.
var numericEqualityDifferences = map[string]string{
	// The value space VictoriaLogs reads in one: a number, a duration and a size are all numbers to it, and Loki marks
	// a value of another kind with __error__ (the order comparisons share this).
	"number 1KB": "a size read as a number", "number 1K": "a size read as a number", "number 1000B": "a size read as a number",
	"number 1µs": "a duration read as a number", "number 1000ns": "a duration read as a number", "number 0s": "a duration read as a number",
	"duration 1GB": "a size read as a duration", "duration 1G": "a size read as a duration", "duration 1000000000": "a number read as a duration",
	"duration 100000000": "a number read as a duration", "duration 5000000000": "a number read as a duration",
	"bytes 1µs": "a duration read as a size", "bytes 1000ns": "a duration read as a size", "bytes 10ns": "a duration read as a size",
	// Values VictoriaLogs reads as a number that Loki marks with __error__.
	"number 0xC8": "hexadecimal integer", "number 0b11001000": "binary integer", "number 0o310": "octal integer",
	"bytes 1_000": "a size with a separator", "bytes 0x3e8": "a hexadecimal size", "bytes 1e3": "an exponent in a size",
	// Spellings Loki reads that VictoriaLogs does not.
	"duration 5000000us": "microseconds written us", "duration 100000us": "microseconds written us",
	"duration 5000000μs": "microseconds written with the Greek mu", "duration +5s": "a signed duration",
	"duration 05s": "a duration with a leading zero",
	"bytes 1 KB":   "a size with a space", "bytes 1kb": "a size in lower case", "bytes 1Kb": "a size in mixed case", "bytes 1kB": "a size in mixed case",
	"bytes 1k": "a size in lower case", "bytes 1000b": "a size in lower case", "bytes 10b": "a size in lower case", "bytes 1KB ": "a size with a trailing space",
	"bytes 1,000": "a size with a thousands separator", "bytes 001KB": "a size with a leading zero", "bytes 1.5 MiB": "a size with a space",
	"bytes 2 KB": "a size with a space",
	// A present but empty value: Loki keeps it with __error__ under != in a json line, VictoriaLogs has no such field.
	"number ": "an empty json value", "duration ": "an empty json value", "bytes ": "an empty json value",
}

// The lowering of a numeric equality keeps the lines Loki keeps for every value of the grid that is not in
// numericEqualityDifferences, on v1.49.0 and v1.53.0: Loki reads the value as a number, a Go duration or a bytes
// size and compares it with the literal as numbers, and the lowering writes the pair of range filters VictoriaLogs
// compares as numbers. A line Loki keeps with __error__ (a value that is none of these) is dropped under = and ==,
// the numeric label filter's open difference (semantics/label-filter-numeric-error-label), and kept under !=, as Loki
// keeps it. The string translator keeps other lines: the recorded answers count how many.
//
// conformance: semantics/label-filter-numeric-equality
func TestNumericEqualityAfterParserMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordNumericEqualityAnswers(t)
	}
	answers := readNumericEqualityAnswers(t)
	queries := numericEqualityQueries()
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: LVP_RUN_ORACLES=1 records them again", len(answers), len(queries))
	}
	sets := map[string]numericEqualitySet{}
	for _, s := range numericEqualitySets() {
		sets[s.app] = s
	}
	seen := map[string]bool{}
	planWrong, translatorWrong, checked := 0, 0, 0
	for i, q := range queries {
		a := answers[i]
		if a.Q != q.Q {
			t.Fatalf("answer %d is for %s, not %s: record them again", i, a.Q, q.Q)
		}
		s := sets[q.Set]
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			got := a.VL[v]
			plan, legacy := numericEqualitySentTexts(t, q.Q, v)
			if plan != got.Plan || legacy != got.Translator {
				t.Errorf("%s: the proxy writes %q and %q, the answer was recorded for %q and %q: record them again", q.Q, plan, legacy, got.Plan, got.Translator)
				continue
			}
			if !got.PlanClone {
				t.Errorf("%s on %s: VictoriaLogs cannot read back the clone of %s", q.Q, v, got.Plan)
			}
			for idx := 0; idx <= len(s.values); idx++ {
				value := "(missing key)"
				if idx < len(s.values) {
					value = s.values[idx]
				}
				// What Loki's pipeline does with the line, for the plan to match: = and == keep the lines that
				// compare equal (a line kept with __error__ is not one), != keeps the others including those.
				kept := slices.Contains(a.Loki, idx)
				want := kept && !slices.Contains(a.LokiErr, idx)
				if q.Op == "!=" {
					want = kept
				}
				checked++
				if slices.Contains(got.PlanRows, idx) != want {
					planWrong++
					key := numericEqualityKind(s.field) + " " + strings.TrimPrefix(value, "#")
					_, listed := numericEqualityDifferences[key]
					if !listed {
						t.Errorf("%s on %s, line %d (%s): Loki keeps %v (error %v), VictoriaLogs %s returns %v for %s",
							q.Q, v, idx, value, kept, slices.Contains(a.LokiErr, idx), v, slices.Contains(got.PlanRows, idx), got.Plan)
					}
					seen[key] = true
				}
				if slices.Contains(got.TranslatorRows, idx) != want {
					translatorWrong++
				}
			}
		}
	}
	for v, reason := range numericEqualityDifferences {
		if !seen[v] {
			t.Errorf("%q (%s) is listed as a difference, but the lowering keeps the lines Loki keeps for it: drop it from the list", v, reason)
		}
	}
	t.Logf("%d queries x values x 2 versions = %d line decisions: the lowering differs from Loki on %d, the string translator on %d",
		len(queries), checked, planWrong, translatorWrong)
	if translatorWrong <= planWrong {
		t.Errorf("the string translator is as close to Loki as the lowering (%d, %d differences): the case is fixed or the grid proves nothing", translatorWrong, planWrong)
	}
}

func readNumericEqualityAnswers(t *testing.T) []numericEqualityAnswer {
	t.Helper()
	f, err := os.Open(numericEqualityAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []numericEqualityAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a numericEqualityAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

// numericEqualityClones runs the plan's text of each query through the VictoriaLogs oracle's roundtrip command (the
// steps of the /select/logsql/query handler and every clone on the way) and returns whether each text was accepted and
// read back by every clone.
func numericEqualityClones(t *testing.T, oracleDir, modfile string, queries []numericEqualityQuery, version string) []bool {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(oracleDir, modfile, work, "vl")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	var b strings.Builder
	for i, q := range queries {
		plan, _ := numericEqualitySentTexts(t, q.Q, version)
		line, _ := json.Marshal(map[string]any{"id": strconv.Itoa(i), "q": plan})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	ok := make([]bool, len(queries))
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var v struct {
			ID     string `json:"id"`
			Accept bool   `json:"accept"`
			OK     bool   `json:"ok"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(v.ID)
		ok[i] = v.Accept && v.OK
	}
	return ok
}

func recordNumericEqualityAnswers(t *testing.T) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	sets := map[string]numericEqualitySet{}
	for _, s := range numericEqualitySets() {
		sets[s.app] = s
	}
	queries := numericEqualityQueries()
	var pc []oraclerun.ProcessCase
	for _, q := range queries {
		s := sets[q.Set]
		for _, line := range numericEqualityLines(s) {
			labels := map[string]string{"app": s.app}
			for k, v := range s.stream {
				labels[k] = v
			}
			pc = append(pc, oraclerun.ProcessCase{Query: q.Q, Labels: labels, Line: line})
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	answers := make([]numericEqualityAnswer, len(queries))
	k := 0
	for i, q := range queries {
		answers[i] = numericEqualityAnswer{numericEqualityQuery: q, Loki: []int{}, VL: map[string]numericEqualitySentVL{}}
		for idx := range numericEqualityLines(sets[q.Set]) {
			l := loki[k]
			k++
			if l.Error != "" {
				t.Fatalf("%s: Loki rejects it: %s", q.Q, l.Error)
			}
			if l.Keep {
				answers[i].Loki = append(answers[i].Loki, idx)
				if l.Labels["__error__"] != "" {
					answers[i].LokiErr = append(answers[i].LokiErr, idx)
				}
			}
		}
	}
	var rows []oraclerun.StoreRow
	rowOf := map[string]int{} // app + line -> index in the set
	for _, s := range numericEqualitySets() {
		for idx, line := range numericEqualityLines(s) {
			stream := [][2]string{{"app", s.app}}
			for k, v := range s.stream {
				stream = append(stream, [2]string{k, v})
			}
			rows = append(rows, oraclerun.StoreRow{Stream: stream, Fields: [][2]string{{"_msg", line}}})
			rowOf[s.app+"\x00"+line] = idx
		}
	}
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		modfile := ""
		if v == "v1.49.0" {
			modfile = "go.v1.49.0.mod"
		}
		var texts []string
		for _, q := range queries {
			plan, legacy := numericEqualitySentTexts(t, q.Q, v)
			texts = append(texts, plan, legacy)
		}
		got, err := oraclerun.VLStore(filepath.Join(root, "test", "oracle", "vl"), modfile, rows, texts)
		if err != nil {
			t.Fatal(err)
		}
		clones := numericEqualityClones(t, filepath.Join(root, "test", "oracle", "vl"), modfile, queries, v)
		for i, q := range queries {
			plan, legacy := numericEqualitySentTexts(t, q.Q, v)
			s := numericEqualitySentVL{Plan: plan, Translator: legacy, PlanClone: clones[i]}
			for j, text := range []string{plan, legacy} {
				a := got[2*i+j]
				if a.Error != "" {
					t.Fatalf("%s on %s: VictoriaLogs rejects %s: %s", q.Q, v, text, a.Error)
				}
				idxs := []int{}
				for _, r := range a.Rows {
					for _, f := range r {
						if line, ok := strings.CutPrefix(f, "_msg="); ok {
							idxs = append(idxs, rowOf[q.Set+"\x00"+line])
						}
					}
				}
				sort.Ints(idxs)
				if j == 0 {
					s.PlanRows = idxs
				} else {
					s.TranslatorRows = idxs
				}
			}
			answers[i].VL[v] = s
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
	if err := os.WriteFile(numericEqualityAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
