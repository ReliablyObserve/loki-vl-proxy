package lower

import (
	"bufio"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The oracle of a filter on detected_level after a stage: for each query, the entries Loki's pipeline keeps (the
// Loki oracle, process, over every fixture entry with the level Loki attaches at ingest as structured metadata) and
// the entries VictoriaLogs returns for the lowering's text and for the string translator's (the VictoriaLogs oracle,
// store, on v1.49.0 and v1.54.0, over the same entries as an ingestion route stores them: no detected_level field of
// their own). A metric query compares the counts per detected_level. LVP_RUN_ORACLES=1 records the answers again.

// levelFixture is one entry: its stream labels besides app, its structured metadata, its line, and the level Loki
// attached to it at ingest (and the detected_level_extracted it added), recorded from a real Loki by
// test/e2e-compat TestCompat_LevelAfterStageIngestLevels.
type levelFixture struct {
	Name      string            `json:"name"`
	Stream    map[string]string `json:"stream"`
	Fields    map[string]string `json:"fields"`
	Line      string            `json:"line"`
	Level     string            `json:"level"`
	Extracted string            `json:"extracted"`
}

var levelFixtures = func() (out []levelFixture) {
	b, err := os.ReadFile("testdata/level-after-stage-entries.json")
	if err == nil {
		err = json.Unmarshal(b, &out)
	}
	if err != nil {
		panic(err)
	}
	return out
}()

// lokiInputs are the stream labels and structured metadata Loki holds for the entry: its own, plus the level at
// ingest, which a stream label of the name holds itself.
func (f levelFixture) lokiInputs() (labels, metadata map[string]string) {
	labels, metadata = map[string]string{"app": "g"}, map[string]string{}
	maps.Copy(labels, f.Stream)
	maps.Copy(metadata, f.Fields)
	if _, ok := f.Stream["detected_level"]; !ok {
		metadata["detected_level"] = f.Level
	} else if f.Extracted != "" {
		metadata["detected_level_extracted"] = f.Extracted
	}
	return labels, metadata
}

// levelAfterStageQueries are the log queries (the six corpus shapes' stages among them) and, after them, the metric
// queries; %s is the selector.
var levelAfterStageQueries = func() (out []string) {
	stages := []string{
		`json`, `logfmt`, `json a="a"`, `logfmt a="a"`, `json detected_level="a"`, "regexp `a=(?P<detected_level>\\d)`",
		"regexp `(?P<n>\\d)`", `pattern "<_> a=<n>"`, `drop a | decolorize`, `decolorize`, `drop level`,
		`keep detected_level`, `keep detected_level, a`, `line_format "{{.a}} info"`, `label_format b=a`, `json | drop __error__`,
		`json | logfmt`, `decolorize | json | line_format "x"`,
	}
	filters := []string{`detected_level="error"`, `detected_level!="info"`, `detected_level=~"error|warn"`, `detected_level!~"unk.*"`, `detected_level=""`, `detected_level!=""`}
	for _, s := range stages {
		for _, f := range filters {
			out = append(out, `{app="g"} | `+s+` | `+f)
		}
	}
	out = append(out,
		`{app="g"} | json | detected_level="error" | a="1"`,
		`{app="g"} | detected_level!="info" | json | detected_level!="error"`,
		`{app="g"} | json | detected_level!="info" | logfmt | detected_level!="error"`,
		`{app="g"} | json | detected_level="error" | drop __error__`,
	)
	// A label a drop or a keep removed compares "".
	for _, s := range []string{`drop detected_level`, `drop a, detected_level`, `keep a`, `json | drop detected_level`, `json | keep a`} {
		for _, f := range []string{`detected_level="error"`, `detected_level!="error"`, `detected_level=""`, `detected_level!=""`, `detected_level=~".*"`, `detected_level!~"x?"`} {
			out = append(out, `{app="g"} | `+s+` | `+f)
		}
	}
	for _, s := range []string{`json`, `logfmt`, `drop a | decolorize`, `json | drop __error__`} {
		for _, f := range []string{`detected_level="error"`, `detected_level!="info"`} {
			q := `{app="g"} | ` + s + ` | ` + f + ` | drop __error__, __error_details__`
			out = append(out, `sum by (detected_level) (count_over_time(`+q+` [1h]))`, `count_over_time(`+q+` [1h])`)
		}
	}
	return out
}()

type levelAfterStageAnswer struct {
	Q string `json:"q"`
	// Loki is the entries (by index) Loki keeps, or, for a metric query, "level=count" per detected_level.
	Loki []string                     `json:"loki"`
	VL   map[string]levelAfterStageVL `json:"vl"`
}

type levelAfterStageVL struct {
	Plan           string   `json:"plan"`
	PlanRows       []string `json:"plan_rows"`
	Translator     string   `json:"translator"`
	TranslatorRows []string `json:"translator_rows"`
	TranslatorErr  string   `json:"translator_error,omitempty"`
}

const levelAfterStageAnswers = "testdata/level-filter-after-stage.jsonl"

func levelAfterStageSentTexts(t testing.TB, q, version string) (plan, legacy string) {
	t.Helper()
	caps := logsql.CapabilitiesFor(version)
	cq := logql.CanonicalizeLogRanges(q)
	op, err := PlanQuery(cq, planOfTB(t, cq), Profile{Caps: caps})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	vl := func(op physical.Op) string {
		switch v := op.(type) {
		case physical.VLQuery:
			return v.Text
		case physical.VLStats:
			return v.Text
		}
		t.Fatalf("%s: unexpected operator %T", q, op)
		return ""
	}
	text, err := translator.TranslateLogQLWithCapabilities(cq, nil, nil, caps)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	lop, err := translator.DecodePhysical(text)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return vl(op), vl(lop)
}

// A filter on detected_level after a stage keeps the entries Loki keeps, and a metric over it counts them per level
// as Loki does, on v1.49.0 and v1.54.0: Loki attaches the level at ingest, so the filter reads it behind a parser, a
// formatting stage or a drop of other labels, and compares "" once a drop or a keep removed it. The comparison is of
// the rows; the response's labels are not measured here.
//
// conformance: semantics/level-filter-after-stage
func TestLevelFilterAfterStageMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordLevelAfterStageAnswers(t)
	}
	f, err := os.Open(levelAfterStageAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var answers []levelAfterStageAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a levelAfterStageAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	if len(answers) != len(levelAfterStageQueries) {
		t.Fatalf("%d recorded answers for %d queries: LVP_RUN_ORACLES=1 records them again", len(answers), len(levelAfterStageQueries))
	}
	translatorWrong, checked := 0, 0
	for i, q := range levelAfterStageQueries {
		a := answers[i]
		if a.Q != q {
			t.Fatalf("answer %d is for %s, not %s: record them again", i, a.Q, q)
		}
		for _, v := range []string{"v1.49.0", "v1.54.0"} {
			got := a.VL[v]
			plan, legacy := levelAfterStageSentTexts(t, q, v)
			if plan != got.Plan || legacy != got.Translator {
				t.Errorf("%s: the proxy writes %q and %q, the answer was recorded for %q and %q: record them again", q, plan, legacy, got.Plan, got.Translator)
				continue
			}
			checked++
			if !slices.Equal(got.PlanRows, a.Loki) {
				t.Errorf("%s on %s: the plan answers %v, Loki %v\n%s", q, v, got.PlanRows, a.Loki, got.Plan)
			}
			if got.TranslatorErr != "" || !slices.Equal(got.TranslatorRows, a.Loki) {
				translatorWrong++
			}
		}
	}
	if translatorWrong == 0 {
		t.Error("the string translator answers every query as Loki does: the grid proves nothing")
	}
	t.Logf("%d query answers compared; the string translator differs from Loki on %d", checked, translatorWrong)
}

func recordLevelAfterStageAnswers(t *testing.T) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	var cases []oraclerun.ProcessCase
	for _, q := range levelAfterStageQueries {
		log, _ := levelLogQuery(q)
		for _, f := range levelFixtures {
			labels, metadata := f.lokiInputs()
			cases = append(cases, oraclerun.ProcessCase{Query: log, Labels: labels, Line: f.Line, Metadata: metadata, WithCategories: true})
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), cases)
	if err != nil {
		t.Fatal(err)
	}
	answers := make([]levelAfterStageAnswer, len(levelAfterStageQueries))
	k := 0
	for i, q := range levelAfterStageQueries {
		_, grouped := levelLogQuery(q)
		var kept []string
		counts := map[string]int{}
		for j := range levelFixtures {
			l := loki[k]
			k++
			if l.Error != "" {
				t.Fatalf("%s: Loki rejects it: %s", q, l.Error)
			}
			if !l.Keep {
				continue
			}
			kept = append(kept, strconv.Itoa(j))
			if grouped {
				counts[l.Labels["detected_level"]]++
			} else {
				counts[""]++
			}
		}
		if !strings.HasPrefix(q, "{") {
			kept = levelCounts(counts, grouped)
		}
		if kept == nil {
			kept = []string{}
		}
		answers[i] = levelAfterStageAnswer{Q: q, Loki: kept, VL: map[string]levelAfterStageVL{}}
	}
	var rows []oraclerun.StoreRow
	for j, f := range levelFixtures {
		row := oraclerun.StoreRow{Stream: [][2]string{{"app", "g"}, {"fx", fmt.Sprintf("%02d", j)}}}
		for _, k := range slices.Sorted(maps.Keys(f.Stream)) {
			row.Stream = append(row.Stream, [2]string{k, f.Stream[k]})
		}
		for _, k := range slices.Sorted(maps.Keys(f.Fields)) {
			row.Fields = append(row.Fields, [2]string{k, f.Fields[k]})
		}
		row.Fields = append(row.Fields, [2]string{"_msg", f.Line})
		rows = append(rows, row)
	}
	for _, v := range []string{"v1.49.0", "v1.54.0"} {
		modfile := ""
		if v == "v1.49.0" {
			modfile = "go.v1.49.0.mod"
		}
		var texts []string
		for _, q := range levelAfterStageQueries {
			plan, legacy := levelAfterStageSentTexts(t, q, v)
			texts = append(texts, plan, legacy)
		}
		got, err := oraclerun.VLStore(filepath.Join(root, "test", "oracle", "vl"), modfile, rows, texts)
		if err != nil {
			t.Fatal(err)
		}
		for i, q := range levelAfterStageQueries {
			plan, legacy := levelAfterStageSentTexts(t, q, v)
			a := levelAfterStageVL{Plan: plan, Translator: legacy}
			if e := got[2*i].Error; e != "" {
				t.Fatalf("%s on %s: VictoriaLogs rejects %s: %s", q, v, plan, e)
			}
			a.PlanRows = levelStoreAnswer(t, q, got[2*i].Rows)
			if a.TranslatorErr = got[2*i+1].Error; a.TranslatorErr == "" {
				a.TranslatorRows = levelStoreAnswer(t, q, got[2*i+1].Rows)
			}
			answers[i].VL[v] = a
		}
	}
	out, err := os.Create(levelAfterStageAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	for _, a := range answers {
		if err := enc.Encode(a); err != nil {
			t.Fatal(err)
		}
	}
}

// levelLogQuery is the log query of q and whether q groups its count by detected_level.
func levelLogQuery(q string) (log string, grouped bool) {
	if strings.HasPrefix(q, "{") {
		return q, false
	}
	return q[strings.Index(q, "{"):strings.LastIndex(q, " [1h]")], strings.HasPrefix(q, "sum by (detected_level)")
}

// levelCounts are the counts as sorted "level=count" entries; the total alone for an ungrouped query.
func levelCounts(counts map[string]int, grouped bool) []string {
	out := []string{}
	for _, level := range slices.Sorted(maps.Keys(counts)) {
		if grouped {
			out = append(out, level+"="+strconv.Itoa(counts[level]))
		} else {
			out = append(out, "="+strconv.Itoa(counts[level]))
		}
	}
	return out
}

// levelStoreAnswer reads VictoriaLogs' rows as Loki's answer is recorded: the entries by their stream's fx label,
// or the count of each detected_level (every group of an ungrouped count summed, as its Loki side counts entries).
func levelStoreAnswer(t *testing.T, q string, rows [][]string) []string {
	t.Helper()
	_, grouped := levelLogQuery(q)
	metric := !strings.HasPrefix(q, "{")
	ids, counts := []string{}, map[string]int{}
	for _, r := range rows {
		fields := map[string]string{}
		for _, kv := range r {
			name, value, _ := strings.Cut(kv, "=")
			fields[name] = value
		}
		if !metric {
			_, rest, ok := strings.Cut(fields["_stream"], `fx="`)
			if !ok {
				t.Fatalf("%s: a row with no fixture stream: %v", q, r)
			}
			j, _ := strconv.Atoi(rest[:2])
			ids = append(ids, strconv.Itoa(j))
			continue
		}
		n := 0
		for name, value := range fields {
			if strings.HasPrefix(name, "count(") || name == "c" {
				n, _ = strconv.Atoi(value)
			}
		}
		if n == 0 {
			t.Fatalf("%s: a stats row with no count: %v", q, r)
		}
		key := ""
		if grouped {
			key = fields["detected_level"]
		}
		counts[key] += n
	}
	if metric {
		return levelCounts(counts, grouped)
	}
	slices.SortFunc(ids, func(a, b string) int {
		x, _ := strconv.Atoi(a)
		y, _ := strconv.Atoi(b)
		return x - y
	})
	return ids
}
