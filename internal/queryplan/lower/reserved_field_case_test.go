package lower

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// reservedFieldCaseRows are the lines of the reserved field case grid: each is a stream (app and a row label naming
// it, maybe one stream label more), its structured metadata and its line, pushed the way the Loki push API hands
// them to VictoriaLogs (stream labels, then structured metadata, then _msg).
var reservedFieldCaseRows = []struct {
	app    string
	stream map[string]string
	meta   map[string]string
	line   string
}{
	{"u", nil, nil, `_TIME=10 k=1`},
	{"u", nil, nil, `_TIME=3.5 k=2`},
	{"u", nil, nil, `_TIME=7 k=3`},
	{"j", nil, nil, `{"_TIME":"4","k":"1"}`},
	{"j", nil, nil, `{"_TIME":"6.5","k":"2"}`},
	{"f", nil, nil, `_Stream_ID=si k=1`},
	{"f", nil, nil, `_Stream_ID=other k=2`},
	{"f", nil, nil, `x=1`},
	{"f", map[string]string{"_Stream_ID": "si"}, nil, `plain`},
	{"f", nil, map[string]string{"_Stream_ID": "si"}, `metadata`},
}

// reservedFieldCaseQueries are the log and metric queries on _TIME and _Stream_ID the grid answers: unwraps of a
// logfmt and a json key, comparisons after a parser, and label filters with and without a parser.
var reservedFieldCaseQueries = []string{
	`{app="f"} | _Stream_ID="si"`, `{app="f"} | _Stream_ID!="si"`, `{app="f"} | _Stream_ID=~"s.*"`,
	`{app="f"} | logfmt | _Stream_ID="si"`, `{app="f"} | logfmt | _Stream_ID!="si"`, `{app="u"} | logfmt | _TIME>5`,
	`{app="j"} | json | _TIME<5`,
	`sum(sum_over_time({app="u"} | logfmt | unwrap _TIME [1m]))`, `sum(sum_over_time({app="j"} | json | unwrap _TIME [1m]))`,
	`sum(max_over_time({app="u"} | logfmt | unwrap _TIME [1m]))`, `sum(count_over_time({app="f"} | logfmt | _Stream_ID="si" [1m]))`,
	`sum(count_over_time({app="f"} | _Stream_ID="si" [1m]))`,
}

const reservedFieldCaseAnswers = "testdata/reserved-field-case.jsonl"

// reservedFieldCaseAnswer is the recorded answer for one query: Loki's (the rows its pipeline keeps, or for a metric
// the value of its one series, summed over the kept rows by the test) and, for each VictoriaLogs version, the text
// the proxy sends (the lowering's or the translator's, through the outbound guard) with VictoriaLogs' answer to it.
type reservedFieldCaseAnswer struct {
	Query string                      `json:"q"`
	Loki  []int                       `json:"loki"`
	Value *float64                    `json:"loki_value,omitempty"`
	VL    map[string]reservedCaseSent `json:"vl"`
}

type reservedCaseSent struct {
	Plan       string   `json:"plan,omitempty"`
	PlanRows   []int    `json:"plan_rows,omitempty"`
	PlanValue  *float64 `json:"plan_value,omitempty"`
	Translator string   `json:"translator"`
	Rows       []int    `json:"rows,omitempty"`
	Value      *float64 `json:"value,omitempty"`
	Error      string   `json:"error,omitempty"`
}

func reservedCaseLabels(i int) map[string]string {
	r := reservedFieldCaseRows[i]
	l := map[string]string{"app": r.app, "row": strconv.Itoa(i)}
	for k, v := range r.stream {
		l[k] = v
	}
	return l
}

// reservedCaseSentTexts are the texts the proxy sends for q under the plain profile of a version: the lowering's and
// the translator's, each through the outbound guard ("" where one does not write q).
func reservedCaseSentTexts(t *testing.T, q, version string) (plan, legacy string) {
	t.Helper()
	caps := logsql.CapabilitiesFor(version)
	if p := rangeAggPlan(q); p != nil {
		if op, err := PlanQuery(q, p, Profile{Caps: caps}); err == nil {
			if v, ok := op.(physical.VLQuery); ok {
				plan = logsql.GuardReservedFieldCase(v.Text)
			}
		}
	}
	text, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), nil, nil, caps)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if op, err := translator.DecodePhysical(text); err == nil {
		if v, ok := op.(physical.VLQuery); ok {
			text = v.Text
		}
	}
	return plan, logsql.GuardReservedFieldCase(text)
}

// The proxy answers a log or metric query on a label named _TIME or _Stream_ID like Loki: for every query of the
// grid, the rows Loki's pipeline keeps (the Loki oracle, process, with the selector's matchers), or the value of the
// metric summed over them, are what VictoriaLogs (the VictoriaLogs oracle, store: the rows stored as the Loki push
// API stores them, v1.49.0 and v1.53.0) returns for the text the proxy sends, the translator's and the lowering's
// alike through the outbound guard. Without the guard these texts filter the quoted field, which VictoriaLogs
// cannot read back from its own print (an unwrap), or name the field bare, which it reads as its own filter (a label
// filter). The answers are recorded; LVP_RUN_ORACLES=1 records them again.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestReservedFieldCaseMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordReservedFieldCaseAnswers(t)
	}
	answers := readReservedFieldCaseAnswers(t)
	if len(answers) != len(reservedFieldCaseQueries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(reservedFieldCaseQueries))
	}
	for i, q := range reservedFieldCaseQueries {
		a := answers[i]
		if a.Query != q {
			t.Fatalf("answer %d is for %s, not %s: record them again", i, a.Query, q)
		}
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			got := a.VL[v]
			plan, legacy := reservedCaseSentTexts(t, q, v)
			if plan != got.Plan || legacy != got.Translator {
				t.Errorf("%s: the proxy sends %q and %q, the answer was recorded for %q and %q: record them again", q, plan, legacy, got.Plan, got.Translator)
				continue
			}
			if got.Error != "" {
				t.Errorf("%s: VictoriaLogs %s: %s", q, v, got.Error)
				continue
			}
			if a.Value != nil {
				if got.Value == nil || math.Abs(*got.Value-*a.Value) > 1e-9 || got.Plan != "" && (got.PlanValue == nil || math.Abs(*got.PlanValue-*a.Value) > 1e-9) {
					t.Errorf("%s: Loki %v, VictoriaLogs %s translator %v plan %v", q, *a.Value, v, deref(got.Value), deref(got.PlanValue))
				}
				continue
			}
			if !slices.Equal(got.Rows, a.Loki) || got.Plan != "" && !slices.Equal(got.PlanRows, a.Loki) {
				t.Errorf("%s: Loki keeps %v, VictoriaLogs %s returns %v (translator) and %v (plan)", q, a.Loki, v, got.Rows, got.PlanRows)
			}
		}
	}
}

func readReservedFieldCaseAnswers(t *testing.T) []reservedFieldCaseAnswer {
	t.Helper()
	f, err := os.Open(reservedFieldCaseAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []reservedFieldCaseAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a reservedFieldCaseAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

// lokiMetricValue is the value of the query's one series over the rows Loki keeps. Every row of the grid is a stream
// of its own (its row label), so a range function reads one sample per series, and the outer sum adds them: the
// unwrapped value for sum_over_time and max_over_time, 1 for count_over_time.
func lokiMetricValue(q string, kept []map[string]string) *float64 {
	var v float64
	for _, l := range kept {
		if strings.Contains(q, "count_over_time") {
			v++
			continue
		}
		x, _ := strconv.ParseFloat(l["_TIME"], 64)
		v += x
	}
	return &v
}

// lokiLogQuery is the log query of a metric query (its range aggregation's selector and pipeline).
func lokiLogQuery(q string) string {
	open := strings.Index(q, "{")
	end := strings.LastIndex(q, " [1m]")
	if !strings.Contains(q, "_over_time(") || end < 0 {
		return q
	}
	inner := q[open:end]
	return strings.Replace(inner, " | unwrap _TIME", "", 1)
}

func recordReservedFieldCaseAnswers(t *testing.T) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	var pc []oraclerun.ProcessCase
	for _, q := range reservedFieldCaseQueries {
		for i, r := range reservedFieldCaseRows {
			labels := reservedCaseLabels(i)
			for k, v := range r.meta {
				labels[k] = v
			}
			pc = append(pc, oraclerun.ProcessCase{Query: lokiLogQuery(q), Labels: labels, Line: r.line})
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]oraclerun.StoreRow, len(reservedFieldCaseRows))
	for i, r := range reservedFieldCaseRows {
		rows[i].Stream = [][2]string{{"app", r.app}, {"row", strconv.Itoa(i)}}
		for k, v := range r.stream {
			rows[i].Stream = append(rows[i].Stream, [2]string{k, v})
		}
		for k, v := range r.meta {
			rows[i].Fields = append(rows[i].Fields, [2]string{k, v})
		}
		rows[i].Fields = append(rows[i].Fields, [2]string{"_msg", r.line})
	}
	answers := make([]reservedFieldCaseAnswer, len(reservedFieldCaseQueries))
	for i, q := range reservedFieldCaseQueries {
		answers[i] = reservedFieldCaseAnswer{Query: q, Loki: []int{}, VL: map[string]reservedCaseSent{}}
		var kept []map[string]string
		for j := range reservedFieldCaseRows {
			l := loki[i*len(reservedFieldCaseRows)+j]
			if l.Error != "" {
				t.Fatalf("%s: Loki rejects it: %s", q, l.Error)
			}
			if l.Selected && l.Keep && l.Labels["__error__"] == "" {
				answers[i].Loki = append(answers[i].Loki, j)
				kept = append(kept, l.Labels)
			}
		}
		if lokiLogQuery(q) != q {
			answers[i].Value = lokiMetricValue(q, kept)
		}
	}
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		modfile := ""
		if v == "v1.49.0" {
			modfile = "go.v1.49.0.mod"
		}
		var texts []string
		for _, q := range reservedFieldCaseQueries {
			plan, legacy := reservedCaseSentTexts(t, q, v)
			texts = append(texts, legacy)
			if plan != "" {
				texts = append(texts, plan)
			}
		}
		got, err := oraclerun.VLStore(filepath.Join(root, "test", "oracle", "vl"), modfile, rows, texts)
		if err != nil {
			t.Fatal(err)
		}
		k := 0
		for i, q := range reservedFieldCaseQueries {
			plan, legacy := reservedCaseSentTexts(t, q, v)
			s := reservedCaseSent{Translator: legacy, Plan: plan}
			s.Rows, s.Value, s.Error = storeAnswer(got[k], answers[i].Value != nil)
			k++
			if plan != "" {
				var perr string
				s.PlanRows, s.PlanValue, perr = storeAnswer(got[k], answers[i].Value != nil)
				if perr != "" && s.Error == "" {
					s.Error = "plan: " + perr
				}
				k++
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
	if err := os.WriteFile(reservedFieldCaseAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// storeAnswer is the rows (row label values) or, for a metric, the value of the one output row's one field that is
// not a group VictoriaLogs' answer holds.
func storeAnswer(a oraclerun.StoreAnswer, metric bool) (rows []int, value *float64, errText string) {
	if a.Error != "" {
		return nil, nil, a.Error
	}
	if metric {
		var total float64
		for _, r := range a.Rows {
			for _, f := range r {
				name, val, _ := strings.Cut(f, "=")
				switch name {
				case "_stream", "_stream_id", "_msg", "_time", "app", "row", "level", "detected_level":
					continue
				}
				if x, err := strconv.ParseFloat(val, 64); err == nil {
					total += x
				}
			}
		}
		return nil, &total, ""
	}
	rows = []int{}
	for _, r := range a.Rows {
		for _, f := range r {
			if n, ok := strings.CutPrefix(f, "row="); ok {
				k, _ := strconv.Atoi(n)
				rows = append(rows, k)
			}
		}
	}
	sort.Ints(rows)
	return rows, nil, ""
}

func deref(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}
