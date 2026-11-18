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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// unspacedParserQueries are log queries with a regexp or pattern parser written with no blank before its expression,
// each with the same query written with the blank. VictoriaLogs has no pattern or regexp pipe: the string translator
// copies the unspaced parser as written (`| pattern"<_> <m>"`), which VictoriaLogs rejects, where it writes the spaced
// one as an extract pipe. The query plan reads both spellings as one parser and writes the extract pipe.
var unspacedParserQueries = [][2]string{
	{`{app="r"} |pattern"<_> <m> <_>"`, `{app="r"} | pattern "<_> <m> <_>"`},
	{`{app="r"} |pattern"<_> <m> <_>" | m="GET"`, `{app="r"} | pattern "<_> <m> <_>" | m="GET"`},
	{"{app=\"r\"} |pattern`<_> <m> <p>` | p=~\"/a.*\"", "{app=\"r\"} | pattern `<_> <m> <p>` | p=~\"/a.*\""},
	{`{app="r"} |= "GET" |pattern"<_> <m> <_>" | m!="POST"`, `{app="r"} |= "GET" | pattern "<_> <m> <_>" | m!="POST"`},
	{`{app="r"} |regexp"(?P<m>[A-Z]+) " | m="GET"`, `{app="r"} | regexp "(?P<m>[A-Z]+) " | m="GET"`},
	{"{app=\"r\"} |regexp`(?P<m>[A-Z]+) (?P<p>\\S+)` | p=\"/b\"", "{app=\"r\"} | regexp `(?P<m>[A-Z]+) (?P<p>\\S+)` | p=\"/b\""},
}

// unspacedParserLines are the lines of the streams the queries run over, one stream each (app="r", row=<index>).
var unspacedParserLines = []string{"1 GET /a", "2 POST /b", "3 GET /c", "x"}

// unspacedParserLabels are the labels the parsers extract, compared between Loki and VictoriaLogs.
var unspacedParserLabels = []string{"m", "p"}

const unspacedParserAnswers = "testdata/unspaced-parser-loki-vl.jsonl"

// unspacedAnswer is the recorded answer for one pair: for each spelling, the rows Loki keeps with the labels it
// extracts (row index to label=value pairs); for each VictoriaLogs version, the plan's text for the unspaced spelling
// and VictoriaLogs' rows for it; and the string translator's text for the unspaced spelling with VictoriaLogs'
// (v1.54.0) answer to it.
type unspacedAnswer struct {
	Unspaced   string                      `json:"unspaced"`
	Spaced     string                      `json:"spaced"`
	LokiBoth   [2]map[string][]string      `json:"loki"`
	VL         map[string]unspacedVLAnswer `json:"vl"`
	Translator unspacedVLAnswer            `json:"translator"`
}

type unspacedVLAnswer struct {
	Text  string              `json:"text"`
	Rows  map[string][]string `json:"rows,omitempty"`
	Error string              `json:"error,omitempty"`
}

// unspacedLowered is the plan's text for q under the plain profile of a VictoriaLogs version.
func unspacedLowered(t *testing.T, q, version string) string {
	t.Helper()
	return reservedLowered(t, q, version)
}

// A regexp or pattern parser written with no blank before its expression is the same parser to Loki: its pipeline
// (the Loki oracle, process) keeps the same lines with the same extracted labels for both spellings. The query plan
// writes both spellings as the extract pipe the string translator writes for the spaced one, and VictoriaLogs (the
// VictoriaLogs oracle, store, v1.49.0 and v1.54.0) returns Loki's lines and labels for it; the string translator copies
// the unspaced parser as written, which VictoriaLogs rejects. The answers are recorded; LVP_RUN_ORACLES=1 records them
// again (the VictoriaLogs oracle fetches VictoriaLogs; set GOMODCACHE to a fresh directory when the shared cache holds
// other copies).
//
// conformance: semantics/translator-copies-unspaced-parser
func TestUnspacedParserMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordUnspacedParserAnswers(t)
	}
	answers := readUnspacedParserAnswers(t)
	if len(answers) != len(unspacedParserQueries) {
		t.Fatalf("%d recorded answers for %d pairs: record them again with LVP_RUN_ORACLES=1", len(answers), len(unspacedParserQueries))
	}
	for i, pair := range unspacedParserQueries {
		a := answers[i]
		if a.Unspaced != pair[0] || a.Spaced != pair[1] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Unspaced, pair[0])
		}
		// Loki reads the two spellings as one query.
		if !equalRows(a.LokiBoth[0], a.LokiBoth[1]) {
			t.Errorf("%s: Loki keeps %v, and %v for %s", pair[0], a.LokiBoth[0], a.LokiBoth[1], pair[1])
		}
		if len(a.LokiBoth[0]) == 0 {
			t.Errorf("%s: Loki keeps no line: the case tests nothing", pair[0])
		}
		for _, v := range reservedFieldVersions {
			got := a.VL[v]
			text := unspacedLowered(t, pair[0], v)
			if text != got.Text {
				t.Errorf("%s: the plan writes %s, the answer was recorded for %s: record them again with LVP_RUN_ORACLES=1", pair[0], text, got.Text)
				continue
			}
			// The plan writes the spaced spelling's text, which the string translator writes too: the query is served as
			// its spaced spelling is.
			if spaced := unspacedLowered(t, pair[1], v); spaced != text {
				t.Errorf("%s: the plan writes %s, and %s for %s", pair[0], text, spaced, pair[1])
			}
			if legacy, err := translator.TranslateLogQLWithCapabilities(pair[1], nil, nil, logsql.CapabilitiesFor(v)); err != nil || legacy != text {
				t.Errorf("%s: the string translator writes %q (%v), the plan %s", pair[1], legacy, err, text)
			}
			if got.Error != "" {
				t.Errorf("%s: VictoriaLogs %s rejects %s: %s", pair[0], v, got.Text, got.Error)
				continue
			}
			if !equalRows(got.Rows, a.LokiBoth[0]) {
				t.Errorf("%s: Loki keeps %v, VictoriaLogs %s returns %v for %s", pair[0], a.LokiBoth[0], v, got.Rows, got.Text)
			}
		}
		if legacy, err := translator.TranslateLogQL(pair[0]); err != nil || legacy != a.Translator.Text {
			t.Errorf("%s: the string translator writes %q (%v), the answer was recorded for %q", pair[0], legacy, err, a.Translator.Text)
		}
		if a.Translator.Error == "" {
			t.Errorf("%s: VictoriaLogs accepts the string translator's %s: the case is fixed", pair[0], a.Translator.Text)
		}
	}
}

// equalRows reports two answers that keep the same rows with the same labels.
func equalRows(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !slices.Equal(v, w) {
			return false
		}
	}
	return true
}

func readUnspacedParserAnswers(t *testing.T) []unspacedAnswer {
	t.Helper()
	f, err := os.Open(unspacedParserAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []unspacedAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a unspacedAnswer
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

// extractedPairs are the label=value pairs of the parsers' labels in a set of labels or fields, sorted.
func extractedPairs(get func(string) string) []string {
	out := []string{}
	for _, l := range unspacedParserLabels {
		if v := get(l); v != "" {
			out = append(out, l+"="+v)
		}
	}
	sort.Strings(out)
	return out
}

func recordUnspacedParserAnswers(t *testing.T) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	var pc []oraclerun.ProcessCase
	for _, pair := range unspacedParserQueries {
		for _, q := range pair {
			for i, line := range unspacedParserLines {
				pc = append(pc, oraclerun.ProcessCase{Query: q, Labels: map[string]string{"app": "r", "row": strconv.Itoa(i)}, Line: line})
			}
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	answers := make([]unspacedAnswer, len(unspacedParserQueries))
	k := 0
	for i, pair := range unspacedParserQueries {
		answers[i] = unspacedAnswer{Unspaced: pair[0], Spaced: pair[1], VL: map[string]unspacedVLAnswer{}}
		for s, q := range pair {
			kept := map[string][]string{}
			for j := range unspacedParserLines {
				l := loki[k]
				k++
				if l.Error != "" {
					t.Fatalf("%s: Loki rejects it: %s", q, l.Error)
				}
				if l.Selected && l.Keep {
					kept[strconv.Itoa(j)] = extractedPairs(func(n string) string { return l.Labels[n] })
				}
			}
			answers[i].LokiBoth[s] = kept
		}
	}
	rows := make([]oraclerun.StoreRow, len(unspacedParserLines))
	for i, line := range unspacedParserLines {
		rows[i].Stream = [][2]string{{"app", "r"}, {"row", strconv.Itoa(i)}}
		rows[i].Fields = [][2]string{{"_msg", line}}
	}
	for _, v := range reservedFieldVersions {
		modfile := ""
		last := v == reservedFieldVersions[len(reservedFieldVersions)-1]
		if !last {
			modfile = "go." + v + ".mod"
		}
		var texts []string
		for _, pair := range unspacedParserQueries {
			texts = append(texts, unspacedLowered(t, pair[0], v))
		}
		if last {
			for _, pair := range unspacedParserQueries {
				legacy, err := translator.TranslateLogQL(pair[0])
				if err != nil {
					t.Fatalf("%s: %v", pair[0], err)
				}
				texts = append(texts, legacy)
			}
		}
		got, err := oraclerun.VLStore(filepath.Join(root, "test", "oracle", "vl"), modfile, rows, texts)
		if err != nil {
			t.Fatal(err)
		}
		for i, g := range got {
			a := unspacedVLAnswer{Text: texts[i], Error: g.Error}
			if g.Error == "" {
				a.Rows = map[string][]string{}
				for _, r := range g.Rows {
					fields := map[string]string{}
					for _, f := range r {
						if n, val, ok := strings.Cut(f, "="); ok {
							fields[n] = val
						}
					}
					a.Rows[fields["row"]] = extractedPairs(func(n string) string { return fields[n] })
				}
			}
			if i < len(unspacedParserQueries) {
				answers[i].VL[v] = a
			} else {
				answers[i-len(unspacedParserQueries)].Translator = a
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
	if err := os.WriteFile(unspacedParserAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
