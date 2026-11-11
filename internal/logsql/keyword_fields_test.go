package logsql

import (
	"bufio"
	"encoding/json"
	"os"
	"regexp"
	"testing"
)

// keywordFieldVerdicts are VictoriaLogs' own verdicts (its parser, lib/logstorage ParseQuery, on v1.49.0 and
// v1.53.0) on the field names that are keywords or filter functions of LogsQL, bare and quoted, at every place a
// filter or a pipe names a field. The grid is the evidence of KeywordFieldName: a bare not, in, contains_all,
// contains_any, contains_common_case or equals_common_case is rejected wherever a filter names a field; and and or
// are rejected right after another filter, where they are the operators; options is rejected as the first filter;
// every other name (i, exact, re, seq, range, offset, of, ...) is read as a field bare, every name is read quoted,
// and pipes read every name bare. Recorded with the VictoriaLogs oracle (test/oracle/vl check) when the grid was
// written: go test ./internal/logsql -run TestRecordKeywordFieldVerdicts with LVP_RUN_ORACLES=1 records them again.
const keywordFieldVerdicts = "testdata/keyword-field-names-vl.jsonl"

type keywordFieldVerdict struct {
	Query string `json:"q"`
	V149  bool   `json:"v1.49.0"`
	V153  bool   `json:"v1.53.0"`
}

func readKeywordFieldVerdicts(t *testing.T) []keywordFieldVerdict {
	t.Helper()
	f, err := os.Open(keywordFieldVerdicts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var rows []keywordFieldVerdict
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r keywordFieldVerdict
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// looserThanVL are the queries the proxy's LogsQL parser reads where VictoriaLogs rejects them: a bare in or filter
// function name as a field (the parser reads `in:="x"` as a field filter), an Exclude of a bare not, and options as
// the first filter. The proxy never writes them since the lowering quotes those names, and it reads back what it
// sends, so the difference is only in what it reads.
var looserThanVL = regexp.MustCompile(`(?i)(?:^|[ (-])(?:in|contains_all|contains_any|contains_common_case|equals_common_case):|-not:|^options:`)

// The proxy's LogsQL parser reads a query the way VictoriaLogs does for the words that are keywords there: it
// accepts every query VictoriaLogs accepts (and, or and not as pipe field names, and and or as field names where
// a filter begins), and rejects what VictoriaLogs rejects but for the registered looseness.
//
// conformance: semantics/translator-writes-keyword-field-names-bare
func TestParseKeywordFieldNamesMatchVictoriaLogs(t *testing.T) {
	rows := readKeywordFieldVerdicts(t)
	if len(rows) < 1000 {
		t.Fatalf("%d recorded verdicts", len(rows))
	}
	for _, r := range rows {
		if r.V149 != r.V153 {
			t.Errorf("%s: VictoriaLogs v1.49.0 accepts=%v, v1.53.0 accepts=%v: the grid is for one reading", r.Query, r.V149, r.V153)
		}
		_, err := Parse(r.Query)
		switch {
		case r.V153 && err != nil:
			t.Errorf("%s: VictoriaLogs accepts it, the proxy's parser rejects it: %v", r.Query, err)
		case !r.V153 && err == nil && !looserThanVL.MatchString(r.Query):
			t.Errorf("%s: VictoriaLogs rejects it, the proxy's parser accepts it", r.Query)
		}
	}
}

func TestKeywordFieldName(t *testing.T) {
	for name, want := range map[string]bool{
		"not": true, "NOT": true, "Not": true, "in": true, "IN": true, "and": true, "Or": true, "options": true,
		"contains_all": true, "CONTAINS_ANY": true, "contains_common_case": true, "equals_common_case": true,
		"i": false, "exact": false, "re": false, "seq": false, "range": false, "offset": false, "of": false,
		"ipv4_range": false, "value_type": false, "app": false, "": false, "nota": false, "not_x": false, "x_not": false,
		`"not"`: false, "not.x": false, "_time": false, "_stream": false, "_msg": false,
	} {
		if got := KeywordFieldName(name); got != want {
			t.Errorf("KeywordFieldName(%q) = %v, want %v", name, got, want)
		}
	}
}

// QuoteKeywordField quotes a keyword name and nothing else, with no allocation for any other name.
//
// conformance: semantics/translator-writes-keyword-field-names-bare
func TestQuoteKeywordField(t *testing.T) {
	for name, want := range map[string]string{
		"not": `"not"`, "IN": `"IN"`, "Or": `"Or"`, "options": `"options"`, "contains_all": `"contains_all"`,
		"app": "app", "range": "range", `"not"`: `"not"`, "": "", "not_x": "not_x",
	} {
		if got := QuoteKeywordField(name); got != want {
			t.Errorf("QuoteKeywordField(%q) = %q, want %q", name, got, want)
		}
		if got := QuoteKeywordField(name); got != "" {
			if _, err := Parse(got + `:="x"`); err != nil {
				t.Errorf("the proxy's parser does not read %s:=\"x\": %v", got, err)
			}
		}
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = QuoteKeywordField("kubernetes_pod_name") }); allocs != 0 {
		t.Errorf("a plain name allocates %v times", allocs)
	}
}
