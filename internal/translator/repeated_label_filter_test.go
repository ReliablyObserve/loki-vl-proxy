package translator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// repeatedFilterCase is a log query, the labels of one stream and its line.
type repeatedFilterCase struct {
	Query  string            `json:"q"`
	Labels map[string]string `json:"labels"`
	Line   string            `json:"line"`
	Keep   bool              `json:"keep"`
}

const repeatedFilterAnswers = "testdata/repeated-label-filters-loki.json"

func repeatedFilterCases() []repeatedFilterCase {
	var out []repeatedFilterCase
	for _, q := range []string{
		`{app="x"} | a="1" | a="2"`,
		`{app="x"} | a="1" | a!="1"`,
		`{app="x"} | a="1" | a!="2"`,
		`{app="x"} | a!="1" | a!="2"`,
		`{app="x"} | a=~"1|2" | a=~"2|3"`,
		`{app="x"} | a="1" | b="2" | a="1"`,
		`{app="x"} | a="1" | b="2" | a="2"`,
		`{app="x"} | a>5 | a<10`,
		`{app="x"} | a>5 | a>10`,
		`{app="x"} | a="1" |= "y" | a="2"`,
		`{app="x"} | a="1" |= "y" | a!="2"`,
		`{app="x"} | level="info" | level="warn"`,
		`{app="x"} | level="info" | level!="warn"`,
	} {
		for _, labels := range []map[string]string{
			{"app": "x", "a": "1", "b": "2"}, {"app": "x", "a": "2", "b": "2"}, {"app": "x", "a": "3", "b": "3"},
			{"app": "x", "a": "7", "b": "2"}, {"app": "x", "a": "12", "b": "2"}, {"app": "x", "b": "2"},
			{"app": "x", "level": "info"}, {"app": "x", "level": "warn"},
		} {
			for _, line := range []string{"y", "z"} {
				out = append(out, repeatedFilterCase{Query: q, Labels: labels, Line: line})
			}
		}
	}
	return out
}

func recordRepeatedFilterAnswers(t *testing.T, cases []repeatedFilterCase) {
	t.Helper()
	pc := make([]oraclerun.ProcessCase, len(cases))
	for i, c := range cases {
		pc[i] = oraclerun.ProcessCase{Query: c.Query, Labels: c.Labels, Line: c.Line}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), pc)
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
	if err := os.WriteFile(repeatedFilterAnswers, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Every label filter of a query applies where it stands, as in Loki: a second filter on the same label does not
// replace the first. The LogsQL the translator writes keeps exactly the rows Loki's pipeline keeps, on rows of
// every combination of the labels (Loki's answers are recorded with the Loki oracle; LVP_RUN_ORACLES=1 records
// them again).
//
// conformance: semantics/repeated-label-filter-replaced-across-label-format
func TestRepeatedLabelFiltersApplyAllLikeLoki(t *testing.T) {
	cases := repeatedFilterCases()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordRepeatedFilterAnswers(t, cases)
	}
	b, err := os.ReadFile(repeatedFilterAnswers)
	if err != nil {
		t.Fatal(err)
	}
	var recorded []repeatedFilterCase
	if err := json.Unmarshal(b, &recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded) != len(cases) {
		t.Fatalf("%d recorded answers for %d cases: record them again with LVP_RUN_ORACLES=1", len(recorded), len(cases))
	}
	kept := 0
	for _, c := range recorded {
		out, err := TranslateLogQLWithLabels(c.Query, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.Query, err)
		}
		row := map[string]string{"_msg": c.Line}
		for k, v := range c.Labels {
			row[k] = v
		}
		if got := evalRow(t, out, row); got != c.Keep {
			t.Errorf("%s on %v line %q: Loki keeps %v, %s keeps %v", c.Query, c.Labels, c.Line, c.Keep, out, got)
		}
		if c.Keep {
			kept++
		}
	}
	if kept == 0 || kept == len(recorded) {
		t.Fatalf("the grid keeps %d of %d rows: it does not tell a filter that applies from one that does not", kept, len(recorded))
	}
}

// The filters of a query stay in the order they are written, each before and after the stages between them, and
// none is dropped: the shapes where a later filter on a label used to replace the earlier one.
//
// conformance: semantics/repeated-label-filter-replaced-across-label-format
func TestRepeatedLabelFiltersKeepTheirPlace(t *testing.T) {
	for q, want := range map[string]string{
		`{app="x"} | json | user="info" | label_format x=msg | user="m"`: `app:="x" | unpack_json | filter user:="info" | format if (msg:*) "<msg>" as x skip_empty_results | delete msg | filter user:="m"`,
		`{app="x"} | a="1" | a="2"`:                                      `app:="x" a:="1" a:="2"`,
		`{app="x"} | a="1" | b="2" | a="3"`:                              `app:="x" a:="1" b:="2" a:="3"`,
		`{app="x"} | json | a="1" | a!="1"`:                              `app:="x" | unpack_json | filter a:="1" | filter -a:="1"`,
		`{app="x"} | a=~"p" | a=~"q"`:                                    `app:="x" a:="p" a:="q"`,
		`{app="x"} | a="1" | a="1"`:                                      `app:="x" a:="1" a:="1"`,
	} {
		got, err := TranslateLogQLWithLabels(q, nil)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", q, got, want)
		}
	}
}
