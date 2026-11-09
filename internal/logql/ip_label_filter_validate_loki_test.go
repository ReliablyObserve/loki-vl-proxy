package logql

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// ipLabelFilterGrid is the grid the ip() label filter's validation is held to Loki on: valid and invalid patterns
// (addresses, prefixes, ranges, zones, spaces, quotes, empty), = and !=, the ip( spellings Loki accepts, stages that
// join several filters, and the order next to other stages that fail.
func ipLabelFilterGrid() []string {
	patterns := []string{"zzz", "", `a"b`, "1.2.3.4", "1.2.3.4/33", "10.0.0.0/8", "1.2.3.5-1.2.3.4", "10.0.0.1-10.0.0.5",
		"::1-1.2.3.4", "::1", "2001:db8::/32", "fe80::1%eth0", " 1.2.3.4", "1.2.3.4 ", "999.1.1.1", "1.2.3", "é"}
	var out []string
	for _, cmp := range []string{"=", "!="} {
		for _, p := range patterns {
			q := fmt.Sprintf("%q", p)
			out = append(out,
				fmt.Sprintf(`{app="a"} | addr%sip(%s)`, cmp, q),
				fmt.Sprintf(`{app="a"} | addr %s IP ( %s )`, cmp, q),
				fmt.Sprintf(`{app="a"} | a="1", addr%sip(%s)`, cmp, q),
				fmt.Sprintf(`{app="a"} | a="1" and addr%sip(%s)`, cmp, q),
				fmt.Sprintf(`{app="a"} | (addr%sip(%s) or b="2")`, cmp, q),
				fmt.Sprintf(`{app="a"} | json | addr%sip(%s)`, cmp, q),
				fmt.Sprintf(`{app="a"} | 日本%sip(%s) | line_format "x"`, cmp, q),
			)
			if !strings.Contains(p, "`") {
				out = append(out, fmt.Sprintf("{app=\"a\"} | addr%sip(`%s`)", cmp, p))
			}
		}
	}
	return append(out,
		`{app="a"} | x=ip("1.1.1.1") | y=ip("yyy")`, `{app="a"} | x=ip("zzz") | y=ip("yyy")`,
		`{app="a"} |~ "(" | x=ip("zzz")`, `{app="a"} | x=ip("zzz") |~ "("`, `{app="a"} | line_format "{{.a}" | x=ip("zzz")`,
		`{app="a"} | x=ip("zzz") | line_format "{{.a}"`, `{app="a"} |> "<a>" | x=ip("zzz")`, `{app="a"} | x=ip("zzz") |> "<a>"`,
	)
}

const ipLabelFilterLokiAnswers = "testdata/ip-label-filter-validate-loki.jsonl"

// A label filter stage that is one ip() comparison with an invalid pattern is rejected exactly where Loki rejects
// it, with Loki's text; a stage that joins several filters is not, as in Loki. Loki's answers are recorded with the
// Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/ip-label-filter-invalid-pattern
func TestIPLabelFilterValidationMatchesLoki(t *testing.T) {
	grid := ipLabelFilterGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordPipelineAnswers(t, grid, ipLabelFilterLokiAnswers)
	}
	b, err := os.ReadFile(ipLabelFilterLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != len(grid) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(lines), len(grid))
	}
	rejected := 0
	for i, q := range grid {
		var loki patternFilterAnswer
		if err := json.Unmarshal([]byte(lines[i]), &loki); err != nil {
			t.Fatal(err)
		}
		if !loki.Accept {
			rejected++
		}
		err := ParseAndValidate(q)
		switch {
		case (err == nil) != loki.Accept:
			t.Errorf("%s: proxy error %v, Loki accepts=%v %s", q, err, loki.Accept, loki.Error)
		case err != nil && err.Error() != loki.Error:
			t.Errorf("%s:\n proxy %s\n Loki  %s", q, err, loki.Error)
		}
	}
	t.Logf("%d queries, %d rejected by Loki", len(grid), rejected)
}
