package logql

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// ipLineFilterGrid is the grid the ip() line filter's validation is held to Loki on: valid and invalid patterns, |= and
// !=, quoted and in backticks, in a chain, and the patterns with a control byte inside.
func ipLineFilterGrid() []string {
	patterns := []string{"1.2.3.4", "1.2.3.0/24", "10.0.0.1-10.0.0.5", "2001:db8::/32", "fe80::1%eth0", "zzz", "", "1.2.3.4/33", "1.2.3.5-1.2.3.4",
		" 1.2.3.4", "999.1.1.1", "é"}
	var out []string
	for _, op := range []string{"|=", "!="} {
		for _, p := range patterns {
			q := strconv.Quote(p)
			out = append(out,
				fmt.Sprintf(`{app="a"} %s ip(%s)`, op, q),
				fmt.Sprintf(`{app="a"} %s IP ( %s )`, op, q),
				fmt.Sprintf(`{app="a"} %s "x" %s ip(%s)`, op, op, q),
				fmt.Sprintf(`{app="a"} | json %s ip(%s)`, op, q),
			)
			if !strings.Contains(p, "`") {
				out = append(out, fmt.Sprintf("{app=\"a\"} %s ip(`%s`)", op, p))
			}
		}
	}
	return append(out, ipControlByteGrid(`{app="a"} %s ip(%s)`)...)
}

const ipLineFilterLokiAnswers = "testdata/ip-line-filter-validate-loki.jsonl"

// A line filter with an ip() pattern that is not an address, a prefix or an ordered range of one family is rejected
// where Loki rejects it, with Loki's text, whatever the pattern holds: a control byte inside an address, a prefix or a
// range is not read as the address before it. Loki's answers are recorded with the Loki oracle (LVP_RUN_ORACLES=1
// records them again).
//
// conformance: semantics/ip-label-filter-invalid-pattern
func TestIPLineFilterValidationMatchesLoki(t *testing.T) {
	grid := ipLineFilterGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordPipelineAnswers(t, grid, ipLineFilterLokiAnswers)
	}
	b, err := os.ReadFile(ipLineFilterLokiAnswers)
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
			t.Errorf("%q: proxy error %v, Loki accepts=%v %s", q, err, loki.Accept, loki.Error)
		case err != nil && err.Error() != loki.Error:
			t.Errorf("%q:\n proxy %s\n Loki  %s", q, err, loki.Error)
		}
	}
	if rejected == 0 || rejected == len(grid) {
		t.Fatalf("%d of %d queries rejected by Loki: the grid must hold both", rejected, len(grid))
	}
	t.Logf("%d queries, %d rejected by Loki", len(grid), rejected)
}
