package logql

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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
	return append(append(out,
		`{app="a"} | x=ip("1.1.1.1") | y=ip("yyy")`, `{app="a"} | x=ip("zzz") | y=ip("yyy")`,
		`{app="a"} |~ "(" | x=ip("zzz")`, `{app="a"} | x=ip("zzz") |~ "("`, `{app="a"} | line_format "{{.a}" | x=ip("zzz")`,
		`{app="a"} | x=ip("zzz") | line_format "{{.a}"`, `{app="a"} |> "<a>" | x=ip("zzz")`, `{app="a"} | x=ip("zzz") |> "<a>"`,
	), ipControlByteGrid(`{app="a"} | addr%sip(%s)`)...)
}

// ipControlBytePatterns are patterns with a NUL or another control byte inside an address, a prefix or a range: the
// bytes Go's address parser rejects, which a filter that cuts at them would read as valid (FuzzIPLabelFilterNeverKeepsWhatLokiDrops).
var ipControlBytePatterns = []string{"::\x00!", "\x00", "1.2.3.4\x00", "::\x00", "1.2.3.4\x00/24", "1.2.3.4-\x001.2.3.5", "\x00-1.2.3.4",
	"2001:db8::\x00/32", "1.2.3.4\x1f", "1.2.3.4\x7f", "fe80::1%e\x00th0"}

// ipControlByteGrid is a query of format (an operator and a quoted pattern) for each control-byte pattern, quoted and
// in backticks (a raw NUL byte in the query is Loki's lexer error, not the pattern's, and is not asked here),
// with = and != (or the line filter operators).
func ipControlByteGrid(format string) []string {
	var out []string
	for _, cmp := range ipControlByteOperators(format) {
		for _, p := range ipControlBytePatterns {
			out = append(out, fmt.Sprintf(format, cmp, strconv.Quote(p)))
			if !strings.Contains(p, "\x00") {
				out = append(out, fmt.Sprintf(format, cmp, "`"+p+"`"))
			}
		}
	}
	return out
}

// ipControlByteOperators are the comparison operators of a query format: = and != for a label filter, |= and != for a
// line filter.
func ipControlByteOperators(format string) []string {
	if strings.Contains(format, "addr%s") {
		return []string{"=", "!="}
	}
	return []string{"|=", "!="}
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
