package translator

import (
	"regexp"
	"strings"
	"testing"
)

// statsFirstLastFuncRE matches the stats functions VictoriaLogs lacks.
var statsFirstLastFuncRE = regexp.MustCompile(`\b(?:first|last)\(`)

// VictoriaLogs has no first or last stats function (lib/logstorage/stats_*.go:
// avg, count, max, min, sum, row_min, row_max, ...). Loki's first_over_time and
// last_over_time are the sample with the earliest and the latest timestamp of
// each series in the window (pkg/logql/first_last_over_time.go), which
// row_min(_time, v) and row_max(_time, v) return as a JSON row; a row_* result
// is a label to VictoriaLogs, so the value is unpacked into a number before the
// outer aggregation reads it.
//
// conformance: semantics/unwrap-first-last-under-aggregation
func TestFirstLastOverTimeUnderAggregationTranslatesToRowStats(t *testing.T) {
	for _, tc := range []struct {
		name, logql string
		row         string // the row_* function the series value comes from
		outer       string // the stats pipe that aggregates the series values
	}{
		{"sum first", `sum(first_over_time({app="p"} | logfmt | unwrap amount [5m]))`, "row_min", "| stats sum(__lvp_inner)"},
		{"sum last", `sum(last_over_time({app="p"} | logfmt | unwrap amount [5m]))`, "row_max", "| stats sum(__lvp_inner)"},
		{"avg last", `avg(last_over_time({app="p"} | logfmt | unwrap amount [5m]))`, "row_max", "| stats avg(__lvp_inner)"},
		{"min first", `min(first_over_time({app="p"} | json | unwrap amount [5m]))`, "row_min", "| stats min(__lvp_inner)"},
		{"max last", `max(last_over_time({app="p"} | json | unwrap amount [5m]))`, "row_max", "| stats max(__lvp_inner)"},
		{"count last", `count(last_over_time({app="p"} | logfmt | unwrap amount [5m]))`, "row_max", "| stats count()"},
		{"no parser", `sum(last_over_time({app="p"} | unwrap amount [5m]))`, "row_max", "| stats sum(__lvp_inner)"},
		{"sum by", `sum by (pod) (last_over_time({app="p"} | logfmt | unwrap amount [5m]))`, "row_max", "| stats by (pod) sum(__lvp_inner)"},
		{"max by", `max by (pod) (first_over_time({app="p"} | logfmt | unwrap amount [5m]))`, "row_min", "| stats by (pod) max(__lvp_inner)"},
		{"count by after", `count(last_over_time({app="p"} | logfmt | unwrap amount [5m])) by (pod)`, "row_max", "| stats by (pod) count()"},
		{"duration", `sum(last_over_time({app="p"} | logfmt | unwrap duration(amount) [5m]))`, "row_max", "| stats sum(__lvp_inner)"},
		{"bytes", `sum(first_over_time({app="p"} | logfmt | unwrap bytes(amount) [5m]))`, "row_min", "| stats sum(__lvp_inner)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatal(err)
			}
			if statsFirstLastFuncRE.MatchString(got) {
				t.Fatalf("VictoriaLogs has no first or last stats function: %s", got)
			}
			row := tc.row + "(_time, " + UnwrapValueAlias + ") as __lvp_inner"
			unpack := ` | unpack_json from __lvp_inner fields (` + UnwrapValueAlias + `) | math ` + UnwrapValueAlias + ` as __lvp_inner `
			if !strings.Contains(got, row+unpack+tc.outer) {
				t.Fatalf("want %s%s%s in\n%s", row, unpack, tc.outer, got)
			}
			// The row function reads the converted value, so the gate (and the
			// conversion-error counter that rides on it) comes first.
			if !strings.Contains(got[:strings.Index(got, row)], " as "+UnwrapValueAlias) {
				t.Fatalf("the unwrap gate must precede the row function: %s", got)
			}
			if !strings.HasSuffix(got, tc.outer) {
				t.Fatalf("the query must end in the outer stats pipe %q: %s", tc.outer, got)
			}
		})
	}
}

// Without an outer aggregation (a bare function, a range-level by (...), the
// inner of topk and bottomk) the query still ends in one numeric value per
// series, grouped like the series: a row function alone is a label.
//
// conformance: semantics/unwrap-first-last-under-aggregation
func TestFirstLastOverTimeBareIsOneValuePerSeries(t *testing.T) {
	for _, tc := range []struct {
		logql, row, by string
	}{
		{`first_over_time({app="p"} | logfmt | unwrap amount [5m])`, "row_min", "_stream, _msg"},
		{`last_over_time({app="p"} | json | unwrap amount [5m])`, "row_max", "_stream, _msg"},
		{`last_over_time({app="p"} | unwrap amount [5m])`, "row_max", "_stream, level"},
		{`last_over_time({app="p"} | logfmt | unwrap amount [5m]) by (pod)`, "row_max", "pod"},
		{`first_over_time({app="p"} | logfmt | unwrap amount [5m]) by ()`, "row_min", ""},
	} {
		got, err := TranslateLogQL(tc.logql)
		if err != nil {
			t.Fatal(err)
		}
		want := "stats by (" + tc.by + ") " + tc.row + "(_time, " + UnwrapValueAlias + ") as __lvp_inner | unpack_json from __lvp_inner fields (" +
			UnwrapValueAlias + ") | math " + UnwrapValueAlias + " as __lvp_inner | stats by (" + tc.by + ") max(__lvp_inner)"
		if statsFirstLastFuncRE.MatchString(got) || !strings.HasSuffix(got, want) {
			t.Fatalf("%s:\n got %s\nwant ...%s", tc.logql, got, want)
		}
	}
}

// A grouped aggregation the translation does not implement keeps failing loudly
// (VictoriaLogs rejects the stats function it has not got) rather than answer the
// per-series values with the aggregation dropped.
//
// conformance: semantics/unwrap-first-last-under-aggregation
func TestFirstLastOverTimeUnsupportedGroupedAggregationFailsLoudly(t *testing.T) {
	for _, logql := range []string{
		`stdvar by (pod) (last_over_time({app="p"} | logfmt | unwrap amount [5m]))`,
		`topk by (pod) (1, last_over_time({app="p"} | logfmt | unwrap amount [5m]))`,
		`bottomk by (pod) (1, first_over_time({app="p"} | logfmt | unwrap amount [5m]))`,
	} {
		got, err := TranslateLogQL(logql)
		if err == nil && !statsFirstLastFuncRE.MatchString(got) {
			t.Errorf("%s: translated without the aggregation: %s", logql, got)
		}
	}
	got, err := TranslateLogQL(`stddev by (pod) (last_over_time({app="p"} | logfmt | unwrap amount [5m]))`)
	if err != nil || !strings.HasSuffix(got, "| stats by (pod) stddev(__lvp_inner)") {
		t.Errorf("stddev by (pod): %s (%v)", got, err)
	}
}
