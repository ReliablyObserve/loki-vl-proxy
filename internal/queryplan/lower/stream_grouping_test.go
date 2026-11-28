package lower

import "testing"

// TestStreamGroupedParsersAt pins the shape the stream grouped sum rule takes (semantics/stream-grouped-sum-skips-
// parsers): the index of the first parser, or -1.
//
// conformance: semantics/stream-grouped-sum-skips-parsers
func TestStreamGroupedParsersAt(t *testing.T) {
	const drop = ` | drop __error__, __error_details__`
	for _, c := range []struct {
		q  string
		at int
	}{
		// The shape: both orders, count, rate and the bytes functions, several labels, an error drop alone, line
		// filters before the parsers, an offset, a ranking around the sum.
		{`sum by (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, 0},
		{`sum by (level) (count_over_time({app="a"} | logfmt | json` + drop + ` [5m]))`, 0},
		{`sum by (level) (rate({app="a"} | json | logfmt` + drop + ` [5m]))`, 0},
		{`sum by (level) (bytes_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, 0},
		{`sum by (level) (bytes_rate({app="a"} | json | logfmt` + drop + ` [5m]))`, 0},
		{`sum by (level, pod) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, 0},
		{`sum by (level) (count_over_time({app="a"} | json | logfmt | drop __error__ [5m]))`, 0},
		{`sum by (level) (count_over_time({app="a"} |= "x" != "y" | json | logfmt` + drop + ` [5m]))`, 2},
		{`sum by (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m] offset 1h))`, 0},
		{`sum by (level) (count_over_time({app="a"} | json | json | logfmt` + drop + ` [5m]))`, 0},
		{`sum by (level) (count_over_time({app="a"} | json | drop x | logfmt` + drop + ` [5m]))`, 0},
		{`sum(count_over_time({app="a"} | json | logfmt` + drop + ` [5m])) by (level)`, 0},
		{`topk(2, sum by (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m])))`, 0},
		{`sort_desc(sum by (level) (count_over_time({app="a"} | logfmt | json` + drop + ` [5m])))`, 0},
		// Not the shape.
		{`sum by (level) (count_over_time({app="a"} | json | logfmt [5m]))`, -1},                                 // errors kept: Loki answers 400 on a failed line
		{`sum by (level) (count_over_time({app="a"} | json` + drop + ` | logfmt [5m]))`, -1},                     // the last parser's errors kept
		{`sum by (level) (count_over_time({app="a"} | json` + drop + ` [5m]))`, -1},                              // one parser
		{`sum by (level) (count_over_time({app="a"} | logfmt | logfmt` + drop + ` [5m]))`, -1},                   // no json
		{`sum by (level) (count_over_time({app="a"} | json | logfmt | drop __error__, level [5m]))`, -1},         // drops the grouping label
		{`sum by (level) (count_over_time({app="a"} | json | logfmt | drop __error__="x" [5m]))`, -1},            // a conditional drop
		{`sum by (level) (count_over_time({app="a"} | json | logfmt | level!=""` + drop + ` [5m]))`, -1},         // a label filter
		{`sum by (level) (count_over_time({app="a"} | json |= "x" | logfmt` + drop + ` [5m]))`, -1},              // a line filter after a parser
		{`sum by (level) (count_over_time({app="a"} | json | logfmt | keep level` + drop + ` [5m]))`, -1},        // a keep
		{`sum by (level) (count_over_time({app="a"} | json a="b" | logfmt` + drop + ` [5m]))`, -1},               // an extraction list
		{`sum by (level) (count_over_time({app="a"} | json | logfmt --strict` + drop + ` [5m]))`, -1},            // a flag
		{`sum by (level) (count_over_time({app="a"} | json | regexp "(?P<x>.)" | logfmt` + drop + ` [5m]))`, -1}, // another parser
		{`sum by (level) (count_over_time({app="a"} | json | logfmt | line_format "x"` + drop + ` [5m]))`, -1},   // a format stage
		{`sum by (level) (count_over_time({app="a"} | drop x | json | logfmt` + drop + ` [5m]))`, -1},            // a drop before the parsers
		{`sum by (level) (count_over_time({app="a"} [5m]))`, -1},                                                 // no parser
		{`sum by (level) (absent_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`sum by (x) (quantile_over_time(0.9, {app="a"} | json | logfmt` + drop + ` | unwrap x [5m]))`, -1},
		{`sum without (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`sum by () (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`sum(count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`max by (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`count_over_time({app="a"} | json | logfmt` + drop + ` [5m])`, -1},
		{`sum by (level_extracted) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1}, // the parsers write it
		{`sum by (__error__) (count_over_time({app="a"} | json | logfmt | drop __error_details__ [5m]))`, -1},
		{`sum by (detected_level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`sum by (service_name) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m]))`, -1},
		{`sum by (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m])) * 2`, -1}, // the operand is a query of its own
		{`topk(2, max by (level) (count_over_time({app="a"} | json | logfmt` + drop + ` [5m])))`, -1},
	} {
		if got := StreamGroupedParsersAt(planOf(t, c.q)); got != c.at {
			t.Errorf("%s: %d, want %d", c.q, got, c.at)
		}
	}
	if StreamGroupedParsersAt(nil) != -1 {
		t.Error("a nil plan has the shape")
	}
}
