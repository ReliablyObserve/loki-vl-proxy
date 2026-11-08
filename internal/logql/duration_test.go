package logql

import (
	"testing"
	"time"
)

func TestRangeAggregationDurations(t *testing.T) {
	for _, tc := range []struct {
		query         string
		rng, off      time.Duration
		hasOffset     bool
		rangeLiteral  string
		offsetLiteral string
	}{
		{`rate({a="b"}[5m])`, 5 * time.Minute, 0, false, "5m", ""},
		{`rate({a="b"}[1h] offset 30m)`, time.Hour, 30 * time.Minute, true, "1h", "30m"},
		{`rate({a="b"}[1d] offset 1w)`, 24 * time.Hour, 7 * 24 * time.Hour, true, "1d", "1w"},
		{`rate({a="b"}[1.5h])`, 90 * time.Minute, 0, false, "1.5h", ""},
		{`rate({a="b"}[500ms])`, 500 * time.Millisecond, 0, false, "500ms", ""},
		{`rate({a="b"}[1y])`, 365 * 24 * time.Hour, 0, false, "1y", ""},
		{`rate({a="b"}[5m] offset 0s)`, 5 * time.Minute, 0, true, "5m", "0s"},
	} {
		expr, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		ra := expr.(*RangeAggregation)
		if ra.RangeDuration != tc.rng || ra.OffsetDuration != tc.off || ra.HasOffset() != tc.hasOffset || ra.Range != tc.rangeLiteral || ra.Offset != tc.offsetLiteral {
			t.Errorf("%s: got range %v offset %v hasOffset %v (%q, %q)", tc.query, ra.RangeDuration, ra.OffsetDuration, ra.HasOffset(), ra.Range, ra.Offset)
		}
		if got := ra.String(); got != tc.query {
			t.Errorf("%s: String() = %q", tc.query, got)
		}
	}
}
