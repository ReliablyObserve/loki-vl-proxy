//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// conformance: semantics/pattern-line-filter
// conformance: loki_api_v1_query_range, loki_api_v1_query
// TestCompat_PatternLineFilter: an empty pattern is a matcher that accepts only
// an empty line (pkg/logql/log/pattern ParseLineFilter, Matcher.Test), so
// `|> ""` keeps no line of the fixture and `!> ""` keeps every one. The proxy
// translated the empty pattern to match-all and returned the opposite lines.
// An empty alternative in an `or` chain is an ordinary pattern, not dropped.
func TestCompat_PatternLineFilter(t *testing.T) {
	fx := ensureOrFilterFixture(t)
	sel := fx.logs.selector()
	start, end := fx.start.Add(-2*time.Minute), fx.start.Add(33*time.Minute)
	for _, tc := range []struct {
		filter string
		lines  int
	}{
		{`|> ""`, 0},
		{"|> ``", 0},
		{`!> ""`, 180},
		{"!> ``", 180},
		{`|> "" or "<_>=red<_>"`, 30},
		{`!> "" or "<_>=red<_>"`, 150},
		{`!> "" |> "<_>=red<_>"`, 30},
		// A line filter right after the pattern needs no pipe.
		{`|> "" != "tick=000"`, 0},
		{`!> "" != "word=red"`, 150},
		{`!> "" !~ "word=(red|blue)"`, 120},
		{`|> "<_>=red<_>" != "tick=000"`, 29},
		{`|= "red" !> ""`, 30},
		{`|= "red" |> ""`, 0},
		{`| logfmt |> ""`, 0},
		{`| logfmt !> "" | word="blue"`, 30},
		// Non-empty patterns keep agreeing.
		{`|> "<_>=red<_>"`, 30},
		{`!> "<_>=red<_>"`, 150},
	} {
		query := sel + " " + tc.filter
		t.Run(query, func(t *testing.T) {
			loki := logStreams(t, lokiURL, query, start, end)
			assertStreamParity(t, query, loki, logStreams(t, proxyURL, query, start, end), tc.lines)
		})
	}
	// The same filters inside a range metric.
	msel := fx.metrics.selector()
	at := fx.start.Add(30 * time.Minute)
	for _, query := range []string{
		`count_over_time(` + msel + ` |> "" [30m])`,
		`sum(count_over_time(` + msel + ` !> "" [30m]))`,
		`sum(count_over_time(` + msel + ` |> "" or "<_>=red<_>" [30m]))`,
		`sum by (word) (count_over_time(` + msel + ` | logfmt !> "" [30m]))`,
	} {
		t.Run("instant "+query, func(t *testing.T) {
			loki := instantVector(t, lokiURL, query, at)
			proxy := instantVector(t, proxyURL, query, at)
			if fmt.Sprint(loki) != fmt.Sprint(proxy) {
				t.Errorf("proxy %v, Loki %v", proxy, loki)
			}
		})
	}
	t.Run("range metric", func(t *testing.T) {
		for _, alt := range []string{
			`count_over_time(` + msel + ` !> "" [1m])`,
			`count_over_time(` + msel + ` |> "" or "<_>=red<_>" [1m])`,
		} {
			assertSlidingParity(t, alt, slidingRangeSeries(t, lokiURL, alt, start, end, time.Minute, nil), slidingRangeSeries(t, proxyURL, alt, start, end, time.Minute, nil))
		}
	})
}
