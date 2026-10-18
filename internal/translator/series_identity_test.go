package translator

import (
	"slices"
	"strings"
	"testing"
)

// TestParsedSeriesLabels: the labels a pipeline adds to a Loki series, less the
// unwrapped one, when every parser names what it extracts; not known (false)
// for a parser that extracts every key and for the stages that change labels in
// ways the grouping does not follow.
func TestParsedSeriesLabels(t *testing.T) {
	cases := []struct {
		query string
		want  []string
		known bool
	}{
		{`{app="a"} | unwrap amount`, nil, true},
		{`{app="a"} | logfmt amount | unwrap amount`, nil, true},
		{`{app="a"} | logfmt amount, route | unwrap amount`, []string{"route"}, true},
		{`{app="a"} | logfmt amount, r="route" | unwrap amount`, nil, false},
		{`{app="a"} | json amount, path="req[0].path" | unwrap amount`, nil, false},
		{`{app="a"} | json amount, path="req.0.path" | unwrap amount`, nil, false},
		{`{app="a"} | json amount, path="req.path" | unwrap amount`, []string{"path"}, true},
		{`{app="a"} |= "x" | logfmt amount | status >= 200 | unwrap amount`, nil, true},
		{`{app="a"} | logfmt amount |= "x" != "y" | unwrap amount`, nil, true},
		{`{app="a"} |~ "a|b" | logfmt amount | unwrap duration(amount)`, nil, true},
		{`{app=~"a|b"} | logfmt amount | line_format "{{.amount}}" | unwrap amount`, nil, true},
		{`{app="a"} | regexp "amount=(?P<amount>\\d+) route=(?P<route>\\S+)" | unwrap amount`, []string{"route"}, true},
		{"{app=\"a\"} | regexp `(?P<amount>\\d+)` | unwrap amount", nil, true},
		{`{app="a"} | pattern "<_> amount=<amount> <route>" | unwrap amount`, []string{"route"}, true},
		{`{app="a"} | logfmt amount | json route | unwrap amount`, []string{"route"}, true},
		{`{app="a"} | logfmt | unwrap amount`, nil, false},
		{`{app="a"} | json | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount | json | unwrap amount`, nil, false},
		{`{app="a"} | unpack | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount | label_format x=amount | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount | drop route | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount | keep amount | unwrap amount`, nil, false},
		{`{app="a"} | logfmt --strict amount | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount, level | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount, service_name | unwrap amount`, nil, false},
		// a parsed key that names a label of the stream selector is `<key>_extracted` on Loki
		{`{app="a", pod=~".+"} | logfmt amount, pod | unwrap amount`, nil, false},
		{`{app="a",pod!="x"} | logfmt amount, pod | unwrap amount`, nil, false},
		{`{app="a", pod=~".+"} | logfmt amount, route | unwrap amount`, []string{"route"}, true},
		{"{`app`=\"a\", pod=\"x,y\"} | logfmt amount, app | unwrap amount", nil, false},
		{`{app="a"} | logfmt amount, a.b | unwrap amount`, nil, false},
		{`{app="a"} | regexp "(?P<amount" | unwrap amount`, nil, false},
		{`{app="a"} | logfmt amount !> "x" | unwrap amount`, nil, false},
		{`{app="a"} | somethingelse | unwrap amount`, nil, false},
	}
	for _, c := range cases {
		got, known := parsedSeriesLabels(c.query, extractUnwrapField(c.query))
		if known != c.known || (known && !slices.Equal(got, c.want)) {
			t.Errorf("%s: got %v known=%v, want %v known=%v", c.query, got, known, c.want, c.known)
		}
	}
}

// TestUnwrapInnerGroupingFollowsParsedSeries: for the `without` the proxy folds,
// an extraction list groups the inner stats by the stream and the labels it
// names; a parser that extracts every key keeps `_msg` as the stand-in for
// them, and so does every other query, whatever its parser.
func TestUnwrapInnerGroupingFollowsParsedSeries(t *testing.T) {
	for query, want := range map[string]string{
		`max_over_time({app="a"} | logfmt amount | unwrap amount [5m])`:                     "_stream",
		`max_over_time({app="a"} | logfmt amount, route | unwrap amount [5m])`:              "_stream, route",
		`max_over_time({app="a", route=~".+"} | logfmt amount, route | unwrap amount [5m])`: "_stream, _msg", // route is a stream label too
		`max_over_time({app="a"} | logfmt | unwrap amount [5m])`:                            "_stream, _msg",
		`max_over_time({app="a"} | logfmt amount | drop x | unwrap amount [5m])`:            "_stream, _msg",
		`max_over_time({app="a"} | logfmt amount, r="route" | unwrap amount [5m])`:          "_stream, _msg", // no field r exists
		`max_over_time({app="a"} | json amount, p="a.b" | unwrap amount [5m])`:              "_stream, p",
	} {
		q, _ := extractQueryAndDuration(strings.TrimSuffix(strings.TrimPrefix(query, "max_over_time("), ")"))
		if got := unwrapInnerGrouping(q, "", "sum", nil, false, "", true); got != want {
			t.Errorf("%s: folded, grouped by %q, want %q", query, got, want)
		}
		if got := unwrapInnerGrouping(q, "", "sum", nil, false, "", false); got != "_stream, _msg" {
			t.Errorf("%s: not folded, grouped by %q, want the grouping of a query that is not a folded without", query, got)
		}
	}
	for query, want := range map[string]string{
		`sum without (pod) (max_over_time({app="a"} | logfmt amount | unwrap amount [5m]))`:        "| stats by (_stream) max(__lvp_v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)",
		`sum without (pod) (max_over_time({app="a"} | logfmt amount, route | unwrap amount [5m]))`: "| stats by (_stream, route) max(__lvp_v) as __lvp_inner | stats by (_stream, route) sum(__lvp_inner)",
		`sum without (pod) (max_over_time({app="a"} | logfmt | unwrap amount [5m]))`:               "| stats by (_stream, _msg) max(__lvp_v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)",
		// the extraction list names a label of the selector: the grouping before the fold
		`sum without (ns) (max_over_time({app="a", pod=~".+"} | logfmt amount, pod | unwrap amount [5m]))`: "| stats by (_stream, _msg) max(__lvp_v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)",
	} {
		got, err := TranslateLogQL(query)
		if err != nil || !strings.Contains(got, want) {
			t.Errorf("%s:\n got %q (%v)\nwant a query containing %q", query, got, err, want)
		}
	}
}
