package logql

import (
	"strings"
	"testing"
)

// conformance: parsed-label-series-identity
// Loki names a metric series with the labels its pipeline extracted. Only
// `| regexp` and `| pattern` name those labels in the query itself.
func TestParserCaptureLabels(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  string
	}{
		{`count_over_time({app="x"} | regexp "job_id=(?P<jid>\\w+)" [1m])`, "jid"},
		{`count_over_time({app="x"} | regexp "(?P<a>x)(?P<b>y)" [1m])`, "a,b"},
		{`count_over_time({app="x"} | pattern "<_> <method> <path>" [1m])`, "method,path"},
		{`sum by (jid) (count_over_time({app="x"} | regexp "(?P<jid>\\w+)" [1m]))`, "jid"},
		{`count_over_time({app="x"} | regexp "(?P<a>x)" | pattern "<a> <b>" [1m])`, "a,b"},
		// Dynamic-key parsers name nothing in the query.
		{`count_over_time({app="x"} | json [1m])`, ""},
		{`count_over_time({app="x"} | logfmt [1m])`, ""},
		// An unnamed group extracts nothing in Loki either.
		{`count_over_time({app="x"} | regexp "(\\w+)" [1m])`, ""},
		{`{app="x"} | regexp "(?P<jid>\\w+)"`, "jid"},
		{`count_over_time({app="x"}[1m])`, ""},
		{`not a query`, ""},
	} {
		if got := strings.Join(ParserCaptureLabels(tc.query), ","); got != tc.want {
			t.Errorf("ParserCaptureLabels(%s) = %q, want %q", tc.query, got, tc.want)
		}
	}
}
