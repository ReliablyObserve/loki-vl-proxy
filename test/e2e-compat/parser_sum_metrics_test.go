//go:build e2e

package e2e_compat

import (
	"fmt"
	"strconv"
	"testing"
)

// A sum with no clause of a count or a rate over bare json and logfmt parsers runs no parser in Loki: the count Logs
// Drilldown sends on every service page answers the number of selected lines (#1821, item 4). The proxy writes it
// with no parser pipe and sends no parser probe (semantics/ungrouped-sum-skips-parsers). Over a stream of JSON
// objects, logfmt lines, plain text and broken JSON, each skipped sum, at an instant and over a range, answers Loki's
// series and values (also over keys named `_time`, which made the earlier unpack pipes miss lines); a grouped
// control answers like Loki as before.
//
// conformance: semantics/ungrouped-sum-skips-parsers, semantics/two-parsers-keep-one, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UngroupedParserSumsLikeLoki(t *testing.T) {
	app, now := unpackFixture(t, "parsersum", `level=info msg="logfmt %d"`, `plain text %d`, `{"a":"cut %d"`, `a="unterminated %d`,
		// Keys named like VictoriaLogs' own fields: the unpack pipes the proxy wrote before let them replace the line's.
		`_time=2020-01-01T00:00:00Z a=%d`, `{"_time":"later","b":"%d"}`)
	s := fmt.Sprintf(`{app=%q}`, app)
	for i, query := range []string{
		`sum(count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum(count_over_time(` + s + ` | json | logfmt [5m]))`,
		`sum(count_over_time(` + s + ` | logfmt | json [5m]))`,
		`sum(count_over_time(` + s + ` | logfmt [5m]))`,
		`sum(rate(` + s + ` | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum by () (count_over_time(` + s + ` |= "packed" | json | logfmt [5m] offset 2m))`,
		`sum(count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [5m])) * 2`,
		`sum by (agent) (count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [5m]))`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(endpoint+"/"+strconv.Itoa(i), func(t *testing.T) {
				unpackAgainstLoki(t, query, endpoint, now)
			})
		}
	}
}
