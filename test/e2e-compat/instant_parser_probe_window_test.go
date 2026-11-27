//go:build e2e

package e2e_compat

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// An instant query that names both `| json` and `| logfmt` makes the proxy
// read 25 recent lines from VictoriaLogs to choose one parser
// (preferWorkingParser). The instant handler passed start and end, which an
// instant query does not carry, so the read had no time range: VictoriaLogs
// scanned the whole retention (#1821) and the verdict came from the newest
// lines of the retention, not from the lines the query evaluates. Here the
// hour before the evaluation time holds logfmt lines and the half hour after it
// JSON lines. Loki runs both parsers and groups the logfmt lines by their
// route; with the verdict from the newer JSON lines the proxy dropped the logfmt
// stage and answered one series without route. The probe now reads the window
// the query evaluates, with an offset too.
//
// conformance: semantics/instant-parser-probe-window, semantics/two-parsers-keep-one, loki_api_v1_query
func TestCompat_InstantParserProbeReadsTheQueryWindow(t *testing.T) {
	at := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	fx := slidingLiveFixture{app: fmt.Sprintf("probe-window-%d", time.Now().UnixNano())}
	for i := 0; i < 60; i++ {
		fx.lines = append(fx.lines, slidingLiveLine{ts: at.Add(-time.Hour + 30*time.Second + time.Duration(i)*time.Minute), msg: fmt.Sprintf("msg=tick route=/r%d", i%3)})
	}
	for i := 0; i < 180; i++ {
		fx.lines = append(fx.lines, slidingLiveLine{ts: at.Add(10*time.Second + time.Duration(i)*10*time.Second), msg: `{"msg":"newer","route":"/r9"}`})
	}
	ingestSlidingFixtures(t, fx)
	pipe := fx.selector() + ` | json | logfmt | drop __error__, __error_details__`
	for _, tc := range []struct {
		name, query string
		at          time.Time
		want        map[string]string
	}{
		{"grouped by a logfmt label", `max by (route) (count_over_time(` + pipe + ` [1h]))`, at, map[string]string{`route=/r0`: "20", `route=/r1`: "20", `route=/r2`: "20"}},
		{"offset", `max by (route) (count_over_time(` + pipe + ` [1h] offset 1h))`, at.Add(time.Hour), map[string]string{`route=/r0`: "20", `route=/r1`: "20", `route=/r2`: "20"}},
		{"the Drilldown count", `sum(count_over_time(` + pipe + ` [1h]))`, at, map[string]string{``: "60"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := unwrapInstant(t, lokiURL, tc.query, tc.at)
			if !reflect.DeepEqual(loki, tc.want) {
				t.Fatalf("%s: Loki answered %v, the fixture holds %v", tc.query, loki, tc.want)
			}
			if proxy := unwrapInstant(t, proxyURL, tc.query, tc.at); !reflect.DeepEqual(proxy, loki) {
				t.Fatalf("%s:\n proxy %v\n  Loki %v", tc.query, proxy, loki)
			}
		})
	}
}
