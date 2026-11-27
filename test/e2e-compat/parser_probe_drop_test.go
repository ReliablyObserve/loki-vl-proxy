//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"
)

// The parser probe keeps one of `| json` and `| logfmt` (preferWorkingParser). A text pattern removed the other
// with everything up to the next `|`, so a stage that was the last one before the range took the range with it
// (#1821): the instant query answered 400 "log queries are not supported as an instant query type" and the range
// query answered log streams, where Loki answers 200 with the metric. The stage is now removed where the parser
// read it. Here the JSON lines make the probe keep json in `| json | logfmt [r]` (logfmt was cut) and the logfmt
// lines make it keep logfmt in `| logfmt | json [r]` (json was cut); each count is held to Loki's, instant and
// range, with an offset and under label_replace.
//
// conformance: semantics/parser-probe-drop-keeps-range, loki_api_v1_query, loki_api_v1_query_range
func TestCompat_ParserProbeDropKeepsTheRange(t *testing.T) {
	at := time.Now().Add(-20 * time.Minute).Truncate(time.Minute)
	stamp := time.Now().UnixNano()
	jsonLines := slidingLiveFixture{app: fmt.Sprintf("probe-drop-json-%d", stamp)}
	logfmtLines := slidingLiveFixture{app: fmt.Sprintf("probe-drop-logfmt-%d", stamp)}
	for i := 0; i < 60; i++ {
		ts := at.Add(-time.Hour + 30*time.Second + time.Duration(i)*time.Minute)
		jsonLines.lines = append(jsonLines.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf(`{"route":"/r%d","msg":"tick"}`, i%3)})
		logfmtLines.lines = append(logfmtLines.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf(`route=/r%d msg=tick`, i%3)})
	}
	ingestSlidingFixtures(t, jsonLines, logfmtLines)
	// Loki answers a fresh stream's range metric queries empty for a while; wait until it holds both streams.
	waitForLokiMetricDataSelector(t, jsonLines.selector())
	waitForLokiMetricDataSelector(t, logfmtLines.selector())
	for _, fx := range []struct {
		name, pipe string
		lines      slidingLiveFixture
	}{
		{"json kept", `| json | logfmt`, jsonLines},
		{"logfmt kept", `| logfmt | json`, logfmtLines},
	} {
		sel := fx.lines.selector() + " " + fx.pipe
		for _, q := range []string{
			`sum(count_over_time(` + sel + ` [1h]))`,
			`sum(count_over_time(` + sel + ` [30m] offset 30m))`,
			`label_replace(sum(count_over_time(` + sel + ` [1h])), "copy", "x", "", "")`,
		} {
			t.Run(fx.name+"/instant/"+q, func(t *testing.T) {
				loki := unwrapInstant(t, lokiURL, q, at)
				if len(loki) == 0 {
					t.Fatalf("%s: Loki answered no series", q)
				}
				if proxy := unwrapInstant(t, proxyURL, q, at); !reflect.DeepEqual(proxy, loki) {
					t.Fatalf("%s:\n proxy %v\n  Loki %v", q, proxy, loki)
				}
			})
		}
		q := `sum(count_over_time(` + sel + ` [5m]))`
		t.Run(fx.name+"/range/"+q, func(t *testing.T) {
			loki := nonSumRange(t, lokiURL, q, at.Add(-time.Hour), at, time.Minute)
			if len(loki) == 0 {
				t.Fatalf("%s: Loki answered no series", q)
			}
			if proxy := nonSumRange(t, proxyURL, q, at.Add(-time.Hour), at, time.Minute); !reflect.DeepEqual(proxy, loki) {
				t.Fatalf("%s:\n proxy %v\n  Loki %v", q, proxy, loki)
			}
		})
	}
}

// A log query_range that leaves start and end out reads the hour before now in Loki. The step alignment moves
// metric queries only, so the proxy sent the log read (and the parser probe of a query naming both parsers) to
// VictoriaLogs with no time range: it read the whole retention and answered lines older than the hour. Here the
// stream holds lines from 100 to 10 minutes ago; the answer holds only the last hour's, as Loki's does.
//
// conformance: semantics/parser-probe-drop-keeps-range, semantics/request-time-bounds, loki_api_v1_query_range
func TestCompat_LogRangeWithoutBoundsReadsTheLastHour(t *testing.T) {
	now := time.Now()
	fx := slidingLiveFixture{app: fmt.Sprintf("range-no-bounds-%d", now.UnixNano())}
	for i := 0; i < 46; i++ {
		fx.lines = append(fx.lines, slidingLiveLine{ts: now.Add(-100*time.Minute + time.Duration(i)*2*time.Minute), msg: fmt.Sprintf(`{"route":"/r%d","seq":%d}`, i%3, i)})
	}
	ingestSlidingFixtures(t, fx)
	for _, q := range []string{fx.selector() + ` | json | logfmt`, fx.selector() + ` | json`, fx.selector()} {
		t.Run(q, func(t *testing.T) {
			lines := func(base string) int {
				status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+url.Values{"query": {q}, "limit": {"100"}}.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				if status != http.StatusOK || resp["status"] != "success" {
					t.Fatalf("%s %s: %d %s", base, q, status, body)
				}
				n := 0
				for _, ts := range responseStreamTimestamps(resp) {
					n += len(ts)
				}
				return n
			}
			loki := lines(lokiURL)
			if loki < 20 || loki > 32 {
				t.Fatalf("%s: Loki answered %d lines, the last hour holds about 26 of the 46", q, loki)
			}
			if proxy := lines(proxyURL); proxy != loki {
				t.Fatalf("%s: proxy answered %d lines, Loki %d", q, proxy, loki)
			}
		})
	}
}
