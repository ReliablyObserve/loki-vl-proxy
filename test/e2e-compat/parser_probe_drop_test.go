//go:build e2e

package e2e_compat

import (
	"fmt"
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
