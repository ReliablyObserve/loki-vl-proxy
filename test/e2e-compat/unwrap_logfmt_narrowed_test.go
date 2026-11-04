//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Unwrap metrics over `| logfmt` whose VictoriaLogs query unpacks only the
// unwrapped and grouped labels answer what Loki answers: bytes(), duration()
// and plain values, a key in another spelling (size-bytes), an empty value and
// a missing key (no sample), grouped by a stream label and by a parsed one,
// summed, as a rate and as a quantile. The in-process proxy sends the narrowed
// unpack for the sums and the rate.
// conformance: quality/unwrap-logfmt-narrowed-unpack, parser-logfmt, metric-series-identity, loki_api_v1_query_range
func TestRangeMetricCompatibilityUnwrapLogfmtNarrowedUnpack(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-6 * time.Hour).Truncate(time.Hour)
	app := fmt.Sprintf("unwrap-narrowed-%d", now.UnixNano())
	var vlRows strings.Builder
	values := make([][]any, 0, 180)
	for i := 0; i < 180; i++ {
		ts := s0.Add(7*time.Second + time.Duration(i)*10*time.Second)
		var line string
		switch i % 5 {
		case 0:
			line = fmt.Sprintf("level=info op=read size_bytes=%d ttl=%ds latency_ms=%d msg=ok", 1000+i, i%7+1, 10+i%13)
		case 1:
			line = fmt.Sprintf(`level=info op=write size_bytes=%dKB ttl=%dms latency_ms=%d msg="cache write"`, i%9+1, 100+i, 20+i%11)
		case 2:
			line = fmt.Sprintf("level=warn op=read size-bytes=%d ttl=1m30s latency_ms=%d", 2000+i, 30+i%5)
		case 3:
			line = fmt.Sprintf("level=info op=evict size_bytes= ttl= latency_ms=%d", 40+i%3)
		default:
			line = fmt.Sprintf("level=info op=read msg=miss n=%d", i)
		}
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": app, "detected_level": "unknown"})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		values = append(values, []any{strconv.FormatInt(ts.UnixNano(), 10), line, map[string]string{"detected_level": "unknown"}})
	}
	sel := ingestUnwrapRawFixture(t, app, vlRows.String(), values, s0)
	route, recorder := newJSONVolumeRouteProxy(t)
	start, end := s0.Add(5*time.Minute), s0.Add(30*time.Minute)
	for _, query := range []string{
		`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap bytes(size_bytes) [5m]))`,
		`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration(ttl) [5m]))`,
		`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap size_bytes [5m]))`,
		`sum by (op) (sum_over_time(` + sel + ` | logfmt | unwrap latency_ms [5m]))`,
		`sum by (service_name) (rate(` + sel + ` | logfmt | unwrap latency_ms [5m]))`,
		`quantile_over_time(0.9, ` + sel + ` | logfmt | unwrap latency_ms [5m]) by (op)`,
	} {
		t.Run(query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
			if len(loki) == 0 {
				t.Fatalf("Loki sanity: no series for %s", query)
			}
			assertSlidingParity(t, query+" [proxy]", loki, slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil))
			recorder.reset()
			assertSlidingParity(t, query+" [in-process]", loki, slidingRangeSeries(t, route, query, start, end, time.Minute, nil))
			narrowed := false
			for _, call := range recorder.reset() {
				narrowed = narrowed || strings.Contains(call, "| unpack_logfmt fields (") && strings.Contains(call, ") skip_empty_results")
			}
			if !narrowed && !strings.HasPrefix(query, "quantile_over_time") { // the quantile reads its histogram its own way
				t.Fatalf("expected a narrowed unpack_logfmt among the VictoriaLogs calls")
			}
		})
	}
}
