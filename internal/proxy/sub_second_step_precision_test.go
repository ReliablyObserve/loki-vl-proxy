package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// vlStepReadWithinFloat64 reports whether read, the nanoseconds VictoriaLogs
// makes of a step it was sent, is the step the proxy meant. VictoriaLogs scales
// the number in a duration by its unit as a float64 and truncates
// (lib/logstorage tryParseDuration, v1.49.0 and v1.53.0), so it holds a count
// exactly up to 2^53 ns (about 104 days) and, beyond that, the nearest float64
// (1024 ns apart near 7e18). No spelling of such a step reads back exactly; it
// is a step far longer than any range, one sample at the start.
func vlStepReadWithinFloat64(read, want int64) bool {
	if want <= 1<<53 {
		return read == want
	}
	diff := read - want
	return diff >= -1024 && diff <= 1024
}

// A step Loki accepts but VictoriaLogs cannot hold exactly. Loki v3.7.9 reads
// `7000000001` as float seconds scaled by float64(time.Second)
// (pkg/loghttp/params.go:191-203, 7.000000001e18 ns, under int64) and answers
// it: the range is not more than 11,000 steps (pkg/loghttp/query.go:510), the
// frontend keeps Milliseconds() of the float (7000000000999) and the range gets
// its one sample at the start. The proxy keeps the same milliseconds and sends
// them as an exact count; VictoriaLogs turns the count into 7000000000999000064
// ns, the nearest float64, which is the fuzz input that failed on main code.
//
// conformance: semantics/sub-second-step-grid
func TestHugeStepIsLokisAndVictoriaLogsReadsTheNearestFloat(t *testing.T) {
	for _, tc := range []struct {
		step      string
		wantMs    int64
		exactRead bool
	}{
		{"7000000001", 7000000000999, false},    // the fuzz input
		{"9007199.254740992", 9007199254, true}, // 2^53 ns, the last count float64 holds
		{"9007200", 0, true},                    // whole milliseconds already: left as sent
		{"7000000000.5", 7000000000499, false},  // Loki's own float truncation lands a millisecond short
		{"9223372036", 0, true},                 // the largest whole second under int64: whole milliseconds, left as sent
		{"9223372037", 0, true},                 // overflows int64: Loki's 400, the proxy leaves it
	} {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?step="+url.QueryEscape(tc.step), nil)
		wholeMs, sub := lokiWholeMillisecondStep(r)
		ref, refOK := lokiStep(tc.step)
		if tc.wantMs == 0 {
			if sub {
				t.Fatalf("step %s: rewritten to %dms, want it left as sent", tc.step, wholeMs)
			}
			continue
		}
		if !sub || wholeMs != tc.wantMs || !refOK || wholeMs != ref.Milliseconds() {
			t.Fatalf("step %s: %dms (rewritten %v), Loki keeps %dms (%v), want %dms", tc.step, wholeMs, sub, ref.Milliseconds(), refOK, tc.wantMs)
		}
		want := wholeMs * int64(time.Millisecond)
		read, ok := vlDurationNs(formatVLStep(withLokiStep(r, wholeMs).FormValue("step")))
		if !ok || !vlStepReadWithinFloat64(read, want) {
			t.Fatalf("step %s: VictoriaLogs reads %dns (%v), want %dns", tc.step, read, ok, want)
		}
		if tc.exactRead && read != want {
			t.Fatalf("step %s: VictoriaLogs reads %dns, want exactly %dns", tc.step, read, want)
		}
		if !tc.exactRead && read == want {
			t.Fatalf("step %s: expected a count float64 cannot hold, VictoriaLogs read it exactly", tc.step)
		}
	}
}
