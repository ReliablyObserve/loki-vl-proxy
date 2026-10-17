package proxy

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// scanFixture is a window of JSON lines without any level, in a stream that has
// a detected_level label or not.
func scanFixture(s0 time.Time, streamLevel bool) []pushdownRow {
	stream := map[string]string{"app": "api", "env": "prod"}
	if streamLevel {
		stream["detected_level"] = "warn"
	}
	var rows []pushdownRow
	for i := 0; i < 30; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i+1) * 10 * time.Second), stream: stream, msg: `{"msg":"x","n":` + strconv.Itoa(i) + `}`})
	}
	return rows
}

// A selector of 12.5 KB, in the band where the keys probe fits VictoriaLogs'
// 16 KiB query length only without its gate (the gate adds 2.5 KB), still gets
// every request of the pushdown within the limit and the answer of the
// shorter selector: no probe is refused.
// conformance: semantics/json-metric-detected-level-derivation
func TestLongSelectorKeepsEveryRequestWithinTheQueryLimit(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(6 * time.Minute)
	var names []string
	for i := 0; len(strings.Join(names, "|")) < 12500; i++ {
		names = append(names, strings.Repeat("n", 100)+strconv.Itoa(i))
	}
	selector := `{env="prod", app=~"api|` + strings.Join(names, "|") + `"}`
	query := `sum by (detected_level) (count_over_time(` + selector + ` | json | drop __error__ [5m]))`
	srv, fake := newPushdownFakeVL(t, scanFixture(s0, false), nil)
	fake.presence = true
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	code, got, body := detectedLevelQuery(t, p, query, end, end, time.Minute)
	if code != http.StatusOK || len(got) == 0 {
		t.Fatalf("status %d %.200s %v", code, body, got)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	longest, keys := 0, 0
	for _, q := range append(append(append([]string(nil), fake.stats...), fake.guards...), fake.presenceCalls...) {
		longest = max(longest, len(q))
		if strings.Contains(q, "(?s:.*)") && !strings.Contains(q, "unpack_json") { // the keys probe, not the unpack probe
			keys++
			if strings.Contains(q, "seq(") {
				t.Errorf("the keys probe of a long selector kept its gate (%d bytes)", len(q))
			}
		}
	}
	if longest > vlDefaultMaxQueryLen || longest < 12500 || keys != 1 {
		t.Fatalf("longest request %d bytes (limit %d), keys probes %d", longest, vlDefaultMaxQueryLen, keys)
	}
}
