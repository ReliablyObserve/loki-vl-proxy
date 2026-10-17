package proxy

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func countPartialParseProbes(guards []string) int {
	n := 0
	for _, g := range guards {
		if strings.Contains(g, "unpack_json fields (level) keep_original_fields | filter (level:=") {
			n++
		}
	}
	return n
}

// A plan that groups by a label and reads detected_level asks twice on rows with
// no stored level: the stored-level query, then the chain. The probes of the
// label (main's partial-parse probe) are the same query both times and are
// remembered apart from the level probes, so the chain takes their verdict from
// the attempt: one request on a cold window, none on the next ask, and a line
// that makes the label risky still sends the query to the raw-row evaluator.
// conformance: semantics/json-metric-detected-level-derivation
func TestLabelProbesRunOnceForTheAttemptAndTheChain(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(6 * time.Minute)
	const query = `sum by (level, detected_level) (count_over_time({env="prod"} | json | drop __error__ [5m]))`
	srv, fake := newPushdownFakeVL(t, scanFixture(s0, false), nil)
	fake.presence = true
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	for i, want := range []int{1, 1} { // the second ask of the same window adds none
		if code, got, body := detectedLevelQuery(t, p, query, end, end, time.Minute); code != http.StatusOK || len(got) == 0 {
			t.Fatalf("ask %d: status %d %.200s %v", i, code, body, got)
		}
		fake.mu.Lock()
		n, raw := countPartialParseProbes(fake.guards), fake.raw
		fake.mu.Unlock()
		if n != want || raw != 0 {
			t.Fatalf("ask %d: %d partial-parse probes (want %d), %d raw requests", i, n, want, raw)
		}
	}

	// A line VictoriaLogs does not parse as Loki does (text after the closing brace) is
	// found by the label probe, whichever ask runs it.
	rows := append(scanFixture(s0, false), pushdownRow{ts: s0.Add(5 * time.Minute), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"level":"warn"} trailing`})
	srv, fake = newPushdownFakeVL(t, rows, nil)
	fake.presence = true
	p = newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	if code, _, body := detectedLevelQuery(t, p, query, end, end, time.Minute); code != http.StatusOK {
		t.Fatalf("status %d %.200s", code, body)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw == 0 {
		t.Fatalf("the risky line did not send the query to the raw-row evaluator (%d partial-parse probes)", countPartialParseProbes(fake.guards))
	}
}
