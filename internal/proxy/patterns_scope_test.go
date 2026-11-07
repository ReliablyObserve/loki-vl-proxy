package proxy

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// perLineStreams is n streams of one line each, as a stream label that changes per line (a trace id) makes them.
func perLineStreams(app string, at time.Time, n int) []stateRow {
	rows := make([]stateRow, n)
	for i := range rows {
		rows[i] = stateRow{stream: fmt.Sprintf("%s/trace-%04d", app, i), level: "info", msg: fmt.Sprintf("pipeline traces flushed received=%d dropped=%d", 100+i, i%7), at: at.Add(time.Duration(i) * time.Second)}
	}
	return rows
}

// With -patterns-scope=stream (Loki's), streams of one line each answer no pattern: each stream's cluster is the
// line itself, and no text reaches 30 lines. With -patterns-scope=selector, the selector's lines are mined by one
// miner per level and answer their pattern, from the state and from the exact read alike.
//
// conformance: loki_api_v1_patterns, patterns-loki-mining
func TestPatternsSelectorScopeMinesAcrossStreams(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(perLineStreams("otel", stateNow.Add(-50*time.Minute), 60)...)
	now := stateNow
	loki := newStateProxy(t, vl, &now, 1<<30)
	if got, _ := statePatterns(t, loki, "", `{app="otel"}`, now.Add(-time.Hour), now); got != "" {
		t.Fatalf("stream scope answered %q, want no pattern (Loki's answer)", got)
	}
	wide := newStateProxy(t, vl, &now, 1<<30)
	wide.patternsSelectorScope = true
	got, _ := statePatterns(t, wide, "", `{app="otel"}`, now.Add(-time.Hour), now)
	if !strings.Contains(got, "pipeline traces flushed received=<_> dropped=<_>=60") {
		t.Fatalf("selector scope answered %q, want the selector's pattern over its 60 lines", got)
	}
	exact := newStateProxy(t, vl, &now, 1<<30)
	exact.patternsSelectorScope = true
	exact.patternState = nil // the exact read alone
	if got, _ := statePatterns(t, exact, "", `{app="otel"}`, now.Add(-time.Hour), now); !strings.Contains(got, "=60") {
		t.Fatalf("exact read under selector scope answered %q, want the 60 lines in one pattern", got)
	}
}

// Two selectors over the same streams each mine every one of their lines into their own miner: a line the
// narrow selector read first is not skipped by the broad one, and a refresh answers the same.
//
// conformance: loki_api_v1_patterns, patterns-loki-mining
func TestPatternsSelectorScopeKeepsSelectorsApart(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(perLineStreams("api", stateNow.Add(-40*time.Minute), 35)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	p.patternsSelectorScope = true
	if narrow, _ := statePatterns(t, p, "", `{app="api", pod="trace-0001"}`, now.Add(-time.Hour), now); narrow != "" {
		t.Fatalf("one line answered %q, want no pattern (under 30 lines)", narrow)
	}
	broad, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	if !strings.HasSuffix(broad, "=35") {
		t.Fatalf("broad selector answered %q, want its 35 lines, the one the narrow selector read among them", broad)
	}
	now = stateNow.Add(time.Minute)
	if again, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now); again != broad {
		t.Fatalf("refresh answered %q, want %q", again, broad)
	}
}

// Under -patterns-scope=selector a JSON line whose message is too short for Loki to mine (under 4 tokens) is mined
// as the whole line, from the state and the exact read; Loki's scope mines none of them.
//
// conformance: loki_api_v1_patterns, patterns-loki-mining
func TestPatternsSelectorScopeMinesShortJSONMessagesAsTheWholeLine(t *testing.T) {
	vl := &fakeStateVL{}
	at := stateNow.Add(-50 * time.Minute)
	for i := 0; i < 40; i++ {
		vl.add(stateRow{stream: fmt.Sprintf("otel/p%d", i%4), level: "warn", at: at.Add(time.Duration(i) * time.Second),
			msg: fmt.Sprintf(`{"message": "pipeline traces/otlp flushed", "level": "warn", "received": %d, "dropped": %d}`, 1000+i, i%9)})
	}
	now := stateNow
	if got, _ := statePatterns(t, newStateProxy(t, vl, &now, 1<<30), "", `{app="otel"}`, now.Add(-time.Hour), now); got != "" {
		t.Fatalf("stream scope answered %q, want none (Loki mines no message under 4 tokens)", got)
	}
	for _, state := range []bool{true, false} {
		p := newStateProxy(t, vl, &now, 1<<30)
		p.patternsSelectorScope = true
		if !state {
			p.patternState = nil
		}
		got, _ := statePatterns(t, p, "", `{app="otel"}`, now.Add(-time.Hour), now)
		if !strings.Contains(got, `"pipeline traces/otlp flushed"`) || !strings.HasSuffix(got, "=40") {
			t.Fatalf("selector scope (state %v) answered %q, want the whole line's pattern over 40 lines", state, got)
		}
	}
}
