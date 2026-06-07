package proxy

import (
	"strings"
	"testing"
)

// FuzzExtractLogPatternsFromWindowEntries fuzzes the production crash site
// extractLogPatternsFromWindowEntriesWithStats(postprocess.go:390). Per the
// post-mortem of the v1.55.1 race, this function reads entry.Stream and
// entry.Msg and entry.Ts from a goroutine concurrent with main-thread
// mutation. The race fix lives at the callsite (snapshotEntriesForPatterns),
// so this fuzz target focuses on the OTHER class of bugs in this hot path:
// panics on adversarial input.
//
// Invariants checked on every run:
//   - never panics regardless of input
//   - returned pattern count never exceeds the requested limit
//   - returned bucket counts are non-negative
//   - never returns more bucket entries than scanned lines
//   - empty entries always yields nil + zero stats (idempotent)
//
// Seeds cover the cases we've actually seen in production: empty messages,
// JSON-shaped messages, logfmt-shaped messages, multibyte runes, very long
// lines, control characters, malformed timestamps, and extreme step values.
func FuzzExtractLogPatternsFromWindowEntries(f *testing.F) {
	f.Add("1700000000000000000", "GET /api/v1/users status=200", "info", "60", 100)
	f.Add("1700000000000000000", `{"level":"error","msg":"db timeout"}`, "error", "1m", 50)
	f.Add("0", "", "", "0", 0)
	f.Add("notatimestamp", "anything", "", "60", 10)
	f.Add("-1", strings.Repeat("a", 10000), "warn", "1h", 1000)
	f.Add("1700000000000000000", "\x00\x01\x02 binary garbage \xff", "info", "60", 10)
	f.Add("1700000000000000000", "你好 世界 — multibyte", "info", "60", 10)
	f.Add("99999999999999999999", "overflow", "info", "60", 10)

	f.Fuzz(func(t *testing.T, ts, msg, level, step string, limit int) {
		// Bound input size aggressively so each exec stays well under the
		// fuzz engine's per-iteration context deadline (the default 10 s
		// before fuzztime). Without this, a 4-worker CI runner can't get a
		// single exec to complete on the slowest inputs (huge msg + step="0"
		// + large limit pushes pattern mining into the seconds-per-exec
		// range) and the whole job dies with `context deadline exceeded`.
		//
		// Production callers never pass these adversarial shapes; we're
		// fuzzing the parser robustness, not the throughput.
		if limit < -2 {
			limit = -2
		}
		if limit > 1000 {
			limit = 1000
		}
		if len(msg) > 1024 {
			msg = msg[:1024]
		}
		if len(ts) > 64 {
			ts = ts[:64]
		}
		if len(step) > 32 {
			step = step[:32]
		}
		if len(level) > 64 {
			level = level[:64]
		}

		entries := []queryRangeWindowEntry{
			{
				Stream: map[string]string{"detected_level": level},
				Ts:     ts,
				Msg:    msg,
			},
			{
				Stream: map[string]string{"level": level},
				Ts:     ts,
				Msg:    msg,
			},
			{
				// Empty stream — covers the path where neither key is present.
				Stream: map[string]string{},
				Ts:     ts,
				Msg:    msg,
			},
		}

		patterns, stats := extractLogPatternsFromWindowEntriesWithStats(entries, step, limit)

		if limit > 0 && len(patterns) > limit {
			t.Fatalf("len(patterns)=%d exceeds limit=%d", len(patterns), limit)
		}
		if stats.scannedLines < 0 {
			t.Fatalf("scannedLines went negative: %d", stats.scannedLines)
		}
		if stats.observedLines > stats.scannedLines {
			t.Fatalf("observedLines=%d > scannedLines=%d", stats.observedLines, stats.scannedLines)
		}

		// Idempotence: nil entries returns nil patterns.
		emptyPatterns, emptyStats := extractLogPatternsFromWindowEntriesWithStats(nil, step, limit)
		if emptyPatterns != nil {
			t.Fatalf("empty entries returned non-nil patterns: %v", emptyPatterns)
		}
		if emptyStats != (patternExtractionStats{}) {
			t.Fatalf("empty entries returned non-zero stats: %+v", emptyStats)
		}
	})
}
