//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// The entries of the oracle of a detected_level filter after a stage
// (internal/queryplan/lower TestLevelFilterAfterStageMatchesLoki), each with the level Loki attached to it at ingest.
// TestCompat_LevelAfterStageIngestLevels pushes every entry to the stack's Loki as its own stream and compares the
// detected_level Loki gives it with the recorded one; with LVP_RECORD_LEVEL_AFTER_STAGE=1 it writes Loki's answers.

const levelAfterStagePath = "../../internal/queryplan/lower/testdata/level-after-stage-entries.json"

type levelAfterStageEntry struct {
	Name   string            `json:"name"`
	Stream map[string]string `json:"stream,omitempty"`
	Fields map[string]string `json:"fields,omitempty"`
	Line   string            `json:"line"`
	// Level is the detected_level Loki gave the entry (its stream label's, where the stream has one), and Extracted
	// the detected_level_extracted it added.
	Level     string `json:"level"`
	Extracted string `json:"extracted,omitempty"`
}

// pushLevelAfterStage pushes every entry to target as its own stream under run and returns the entries' timestamps.
func pushLevelAfterStage(t *testing.T, target, run string, entries []levelAfterStageEntry, base time.Time) []string {
	t.Helper()
	var streams []any
	stamps := make([]string, len(entries))
	for i, e := range entries {
		stamps[i] = strconv.FormatInt(base.Add(time.Duration(i)*time.Millisecond).UnixNano(), 10)
		labels := map[string]string{"cr_las": run, "cr_row": fmt.Sprintf("%02d", i)}
		for k, v := range e.Stream {
			labels[k] = v
		}
		value := []any{stamps[i], e.Line}
		if len(e.Fields) > 0 {
			value = append(value, e.Fields)
		}
		streams = append(streams, map[string]any{"stream": labels, "values": []any{value}})
	}
	path := "/loki/api/v1/push"
	if target == vlURL {
		path = "/insert/loki/api/v1/push?disable_message_parsing=1"
	}
	if status, msg := dlGoldenPost(t, target+path, "application/json", dlJSON(map[string]any{"streams": streams})); status/100 != 2 {
		t.Fatalf("push to %s: %d %s", target, status, msg)
	}
	return stamps
}

// lokiEntriesWhenAll polls the query until n entries are visible.
func lokiEntriesWhenAll(t *testing.T, target, query string, window dlFixture, n int) map[string]dlEntry {
	t.Helper()
	headers := map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"}
	deadline := time.Now().Add(90 * time.Second)
	for {
		entries := dlGoldenEntries(t, target, query, window, headers)
		if len(entries) == n {
			return entries
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s shows %d of %d entries of %s", target, len(entries), n, query)
		}
		time.Sleep(time.Second)
	}
}

// conformance: severity-detected-level-derivation, semantics/level-filter-after-stage
func TestCompat_LevelAfterStageIngestLevels(t *testing.T) {
	raw, err := os.ReadFile(levelAfterStagePath)
	if err != nil {
		t.Fatal(err)
	}
	var entries []levelAfterStageEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	run := fmt.Sprintf("las%d", time.Now().UnixNano())
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	stamps := pushLevelAfterStage(t, lokiURL, run, entries, base)
	window := dlFixture{start: base, end: base.Add(time.Minute)}
	got := lokiEntriesWhenAll(t, lokiURL, fmt.Sprintf(`{cr_las=%q}`, run), window, len(entries))
	record := os.Getenv("LVP_RECORD_LEVEL_AFTER_STAGE") == "1"
	for i, e := range entries {
		answer := got[stamps[i]]
		level, extracted := answer.metadata["detected_level"], answer.metadata["detected_level_extracted"]
		if v, ok := answer.stream["detected_level"]; ok {
			level = v
		}
		if record {
			entries[i].Level, entries[i].Extracted = level, extracted
			continue
		}
		if level != e.Level || extracted != e.Extracted {
			t.Errorf("%s: Loki gives level %q (extracted %q), recorded %q (%q); line %q", e.Name, level, extracted, e.Level, e.Extracted, e.Line)
		}
	}
	if record {
		dlGoldenWrite(t, levelAfterStagePath, entries)
		t.Logf("recorded %d entries from Loki %s", len(entries), dlGoldenBuildInfo(t)["version"])
	}
}

