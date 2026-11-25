//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
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

// levelAfterStageStages and levelAfterStageFilters span the queries of TestCompat_LevelFilterAfterStageLikeLoki: a
// stage the filter stands behind, and the filter.
var (
	levelAfterStageStages = []string{`json`, `logfmt`, `json a="a"`, `regexp "a=(?P<n>\\d)"`, `pattern "<_> a=<n>"`, `drop a | decolorize`,
		`drop level`, `keep detected_level, a`, `line_format "{{.a}} info"`, `label_format b=a`, `json | drop __error__, __error_details__`,
		`drop detected_level`, `keep a`}
	levelAfterStageFilters = []string{`detected_level="error"`, `detected_level!="info"`, `detected_level=~"error|warn"`}
)

// A filter on detected_level behind a stage selects the entries Loki selects, through the proxy, and behind a stage
// other than a json or logfmt parser the proxy's entries carry the detected_level labels Loki's do, category by
// category. Behind those parsers the labels differ on lines with a level key of their own
// (semantics/detected-level-labels-after-parser, open): only the selection is held there.
//
// conformance: semantics/level-filter-after-stage, severity-detected-level-derivation, loki_api_v1_query_range
func TestCompat_LevelFilterAfterStageLikeLoki(t *testing.T) {
	raw, err := os.ReadFile(levelAfterStagePath)
	if err != nil {
		t.Fatal(err)
	}
	var entries []levelAfterStageEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	run := fmt.Sprintf("lasq%d", time.Now().UnixNano())
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	stamps := pushLevelAfterStage(t, lokiURL, run, entries, base)
	pushLevelAfterStage(t, vlURL, run, entries, base)
	forceVLFlush(t)
	window := dlFixture{start: base, end: base.Add(time.Minute)}
	selector := fmt.Sprintf(`{cr_las=%q}`, run)
	lokiEntriesWhenAll(t, lokiURL, selector, window, len(entries))
	lokiEntriesWhenAll(t, proxyURL, selector, window, len(entries))
	names := map[string]string{}
	for i, e := range entries {
		names[stamps[i]] = e.Name
	}
	headers := map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"}
	level := func(e dlEntry) string {
		var out []string
		for _, c := range []struct {
			name   string
			labels map[string]string
		}{{"stream", e.stream}, {"metadata", e.metadata}, {"parsed", e.parsed}} {
			for _, k := range []string{"detected_level", "detected_level_extracted"} {
				if v, ok := c.labels[k]; ok {
					out = append(out, c.name+"."+k+"="+v)
				}
			}
		}
		return strings.Join(out, " ")
	}
	for _, stage := range levelAfterStageStages {
		for _, filter := range levelAfterStageFilters {
			query := selector + " | " + stage + " | " + filter
			t.Run(stage+" | "+filter, func(t *testing.T) {
				loki := dlGoldenEntries(t, lokiURL, query, window, headers)
				proxy := dlGoldenEntries(t, proxyURL, query, window, headers)
				for stamp, want := range loki {
					got, ok := proxy[stamp]
					if !ok {
						t.Errorf("%s: Loki selects it, the proxy does not", names[stamp])
						continue
					}
					if strings.HasPrefix(stage, "json") || stage == "logfmt" {
						continue
					}
					if g, w := level(got), level(want); g != w {
						t.Errorf("%s: proxy %q, Loki %q", names[stamp], g, w)
					}
				}
				if len(loki) == 0 && !strings.Contains(stage, "keep a") && !strings.Contains(stage, "drop detected_level") {
					t.Fatal("Loki selects no entry: refusing an empty comparison")
				}
				for stamp := range proxy {
					if _, ok := loki[stamp]; !ok {
						t.Errorf("%s: the proxy selects it, Loki does not", names[stamp])
					}
				}
			})
		}
	}
}
