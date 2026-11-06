//go:build e2e

package e2e_compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

type countedPatternEntry struct {
	Pattern string          `json:"pattern"`
	Level   string          `json:"level"`
	Samples [][]json.Number `json:"samples"`
}

// fetchPatternsNoStep asks a Grafana datasource for /patterns as Logs Drilldown does: no step, so the backend
// derives it.
func fetchPatternsNoStep(t *testing.T, dsUID, query string, start, end time.Time) []countedPatternEntry {
	t.Helper()
	params := url.Values{"query": {query}, "start": {start.Format(time.RFC3339)}, "end": {end.Format(time.RFC3339)}}
	resp, err := http.Get(grafanaURL + "/api/datasources/uid/" + url.PathEscape(dsUID) + "/resources/patterns?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patterns from %s: status %d: %s", dsUID, resp.StatusCode, body)
	}
	var payload struct {
		Data []countedPatternEntry `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return payload.Data
}

// patternSteps is the sample timestamps of an answer, sorted, and the smallest gap between them.
func patternSteps(entries []countedPatternEntry) ([]int64, int64) {
	seen := map[int64]bool{}
	for _, e := range entries {
		for _, s := range e.Samples {
			ts, _ := s[0].Int64()
			seen[ts] = true
		}
	}
	steps := make([]int64, 0, len(seen))
	for ts := range seen {
		steps = append(steps, ts)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
	gap := int64(0)
	for i := 1; i < len(steps); i++ {
		if d := steps[i] - steps[i-1]; gap == 0 || d < gap {
			gap = d
		}
	}
	return steps, gap
}

// The proxy counts every line of a /patterns range, per step, at Loki's step: on data both backends hold, each
// pattern's samples are the lines seeded in each step (a number that never varies stays in the pattern, one that
// varies is <_>), with no zero samples and one entry per level and pattern; without a step, its step and sample
// timestamps are Loki's.
//
// conformance: drilldown/patterns-full-range-counts, drilldown/patterns-one-series-per-level, loki_api_v1_patterns
func TestDrilldown_Patterns_CountEveryLineAtLokisStep(t *testing.T) {
	waitForPatternABReady(t)

	cfg := stablePatternABConfig{rangeWindow: time.Hour, step: 30 * time.Second, batchSize: 1000}
	serviceName := fmt.Sprintf("pattern-counts-%d", time.Now().UTC().UnixNano())
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(time.Minute)
	end := start.Add(cfg.rangeWindow)
	const (
		alpha   = "pc_alpha component=collector status=200 took <_>"
		bravo   = "pc_bravo component=worker action=ship outcome=ok"
		charlie = "pc_charlie component=api code=500 retry later"
	)
	want := map[string]map[int64]int{alpha: {}, bravo: {}, charlie: {}}
	seedPatternMessagesToLokiAndVL(t, serviceName, start, end, cfg, func(bucket time.Time) []string {
		i := int(bucket.Sub(start) / cfg.step)
		ts := bucket.Unix()
		if !bucket.Before(end) {
			// Loki answers samples before the end only (pkg/pattern/drain Chunk.ForRange); a line at the end is
			// seeded but in no step of the answer.
			ts = -1
		}
		// Two lines of the same pattern in each step, which differ: Loki keeps one of two equal lines with the
		// same timestamp in a stream.
		msgs := []string{
			fmt.Sprintf("pc_alpha component=collector status=200 took %dms", 5+i%40),
			fmt.Sprintf("pc_alpha component=collector status=200 took %dms", 50+i%40),
		}
		want[alpha][ts] += 2
		if i%2 == 0 {
			msgs = append(msgs, "pc_bravo component=worker action=ship outcome=ok")
			want[bravo][ts]++
		}
		if i%9 == 0 {
			msgs = append(msgs, "pc_charlie component=api code=500 retry later")
			want[charlie][ts]++
		}
		return msgs
	})
	query := fmt.Sprintf(`{service_name=%q}`, serviceName)
	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	lokiUID := grafanaDatasourceUID(t, "Loki (direct)")

	// At the seeded step: every line, in its step.
	entries, err := fetchPatternsViaGrafanaDatasource(proxyUID, query, start, end, cfg.step, 1000)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]map[int64]int{}
	for _, e := range entries {
		if got[e.Pattern] != nil {
			t.Fatalf("two entries for pattern %q", e.Pattern)
		}
		got[e.Pattern] = map[int64]int{}
		for _, s := range e.Samples {
			ts := int64(s[0].(float64))
			n := int(s[1].(float64))
			if n != 0 {
				got[e.Pattern][ts] = n
			}
		}
	}
	for _, steps := range want {
		delete(steps, -1)
	}
	for pattern, steps := range want {
		if got[pattern] == nil {
			t.Fatalf("pattern %q missing from the proxy's answer %v", pattern, entries)
		}
		for ts, n := range steps {
			if got[pattern][ts] != n {
				t.Errorf("pattern %q at %d: %d lines, seeded %d", pattern, ts, got[pattern][ts], n)
			}
		}
		if len(got[pattern]) != len(steps) {
			t.Errorf("pattern %q has samples on %d steps, seeded %d", pattern, len(got[pattern]), len(steps))
		}
	}

	// As Logs Drilldown asks, without a step: every pattern both answer has Loki's samples, step for step, with
	// Loki's 0 on the steps between its first and last sample that have no lines.
	proxyNoStep := fetchPatternsNoStep(t, proxyUID, query, start, end)
	var lokiNoStep []countedPatternEntry
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(time.Second) {
		// Loki's pattern ingester answers a push after a few seconds.
		if lokiNoStep = fetchPatternsNoStep(t, lokiUID, query, start, end); len(lokiNoStep) > 0 || time.Now().After(deadline) {
			break
		}
	}
	samples := func(e countedPatternEntry) string {
		parts := make([]string, len(e.Samples))
		for i, s := range e.Samples {
			parts[i] = s[0].String() + ":" + s[1].String()
		}
		return strings.Join(parts, " ")
	}
	proxyByPattern := map[string]countedPatternEntry{}
	for _, e := range proxyNoStep {
		proxyByPattern[e.Level+" "+e.Pattern] = e
	}
	compared := 0
	for _, l := range lokiNoStep {
		p, ok := proxyByPattern[l.Level+" "+l.Pattern]
		if !ok {
			continue
		}
		compared++
		if samples(p) != samples(l) {
			t.Errorf("pattern %q: proxy samples\n %s\nLoki's\n %s", l.Pattern, samples(p), samples(l))
		}
	}
	if compared < 2 {
		t.Fatalf("only %d patterns with the same text on both: the comparison would prove little (Loki %v)", compared, lokiNoStep)
	}
	_, gap := patternSteps(proxyNoStep)
	t.Logf("compared %d patterns with Loki at a %ds step", compared, gap)
}
