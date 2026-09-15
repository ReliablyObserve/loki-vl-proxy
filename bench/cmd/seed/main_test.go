package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func labelKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

func TestBuildActiveServicesLabelSetsAreUnique(t *testing.T) {
	for _, n := range []int{1, 12, 13, 14, 15, 24, 25, 40, 100} {
		seen := map[string]int{}
		for i, svc := range buildActiveServices(n) {
			key := labelKey(streamLabels(svc))
			if prev, ok := seen[key]; ok {
				t.Fatalf("--services=%d: index %d and %d share label set %s", n, prev, i, key)
			}
			seen[key] = i
		}
	}
}

func TestBuildActiveServicesKeepsPoolForFirstCycle(t *testing.T) {
	got := buildActiveServices(len(services))
	for i := range services {
		if got[i] != services[i] {
			t.Fatalf("index %d changed: got %+v want %+v", i, got[i], services[i])
		}
	}
}

func TestHighCardinalityPodPoolKeysAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, svc := range buildActiveServices(30) {
		k := podPoolKey(svc)
		if seen[k] {
			t.Fatalf("pod pool key %q reused", k)
		}
		seen[k] = true
	}
}

func TestJSONLinesHaveNoInjectedMessageField(t *testing.T) {
	svcs := buildActiveServices(len(services))
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, svc := range svcs {
		if svc.format != "json" {
			continue
		}
		line := genLine(svc, ts)
		var obj map[string]any
		if err := json.Unmarshal([]byte(line.text), &obj); err != nil {
			t.Fatalf("%s: line is not JSON: %v", svc.app, err)
		}
		if _, ok := obj["_msg"]; ok {
			t.Fatalf("%s: line still carries an injected _msg field: %s", svc.app, line.text)
		}
		if obj["level"] != line.level {
			t.Fatalf("%s: metadata level %q differs from line level %v", svc.app, line.level, obj["level"])
		}
	}
}

type decodedEntry struct {
	labels string
	tsNs   int64
	line   string
	level  string
}

func decodeLoki(t *testing.T, body []byte) []decodedEntry {
	t.Helper()
	var payload struct {
		Streams []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode loki payload: %v", err)
	}
	var out []decodedEntry
	for _, s := range payload.Streams {
		for _, v := range s.Values {
			var tsStr, line string
			if err := json.Unmarshal(v[0], &tsStr); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(v[1], &line); err != nil {
				t.Fatal(err)
			}
			var ns int64
			fmt.Sscan(tsStr, &ns)
			e := decodedEntry{labels: labelKey(s.Stream), tsNs: ns, line: line}
			if len(v) == 3 {
				var md map[string]string
				if err := json.Unmarshal(v[2], &md); err != nil {
					t.Fatal(err)
				}
				if len(md) != 1 {
					t.Fatalf("unexpected structured metadata %v", md)
				}
				e.level = md["level"]
			}
			out = append(out, e)
		}
	}
	return out
}

func decodeVL(t *testing.T, body []byte, streamFields []string) []decodedEntry {
	t.Helper()
	var out []decodedEntry
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var row map[string]string
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("decode vl row: %v", err)
		}
		ts, err := time.Parse(time.RFC3339Nano, row["_time"])
		if err != nil {
			t.Fatal(err)
		}
		labels := map[string]string{}
		for _, f := range streamFields {
			if v, ok := row[f]; ok {
				labels[f] = v
			}
		}
		for k := range row {
			switch k {
			case "_time", "_msg", "level":
			default:
				if _, ok := labels[k]; !ok {
					t.Fatalf("vl row carries non-stream field %q not pushed to Loki", k)
				}
			}
		}
		out = append(out, decodedEntry{labels: labelKey(labels), tsNs: ts.UnixNano(), line: row["_msg"], level: row["level"]})
	}
	return out
}

func TestLokiAndVictoriaLogsReceiveIdenticalEntries(t *testing.T) {
	svcs := buildActiveServices(15)
	ts := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	for _, hc := range []bool{false, true} {
		var batches []streamBatch
		if hc {
			pool := map[string][]string{}
			for _, svc := range svcs {
				pool[podPoolKey(svc)] = []string{svc.app + "-pod-a", svc.app + "-pod-b"}
			}
			batches = buildStreamsHighCardinality(ts, 7, svcs, pool, 1)
		} else {
			batches = buildStreamsFor(ts, 7, svcs)
		}

		lokiBody, err := lokiPushBody(batches)
		if err != nil {
			t.Fatal(err)
		}
		vlBody, fields, err := vlJSONLineBody(batches)
		if err != nil {
			t.Fatal(err)
		}
		loki := decodeLoki(t, lokiBody)
		vl := decodeVL(t, vlBody, fields)
		if len(loki) != 15*7 || len(vl) != len(loki) {
			t.Fatalf("hc=%v: entry counts loki=%d vl=%d", hc, len(loki), len(vl))
		}
		for i := range loki {
			if loki[i] != vl[i] {
				t.Fatalf("hc=%v entry %d differs:\nloki=%+v\nvl=  %+v", hc, i, loki[i], vl[i])
			}
			if loki[i].line == "" {
				t.Fatalf("empty line at %d", i)
			}
		}
		if hc && !strings.Contains(strings.Join(fields, ","), "pod") {
			t.Fatalf("pod missing from stream fields %v", fields)
		}
	}
}

func TestTimestampsAreUniqueAcrossStreams(t *testing.T) {
	svcs := buildActiveServices(15)
	ts := time.Date(2026, 3, 4, 5, 6, 0, 0, time.UTC)
	seen := map[int64]string{}
	for batch := 0; batch < 3; batch++ {
		bts := ts.Add(time.Duration(batch) * 30 * time.Second)
		for _, b := range buildStreamsFor(bts, 21, svcs) {
			prev := int64(-1)
			for _, e := range b.entries {
				ns := e.ts.UnixNano()
				if other, dup := seen[ns]; dup {
					t.Fatalf("timestamp %d shared by %s and %s", ns, other, b.labels["app"]+"/"+b.labels["cluster"])
				}
				seen[ns] = b.labels["app"] + "/" + b.labels["cluster"]
				if ns <= prev {
					t.Fatalf("timestamps not increasing within stream %v", b.labels)
				}
				if e.ts.Before(bts) || !e.ts.Before(bts.Add(time.Second)) {
					t.Fatalf("timestamp %s outside the batch second", e.ts)
				}
				prev = ns
			}
		}
	}
	if len(seen) != 3*15*21 {
		t.Fatalf("got %d unique timestamps", len(seen))
	}
}

// TestLokiBenchConfigOnlyAddsParitySettings keeps the benchmark Loki config in
// sync with the CI e2e config: outside its header comment and the marked
// benchmark-only block the two files must be identical.
func TestLokiBenchConfigOnlyAddsParitySettings(t *testing.T) {
	base, err := os.ReadFile("../../../test/e2e-compat/loki-local-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	bench, err := os.ReadFile("../../../test/e2e-compat/loki-bench-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var kept, block []string
	inBlock, headerDone, sawBlock := false, false, false
	parentAtBlock := ""
	topLevel := ""
	for _, line := range strings.Split(string(bench), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case !headerDone && strings.HasPrefix(line, "#"):
			continue
		case trimmed == "# --- benchmark-only begin ---":
			inBlock, sawBlock, parentAtBlock = true, true, topLevel
			continue
		case trimmed == "# --- benchmark-only end ---":
			inBlock = false
			continue
		}
		headerDone = true
		if inBlock {
			// Keep indentation: nesting is part of the contract.
			if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				block = append(block, line)
			}
			continue
		}
		if line != "" && line[0] != ' ' && line[0] != '#' {
			topLevel = strings.TrimSuffix(strings.Fields(line)[0], ":")
		}
		kept = append(kept, line)
	}
	if strings.Join(kept, "\n") != string(base) {
		t.Fatal("loki-bench-config.yaml differs from loki-local-config.yaml outside the benchmark-only block; apply the base change to both files")
	}
	if !sawBlock || parentAtBlock != "limits_config" {
		t.Fatalf("benchmark-only block must sit inside limits_config, found under %q", parentAtBlock)
	}
	want := []string{
		"  shard_streams:",
		"    enabled: false",
		"  discover_log_levels: true",
	}
	if strings.Join(block, "\n") != strings.Join(want, "\n") {
		t.Fatalf("benchmark-only block keys or nesting changed:\n%s\nwant:\n%s", strings.Join(block, "\n"), strings.Join(want, "\n"))
	}
}

// TestLokiBenchNoCacheConfigOnlyDisablesResultsCaches keeps the cold-mode Loki
// config equal to the benchmark config except for the marked block that turns
// every query_range results cache off in place of `cache_results: true`.
func TestLokiBenchNoCacheConfigOnlyDisablesResultsCaches(t *testing.T) {
	bench, err := os.ReadFile("../../../test/e2e-compat/loki-bench-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cold, err := os.ReadFile("../../../test/e2e-compat/loki-bench-nocache-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	stripHeader := func(b []byte) []string {
		lines := strings.Split(string(b), "\n")
		for len(lines) > 0 && strings.HasPrefix(lines[0], "#") {
			lines = lines[1:]
		}
		return lines
	}
	var kept, block []string
	inBlock, sawBlock := false, false
	section := ""
	for _, line := range stripHeader(cold) {
		trimmed := strings.TrimSpace(line)
		switch trimmed {
		case "# --- benchmark-cold begin ---":
			if section != "query_range" {
				t.Fatalf("cold block must sit inside query_range, found under %q", section)
			}
			inBlock, sawBlock = true, true
			kept = append(kept, "  cache_results: true")
			continue
		case "# --- benchmark-cold end ---":
			inBlock = false
			continue
		}
		if inBlock {
			if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				block = append(block, line)
			}
			continue
		}
		if line != "" && line[0] != ' ' && line[0] != '#' {
			section = strings.TrimSuffix(strings.Fields(line)[0], ":")
		}
		kept = append(kept, line)
	}
	if !sawBlock {
		t.Fatal("cold config lacks the benchmark-cold block")
	}
	if strings.Join(kept, "\n") != strings.Join(stripHeader(bench), "\n") {
		t.Fatal("loki-bench-nocache-config.yaml differs from loki-bench-config.yaml outside the benchmark-cold block; apply the change to both files")
	}
	want := []string{
		"  cache_results: false",
		"  cache_index_stats_results: false",
		"  cache_volume_results: false",
		"  cache_instant_metric_results: false",
		"  cache_series_results: false",
		"  cache_label_results: false",
	}
	if strings.Join(block, "\n") != strings.Join(want, "\n") {
		t.Fatalf("benchmark-cold block changed:\n%s\nwant:\n%s", strings.Join(block, "\n"), strings.Join(want, "\n"))
	}
}

// fakeBackends serves the Loki and VictoriaLogs endpoints the seed uses.
type fakeBackends struct {
	loki, vl         *httptest.Server
	lokiPushes       atomic.Int32
	vlPushes         atomic.Int32
	lokiLabels       string // /loki/api/v1/labels answer
	vlLines          string // lines in the target span
	failLokiPushFrom int32  // 1-based push number that starts failing; 0 = never
}

func newFakeBackends(t *testing.T) *fakeBackends {
	t.Helper()
	f := &fakeBackends{lokiLabels: `{"status":"success"}`, vlLines: "0"}
	f.loki = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/loki/api/v1/labels":
			_, _ = w.Write([]byte(f.lokiLabels))
		case "/loki/api/v1/label/app/values":
			_, _ = w.Write([]byte(`{"status":"success","data":["unrelated"]}`))
		case "/loki/api/v1/push":
			n := f.lokiPushes.Add(1)
			if f.failLokiPushFrom > 0 && n >= f.failLokiPushFrom {
				http.Error(w, "ingestion rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected Loki request %s", r.URL.Path)
		}
	}))
	f.vl = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			_, _ = w.Write([]byte(`{"lines":"` + f.vlLines + `"}`))
		case "/select/logsql/field_values":
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/insert/jsonline":
			f.vlPushes.Add(1)
		default:
			t.Errorf("unexpected VictoriaLogs request %s", r.URL.Path)
		}
	}))
	t.Cleanup(func() { f.loki.Close(); f.vl.Close() })
	return f
}

func (f *fakeBackends) config() seedConfig {
	return seedConfig{
		lokiURL: f.loki.URL, vlURL: f.vl.URL, serviceCount: 3, linesPerBatch: 2,
		batchInterval: time.Minute, span: 10 * time.Minute, now: time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC),
	}
}

func TestSeedRefusesExistingDataUnlessForced(t *testing.T) {
	f := newFakeBackends(t)
	f.lokiLabels = `{"status":"success","data":["app","service_name"]}`
	f.vlLines = "42"
	var out bytes.Buffer
	err := run(context.Background(), f.config(), &out)
	if err == nil || !strings.Contains(err.Error(), "refusing to seed") || !strings.Contains(err.Error(), "Loki holds streams") || !strings.Contains(err.Error(), "VictoriaLogs holds 42 lines") {
		t.Fatalf("expected a refusal naming both backends, got %v", err)
	}
	if f.lokiPushes.Load() != 0 || f.vlPushes.Load() != 0 {
		t.Fatal("refused seed must not push")
	}
	cfg := f.config()
	cfg.force = true
	if err := run(context.Background(), cfg, &out); err != nil {
		t.Fatalf("--force: %v", err)
	}
	if f.lokiPushes.Load() != 10 || f.vlPushes.Load() != 10 || !strings.Contains(out.String(), "--force: seeding over existing data") {
		t.Fatalf("forced seed pushed loki=%d vl=%d\n%s", f.lokiPushes.Load(), f.vlPushes.Load(), out.String())
	}
}

func TestSeedStopsOnPushError(t *testing.T) {
	f := newFakeBackends(t)
	f.failLokiPushFrom = 3
	err := run(context.Background(), f.config(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "Loki push") || !strings.Contains(err.Error(), "HTTP 429") || !strings.Contains(err.Error(), "re-seed on a fresh stack") {
		t.Fatalf("expected a push error, got %v", err)
	}
	if f.lokiPushes.Load() != 3 || f.vlPushes.Load() != 2 {
		t.Fatalf("seed kept pushing after the failure: loki=%d vl=%d", f.lokiPushes.Load(), f.vlPushes.Load())
	}
}

func TestNginxLinesCarryNoLevel(t *testing.T) {
	svc := services[4]
	if svc.format != "nginx" {
		t.Fatalf("pool changed: %+v", svc)
	}
	line := genLine(svc, time.Now())
	if line.level != "" {
		t.Fatalf("nginx line got level %q", line.level)
	}
	body, _ := lokiPushBody([]streamBatch{{labels: streamLabels(svc), entries: []entry{{ts: time.Now(), line: line}}}})
	if strings.Contains(string(body), `"level"`) {
		t.Fatalf("nginx push carries structured metadata: %s", body)
	}
}
