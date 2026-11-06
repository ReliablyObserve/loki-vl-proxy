package proxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
)

// patternMinerAnswers holds the patterns the miner gave for patternMinerCorpusLines (seeds 1 to 8, limit 1000), both
// extracted in one pass and merged from three windows, recorded before the miner read each token's placeholder
// once per line. Patterns are compared in a canonical order: clusters that join to the same pattern with the same
// total and level are returned in map order. LVP_PIN_PATTERN_MINER=1 records the file again.
const patternMinerAnswers = "testdata/pattern_miner_answers.jsonl.gz"

// conformance: loki_api_v1_patterns
func TestPatternMinerKeepsTheRecordedPatterns(t *testing.T) {
	var got bytes.Buffer
	for seed := int64(1); seed <= 8; seed++ {
		lines := patternMinerCorpusLines(seed, 400+int(seed)*150)
		extracted, _ := extractLogPatternsStreamWithStats(bytes.NewReader(patternMinerCorpusNDJSON(lines, 0)), "1m", 1000, defaultLogRowLevels())
		writePatternMinerAnswer(t, &got, fmt.Sprintf("extract/%d", seed), patternResultEntriesFromMaps(extracted))
		var merged []patternResultEntry
		third := len(lines) / 3
		for w := 0; w < 3; w++ {
			part, _ := extractLogPatternsStreamWithStats(bytes.NewReader(patternMinerCorpusNDJSON(lines[w*third:(w+1)*third], w)), "1m", 1000, defaultLogRowLevels())
			merged = mergePatternResultEntries(merged, patternResultEntriesFromMaps(part))
		}
		writePatternMinerAnswer(t, &got, fmt.Sprintf("merge/%d", seed), merged)
	}
	if os.Getenv("LVP_PIN_PATTERN_MINER") == "1" {
		var zipped bytes.Buffer
		zw := gzip.NewWriter(&zipped)
		if _, err := zw.Write(got.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(patternMinerAnswers, zipped.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("recorded %s", patternMinerAnswers)
		return
	}
	fh, err := os.Open(patternMinerAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if _, err := want.ReadFrom(zr); err != nil {
		t.Fatal(err)
	}
	gotLines, wantLines := strings.Split(got.String(), "\n"), strings.Split(want.String(), "\n")
	if len(gotLines) != len(wantLines) {
		t.Fatalf("%d answers, %d recorded", len(gotLines), len(wantLines))
	}
	for i := range gotLines {
		if gotLines[i] != wantLines[i] {
			t.Fatalf("answer %d differs from the recorded one:\n got  %.400s\n want %.400s", i, gotLines[i], wantLines[i])
		}
	}
}

// writePatternMinerAnswer writes one answer as a JSON line, its patterns in a canonical order.
func writePatternMinerAnswer(t *testing.T, w *bytes.Buffer, id string, entries []patternResultEntry) {
	t.Helper()
	type row struct {
		Pattern string     `json:"pattern"`
		Level   string     `json:"level,omitempty"`
		Samples [][2]int64 `json:"samples"`
	}
	rows := make([]row, 0, len(entries))
	for _, e := range entries {
		r := row{Pattern: e.Pattern, Level: e.Level}
		for _, s := range e.Samples {
			ts, _ := numberToInt64(s[0])
			n, _ := numberToInt64(s[1])
			r.Samples = append(r.Samples, [2]int64{ts, n})
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Pattern != rows[j].Pattern {
			return rows[i].Pattern < rows[j].Pattern
		}
		if rows[i].Level != rows[j].Level {
			return rows[i].Level < rows[j].Level
		}
		return fmt.Sprint(rows[i].Samples) < fmt.Sprint(rows[j].Samples)
	})
	b, err := json.Marshal(map[string]any{"id": id, "patterns": rows})
	if err != nil {
		t.Fatal(err)
	}
	w.Write(b)
	w.WriteByte('\n')
}

// patternMinerCorpusLines generates n log lines for the miner from seed: request, logfmt, connection, trace, JSON,
// worker and key=value lines with numbers, IPs, UUIDs, timestamps, paths and hex values, and random lines.
func patternMinerCorpusLines(seed int64, n int) []string {
	rng := rand.New(rand.NewSource(seed))
	words := []string{"user", "order", "cache", "db", "auth", "payment", "queue", "worker", "api", "session", "token", "retry"}
	verbs := []string{"GET", "POST", "PUT", "DELETE"}
	hex := func(k int) string {
		const h = "0123456789abcdef"
		b := make([]byte, k)
		for i := range b {
			b[i] = h[rng.Intn(16)]
		}
		return string(b)
	}
	uuid := func() string { return hex(8) + "-" + hex(4) + "-" + hex(4) + "-" + hex(4) + "-" + hex(12) }
	ip := func() string {
		return fmt.Sprintf("%d.%d.%d.%d", rng.Intn(256), rng.Intn(256), rng.Intn(256), rng.Intn(256))
	}
	ts := func() string {
		return fmt.Sprintf("2026-11-06T%02d:%02d:%02d.%03dZ", rng.Intn(24), rng.Intn(60), rng.Intn(60), rng.Intn(1000))
	}
	w := func() string { return words[rng.Intn(len(words))] }
	templates := []func() string{
		func() string {
			return fmt.Sprintf("%s /api/v1/%s/%d HTTP/1.1 status=%d duration_ms=%d", verbs[rng.Intn(4)], w(), rng.Intn(5000), []int{200, 201, 404, 500}[rng.Intn(4)], rng.Intn(900))
		},
		func() string {
			return fmt.Sprintf("level=info ts=%s caller=%s.go:%d msg=\"%s %s done\" id=%s", ts(), w(), rng.Intn(400), w(), w(), uuid())
		},
		func() string {
			return fmt.Sprintf("connection from %s:%d accepted for user %s", ip(), 1024+rng.Intn(60000), w())
		},
		func() string {
			return fmt.Sprintf("trace_id=%s span_id=%s %s took %dms", hex(32), hex(16), w(), rng.Intn(3000))
		},
		func() string {
			return fmt.Sprintf("{\"level\":\"%s\",\"msg\":\"%s failed\",\"code\":%d,\"path\":\"/var/lib/%s/%d.db\"}", []string{"error", "warn"}[rng.Intn(2)], w(), rng.Intn(600), w(), rng.Intn(99))
		},
		func() string {
			return fmt.Sprintf("%s worker %d processed %d items in %.3fs (0x%s)", ts(), rng.Intn(16), rng.Intn(100000), rng.Float64()*10, hex(10))
		},
		func() string {
			return fmt.Sprintf("user=%s action=%s result=%s latency=%dus retries=%d", w(), w(), []string{"ok", "denied", "timeout"}[rng.Intn(3)], rng.Intn(99999), rng.Intn(5))
		},
		func() string {
			alphabet := []rune("abcXYZ019 _-.:/=,;!?()[]{}\"'+$|~^<>@#%&*\t«»…✓🙂日é")
			k := 4 + rng.Intn(120)
			var b strings.Builder
			for j := 0; j < k; j++ {
				b.WriteRune(alphabet[rng.Intn(len(alphabet))])
			}
			return b.String()
		},
	}
	out := make([]string, n)
	for i := range out {
		out[i] = templates[rng.Intn(len(templates))]()
	}
	return out
}

// patternMinerCorpusNDJSON is lines as VictoriaLogs rows, ten a second from startMin hours, with the levels in turn.
func patternMinerCorpusNDJSON(lines []string, startMin int) []byte {
	var buf bytes.Buffer
	levels := []string{"info", "warn", "error", "debug", ""}
	for i, l := range lines {
		row := map[string]string{
			"_time": fmt.Sprintf("2026-11-06T%02d:%02d:%02dZ", (startMin+i/600)%24, (i/10)%60, i%60),
			"_msg":  l,
		}
		if lv := levels[i%len(levels)]; lv != "" {
			row["detected_level"] = lv
		}
		b, _ := json.Marshal(row)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}
