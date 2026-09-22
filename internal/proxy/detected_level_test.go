package proxy

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

type detectedLevelGoldenCase struct {
	Name         string            `json:"name"`
	Stream       map[string]string `json:"stream"`
	Fields       map[string]string `json:"fields"`
	Msg          string            `json:"msg"`
	JSONUnpacked bool              `json:"jsonUnpacked"`
	Source       string            `json:"source"`
	Want         string            `json:"want"`
	Note         string            `json:"note"`
}

func loadDetectedLevelGolden(t testing.TB) []detectedLevelGoldenCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/detected_level_golden.json")
	if err != nil {
		t.Fatalf("read golden table: %v", err)
	}
	var cases []detectedLevelGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("decode golden table: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("golden table is empty")
	}
	return cases
}

func goldenSortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// goldenLevelFields fills levelFields in VictoriaLogs' field order (sorted).
func goldenLevelFields(m map[string]string) *levelFields {
	var f levelFields
	for _, k := range goldenSortedKeys(m) {
		f.observe([]byte(k), []byte(m[k]))
	}
	return &f
}

// goldenReconstructedLine is the line returned for an unpacked row: the
// non-stream fields rebuilt by storedLogLineFromEntry.
func goldenReconstructedLine(msg string, fields map[string]string) string {
	entry := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		entry[k] = v
	}
	return storedLogLineFromEntry(msg, entry, nil, nil, "")
}

func TestDetectedLevelGoldenTable(t *testing.T) {
	sources := map[string]int{}
	for _, tc := range loadDetectedLevelGolden(t) {
		sources[tc.Source]++
		t.Run(tc.Name, func(t *testing.T) {
			body := tc.Msg
			if tc.JSONUnpacked {
				body = goldenReconstructedLine(tc.Msg, tc.Fields)
			}
			got := deriveDetectedLevel(detectedLevelInput{
				stream:       goldenLevelFields(tc.Stream),
				fields:       goldenLevelFields(tc.Fields),
				body:         []byte(body),
				jsonUnpacked: tc.JSONUnpacked,
				bodyScan:     true,
			}).String()
			if got != tc.Want {
				t.Fatalf("detected_level = %q, want %q (msg %q, stream %v, fields %v) %s", got, tc.Want, truncateForLog(tc.Msg), tc.Stream, tc.Fields, tc.Note)
			}
		})
	}
	for _, source := range []string{"loki-test", "loki-live", "loki-oracle", "victorialogs"} {
		if sources[source] == 0 {
			t.Fatalf("golden table has no %q cases", source)
		}
	}
}

func truncateForLog(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

func TestDetectedLevelWithoutBodyScan(t *testing.T) {
	cases := []struct {
		name   string
		stream map[string]string
		fields map[string]string
		msg    string
		want   string
	}{
		{name: "stored level still normalised", fields: map[string]string{"level": "WARNING"}, msg: "error", want: "warn"},
		{name: "severity number still mapped", fields: map[string]string{"severity_number": "17"}, msg: "info", want: "error"},
		{name: "body keyword ignored", msg: "request failed with error", want: "unknown"},
		{name: "json body ignored", msg: `{"level":"error"}`, want: "unknown"},
		{name: "stream label kept", stream: map[string]string{"lvl": "x"}, msg: "error", want: "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveDetectedLevel(detectedLevelInput{
				stream: goldenLevelFields(tc.stream),
				fields: goldenLevelFields(tc.fields),
				body:   []byte(tc.msg),
			}).String()
			if got != tc.want {
				t.Fatalf("detected_level = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLevelFieldSlotCoversLokiFieldList(t *testing.T) {
	for i, name := range levelFieldNames {
		if got := levelFieldSlot([]byte(name)); got != i+levelSlotFirstName {
			t.Fatalf("levelFieldSlot(%q) = %d, want %d", name, got, i+levelSlotFirstName)
		}
	}
	if levelFieldSlot([]byte("detected_level")) != levelSlotDetected {
		t.Fatal("detected_level slot")
	}
	if levelFieldSlot([]byte("severity_number")) != levelSlotSeverityNumber {
		t.Fatal("severity_number slot")
	}
	for _, name := range []string{"", "lev", "levels", "LeVeL", "loglevel", "severity.text", "_msg"} {
		if got := levelFieldSlot([]byte(name)); got != -1 {
			t.Fatalf("levelFieldSlot(%q) = %d, want -1", name, got)
		}
	}
	if levelFieldNames[levelSlotSeverityText-levelSlotFirstName] != "severity_text" {
		t.Fatal("levelSlotSeverityText does not point at severity_text")
	}
}

func TestDetectedLevelRawValueIsNotRetainedAcrossCanonical(t *testing.T) {
	d := deriveDetectedLevel(detectedLevelInput{fields: goldenLevelFields(map[string]string{"level": "notice"}), bodyScan: true})
	if d.canonical != "" || string(d.raw) != "notice" || d.String() != "notice" || string(d.appendTo(nil)) != "notice" {
		t.Fatalf("raw value = %+v", d)
	}
	d = deriveDetectedLevel(detectedLevelInput{fields: goldenLevelFields(map[string]string{"level": "Err"}), bodyScan: true})
	if d.canonical != levelError || d.raw != nil {
		t.Fatalf("canonical value = %+v", d)
	}
}

var detectedLevelBenchSink detectedLevel

func BenchmarkDeriveDetectedLevel(b *testing.B) {
	long := []byte(strings.Repeat("request handled by worker pool ", 40) + "error")
	cases := []struct {
		name string
		in   detectedLevelInput
	}{
		{name: "stored", in: detectedLevelInput{fields: goldenLevelFields(map[string]string{"level": "warn"}), body: []byte("GET /api 200"), bodyScan: true}},
		{name: "stream", in: detectedLevelInput{stream: goldenLevelFields(map[string]string{"level": "info"}), body: []byte("GET /api 200"), bodyScan: true}},
		{name: "otel", in: detectedLevelInput{fields: goldenLevelFields(map[string]string{"severity_number": "13", "severity_text": "Warn"}), body: []byte("hello"), bodyScan: true}},
		{name: "json", in: detectedLevelInput{body: []byte(`{"ts":"2026-01-01T00:00:00Z","caller":"main.go:12","level":"info","msg":"request served"}`), bodyScan: true}},
		{name: "logfmt", in: detectedLevelInput{body: []byte(`ts=2026-01-01T00:00:00Z caller=main.go:12 level=info msg="request served" duration=12ms`), bodyScan: true}},
		{name: "plain", in: detectedLevelInput{body: []byte("2026-01-01 10:00:00.000  INFO 1234 --- [main] com.example.App : Started"), bodyScan: true}},
		{name: "plain-nolevel", in: detectedLevelInput{body: []byte("GET /api/v1/users/12345 200 512 12ms"), bodyScan: true}},
		{name: "long", in: detectedLevelInput{body: long, bodyScan: true}},
	}
	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				detectedLevelBenchSink = deriveDetectedLevel(bc.in)
			}
		})
	}
}
