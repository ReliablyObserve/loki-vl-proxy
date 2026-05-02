package proxy

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newOpt2Proxy(t testing.TB) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             "http://unused",
		Cache:                  cache.New(30*time.Second, 100),
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

var opt2TestEntry = map[string]interface{}{
	"_time":   "2026-01-15T10:30:00Z",
	"_msg":    `{"method":"GET","status":200,"path":"/api/users"}`,
	"_stream": `{app="api-gateway",level="info"}`,
	"app":     "api-gateway",
	"level":   "info",
	"method":  "GET",
	"status":  "200",
}

// TestOPT2_ClassifyEntryFieldsWithFlags_ParityWithOriginal verifies that the
// fast-path variant produces byte-identical output to the wrapper for all
// query types that affect parser flag classification.
func TestOPT2_ClassifyEntryFieldsWithFlags_ParityWithOriginal(t *testing.T) {
	queries := []string{
		`{app="api-gateway"} | json`,
		`{app="api-gateway"} | logfmt`,
		`{app="api-gateway"} | regexp "(?P<method>\\w+)"`,
		`{app="api-gateway"}`,
		`{app="api-gateway"} |= "error"`,
	}
	p := newOpt2Proxy(t)

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			smBuf1 := make(map[string]string)
			pfBuf1 := make(map[string]string)
			cache1 := make(map[string][]metadataFieldExposure, 8)
			labels1, sm1, pf1 := p.classifyEntryFields(opt2TestEntry, q, cache1, smBuf1, pfBuf1)

			classifyAsParsed := hasParserStage(q, "json") || hasParserStage(q, "logfmt")
			streamLabels := parseStreamLabels(asString(opt2TestEntry["_stream"]))
			smBuf2 := make(map[string]string)
			pfBuf2 := make(map[string]string)
			cache2 := make(map[string][]metadataFieldExposure, 8)
			labels2, sm2, pf2 := p.classifyEntryFieldsWithFlags(opt2TestEntry, streamLabels, classifyAsParsed, cache2, smBuf2, pfBuf2)

			if !reflect.DeepEqual(labels1, labels2) {
				t.Errorf("labels mismatch\n  original: %v\n  withFlags: %v", labels1, labels2)
			}
			if !reflect.DeepEqual(sm1, sm2) {
				t.Errorf("structuredMetadata mismatch\n  original: %v\n  withFlags: %v", sm1, sm2)
			}
			if !reflect.DeepEqual(pf1, pf2) {
				t.Errorf("parsedFields mismatch\n  original: %v\n  withFlags: %v", pf1, pf2)
			}
		})
	}
}

// TestOPT2_ReconstructLogLineWithFlag_ParityWithOriginal verifies the fast-path
// reconstruction variant produces byte-identical output to the original.
func TestOPT2_ReconstructLogLineWithFlag_ParityWithOriginal(t *testing.T) {
	queries := []string{
		`{app="api-gateway"}`,
		`{app="api-gateway"} | json`,
		`{app="api-gateway"} | logfmt`,
		`{app="api-gateway"} | regexp "(?P<m>\\w+)"`,
		`{app="api-gateway"} | pattern "<m> <_>"`,
		``,
	}
	entry := map[string]interface{}{
		"_time":  "2026-01-15T10:30:00Z",
		"_msg":   "GET /api/users 200",
		"method": "GET",
		"status": "200",
	}
	streamLabels := map[string]string{"app": "api-gateway"}
	msg := "GET /api/users 200"

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			want := reconstructLogLine(msg, entry, streamLabels, q)
			skipFlag := hasTextExtractionParser(q)
			got := reconstructLogLineWithFlag(msg, entry, streamLabels, skipFlag)
			// Both may produce JSON with non-deterministic key order; compare parsed.
			if got != want {
				var wantParsed, gotParsed interface{}
				wantIsJSON := json.Unmarshal([]byte(want), &wantParsed) == nil
				gotIsJSON := json.Unmarshal([]byte(got), &gotParsed) == nil
				if wantIsJSON && gotIsJSON {
					if !reflect.DeepEqual(wantParsed, gotParsed) {
						t.Errorf("reconstructLogLineWithFlag(%q): JSON content differs\n  got:  %v\n  want: %v", q, gotParsed, wantParsed)
					}
				} else {
					t.Errorf("reconstructLogLineWithFlag(%q):\n  got:  %q\n  want: %q", q, got, want)
				}
			}
		})
	}
}

// BenchmarkOPT2_ClassifyEntryFields_PerCall measures the original per-call cost
// (2 regex matches per entry). This is the pre-fix baseline.
func BenchmarkOPT2_ClassifyEntryFields_PerCall(b *testing.B) {
	p := newOpt2Proxy(b)
	q := `{app="api-gateway"} | json`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smBuf := make(map[string]string)
		pfBuf := make(map[string]string)
		ec := make(map[string][]metadataFieldExposure, 8)
		_, _, _ = p.classifyEntryFields(opt2TestEntry, q, ec, smBuf, pfBuf)
	}
}

// BenchmarkOPT2_ClassifyEntryFieldsWithFlags_PreComputed measures the fast-path
// cost (flags pre-computed once before the loop).
func BenchmarkOPT2_ClassifyEntryFieldsWithFlags_PreComputed(b *testing.B) {
	p := newOpt2Proxy(b)
	q := `{app="api-gateway"} | json`
	classifyAsParsed := hasParserStage(q, "json") || hasParserStage(q, "logfmt")
	streamLabels := parseStreamLabels(asString(opt2TestEntry["_stream"]))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smBuf := make(map[string]string)
		pfBuf := make(map[string]string)
		ec := make(map[string][]metadataFieldExposure, 8)
		_, _, _ = p.classifyEntryFieldsWithFlags(opt2TestEntry, streamLabels, classifyAsParsed, ec, smBuf, pfBuf)
	}
}
