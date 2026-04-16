package proxy

import (
	"strings"
	"testing"
)

func TestPostprocessHelperBranchesCoverage(t *testing.T) {
	streams := []map[string]interface{}{
		{"stream": map[string]string{"app": "web"}, "values": "bad-values"},
		{"values": [][]string{{"1000"}, {"1001", "\x1b[31m  error  \x1b[0m"}}},
	}
	decolorizeStreams(streams)
	values := streams[1]["values"].([][]string)
	if values[1][1] != "  error  " {
		t.Fatalf("expected ansi sequences to be stripped, got %q", values[1][1])
	}

	formatted := cloneStreams([]map[string]interface{}{
		{"values": [][]string{{"1000"}, {"1001", "  padded line  "}}},
		{"stream": map[string]string{"app": "web-api"}, "values": [][]string{{"1002", "unchanged"}}},
	})
	applyLineFormatTemplate(formatted, `{{TrimSpace ._line}}|{{default "n/a" .app}}`)
	if got := formatted[0]["values"].([][]string)[1][1]; got != "padded line|n/a" {
		t.Fatalf("unexpected formatted line %q", got)
	}

	entries := []queryRangeWindowEntry{
		{Value: []interface{}{"only-timestamp"}},
		{Value: []interface{}{"bad-time", "GET /health 200 1ms"}},
		{Value: []interface{}{"1712311200000000000", ""}},
		{Stream: map[string]string{"level": "error"}, Value: []interface{}{"1712311200000000000", "POST /api/orders 500 10ms"}},
	}
	patterns := extractLogPatternsFromWindowEntries(entries, "10s", 10)
	if len(patterns) != 1 {
		t.Fatalf("expected one pattern from valid window entry, got %#v", patterns)
	}
	if patterns[0]["level"] != "error" {
		t.Fatalf("expected fallback level label, got %#v", patterns[0])
	}

	if got := parsePatternStepSeconds(""); got != 60 {
		t.Fatalf("expected default pattern step, got %d", got)
	}
	if got := parsePatternStepSeconds("15"); got != 15 {
		t.Fatalf("expected numeric pattern step 15, got %d", got)
	}
	if got := parsePatternStepSeconds("-5"); got != 60 {
		t.Fatalf("expected invalid pattern step to fall back to default, got %d", got)
	}

	if ts, ok := patternUnixSecondsFromEntry(map[string]interface{}{"timestamp": "2026-04-04T10:00:00Z"}); !ok || ts == 0 {
		t.Fatalf("expected timestamp field to be parsed, got %d %v", ts, ok)
	}
	if _, ok := patternUnixSecondsFromEntry(map[string]interface{}{"msg": "no time"}); ok {
		t.Fatalf("expected missing time fields to fail parsing")
	}

	if sim, params := getPatternSimilarity([]string{"GET", patternVarPlaceholder}, []string{"GET", "42"}); sim != 0.5 || params != 1 {
		t.Fatalf("unexpected similarity result sim=%v params=%d", sim, params)
	}
	if sim, params := getPatternSimilarity([]string{"GET"}, []string{"GET", "42"}); sim != 0 || params != -1 {
		t.Fatalf("expected mismatched token lengths to fail similarity, got sim=%v params=%d", sim, params)
	}

	template := []string{"GET", "200"}
	if merged := mergePatternTemplate(template, []string{"POST", "200"}); merged[0] != patternVarPlaceholder || merged[1] != "200" {
		t.Fatalf("unexpected merged template %#v", merged)
	}
	if merged := mergePatternTemplate([]string{"GET"}, []string{"GET", "200"}); len(merged) != 1 || merged[0] != "GET" {
		t.Fatalf("expected mismatched merge to keep original template, got %#v", merged)
	}

	tokenizer := newPatternLineTokenizer()
	if _, _, ok := tokenizer.Tokenize(""); ok {
		t.Fatalf("expected empty line tokenization to fail")
	}
	if _, _, ok := tokenizer.Tokenize(strings.Repeat("x", patternMaxLineLength+1)); ok {
		t.Fatalf("expected oversized line tokenization to fail")
	}
	tokens, spaces, ok := tokenizer.Tokenize("level=info path=/ready status=200")
	if !ok || len(tokens) == 0 {
		t.Fatalf("expected valid tokenization, got tokens=%#v spaces=%#v ok=%v", tokens, spaces, ok)
	}
	if got := tokenizer.Join([]string{patternVarPlaceholder, patternVarPlaceholder}, nil); got != patternVarPlaceholder {
		t.Fatalf("expected adjacent placeholders to deduplicate, got %q", got)
	}

	if isHexLike("token-xyz") {
		t.Fatalf("expected non-hex token to be rejected")
	}
}
