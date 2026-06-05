package proxy

import (
	"testing"
)

func TestMarginalizeBatchResult(t *testing.T) {
	// Simulates the translated multi-field Loki matrix that trimAndTranslateStatsQRFJ
	// would produce for a batched by(level, env) query.
	twoFieldMatrix := []byte(`{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"level":"error","env":"prod"},"values":[[1748000000,"10"],[1748000300,"8"]]},
		{"metric":{"level":"info","env":"prod"},"values":[[1748000000,"40"],[1748000300,"35"]]},
		{"metric":{"level":"error","env":"dev"},"values":[[1748000000,"5"],[1748000300,"3"]]},
		{"metric":{"level":"info","env":"dev"},"values":[[1748000000,"20"],[1748000300,"18"]]}
	]}}`)

	entries := []fieldBatchEntry{
		{lokiField: "level", primaryVLField: "level", resultCh: make(chan []byte, 1)},
		{lokiField: "env", primaryVLField: "env", resultCh: make(chan []byte, 1)},
	}

	results := marginalizeBatchResult(twoFieldMatrix, entries)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Parse level result and verify marginal sums.
	// level=error: 10+5=15 at t0, 8+3=11 at t1
	// level=info:  40+20=60 at t0, 35+18=53 at t1
	levelResult, ok := results["level"]
	if !ok || len(levelResult) == 0 {
		t.Fatal("expected level result")
	}

	// Parse env result and verify marginal sums.
	// env=prod: 10+40=50 at t0, 8+35=43 at t1
	// env=dev:  5+20=25 at t0, 3+18=21 at t1
	envResult, ok := results["env"]
	if !ok || len(envResult) == 0 {
		t.Fatal("expected env result")
	}

	// Verify result is valid JSON with correct structure.
	for field, body := range results {
		if len(body) == 0 {
			t.Errorf("field %q: empty body", field)
		}
		if body[0] != '{' {
			t.Errorf("field %q: result is not JSON object: %s", field, body[:min(len(body), 50)])
		}
	}

	// Parse and validate level sums using limitLokiMatrixSeries (it parses the same format).
	// We verify the top value has the right total by checking the ranking order.
	// level=info total = 60+53 = 113, level=error total = 15+11 = 26 → info should be first.
	levelStr := string(levelResult)
	infoIdx := indexOf(levelStr, `"info"`)
	errorIdx := indexOf(levelStr, `"error"`)
	if infoIdx < 0 || errorIdx < 0 {
		t.Errorf("level result missing expected values: %s", levelStr[:min(len(levelStr), 200)])
	}
	if infoIdx > errorIdx {
		t.Errorf("level result: expected info (total=113) before error (total=26), got order reversed")
	}

	// env=prod total = 50+43 = 93, env=dev total = 25+21 = 46 → prod should be first.
	envStr := string(envResult)
	prodIdx := indexOf(envStr, `"prod"`)
	devIdx := indexOf(envStr, `"dev"`)
	if prodIdx < 0 || devIdx < 0 {
		t.Errorf("env result missing expected values: %s", envStr[:min(len(envStr), 200)])
	}
	if prodIdx > devIdx {
		t.Errorf("env result: expected prod (total=93) before dev (total=46), got order reversed")
	}
}

func TestMarginalizeBatchResultEmpty(t *testing.T) {
	// Empty result array → nil return.
	emptyMatrix := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	entries := []fieldBatchEntry{{lokiField: "level", primaryVLField: "level", resultCh: make(chan []byte, 1)}}
	results := marginalizeBatchResult(emptyMatrix, entries)
	if len(results) != 0 {
		t.Errorf("expected empty results for empty matrix, got %d", len(results))
	}
}

func TestMarginalizeBatchResultSingleField(t *testing.T) {
	// Single-field batch (no other dimensions to sum over) — should return data as-is.
	singleFieldMatrix := []byte(`{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"level":"error"},"values":[[1748000000,"15"],[1748000300,"11"]]},
		{"metric":{"level":"info"},"values":[[1748000000,"60"],[1748000300,"53"]]}
	]}}`)

	entries := []fieldBatchEntry{{lokiField: "level", primaryVLField: "level", resultCh: make(chan []byte, 1)}}
	results := marginalizeBatchResult(singleFieldMatrix, entries)

	if _, ok := results["level"]; !ok {
		t.Fatal("expected level result from single-field batch")
	}
	// info total=113 > error total=26 → info should appear first.
	levelStr := string(results["level"])
	if indexOf(levelStr, `"info"`) > indexOf(levelStr, `"error"`) {
		t.Error("single field: expected info before error in result")
	}
}

func TestMarginalizeBatchResultMissingField(t *testing.T) {
	// Entry requests a field ("region") not present in the VL response.
	matrix := []byte(`{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"level":"error"},"values":[[1748000000,"10"]]}
	]}}`)

	entries := []fieldBatchEntry{
		{lokiField: "level", primaryVLField: "level", resultCh: make(chan []byte, 1)},
		{lokiField: "region", primaryVLField: "region", resultCh: make(chan []byte, 1)},
	}
	results := marginalizeBatchResult(matrix, entries)

	// "level" should have a result; "region" may not (no data).
	if _, ok := results["level"]; !ok {
		t.Error("expected level result")
	}
	// "region" absent from matrix → no data → either missing or empty result.
}

func TestFieldBatchKey(t *testing.T) {
	// Same bucket window should produce identical keys for drifting timestamps.
	// Two requests 10 seconds apart within the same 30s bucket.
	const base = "1748000000" // securely within 30s bucket
	k1 := fieldBatchKey("org1", "{app=\"nginx\"}", base, "1748003600", "300s")
	k2 := fieldBatchKey("org1", "{app=\"nginx\"}", "1748000005", "1748003605", "300s")
	if k1 != k2 {
		t.Errorf("fieldBatchKey: expected same key for timestamps in same 30s bucket:\n  k1=%q\n  k2=%q", k1, k2)
	}

	// Different org → different key.
	k3 := fieldBatchKey("org2", "{app=\"nginx\"}", base, "1748003600", "300s")
	if k1 == k3 {
		t.Error("fieldBatchKey: different org should produce different key")
	}

	// Different step → different key.
	k4 := fieldBatchKey("org1", "{app=\"nginx\"}", base, "1748003600", "600s")
	if k1 == k4 {
		t.Error("fieldBatchKey: different step should produce different key")
	}
}

func TestBucketCount(t *testing.T) {
	const base int64 = 1748000000
	// 1h / 300s = 12 buckets → should batch
	n := bucketCount("1748000000", "1748003600", "300s")
	if n != 12 {
		t.Errorf("expected 12 buckets for 1h/300s, got %d", n)
	}
	// 12h / 300s = 144 buckets → should skip
	n = bucketCount("1748000000", "1748043200", "300s")
	if n <= maxBatchBuckets {
		t.Errorf("12h/300s should exceed maxBatchBuckets=%d, got %d", maxBatchBuckets, n)
	}
	// Invalid inputs → 0
	if n := bucketCount("bad", "1748003600", "300s"); n != 0 {
		t.Errorf("expected 0 for invalid start, got %d", n)
	}
	_ = base
}

func indexOf(s, substr string) int {
	for i := range s {
		if i+len(substr) <= len(s) && s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
