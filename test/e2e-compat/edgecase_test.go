//go:build e2e

// Edge case tests based on known VictoriaLogs issues and limitations
// discovered from GitHub issues, community reports, and VL team discussions.
//
// References:
//   - #1077: stream filter vs field filter performance gap
//   - #281:  log count differences between Loki and VL
//   - #91:   missing records with large body fields
//   - #263:  Vector structured metadata breaking ingestion
//   - Datasource plugin: sort ordering, variable interpolation
package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestSetup_IngestEdgeCaseData pushes logs that trigger known VL edge cases.
func TestSetup_IngestEdgeCaseData(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	now := time.Now()

	// 1. Large body field (VL issue #91)
	largeLine := strings.Repeat("A", 50000) + " error in large payload"
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-large-body", "namespace": "edge-tests", "level": "error",
		},
		Lines: []string{largeLine},
	})

	// 2. Logs with many labels / high cardinality (VL strength, Loki weakness)
	for i := range 5 {
		pushStream(t, now, streamDef{
			Labels: map[string]string{
				"app":      "edge-high-card",
				"namespace": "edge-tests",
				"level":    "info",
				"trace_id": fmt.Sprintf("trace_%05d", i),
				"user_id":  fmt.Sprintf("user_%03d", i),
				"request_id": fmt.Sprintf("req_%08d", i*1000+42),
			},
			Lines: []string{
				fmt.Sprintf(`{"method":"GET","path":"/api/v1/items/%d","status":200,"duration_ms":%d}`, i, i*10+5),
			},
		})
	}

	// 3. Logs with dots in field names (OTel semantic conventions)
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app":                "edge-otel-dots",
			"namespace":         "edge-tests",
			"k8s.cluster.name":  "us-east-1",
			"k8s.namespace.name": "production",
			"service.name":      "payment-api",
			"level":             "info",
		},
		Lines: []string{
			`{"msg":"processing payment","amount":42.50,"currency":"USD"}`,
		},
	})

	// 4. Multiline logs (stack traces)
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-multiline", "namespace": "edge-tests", "level": "error",
		},
		Lines: []string{
			"Exception in thread \"main\" java.lang.NullPointerException\n\tat com.example.App.main(App.java:10)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
		},
	})

	// 5. Empty message body
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-empty-msg", "namespace": "edge-tests", "level": "debug",
		},
		Lines: []string{""},
	})

	// 6. Unicode in labels and messages
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-unicode", "namespace": "edge-tests", "level": "info",
		},
		Lines: []string{
			`{"user":"太郎","message":"注文が完了しました","emoji":"🎉"}`,
		},
	})

	// 7. Special characters in values
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-special-chars", "namespace": "edge-tests", "level": "warn",
		},
		Lines: []string{
			`path="/api/v1/users?filter=name%3D%22john%22&sort=desc" status=200`,
			`SQL: SELECT * FROM users WHERE name = 'O''Brien' AND age > 30`,
			`regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
		},
	})

	// 8. Structured metadata (Loki 3.x feature) — push via Loki with metadata
	lokiPayloadWithMetadata := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": map[string]string{
					"app": "edge-structured-meta", "namespace": "edge-tests",
				},
				"values": [][]interface{}{
					{fmt.Sprintf("%d", now.UnixNano()), "log with structured metadata",
						map[string]string{"trace_id": "meta_trace_001", "span_id": "meta_span_001"}},
				},
			},
		},
	}
	body, _ := json.Marshal(lokiPayloadWithMetadata)
	resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Logf("Loki structured metadata push: failed: %v", err)
	} else {
		resp.Body.Close()
		t.Logf("Loki structured metadata push: %d", resp.StatusCode)
	}

	time.Sleep(3 * time.Second)
	t.Log("Edge case test data ingested")
}

// =============================================================================
// Edge Case: Large body fields (#91)
// =============================================================================

func TestEdge_LargeBodyField(t *testing.T) {
	score := &CompatScore{}
	q := `{app="edge-large-body"}`

	proxyResult := queryProxy(t, q)
	lokiResult := queryLoki(t, q)

	proxyLines := countLogLines(proxyResult)
	lokiLines := countLogLines(lokiResult)

	t.Logf("[large_body] Loki=%d, Proxy=%d", lokiLines, proxyLines)

	if checkStatus(proxyResult) {
		score.pass("large_body", "proxy returns success")
	} else {
		score.fail("large_body", "proxy error")
	}

	if proxyLines > 0 {
		score.pass("large_body", "proxy returns large body log")
	} else {
		score.fail("large_body", "proxy dropped large body log (known VL issue #91)")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: High cardinality labels
// =============================================================================

func TestEdge_HighCardinalityLabels(t *testing.T) {
	score := &CompatScore{}
	q := `{app="edge-high-card"}`

	proxyResult := queryProxy(t, q)
	proxyLines := countLogLines(proxyResult)

	t.Logf("[high_card] Proxy=%d lines", proxyLines)

	if proxyLines >= 5 {
		score.pass("high_card", fmt.Sprintf("proxy returns all %d high-cardinality logs", proxyLines))
	} else {
		score.fail("high_card", fmt.Sprintf("expected >=5 lines, got %d", proxyLines))
	}

	// Verify individual items are searchable by message content
	q2 := `{app="edge-high-card"} |= "items/2"`
	result := queryProxy(t, q2)
	lines := countLogLines(result)
	if lines >= 1 {
		score.pass("high_card", "can search specific item in high-cardinality logs")
	} else {
		score.fail("high_card", "cannot find specific item")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Dots in label names (OTel)
// =============================================================================

func TestEdge_DottedLabelNames(t *testing.T) {
	score := &CompatScore{}

	// Query using dotted label name
	q := `{app="edge-otel-dots"}`
	proxyResult := queryProxy(t, q)
	proxyLines := countLogLines(proxyResult)

	if proxyLines > 0 {
		score.pass("dotted_labels", "proxy returns logs with OTel dotted labels")
	} else {
		score.fail("dotted_labels", "proxy cannot find logs with dotted labels")
	}

	// Check that dotted label values appear in label values endpoint
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/service.name/values")
	if proxyResp != nil {
		values := extractStrings(proxyResp, "data")
		t.Logf("[dotted_labels] service.name values: %v", values)
		if contains(values, "payment-api") {
			score.pass("dotted_labels", "dotted label values accessible via API")
		} else {
			score.fail("dotted_labels", "dotted label values not found")
		}
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Multiline logs (stack traces)
// =============================================================================

func TestEdge_MultilineLogs(t *testing.T) {
	score := &CompatScore{}
	q := `{app="edge-multiline"} |= "NullPointerException"`

	proxyResult := queryProxy(t, q)
	proxyLines := countLogLines(proxyResult)

	t.Logf("[multiline] Proxy=%d lines", proxyLines)

	if proxyLines > 0 {
		score.pass("multiline", "proxy can search multiline stack traces")
	} else {
		score.fail("multiline", "proxy cannot find multiline logs")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Unicode content
// =============================================================================

func TestEdge_UnicodeContent(t *testing.T) {
	score := &CompatScore{}
	q := `{app="edge-unicode"}`

	proxyResult := queryProxy(t, q)
	proxyLines := countLogLines(proxyResult)

	if proxyLines > 0 {
		score.pass("unicode", "proxy handles unicode content")
	} else {
		score.fail("unicode", "proxy cannot find unicode logs")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Special characters
// =============================================================================

func TestEdge_SpecialCharacters(t *testing.T) {
	score := &CompatScore{}
	q := `{app="edge-special-chars"}`

	proxyResult := queryProxy(t, q)
	proxyLines := countLogLines(proxyResult)

	t.Logf("[special_chars] Proxy=%d lines", proxyLines)

	if proxyLines >= 3 {
		score.pass("special_chars", "proxy handles special chars in log lines")
	} else {
		score.fail("special_chars", fmt.Sprintf("expected >=3 lines, got %d", proxyLines))
	}

	// Search for SQL pattern
	q2 := `{app="edge-special-chars"} |= "SELECT"`
	result := queryProxy(t, q2)
	if countLogLines(result) > 0 {
		score.pass("special_chars", "can search SQL patterns in logs")
	} else {
		score.fail("special_chars", "cannot search SQL patterns")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Label value with regex special chars
// =============================================================================

func TestEdge_RegexSpecialCharsInFilter(t *testing.T) {
	score := &CompatScore{}

	// Search with regex that contains special chars
	q := `{namespace=~"edge-tests"} |~ "\\[a-zA-Z0-9"`
	proxyResult := queryProxy(t, q)
	if checkStatus(proxyResult) {
		score.pass("regex_special", "proxy handles regex with special chars")
	} else {
		score.fail("regex_special", "proxy fails on regex with special chars")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Querying with POST (large queries)
// =============================================================================

func TestEdge_PostQueryRange(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{namespace="edge-tests"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	resp, err := http.Post(
		proxyURL+"/loki/api/v1/query_range",
		"application/x-www-form-urlencoded",
		strings.NewReader(params.Encode()),
	)
	if err != nil {
		score.fail("post_query", err.Error())
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		score.pass("post_query", "POST query_range returns 200")
	} else {
		score.fail("post_query", fmt.Sprintf("POST returned %d", resp.StatusCode))
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Empty query (all logs)
// =============================================================================

func TestEdge_WildcardQuery(t *testing.T) {
	score := &CompatScore{}
	q := `{namespace="edge-tests"}`

	proxyResult := queryProxy(t, q)
	proxyLines := countLogLines(proxyResult)

	t.Logf("[wildcard] All edge-tests logs: %d", proxyLines)

	if proxyLines >= 10 {
		score.pass("wildcard", fmt.Sprintf("proxy returns %d logs for namespace query", proxyLines))
	} else {
		score.fail("wildcard", fmt.Sprintf("expected >=10 logs, got %d", proxyLines))
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Structured metadata (Loki 3.x)
// =============================================================================

func TestEdge_StructuredMetadata(t *testing.T) {
	score := &CompatScore{}

	// Query Loki for structured metadata
	q := `{app="edge-structured-meta"}`
	lokiResult := queryLoki(t, q)
	lokiLines := countLogLines(lokiResult)

	t.Logf("[structured_meta] Loki=%d lines", lokiLines)

	// In Loki 3.x, structured metadata (trace_id, span_id) should be
	// searchable as labels. This is a Loki-only feature.
	if lokiLines > 0 {
		score.pass("structured_meta", "Loki stores structured metadata")
	}

	// The proxy doesn't need to support structured metadata ingestion,
	// but should handle queries that reference metadata fields gracefully
	proxyResult := queryProxy(t, q)
	if checkStatus(proxyResult) {
		score.pass("structured_meta", "proxy handles query for structured metadata logs")
	}

	score.report(t)
}
