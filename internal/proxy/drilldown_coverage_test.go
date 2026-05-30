package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	fj "github.com/valyala/fastjson"
)

func TestDrilldownHelpers_AdditionalCoverage(t *testing.T) {
	t.Run("synthetic service name and detected level", func(t *testing.T) {
		ensureSyntheticServiceName(nil)

		labels := map[string]string{"app": "api"}
		ensureSyntheticServiceName(labels)
		if got := labels["service_name"]; got != "api" {
			t.Fatalf("expected synthetic service_name from app, got %q", got)
		}

		labels["service_name"] = "frontend"
		ensureSyntheticServiceName(labels)
		if got := labels["service_name"]; got != "frontend" {
			t.Fatalf("expected existing service_name to win, got %q", got)
		}

		levelLabels := map[string]string{"level": "warn"}
		ensureDetectedLevel(levelLabels)
		if got := levelLabels["detected_level"]; got != "warn" {
			t.Fatalf("expected detected_level from level, got %q", got)
		}
	})

	t.Run("structured field exposure and detected field accumulation", func(t *testing.T) {
		lt := NewLabelTranslator(LabelStyleUnderscores, []FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}})

		if shouldExposeStructuredField("_time", nil, lt) {
			t.Fatal("expected internal field to be hidden")
		}
		if !shouldExposeStructuredField("service.name", map[string]string{"service.name": "api"}, lt) {
			t.Fatal("expected dotted field to remain exposed")
		}
		if shouldExposeStructuredField("service_name", map[string]string{"service_name": "api"}, nil) {
			t.Fatal("expected passthrough structured field to stay hidden without translator")
		}
		if !shouldExposeStructuredField("custom", map[string]string{}, lt) {
			t.Fatal("expected unknown structured field to be exposed")
		}

		fields := map[string]*detectedFieldSummary{}
		addDetectedField(fields, "", "", "string", nil, "ignored")
		addDetectedField(fields, "duration", "json", "int", []string{"duration"}, "10")
		addDetectedField(fields, "duration", "logfmt", "string", nil, "ten")
		addDetectedField(fields, "timestamp_end", "json", "string", nil, "2026-04-21T10:00:00Z")
		addDetectedField(fields, "observed_timestamp_end", "json", "string", nil, "2026-04-21T10:00:00Z")

		summary := fields["duration"]
		if summary == nil {
			t.Fatal("expected detected field summary")
			return
		}
		if summary.typ != "string" {
			t.Fatalf("expected detected type to widen to string, got %q", summary.typ)
		}
		if len(summary.parsers) != 2 {
			t.Fatalf("expected two parser sources, got %d", len(summary.parsers))
		}
		if len(summary.jsonPath) != 1 || summary.jsonPath[0] != "duration" {
			t.Fatalf("expected original json path to be preserved, got %#v", summary.jsonPath)
		}
		if _, exists := fields["timestamp_end"]; exists {
			t.Fatal("expected suppressed high-cardinality timestamp_end field to be ignored")
		}
		if _, exists := fields["observed_timestamp_end"]; exists {
			t.Fatal("expected suppressed high-cardinality observed_timestamp_end field to be ignored")
		}
		if !shouldSuppressDetectedField("timestamp_end") {
			t.Fatal("expected timestamp_end to be part of suppression list")
		}
		if got := asString(map[string]any{"line": "hello\nworld"}); !strings.Contains(got, "hello") || strings.Contains(got, "\n") {
			t.Fatalf("expected asString to flatten JSON, got %q", got)
		}
	})

	t.Run("selector and query helpers", func(t *testing.T) {
		if got := appendSyntheticLabels([]string{"app", "app"}); len(got) != 2 || got[1] != "service_name" {
			t.Fatalf("unexpected synthetic labels %#v", got)
		}
		if got := streamSelectorPrefix(`{app="api|web"} |= "error"`); got != `{app="api|web"}` {
			t.Fatalf("unexpected selector prefix %q", got)
		}
		if got := inferPrimaryTargetLabel(`{namespace="prod",pod=~"api-.*"} |= "error"`); got != "namespace" {
			t.Fatalf("unexpected primary target label %q", got)
		}
		if got := normalizeBareSelectorQuery(`app="api"`); got != `{app="api"}` {
			t.Fatalf("expected bare selector to be wrapped, got %q", got)
		}
		if got := normalizeBareSelectorQuery(`{app="api"} |= "error"`); got != `{app="api"} |= "error"` {
			t.Fatalf("expected LogQL pipeline query to remain unchanged, got %q", got)
		}

		dst := map[string]*detectedLabelSummary{
			"service_name": {label: "service_name", values: map[string]struct{}{"api": {}}},
		}
		scanned := map[string]*detectedLabelSummary{
			"level": {label: "level", values: map[string]struct{}{"error": {}}},
		}
		mergeDetectedLabelSupplements(dst, scanned)
		if dst["level"] == nil {
			t.Fatal("expected level supplement to be merged")
		}
	})
}

func TestFetchNativeFieldValues_RelaxesAfterSuccessfulEmptyPrimary(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/field_values" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		call := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if call == 1 {
			_, _ = w.Write([]byte(`{"values":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"values":[{"value":"unexpected","hits":3}]}`))
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	values, err := p.fetchNativeFieldValues(context.Background(), `{app="api"} | duration>1s`, "", "", "service.name", 10)
	if err != nil {
		t.Fatalf("fetchNativeFieldValues returned error: %v", err)
	}
	if len(values) != 1 || values[0] != "unexpected" {
		t.Fatalf("expected relaxed fallback values after empty primary response, got %v", values)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected strict+relaxed backend requests, got %d", got)
	}
}

func TestDetectScannedLabels_DoesNotRelaxOnSuccessfulEmptyPrimary(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		call := calls.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		if call == 1 {
			_, _ = w.Write([]byte(""))
			return
		}
		_, _ = w.Write([]byte("{\"_time\":\"2026-04-20T10:00:00Z\",\"_msg\":\"ok\",\"_stream\":\"{service_name=\\\"should-not-appear\\\"}\"}\n"))
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	labels, err := p.detectScannedLabels(context.Background(), `{app="api"} | duration>1s`, "", "", 100)
	if err != nil {
		t.Fatalf("detectScannedLabels returned error: %v", err)
	}
	if len(labels) != 0 {
		t.Fatalf("expected empty primary response to stop without relaxed fallback, got %#v", labels)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly one backend request, got %d", got)
	}
}

func TestDrilldownBackendHelpers_AdditionalCoverage(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"{service_name=\"api\"}","hits":2},{"value":"{app=\"worker\"}","hits":1},{"value":"{service_name=\"api\"}","hits":1}]}`))
		case "/select/logsql/field_values":
			if r.URL.Query().Get("field") == "broken" {
				http.Error(w, "backend boom", http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"zulu","hits":1},{"value":"alpha","hits":2}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	t.Cleanup(func() { p.cache.Close() })
	p.translationCache = cache.New(5, 10)
	t.Cleanup(func() { p.translationCache.Close() })

	values, err := p.serviceNameValues(context.Background(), `{app="api"}`, "", "")
	if err != nil {
		t.Fatalf("serviceNameValues returned error: %v", err)
	}
	if strings.Join(values, ",") != "api,worker" {
		t.Fatalf("unexpected service names %v", values)
	}

	fieldValues, err := p.fetchNativeFieldValues(context.Background(), `{app="api"}`, "", "", "service.name", 2)
	if err != nil {
		t.Fatalf("fetchNativeFieldValues returned error: %v", err)
	}
	if strings.Join(fieldValues, ",") != "alpha,zulu" {
		t.Fatalf("unexpected field values %v", fieldValues)
	}

	if _, err := p.fetchNativeFieldValues(context.Background(), `{app="api"}`, "", "", "broken", 0); err == nil || !strings.Contains(err.Error(), "backend boom") {
		t.Fatalf("expected backend error to surface, got %v", err)
	}
}

// TestUnifyDetectedType_StringBeatsFloat is a regression guard for the type-widening
// bug where float+string incorrectly returned float instead of string. Once any
// observed value is non-numeric the field must be classified as string — a hex
// trace_id like "138ee486703e" contains 'e' in a position that Go's ParseFloat
// accepts as scientific notation for some values, which permanently poisoned the
// field type. The fix ensures string always wins over float.
func TestUnifyDetectedType_StringBeatsFloat(t *testing.T) {
	cases := []struct {
		current, next, want string
	}{
		// string always wins (the regression fix)
		{"float", "string", "string"},
		{"string", "float", "string"},
		{"string", "int", "string"},
		{"int", "string", "string"},
		// float widens int (numeric promotion)
		{"int", "float", "float"},
		{"float", "int", "float"},
		// same type is idempotent
		{"int", "int", "int"},
		{"float", "float", "float"},
		{"string", "string", "string"},
		// empty current adopts next
		{"", "int", "int"},
		{"", "float", "float"},
		{"", "string", "string"},
	}
	for _, tc := range cases {
		got := unifyDetectedType(tc.current, tc.next)
		if got != tc.want {
			t.Errorf("unifyDetectedType(%q, %q) = %q, want %q", tc.current, tc.next, got, tc.want)
		}
	}
}

// TestInferDetectedType_HexTraceIDsAreString guards that hex strings which
// happen to contain a digit-'e'-digit sequence (matching Go's scientific-
// notation float syntax) are NOT classified as float. This is the root cause
// of the "trace_id type flip" regression: "138ee486703e" was parsed as float
// because "138e" satisfied ParseFloat, poisoning the field type via unifyDetectedType.
func TestInferDetectedType_HexTraceIDsAreString(t *testing.T) {
	hexIDs := []string{
		"138ee486703e", // regression case: "138e" prefix looks like sci-notation
		"96e6f034a5b1", // 'f' after exponent digits → always fails ParseFloat
		"b4216faff2af", // starts with 'b' → always fails
		"42faf695202a", // has 'a','f' → always fails
		"449ec1530f2f", // '9e' then 'c' → fails
	}
	for _, id := range hexIDs {
		got := inferDetectedType(id)
		if got != "string" {
			t.Errorf("inferDetectedType(%q) = %q, want %q (hex trace ID must be string)", id, got, "string")
		}
	}
}

// TestUnifyDetectedType_TraceIDScenario simulates the accumulation of a hex
// trace_id field across many log entries, including a value that ParseFloat
// accepts (e.g. "1234e5678" as +Inf in scientific notation). After seeing
// the non-numeric hex values the final type must be "string".
func TestUnifyDetectedType_TraceIDScenario(t *testing.T) {
	// Simulate a field that accumulates types across sampled log entries.
	// Values include: some "int" looking (pure digits), one "float" looking
	// (scientific-notation), and many "string" (hex with a-f chars).
	sampleTypes := []string{"int", "int", "float", "string", "string", "string"}
	current := ""
	for _, typ := range sampleTypes {
		current = unifyDetectedType(current, typ)
	}
	if current != "string" {
		t.Errorf("accumulated type = %q, want %q — hex trace_id field must resolve to string", current, "string")
	}
}

// TestExpandDrilldownStep verifies proxy-side step expansion converts sparse coarse
// VL buckets into dense fine-resolution sub-buckets for Grafana's bar-chart view.
func TestExpandDrilldownStep(t *testing.T) {
	// A 2-bucket coarse response at 3600s step (3600 / 300 = 12 sub-buckets each).
	coarseBody := `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"error"},"values":[[1748000000,"120"],[1748003600,"240"]]}` +
		`]}}`

	t.Run("expands 2 coarse buckets into 24 fine buckets", func(t *testing.T) {
		got := expandDrilldownStep([]byte(coarseBody), "300s", "3600s", "1748007200")
		v, err := fj.ParseBytes(got)
		if err != nil {
			t.Fatalf("parse expanded body: %v", err)
		}
		results := v.GetArray("data", "result")
		if len(results) != 1 {
			t.Fatalf("expected 1 result series, got %d", len(results))
		}
		values := results[0].GetArray("values")
		if len(values) != 24 {
			t.Fatalf("expected 24 fine buckets (2 coarse × 12), got %d", len(values))
		}
		// First sub-bucket: ts=1748000000, value=120/12=10.00
		first := values[0].GetArray()
		if len(first) < 2 {
			t.Fatal("expected [ts, val] pair")
		}
		if first[0].GetInt64() != 1748000000 {
			t.Errorf("first ts = %d, want 1748000000", first[0].GetInt64())
		}
		if string(first[1].GetStringBytes()) != "10.00" {
			t.Errorf("first val = %q, want %q", string(first[1].GetStringBytes()), "10.00")
		}
		// 13th sub-bucket: ts=1748003600 (start of 2nd coarse bucket), value=240/12=20.00
		thirteenth := values[12].GetArray()
		if thirteenth[0].GetInt64() != 1748003600 {
			t.Errorf("13th ts = %d, want 1748003600", thirteenth[0].GetInt64())
		}
		if string(thirteenth[1].GetStringBytes()) != "20.00" {
			t.Errorf("13th val = %q, want %q", string(thirteenth[1].GetStringBytes()), "20.00")
		}
	})

	t.Run("no-op when fine step equals coarse step", func(t *testing.T) {
		got := expandDrilldownStep([]byte(coarseBody), "3600s", "3600s", "1748007200")
		if string(got) != coarseBody {
			t.Errorf("expected body unchanged when steps are equal")
		}
	})

	t.Run("no-op when fine step exceeds coarse step", func(t *testing.T) {
		got := expandDrilldownStep([]byte(coarseBody), "7200s", "3600s", "1748007200")
		if string(got) != coarseBody {
			t.Errorf("expected body unchanged when fine step > coarse step")
		}
	})

	t.Run("truncates sub-buckets at end boundary", func(t *testing.T) {
		// End is 1748001200: only 4 sub-buckets of 2nd coarse bucket fit within
		// coarse bucket 1 (1748000000..1748003599): ts 1748000000,300,600,900 → 4 sub-buckets.
		// Then coarse bucket 2 at 1748003600 > end=1748001200 → trimmed.
		// Actually end=1748001200 means: bucket 1 sub-buckets: 1748000000,300,600,900,1200 → 5 fit
		// (1748001200 == end so it's included). Bucket 2 at 1748003600 > end → all 0 sub-buckets.
		got := expandDrilldownStep([]byte(coarseBody), "300s", "3600s", "1748001200")
		v, err := fj.ParseBytes(got)
		if err != nil {
			t.Fatalf("parse truncated body: %v", err)
		}
		values := v.GetArray("data", "result")[0].GetArray("values")
		// 1748000000,300,600,900,1200 = 5 sub-buckets from first coarse; second coarse
		// starts at 1748003600 > 1748001200, so all 12 sub-buckets are truncated.
		if len(values) != 5 {
			t.Errorf("expected 5 sub-buckets before end, got %d", len(values))
		}
	})

	t.Run("empty result passes through unchanged", func(t *testing.T) {
		empty := `{"status":"success","data":{"resultType":"matrix","result":[]}}`
		got := expandDrilldownStep([]byte(empty), "300s", "3600s", "1748007200")
		if string(got) != empty {
			t.Errorf("expected empty result unchanged")
		}
	})

	t.Run("no-op when factor exceeds maxDrilldownExpansionFactor", func(t *testing.T) {
		// 7-day range: fineStep=20s, coarseStep=6h=21600s → factor=1080 > 120.
		// Expanding would divide counts by 1080, making bars invisible. Return as-is.
		got := expandDrilldownStep([]byte(coarseBody), "20s", "21600s", "1748604800")
		if string(got) != coarseBody {
			t.Errorf("expected body unchanged when expansion factor > %d", maxDrilldownExpansionFactor)
		}
	})

	t.Run("factor exactly at threshold expands", func(t *testing.T) {
		// factor=120 (coarseStep=3600s / fineStep=30s) is ≤ threshold → expand.
		got := expandDrilldownStep([]byte(coarseBody), "30s", "3600s", "1748007200")
		v, err := fj.ParseBytes(got)
		if err != nil {
			t.Fatalf("parse body: %v", err)
		}
		values := v.GetArray("data", "result")[0].GetArray("values")
		// 2 coarse buckets × 120 = 240 fine buckets.
		if len(values) != 240 {
			t.Errorf("expected 240 fine buckets at threshold factor, got %d", len(values))
		}
	})

	t.Run("multiple series expanded independently", func(t *testing.T) {
		multi := `{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"level":"error"},"values":[[1748000000,"120"]]}` +
			`,{"metric":{"level":"warn"},"values":[[1748000000,"60"]]}` +
			`]}}`
		got := expandDrilldownStep([]byte(multi), "300s", "3600s", "1748003600")
		v, _ := fj.ParseBytes(got)
		results := v.GetArray("data", "result")
		if len(results) != 2 {
			t.Fatalf("expected 2 series, got %d", len(results))
		}
		// Each coarse bucket should expand to 12 sub-buckets.
		for i, res := range results {
			if n := len(res.GetArray("values")); n != 12 {
				t.Errorf("series %d: expected 12 sub-buckets, got %d", i, n)
			}
		}
	})
}

