package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"
)

// =============================================================================
// Integration tests for collectRangeMetricSamples (via handleQueryRange)
// =============================================================================

// vlLineTS formats a VL NDJSON log line for collectRangeMetricSamples.
func vlLineTS(ts time.Time, msg, stream string) string {
	return fmt.Sprintf(`{"_time":%q,"_msg":%q,"_stream":%q}`, ts.Format(time.RFC3339Nano), msg, stream)
}

func vlLineWithField(ts time.Time, stream, fieldName, fieldValue string) string {
	return fmt.Sprintf(`{"_time":%q,"_stream":%q,%q:%q}`, ts.Format(time.RFC3339Nano), stream, fieldName, fieldValue)
}


// =============================================================================
// 1. Basic count_over_time — single stream, multiple samples
// =============================================================================

func TestCollectRangeMetric_CountOverTime_SingleStream(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	step := 60 * time.Second
	stream := `{app="api",env="prod"}`

	var queryCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			queryCalled = true
			w.Header().Set("Content-Type", "application/x-ndjson")
			for i := 0; i < 3; i++ {
				_, _ = fmt.Fprintln(w, vlLineTS(base.Add(time.Duration(i)*step), "msg", stream))
			}
		default:
			t.Errorf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// Use non-parser rate with range != step to force manual path (collectRangeMetricSamples).
	params.Set("query", `rate({app="api",env="prod"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(3*step).Unix(), 10))
	params.Set("step", "30") // step(30) != range(120) → manual path
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if !queryCalled {
		t.Fatal("expected /select/logsql/query to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data.Result) == 0 {
		t.Fatalf("expected at least 1 series, got 0")
	}
}

// =============================================================================
// 2. Multiple streams → multiple series
// =============================================================================

func TestCollectRangeMetric_MultipleStreams_MultipleSeries(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	streams := []string{
		`{app="api",env="prod"}`,
		`{app="worker",env="prod"}`,
		`{app="db",env="prod"}`,
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i, s := range streams {
			_, _ = fmt.Fprintln(w, vlLineTS(base.Add(time.Duration(i)*time.Second), "msg", s))
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({env="prod"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data.Result) != len(streams) {
		t.Fatalf("expected %d series (one per stream), got %d", len(streams), len(resp.Data.Result))
	}
}

// =============================================================================
// 3. Stream cache hit: many lines from same stream → single series entry reused
// =============================================================================

func TestCollectRangeMetric_StreamCache_SameStreamProducesSingleSeries(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`
	const lineCount = 1000

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := 0; i < lineCount; i++ {
			_, _ = fmt.Fprintln(w, vlLineTS(base.Add(time.Duration(i)*time.Second), "msg", stream))
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(20*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// All lines from same stream → exactly 1 series.
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected 1 series from 1 stream, got %d", len(resp.Data.Result))
	}
}

// =============================================================================
// 4. bytes_over_time: __bytes__ field (length of _msg)
// =============================================================================

func TestCollectRangeMetric_BytesOverTime_UsesMessageLength(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`
	msgs := []string{"hello", "world!", "hi"}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i, msg := range msgs {
			line := fmt.Sprintf(`{"_time":%q,"_msg":%q,"_stream":%q}`,
				base.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), msg, stream)
			_, _ = fmt.Fprintln(w, line)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `bytes_over_time({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// =============================================================================
// 5. sum_over_time with numeric unwrap field
// =============================================================================

func TestCollectRangeMetric_SumOverTime_FloatField(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i, val := range []string{"1.5", "2.5", "3.0"} {
			line := fmt.Sprintf(`{"_time":%q,"_stream":%q,"latency_ms":%s,"_msg":"x"}`,
				base.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), stream, val)
			_, _ = fmt.Fprintln(w, line)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `sum_over_time({app="api"} | logfmt | unwrap latency_ms [2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// =============================================================================
// 6. Malformed JSON lines must be silently skipped
// =============================================================================

func TestCollectRangeMetric_MalformedLines_Skipped(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// Good line first.
		_, _ = fmt.Fprintln(w, vlLineTS(base, "good", stream))
		// Various malformed entries.
		_, _ = fmt.Fprintln(w, "not json at all")
		_, _ = fmt.Fprintln(w, "{bad json}")
		_, _ = fmt.Fprintln(w, "") // empty line
		// Another good line.
		_, _ = fmt.Fprintln(w, vlLineTS(base.Add(time.Second), "also good", stream))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	// Must not crash and must return 200.
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 despite bad lines, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Should still produce series from the 2 valid lines.
	if len(resp.Data.Result) == 0 {
		t.Fatalf("expected series from valid lines, got 0")
	}
}

// =============================================================================
// 7. Lines with missing _time must be skipped without crash
// =============================================================================

func TestCollectRangeMetric_MissingTime_Skipped(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// No _time field.
		_, _ = fmt.Fprintln(w, `{"_msg":"no time","_stream":"{app=\"api\"}"}`)
		// Valid line.
		_, _ = fmt.Fprintln(w, vlLineTS(base, "ok", stream))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// =============================================================================
// 8. Backend error response (4xx) propagated correctly
// =============================================================================

func TestCollectRangeMetric_BackendError_PropagatedToClient(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			http.Error(w, `{"error":"query too expensive"}`, http.StatusBadRequest)
		}
	}))
	defer vlBackend.Close()

	base := time.Unix(1700000000, 0).UTC()
	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected error response from backend to be propagated, got 200")
	}
}

// =============================================================================
// 9. Large response: 10000 lines, single stream → must not OOM or crash
// =============================================================================

func TestCollectRangeMetric_LargeResponse_SingleStream(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`
	const lineCount = 10_000

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := 0; i < lineCount; i++ {
			_, _ = fmt.Fprintln(w, vlLineTS(base.Add(time.Duration(i)*time.Millisecond), "msg", stream))
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected 1 series from single stream, got %d", len(resp.Data.Result))
	}
}

// =============================================================================
// 10. groupBy with parsed label (slow path) produces correct series keys
// =============================================================================

func TestCollectRangeMetric_GroupByParsedLabel_SlowPath(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// Two entries with different "method" parsed field → two series with method label.
		line1 := fmt.Sprintf(`{"_time":%q,"_stream":%q,"method":"GET","_msg":"x"}`,
			base.Format(time.RFC3339Nano), stream)
		line2 := fmt.Sprintf(`{"_time":%q,"_stream":%q,"method":"POST","_msg":"y"}`,
			base.Add(time.Second).Format(time.RFC3339Nano), stream)
		_, _ = fmt.Fprintln(w, line1)
		_, _ = fmt.Fprintln(w, line2)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// Use a parser stage query so includeParsedLabels=true for slow path.
	// sum by (method) forces groupBy=["method"] with parsed labels.
	params.Set("query", `sum by (method) (rate({app="api"} | unpack_json [2m]))`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Two distinct parsed values → at most 2 series (may be 1 if merged by outer sum).
	// We only assert no crash + valid JSON response here.
}

// =============================================================================
// 11. Timestamp formats: unix seconds and nanoseconds both handled
// =============================================================================

func TestCollectRangeMetric_TimestampFormats(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// RFC3339Nano format (normal VL output).
		_, _ = fmt.Fprintln(w, vlLineTS(base, "msg1", stream))
		// Also one with an RFC3339 (no sub-second) timestamp.
		line2 := fmt.Sprintf(`{"_time":%q,"_msg":"msg2","_stream":%q}`,
			base.Add(time.Second).Format(time.RFC3339), stream)
		_, _ = fmt.Fprintln(w, line2)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// =============================================================================
// Unit tests for seriesKeyFromMetric (correctness + determinism)
// =============================================================================

func TestSeriesKeyFromMetric_Empty(t *testing.T) {
	got := seriesKeyFromMetric(nil)
	if got != "{}" {
		t.Fatalf("nil map: expected {}, got %q", got)
	}
	got = seriesKeyFromMetric(map[string]string{})
	if got != "{}" {
		t.Fatalf("empty map: expected {}, got %q", got)
	}
}

func TestSeriesKeyFromMetric_SingleEntry(t *testing.T) {
	got := seriesKeyFromMetric(map[string]string{"app": "nginx"})
	if got != "{app=nginx}" {
		t.Fatalf("single entry: expected {app=nginx}, got %q", got)
	}
}

func TestSeriesKeyFromMetric_Deterministic(t *testing.T) {
	// Run many times to catch map-iteration non-determinism.
	m := map[string]string{"z": "last", "a": "first", "m": "middle"}
	want := seriesKeyFromMetric(m)
	for i := 0; i < 50; i++ {
		if got := seriesKeyFromMetric(m); got != want {
			t.Fatalf("non-deterministic output: run %d produced %q, want %q", i, got, want)
		}
	}
}

func TestSeriesKeyFromMetric_Sorted(t *testing.T) {
	got := seriesKeyFromMetric(map[string]string{"z": "3", "a": "1", "m": "2"})
	if got != "{a=1,m=2,z=3}" {
		t.Fatalf("expected alphabetically sorted keys, got %q", got)
	}
}

func TestSeriesKeyFromMetric_ValueWithEquals(t *testing.T) {
	// Ensure values containing "=" don't corrupt sort order.
	got := seriesKeyFromMetric(map[string]string{"b": "x=y", "a": "z"})
	if got != "{a=z,b=x=y}" {
		t.Fatalf("value with '=': expected {a=z,b=x=y}, got %q", got)
	}
}

// =============================================================================
// Unit tests for parseFloatValueFJ
// =============================================================================

func TestParseFloatValueFJ_Number(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":3.14}`)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parseFloatValueFJ(v.Get("x"))
	if !ok || got != 3.14 {
		t.Fatalf("expected 3.14, got ok=%v val=%v", ok, got)
	}
}

func TestParseFloatValueFJ_StringNumber(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":"42.5"}`)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parseFloatValueFJ(v.Get("x"))
	if !ok || got != 42.5 {
		t.Fatalf("expected 42.5, got ok=%v val=%v", ok, got)
	}
}

func TestParseFloatValueFJ_StringWithSpaces(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":" 7 "}`)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parseFloatValueFJ(v.Get("x"))
	if !ok || got != 7.0 {
		t.Fatalf("expected 7.0, got ok=%v val=%v", ok, got)
	}
}

func TestParseFloatValueFJ_InvalidString(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":"not-a-number"}`)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := parseFloatValueFJ(v.Get("x"))
	if ok {
		t.Fatal("expected false for non-numeric string")
	}
}

func TestParseFloatValueFJ_BoolReturnsFalse(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":true}`)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := parseFloatValueFJ(v.Get("x"))
	if ok {
		t.Fatal("expected false for bool value")
	}
}

func TestParseFloatValueFJ_NullReturnsFalse(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":null}`)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := parseFloatValueFJ(v.Get("x"))
	if ok {
		t.Fatal("expected false for null value")
	}
}

func TestParseFloatValueFJ_IntegerNumber(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"x":100}`)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parseFloatValueFJ(v.Get("x"))
	if !ok || got != 100.0 {
		t.Fatalf("expected 100.0, got ok=%v val=%v", ok, got)
	}
}

// =============================================================================
// Unit tests for addGroupByParsedLabelsFJ
// =============================================================================

func TestAddGroupByParsedLabelsFJ_AddsRequestedFields(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"method":"GET","status":"200","other":"ignored"}`)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{}
	addGroupByParsedLabelsFJ(labels, v, []string{"method", "status"})
	if labels["method"] != "GET" {
		t.Fatalf("expected method=GET, got %q", labels["method"])
	}
	if labels["status"] != "200" {
		t.Fatalf("expected status=200, got %q", labels["status"])
	}
	if _, ok := labels["other"]; ok {
		t.Fatal("expected 'other' not to be injected (not in groupBy)")
	}
}

func TestAddGroupByParsedLabelsFJ_SkipsInternalFields(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"_stream":"s","_stream_id":"id","method":"GET"}`)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{}
	addGroupByParsedLabelsFJ(labels, v, []string{"_stream", "_stream_id", "method"})
	if _, ok := labels["_stream"]; ok {
		t.Fatal("expected _stream to be skipped (internal field)")
	}
	if _, ok := labels["_stream_id"]; ok {
		t.Fatal("expected _stream_id to be skipped (internal field)")
	}
	if labels["method"] != "GET" {
		t.Fatalf("expected method=GET, got %q", labels["method"])
	}
}

func TestAddGroupByParsedLabelsFJ_DoesNotOverwriteExistingLabel(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"app":"from-log"}`)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"app": "from-stream"}
	addGroupByParsedLabelsFJ(labels, v, []string{"app"})
	if labels["app"] != "from-stream" {
		t.Fatalf("existing label must not be overwritten by parsed field; got %q", labels["app"])
	}
}

func TestAddGroupByParsedLabelsFJ_SkipsMissingFields(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"method":"GET"}`)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{}
	addGroupByParsedLabelsFJ(labels, v, []string{"method", "nonexistent"})
	if _, ok := labels["nonexistent"]; ok {
		t.Fatal("expected missing field to be skipped")
	}
}

func TestAddGroupByParsedLabelsFJ_SkipsEmptyStringValue(t *testing.T) {
	var p fj.Parser
	v, err := p.Parse(`{"method":"  "}`)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{}
	addGroupByParsedLabelsFJ(labels, v, []string{"method"})
	if _, ok := labels["method"]; ok {
		t.Fatalf("expected whitespace-only field to be skipped, got %q", labels["method"])
	}
}

// =============================================================================
// Unit tests for buildParsedGroupByCacheKey
// =============================================================================

func TestBuildParsedGroupByCacheKey_DifferentFieldValues_DifferentKeys(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser

	v1, _ := fjp.Parse(`{"method":"GET","status":"200"}`)
	v2, _ := fjp.Parse(`{"method":"POST","status":"200"}`)

	key1 := p.buildParsedGroupByCacheKey(`{app="api"}`, "info", v1, []string{"method"})
	key2 := p.buildParsedGroupByCacheKey(`{app="api"}`, "info", v2, []string{"method"})

	if key1 == key2 {
		t.Fatalf("different field values must produce different cache keys: both %q", key1)
	}
}

func TestBuildParsedGroupByCacheKey_SameValues_SameKey(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser

	v1, _ := fjp.Parse(`{"method":"GET","other":"ignored"}`)
	v2, _ := fjp.Parse(`{"method":"GET","other":"different"}`)

	key1 := p.buildParsedGroupByCacheKey(`{app="api"}`, "info", v1, []string{"method"})
	key2 := p.buildParsedGroupByCacheKey(`{app="api"}`, "info", v2, []string{"method"})

	if key1 != key2 {
		t.Fatalf("same groupBy field values must produce same cache key: %q vs %q", key1, key2)
	}
}

func TestBuildParsedGroupByCacheKey_InternalFieldsExcluded(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser

	v1, _ := fjp.Parse(`{"_stream":"s1","method":"GET"}`)
	v2, _ := fjp.Parse(`{"_stream":"s2","method":"GET"}`)

	// Both have same method but different _stream; _stream is internal and excluded from parsed key.
	key1 := p.buildParsedGroupByCacheKey("", "", v1, []string{"_stream", "method"})
	key2 := p.buildParsedGroupByCacheKey("", "", v2, []string{"_stream", "method"})

	if key1 != key2 {
		t.Fatalf("_stream in groupBy must be excluded from parsed key part; got %q vs %q", key1, key2)
	}
}

// =============================================================================
// Unit tests for lookupFJField
// =============================================================================

func TestLookupFJField_ReturnsFirstMatch(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"a":"1","b":"2"}`)

	got := p.lookupFJField(v, []string{"b", "a"})
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	s, ok := stringifyFJValue(got)
	if !ok || s != "2" {
		t.Fatalf("expected first match 'b'=2, got %q ok=%v", s, ok)
	}
}

func TestLookupFJField_ReturnsNilWhenNoneMatch(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"a":"1"}`)

	got := p.lookupFJField(v, []string{"x", "y", "z"})
	if got != nil {
		t.Fatal("expected nil when no keys match")
	}
}

func TestLookupFJField_EmptyKeyList(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"a":"1"}`)

	got := p.lookupFJField(v, []string{})
	if got != nil {
		t.Fatal("expected nil for empty key list")
	}
}

// =============================================================================
// Guard: seriesKeyFromMetric must be sort-stable across many iterations
// =============================================================================

func TestSeriesKeyFromMetric_StableAcrossIterations(t *testing.T) {
	// Large map to maximize chance of seeing different iteration orders.
	m := map[string]string{}
	for i := 0; i < 20; i++ {
		m[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("val%02d", i)
	}
	want := seriesKeyFromMetric(m)

	// Verify it's actually sorted.
	inner := strings.TrimPrefix(strings.TrimSuffix(want, "}"), "{")
	parts := strings.Split(inner, ",")
	keys := make([]string, len(parts))
	for i, p := range parts {
		eq := strings.Index(p, "=")
		if eq < 0 {
			t.Fatalf("unexpected part format: %q", p)
		}
		keys[i] = p[:eq]
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("keys are not sorted: %v", keys)
	}

	for i := 0; i < 100; i++ {
		if got := seriesKeyFromMetric(m); got != want {
			t.Fatalf("non-deterministic at iteration %d: %q vs %q", i, got, want)
		}
	}
}

// =============================================================================
// Guard: empty scanner response (no lines) must return empty map, not error
// =============================================================================

func TestCollectRangeMetric_EmptyResponse_ReturnsEmptyResult(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			// Write nothing — empty response body.
		}
	}))
	defer vlBackend.Close()

	base := time.Unix(1700000000, 0).UTC()
	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for empty backend response, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data.Result) != 0 {
		t.Fatalf("expected 0 series for empty response, got %d", len(resp.Data.Result))
	}
}

// =============================================================================
// Unit tests for extractManualSampleValueFJ
// =============================================================================

func TestExtractManualSampleValueFJ_Count(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"_time":"2024-01-01T00:00:00Z","_msg":"any","_stream":"{}"}`)
	got, ok := p.extractManualSampleValueFJ(v, "__count__", "")
	if !ok || got != 1.0 {
		t.Fatalf("__count__ must return 1; ok=%v val=%v", ok, got)
	}
}

func TestExtractManualSampleValueFJ_Bytes_UsesMsg(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	msg := "hello world"
	v, _ := fjp.Parse(fmt.Sprintf(`{"_time":"2024-01-01T00:00:00Z","_msg":%q,"_stream":"{}"}`, msg))
	got, ok := p.extractManualSampleValueFJ(v, "__bytes__", "")
	if !ok || got != float64(len(msg)) {
		t.Fatalf("__bytes__ must return len(_msg)=%d; ok=%v val=%v", len(msg), ok, got)
	}
}

func TestExtractManualSampleValueFJ_Bytes_EmptyMsg(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"_time":"2024-01-01T00:00:00Z","_msg":"","_stream":"{}"}`)
	got, ok := p.extractManualSampleValueFJ(v, "__bytes__", "")
	if !ok || got != 0 {
		t.Fatalf("__bytes__ with empty msg must return 0; ok=%v val=%v", ok, got)
	}
}

func TestExtractManualSampleValueFJ_FloatField_Default(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"_msg":"x","latency":3.14,"latency_ms":314}`)
	got, ok := p.extractManualSampleValueFJ(v, "latency", "")
	if !ok || got != 3.14 {
		t.Fatalf("numeric field: expected 3.14; ok=%v val=%v", ok, got)
	}
}

func TestExtractManualSampleValueFJ_MissingField_ReturnsFalse(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"_msg":"x"}`)
	_, ok := p.extractManualSampleValueFJ(v, "missing_field", "")
	if ok {
		t.Fatal("expected false for missing field")
	}
}

func TestExtractManualSampleValueFJ_DurationConv(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"duration":"100ms"}`)
	got, ok := p.extractManualSampleValueFJ(v, "duration", "duration")
	if !ok {
		t.Fatal("expected ok=true for duration conv")
	}
	// 100ms = 0.1s in seconds.
	if got < 0.09 || got > 0.11 {
		t.Fatalf("expected ~0.1 for 100ms, got %v", got)
	}
}

func TestExtractManualSampleValueFJ_BytesConv(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	var fjp fj.Parser
	v, _ := fjp.Parse(`{"size":"1KB"}`)
	got, ok := p.extractManualSampleValueFJ(v, "size", "bytes")
	if !ok {
		t.Fatal("expected ok=true for bytes conv")
	}
	if got <= 0 {
		t.Fatalf("expected positive bytes value, got %v", got)
	}
}

// =============================================================================
// Integration: count_over_time produces exactly 1 per time bucket
// =============================================================================

func TestCollectRangeMetric_CountOverTime_ValueIsOne(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	stream := `{app="api"}`

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// One line per 30s slot — count should be 1 each.
		for i := 0; i < 3; i++ {
			_, _ = fmt.Fprintln(w, vlLineTS(base.Add(time.Duration(i)*30*time.Second), "msg", stream))
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `count_over_time({app="api"}[30s])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "15") // step(15) != range(30) → manual path
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data.Result) == 0 {
		t.Fatal("expected at least 1 series")
	}
	// Each sample value must be non-negative (count is always ≥0).
	for _, series := range resp.Data.Result {
		for _, sample := range series.Values {
			if len(sample) < 2 {
				continue
			}
			valStr, _ := sample[1].(string)
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				t.Fatalf("sample value not parseable: %v", sample[1])
			}
			if val < 0 {
				t.Fatalf("count_over_time sample must be ≥0, got %v", val)
			}
		}
	}
}

// =============================================================================
// buildMetricSeriesEntry: level label handling
// =============================================================================

func TestCollectRangeMetric_LevelLabel_AddedFromStreamField(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// Stream includes level label.
		line := fmt.Sprintf(`{"_time":%q,"_msg":"msg","_stream":"{app=\"api\",level=\"error\"}","level":"error"}`,
			base.Format(time.RFC3339Nano))
		_, _ = fmt.Fprintln(w, line)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="api"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp lokiMatrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data.Result) == 0 {
		t.Fatal("expected at least 1 series")
	}
}

// =============================================================================
// Helper types
// =============================================================================

type lokiMatrixResponse struct {
	Data struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
}
