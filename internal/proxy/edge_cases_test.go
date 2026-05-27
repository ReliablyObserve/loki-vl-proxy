package proxy

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

// ── bucketMetadataTime ────────────────────────────────────────────────────────

func TestBucketMetadataTime_SmallInterval_5mBuckets(t *testing.T) {
	t.Parallel()
	const ns5m = int64(5 * time.Minute)
	// 1h window — should use 5-minute buckets
	end := int64(1_000_000_000) * 3600 // arbitrary aligned epoch second
	start := end - int64(time.Hour)
	bs, be := bucketMetadataTime(start, end)
	if bs%ns5m != 0 {
		t.Errorf("start %d not aligned to 5m bucket (mod=%d)", bs, bs%ns5m)
	}
	if be%ns5m != 0 {
		t.Errorf("end %d not aligned to 5m bucket (mod=%d)", be, be%ns5m)
	}
}

func TestBucketMetadataTime_6hBoundary_5mBuckets(t *testing.T) {
	t.Parallel()
	const ns5m = int64(5 * time.Minute)
	// exactly 6h — still in the ≤6h bucket tier
	end := int64(1_000_000_000) * 3600 * 6
	start := end - int64(6*time.Hour)
	bs, be := bucketMetadataTime(start, end)
	if bs%ns5m != 0 {
		t.Errorf("6h boundary start should use 5m buckets, got mod=%d", bs%ns5m)
	}
	if be%ns5m != 0 {
		t.Errorf("6h boundary end should use 5m buckets, got mod=%d", be%ns5m)
	}
}

func TestBucketMetadataTime_7hInterval_1hBuckets(t *testing.T) {
	t.Parallel()
	const ns1h = int64(time.Hour)
	// 7h window — falls into 6h < interval ≤ 48h tier → 1h buckets
	end := int64(1_000_000_000) * 3600 * 7
	start := end - int64(7*time.Hour)
	bs, be := bucketMetadataTime(start, end)
	if bs%ns1h != 0 {
		t.Errorf("7h interval start should use 1h buckets, got mod=%d", bs%ns1h)
	}
	if be%ns1h != 0 {
		t.Errorf("7h interval end should use 1h buckets, got mod=%d", be%ns1h)
	}
}

func TestBucketMetadataTime_48hBoundary_1hBuckets(t *testing.T) {
	t.Parallel()
	const ns1h = int64(time.Hour)
	// exactly 48h — still in the ≤48h tier
	end := int64(1_000_000_000) * 3600 * 48
	start := end - int64(48*time.Hour)
	bs, be := bucketMetadataTime(start, end)
	if bs%ns1h != 0 {
		t.Errorf("48h boundary start should use 1h buckets, got mod=%d", bs%ns1h)
	}
	if be%ns1h != 0 {
		t.Errorf("48h boundary end should use 1h buckets, got mod=%d", be%ns1h)
	}
}

func TestBucketMetadataTime_49hInterval_6hBuckets(t *testing.T) {
	t.Parallel()
	const ns6h = int64(6 * time.Hour)
	// 49h window — falls into the >48h tier → 6h buckets
	end := int64(1_000_000_000) * 3600 * 49
	start := end - int64(49*time.Hour)
	bs, be := bucketMetadataTime(start, end)
	if bs%ns6h != 0 {
		t.Errorf("49h interval start should use 6h buckets, got mod=%d", bs%ns6h)
	}
	if be%ns6h != 0 {
		t.Errorf("49h interval end should use 6h buckets, got mod=%d", be%ns6h)
	}
}

func TestBucketMetadataTime_BucketedStartNotExceedsOriginal(t *testing.T) {
	t.Parallel()
	// Bucketing must always floor timestamps — never round up.
	end := int64(1_779_700_000_000_000_000) // arbitrary recent ns timestamp
	start := end - int64(30*time.Minute)
	bs, be := bucketMetadataTime(start, end)
	if bs > start {
		t.Errorf("bucketed start %d > original start %d (rounded up)", bs, start)
	}
	if be > end {
		t.Errorf("bucketed end %d > original end %d (rounded up)", be, end)
	}
}

// ── capMetadataStartOnly ──────────────────────────────────────────────────────

func TestCapMetadataStartOnly_WithinWindow_Unchanged(t *testing.T) {
	t.Parallel()
	params := url.Values{
		"start": []string{"1779694000000000000"},
		"end":   []string{"1779697600000000000"}, // 1h later
	}
	out := capMetadataStartOnly(params, 6*time.Hour)
	if out.Get("start") != params.Get("start") {
		t.Errorf("within-window: start should be unchanged, got %s", out.Get("start"))
	}
}

func TestCapMetadataStartOnly_ExceedsWindow_CapsStart(t *testing.T) {
	t.Parallel()
	endNs := int64(1_779_700_000_000_000_000)
	startNs := endNs - int64(24*time.Hour)
	params := url.Values{
		"start": []string{fmt.Sprintf("%d", startNs)},
		"end":   []string{fmt.Sprintf("%d", endNs)},
	}
	out := capMetadataStartOnly(params, 6*time.Hour)
	wantStart := fmt.Sprintf("%d", endNs-int64(6*time.Hour))
	if out.Get("start") != wantStart {
		t.Errorf("capped start: want %s, got %s", wantStart, out.Get("start"))
	}
	// End must be preserved exactly — no bucketing.
	if out.Get("end") != params.Get("end") {
		t.Errorf("end should be unchanged: want %s, got %s", params.Get("end"), out.Get("end"))
	}
}

func TestCapMetadataStartOnly_PreservesEnd(t *testing.T) {
	t.Parallel()
	// Verify that end is never bucketed (unlike capMetadataTimeRange).
	// It IS normalized to nanoseconds for consistent VL query format.
	endNs := int64(1_779_700_123_456_789_000) // nanoseconds, deliberately not aligned to any bucket
	startNs := endNs - int64(48*time.Hour)
	params := url.Values{
		"start": []string{fmt.Sprintf("%d", startNs)},
		"end":   []string{fmt.Sprintf("%d", endNs)},
	}
	out := capMetadataStartOnly(params, 6*time.Hour)
	if out.Get("end") != fmt.Sprintf("%d", endNs) {
		t.Errorf("end was modified: want %d, got %s", endNs, out.Get("end"))
	}
}

func TestCapMetadataStartOnly_FromToAliases(t *testing.T) {
	t.Parallel()
	endNs := int64(1_779_700_000_000_000_000)
	startNs := endNs - int64(24*time.Hour)
	params := url.Values{
		"from": []string{fmt.Sprintf("%d", startNs)},
		"to":   []string{fmt.Sprintf("%d", endNs)},
	}
	out := capMetadataStartOnly(params, 6*time.Hour)
	// After capping, result must use "start" key (not "from").
	if out.Get("start") == "" {
		t.Error("expected 'start' key in result, got empty")
	}
	wantStart := fmt.Sprintf("%d", endNs-int64(6*time.Hour))
	if out.Get("start") != wantStart {
		t.Errorf("from/to aliases: capped start want %s, got %s", wantStart, out.Get("start"))
	}
}

func TestCapMetadataStartOnly_EndBeforeStart_Unchanged(t *testing.T) {
	t.Parallel()
	params := url.Values{
		"start": []string{"1779700000000000000"},
		"end":   []string{"1779690000000000000"}, // end < start
	}
	out := capMetadataStartOnly(params, 6*time.Hour)
	if out.Get("start") != params.Get("start") {
		t.Error("invalid range (end<start) should return params unchanged")
	}
}

func TestCapMetadataStartOnly_MissingEnd_Unchanged(t *testing.T) {
	t.Parallel()
	params := url.Values{"start": []string{"1779700000000000000"}}
	out := capMetadataStartOnly(params, 6*time.Hour)
	if out.Get("start") != params.Get("start") {
		t.Error("missing end should return params unchanged")
	}
}

// ── stripMetricWrapper ────────────────────────────────────────────────────────

func TestStripMetricWrapper_NonMetric_Unchanged(t *testing.T) {
	t.Parallel()
	q := `{app="foo"} | json`
	if got := stripMetricWrapper(q); got != q {
		t.Errorf("non-metric query modified: %s", got)
	}
}

func TestStripMetricWrapper_CountOverTime(t *testing.T) {
	t.Parallel()
	q := `count_over_time({app="foo"}[5m])`
	want := `{app="foo"}[5m]`
	if got := stripMetricWrapper(q); got != want {
		t.Errorf("count_over_time: want %q, got %q", want, got)
	}
}

func TestStripMetricWrapper_SumOverTime(t *testing.T) {
	t.Parallel()
	q := `sum_over_time({app="foo"} | json | unwrap latency [5m])`
	want := `{app="foo"} | json | unwrap latency [5m]`
	if got := stripMetricWrapper(q); got != want {
		t.Errorf("sum_over_time: want %q, got %q", want, got)
	}
}

func TestStripMetricWrapper_Rate(t *testing.T) {
	t.Parallel()
	q := `rate({job="ingester"}[1m])`
	want := `{job="ingester"}[1m]`
	if got := stripMetricWrapper(q); got != want {
		t.Errorf("rate: want %q, got %q", want, got)
	}
}

func TestStripMetricWrapper_NestedMetric(t *testing.T) {
	t.Parallel()
	// Grafana metric builder emits sum(rate({...}[5m])); the outer call is not in
	// reMetricWrapper, so the query is returned unchanged.
	q := `sum(rate({app="foo"}[5m]))`
	if got := stripMetricWrapper(q); got != q {
		t.Errorf("outer sum not in wrapper regex — should be unchanged, got %q", got)
	}
}

func TestStripMetricWrapper_UnclosedParen_ReturnOriginal(t *testing.T) {
	t.Parallel()
	// Malformed: opening paren never closed — function must not panic or hang.
	q := `rate({app="foo"}`
	got := stripMetricWrapper(q)
	// Original returned because paren balance never reaches 0.
	if got != q {
		t.Errorf("unclosed paren: want original %q, got %q", q, got)
	}
}

func TestStripMetricWrapper_CaseInsensitive(t *testing.T) {
	t.Parallel()
	q := `COUNT_OVER_TIME({app="foo"}[5m])`
	want := `{app="foo"}[5m]`
	if got := stripMetricWrapper(q); got != want {
		t.Errorf("case insensitive: want %q, got %q", want, got)
	}
}

func TestStripMetricWrapper_WithLeadingWhitespace(t *testing.T) {
	t.Parallel()
	q := `  rate({job="x"}[10m])  `
	want := `{job="x"}[10m]`
	if got := stripMetricWrapper(q); got != want {
		t.Errorf("leading/trailing whitespace: want %q, got %q", want, got)
	}
}

// ── extractVLErrorMsg ─────────────────────────────────────────────────────────

func TestExtractVLErrorMsg_EscapedQuotes(t *testing.T) {
	t.Parallel()
	// Verify the JSON-based fast path handles escaped quotes in error values.
	body := []byte(`{"error":"query with \"quotes\" failed"}`)
	got := extractVLErrorMsg(body)
	want := `query with "quotes" failed`
	if got != want {
		t.Errorf("escaped quotes: want %q, got %q", want, got)
	}
}

func TestExtractVLErrorMsg_PlainError(t *testing.T) {
	t.Parallel()
	body := []byte(`{"error":"unexpected token at position 5"}`)
	if got := extractVLErrorMsg(body); got != "unexpected token at position 5" {
		t.Errorf("plain error: got %q", got)
	}
}

func TestExtractVLErrorMsg_NonJSON_ReturnRaw(t *testing.T) {
	t.Parallel()
	body := []byte("bad gateway")
	if got := extractVLErrorMsg(body); got != "bad gateway" {
		t.Errorf("non-JSON: got %q", got)
	}
}

func TestExtractVLErrorMsg_Empty(t *testing.T) {
	t.Parallel()
	if got := extractVLErrorMsg(nil); got != "" {
		t.Errorf("nil body: got %q", got)
	}
}

func TestExtractVLErrorMsg_NoErrorKey(t *testing.T) {
	t.Parallel()
	// JSON object without "error" key — fall through to raw body.
	body := []byte(`{"status":"ok"}`)
	got := extractVLErrorMsg(body)
	if got != `{"status":"ok"}` {
		t.Errorf("no error key: got %q", got)
	}
}
