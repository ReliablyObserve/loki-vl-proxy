package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestProxyStatsQueryRange_StripsParserAndDelete verifies that proxyStatsQueryRange
// removes | unpack_json and | delete __error__ from Drilldown field histogram queries
// but preserves the stats by (field) count() grouping (needed for Grafana's histogram
// and top-values display).
func TestProxyStatsQueryRange_StripsParserAndDelete(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	// Full VL translation with parser stage and drop-error:
	// sum by (trace_id) (count_over_time({env="production"} | json trace_id | drop __error__ | trace_id!="" [1m]))
	logsqlQuery := `env:="production" _msg:!"" | unpack_json | delete __error__, __error_details__ | filter trace_id:!"" | stats by (trace_id) count()`

	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production",_msg!=""}|json trace_id|drop __error__,__error_details__|trace_id!=""`+` [1m]))`)
	form.Set("start", "1779957360")
	form.Set("end", "1780000670")
	form.Set("step", "60s")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")
	// Parser stage stripping is gated on the Drilldown header — simulate Drilldown request.
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")

	w := httptest.NewRecorder()
	p.proxyStatsQueryRange(w, req, logsqlQuery)

	t.Logf("VL received: %s", receivedQuery)

	if strings.Contains(receivedQuery, "unpack_json") {
		t.Errorf("query still contains | unpack_json: %s", receivedQuery)
	}
	if strings.Contains(receivedQuery, "| delete") {
		t.Errorf("query still contains | delete (should be stripped): %s", receivedQuery)
	}
	// stats by (trace_id) count() MUST be preserved — Grafana needs per-value breakdown
	if !strings.Contains(receivedQuery, "stats by (trace_id) count()") {
		t.Errorf("query lost stats by (trace_id) count() grouping: %s", receivedQuery)
	}
	if !strings.Contains(receivedQuery, "| filter trace_id") {
		t.Errorf("query lost | filter trace_id existence check: %s", receivedQuery)
	}
}

// TestLimitLokiMatrixSeries verifies that limitLokiMatrixSeries truncates a matrix
// response to the first N series without modifying responses that are already within
// the limit.
func TestLimitLokiMatrixSeries(t *testing.T) {
	makeSeries := func(n int) []byte {
		var sb strings.Builder
		sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, `{"metric":{"trace_id":"id-%d"},"values":[[1700000060,"1"]]}`, i)
		}
		sb.WriteString(`]}}`)
		return []byte(sb.String())
	}

	t.Run("under limit unchanged", func(t *testing.T) {
		body := makeSeries(10)
		got := limitLokiMatrixSeries(body, 100)
		if string(got) != string(body) {
			t.Errorf("expected body unchanged, got different result")
		}
	})

	t.Run("at limit unchanged", func(t *testing.T) {
		body := makeSeries(100)
		got := limitLokiMatrixSeries(body, 100)
		if string(got) != string(body) {
			t.Errorf("expected body unchanged at exact limit")
		}
	})

	t.Run("over limit truncated keeping top by count", func(t *testing.T) {
		// Build 200 series where series 150 has a very high count and should survive
		// even though it's beyond the first-100 position by index.
		var sb strings.Builder
		sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for i := 0; i < 200; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			count := "1"
			if i == 150 {
				count = "9999" // highest count — must survive the cut
			}
			fmt.Fprintf(&sb, `{"metric":{"trace_id":"id-%d"},"values":[[1700000060,"%s"]]}`, i, count)
		}
		sb.WriteString(`]}}`)
		body := []byte(sb.String())

		got := limitLokiMatrixSeries(body, 100)
		// High-count series at index 150 must be in the top-100 output
		if !strings.Contains(string(got), `"id-150"`) {
			t.Errorf("high-count series (id-150) should survive top-by-count cut")
		}
	})

	t.Run("invalid json returned unchanged", func(t *testing.T) {
		body := []byte(`not json`)
		got := limitLokiMatrixSeries(body, 5)
		if string(got) != string(body) {
			t.Errorf("invalid JSON should be returned unchanged")
		}
	})
}

// TestProxyStatsQueryRange_DrilldownHeaderGate verifies that Drilldown-specific
// optimizations (500-series cap via | limit, two-phase fallback) are gated behind
// the X-Query-Tags header. Explore and direct API clients with the same query pattern
// must go through the direct path and receive unfiltered results.
func TestProxyStatsQueryRange_DrilldownHeaderGate(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	effectiveQuery := `env:="production" | filter trace_id:!"" | stats by (trace_id) count()`

	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|trace_id!=""`+` [1m]))`)
	form.Set("start", "1700000000")
	form.Set("end", "1700003600")
	form.Set("step", "60s")

	t.Run("without_header_uses_direct_path_no_limit", func(t *testing.T) {
		receivedQuery = ""
		req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
		req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
		req.Header.Set("X-Scope-OrgID", "default")
		// No X-Query-Tags — simulates Explore or direct API client

		p.proxyStatsQueryRange(httptest.NewRecorder(), req, effectiveQuery)

		if strings.Contains(receivedQuery, "| limit") {
			t.Errorf("direct path (no header) must not apply series limit; VL received: %s", receivedQuery)
		}
	})

	t.Run("with_drilldown_header_uses_drilldown_path_with_limit", func(t *testing.T) {
		receivedQuery = ""
		req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
		req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
		req.Header.Set("X-Scope-OrgID", "default")
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")

		p.proxyStatsQueryRange(httptest.NewRecorder(), req, effectiveQuery)

		if !strings.Contains(receivedQuery, fmt.Sprintf("| limit %d", maxDrilldownSeries)) {
			t.Errorf("Drilldown path must add VL-side | limit %d; VL received: %s", maxDrilldownSeries, receivedQuery)
		}
	})
}

// TestProxyStatsQueryRangeDrilldown_ReturnsPerValueSeries verifies that the
// Drilldown direct path (low-cardinality field, short range) returns per-value
// series, appends VL-side sort+limit, and preserves the original step.
// Uses status_code — not a known high-cardinality field, short range (60 buckets).
func TestProxyStatsQueryRangeDrilldown_ReturnsPerValueSeries(t *testing.T) {
	var receivedQuery, receivedStep string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		receivedStep = r.FormValue("step")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
			`{"metric":{"status_code":"200"},"values":[[1700000060,"3"]]},`+
			`{"metric":{"status_code":"404"},"values":[[1700000060,"2"]]},`+
			`{"metric":{"status_code":"500"},"values":[[1700000060,"1"]]}]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	effectiveQuery := `env:="production" | filter status_code:!"" | stats by (status_code) count()`
	form := url.Values{}
	form.Set("query", `sum by (status_code) (count_over_time({env="production"}|status_code!=""`+` [1m]))`)
	form.Set("start", "1700000000")
	form.Set("end", "1700003600")
	form.Set("step", "60s")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	w := httptest.NewRecorder()
	p.proxyStatsQueryRangeDrilldown(w, req, effectiveQuery, `env:="production"`, "status_code")

	t.Logf("VL received query: %s step: %s", receivedQuery, receivedStep)
	body := w.Body.String()

	// Direct path must append VL-side sort+limit
	if !strings.Contains(receivedQuery, "stats by (status_code) count()") {
		t.Errorf("Drilldown path changed the query: %s", receivedQuery)
	}
	if !strings.Contains(receivedQuery, fmt.Sprintf("| limit %d", maxDrilldownSeries)) {
		t.Errorf("Drilldown path must add VL-side | limit %d: %s", maxDrilldownSeries, receivedQuery)
	}
	// Must return 3 separate per-value series — NOT a single aggregate
	seriesCount := strings.Count(body, `"status_code"`)
	if seriesCount != 3 {
		t.Errorf("expected 3 per-value series, got %d occurrences of status_code in: %s", seriesCount, body)
	}
	if strings.Contains(receivedQuery, "count() if") {
		t.Errorf("Drilldown path must not rewrite to count() if: %s", receivedQuery)
	}
	// Step must NOT be coarsened: 1h range uses the 60-bucket cap
	// (ranges ≤6h use maxDrilldownStatsBucketsShort=60), so minStep=3600/60=60s
	// which equals the requested step → proxy passes it through unchanged.
	if receivedStep != "60s" {
		t.Errorf("step must be unchanged (60s) for 1h/60s query with 60-bucket cap, VL received: %q", receivedStep)
	}
}

// TestProxyStatsQueryRangeDrilldown_EagerTwoPhaseForKnownHCField verifies that
// trace_id (and other *_id fields) skip the direct VL attempt entirely and go
// straight to the two-phase path. This is the primary CPU/memory optimization:
// no wasted 32 MB read-and-reject for known high-cardinality fields.
func TestProxyStatsQueryRangeDrilldown_EagerTwoPhaseForKnownHCField(t *testing.T) {
	var receivedQueries []string
	var mu sync.Mutex
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedQueries = append(receivedQueries, r.FormValue("query"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// Return 3 trace_id values from Phase 1 (single-bucket query)
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
			`{"metric":{"trace_id":"abc"},"values":[[1700001800,"3"]]},`+
			`{"metric":{"trace_id":"def"},"values":[[1700001800,"2"]]},`+
			`{"metric":{"trace_id":"ghi"},"values":[[1700001800,"1"]]}]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	effectiveQuery := `env:="production" | filter trace_id:!"" | stats by (trace_id) count()`
	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|trace_id!=""`+` [1m]))`)
	form.Set("start", "1700000000000000000")
	form.Set("end", "1700003600000000000")
	form.Set("step", "60s")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	w := httptest.NewRecorder()
	p.proxyStatsQueryRangeDrilldown(w, req, effectiveQuery, `env:="production"`, "trace_id")

	mu.Lock()
	nCalls := len(receivedQueries)
	q0 := ""
	q1 := ""
	if nCalls > 0 {
		q0 = receivedQueries[0]
	}
	if nCalls > 1 {
		q1 = receivedQueries[1]
	}
	mu.Unlock()

	t.Logf("VL received %d calls: q0=%s | q1=%s", nCalls, q0, q1)

	// Eager two-phase must issue exactly 2 VL calls (Phase 1 + Phase 2), NOT 1.
	// If only 1 call was made, the direct path was used — that's a regression.
	if nCalls != 2 {
		t.Errorf("expected 2 VL calls (two-phase), got %d — direct path used for trace_id?", nCalls)
	}
	// Phase 1: single-bucket call with the entire range as step, must have | limit 500
	if !strings.Contains(q0, fmt.Sprintf("| limit %d", maxDrilldownSeries)) {
		t.Errorf("Phase 1 query must have | limit %d: %s", maxDrilldownSeries, q0)
	}
	// Phase 2: filtered range with field:in(...) — NOT | limit (already bounded by Phase 1)
	if !strings.Contains(q1, `trace_id:in(`) {
		t.Errorf("Phase 2 query must filter via trace_id:in(...): %s", q1)
	}
	// Phase 2 must NOT have the direct | limit suffix — Phase 1 already bounded the values
	if strings.Contains(q1, "| limit") {
		t.Errorf("Phase 2 query must not add a second | limit: %s", q1)
	}
	// Response must include all 3 series the VL backend returned
	body := w.Body.String()
	if strings.Count(body, `"trace_id"`) < 3 {
		t.Errorf("response must include 3 trace_id series, body: %s", body)
	}
}

// TestAppendDrilldownSeriesLimit verifies the helper that appends VL-side sort+limit
// to Drilldown count() queries, and leaves other queries unchanged.
func TestAppendDrilldownSeriesLimit(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		limit     int
		want      string
		unchanged bool
	}{
		{
			name:  "standard drilldown count query",
			query: `env:="production" | filter trace_id:!"" | stats by (trace_id) count()`,
			limit: 500,
			want:  `env:="production" | filter trace_id:!"" | stats by (trace_id) count() as _c | sort by (_c desc) | limit 500`,
		},
		{
			name:  "with underscore fallback expansion in by()",
			query: `env:="production" | filter trace_id:!"" | stats by (trace_id, _trace_id) count()`,
			limit: 500,
			want:  `env:="production" | filter trace_id:!"" | stats by (trace_id, _trace_id) count() as _c | sort by (_c desc) | limit 500`,
		},
		{
			name:  "limit value is respected",
			query: `env:="production" | stats by (level) count()`,
			limit: 100,
			want:  `env:="production" | stats by (level) count() as _c | sort by (_c desc) | limit 100`,
		},
		{
			name:      "already-aliased count — suffix is not bare count()",
			query:     `env:="production" | stats by (trace_id) count() as x`,
			limit:     500,
			unchanged: true,
		},
		{
			name:      "non-count aggregation — unchanged",
			query:     `env:="production" | stats by (trace_id) sum(bytes)`,
			limit:     500,
			unchanged: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := appendDrilldownSeriesLimit(tc.query, tc.limit)
			if tc.unchanged {
				if got != tc.query {
					t.Errorf("expected unchanged, got %q", got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestProxyStatsQueryRange_StripsDeleteAlone verifies that | delete is stripped
// even when | unpack_json is already absent (e.g. second query after first pass).
// This matches the real-world slow query pattern: no unpack_json but still slow
// due to | delete overhead + stats by high-cardinality grouping.
func TestProxyStatsQueryRange_StripsDeleteAlone(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	// Real slow query seen in VL logs (no unpack_json, but delete still present)
	logsqlQuery := `env:="production" | delete __error__, __error_details__ | filter trace_id:!"" | stats by (trace_id) count()`

	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|json trace_id|drop __error__,__error_details__|trace_id!=""`+` [1m]))`)
	form.Set("start", "1779991980")
	form.Set("end", "1780035259")
	form.Set("step", "60000ms")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	w := httptest.NewRecorder()
	p.proxyStatsQueryRange(w, req, logsqlQuery)

	t.Logf("VL received: %s", receivedQuery)

	if strings.Contains(receivedQuery, "| delete") {
		t.Errorf("query still has | delete (should be stripped): %s", receivedQuery)
	}
	if !strings.Contains(receivedQuery, "stats by (trace_id) count()") {
		t.Errorf("query lost stats by grouping: %s", receivedQuery)
	}
	if !strings.Contains(receivedQuery, "| filter trace_id") {
		t.Errorf("query lost | filter trace_id: %s", receivedQuery)
	}
}

// TestIsLikelyHighCardinalityField verifies the field-name heuristic that gates
// eager two-phase skipping the direct VL path for known high-cardinality fields.
func TestIsLikelyHighCardinalityField(t *testing.T) {
	yes := []string{
		"trace_id", "span_id", "parent_id", "session_id",
		"request_id", "correlation_id", "transaction_id",
		"traceid", "spanid", "parentid",
		"trace.id", "span.id",
		"user_id", "order_id", "invoice_id",    // arbitrary _id suffix
		"request_uuid", "session_uuid",          // _uuid suffix
		"auth_token", "csrf_token",              // _token suffix
		"content_hash", "body_hash",             // _hash suffix
		"api_key", "secret_key",                 // _key suffix
		"TRACE_ID", "Span_ID",                   // case-insensitive
	}
	no := []string{
		"level", "env", "service_name", "status_code",
		"method", "path", "duration_ms", "host",
		"app", "cluster", "namespace", "pod",
		"detected_level", "log_level",
	}
	for _, name := range yes {
		if !isLikelyHighCardinalityField(name) {
			t.Errorf("expected isLikelyHighCardinalityField(%q) = true", name)
		}
	}
	for _, name := range no {
		if isLikelyHighCardinalityField(name) {
			t.Errorf("expected isLikelyHighCardinalityField(%q) = false", name)
		}
	}
}

// TestDrilldownShouldEagerTwoPhase verifies all two triggers for skipping the
// direct VL path in proxyStatsQueryRangeDrilldown.
func TestDrilldownShouldEagerTwoPhase(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	makeReq := func(start, end, step, orgID string) *http.Request {
		form := url.Values{}
		form.Set("start", start)
		form.Set("end", end)
		form.Set("step", step)
		req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
		req = req.WithContext(context.WithValue(req.Context(), orgIDKey, orgID))
		req.Header.Set("X-Scope-OrgID", orgID)
		return req
	}

	t.Run("known_hc_field_triggers_eager", func(t *testing.T) {
		req := makeReq("1700000000000000000", "1700003600000000000", "60s", "org1")
		if !p.drilldownShouldEagerTwoPhase(req, `env:="prod"`, "trace_id") {
			t.Error("trace_id must trigger eager two-phase")
		}
	})

	t.Run("low_cardinality_field_no_trigger", func(t *testing.T) {
		// Short range (60 buckets); not an HC field name; not in cardinality cache.
		req := makeReq("1700000000000000000", "1700003600000000000", "60s", "org1")
		if p.drilldownShouldEagerTwoPhase(req, `env:="prod"`, "level") {
			t.Error("level with short range must NOT trigger eager two-phase")
		}
	})

	t.Run("cardinality_cache_triggers_eager", func(t *testing.T) {
		req := makeReq("1700000000000000000", "1700003600000000000", "60s", "org2")
		p.drilldownCardCache.markHigh("org2", `env:="prod"`, "custom_id_field")
		if !p.drilldownShouldEagerTwoPhase(req, `env:="prod"`, "custom_id_field") {
			t.Error("cached high-cardinality must trigger eager two-phase")
		}
	})

	t.Run("cardinality_cache_other_org_no_trigger", func(t *testing.T) {
		req := makeReq("1700000000000000000", "1700003600000000000", "60s", "org3")
		// cache entry is for org2, not org3
		if p.drilldownShouldEagerTwoPhase(req, `env:="prod"`, "custom_id_field") {
			t.Error("cache entry for different org must not trigger eager two-phase")
		}
	})
}

// TestDrilldownTopValuesFromMatrix verifies that drilldownTopValuesFromMatrix
// extracts label values for the requested field from a Loki matrix JSON response,
// skipping entries where the field is absent.
func TestDrilldownTopValuesFromMatrix(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"trace_id":"abc","_c":"5"},"values":[[1700000000,"5"]]},` +
		`{"metric":{"trace_id":"def","_c":"3"},"values":[[1700000000,"3"]]},` +
		`{"metric":{"_c":"1"},"values":[[1700000000,"1"]]}` + // no trace_id — must be skipped
		`]}}`)

	got := drilldownTopValuesFromMatrix(body, "trace_id")
	if len(got) != 2 {
		t.Fatalf("expected 2 values (entry without trace_id skipped), got %d: %v", len(got), got)
	}
	if got[0] != "abc" || got[1] != "def" {
		t.Errorf("unexpected values: %v", got)
	}
}

func TestDrilldownTopValuesFromMatrix_InvalidJSON(t *testing.T) {
	got := drilldownTopValuesFromMatrix([]byte(`not json`), "trace_id")
	if got != nil {
		t.Errorf("invalid JSON should return nil, got %v", got)
	}
}

func TestDrilldownTopValuesFromMatrix_EmptyResult(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	got := drilldownTopValuesFromMatrix(body, "trace_id")
	if len(got) != 0 {
		t.Errorf("expected nil/empty for empty result, got %v", got)
	}
}

// TestBuildVLInFilter verifies the LogsQL field:in("v1","v2",...) string construction.
func TestBuildVLInFilter(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		values []string
		want   string
	}{
		{
			name:   "simple field with UUIDs",
			field:  "trace_id",
			values: []string{"abc123", "def456", "ghi789"},
			want:   `trace_id:in("abc123","def456","ghi789")`,
		},
		{
			name:   "field needing backtick quoting",
			field:  "service.name",
			values: []string{"frontend", "backend"},
			want:   "`service.name`:in(\"frontend\",\"backend\")",
		},
		{
			name:   "single value",
			field:  "level",
			values: []string{"error"},
			want:   `level:in("error")`,
		},
		{
			name:   "value with double quote is escaped",
			field:  "msg",
			values: []string{`say "hello"`},
			want:   `msg:in("say \"hello\"")`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildVLInFilter(tc.field, tc.values)
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestDrilldownTwoPhase_BasicFlow verifies the two-phase fallback:
//   - Phase 1 is a single-bucket query (step = entire range) that returns global top-N
//   - Phase 2 is a filtered range query using field:in(...) restricted to Phase 1 values
//   - The final response contains the per-value series from Phase 2
func TestDrilldownTwoPhase_BasicFlow(t *testing.T) {
	var phase1Query, phase1Step, phase2Query string
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		switch callCount {
		case 1: // Phase 1: single-bucket query returning global top-3 trace_ids
			phase1Query = r.FormValue("query")
			phase1Step = r.FormValue("step")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
				`{"metric":{"trace_id":"trace-abc"},"values":[[1700000000,"5"]]},`+
				`{"metric":{"trace_id":"trace-def"},"values":[[1700000000,"3"]]},`+
				`{"metric":{"trace_id":"trace-ghi"},"values":[[1700000000,"1"]]}]}}`)
		case 2: // Phase 2: filtered range query
			phase2Query = r.FormValue("query")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
				`{"metric":{"trace_id":"trace-abc"},"values":[[1700000060,"5"]]},`+
				`{"metric":{"trace_id":"trace-def"},"values":[[1700000060,"3"]]},`+
				`{"metric":{"trace_id":"trace-ghi"},"values":[[1700000060,"1"]]}]}}`)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|trace_id!=""`+` [1m]))`)
	form.Set("start", "1700000000")
	form.Set("end", "1700003600")
	form.Set("step", "60s")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	effectiveQuery := `env:="production" | filter trace_id:!"" | stats by (trace_id) count()`
	cleanBase := `env:="production"`

	body := p.drilldownTwoPhase(req, effectiveQuery, cleanBase, "trace_id", "")

	if body == nil {
		t.Fatal("drilldownTwoPhase returned nil, expected valid response")
	}
	if callCount != 2 {
		t.Errorf("expected 2 VL calls (Phase 1 + Phase 2), got %d", callCount)
	}

	// Phase 1 must use step = entire range (3600s for 1700000000–1700003600)
	if phase1Step != "3600s" {
		t.Errorf("Phase 1 step should be entire range 3600s, got %q", phase1Step)
	}
	// Phase 1 must include | limit 500
	if !strings.Contains(phase1Query, fmt.Sprintf("| limit %d", maxDrilldownSeries)) {
		t.Errorf("Phase 1 query must append | limit %d: %q", maxDrilldownSeries, phase1Query)
	}

	// Phase 2 must use field:in(...) filter with the values from Phase 1
	for _, id := range []string{"trace-abc", "trace-def", "trace-ghi"} {
		if !strings.Contains(phase2Query, `"`+id+`"`) {
			t.Errorf("Phase 2 query missing trace_id %q: %q", id, phase2Query)
		}
	}
	if !strings.Contains(phase2Query, "trace_id:in(") {
		t.Errorf("Phase 2 query must use field:in() filter: %q", phase2Query)
	}
	// Phase 2 must NOT include the original | filter trace_id:!"" existence check
	// (the in() filter replaces it)
	if strings.Contains(phase2Query, `trace_id:!""`) {
		t.Errorf("Phase 2 query must not contain original existence filter: %q", phase2Query)
	}

	// Final response must contain all 3 trace_id series
	bodyStr := string(body)
	for _, id := range []string{"trace-abc", "trace-def", "trace-ghi"} {
		if !strings.Contains(bodyStr, id) {
			t.Errorf("response missing trace_id %q: %s", id, bodyStr)
		}
	}
}

// TestDrilldownTwoPhase_EmptyPhase1 verifies that drilldownTwoPhase returns
// emptyLokiMatrix (not nil) when Phase 1 finds no field values.
func TestDrilldownTwoPhase_EmptyPhase1(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|trace_id!=""`+` [1m]))`)
	form.Set("start", "1700000000")
	form.Set("end", "1700003600")
	form.Set("step", "60s")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	body := p.drilldownTwoPhase(req,
		`env:="production" | filter trace_id:!"" | stats by (trace_id) count()`,
		`env:="production"`, "trace_id", "")

	// Must return emptyLokiMatrix — not nil — so the caller can write it to Grafana
	if body == nil {
		t.Fatal("expected emptyLokiMatrix, got nil")
	}
	if string(body) != string(emptyLokiMatrix) {
		t.Errorf("expected emptyLokiMatrix, got %q", body)
	}
}

func TestCoarsenDrilldownStep(t *testing.T) {
	// Use Unix second strings with a realistic base timestamp.
	// parseLokiTimeToUnixNano treats values < 1e11 as seconds, so real
	// Unix timestamps (~1.75e9) are safely in that range and avoid the
	// millisecond misclassification that hits ns-formatted near-epoch values.
	const baseTS int64 = 1748000000 // 2025-05-23, well within the seconds range
	ts := func(relSec int64) string { return strconv.FormatInt(baseTS+relSec, 10) }

	tests := []struct {
		name     string
		start    string
		end      string
		step     time.Duration
		wantStep time.Duration
	}{
		// Ranges >6h use the 30-bucket cap.
		{
			name:     "12h range step=60s coarsens to 30min",
			start:    ts(0),
			end:      ts(12 * 3600),
			step:     60 * time.Second,
			wantStep: 30 * time.Minute, // 12h/30 = 1440s → snap to 30min
		},
		{
			name:     "24h range step=120s coarsens to 1h",
			start:    ts(0),
			end:      ts(24 * 3600),
			step:     120 * time.Second,
			wantStep: time.Hour, // 24h/30 = 2880s → snap to 1h
		},
		// Ranges ≤6h use the 60-bucket cap, reducing expansion factor and
		// giving finer-grained VL data without the staircase pattern.
		{
			name:     "6h range step=60s coarsens to 10min",
			start:    ts(0),
			end:      ts(6 * 3600),
			step:     60 * time.Second,
			wantStep: 10 * time.Minute, // 6h/60 = 360s → snap to 10min
		},
		{
			name:     "3h range step=38s coarsens to 3min",
			start:    ts(0),
			end:      ts(3 * 3600),
			step:     38 * time.Second,
			wantStep: 3 * time.Minute, // 3h/60 = 180s → snap to 3min (factor ≈4 vs old 15)
		},
		{
			name:     "1h range step=60s within 60-bucket budget, no coarsening",
			start:    ts(0),
			end:      ts(3600),
			step:     60 * time.Second,
			wantStep: 60 * time.Second, // 1h/60 = 60s ≤ step → unchanged
		},
		{
			name:     "30min range step=30s within 60-bucket budget, no coarsening",
			start:    ts(0),
			end:      ts(1800),
			step:     30 * time.Second,
			wantStep: 30 * time.Second, // 30min/60 = 30s ≤ step → unchanged
		},
		{
			name:     "zero step passes through",
			start:    ts(0),
			end:      ts(12 * 3600),
			step:     0,
			wantStep: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coarsenDrilldownStep(tt.start, tt.end, tt.step)
			if got != tt.wantStep {
				t.Errorf("coarsenDrilldownStep(%q, %q, %v) = %v, want %v",
					tt.start, tt.end, tt.step, got, tt.wantStep)
			}
		})
	}
}

// TestProxyStatsQueryRange_DrilldownDetectionForTranslatedQueries verifies that
// both translation patterns that previously broke drilldown detection now work:
//
//  1. detected_level (pipe-filter after parser strip): after translation the query
//     contains | unpack_logfmt | filter level:!""; the proxy must strip the parser
//     stage and detect drilldown, coarsening the step for the 12h range.
//
//  2. stream-label level (stream-selector existence check): the VL stream selector
//     contains level:!"" without a | filter prefix; detection must recognise the
//     existence check and coarsen the step.
func TestProxyStatsQueryRange_DrilldownDetectionForTranslatedQueries(t *testing.T) {
	const baseTS int64 = 1748000000 // 2025-05-23
	ts := func(relSec int64) string { return strconv.FormatInt(baseTS+relSec, 10) }

	cases := []struct {
		name      string
		logsql    string // VL-translated query as proxy receives it
		wantStrip string // pipe stage that must NOT appear in the forwarded query
	}{
		{
			// detected_level: Loki adds | logfmt level | drop __error__ | level!="" before
			// the stats clause; proxy must strip | unpack_logfmt and route drilldown path.
			name:      "detected_level_with_parser_stage",
			logsql:    `env:="production" | unpack_logfmt | filter level:!"" | stats by (level) count()`,
			wantStrip: "unpack_logfmt",
		},
		{
			// stream-label level: Loki stream selector {env="production", level!=""} translates
			// to env:="production" level:!"" in VL — no | filter prefix for the existence check.
			name:   "stream_label_level_selector",
			logsql: `env:="production" level:!"" | stats by (level) count()`,
			// No parser stage to strip — just verify drilldown detection fires (step coarsened).
			wantStrip: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var receivedQuery, receivedStep string
			vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedQuery = r.FormValue("query")
				receivedStep = r.FormValue("step")
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
			}))
			defer vlBackend.Close()

			p := newTestProxy(t, vlBackend.URL)

			form := url.Values{}
			form.Set("query", `sum by (level) (count_over_time({env="production",level!=""} [1m]))`)
			form.Set("start", ts(0))
			form.Set("end", ts(12*3600)) // 12h range
			form.Set("step", "60s")      // fine step — must coarsen to ≥1800s

			req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
			req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
			req.Header.Set("X-Scope-OrgID", "default")
			req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")

			p.proxyStatsQueryRange(httptest.NewRecorder(), req, tc.logsql)

			t.Logf("VL received query=%s step=%s", receivedQuery, receivedStep)

			if tc.wantStrip != "" && strings.Contains(receivedQuery, tc.wantStrip) {
				t.Errorf("parser stage %q still present in VL query: %s", tc.wantStrip, receivedQuery)
			}
			// Drilldown detection must fire — step must be coarsened (12h/60s=720 buckets > 30).
			if receivedStep == "60s" {
				t.Errorf("step not coarsened (still 60s) — drilldown detection did not fire for %q", tc.logsql)
			}
		})
	}
}
