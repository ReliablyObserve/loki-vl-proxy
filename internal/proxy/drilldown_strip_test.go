package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
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

// TestProxyStatsQueryRangeDrilldown_ReturnsPerValueSeries verifies that the
// Drilldown path returns actual per-value series (not a single aggregate) from VL,
// appends a VL-side sort+limit, and applies series limiting when the response
// contains more than maxDrilldownSeries.
func TestProxyStatsQueryRangeDrilldown_ReturnsPerValueSeries(t *testing.T) {
	var receivedQuery, receivedStep string
	// VL returns 3 per-value series for trace_id — the Drilldown path must preserve
	// all of them (unlike a count() if approach which collapses to one).
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		receivedStep = r.FormValue("step")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
			`{"metric":{"trace_id":"abc"},"values":[[1700000060,"3"]]},`+
			`{"metric":{"trace_id":"def"},"values":[[1700000060,"2"]]},`+
			`{"metric":{"trace_id":"ghi"},"values":[[1700000060,"1"]]}]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	effectiveQuery := `env:="production" | filter trace_id:!"" | stats by (trace_id) count()`
	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|trace_id!=""`+` [1m]))`)
	form.Set("start", "1700000000")
	form.Set("end", "1700003600")
	form.Set("step", "60s")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	w := httptest.NewRecorder()
	p.proxyStatsQueryRangeDrilldown(w, req, effectiveQuery, `env:="production"`, "trace_id")

	t.Logf("VL received query: %s step: %s", receivedQuery, receivedStep)
	body := w.Body.String()

	// Must forward stats by (trace_id) count() with VL-side sort+limit
	if !strings.Contains(receivedQuery, "stats by (trace_id) count()") {
		t.Errorf("Drilldown path changed the query: %s", receivedQuery)
	}
	// Must append VL-side limit to bound high-cardinality responses at source
	if !strings.Contains(receivedQuery, fmt.Sprintf("| limit %d", maxDrilldownSeries)) {
		t.Errorf("Drilldown path must add VL-side | limit %d: %s", maxDrilldownSeries, receivedQuery)
	}
	// Must return 3 separate per-value series — NOT a single aggregate
	seriesCount := strings.Count(body, `"trace_id"`)
	if seriesCount != 3 {
		t.Errorf("expected 3 per-value series, got %d occurrences of trace_id in: %s", seriesCount, body)
	}
	// Must NOT use count() if
	if strings.Contains(receivedQuery, "count() if") {
		t.Errorf("Drilldown path must not rewrite to count() if: %s", receivedQuery)
	}
	// REGRESSION GUARD: step must NOT be inflated. Inflating the step to VL breaks
	// Grafana's timestamp alignment and causes "no data" for ALL Drilldown fields.
	// The step VL receives on the direct path must exactly match Grafana's 60s step.
	// (The two-phase fallback uses a different step only for its Phase 1 single-bucket
	// call — Phase 2 always uses the original step.)
	if receivedStep != "60s" {
		t.Errorf("step must be preserved as 60s (not inflated), VL received: %q", receivedStep)
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

	body := p.drilldownTwoPhase(req, effectiveQuery, cleanBase, "trace_id")

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
		`env:="production"`, "trace_id")

	// Must return emptyLokiMatrix — not nil — so the caller can write it to Grafana
	if body == nil {
		t.Fatal("expected emptyLokiMatrix, got nil")
	}
	if string(body) != string(emptyLokiMatrix) {
		t.Errorf("expected emptyLokiMatrix, got %q", body)
	}
}

