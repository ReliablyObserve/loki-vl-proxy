package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

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
		got := limitLokiResultSeries(body, 100)
		if string(got) != string(body) {
			t.Errorf("expected body unchanged, got different result")
		}
	})

	t.Run("at limit unchanged", func(t *testing.T) {
		body := makeSeries(100)
		got := limitLokiResultSeries(body, 100)
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

		got := limitLokiResultSeries(body, 100)
		// High-count series at index 150 must be in the top-100 output
		if !strings.Contains(string(got), `"id-150"`) {
			t.Errorf("high-count series (id-150) should survive top-by-count cut")
		}
	})

	t.Run("invalid json returned unchanged", func(t *testing.T) {
		body := []byte(`not json`)
		got := limitLokiResultSeries(body, 5)
		if string(got) != string(body) {
			t.Errorf("invalid JSON should be returned unchanged")
		}
	})
}

// TestProxyStatsQueryRange_StripsDeleteAlone verifies that | delete is stripped
// even when | unpack_json is already absent (e.g. second query after first pass).
// This matches the real-world slow query pattern: no unpack_json but still slow
// due to | delete overhead + stats by high-cardinality grouping.
func TestProxyStatsQueryRange_StripsDeleteAlone(t *testing.T) {
	var receivedQuery string
	var receivedMu sync.Mutex
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMu.Lock()
		receivedQuery = r.FormValue("query")
		receivedMu.Unlock()
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
