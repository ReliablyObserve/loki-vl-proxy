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
