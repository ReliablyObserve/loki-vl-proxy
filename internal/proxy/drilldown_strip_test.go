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

// TestProxyStatsQueryRange_StripsToCountIf verifies the two-pass Drilldown rewrite:
// 1. | unpack_json stripped (pass 1)
// 2. stats by (field) count() → count() if (field:*) with redundant pipes dropped (pass 2)
func TestProxyStatsQueryRange_StripsToCountIf(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1779957360,"42"]]}]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	// VL translation of: sum by (trace_id) (count_over_time({...} | json trace_id | drop __error__ | trace_id!="" [1m]))
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
	if strings.Contains(receivedQuery, "stats by") {
		t.Errorf("query still uses stats by (high-cardinality grouping): %s", receivedQuery)
	}
	if !strings.Contains(receivedQuery, "count() if (trace_id:*)") {
		t.Errorf("query missing count() if rewrite: %s", receivedQuery)
	}
}

// TestProxyStatsQueryRange_CountIfRewrite_AlreadyStripped verifies pass 2 in isolation:
// when unpack_json was already absent, stats by (field) count() still gets rewritten.
func TestProxyStatsQueryRange_CountIfRewrite_AlreadyStripped(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1779991980,"10"]]}]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	// This matches what VL actually logged: no unpack_json, but still stats by (trace_id) count()
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

	if strings.Contains(receivedQuery, "stats by") {
		t.Errorf("query still uses stats by grouping: %s", receivedQuery)
	}
	if !strings.Contains(receivedQuery, "count() if (trace_id:*)") {
		t.Errorf("query missing count() if rewrite: %s", receivedQuery)
	}
	if strings.Contains(receivedQuery, "delete") {
		t.Errorf("query still has | delete pipe (redundant): %s", receivedQuery)
	}
	if strings.Contains(receivedQuery, "| filter") {
		t.Errorf("query still has | filter pipe (redundant): %s", receivedQuery)
	}
}

// TestProxyStatsQueryRange_MultiFilterNotRewritten verifies that when multiple
// existence filters are present (for different fields), the count() if rewrite
// is NOT applied — it would change semantics by dropping the extra filter.
func TestProxyStatsQueryRange_MultiFilterNotRewritten(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	// Two existence filters — the span_id filter is not for the GroupBy field (trace_id)
	logsqlQuery := `env:="production" | filter span_id:!"" | filter trace_id:!"" | stats by (trace_id) count()`

	form := url.Values{}
	form.Set("query", `sum by (trace_id) (count_over_time({env="production"}|trace_id!=""|span_id!=""`+` [1m]))`)
	form.Set("start", "1779991980")
	form.Set("end", "1780035259")
	form.Set("step", "60000ms")

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	req = req.WithContext(context.WithValue(req.Context(), orgIDKey, "default"))
	req.Header.Set("X-Scope-OrgID", "default")

	w := httptest.NewRecorder()
	p.proxyStatsQueryRange(w, req, logsqlQuery)

	t.Logf("VL received: %s", receivedQuery)

	// Should NOT rewrite — two different existence filters mean drop-filter changes semantics
	if strings.Contains(receivedQuery, "count() if") {
		t.Errorf("count() if was applied with multiple filters (unsafe): %s", receivedQuery)
	}
}
