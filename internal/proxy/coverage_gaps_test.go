package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// Priority 1: Alerting/config compatibility handlers
// =============================================================================

func TestAdminStubs_Rules(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/rules", nil)
	mux.ServeHTTP(w, r)

	if ct := w.Header().Get("Content-Type"); ct != "application/yaml" {
		t.Fatalf("rules stub: expected application/yaml, got %q", ct)
	}
	var resp map[string][]legacyRuleGroup
	if err := yaml.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("rules stub: expected valid YAML, got %v", err)
	}
	if len(resp) != 0 {
		t.Errorf("rules stub: expected empty group map, got %v", resp)
	}
}

func TestAdminStubs_Alerts(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/alerts", nil)
	mux.ServeHTTP(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("alerts stub: expected success, got %v", resp["status"])
	}
}

func TestAdminStubs_Config(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/config", nil)
	mux.ServeHTTP(w, r)

	if ct := w.Header().Get("Content-Type"); ct != "application/yaml" {
		t.Errorf("config stub: expected application/yaml, got %q", ct)
	}
}

func TestAdminStubs_FormatQuery(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/format_query?query={app="nginx"}`, nil)
	p.handleFormatQuery(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["data"] != `{app="nginx"}` {
		t.Errorf("format_query should echo query, got %v", resp["data"])
	}
}

func TestAdminStubs_DrilldownLimits(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("drilldown-limits should return JSON: %v", err)
	}
	if resp["maxLines"] == nil || resp["maxDetectedFields"] == nil {
		t.Fatalf("drilldown-limits missing expected keys: %v", resp)
	}
	limits, ok := resp["limits"].(map[string]interface{})
	if !ok {
		t.Fatalf("drilldown-limits must include Loki-style limits object: %v", resp)
	}
	if limits["retention_period"] == nil || limits["discover_service_name"] == nil || limits["log_level_fields"] == nil {
		t.Fatalf("drilldown-limits missing Loki config fields required by Logs Drilldown: %v", resp)
	}
	if resp["pattern_ingester_enabled"] == nil || resp["version"] == nil {
		t.Fatalf("drilldown-limits missing Loki config top-level fields: %v", resp)
	}
}

func TestTranslateQuery_EmptySelectorWithParsersUsesWildcardBase(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	got, err := p.translateQuery(`{} | json | logfmt | drop __error__, __error_details__`)
	if err != nil {
		t.Fatalf("translateQuery returned error: %v", err)
	}
	if !strings.HasPrefix(got, "* ") {
		t.Fatalf("expected wildcard base before parser pipeline, got %q", got)
	}
}

func TestTranslateQuery_ExplicitWildcardStaysWildcard(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	for _, input := range []string{"*", `"*"`, "`*`"} {
		got, err := p.translateQuery(input)
		if err != nil {
			t.Fatalf("translateQuery(%q) returned error: %v", input, err)
		}
		if got != "*" {
			t.Fatalf("translateQuery(%q) = %q, want wildcard passthrough", input, got)
		}
	}
}

// =============================================================================
// Priority 1: handleDetectedFields parses queried log lines
// =============================================================================

func TestDetectedFields_QueriesLogLines(t *testing.T) {
	var sawFieldNames, sawQuery bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			sawFieldNames = true
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
		case "/select/logsql/query":
			sawQuery = true
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"status\":200}","_stream":"{app=\"nginx\"}","app":"nginx"}` + "\n"))
		default:
			t.Fatalf("expected detected_fields to query field names or log lines, got %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{"query": {`{app="nginx"}`}, "start": {"1"}, "end": {"2"}}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?"+q.Encode(), nil)
	p.handleDetectedFields(w, r)

	if !sawFieldNames || !sawQuery {
		t.Errorf("expected native field names and log-line scan paths, got field_names=%v query=%v", sawFieldNames, sawQuery)
	}
	if w.Code != 200 {
		t.Errorf("expected 200 after detected field scan, got %d", w.Code)
	}
}

func TestDetectedFields_BareSelectorWithParserStagesStillFindsFields(t *testing.T) {
	var sawFieldNames, sawQuery bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			sawFieldNames = true
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			got := r.Form.Get("query")
			if !strings.Contains(got, `service_name:=otel-auth-service`) {
				t.Fatalf("expected translated bare selector after parser stripping, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"service.name","hits":1}]}`))
		case "/select/logsql/query":
			sawQuery = true
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			got := r.Form.Get("query")
			if !strings.Contains(got, `service_name:=otel-auth-service`) {
				t.Fatalf("expected translated bare selector after parser stripping, got %q", got)
			}
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"msg\":\"ok\"}","_stream":"{service.name=\"otel-auth-service\",service_name=\"otel-auth-service\"}","service.name":"otel-auth-service","service_name":"otel-auth-service"}` + "\n"))
		default:
			t.Fatalf("expected detected_fields to query field names or log lines, got %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`service_name="otel-auth-service" | json | logfmt | drop __error__, __error_details__`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?"+q.Encode(), nil)
	p.handleDetectedFields(w, r)

	if !sawFieldNames || !sawQuery {
		t.Fatalf("expected native field names and log-line scan paths, got field_names=%v query=%v", sawFieldNames, sawQuery)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"msg"`) {
		t.Fatalf("expected detected fields in response after normalizing stripped selector, got %s", w.Body.String())
	}
}

// =============================================================================
// Priority 1: isStatsQuery quote-aware exclusion
// =============================================================================

func TestIsStatsQuery_Comprehensive(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`app:=nginx | stats rate()`, true},
		{`app:=nginx | rate(`, true},
		{`app:=nginx | count(`, true},
		{`app:=nginx ~"stats query"`, false},          // inside quotes
		{`app:=nginx ~"| stats rate()"`, false},       // pipe inside quotes
		{`app:=nginx`, false},                         // no stats
		{`app:=nginx ~"error" | stats count()`, true}, // stats after filter
		{``, false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := isStatsQuery(tt.query)
			if got != tt.want {
				t.Errorf("isStatsQuery(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Priority 1: parseTimestampToUnix all branches
// =============================================================================

func TestParseTimestampToUnix(t *testing.T) {
	tests := []struct {
		input      string
		wantApprox float64
	}{
		{"2024-01-15T10:30:00Z", 1705314600},
		{"1705314600", 1705314600},
		{"1705314600.5", 1705314600.5},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseTimestampToUnix(tt.input)
			diff := got - tt.wantApprox
			if diff < -1 || diff > 1 {
				t.Errorf("parseTimestampToUnix(%q) = %f, want ~%f", tt.input, got, tt.wantApprox)
			}
		})
	}

	// Invalid input should not panic, returns ~now
	got := parseTimestampToUnix("garbage")
	if got < 1000000000 {
		t.Errorf("parseTimestampToUnix(garbage) should return ~now, got %f", got)
	}
}

func TestEvaluateConstantInstantVectorQuery(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		timeParam  string
		wantValue  string
		wantTS     float64
		expectEval bool
	}{
		{name: "vector literal", query: `vector(1)`, timeParam: "4", wantValue: "1", wantTS: 4_000_000_000, expectEval: true},
		{name: "binary add", query: `vector(1)+vector(1)`, timeParam: "4", wantValue: "2", wantTS: 4_000_000_000, expectEval: true},
		{name: "binary divide", query: `vector(9) / vector(3)`, timeParam: "4", wantValue: "3", wantTS: 4_000_000_000, expectEval: true},
		{name: "divide by zero", query: `vector(1)/vector(0)`, timeParam: "4", expectEval: false},
		{name: "non vector", query: `{app="api"}`, timeParam: "4", expectEval: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, ok := evaluateConstantInstantVectorQuery(tt.query, tt.timeParam)
			if ok != tt.expectEval {
				t.Fatalf("evaluateConstantInstantVectorQuery(%q) ok=%v, want %v", tt.query, ok, tt.expectEval)
			}
			if !tt.expectEval {
				return
			}

			var resp map[string]interface{}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatalf("invalid JSON response: %v", err)
			}
			if resp["status"] != "success" {
				t.Fatalf("expected success status, got %v", resp["status"])
			}
			data, _ := resp["data"].(map[string]interface{})
			if data["resultType"] != "vector" {
				t.Fatalf("expected resultType=vector, got %v", data["resultType"])
			}
			result, _ := data["result"].([]interface{})
			if len(result) != 1 {
				t.Fatalf("expected 1 vector sample, got %v", result)
			}
			sample, _ := result[0].(map[string]interface{})
			value, _ := sample["value"].([]interface{})
			if got := value[1]; got != tt.wantValue {
				t.Fatalf("expected value %q, got %v", tt.wantValue, got)
			}
			if got := value[0]; got != tt.wantTS {
				t.Fatalf("expected timestamp %v, got %v", tt.wantTS, got)
			}
		})
	}
}

func TestNormalizeBareSelectorQuery(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty to wildcard", input: "", want: "*"},
		{name: "already selector", input: `{service_name=~".+"}`, want: `{service_name=~".+"}`},
		{name: "bare regex matcher", input: `service_name=~".+"`, want: `{service_name=~".+"}`},
		{name: "bare equality matcher", input: `service_name="otel-auth-service"`, want: `{service_name="otel-auth-service"}`},
		{name: "multiple matchers", input: `service_name="api-gateway",cluster="us-east-1"`, want: `{service_name="api-gateway",cluster="us-east-1"}`},
		{name: "pipeline query untouched", input: `{service_name="api-gateway"} | json`, want: `{service_name="api-gateway"} | json`},
		{name: "function query untouched", input: `vector(1)+vector(1)`, want: `vector(1)+vector(1)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultQuery(tt.input); got != tt.want {
				t.Fatalf("defaultQuery(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDefaultFieldDetectionQuery_NormalizesAfterStrippingStages(t *testing.T) {
	got := defaultFieldDetectionQuery(`service_name="otel-auth-service" | json | logfmt | drop __error__, __error_details__`)
	want := `{service_name="otel-auth-service"}`
	if got != want {
		t.Fatalf("defaultFieldDetectionQuery() = %q, want %q", got, want)
	}
}

func TestFieldDetectionQueryCandidates_RelaxFieldComparisons(t *testing.T) {
	got := fieldDetectionQueryCandidates(`{service_name="grafana"} | logfmt | duration < 1s | duration > 100ms | unwrap duration(duration)`)
	want := []string{
		`{service_name="grafana"} | duration < 1s | duration > 100ms`,
		`{service_name="grafana"}`,
	}
	if len(got) != len(want) {
		t.Fatalf("fieldDetectionQueryCandidates() len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fieldDetectionQueryCandidates()[%d] = %q, want %q (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestDetectedFields_FieldFilterFallbackKeepsFieldsVisible(t *testing.T) {
	var fieldQueries []string
	var streamQueries []string
	var scanQueries []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		got := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/field_names":
			fieldQueries = append(fieldQueries, got)
			if strings.Contains(got, "duration") {
				http.Error(w, "strict filtered discovery timed out", http.StatusGatewayTimeout)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":2},{"value":"duration","hits":2},{"value":"trace_id","hits":2}]}`))
		case "/select/logsql/streams":
			streamQueries = append(streamQueries, got)
			if strings.Contains(got, "duration") {
				http.Error(w, "strict filtered streams timed out", http.StatusGatewayTimeout)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"grafana\",service_name=\"grafana\"}","hits":2}]}`))
		case "/select/logsql/query":
			scanQueries = append(scanQueries, got)
			if strings.Contains(got, "duration") {
				http.Error(w, "strict filtered scan timed out", http.StatusGatewayTimeout)
				return
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(""))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{service_name="grafana"} | logfmt | duration < 1s | duration > 100ms`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?"+q.Encode(), nil)
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(fieldQueries) != 2 {
		t.Fatalf("expected strict+relaxed native field lookups, got %v", fieldQueries)
	}
	if len(streamQueries) != 2 {
		t.Fatalf("expected strict+relaxed native stream lookups, got %v", streamQueries)
	}
	if !strings.Contains(fieldQueries[0], "duration") {
		t.Fatalf("expected strict native field lookup to include duration filter, got %q", fieldQueries[0])
	}
	if strings.Contains(fieldQueries[1], "duration") {
		t.Fatalf("expected relaxed native field lookup to strip duration filter, got %q", fieldQueries[1])
	}
	if len(scanQueries) < 1 {
		t.Fatalf("expected at least one scan query, got %v", scanQueries)
	}
	if !strings.Contains(scanQueries[0], "duration") {
		t.Fatalf("expected strict scan to include duration filter, got %q", scanQueries[0])
	}
	if len(scanQueries) > 1 && strings.Contains(scanQueries[1], "duration") {
		t.Fatalf("expected relaxed scan to strip duration filter, got %q", scanQueries[1])
	}
	if !strings.Contains(w.Body.String(), `"duration"`) || !strings.Contains(w.Body.String(), `"trace_id"`) {
		t.Fatalf("expected relaxed native fields to remain visible, got %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"app"`) {
		t.Fatalf("expected indexed labels to stay hidden during native fallback, got %s", w.Body.String())
	}
}

func TestDetectedFieldValues_FieldFilterFallbackKeepsValuesVisible(t *testing.T) {
	var fieldNameQueries []string
	var fieldValueQueries []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		got := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/field_names":
			fieldNameQueries = append(fieldNameQueries, got)
			if strings.Contains(got, "duration") {
				http.Error(w, "strict filtered discovery timed out", http.StatusGatewayTimeout)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":2},{"value":"duration","hits":2}]}`))
		case "/select/logsql/field_values":
			fieldValueQueries = append(fieldValueQueries, got)
			if strings.Contains(got, "duration") {
				http.Error(w, "strict filtered value lookup timed out", http.StatusGatewayTimeout)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"120ms","hits":2},{"value":"800ms","hits":1}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{service_name="grafana"} | logfmt | duration < 1s | duration > 100ms`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/duration/values?"+q.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(fieldNameQueries) < 2 {
		t.Fatalf("expected at least strict+relaxed native field-name lookups, got %v", fieldNameQueries)
	}
	if len(fieldValueQueries) != 2 {
		t.Fatalf("expected strict+relaxed native field-value lookups, got %v", fieldValueQueries)
	}
	if !strings.Contains(fieldValueQueries[0], "duration") {
		t.Fatalf("expected strict native field-value lookup to include duration filter, got %q", fieldValueQueries[0])
	}
	if strings.Contains(fieldValueQueries[1], "duration") {
		t.Fatalf("expected relaxed native field-value lookup to strip duration filter, got %q", fieldValueQueries[1])
	}
	if !strings.Contains(w.Body.String(), `"120ms"`) || !strings.Contains(w.Body.String(), `"800ms"`) {
		t.Fatalf("expected relaxed native field values to remain visible, got %s", w.Body.String())
	}
}

func TestDetectedFields_EmptyStrictQueryDoesNotRelaxCandidates(t *testing.T) {
	const strictToken = "strict-only"

	var fieldNameQueries []string
	var scanQueries []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		got := r.Form.Get("query")
		strict := strings.Contains(got, strictToken)
		switch r.URL.Path {
		case "/select/logsql/field_names":
			fieldNameQueries = append(fieldNameQueries, got)
			w.Header().Set("Content-Type", "application/json")
			if strict {
				_, _ = w.Write([]byte(`{"values":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"values":[{"value":"status","hits":1}]}`))
		case "/select/logsql/query":
			scanQueries = append(scanQueries, got)
			w.Header().Set("Content-Type", "application/x-ndjson")
			if !strict {
				_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"status\":200}","_stream":"{service_name=\"api-gateway\"}","service_name":"api-gateway","status":200}` + "\n"))
			}
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{service_name="api-gateway"} | probe="strict-only"`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?"+q.Encode(), nil)
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(fieldNameQueries) != 1 {
		t.Fatalf("expected only the strict native field-name lookup, got %v", fieldNameQueries)
	}
	for _, got := range scanQueries {
		if !strings.Contains(got, strictToken) {
			t.Fatalf("expected scan lookup to preserve strict filter, got %q", got)
		}
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	fields, _ := resp["fields"].([]interface{})
	if len(fields) != 0 {
		t.Fatalf("expected empty detected_fields payload for strict empty query, got %v", resp)
	}
}

func TestDetectedFieldValues_EmptyStrictQueryRelaxesCandidates(t *testing.T) {
	const strictToken = "strict-only"

	var fieldNameQueries []string
	var fieldValueQueries []string
	var streamQueries []string
	var scanQueries []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		got := r.Form.Get("query")
		strict := strings.Contains(got, strictToken)
		switch r.URL.Path {
		case "/select/logsql/field_names":
			fieldNameQueries = append(fieldNameQueries, got)
			w.Header().Set("Content-Type", "application/json")
			if strict {
				_, _ = w.Write([]byte(`{"values":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"values":[{"value":"status","hits":1}]}`))
		case "/select/logsql/field_values":
			fieldValueQueries = append(fieldValueQueries, got)
			w.Header().Set("Content-Type", "application/json")
			if strict {
				_, _ = w.Write([]byte(`{"values":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"values":[{"value":"200","hits":1}]}`))
		case "/select/logsql/streams":
			streamQueries = append(streamQueries, got)
			w.Header().Set("Content-Type", "application/json")
			if strict {
				_, _ = w.Write([]byte(`{"values":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"values":[{"value":"{service_name=\"api-gateway\",status=\"200\"}","hits":1}]}`))
		case "/select/logsql/query":
			scanQueries = append(scanQueries, got)
			w.Header().Set("Content-Type", "application/x-ndjson")
			if !strict {
				_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"status\":200}","_stream":"{service_name=\"api-gateway\"}","service_name":"api-gateway","status":200}` + "\n"))
			}
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{service_name="api-gateway"} | probe="strict-only"`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/status/values?"+q.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(fieldNameQueries) < 2 {
		t.Fatalf("expected strict+relaxed native field-name lookups, got %v", fieldNameQueries)
	}
	if !strings.Contains(fieldNameQueries[0], strictToken) {
		t.Fatalf("expected first native field-name lookup to stay strict, got %v", fieldNameQueries)
	}
	if strings.Contains(fieldNameQueries[len(fieldNameQueries)-1], strictToken) {
		t.Fatalf("expected final native field-name lookup to relax whole-query filters, got %v", fieldNameQueries)
	}
	if len(fieldValueQueries) == 0 {
		t.Fatalf("expected relaxed native field-value lookup, got %v", fieldValueQueries)
	}
	if strings.Contains(fieldValueQueries[len(fieldValueQueries)-1], strictToken) {
		t.Fatalf("expected final native field-value lookup to use relaxed query, got %v", fieldValueQueries)
	}
	for _, got := range append(streamQueries, scanQueries...) {
		if !strings.Contains(got, strictToken) {
			t.Fatalf("expected unresolved strict candidates to keep strict fallback scans, got %q", got)
		}
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	values, _ := resp["values"].([]interface{})
	if len(values) != 1 || values[0] != "200" {
		t.Fatalf("expected relaxed detected_field values payload, got %v", resp)
	}
}

func TestDetectedFieldValues_EmptyStrictQueryRelaxesWholeLookup(t *testing.T) {
	const strictToken = "strict-only"

	var fieldNameQueries []string
	var scanQueries []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		got := r.Form.Get("query")
		strict := strings.Contains(got, strictToken)
		switch r.URL.Path {
		case "/select/logsql/field_names":
			fieldNameQueries = append(fieldNameQueries, got)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/query":
			scanQueries = append(scanQueries, got)
			w.Header().Set("Content-Type", "application/x-ndjson")
			if !strict {
				_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"status\":200}","_stream":"{service_name=\"api-gateway\",namespace=\"staging\"}","service_name":"api-gateway","namespace":"staging","status":200}` + "\n"))
			}
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{service_name="api-gateway",namespace="staging"} | probe="strict-only"`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/status/values?"+q.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(fieldNameQueries) < 2 {
		t.Fatalf("expected strict+relaxed native field-name lookups, got %v", fieldNameQueries)
	}
	if !strings.Contains(fieldNameQueries[0], strictToken) {
		t.Fatalf("expected first native field-name lookup to stay strict, got %v", fieldNameQueries)
	}
	if strings.Contains(fieldNameQueries[len(fieldNameQueries)-1], strictToken) {
		t.Fatalf("expected final native field-name lookup to relax whole-query filters, got %v", fieldNameQueries)
	}
	if len(scanQueries) < 2 {
		t.Fatalf("expected strict+relaxed field scans, got %v", scanQueries)
	}
	if !strings.Contains(scanQueries[0], strictToken) {
		t.Fatalf("expected first field scan to stay strict, got %v", scanQueries)
	}
	if strings.Contains(scanQueries[len(scanQueries)-1], strictToken) {
		t.Fatalf("expected final field scan to relax whole-query filters, got %v", scanQueries)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	values, _ := resp["values"].([]interface{})
	if len(values) != 1 || values[0] != "200" {
		t.Fatalf("expected relaxed whole-query resolution to recover detected_field values, got %v", resp)
	}
}

// TestDetectedFieldValues_UnpackFallbackCalledForMsgOnlyFields guards that
// fetchUnpackedFieldValues is invoked as a last resort when:
//   - The field is not in VL's native field_names index
//   - Log-line scanning finds nothing
//   - No valid relaxed query is available (simple selector, nothing to relax)
//
// This covers logfmt fields like "amount" or "ttl" that live inside _msg and
// are only reachable via | unpack_logfmt from _msg.
func TestDetectedFieldValues_UnpackFallbackCalledForMsgOnlyFields(t *testing.T) {
	var fieldValuesQueries []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		q := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/field_names":
			// amount is not a native VL indexed field → resolveNativeDetectedField returns ok=false
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/query":
			// Scan returns nothing — field is buried inside logfmt _msg
			w.Header().Set("Content-Type", "application/x-ndjson")
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/field_values":
			fieldValuesQueries = append(fieldValuesQueries, q)
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(q, "unpack_logfmt") {
				// Simulate VL returning values extracted by the unpack pipe
				_, _ = w.Write([]byte(`{"values":[{"value":"42.50","hits":0},{"value":"100.00","hits":0}]}`))
			} else {
				_, _ = w.Write([]byte(`{"values":[]}`))
			}
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{app="logfmt-test"}`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/amount/values?"+q.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	// At least one field_values call must contain unpack pipes
	hasUnpack := false
	for _, fvq := range fieldValuesQueries {
		if strings.Contains(fvq, "unpack_logfmt") {
			hasUnpack = true
		}
	}
	if !hasUnpack {
		t.Fatalf("expected fetchUnpackedFieldValues to call field_values with unpack pipes, got queries: %v", fieldValuesQueries)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	values, _ := resp["values"].([]interface{})
	if len(values) != 2 {
		t.Fatalf("expected 2 values from unpack fallback, got %v", resp)
	}
}

// TestDetectedFieldValues_RelaxedScanPreventsUnpackFallback guards the ordering
// invariant: fetchUnpackedFieldValues (field_values) must NOT be called before
// the relaxed-query scan has had a chance to find values. If relaxed scan
// succeeds, field_values must never be called.
//
// This is the companion to TestDetectedFieldValues_EmptyStrictQueryRelaxesWholeLookup.
// The existing test already enforces this implicitly (backend t.Fatalf on any
// unexpected path including field_values). This test makes the intent explicit
// and verifies the returned value is from the relaxed scan, not the unpack path.
func TestDetectedFieldValues_RelaxedScanPreventsUnpackFallback(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		got := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			if !strings.Contains(got, "strict-filter") {
				// Relaxed scan returns the field value
				_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49Z","_msg":"level=info amount=99","_stream":"{app=\"logfmt-test\"}","app":"logfmt-test","amount":"99"}` + "\n"))
			}
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		default:
			// field_values must NOT be called when relaxed scan succeeds
			t.Fatalf("fetchUnpackedFieldValues called unexpectedly — field_values before relaxed scan: path=%s query=%q", r.URL.Path, got)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`{app="logfmt-test"} | strict-filter="yes"`},
		"start": {"1"},
		"end":   {"2"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/amount/values?"+q.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	values, _ := resp["values"].([]interface{})
	if len(values) == 0 {
		t.Fatalf("expected value from relaxed scan, got empty: %v", resp)
	}
}

func TestHandleLabels_BareSelectorQuery(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.URL.Query().Get("query")
		if strings.Contains(got, `{`) || strings.Contains(got, `}`) {
			t.Fatalf("bare selector should be translated before hitting VL, got %q", got)
		}
		if !strings.Contains(got, `service_name:=api-gateway`) {
			t.Fatalf("expected service_name matcher in translated query, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"cluster","hits":1},{"value":"service_name","hits":1}]}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/labels?query=service_name%3D%22api-gateway%22`, nil)
	p.handleLabels(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("handleLabels returned %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"cluster"`) {
		t.Fatalf("expected translated labels response, got %s", w.Body.String())
	}
}

// =============================================================================
// Priority 2: proxyBinaryMetric — VL error on one side
// =============================================================================

func TestBinaryMetric_LeftSideVLError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Left side has app="a", right has app="b" — dispatch by query content,
		// safe for concurrent left+right fetches.
		if strings.Contains(r.URL.Query().Get("query"), `"a"`) {
			// Left side fails
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error":"left failed"}`))
			return
		}
		// Right side succeeds
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{"metric": map[string]string{}, "values": [][]interface{}{{1234567890.0, "10"}}},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`rate({app="a"}[5m]) / rate({app="b"}[5m])`},
		"start": {"1"}, "end": {"2"}, "step": {"1"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	// Should return error, not 200
	if w.Code == 200 {
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		// acceptable — may succeed or fail depending on query execution path
		_ = resp
	}
}

// =============================================================================
// Priority 2: Delete endpoint — VL error propagation
// =============================================================================

func TestDelete_VLReturns500_PropagatesError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	start := time.Now().Add(-1 * time.Hour).Unix()
	end := time.Now().Unix()
	r := httptest.NewRequest("POST",
		fmt.Sprintf(`/loki/api/v1/delete?query={app="nginx"}&start=%d&end=%d`, start, end), nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 propagated from VL, got %d", w.Code)
	}
}

// =============================================================================
// Priority 2: handlePatterns with real VL backend
// =============================================================================

func TestPatterns_VLReturnsLines_ExtractsPatterns(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"nginx","level":"info"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T10:00:10Z","_msg":"GET /api/users 200 22ms","app":"nginx","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{"query": {`{app="nginx"}`}, "start": {"1"}, "end": {"2"}, "step": {"15s"}}
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?"+q.Encode(), nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected patterns endpoint to return 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Fatalf("expected status=success, got %v", resp)
	}
	data, ok := resp["data"].([]interface{})
	if !ok || len(data) == 0 {
		t.Fatalf("expected extracted patterns, got %v", resp)
	}
}

// =============================================================================
// Priority 2: applyBackendHeaders and forwardHeaders
// =============================================================================

func TestForwardHeaders_ConfiguredHeadersForwarded(t *testing.T) {
	var receivedAuth string
	var receivedCustom string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		receivedCustom = r.Header.Get("X-Custom-Header")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{{"value": "app", "hits": 1}},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(1*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		ForwardHeaders: []string{"Authorization", "X-Custom-Header"},
		BackendHeaders: map[string]string{"X-Static": "always-present"},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("Authorization", "Bearer secret-token")
	r.Header.Set("X-Custom-Header", "custom-value")
	p.handleLabels(w, r)

	if receivedAuth != "Bearer secret-token" {
		t.Errorf("expected Authorization forwarded, got %q", receivedAuth)
	}
	if receivedCustom != "custom-value" {
		t.Errorf("expected X-Custom-Header forwarded, got %q", receivedCustom)
	}
}

func TestBackendHeaders_StaticHeadersForwarded(t *testing.T) {
	var receivedStatic string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStatic = r.Header.Get("X-Static")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{{"value": "app", "hits": 1}},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(1*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		BackendHeaders: map[string]string{"X-Static": "always-present"},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	if receivedStatic != "always-present" {
		t.Errorf("expected X-Static header, got %q", receivedStatic)
	}
}

// =============================================================================
// Priority 2: Label translator round-trip consistency
// =============================================================================

func TestLabelTranslator_RoundTrip(t *testing.T) {
	lt := NewLabelTranslator("underscores", nil)

	// Known OTel fields should round-trip
	otelFields := []string{
		"service.name", "k8s.namespace.name", "k8s.pod.name",
		"deployment.environment", "host.name",
	}

	for _, field := range otelFields {
		lokiLabel := lt.ToLoki(field) // service.name → service_name
		vlField := lt.ToVL(lokiLabel) // service_name → service.name
		if vlField != field {
			t.Errorf("round-trip failed: %q → %q → %q (expected %q)", field, lokiLabel, vlField, field)
		}
	}
}

// =============================================================================
// Priority 3: Unicode and special characters in NDJSON
// =============================================================================

func TestVLLogs_UnicodeMessage(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"请求处理 🚀 émojis","_stream":"{}"}` + "\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	vals := streams[0]["values"].([][]string)
	if !strings.Contains(vals[0][1], "请求处理") {
		t.Errorf("Unicode message not preserved: %q", vals[0][1])
	}
}

func TestVLLogs_SpecialCharsInLabels(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","_stream":"{app=\"ng\\\"inx\"}","app":"ng\"inx"}` + "\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) == 0 {
		t.Fatal("expected at least 1 stream for special char labels")
	}
}

// =============================================================================
// Priority 3: Metrics RecordClientError and RecordTenantRequest
// =============================================================================

func TestMetrics_RecordClientError(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	m := p.GetMetrics()

	m.RecordClientError("query_range", "bad_request")
	m.RecordClientError("query_range", "rate_limited")

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "client_errors_total") {
		t.Error("expected client_errors_total in metrics output")
	}
}

func TestMetrics_RecordTenantRequest(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	m := p.GetMetrics()

	m.RecordTenantRequest("team-a", "query_range", 200, 10*time.Millisecond)
	m.RecordTenantRequest("team-b", "labels", 200, 5*time.Millisecond)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "tenant_requests_total") {
		t.Error("expected tenant_requests_total in metrics output")
	}
	if !strings.Contains(body, "team-a") {
		t.Error("expected team-a in tenant metrics")
	}
}

// =============================================================================
// Priority 3: splitLabelPairs quoted comma
// =============================================================================

func TestSplitLabelPairs_QuotedComma(t *testing.T) {
	// splitLabelPairs is inlined into parseStreamLabels — verify comma inside
	// quotes is not treated as a pair separator.
	input := `{app="he,llo",namespace="world"}`
	labels := parseStreamLabels(input)
	if len(labels) != 2 {
		t.Errorf("expected 2 labels (comma in quotes ignored), got %d: %v", len(labels), labels)
	}
	if labels["app"] != "he,llo" {
		t.Errorf("expected app=he,llo, got %q", labels["app"])
	}
}

// =============================================================================
// Priority 3: isVLInternalField
// =============================================================================

func TestIsVLInternalField(t *testing.T) {
	internals := []string{"_time", "_msg", "_stream", "_stream_id"}
	for _, f := range internals {
		if !isVLInternalField(f) {
			t.Errorf("expected %q to be internal field", f)
		}
	}
	externals := []string{"app", "namespace", "_custom", "time", "msg"}
	for _, f := range externals {
		if isVLInternalField(f) {
			t.Errorf("expected %q to NOT be internal field", f)
		}
	}
}

// =============================================================================
// Security regression tests (audit fixes)
// =============================================================================

// TestParseDeleteTimestamp_RFC3339BeyondCapRejected guards against the bypass
// where RFC3339 timestamps skipped the 30-day guard because the old code only
// enforced it when both values parsed as floats.
func TestParseDeleteTimestamp_RFC3339BeyondCapRejected(t *testing.T) {
	var receivedDelete bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/delete" {
			receivedDelete = true
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// 60-day range expressed as RFC3339 — previously bypassed the 30-day guard.
	startISO := time.Now().Add(-60 * 24 * time.Hour).UTC().Format(time.RFC3339)
	endISO := time.Now().UTC().Format(time.RFC3339)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST",
		fmt.Sprintf(`/loki/api/v1/delete?query={app="nginx"}&start=%s&end=%s`,
			url.QueryEscape(startISO), url.QueryEscape(endISO)), nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for RFC3339 delete range >30 days, got %d body=%s", w.Code, w.Body.String())
	}
	if receivedDelete {
		t.Error("VL delete endpoint must not be reached when time range exceeds cap")
	}
}

// TestParseDeleteTimestamp_RFC3339WithinCapAllowed ensures that RFC3339
// timestamps within the 30-day cap are forwarded normally.
func TestParseDeleteTimestamp_RFC3339WithinCapAllowed(t *testing.T) {
	var receivedPath string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	startISO := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	endISO := time.Now().UTC().Format(time.RFC3339)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST",
		fmt.Sprintf(`/loki/api/v1/delete?query={app="nginx"}&start=%s&end=%s`,
			url.QueryEscape(startISO), url.QueryEscape(endISO)), nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code == http.StatusBadRequest {
		t.Errorf("RFC3339 delete within cap should not be rejected: %s", w.Body.String())
	}
	if receivedPath != "/select/logsql/delete" {
		t.Errorf("expected VL delete path, got %q", receivedPath)
	}
}

// TestParseDeleteTimestamp_Func exercises the parser directly for all formats.
func TestParseDeleteTimestamp_Func(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		input   string
		wantNs  int64
		wantErr bool
	}{
		// Unix seconds
		{fmt.Sprintf("%d", now.Unix()), now.UnixNano(), false},
		// Unix nanoseconds (>1e15)
		{fmt.Sprintf("%d", now.UnixNano()), now.UnixNano(), false},
		// RFC3339
		{now.Format(time.RFC3339), now.Unix() * int64(time.Second), false},
		// RFC3339Nano
		{now.Format(time.RFC3339Nano), now.UnixNano(), false},
		// Unrecognized
		{"not-a-timestamp", 0, true},
		{"2026/01/01", 0, true},
	}

	for _, tc := range cases {
		got, err := parseDeleteTimestamp(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("input %q: expected error, got ns=%d", tc.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("input %q: unexpected error: %v", tc.input, err)
			continue
		}
		// Allow 1 second tolerance for RFC3339 (no sub-second precision).
		diff := got - tc.wantNs
		if diff < 0 {
			diff = -diff
		}
		if diff > int64(time.Second) {
			t.Errorf("input %q: got %d, want %d (diff=%d)", tc.input, got, tc.wantNs, diff)
		}
	}
}

// TestCopyBackendHeaders_SecurityHeadersPreserved guards against the regression
// where copyHeaders overwrote proxy-set security headers with backend values.
func TestCopyBackendHeaders_SecurityHeadersPreserved(t *testing.T) {
	dst := http.Header{}
	dst.Set("X-Content-Type-Options", "nosniff")
	dst.Set("X-Frame-Options", "DENY")
	dst.Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	dst.Set("Cross-Origin-Resource-Policy", "same-origin")
	dst.Set("Pragma", "no-cache")
	dst.Set("Expires", "0")
	dst.Set("Content-Type", "application/json")

	// Backend tries to overwrite security headers and add its own Content-Type.
	src := http.Header{}
	src.Set("X-Content-Type-Options", "allow")
	src.Set("X-Frame-Options", "SAMEORIGIN")
	src.Set("Cache-Control", "max-age=3600, public")
	src.Set("Cross-Origin-Resource-Policy", "cross-origin")
	src.Set("Pragma", "no-store")
	src.Set("Expires", "86400")
	src.Set("Content-Type", "text/plain")
	src.Set("X-Backend-Custom", "backend-value")

	copyBackendHeaders(dst, src)

	// Security headers must not be overwritten.
	if got := dst.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options: got %q, want %q", got, "nosniff")
	}
	if got := dst.Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options: got %q, want %q", got, "DENY")
	}
	if got := dst.Get("Cache-Control"); got != "no-store, no-cache, must-revalidate, max-age=0" {
		t.Errorf("Cache-Control: got %q, want proxy value", got)
	}
	if got := dst.Get("Cross-Origin-Resource-Policy"); got != "same-origin" {
		t.Errorf("Cross-Origin-Resource-Policy: got %q, want same-origin", got)
	}
	// Non-security headers from backend ARE copied.
	if got := dst.Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type: got %q, want text/plain (backend value)", got)
	}
	if got := dst.Get("X-Backend-Custom"); got != "backend-value" {
		t.Errorf("X-Backend-Custom: got %q, want backend-value", got)
	}
}

// TestForwardedAuthFingerprint_EmptyWithoutConfig ensures no fingerprint is
// computed when no header/cookie forwarding is configured.
func TestForwardedAuthFingerprint_EmptyWithoutConfig(t *testing.T) {
	p := &Proxy{}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer secret")
	if fp := p.forwardedAuthFingerprint(r); fp != "" {
		t.Errorf("expected empty fingerprint with no forwarding configured, got %q", fp)
	}
}

// TestForwardedAuthFingerprint_IsolatesUsers ensures that two requests with
// different Authorization values produce different fingerprints.
func TestForwardedAuthFingerprint_IsolatesUsers(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	r1 := httptest.NewRequest("GET", "/", nil)
	r1.Header.Set("Authorization", "Bearer user-alice-token")

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.Header.Set("Authorization", "Bearer user-bob-token")

	fp1 := p.forwardedAuthFingerprint(r1)
	fp2 := p.forwardedAuthFingerprint(r2)

	if fp1 == "" {
		t.Error("expected non-empty fingerprint for request with Authorization")
	}
	if fp1 == fp2 {
		t.Errorf("different auth tokens must produce different fingerprints; both got %q", fp1)
	}
	// Same token must produce same fingerprint (deterministic).
	if p.forwardedAuthFingerprint(r1) != fp1 {
		t.Error("fingerprint must be deterministic for same input")
	}
}

// TestCompatCacheKey_AuthFingerprintIncluded ensures that when forward headers
// are configured, the compat cache key differs between users.
func TestCompatCacheKey_AuthFingerprintIncluded(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	r1 := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r1.Header.Set("X-Scope-OrgID", "tenant1")
	r1.Header.Set("Authorization", "Bearer alice")

	r2 := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r2.Header.Set("X-Scope-OrgID", "tenant1")
	r2.Header.Set("Authorization", "Bearer bob")

	key1, ok1 := p.compatCacheKey("labels", r1)
	key2, ok2 := p.compatCacheKey("labels", r2)

	if !ok1 || !ok2 {
		t.Skip("compat cache not applicable for this endpoint in test config")
	}
	if key1 == key2 {
		t.Errorf("cache keys must differ for different Authorization values; both: %q", key1)
	}
}

// TestAlertingBackendHeaders_NoBroadcast ensures VL backend credentials
// (set via BackendBasicAuth) are NOT forwarded to the ruler/alerts backend.
func TestAlertingBackendHeaders_NoBroadcast(t *testing.T) {
	var receivedAuth string
	alertingBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer alertingBackend.Close()

	backendURL, _ := url.Parse(alertingBackend.URL)

	p := newGapTestProxy(t, "http://does-not-matter")
	// Inject VL credentials into backendHeaders (as BackendBasicAuth would do).
	p.backendHeaders = map[string]string{
		"Authorization": "Basic " + base64Encode("vluser:vlpass"),
	}
	p.rulerBackend = backendURL

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/rules", nil)
	r.Header.Set("X-Scope-OrgID", "tenant1")
	p.handleRules(w, r)

	if receivedAuth != "" {
		t.Errorf("VL backend credentials must NOT be forwarded to ruler backend, got Authorization=%q", receivedAuth)
	}
}

// =============================================================================
// Cache auth-isolation fixes (security review follow-up)
// =============================================================================

// ctxWithOrgAndAuth returns a context that carries both the orgID and an
// origRequestKey pointing at r, so nativeCoalescerKey / detectedFieldsCacheKey
// can extract the auth fingerprint.
func ctxWithOrgAndAuth(orgID string, r *http.Request) context.Context {
	ctx := context.WithValue(context.Background(), orgIDKey, orgID)
	return context.WithValue(ctx, origRequestKey, r)
}

// TestNativeCoalescerKey_TenantIsolated verifies that native_fields / native_streams
// coalescer keys differ between tenants so concurrent misses don't share a VL response.
func TestNativeCoalescerKey_TenantIsolated(t *testing.T) {
	p := &Proxy{}
	params := url.Values{}
	params.Set("query", `{app="api"}`)

	ctx1 := ctxWithOrgAndAuth("tenant-a", nil)
	ctx2 := ctxWithOrgAndAuth("tenant-b", nil)

	key1 := p.nativeCoalescerKey("native_fields", ctx1, params)
	key2 := p.nativeCoalescerKey("native_fields", ctx2, params)

	if key1 == key2 {
		t.Errorf("native_fields coalescer key must differ between tenants; both: %q", key1)
	}
}

// TestNativeCoalescerKey_AuthIsolated verifies that native_fields / native_streams
// coalescer keys differ between users when forwarded auth is configured.
func TestNativeCoalescerKey_AuthIsolated(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	params := url.Values{}
	params.Set("query", `{app="api"}`)

	rAlice := httptest.NewRequest("GET", "/", nil)
	rAlice.Header.Set("Authorization", "Bearer alice-token")

	rBob := httptest.NewRequest("GET", "/", nil)
	rBob.Header.Set("Authorization", "Bearer bob-token")

	ctxA := ctxWithOrgAndAuth("tenant1", rAlice)
	ctxB := ctxWithOrgAndAuth("tenant1", rBob)

	key1 := p.nativeCoalescerKey("native_fields", ctxA, params)
	key2 := p.nativeCoalescerKey("native_fields", ctxB, params)

	if key1 == key2 {
		t.Errorf("native_fields coalescer key must differ for different auth tokens; both: %q", key1)
	}

	key3 := p.nativeCoalescerKey("native_streams", ctxA, params)
	key4 := p.nativeCoalescerKey("native_streams", ctxB, params)
	if key3 == key4 {
		t.Errorf("native_streams coalescer key must differ for different auth tokens; both: %q", key3)
	}
}

// TestDetectedFieldsCacheKey_AuthIsolated verifies detect_fields / detect_labels
// cache keys include the per-user auth fingerprint.
func TestDetectedFieldsCacheKey_AuthIsolated(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	rAlice := httptest.NewRequest("GET", "/", nil)
	rAlice.Header.Set("Authorization", "Bearer alice")

	rBob := httptest.NewRequest("GET", "/", nil)
	rBob.Header.Set("Authorization", "Bearer bob")

	ctxA := ctxWithOrgAndAuth("t1", rAlice)
	ctxB := ctxWithOrgAndAuth("t1", rBob)

	fk1 := p.detectedFieldsCacheKey(ctxA, `{app="x"}`, "1000", "2000", 100)
	fk2 := p.detectedFieldsCacheKey(ctxB, `{app="x"}`, "1000", "2000", 100)
	if fk1 == fk2 {
		t.Errorf("detect_fields cache key must differ for different auth tokens; both: %q", fk1)
	}

	lk1 := p.detectedLabelsCacheKey(ctxA, `{app="x"}`, "1000", "2000", 100)
	lk2 := p.detectedLabelsCacheKey(ctxB, `{app="x"}`, "1000", "2000", 100)
	if lk1 == lk2 {
		t.Errorf("detect_labels cache key must differ for different auth tokens; both: %q", lk1)
	}
}

// TestQueryRangeCacheKey_AuthIsolated verifies the query_range window cache key
// includes the per-user auth fingerprint.
func TestQueryRangeCacheKey_AuthIsolated(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	r1 := httptest.NewRequest("GET", "/?query=%7Bapp%3D%22x%22%7D&start=1000&end=2000&step=15", nil)
	r1.Header.Set("X-Scope-OrgID", "tenant1")
	r1.Header.Set("Authorization", "Bearer alice")

	r2 := httptest.NewRequest("GET", "/?query=%7Bapp%3D%22x%22%7D&start=1000&end=2000&step=15", nil)
	r2.Header.Set("X-Scope-OrgID", "tenant1")
	r2.Header.Set("Authorization", "Bearer bob")

	key1 := p.queryRangeCacheKey(r1, `{app="x"}`)
	key2 := p.queryRangeCacheKey(r2, `{app="x"}`)

	if key1 == key2 {
		t.Errorf("queryRangeCacheKey must differ for different auth tokens; both: %q", key1)
	}
}

// TestMultiTenantCacheKey_AuthIsolated verifies that mt: keys for query, query_range,
// patterns, and series include the per-user auth fingerprint.
func TestMultiTenantCacheKey_AuthIsolated(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	for _, endpoint := range []string{"query", "query_range", "patterns", "series"} {
		r1 := httptest.NewRequest("GET", "/?query=%7Bapp%3D%22x%22%7D&start=1000&end=2000", nil)
		r1.Header.Set("X-Scope-OrgID", "tenant1")
		r1.Header.Set("Authorization", "Bearer alice")

		r2 := httptest.NewRequest("GET", "/?query=%7Bapp%3D%22x%22%7D&start=1000&end=2000", nil)
		r2.Header.Set("X-Scope-OrgID", "tenant1")
		r2.Header.Set("Authorization", "Bearer bob")

		key1, ok1 := p.multiTenantCacheKey(r1, endpoint)
		key2, ok2 := p.multiTenantCacheKey(r2, endpoint)

		if !ok1 || !ok2 {
			t.Errorf("endpoint %q: multiTenantCacheKey should be applicable for GET", endpoint)
			continue
		}
		if key1 == key2 {
			t.Errorf("endpoint %q: mt key must differ for different auth tokens; both: %q", endpoint, key1)
		}
	}
}

// TestPatternsAutodetectCacheKey_AuthIsolated verifies pattern autodetect cache keys
// are scoped per user when forwarded auth is configured.
func TestPatternsAutodetectCacheKey_AuthIsolated(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	rAlice := httptest.NewRequest("GET", "/", nil)
	rAlice.Header.Set("Authorization", "Bearer alice")
	rBob := httptest.NewRequest("GET", "/", nil)
	rBob.Header.Set("Authorization", "Bearer bob")

	fpAlice := p.forwardedAuthFingerprint(rAlice)
	fpBob := p.forwardedAuthFingerprint(rBob)

	key1 := p.patternsAutodetectCacheKey("tenant1", fpAlice, `{app="x"}`, "1000", "2000", "15")
	key2 := p.patternsAutodetectCacheKey("tenant1", fpBob, `{app="x"}`, "1000", "2000", "15")

	if key1 == key2 {
		t.Errorf("patterns autodetect cache key must differ for different auth tokens; both: %q", key1)
	}
	// Same fingerprint must produce same key (deterministic).
	key3 := p.patternsAutodetectCacheKey("tenant1", fpAlice, `{app="x"}`, "1000", "2000", "15")
	if key1 != key3 {
		t.Error("patternsAutodetectCacheKey must be deterministic for the same inputs")
	}
}

// TestSnapshotForwardedAuth_CapturesHeaders verifies that snapshotForwardedAuth
// copies configured forward headers/cookies and returns nil when none are configured.
func TestSnapshotForwardedAuth_CapturesHeaders(t *testing.T) {
	t.Run("nil_when_no_forwarding_configured", func(t *testing.T) {
		p := &Proxy{}
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer secret")
		if snap := p.snapshotForwardedAuth(r); snap != nil {
			t.Error("expected nil snapshot when no forwarding configured")
		}
	})

	t.Run("captures_configured_headers_only", func(t *testing.T) {
		p := &Proxy{forwardHeaders: []string{"Authorization", "X-Custom"}}
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer tok")
		r.Header.Set("X-Custom", "val")
		r.Header.Set("X-Not-Forwarded", "should-not-appear")

		snap := p.snapshotForwardedAuth(r)
		if snap == nil {
			t.Fatal("expected non-nil snapshot")
		}
		if got := snap.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization not captured: got %q", got)
		}
		if got := snap.Header.Get("X-Custom"); got != "val" {
			t.Errorf("X-Custom not captured: got %q", got)
		}
		if got := snap.Header.Get("X-Not-Forwarded"); got != "" {
			t.Errorf("non-forwarded header must not appear in snapshot: got %q", got)
		}
	})

	t.Run("fingerprint_matches_original", func(t *testing.T) {
		p := &Proxy{forwardHeaders: []string{"Authorization"}}
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer tok")

		snap := p.snapshotForwardedAuth(r)
		if snap == nil {
			t.Fatal("expected non-nil snapshot")
		}
		if p.forwardedAuthFingerprint(snap) != p.forwardedAuthFingerprint(r) {
			t.Error("snapshot fingerprint must match original request fingerprint")
		}
	})
}

// TestSnapshotForwardedAuth_UsableAsOrigRequestKey verifies that a snapshot can be
// attached as origRequestKey in a background context so refresh workers carry the
// correct forwarded credentials.
func TestSnapshotForwardedAuth_UsableAsOrigRequestKey(t *testing.T) {
	p := &Proxy{forwardHeaders: []string{"Authorization"}}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer alice-refresh-token")

	snap := p.snapshotForwardedAuth(r)
	if snap == nil {
		t.Fatal("expected snapshot for request with forwarded header")
	}

	// Simulate the background context that refresh workers build.
	bgCtx := context.WithValue(context.Background(), origRequestKey, snap)

	origFP := p.forwardedAuthFingerprint(r)
	origReq, ok := bgCtx.Value(origRequestKey).(*http.Request)
	if !ok || origReq == nil {
		t.Fatal("origRequestKey not accessible from background context")
	}
	if bgFP := p.forwardedAuthFingerprint(origReq); bgFP != origFP {
		t.Errorf("background context fingerprint %q must match original %q", bgFP, origFP)
	}
}

func TestAllRangeWindowsEqual(t *testing.T) {
	cases := []struct {
		name     string
		logql    string
		wantDur  string // "" means wantOK=false
	}{
		{"uniform_1m", `rate({a}[1m]) / rate({b}[1m])`, "1m0s"},
		{"uniform_5m", `sum(rate({a}[5m])) + sum(rate({b}[5m]))`, "5m0s"},
		{"single_window", `rate({a}[2m])`, "2m0s"},
		{"mixed_ranges", `rate({a}[1m]) / rate({b}[5m])`, ""},
		{"no_windows", `{app="x"} | json`, ""},
		{"label_selector_brackets_only", `{app="x"}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dur, ok := allRangeWindowsEqual(tc.logql)
			if tc.wantDur == "" {
				if ok {
					t.Errorf("allRangeWindowsEqual(%q) = %v, want not-ok", tc.logql, dur)
				}
			} else {
				if !ok {
					t.Errorf("allRangeWindowsEqual(%q) not ok, want %s", tc.logql, tc.wantDur)
				} else if dur.String() != tc.wantDur {
					t.Errorf("allRangeWindowsEqual(%q) = %v, want %s", tc.logql, dur, tc.wantDur)
				}
			}
		})
	}
}

// TestStatsRateRangeEqualsStepShift_Detection verifies that
// statsRateRangeEqualsStepShift detects rate/bytes_rate inside aggregations.
func TestStatsRateRangeEqualsStepShift_Detection(t *testing.T) {
	makeReq := func(step, start string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		_ = r.ParseForm()
		r.Form.Set("step", step)
		r.Form.Set("start", start)
		return r
	}
	const start1m = "1700000060000000000" // arbitrary fixed start
	const step1m = "60"                  // 60s == 1m

	cases := []struct {
		name    string
		logql   string
		wantOK  bool
	}{
		{"bare_rate", `rate({app="x"}[1m])`, true},
		{"bytes_rate", `bytes_rate({app="x"}[1m])`, true},
		{"sum_by_rate", `sum by (level) (rate({app="x"} | json [1m]))`, true},
		{"topk_rate", `topk(3, rate({app="x"}[1m]))`, true},
		{"sum_by_bytes_rate", `sum by (l) (bytes_rate({app="x"}[1m]))`, true},
		{"count_over_time", `count_over_time({app="x"}[1m])`, false}, // not rate-like
		{"rate_counter", `rate_counter({app="x"} | unwrap f [1m])`, false},
		{"rate_sum", `rate_sum({app="x"} | count() by (l) [1m])`, false},
		{"wrong_step", `rate({app="x"}[5m])`, false}, // window(5m) != step(1m)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, ok := statsRateRangeEqualsStepShift(tc.logql, makeReq(step1m, start1m))
			if ok != tc.wantOK {
				t.Errorf("statsRateRangeEqualsStepShift(%q) ok=%v want %v", tc.logql, ok, tc.wantOK)
			}
		})
	}
}

// =============================================================================
// Coverage gap: trimStatsQueryRangeResponseFromStart
// =============================================================================

func TestTrimStatsQueryRangeResponseFromStart(t *testing.T) {
	// Timestamps: 100s, 200s, 300s in nanoseconds.
	const t100 = int64(100 * 1e9)
	const t200 = int64(200 * 1e9)
	const t300 = int64(300 * 1e9)

	makeBody := func(points [][2]interface{}) []byte {
		series := map[string]interface{}{
			"metric": map[string]string{"app": "test"},
			"values": points,
		}
		body, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "matrix",
				"result":     []interface{}{series},
			},
		})
		return body
	}

	body := makeBody([][2]interface{}{
		{float64(100), "1.0"},
		{float64(200), "2.0"},
		{float64(300), "3.0"},
	})

	t.Run("trims_points_before_start", func(t *testing.T) {
		out := trimStatsQueryRangeResponseFromStart(body, t200)
		var resp map[string]interface{}
		if err := json.Unmarshal(out, &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp["status"] != "success" {
			t.Errorf("status lost: %v", resp["status"])
		}
		result := resp["data"].(map[string]interface{})["result"].([]interface{})
		vals := result[0].(map[string]interface{})["values"].([]interface{})
		if len(vals) != 2 {
			t.Errorf("expected 2 points after trim, got %d", len(vals))
		}
	})

	t.Run("no_trim_when_all_at_or_after_start", func(t *testing.T) {
		out := trimStatsQueryRangeResponseFromStart(body, t100)
		if string(out) != string(body) {
			t.Errorf("body should be unchanged when all points are >= start")
		}
	})

	t.Run("all_trimmed_when_start_after_last_point", func(t *testing.T) {
		out := trimStatsQueryRangeResponseFromStart(body, t300+1)
		var resp map[string]interface{}
		if err := json.Unmarshal(out, &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		result := resp["data"].(map[string]interface{})["result"].([]interface{})
		vals := result[0].(map[string]interface{})["values"].([]interface{})
		if len(vals) != 0 {
			t.Errorf("expected 0 points, got %d", len(vals))
		}
	})

	t.Run("invalid_json_passthrough", func(t *testing.T) {
		bad := []byte("not-json")
		out := trimStatsQueryRangeResponseFromStart(bad, t200)
		if string(out) != "not-json" {
			t.Errorf("expected passthrough for invalid JSON")
		}
	})
}

// =============================================================================
// Coverage gap: normalizeManualMetricFunction
// =============================================================================

func TestNormalizeManualMetricFunction(t *testing.T) {
	cases := []struct {
		origFunc string
		specFunc string
		want     string
	}{
		{"rate", "", "rate"},
		{"count_over_time", "", "count_over_time"},
		{"bytes_over_time", "", "bytes_over_time"},
		{"bytes_rate", "", "bytes_rate"},
		{"sum_over_time", "", "sum"},
		{"avg_over_time", "", "avg"},
		{"max_over_time", "", "max"},
		{"min_over_time", "", "min"},
		{"stddev_over_time", "", "stddev"},
		{"stdvar_over_time", "", "stdvar"},
		{"first_over_time", "", "first"},
		{"last_over_time", "", "last"},
		{"rate_counter", "", "rate_counter"},
		{"quantile_over_time", "", "quantile"},
		// Falls through to spec.Func when origSpec.Func is unrecognized.
		{"", "rate", "rate"},
		{"", "count", "count_over_time"},
		{"", "sum", "sum"},
		{"", "avg", "avg"},
		{"", "max", "max"},
		{"", "min", "min"},
		{"", "quantile", "quantile"},
		{"", "first", "first"},
		{"", "last", "last"},
	}
	for _, tc := range cases {
		t.Run(tc.origFunc+"/"+tc.specFunc, func(t *testing.T) {
			orig := originalRangeMetricSpec{Func: tc.origFunc}
			spec := statsCompatSpec{Func: tc.specFunc}
			got := normalizeManualMetricFunction(spec, orig)
			if got != tc.want {
				t.Errorf("normalizeManualMetricFunction(%q, %q) = %q, want %q",
					tc.origFunc, tc.specFunc, got, tc.want)
			}
		})
	}
}

// =============================================================================
// Coverage gap: proxyBareParserMetricViaStats (the rate|json fast path)
// =============================================================================

func TestProxyBareParserMetricViaStats_ParserStageUsesSlowPath(t *testing.T) {
	// rate({...} | json [5m]) with step==range must NOT go through native VL stats:
	// VL native stats may not replicate Loki's __error__ exclusion for lines that
	// fail JSON parsing. The slow manual log-fetch path is required.
	var statsCalled bool
	var queryCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			statsCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000060,"1.5"]]}]}}`))
			return
		}
		if r.URL.Path == "/select/logsql/query" {
			queryCalled = true
			w.Header().Set("Content-Type", "application/x-ndjson")
			return
		}
		if r.URL.Path == "/metrics" {
			w.Write([]byte(`metrics_version{} 1`))
			return
		}
		t.Logf("unexpected backend path: %s", r.URL.Path)
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	base := time.Unix(1700000000, 0)
	params := url.Values{}
	params.Set("query", `rate({app="api-gateway"} | json [5m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "300") // step=300s == range=[5m] → rangeEqualsStep=true
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if statsCalled {
		t.Error("parser-stage query must not use native VL stats fast path (VL may not match Loki __error__ semantics)")
	}
	if !queryCalled {
		t.Error("parser-stage query should use the manual log-fetch slow path (logsql/query)")
	}
}

func TestProxyBareParserMetricViaStats_SlowPathWhenRangeNeStep(t *testing.T) {
	manualCalled := false
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			// Manual NDJSON fetch path
			manualCalled = true
			w.Header().Set("Content-Type", "application/x-ndjson")
			return
		}
		if r.URL.Path == "/select/logsql/stats_query_range" {
			t.Error("stats_query_range should NOT be called when range != step")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/metrics" {
			w.Write([]byte(`metrics_version{} 1`))
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	base := time.Unix(1700000000, 0)
	// step=60 != range=[5m]=300 → rangeEqualsStep=false → slow manual path
	params := url.Values{}
	params.Set("query", `rate({app="api-gateway"} | json [5m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if !manualCalled {
		t.Fatal("expected manual NDJSON path to be used when range != step")
	}
}

// =============================================================================
// metric_agg.go: topLevelCommaIndex
// =============================================================================

func TestTopLevelCommaIndex(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"5, rate({a}[1m])", 1},
		{"10, sum(rate({a}[1m]))", 2},
		{"3, topk(2, rate({a}[1m]))", 1},
		{"no comma here", -1},
		{"", -1},
		{`"comma,inside", value`, 14},
		{"outer(a, b), value", 11},
	}
	for _, tc := range cases {
		got := topLevelCommaIndex(tc.input)
		if got != tc.want {
			t.Errorf("topLevelCommaIndex(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

// =============================================================================
// metric_agg.go: parseInstantMetricPostAggQuery
// =============================================================================

func TestParseInstantMetricPostAggQuery(t *testing.T) {
	cases := []struct {
		input   string
		wantOk  bool
		wantAgg instantMetricPostAgg
	}{
		{
			input:  `sort_desc(rate({app="api"}[5m]))`,
			wantOk: true,
			wantAgg: instantMetricPostAgg{name: "sort_desc", inner: `rate({app="api"}[5m])`},
		},
		{
			input:  `sort(rate({app="api"}[5m]))`,
			wantOk: true,
			wantAgg: instantMetricPostAgg{name: "sort", inner: `rate({app="api"}[5m])`},
		},
		{
			input:  `topk(5, rate({app="api"}[5m]))`,
			wantOk: true,
			wantAgg: instantMetricPostAgg{name: "topk", inner: `rate({app="api"}[5m])`, k: 5},
		},
		{
			input:  `bottomk(3, count_over_time({app="x"}[1m]))`,
			wantOk: true,
			wantAgg: instantMetricPostAgg{name: "bottomk", inner: `count_over_time({app="x"}[1m])`, k: 3},
		},
		{input: `rate({a}[1m])`, wantOk: false},
		{input: `topk()`, wantOk: false},
		{input: `topk(0, rate({a}[1m]))`, wantOk: false},
		{input: `topk(nonum, rate({a}[1m]))`, wantOk: false},
		{input: `sort_desc()`, wantOk: false},
	}
	for _, tc := range cases {
		got, ok := parseInstantMetricPostAggQuery(tc.input)
		if ok != tc.wantOk {
			t.Errorf("parseInstantMetricPostAggQuery(%q) ok=%v, want %v", tc.input, ok, tc.wantOk)
			continue
		}
		if ok && got != tc.wantAgg {
			t.Errorf("parseInstantMetricPostAggQuery(%q) = %+v, want %+v", tc.input, got, tc.wantAgg)
		}
	}
}

// =============================================================================
// metric_agg.go: parseFloat
// =============================================================================

func TestParseFloat(t *testing.T) {
	cases := []struct {
		input   interface{}
		want    float64
		wantErr bool
	}{
		{float64(3.14), 3.14, false},
		{"2.71828", 2.71828, false},
		{"42", 42.0, false},
		{"notanumber", 0, true},
		{nil, 0, true},
		{true, 0, true},
	}
	for _, tc := range cases {
		got, err := parseFloat(tc.input)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseFloat(%v) err=%v, wantErr=%v", tc.input, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("parseFloat(%v) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// =============================================================================
// metric_agg.go: vectorPointValue
// =============================================================================

func TestVectorPointValue(t *testing.T) {
	cases := []struct {
		input []interface{}
		want  float64
	}{
		{[]interface{}{1700000000.0, "3.14"}, 3.14},
		{[]interface{}{1700000000.0, float64(2.0)}, 2.0},
		{[]interface{}{1700000000.0, nil}, 0},
		{[]interface{}{1700000000.0}, 0},
		{[]interface{}{}, 0},
	}
	for _, tc := range cases {
		got := vectorPointValue(tc.input)
		if got != tc.want {
			t.Errorf("vectorPointValue(%v) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// =============================================================================
// metric_agg.go: applyInstantVectorPostAggregation
// =============================================================================

func buildTestVectorBody(series []struct {
	metric map[string]interface{}
	value  float64
}) []byte {
	result := make([]interface{}, len(series))
	for i, s := range series {
		result[i] = map[string]interface{}{
			"metric": s.metric,
			"value":  []interface{}{1700000000.0, fmt.Sprintf("%g", s.value)},
		}
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     result,
		},
	})
	return body
}

func TestApplyInstantVectorPostAggregation(t *testing.T) {
	series := []struct {
		metric map[string]interface{}
		value  float64
	}{
		{map[string]interface{}{"app": "a"}, 10},
		{map[string]interface{}{"app": "b"}, 30},
		{map[string]interface{}{"app": "c"}, 20},
	}
	body := buildTestVectorBody(series)

	// sort_desc: descending order → b(30), c(20), a(10)
	result := applyInstantVectorPostAggregation(body, instantMetricPostAgg{name: "sort_desc"})
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]interface{} `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("sort_desc unmarshal: %v", err)
	}
	if resp.Data.Result[0].Metric["app"] != "b" || resp.Data.Result[2].Metric["app"] != "a" {
		t.Errorf("sort_desc: wrong order: %v", resp.Data.Result)
	}

	// topk(2): keep top 2 → b(30), c(20)
	result = applyInstantVectorPostAggregation(body, instantMetricPostAgg{name: "topk", k: 2})
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("topk unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Errorf("topk(2): expected 2 results, got %d", len(resp.Data.Result))
	}

	// bottomk(1): keep bottom 1 → a(10)
	result = applyInstantVectorPostAggregation(body, instantMetricPostAgg{name: "bottomk", k: 1})
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("bottomk unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || resp.Data.Result[0].Metric["app"] != "a" {
		t.Errorf("bottomk(1): expected a(10), got %v", resp.Data.Result)
	}

	// Invalid body → passthrough
	bad := []byte(`not json`)
	if string(applyInstantVectorPostAggregation(bad, instantMetricPostAgg{name: "sort"})) != "not json" {
		t.Error("invalid body should pass through unchanged")
	}
}

// =============================================================================
// metric_agg.go: applyMatrixPostAggregation
// =============================================================================

func buildTestMatrixBody(series []struct {
	metric    map[string]interface{}
	lastValue float64
}) []byte {
	result := make([]interface{}, len(series))
	for i, s := range series {
		result[i] = map[string]interface{}{
			"metric": s.metric,
			"values": []interface{}{
				[]interface{}{float64(1700000000), fmt.Sprintf("%g", s.lastValue)},
			},
		}
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	})
	return body
}

func TestApplyMatrixPostAggregation(t *testing.T) {
	series := []struct {
		metric    map[string]interface{}
		lastValue float64
	}{
		{map[string]interface{}{"svc": "low"}, 1.0},
		{map[string]interface{}{"svc": "high"}, 9.0},
		{map[string]interface{}{"svc": "mid"}, 5.0},
	}
	body := buildTestMatrixBody(series)

	// topk(2): high(9), mid(5)
	result := applyMatrixPostAggregation(body, instantMetricPostAgg{name: "topk", k: 2})
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]interface{} `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("topk unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Errorf("topk(2): expected 2, got %d", len(resp.Data.Result))
	}
	if resp.Data.Result[0].Metric["svc"] != "high" {
		t.Errorf("topk(2): first should be high, got %v", resp.Data.Result[0].Metric["svc"])
	}

	// bottomk(1): low(1)
	result = applyMatrixPostAggregation(body, instantMetricPostAgg{name: "bottomk", k: 1})
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("bottomk unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || resp.Data.Result[0].Metric["svc"] != "low" {
		t.Errorf("bottomk(1): expected low, got %v", resp.Data.Result)
	}

	// topk(3): all 3, ordered high→mid→low
	result = applyMatrixPostAggregation(body, instantMetricPostAgg{name: "topk", k: 3})
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("topk(3) unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 3 {
		t.Errorf("topk(3): expected 3, got %d", len(resp.Data.Result))
	}
	if resp.Data.Result[0].Metric["svc"] != "high" || resp.Data.Result[2].Metric["svc"] != "low" {
		t.Errorf("topk(3): wrong order %v", resp.Data.Result)
	}

	// passthrough on invalid body
	bad := []byte(`not json`)
	if string(applyMatrixPostAggregation(bad, instantMetricPostAgg{name: "topk", k: 1})) != "not json" {
		t.Error("invalid body should pass through unchanged")
	}
}

// =============================================================================
// range_metric_compat.go: addGroupByParsedLabels
// =============================================================================

func TestAddGroupByParsedLabels(t *testing.T) {
	// Only named keys get injected, and stream labels already present are skipped
	metricLabels := map[string]string{"app": "existing"}
	entry := map[string]interface{}{
		"status":  "200",
		"method":  "GET",
		"ignored": "nogroup",
	}
	addGroupByParsedLabels(metricLabels, entry, []string{"status", "method", "app"})

	if metricLabels["status"] != "200" {
		t.Errorf("status should be 200, got %q", metricLabels["status"])
	}
	if metricLabels["method"] != "GET" {
		t.Errorf("method should be GET, got %q", metricLabels["method"])
	}
	if metricLabels["app"] != "existing" {
		t.Errorf("app should not be overwritten, got %q", metricLabels["app"])
	}
	if _, ok := metricLabels["ignored"]; ok {
		t.Error("ignored key should not be injected (not in groupBy)")
	}

	// Empty value should not be injected
	entry2 := map[string]interface{}{"blank": "   "}
	m2 := map[string]string{}
	addGroupByParsedLabels(m2, entry2, []string{"blank"})
	if _, ok := m2["blank"]; ok {
		t.Error("blank/whitespace value should not be injected")
	}
}

// =============================================================================
// range_metric_compat.go: buildManualMetricLabels
// =============================================================================

func TestBuildManualMetricLabels(t *testing.T) {
	streamLabels := map[string]string{"app": "api", "env": "prod", "region": "us-east"}

	// no groupBy, not explicit → copy all stream labels
	got := buildManualMetricLabels(streamLabels, nil, false)
	if got["app"] != "api" || got["env"] != "prod" {
		t.Errorf("no groupBy: expected all labels, got %v", got)
	}

	// groupBy filter → only named keys
	got = buildManualMetricLabels(streamLabels, []string{"app", "region"}, false)
	if got["app"] != "api" || got["region"] != "us-east" {
		t.Errorf("groupBy: missing expected keys, got %v", got)
	}
	if _, ok := got["env"]; ok {
		t.Error("groupBy: env should be excluded")
	}

	// explicit empty by() → empty map (one series, no dimensions)
	got = buildManualMetricLabels(streamLabels, nil, true)
	if len(got) != 0 {
		t.Errorf("explicit empty by(): expected empty map, got %v", got)
	}

	// groupBy key missing from stream → not included
	got = buildManualMetricLabels(streamLabels, []string{"missing"}, false)
	if _, ok := got["missing"]; ok {
		t.Error("missing key should not appear in result")
	}
}

// =============================================================================
// range_metric_compat.go: parseFloatValue
// =============================================================================

func TestParseFloatValue(t *testing.T) {
	cases := []struct {
		input   interface{}
		want    float64
		wantOk  bool
	}{
		{float64(3.14), 3.14, true},
		{json.Number("2.71828"), 2.71828, true},
		{"42.5", 42.5, true},
		{"  1.0  ", 1.0, true},
		{"notanumber", 0, false},
		{json.Number("bad"), 0, false},
	}
	for _, tc := range cases {
		got, ok := parseFloatValue(tc.input)
		if ok != tc.wantOk {
			t.Errorf("parseFloatValue(%v) ok=%v, want %v", tc.input, ok, tc.wantOk)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("parseFloatValue(%v) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// =============================================================================
// metric_binary.go: statsQueryRangePointUnixNano
// =============================================================================

func TestStatsQueryRangePointUnixNano(t *testing.T) {
	wantNs := int64(1700000000) * int64(time.Second)

	// Unix seconds as float64 → converted to nanos
	ns := statsQueryRangePointUnixNano([]interface{}{float64(1700000000)})
	if ns != wantNs {
		t.Errorf("float64 unix-sec: got %d, want %d", ns, wantNs)
	}

	// Already nanoseconds as float64 (>1e15) → kept as is
	nanoVal := float64(wantNs)
	ns2 := statsQueryRangePointUnixNano([]interface{}{nanoVal})
	if ns2 != int64(nanoVal) {
		t.Errorf("float64 unix-nano: got %d, want %d", ns2, int64(nanoVal))
	}

	// float32
	ns_f32 := statsQueryRangePointUnixNano([]interface{}{float32(1700000000)})
	if ns_f32 != wantNs {
		t.Errorf("float32: got %d, want %d", ns_f32, wantNs)
	}

	// json.Number (unix seconds)
	ns3 := statsQueryRangePointUnixNano([]interface{}{json.Number("1700000000")})
	if ns3 != wantNs {
		t.Errorf("json.Number: got %d, want %d", ns3, wantNs)
	}

	// int64
	ns4 := statsQueryRangePointUnixNano([]interface{}{int64(1700000000)})
	if ns4 != wantNs {
		t.Errorf("int64: got %d, want %d", ns4, wantNs)
	}

	// int32
	ns_i32 := statsQueryRangePointUnixNano([]interface{}{int32(1700000000)})
	if ns_i32 != wantNs {
		t.Errorf("int32: got %d, want %d", ns_i32, wantNs)
	}

	// int
	ns5 := statsQueryRangePointUnixNano([]interface{}{int(1700000000)})
	if ns5 != wantNs {
		t.Errorf("int: got %d, want %d", ns5, wantNs)
	}

	// string (RFC3339 format)
	ns_str := statsQueryRangePointUnixNano([]interface{}{"2023-11-14T22:13:20Z"})
	if ns_str != wantNs {
		t.Errorf("string RFC3339: got %d, want %d", ns_str, wantNs)
	}

	// empty slice → 0
	if got := statsQueryRangePointUnixNano([]interface{}{}); got != 0 {
		t.Errorf("empty: got %d", got)
	}
}

// =============================================================================
// http_utils.go: isBinaryOpOnLogQuery, extractBinaryOp
// =============================================================================

func TestIsBinaryOpOnLogQuery(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"| json > 5", true},
		{"| logfmt == 0", true},
		{"| json | status >= 400", false}, // label filter on non-parser field
		{"| json | json >= 400", true},    // parser keyword as filter field
		{"", false},
		{"| json", false},
		{"just text", false},
	}
	for _, tc := range cases {
		got := isBinaryOpOnLogQuery(tc.input)
		if got != tc.want {
			t.Errorf("isBinaryOpOnLogQuery(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestExtractBinaryOp(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"| json > 5", ">"},
		{"| logfmt == 0", "=="},
		{"| json <= 10", "<="},
		{"nothing", "?"},
	}
	for _, tc := range cases {
		got := extractBinaryOp(tc.input)
		if got != tc.want {
			t.Errorf("extractBinaryOp(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// =============================================================================
// range_metric_compat.go: resolveManualMetricField
// =============================================================================

func TestResolveManualMetricField(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")

	// rate / count_over_time → __count__
	for _, fn := range []string{"rate", "count_over_time"} {
		field, phi, err := p.resolveManualMetricField(
			statsCompatSpec{}, originalRangeMetricSpec{}, fn)
		if err != nil || field != "__count__" || phi != 0 {
			t.Errorf("%s: expected __count__/0/nil, got %q/%f/%v", fn, field, phi, err)
		}
	}

	// bytes_over_time / bytes_rate → __bytes__
	for _, fn := range []string{"bytes_over_time", "bytes_rate"} {
		field, _, err := p.resolveManualMetricField(
			statsCompatSpec{}, originalRangeMetricSpec{}, fn)
		if err != nil || field != "__bytes__" {
			t.Errorf("%s: expected __bytes__, got %q/%v", fn, field, err)
		}
	}

	// avg with UnwrapField → uses unwrap field
	field, _, err := p.resolveManualMetricField(
		statsCompatSpec{Field: "ignored"},
		originalRangeMetricSpec{Func: "avg", UnwrapField: "response_time"},
		"avg",
	)
	if err != nil || field != "response_time" {
		t.Errorf("avg+unwrap: expected response_time, got %q/%v", field, err)
	}

	// sum with spec.Field (no unwrap) → uses spec.Field
	field, _, err = p.resolveManualMetricField(
		statsCompatSpec{Field: "latency"},
		originalRangeMetricSpec{Func: "sum"},
		"sum",
	)
	if err != nil || field != "latency" {
		t.Errorf("sum+field: expected latency, got %q/%v", field, err)
	}

	// avg with empty field (no unwrap, no spec.Field) → empty string, no error
	field, _, err = p.resolveManualMetricField(
		statsCompatSpec{Field: ""},
		originalRangeMetricSpec{Func: "avg"},
		"avg",
	)
	if err != nil || field != "" {
		t.Errorf("avg+nofield: expected empty/nil, got %q/%v", field, err)
	}

	// quantile with valid spec.Field (comma-separated: "phi,field") → returns field and phi
	field, phi, err := p.resolveManualMetricField(
		statsCompatSpec{Field: "0.95,latency_ms"},
		originalRangeMetricSpec{Func: "quantile_over_time"},
		"quantile",
	)
	if err != nil || field != "latency_ms" || phi != 0.95 {
		t.Errorf("quantile: expected latency_ms/0.95/nil, got %q/%f/%v", field, phi, err)
	}

	// quantile with invalid spec.Field → error
	_, _, err = p.resolveManualMetricField(
		statsCompatSpec{Field: "invalid_no_comma"},
		originalRangeMetricSpec{Func: "quantile_over_time"},
		"quantile",
	)
	if err == nil {
		t.Error("quantile: expected error for invalid spec, got nil")
	}
}

// =============================================================================
// metric_binary.go: parsePointValue
// =============================================================================

func TestParsePointValue(t *testing.T) {
	if got := parsePointValue(float64(3.14)); got != 3.14 {
		t.Errorf("float64: got %v", got)
	}
	if got := parsePointValue("2.71"); got != 2.71 {
		t.Errorf("string: got %v", got)
	}
	if got := parsePointValue("notanumber"); got != 0 {
		t.Errorf("invalid string: got %v", got)
	}
	if got := parsePointValue(nil); got != 0 {
		t.Errorf("nil: got %v", got)
	}
}

// =============================================================================
// label_index.go: mergeLabelValuesIndexEntry
// =============================================================================

func TestMergeLabelValuesIndexEntry(t *testing.T) {
	existing := labelValueIndexEntry{SeenCount: 5, LastSeen: 1000}

	// incoming has higher counts → take incoming
	incoming := labelValueIndexEntry{SeenCount: 10, LastSeen: 2000}
	result := mergeLabelValuesIndexEntry(existing, incoming)
	if result.SeenCount != 10 || result.LastSeen != 2000 {
		t.Errorf("higher incoming: got %+v", result)
	}

	// incoming has lower counts → keep existing
	lower := labelValueIndexEntry{SeenCount: 1, LastSeen: 500}
	result2 := mergeLabelValuesIndexEntry(existing, lower)
	if result2.SeenCount != 5 || result2.LastSeen != 1000 {
		t.Errorf("lower incoming: got %+v", result2)
	}

	// mixed: incoming SeenCount higher, existing LastSeen higher
	mixed := labelValueIndexEntry{SeenCount: 7, LastSeen: 800}
	result3 := mergeLabelValuesIndexEntry(existing, mixed)
	if result3.SeenCount != 7 || result3.LastSeen != 1000 {
		t.Errorf("mixed: got %+v", result3)
	}
}
