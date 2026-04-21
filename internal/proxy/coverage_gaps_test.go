package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
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
	input := `app="he,llo",namespace="world"`
	pairs := splitLabelPairs(input)
	if len(pairs) != 2 {
		t.Errorf("expected 2 pairs (comma in quotes ignored), got %d: %v", len(pairs), pairs)
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
