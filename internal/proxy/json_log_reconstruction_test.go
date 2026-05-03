package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestReconstructLogLine verifies the core reconstruction logic.
func TestReconstructLogLine(t *testing.T) {
	tests := []struct {
		name          string
		msg           string
		entry         map[string]interface{}
		originalQuery string
		wantMsg       string // if empty, wantJSON is used
		wantJSON      map[string]string
		wantPlain     bool // expect original msg returned unchanged
	}{
		{
			name: "plain text log - no extra fields",
			msg:  "GET /api/users 200 15ms",
			entry: map[string]interface{}{
				"_time":   "2026-01-01T00:00:00Z",
				"_msg":    "GET /api/users 200 15ms",
				"_stream": `{app="nginx",namespace="prod"}`,
				"app":     "nginx",
				"namespace": "prod",
			},
			originalQuery: `{app="nginx"}`,
			wantPlain:     true,
		},
		{
			name: "JSON log - extra fields reconstructed",
			msg:  "PUT /api/v1/users 401 233ms",
			entry: map[string]interface{}{
				"_time":      "2026-01-01T00:00:00Z",
				"_msg":       "PUT /api/v1/users 401 233ms",
				"_stream":    `{app="api-gateway",namespace="prod",level="warn"}`,
				"app":        "api-gateway",
				"namespace":  "prod",
				"level":      "warn",
				"method":     "PUT",
				"path":       "/api/v1/users",
				"status":     "401",
				"trace_id":   "abc123",
				"service.name": "api-gateway",
			},
			originalQuery: `{app="api-gateway"}`,
			wantJSON: map[string]string{
				"_msg":         "PUT /api/v1/users 401 233ms",
				"method":       "PUT",
				"path":         "/api/v1/users",
				"status":       "401",
				"trace_id":     "abc123",
				"service.name": "api-gateway",
			},
		},
		{
			name: "JSON log - stream label fields excluded from JSON",
			msg:  "GET /health 200 5ms",
			entry: map[string]interface{}{
				"_time":     "2026-01-01T00:00:00Z",
				"_msg":      "GET /health 200 5ms",
				"_stream":   `{app="api",namespace="prod",pod="api-abc"}`,
				"app":       "api",
				"namespace": "prod",
				"pod":       "api-abc",
				"method":    "GET",
				"status":    "200",
			},
			originalQuery: `{app="api"}`,
			wantJSON: map[string]string{
				"_msg":   "GET /health 200 5ms",
				"method": "GET",
				"status": "200",
			},
		},
		{
			name: "logfmt query - no reconstruction",
			msg:  "level=info method=GET status=200",
			entry: map[string]interface{}{
				"_time":   "2026-01-01T00:00:00Z",
				"_msg":    "level=info method=GET status=200",
				"_stream": `{app="logfmt-svc"}`,
				"app":     "logfmt-svc",
				"method":  "GET",
				"status":  "200",
			},
			originalQuery: `{app="logfmt-svc"} | logfmt`,
			wantPlain:     true,
		},
		{
			name: "regexp query - no reconstruction",
			msg:  "192.168.1.1 GET /api 200",
			entry: map[string]interface{}{
				"_time":   "2026-01-01T00:00:00Z",
				"_msg":    "192.168.1.1 GET /api 200",
				"_stream": `{app="nginx"}`,
				"app":     "nginx",
				"method":  "GET",
				"status":  "200",
			},
			originalQuery: `{app="nginx"} | regexp "(?P<method>[A-Z]+)"`,
			wantPlain:     true,
		},
		{
			name: "pattern query - no reconstruction",
			msg:  "GET /api 200",
			entry: map[string]interface{}{
				"_time":   "2026-01-01T00:00:00Z",
				"_msg":    "GET /api 200",
				"_stream": `{app="nginx"}`,
				"app":     "nginx",
				"method":  "GET",
			},
			originalQuery: `{app="nginx"} | pattern "<method> <path> <status>"`,
			wantPlain:     true,
		},
		{
			name: "json query - no reconstruction (Loki returns original msg)",
			msg:  "POST /login 200 12ms",
			entry: map[string]interface{}{
				"_time":   "2026-01-01T00:00:00Z",
				"_msg":    "POST /login 200 12ms",
				"_stream": `{app="auth"}`,
				"app":     "auth",
				"method":  "POST",
				"status":  "200",
			},
			originalQuery: `{app="auth"} | json`,
			wantPlain:     true,
		},
		{
			name: "no _stream field - reconstructs because method is extra",
			msg:  "some log line",
			entry: map[string]interface{}{
				"_time":  "2026-01-01T00:00:00Z",
				"_msg":   "some log line",
				"method": "GET",
			},
			originalQuery: `{app="x"}`,
			wantJSON: map[string]string{
				"_msg":   "some log line",
				"method": "GET",
			},
		},
		{
			name: "empty extra fields - plain text",
			msg:  "plain message",
			entry: map[string]interface{}{
				"_time":   "2026-01-01T00:00:00Z",
				"_msg":    "plain message",
				"_stream": `{app="svc",level="info"}`,
				"app":     "svc",
				"level":   "info",
			},
			originalQuery: `{app="svc"}`,
			wantPlain:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamLabels := parseStreamLabels(asString(tt.entry["_stream"]))
			result := reconstructLogLine(tt.msg, tt.entry, streamLabels, tt.originalQuery)

			if tt.wantPlain {
				if result != tt.msg {
					t.Errorf("expected plain msg %q, got %q", tt.msg, result)
				}
				return
			}

			// Parse result as JSON and verify required fields
			var parsed map[string]string
			if err := json.Unmarshal([]byte(result), &parsed); err != nil {
				t.Fatalf("result is not valid JSON: %v\nresult: %s", err, result)
			}

			for k, wantV := range tt.wantJSON {
				gotV, ok := parsed[k]
				if !ok {
					t.Errorf("missing field %q in result", k)
					continue
				}
				if gotV != wantV {
					t.Errorf("field %q: got %q, want %q", k, gotV, wantV)
				}
			}
		})
	}
}

// TestReconstructLogLine_StreamLabelExclusion verifies that stream label fields
// are excluded from the reconstructed JSON to match Loki's native format.
func TestReconstructLogLine_StreamLabelExclusion(t *testing.T) {
	entry := map[string]interface{}{
		"_time":     "2026-01-01T00:00:00Z",
		"_msg":      "request processed",
		"_stream":   `{app="api-gateway",cluster="us-east-1",namespace="prod",pod="api-xyz"}`,
		"app":       "api-gateway",
		"cluster":   "us-east-1",
		"namespace": "prod",
		"pod":       "api-xyz",
		"method":    "GET",
		"path":      "/api/v1/users",
		"status":    "200",
		"trace_id":  "abc123",
	}

	streamLabels := parseStreamLabels(asString(entry["_stream"]))
	result := reconstructLogLine("request processed", entry, streamLabels, "")

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	// Stream label fields should NOT be in the JSON
	for _, streamField := range []string{"app", "cluster", "namespace", "pod"} {
		if _, ok := parsed[streamField]; ok {
			t.Errorf("stream label field %q should not be in reconstructed JSON", streamField)
		}
	}

	// Extra fields SHOULD be in the JSON
	for _, extraField := range []string{"method", "path", "status", "trace_id"} {
		if _, ok := parsed[extraField]; !ok {
			t.Errorf("extra field %q should be in reconstructed JSON", extraField)
		}
	}

	// _msg must always be present
	if parsed["_msg"] == nil {
		t.Error("_msg field must be present in reconstructed JSON")
	}
}

// TestHasTextExtractionParser verifies correct detection of text-extraction parsers.
func TestHasTextExtractionParser(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`{app="x"}`, false},
		{`{app="x"} | json`, true},
		{`{app="x"} | json method, status`, true},
		{`{app="x"} | logfmt`, true},
		{`{app="x"} | logfmt method, status`, true},
		{`{app="x"} | regexp "(?P<method>[A-Z]+)"`, true},
		{`{app="x"} | pattern "<method> <path>"`, true},
		{`{app="x"} | json | logfmt`, true},
		{`{app="x"} | json | regexp "foo"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := hasTextExtractionParser(tt.query)
			if got != tt.want {
				t.Errorf("hasTextExtractionParser(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestJSONLogLineReconstructionInQueryRange tests reconstruction end-to-end
// via the full proxy handler stack.
func TestJSONLogLineReconstructionInQueryRange(t *testing.T) {
	// VL response with extra non-stream fields (simulating JSON log ingestion)
	vlResp := `{"_time":"2026-01-01T10:00:00Z","_msg":"PUT /api/v1/users 401 233ms","_stream":"{app=\"api-gateway\",namespace=\"prod\"}","app":"api-gateway","namespace":"prod","method":"PUT","path":"/api/v1/users","status":"401","trace_id":"abc123"}` + "\n"

	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(vlResp))
	}))
	defer vl.Close()

	p := newTestProxy(t, vl.URL)

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=%7Bapp%3D%22api-gateway%22%7D&start=1000000000000000000&end=2000000000000000000&limit=10", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}

	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Fatal("expected at least one stream result")
	}

	stream, _ := result[0].(map[string]interface{})
	values, _ := stream["values"].([]interface{})
	if len(values) == 0 {
		t.Fatal("expected at least one log value")
	}

	entry, _ := values[0].([]interface{})
	if len(entry) < 2 {
		t.Fatal("expected [timestamp, log_line] in values entry")
	}

	logLine, _ := entry[1].(string)

	// Log line should be JSON containing the extra fields
	if !strings.HasPrefix(logLine, "{") {
		t.Errorf("expected JSON log line, got: %s", logLine)
	}

	var logJSON map[string]string
	if err := json.Unmarshal([]byte(logLine), &logJSON); err != nil {
		t.Fatalf("log line is not valid JSON: %v\nlog line: %s", err, logLine)
	}

	// Verify required fields
	want := map[string]string{
		"_msg":   "PUT /api/v1/users 401 233ms",
		"method": "PUT",
		"path":   "/api/v1/users",
		"status": "401",
	}
	for k, v := range want {
		if logJSON[k] != v {
			t.Errorf("log line field %q: got %q, want %q", k, logJSON[k], v)
		}
	}

	// Stream labels should NOT be in the log line
	for _, streamField := range []string{"app", "namespace"} {
		if _, ok := logJSON[streamField]; ok {
			t.Errorf("stream label %q should not be in log line JSON", streamField)
		}
	}
}

// TestLogfmtQueryNoReconstruction verifies that | logfmt queries return plain text log lines.
func TestLogfmtQueryNoReconstruction(t *testing.T) {
	vlResp := `{"_time":"2026-01-01T10:00:00Z","_msg":"level=info method=GET status=200","_stream":"{app=\"logfmt-svc\"}","app":"logfmt-svc","method":"GET","status":"200"}` + "\n"

	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(vlResp))
	}))
	defer vl.Close()

	p := newTestProxy(t, vl.URL)

	mux2 := http.NewServeMux()
	p.RegisterRoutes(mux2)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=%7Bapp%3D%22logfmt-svc%22%7D+%7C+logfmt&start=1000000000000000000&end=2000000000000000000&limit=10", nil)
	w := httptest.NewRecorder()
	mux2.ServeHTTP(w, req)

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}

	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Fatal("expected at least one stream result")
	}

	stream, _ := result[0].(map[string]interface{})
	values, _ := stream["values"].([]interface{})
	if len(values) == 0 {
		t.Fatal("expected at least one value")
	}

	entry, _ := values[0].([]interface{})
	logLine, _ := entry[1].(string)

	// Log line should be the original logfmt text, NOT wrapped in JSON
	if strings.HasPrefix(logLine, "{") {
		t.Errorf("logfmt query should not produce JSON log line, got: %s", logLine)
	}
	if logLine != "level=info method=GET status=200" {
		t.Errorf("expected logfmt text, got: %s", logLine)
	}
}

// TestPlainTextLogNoReconstruction verifies that plain text logs are not reconstructed.
func TestPlainTextLogNoReconstruction(t *testing.T) {
	// Entry with NO extra non-stream fields (plain text log)
	vlResp := `{"_time":"2026-01-01T10:00:00Z","_msg":"application started","_stream":"{app=\"myapp\",env=\"prod\"}","app":"myapp","env":"prod"}` + "\n"

	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(vlResp))
	}))
	defer vl.Close()

	p := newTestProxy(t, vl.URL)

	mux3 := http.NewServeMux()
	p.RegisterRoutes(mux3)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=%7Bapp%3D%22myapp%22%7D&start=1000000000000000000&end=2000000000000000000&limit=10", nil)
	w := httptest.NewRecorder()
	mux3.ServeHTTP(w, req)

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}

	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Fatal("expected at least one stream result")
	}

	stream, _ := result[0].(map[string]interface{})
	values, _ := stream["values"].([]interface{})
	entry, _ := values[0].([]interface{})
	logLine, _ := entry[1].(string)

	// Plain text log should be returned unchanged
	if logLine != "application started" {
		t.Errorf("plain text log should be unchanged, got: %s", logLine)
	}
}
