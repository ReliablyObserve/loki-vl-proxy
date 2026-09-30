package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// A label filter after `| json` names Loki's sanitized label (http_method) while
// VictoriaLogs' unpack_json keeps the original key (http.method): the query sent
// upstream must accept every spelling, for log and metric queries alike.
//
// conformance: profiles/sanitized-json-key-filter
func TestSanitizedJSONKeyFilterReachesVictoriaLogs(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		queries = append(queries, r.Form.Get("query"))
		mu.Unlock()
	}))
	defer backend.Close()

	for _, tc := range []struct{ name, path, query string }{
		{"log query", "/loki/api/v1/query_range", `{app="web"} | json | http_method="GET"`},
		{"metric query", "/loki/api/v1/query_range", `sum(count_over_time({app="web"} | json | http_method="GET" | http_status_code>=400 [5m]))`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			queries = nil
			mu.Unlock()
			p := newTestProxy(t, backend.URL)
			q := url.Values{"query": {tc.query}, "start": {"1767225600"}, "end": {"1767229200"}, "step": {"60"}, "time": {"1767229200"}}
			if res := doCompatProxyRequest(p, tc.path+"?"+q.Encode(), nil); res.Code != http.StatusOK {
				t.Fatalf("%s: %d %s", tc.query, res.Code, res.Body)
			}
			mu.Lock()
			defer mu.Unlock()
			var sawSpelling bool
			for _, vl := range queries {
				if strings.Contains(vl, `"http.method"`) && strings.Contains(vl, `"http-method"`) {
					sawSpelling = true
				}
			}
			if !sawSpelling {
				t.Fatalf("no upstream query accepts the original keys of http_method: %q", queries)
			}
		})
	}
}

// Loki's logfmt parser sanitizes keys like its json parser: detected_fields
// must name a dotted or hyphenated logfmt key by the label a query can use.
//
// conformance: profiles/detected-fields-dotted-json-keys
func TestDetectedFieldsSanitizeLogfmtKeys(t *testing.T) {
	const row = `{"_time":"2026-04-04T17:18:49.971082Z","_msg":"msg=login http.method=GET user-agent=curl","_stream":"{app=\"svc\"}","app":"svc"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(row + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	for _, tc := range []struct {
		dotted string
		want   []string
		absent []string
	}{
		{CompatReject, []string{"http_method", "user_agent"}, []string{"http.method", "user-agent"}},
		{CompatAccept, []string{"http.method", "user-agent"}, []string{"http_method", "user_agent"}},
	} {
		t.Run(tc.dotted, func(t *testing.T) {
			p := newCompatProxy(t, backend.URL, compatOptions{style: LabelStyleUnderscores, mode: MetadataFieldModeTranslated, emit: true, dotted: tc.dotted})
			w := serve(p, http.MethodGet, "/loki/api/v1/detected_fields?start=1775322000000000000&end=1775325600000000000&query="+url.QueryEscape(`{app="svc"}`), nil)
			if w.Code != http.StatusOK {
				t.Fatalf("detected_fields: %d %s", w.Code, w.Body)
			}
			body := w.Body.String()
			for _, name := range tc.want {
				if !strings.Contains(body, `"label":"`+name+`"`) {
					t.Errorf("no field %q: %s", name, body)
				}
			}
			for _, name := range tc.absent {
				if strings.Contains(body, `"label":"`+name+`"`) {
					t.Errorf("unexpected field %q: %s", name, body)
				}
			}
		})
	}
}

// The service_name values are read through the same response cap as every
// other label's values.
//
// conformance: operator-configurable-limits, limits/label-values-response-cap
func TestLabelValuesResponseCap_ServiceNameValuesAreBounded(t *testing.T) {
	values := make([]fieldHit, 2000)
	for i := range values {
		values[i] = fieldHit{Value: fmt.Sprintf("{service_name=\"svc-%06d\"}", i), Hits: 1}
	}
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_values", "/select/logsql/stream_field_values", "/select/logsql/streams":
			writeVLFieldValues(w, values)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(vl.Close)
	p := newGuardTestProxy(t, Config{BackendURL: vl.URL, ExecutionLimits: ExecutionLimitsConfig{LabelValuesMaxResponseBytes: 4096}})
	now := time.Now()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/label/service_name/values?start=%d&end=%d", now.Add(-time.Hour).UnixNano(), now.UnixNano()), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	p.handleLabelValues(rec, req)
	if rec.Code != http.StatusInternalServerError || !resourceExhaustedRE.MatchString(lokiErrorText(t, rec)) {
		t.Fatalf("status %d body %s, want Loki's ResourceExhausted 500", rec.Code, bodyHead(rec))
	}
}
