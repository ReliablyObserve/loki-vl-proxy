package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

const alertingLeakSentinel = "RULERSENTINEL-do-not-leak"

// alertingBackendAnswer is what a fake ruler or alerts backend sends.
type alertingBackendAnswer struct {
	name   string
	status int
	body   string
	// brokenAfter, when set, announces the whole body and drops the connection
	// after that many bytes: a truncated answer.
	brokenAfter int
	wantOK      bool
}

func (a alertingBackendAnswer) serve(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if a.brokenAfter > 0 {
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		c, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(a.body))
		_, _ = buf.WriteString(a.body[:a.brokenAfter])
		_ = buf.Flush()
		_ = c.Close()
		return
	}
	status := a.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(a.body))
}

func alertingBackendAnswers(valid string) []alertingBackendAnswer {
	return []alertingBackendAnswer{
		{name: "valid", body: valid, wantOK: true},
		{name: "malformed json", body: `{"status":"success","data":{"result":[` + alertingLeakSentinel + "\n"},
		{name: "truncated body", body: valid, brokenAfter: len(valid) / 2},
		{name: "empty body", body: ""},
		{name: "not json", body: alertingLeakSentinel},
		{name: "json that is not an object", body: `[` + `"` + alertingLeakSentinel + `"]`},
		{name: "error json under 200", body: `{"status":"error","errorType":"execution","error":"` + alertingLeakSentinel + `"}`},
		{name: "bare error under 200", body: `{"error":"` + alertingLeakSentinel + `"}`},
	}
}

// A ruler or alerts backend that answers with a body the proxy cannot pass on
// (broken off, not JSON, empty, or its own error under a success status)
// fails the request as Loki fails a ruler call it could not complete: 500 in
// the route's own error shape and Loki's text, never a 200 and never a word of
// the backend's body.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-silent-success, semantics/error-layer-backend-failure-status
func TestAlertingBackendAnswerDefects_FailAsLokiFails(t *testing.T) {
	const (
		validAlerts = `{"status":"success","data":{"alerts":[{"labels":{"alertname":"a"},"state":"firing"},{"labels":{"alertname":"b"},"state":"firing"}]}}`
		validRules  = `{"status":"success","data":{"groups":[{"name":"g","file":"f","interval":60,"rules":[{"name":"r","query":"sum(rate({app=\"x\"}[1m]))","type":"alerting","duration":0}]}]}}`
		rulerJSON   = `{"status":"error","data":null,"errorType":"server_error","error":"Internal Server Error"}`
	)
	routes := []struct {
		name, path, valid string
		rules             bool
		wantType          string // Content-Type of the failure
	}{
		{"alerts", "/prometheus/api/v1/alerts", validAlerts, false, "application/json"},
		{"alerts loki path", "/loki/api/v1/alerts", validAlerts, false, "application/json"},
		{"rules", "/prometheus/api/v1/rules", validRules, true, "text/plain; charset=utf-8"},
	}
	for _, rt := range routes {
		for _, ans := range alertingBackendAnswers(rt.valid) {
			t.Run(rt.name+"/"+ans.name, func(t *testing.T) {
				backend := httptest.NewServer(http.HandlerFunc(ans.serve))
				defer backend.Close()
				cfg := Config{BackendURL: "http://unused"}
				if rt.rules {
					cfg.RulerBackendURL = backend.URL
				} else {
					cfg.AlertsBackendURL = backend.URL
				}
				_, mux := newAlertProxyMux(t, cfg)
				req := httptest.NewRequest(http.MethodGet, rt.path, nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)

				if ans.wantOK {
					if rec.Code != http.StatusOK || rec.Body.String() != ans.body {
						t.Fatalf("valid answer: status %d body %q", rec.Code, rec.Body.String())
					}
					return
				}
				if rec.Code != http.StatusInternalServerError {
					t.Fatalf("status %d, want 500; body %q", rec.Code, rec.Body.String())
				}
				if got := rec.Header().Get("Content-Type"); got != rt.wantType {
					t.Fatalf("Content-Type %q, want %q", got, rt.wantType)
				}
				if got := rec.Body.String(); got != rulerJSON {
					t.Fatalf("body %q, want %q", got, rulerJSON)
				}
				visible := rec.Body.String()
				for k, vs := range rec.Header() {
					visible += "\n" + k + ": " + strings.Join(vs, ",")
				}
				if strings.Contains(visible, alertingLeakSentinel) || strings.Contains(visible, backend.URL) {
					t.Fatalf("the backend's words reached the client: %q", visible)
				}
			})
		}
	}
}

// The legacy rules views decode the ruler's answer into their own document.
// An error under a success status is refused (it would decode as no rules at
// all and answer an empty 200), and so is a body that does not decode.
// conformance: semantics/error-layer-silent-success, semantics/error-layer-backend-text
func TestAlertingLegacyRules_ErrorJSONUnder200Fails(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"error json", `{"status":"error","errorType":"execution","error":"` + alertingLeakSentinel + `"}`},
		{"truncated", `{"status":"success","data":{"groups":[{"name":"g"`},
		{"empty", ``},
		{"not json", alertingLeakSentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(alertingBackendAnswer{body: tc.body}.serve))
			defer backend.Close()
			_, mux := newAlertProxyMux(t, Config{BackendURL: "http://unused", RulerBackendURL: backend.URL})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/rules", nil)
			req.Header.Set("X-Scope-OrgID", "0")
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusInternalServerError || rec.Body.String() != "Internal Server Error\n" {
				t.Fatalf("status %d body %q, want Loki's 500", rec.Code, rec.Body.String())
			}
		})
	}
}

// Tenant-label mode reads the whole answer to filter it: the same defects
// fail the same way.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-silent-success
func TestAlertingTenantConfined_AnswerDefectsFail(t *testing.T) {
	for _, ans := range alertingBackendAnswers(`{"status":"success","data":{"alerts":[]}}`) {
		if ans.wantOK {
			continue
		}
		t.Run(ans.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(ans.serve))
			defer backend.Close()
			p := newTestProxyWithOptions(t, withTenantLabel("org_id"), func(c *Config) {
				c.AlertsBackendURL = backend.URL
			})
			req := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/alerts", nil)
			req.Header.Set("X-Scope-OrgID", "A")
			rec := httptest.NewRecorder()
			serveProxy(p, rec, req)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status %d, want 500; body %q", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), alertingLeakSentinel) {
				t.Fatalf("the backend's words reached the client: %q", rec.Body.String())
			}
		})
	}
}

// The decision itself: an answer passes on only when it is one JSON object
// that is not an error.
// conformance: semantics/error-layer-silent-success
func TestAlertingAnswerDefect_Table(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want error
	}{
		{"alerts", `{"status":"success","data":{"alerts":[]}}`, nil},
		{"rules", `{"status":"success","data":{"groups":[]}}`, nil},
		{"leading space", " \n{\"status\":\"success\"}", nil},
		{"empty", ``, errAlertingBodyNotJSON},
		{"blank", " \n", errAlertingBodyNotJSON},
		{"truncated", `{"status":"success","data":{"alerts":[`, errAlertingBodyNotJSON},
		{"trailing garbage", `{"status":"success"}x`, errAlertingBodyNotJSON},
		{"two documents", `{"status":"success"}{"status":"success"}`, errAlertingBodyNotJSON},
		{"array", `[]`, errAlertingBodyNotJSON},
		{"string", `"ok"`, errAlertingBodyNotJSON},
		{"null", `null`, errAlertingBodyNotJSON},
		{"text", `internal error`, errAlertingBodyNotJSON},
		{"status error", `{"status":"error","error":"x"}`, errAlertingErrorBody},
		{"error key without a status", `{"error":"x"}`, errAlertingErrorBody},
		{"status first decides", `{"status":"success","error":"x"}`, nil},
		{"status error with data", `{"status":"error","data":{"alerts":[]}}`, errAlertingErrorBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := alertingAnswerDefect([]byte(tc.body)); !errors.Is(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// FuzzAlertingAnswerDefect: whatever the body, the answer passes on only when
// it is one JSON object, and the failure's text carries none of the body.
func FuzzAlertingAnswerDefect(f *testing.F) {
	for _, s := range []string{``, `{}`, `[]`, `{"status":"success","data":{"alerts":[]}}`, `{"status":"error"}`, `{"error":"x"}`, `{"a":`, "null", ` {"a":1} `, `{"status":"success"}x`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		err := alertingAnswerDefect(body)
		var doc map[string]any
		isObject := json.Unmarshal(body, &doc) == nil && doc != nil && bytes.HasPrefix(bytes.TrimSpace(body), []byte("{"))
		if err == nil && !isObject {
			t.Fatalf("%q passed on but is not one JSON object", body)
		}
		if errors.Is(err, errAlertingErrorBody) && !bytes.Contains(body, []byte("error")) {
			t.Fatalf("%q failed as an error body but holds no error", body)
		}
		if err != nil && errors.Is(err, errAlertingBodyNotJSON) && isObject {
			t.Fatalf("%q is one JSON object but failed as not JSON", body)
		}
		if err != nil {
			failure := alertingBackendFailure(err)
			if failure.Text() != lokiErrInternal || failure.Status() != http.StatusInternalServerError {
				t.Fatalf("failure %d %q, want Loki's 500", failure.Status(), failure.Text())
			}
			if failure.Kind() != lokierr.KindBackendFailed {
				t.Fatalf("failure kind %v", failure.Kind())
			}
		}
	})
}

// alertingRoundTrip answers every backend call from memory, so a benchmark
// measures the proxy's work on an answer and not the loopback sockets.
type alertingRoundTrip struct {
	answer string
}

func (rt alertingRoundTrip) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(strings.NewReader(rt.answer)),
		ContentLength: int64(len(rt.answer)),
	}, nil
}

// BenchmarkAlertingPassthrough is one alerts read through the proxy, with the
// backend answering `alerts` alerts from memory: what the validation before the
// first byte costs on a route that used to stream the answer.
func BenchmarkAlertingPassthrough(b *testing.B) {
	for _, alerts := range []int{10, 1000} {
		var body strings.Builder
		body.WriteString(`{"status":"success","data":{"alerts":[`)
		for i := 0; i < alerts; i++ {
			if i > 0 {
				body.WriteByte(',')
			}
			fmt.Fprintf(&body, `{"labels":{"alertname":"a%d","severity":"page"},"annotations":{"summary":"an alert"},"state":"firing","activeAt":"2026-01-01T00:00:00Z","value":"1"}`, i)
		}
		body.WriteString(`]}}`)
		p, err := New(Config{BackendURL: "http://unused", AlertsBackendURL: "http://alerts.invalid", Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
		if err != nil {
			b.Fatal(err)
		}
		p.client = &http.Client{Transport: alertingRoundTrip{answer: body.String()}}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		b.Run(fmt.Sprintf("alerts=%d", alerts), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/alerts", nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d", rec.Code)
				}
			}
		})
	}
}
