package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

const statsLeakSentinel = "STATSSENTINEL-do-not-leak"

// statsDefectBodies are answers of a stats endpoint, sent under a success
// status, that are no answer.
var statsDefectBodies = []struct{ name, body string }{
	{"not json", statsLeakSentinel},
	{"truncated", `{"results":[{"metric":{"app":"` + statsLeakSentinel},
	{"null", `null`},
	{"error key", `{"error":"` + statsLeakSentinel + `"}`},
	{"status error", `{"status":"error","msg":"` + statsLeakSentinel + `"}`},
}

// A body that is no answer fails through the error layer, with Loki's failed
// call (500) and none of the backend's words, from every function that wraps
// or combines stats bodies.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-silent-success, semantics/error-layer-backend-failure-status
func TestStatsBodyDefects_FailTyped(t *testing.T) {
	valid := scalarVectorBody(4)
	for _, d := range statsDefectBodies {
		body := []byte(d.body)
		for name, call := range map[string]func() ([]byte, error){
			"wrapAsLokiResponse":   func() ([]byte, error) { return wrapAsLokiResponse(body, "matrix") },
			"applyScalarOp":        func() ([]byte, error) { return applyScalarOp(body, "+", 2, "vector") },
			"applyScalarOpReverse": func() ([]byte, error) { return applyScalarOpReverse(body, "-", 2, "vector") },
			"combine left": func() ([]byte, error) {
				return combineBinaryMetricResults(body, valid, "+", "vector", false, false, "", "")
			},
			"combine right": func() ([]byte, error) {
				return combineBinaryMetricResults(valid, body, "+", "vector", false, false, "", "")
			},
			"combine scalar on right": func() ([]byte, error) {
				return combineBinaryMetricResults(body, nil, "+", "vector", false, true, "", "2")
			},
			"combine scalar on left": func() ([]byte, error) {
				return combineBinaryMetricResults(nil, body, "+", "vector", true, false, "2", "")
			},
		} {
			t.Run(name+"/"+d.name, func(t *testing.T) {
				out, err := call()
				if err == nil {
					t.Fatalf("a body that is no answer was answered: %s", out)
				}
				var typed *lokierr.Error
				if !errors.As(err, &typed) || typed.Kind() != lokierr.KindBackendFailed {
					t.Fatalf("not a typed backend failure: %T %v", err, err)
				}
				if typed.Status() != http.StatusInternalServerError || typed.Text() != lokiErrInternal {
					t.Fatalf("client sees %d %q, want Loki's 500 text", typed.Status(), typed.Text())
				}
				if strings.Contains(typed.Text(), statsLeakSentinel) {
					t.Fatalf("the backend's words are in the client text: %q", typed.Text())
				}
				if out != nil {
					t.Fatalf("a failed call returned a body: %s", out)
				}
			})
		}
	}
}

// Valid answers, and the shapes the wrapper accepts, still wrap into Loki's
// envelope.
// conformance: semantics/error-layer-silent-success
func TestStatsBodyDefects_ValidAnswersStillWrap(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"envelope", `{"status":"success","data":{"resultType":"matrix","result":[]}}`},
		{"no status", `{"data":{"resultType":"matrix","result":[]}}`},
		{"no envelope", `{"resultType":"matrix","result":[]}`},
		{"results key", `{"results":[]}`},
		{"empty object", `{}`},
		{"empty body is an empty result", ``},
		{"blank body is an empty result", " \n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := wrapAsLokiResponse([]byte(tc.body), "matrix")
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(out, &doc) != nil || doc.Status != "success" {
				t.Fatalf("not a success envelope: %s", out)
			}
		})
	}
}

// FuzzWrapAsLokiResponse: whatever the body, the wrapper answers a valid
// success envelope or fails with Loki's typed 500; it never answers a body that
// is not JSON, and the failure's client text never holds the body.
func FuzzWrapAsLokiResponse(f *testing.F) {
	for _, d := range statsDefectBodies {
		f.Add([]byte(d.body))
	}
	for _, s := range []string{`{"status":"success","data":{"resultType":"matrix","result":[]}}`, `{"data":{"resultType":"vector","result":[]}}`, `{"results":[]}`, `{"data":1}`, `[]`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		if bytes.HasPrefix(body, []byte(`{"status":"success","data":{"resultType":`)) || bytes.HasPrefix(body, []byte(`{"data":{"resultType":`)) {
			t.Skip("the fast paths return a body the proxy already wrapped, unchecked")
		}
		out, err := wrapAsLokiResponse(body, "matrix")
		if err != nil {
			typed, ok := err.(*lokierr.Error)
			if !ok || typed.Status() != http.StatusInternalServerError || typed.Text() != lokiErrInternal {
				t.Fatalf("%q failed with %T %v, want Loki's typed 500", body, err, err)
			}
			return
		}
		if !json.Valid(out) {
			t.Fatalf("%q answered a body that is not JSON: %q", body, out)
		}
	})
}

// A stats or binary-operand answer that is no answer fails the request as
// Loki fails a call it could not complete: 500 and Loki's text, never a 200
// and never the backend's words.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-silent-success, semantics/error-layer-backend-failure-status
func TestStatsBodyDefects_RoutesFailAsLokiFails(t *testing.T) {
	queries := []struct{ name, path, query string }{
		{"range scalar op", "/loki/api/v1/query_range", `sum(rate({app="x"}[1m])) * 2`},
		{"range reverse scalar op", "/loki/api/v1/query_range", `2 / sum(rate({app="x"}[1m]))`},
		{"range two vectors", "/loki/api/v1/query_range", `sum(rate({app="x"}[1m])) + sum(rate({app="y"}[1m]))`},
		{"instant scalar op", "/loki/api/v1/query", `sum(rate({app="x"}[1m])) * 2`},
		{"instant two vectors", "/loki/api/v1/query", `sum(rate({app="x"}[1m])) + sum(rate({app="y"}[1m]))`},
		{"range sum", "/loki/api/v1/query_range", `sum(rate({app="x"}[1m]))`},
		{"instant sum", "/loki/api/v1/query", `sum(rate({app="x"}[1m]))`},
	}
	for _, q := range queries {
		for _, d := range statsDefectBodies {
			t.Run(q.name+"/"+d.name, func(t *testing.T) {
				vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query") {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(d.body))
						return
					}
					_, _ = w.Write([]byte(`{"values":[]}`))
				}))
				defer vl.Close()
				p := newTestProxyWithOptions(t, withBackendURL(vl.URL))
				params := url.Values{"query": {q.query}, "start": {"1704067200"}, "end": {"1704070800"}, "step": {"60"}, "time": {"1704070800"}}
				req := httptest.NewRequest(http.MethodGet, q.path+"?"+params.Encode(), nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				serveProxy(p, rec, req)

				if rec.Code != http.StatusInternalServerError {
					t.Fatalf("status %d, want 500; body %q", rec.Code, rec.Body.String())
				}
				if got := strings.TrimSpace(rec.Body.String()); got != lokiErrInternal {
					t.Fatalf("body %q, want %q", got, lokiErrInternal)
				}
				if strings.Contains(rec.Body.String(), statsLeakSentinel) {
					t.Fatalf("the backend's words reached the client: %q", rec.Body.String())
				}
			})
		}
	}
}

// A coalesced backend answer above the bytes the proxy buffers fails as every
// buffered read of that size does: Loki's message size error (500), the flag in
// the log line only. An answer of exactly the limit is served.
// conformance: semantics/error-layer-proxy-limit-status
func TestCoalescedBackendAnswer_OverTheBufferLimitFailsLikeLoki(t *testing.T) {
	const limit = 2048
	for _, rt := range []struct{ name, path, query, answer string }{
		{"index_stats", "/loki/api/v1/index/stats", `{app="x"}`, `{"entries":"5","streams":"1"}`},
		{"instant_stats", "/loki/api/v1/query", `sum(count_over_time({app="x"}[5m]))`, `{"status":"success","data":{"resultType":"vector","result":[]}}`},
	} {
		for _, tc := range []struct {
			name string
			pad  int // bytes of trailing whitespace after the answer
			want int
		}{
			{"under the limit", 0, http.StatusOK},
			{"at the limit", limit - len(rt.answer), http.StatusOK},
			{"one byte over", limit - len(rt.answer) + 1, http.StatusInternalServerError},
			{"far over", 10 * limit, http.StatusInternalServerError},
		} {
			t.Run(rt.name+"/"+tc.name, func(t *testing.T) {
				vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(rt.answer + strings.Repeat(" ", tc.pad)))
				}))
				defer vl.Close()
				p := newTestProxyWithOptions(t, withBackendURL(vl.URL), func(c *Config) {
					c.ExecutionLimits.BackendMaxBufferedResponseBytes = limit
				})
				params := url.Values{"query": {rt.query}, "start": {"1704067200"}, "end": {"1704070800"}, "time": {"1704070800"}}
				req := httptest.NewRequest(http.MethodGet, rt.path+"?"+params.Encode(), nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				serveProxy(p, rec, req)
				if rec.Code != tc.want {
					t.Fatalf("status %d, want %d; body %q", rec.Code, tc.want, rec.Body.String())
				}
				if tc.want == http.StatusOK {
					return
				}
				if !errLayerMessageSizeRE.MatchString(rec.Body.String()) || strings.Contains(rec.Body.String(), "-backend-max") {
					t.Fatalf("body %q is not Loki's message size error without a flag", rec.Body.String())
				}
			})
		}
	}
}

// A binary operand above the bytes the proxy buffers fails with Loki's message
// size error (500), not a bare 500 and not a partial answer; the flag is in the
// log line only. An operand of exactly the limit is served. The handler under
// test is proxyBinaryMetric, which evaluates the operands itself.
// conformance: semantics/error-layer-proxy-limit-status
func TestBinaryOperand_OverTheBufferLimitFailsLikeLoki(t *testing.T) {
	const limit = 4096
	rows := func(n int) string {
		var b strings.Builder
		b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"x"},"values":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, 1704067200+i*60, i)
		}
		b.WriteString(`]}]}}`)
		return b.String()
	}
	small, big := rows(3), rows(2000)
	query := func(app string) physical.Op {
		return physical.VLQuery{Text: `{app="` + app + `"} | stats count() hits`}
	}
	scalar := physical.ScalarOp{V: 2, Text: "2"}
	for _, shape := range []struct {
		name        string
		left, right physical.Op
	}{
		{"vector op scalar", query("x"), scalar},
		{"scalar op vector", scalar, query("x")},
		{"two vectors", query("x"), query("y")},
		{"nested binary", physical.Binary{Op: "+", L: query("x"), R: scalar}, query("y")},
	} {
		for _, tc := range []struct {
			name, body string
			want       int
		}{{"under the limit", small, http.StatusOK}, {"over the limit", big, http.StatusInternalServerError}} {
			t.Run(shape.name+"/"+tc.name, func(t *testing.T) {
				vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(tc.body))
				}))
				defer vl.Close()
				p := newTestProxyWithOptions(t, withBackendURL(vl.URL), func(c *Config) {
					c.ExecutionLimits.BackendMaxBufferedResponseBytes = limit
				})
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?start=1704067200&end=1704070800&step=60", nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				p.proxyBinaryMetric(rec, req, "+", shape.left, shape.right, "stats_query_range", "matrix")
				if rec.Code != tc.want {
					t.Fatalf("status %d, want %d; body %.200q", rec.Code, tc.want, rec.Body.String())
				}
				if tc.want != http.StatusOK && (!errLayerMessageSizeRE.MatchString(rec.Body.String()) || strings.Contains(rec.Body.String(), "-backend-max")) {
					t.Fatalf("body %.200q is not Loki's message size error without a flag", rec.Body.String())
				}
			})
		}
	}
}
