package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// vlParseErrorBody mirrors the plain-text body VictoriaLogs writes with HTTP 400
// when it rejects a query (httpserver.Errorf). It echoes the query, so the
// secret literal must never reach the client.
const vlParseErrorBody = "cannot parse `query` arg: invalid regexp \"app\":\"(billing-secret-value\": error parsing regexp: missing closing ): `(billing-secret-value`; context: [app:~\"(billing-secret-value\"]; query=app:~\"(billing-secret-value\""

type vlBadRequestBackend struct {
	*httptest.Server
	mu    sync.Mutex
	paths []string
}

func (b *vlBadRequestBackend) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.paths...)
}

// newVLBadRequestBackend answers every VictoriaLogs call with HTTP 400 and a
// parse error body, recording the requested paths.
func newVLBadRequestBackend(t *testing.T) *vlBadRequestBackend {
	t.Helper()
	b := &vlBadRequestBackend{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		b.paths = append(b.paths, r.URL.Path)
		b.mu.Unlock()
		http.Error(w, vlParseErrorBody, http.StatusBadRequest)
	}))
	t.Cleanup(b.Close)
	return b
}

func assertLokiBadData(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v; body=%s", err, w.Body.String())
	}
	if body.Status != "error" || body.ErrorType != "bad_data" {
		t.Fatalf("envelope = %+v, want status=error errorType=bad_data", body)
	}
	if body.Error == "" {
		t.Fatalf("expected a non-empty error message")
	}
	if strings.Contains(w.Body.String(), "billing-secret-value") {
		t.Fatalf("backend query echo leaked to client: %s", w.Body.String())
	}
}

func badRequestTestRange() (string, string) {
	end := time.Now()
	return strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10), strconv.FormatInt(end.UnixNano(), 10)
}

func badRequestURL(path string, params map[string]string) string {
	start, end := badRequestTestRange()
	values := url.Values{}
	values.Set("start", start)
	values.Set("end", end)
	for k, v := range params {
		values.Set(k, v)
	}
	return path + "?" + values.Encode()
}

// TestUpstreamBadRequest_MapsToLokiBadData pins Loki's contract for a query the
// backend rejects: every endpoint family answers 400 bad_data, not 502.
func TestUpstreamBadRequest_MapsToLokiBadData(t *testing.T) {
	const sel = `{app="billing"}`
	cases := []struct {
		name    string
		handler func(p *Proxy) http.HandlerFunc
		uri     string
		headers map[string]string
	}{
		{"labels", func(p *Proxy) http.HandlerFunc { return p.handleLabels },
			badRequestURL("/loki/api/v1/labels", map[string]string{"query": sel}), nil},
		{"label_values", func(p *Proxy) http.HandlerFunc { return p.handleLabelValues },
			badRequestURL("/loki/api/v1/label/app/values", map[string]string{"query": sel}), nil},
		{"label_values_service_name", func(p *Proxy) http.HandlerFunc { return p.handleLabelValues },
			badRequestURL("/loki/api/v1/label/service_name/values", map[string]string{"query": sel}), nil},
		{"series", func(p *Proxy) http.HandlerFunc { return p.handleSeries },
			badRequestURL("/loki/api/v1/series", map[string]string{"match[]": sel}), nil},
		{"query_range_log", func(p *Proxy) http.HandlerFunc { return p.handleQueryRange },
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": sel, "limit": "10"}), nil},
		{"query_range_metric", func(p *Proxy) http.HandlerFunc { return p.handleQueryRange },
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `sum by (level) (count_over_time(` + sel + `[1m]))`, "step": "60"}), nil},
		{"query_range_bare_parser_metric", func(p *Proxy) http.HandlerFunc { return p.handleQueryRange },
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `count_over_time(` + sel + ` | json [5m])`, "step": "60"}), nil},
		{"query_instant_metric", func(p *Proxy) http.HandlerFunc { return p.handleQuery },
			badRequestURL("/loki/api/v1/query", map[string]string{"query": `count_over_time(` + sel + `[5m])`}), nil},
		{"index_stats", func(p *Proxy) http.HandlerFunc { return p.handleIndexStats },
			badRequestURL("/loki/api/v1/index/stats", map[string]string{"query": sel}), nil},
		{"volume", func(p *Proxy) http.HandlerFunc { return p.handleVolume },
			badRequestURL("/loki/api/v1/index/volume", map[string]string{"query": sel}), nil},
		{"volume_range", func(p *Proxy) http.HandlerFunc { return p.handleVolumeRange },
			badRequestURL("/loki/api/v1/index/volume_range", map[string]string{"query": sel, "step": "60", "targetLabels": "app"}), nil},
		{"detected_fields", func(p *Proxy) http.HandlerFunc { return p.handleDetectedFields },
			badRequestURL("/loki/api/v1/detected_fields", map[string]string{"query": sel}), nil},
		{"detected_labels", func(p *Proxy) http.HandlerFunc { return p.handleDetectedLabels },
			badRequestURL("/loki/api/v1/detected_labels", map[string]string{"query": sel}), nil},
		{"detected_field_values", func(p *Proxy) http.HandlerFunc { return p.handleDetectedFieldValues },
			badRequestURL("/loki/api/v1/detected_field/level/values", map[string]string{"query": sel}), nil},
		{"patterns", func(p *Proxy) http.HandlerFunc { return p.handlePatterns },
			badRequestURL("/loki/api/v1/patterns", map[string]string{"query": sel, "step": "60"}), nil},
		{"tail_preflight", func(p *Proxy) http.HandlerFunc { return p.handleTail },
			"/loki/api/v1/tail?query=" + url.QueryEscape(sel), nil},
		{"multi_tenant_labels", func(p *Proxy) http.HandlerFunc { return p.handleLabels },
			badRequestURL("/loki/api/v1/labels", map[string]string{"query": sel}), map[string]string{"X-Scope-OrgID": "tenant-a|tenant-b"}},
		{"multi_tenant_detected_fields", func(p *Proxy) http.HandlerFunc { return p.handleDetectedFields },
			badRequestURL("/loki/api/v1/detected_fields", map[string]string{"query": sel}), map[string]string{"X-Scope-OrgID": "tenant-a|tenant-b"}},
		{"multi_tenant_detected_labels", func(p *Proxy) http.HandlerFunc { return p.handleDetectedLabels },
			badRequestURL("/loki/api/v1/detected_labels", map[string]string{"query": sel}), map[string]string{"X-Scope-OrgID": "tenant-a|tenant-b"}},
		{"multi_tenant_query_range", func(p *Proxy) http.HandlerFunc { return p.handleQueryRange },
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": sel, "limit": "10"}), map[string]string{"X-Scope-OrgID": "tenant-a|tenant-b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vl := newVLBadRequestBackend(t)
			p := newTestProxy(t, vl.URL)
			req := httptest.NewRequest(http.MethodGet, tc.uri, nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			_ = req.ParseForm()
			w := httptest.NewRecorder()
			tc.handler(p)(w, req)
			assertLokiBadData(t, w)
			if len(vl.calls()) == 0 {
				t.Fatalf("expected the backend to be consulted")
			}
			if state := p.breaker.State(); state != "closed" {
				t.Fatalf("circuit breaker state = %q after backend 400, want closed", state)
			}
		})
	}
}

// TestUpstreamBadRequest_TranslationErrorsMapToBadData covers queries the proxy
// rejects before reaching VictoriaLogs on endpoints that used to surface the
// translator error through the upstream-error mapper as 502.
func TestUpstreamBadRequest_TranslationErrorsMapToBadData(t *testing.T) {
	const bad = `{app=`
	cases := []struct {
		name    string
		handler func(p *Proxy) http.HandlerFunc
		uri     string
	}{
		{"labels", func(p *Proxy) http.HandlerFunc { return p.handleLabels },
			badRequestURL("/loki/api/v1/labels", map[string]string{"query": bad})},
		{"label_values", func(p *Proxy) http.HandlerFunc { return p.handleLabelValues },
			badRequestURL("/loki/api/v1/label/app/values", map[string]string{"query": bad})},
		{"label_values_service_name", func(p *Proxy) http.HandlerFunc { return p.handleLabelValues },
			badRequestURL("/loki/api/v1/label/service_name/values", map[string]string{"query": bad})},
		{"series", func(p *Proxy) http.HandlerFunc { return p.handleSeries },
			badRequestURL("/loki/api/v1/series", map[string]string{"match[]": bad})},
		{"index_stats", func(p *Proxy) http.HandlerFunc { return p.handleIndexStats },
			badRequestURL("/loki/api/v1/index/stats", map[string]string{"query": bad})},
		{"volume", func(p *Proxy) http.HandlerFunc { return p.handleVolume },
			badRequestURL("/loki/api/v1/index/volume", map[string]string{"query": bad})},
		{"volume_range", func(p *Proxy) http.HandlerFunc { return p.handleVolumeRange },
			badRequestURL("/loki/api/v1/index/volume_range", map[string]string{"query": bad, "step": "60"})},
		{"detected_fields", func(p *Proxy) http.HandlerFunc { return p.handleDetectedFields },
			badRequestURL("/loki/api/v1/detected_fields", map[string]string{"query": bad})},
		{"detected_labels", func(p *Proxy) http.HandlerFunc { return p.handleDetectedLabels },
			badRequestURL("/loki/api/v1/detected_labels", map[string]string{"query": bad})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// A backend that would succeed: the 400 must come from the query itself.
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"values":[],"hits":[]}`))
			}))
			defer vl.Close()
			p := newTestProxy(t, vl.URL)
			req := httptest.NewRequest(http.MethodGet, tc.uri, nil)
			// The route middleware parses the form before handlers read match[].
			_ = req.ParseForm()
			w := httptest.NewRecorder()
			tc.handler(p)(w, req)
			assertLokiBadData(t, w)
		})
	}
}

// TestUpstreamBadRequest_NeverServesStaleCache: a query the backend rejects is
// wrong for every caller, so a previously cached answer for the same key must
// not mask the 400.
func TestUpstreamBadRequest_NeverServesStaleCache(t *testing.T) {
	const sel = `{app="billing"}`
	cases := []struct {
		name     string
		endpoint string
		handler  func(p *Proxy) http.HandlerFunc
		uri      string
		extra    []string
		payload  string
	}{
		{"index_stats", "index_stats", func(p *Proxy) http.HandlerFunc { return p.handleIndexStats },
			"/loki/api/v1/index/stats?query=" + url.QueryEscape(sel) + "&start=1&end=2", nil,
			`{"streams":1,"chunks":1,"bytes":100,"entries":1}`},
		{"volume", "volume", func(p *Proxy) http.HandlerFunc { return p.handleVolume },
			"/loki/api/v1/index/volume?query=" + url.QueryEscape(sel) + "&start=1&end=2", nil,
			`{"status":"success","data":{"resultType":"vector","result":[]}}`},
		{"volume_range", "volume_range", func(p *Proxy) http.HandlerFunc { return p.handleVolumeRange },
			"/loki/api/v1/index/volume_range?query=" + url.QueryEscape(sel) + "&start=1&end=2&step=60", nil,
			`{"status":"success","data":{"resultType":"matrix","result":[]}}`},
		{"detected_fields", "detected_fields", func(p *Proxy) http.HandlerFunc { return p.handleDetectedFields },
			"/loki/api/v1/detected_fields?query=" + url.QueryEscape(sel), nil,
			`{"status":"success","data":[{"label":"cached_field","type":"string","cardinality":1}],"fields":[{"label":"cached_field","type":"string","cardinality":1}],"limit":1000}`},
		{"detected_labels", "detected_labels", func(p *Proxy) http.HandlerFunc { return p.handleDetectedLabels },
			"/loki/api/v1/detected_labels?query=" + url.QueryEscape(sel), nil,
			`{"status":"success","data":[{"label":"service_name","cardinality":1}],"detectedLabels":[{"label":"service_name","cardinality":1}],"limit":1000}`},
		{"detected_field_values", "detected_field_values", func(p *Proxy) http.HandlerFunc { return p.handleDetectedFieldValues },
			"/loki/api/v1/detected_field/level/values?query=" + url.QueryEscape(sel), []string{"level"},
			`{"status":"success","data":["cached"],"values":["cached"],"limit":1000}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vl := newVLBadRequestBackend(t)
			p := newTestProxy(t, vl.URL)
			req := httptest.NewRequest(http.MethodGet, tc.uri, nil)
			cacheKey := p.canonicalReadCacheKey(tc.endpoint, "", req, tc.extra...)
			p.setEndpointReadCacheWithTTL(tc.endpoint, cacheKey, []byte(tc.payload), time.Millisecond)
			time.Sleep(5 * time.Millisecond)
			if _, _, _, ok := p.staleEndpointCacheEntry(tc.endpoint, cacheKey); !ok {
				t.Fatalf("test setup: expected a stale cache entry for %s", tc.endpoint)
			}

			w := httptest.NewRecorder()
			tc.handler(p)(w, req)
			assertLokiBadData(t, w)
		})
	}
}

// vlBodyUnknownStatsFirst is VictoriaLogs v1.50.0's 422 for a stats function it
// lacks (captured from the e2e stack); only proxy-built LogsQL can cause it.
const vlBodyUnknownStatsFirst = `{"status":"error","errorType":"422","error":"cannot parse ` + "`query`" + ` arg: cannot parse \"stats\" pipe: cannot parse \u0027stats\u0027 entry: unknown stats func \"first\"; context: [illing\" | unpack_logfmt | stats by (_stream) first]; query=app:=\"billing\" | unpack_logfmt | stats by (_stream) first(latency) as c"}`

// rawUnwrapRows writes NDJSON rows for the raw metric evaluator: one latency
// sample every minute over the last hour.
func rawUnwrapRows(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/stream+json")
	now := time.Now().Truncate(time.Minute)
	for i := 1; i <= 50; i++ {
		ts := now.Add(-time.Duration(i) * time.Minute).UTC().Format(time.RFC3339Nano)
		_, _ = fmt.Fprintf(w, `{"_time":%q,"_stream":"{app=\"billing\"}","_msg":"latency=%d","app":"billing","latency":"%d"}`+"\n", ts, i, i)
	}
}

// fallbackBackend answers paths from a table: a status and body for rejected
// paths, rawUnwrapRows for /select/logsql/query, an empty success otherwise. It
// records every call as "path query".
type fallbackBackend struct {
	*httptest.Server
	mu    sync.Mutex
	calls []string
}

func newFallbackBackend(t *testing.T, reject func(r *http.Request, call int) (int, string)) *fallbackBackend {
	t.Helper()
	b := &fallbackBackend{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		b.mu.Lock()
		b.calls = append(b.calls, r.URL.Path+" "+r.Form.Get("query")+" offset="+r.Form.Get("offset"))
		call := len(b.calls)
		b.mu.Unlock()
		if status, body := reject(r, call); status != 0 {
			if strings.HasPrefix(body, "{") {
				w.Header().Set("Content-Type", "application/json")
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		switch r.URL.Path {
		case "/select/logsql/query":
			rawUnwrapRows(w)
		case "/select/logsql/hits":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"hits":[]}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *fallbackBackend) called(path string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, call := range b.calls {
		if strings.HasPrefix(call, path+" ") {
			return true
		}
	}
	return false
}

func (b *fallbackBackend) calledWith(fragment string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, call := range b.calls {
		if strings.Contains(call, fragment) {
			return true
		}
	}
	return false
}

// TestUpstreamBadRequest_OptimisedPathRejectionFallsBack: a VictoriaLogs parse
// rejection of LogsQL the proxy built as an optimisation says nothing about the
// user's query, so the next path runs exactly as before. Only the exact path's
// verdict is returned.
func TestUpstreamBadRequest_OptimisedPathRejectionFallsBack(t *testing.T) {
	t.Run("unwrap_first_last_skip_stats_and_use_raw_evaluator", func(t *testing.T) {
		for _, fn := range []string{"first_over_time", "last_over_time"} {
			vl := newFallbackBackend(t, func(r *http.Request, _ int) (int, string) {
				q := r.Form.Get("query")
				if r.URL.Path == "/select/logsql/stats_query_range" && (strings.Contains(q, "first(") || strings.Contains(q, "last(")) {
					return http.StatusUnprocessableEntity, vlBodyUnknownStatsFirst
				}
				return 0, ""
			})
			p := newTestProxy(t, vl.URL)
			p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
			w := httptest.NewRecorder()
			p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
				badRequestURL("/loki/api/v1/query_range", map[string]string{"query": fn + `({app="billing"} | logfmt | unwrap latency [5m])`, "step": "60"}), nil))
			if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"matrix"`) || strings.Contains(w.Body.String(), `"result":[]`) {
				t.Fatalf("%s: want 200 with samples from the raw evaluator, got %d %s", fn, w.Code, w.Body.String())
			}
			if vl.calledWith("first(") || vl.calledWith("last(") {
				t.Fatalf("%s: sent a stats function VictoriaLogs lacks: %v", fn, vl.calls)
			}
		}
	})

	t.Run("unknown_stats_func_is_not_a_client_error", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		err := p.redactedBackendStatusError("", http.StatusUnprocessableEntity, []byte(vlBodyUnknownStatsFirst))
		if isUpstreamQueryRejected(err) {
			t.Fatalf("an unknown stats function is a proxy translation gap, not an invalid user query")
		}
	})

	t.Run("tryUnwrapViaStatsFastPath_rejection_falls_back_to_raw", func(t *testing.T) {
		vl := newFallbackBackend(t, func(r *http.Request, _ int) (int, string) {
			if r.URL.Path == "/select/logsql/stats_query_range" {
				return http.StatusUnprocessableEntity, vlBodyParseRegexStats
			}
			return 0, ""
		})
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `sum_over_time({app="billing"} | logfmt | unwrap latency [5m])`, "step": "60"}), nil))
		if w.Code != http.StatusOK || !vl.called("/select/logsql/stats_query_range") || !vl.called("/select/logsql/query") {
			t.Fatalf("want stats then raw fallback with 200, got %d calls=%v body=%s", w.Code, vl.calls, w.Body.String())
		}
	})

	t.Run("fetchBareParserStatsBuckets_rejection_falls_back_to_raw", func(t *testing.T) {
		vl := newFallbackBackend(t, func(r *http.Request, _ int) (int, string) {
			if r.URL.Path == "/select/logsql/stats_query_range" {
				return http.StatusUnprocessableEntity, vlBodyParseRegexStats
			}
			return 0, ""
		})
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `count_over_time({app="billing"} | logfmt [5m])`, "step": "60"}), nil))
		if w.Code != http.StatusOK || !vl.called("/select/logsql/stats_query_range") || !vl.called("/select/logsql/query") {
			t.Fatalf("want stats then raw fallback with 200, got %d calls=%v body=%s", w.Code, vl.calls, w.Body.String())
		}
	})

	t.Run("tryBareParserLogRangeBuckets_hits_rejection_falls_back_to_stats_buckets_and_raw", func(t *testing.T) {
		vl := newFallbackBackend(t, func(r *http.Request, _ int) (int, string) {
			switch r.URL.Path {
			case "/select/logsql/hits":
				return http.StatusBadRequest, vlBodyParsePattern
			case "/select/logsql/stats_query_range":
				return http.StatusUnprocessableEntity, vlBodyParseRegexStats
			}
			return 0, ""
		})
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
		p.declaredLabelFields = []string{"app"}
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `count_over_time({app="billing"} | logfmt [5m])`, "step": "60"}), nil))
		for _, path := range []string{"/select/logsql/hits", "/select/logsql/stats_query_range", "/select/logsql/query"} {
			if !vl.called(path) {
				t.Fatalf("fallback chain skipped %s after an optimised-path rejection; calls %v", path, vl.calls)
			}
		}
		if w.Code != http.StatusOK {
			t.Fatalf("want 200 from the raw path, got %d %s", w.Code, w.Body.String())
		}
		// The anchored /hits request carries VictoriaLogs' bucket offset.
		for _, call := range vl.calls {
			if strings.HasPrefix(call, "/select/logsql/hits ") && strings.HasSuffix(call, " offset=") {
				t.Fatalf("the /hits bucket request did not carry an offset: %v", vl.calls)
			}
		}
	})

	t.Run("volume_derived_labels_rejection_falls_back_to_stream_stats", func(t *testing.T) {
		vl := newFallbackBackend(t, func(r *http.Request, call int) (int, string) {
			if r.URL.Path == "/select/logsql/stats_query" && call == 1 {
				return http.StatusBadRequest, vlBodyParsePattern
			}
			return 0, ""
		})
		p := newTestProxy(t, vl.URL)
		w := httptest.NewRecorder()
		p.handleVolume(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/index/volume", map[string]string{"query": `{app="billing"}`, "targetLabels": "service_name"}), nil))
		if w.Code != http.StatusOK || len(vl.calls) != 2 || !strings.Contains(vl.calls[1], "| stats by (_stream) sum_len(_msg) as _b") {
			t.Fatalf("want the derived stats call then the plain _stream bytes fallback with 200, got %d calls=%v body=%s", w.Code, vl.calls, w.Body.String())
		}
		for _, call := range vl.calls {
			if strings.HasPrefix(call, "/select/logsql/hits ") {
				t.Fatalf("volume must never fall back to /hits line counts: %v", vl.calls)
			}
		}
	})

	t.Run("volume_range_stats_rejection_falls_back_to_stream_stats", func(t *testing.T) {
		vl := newFallbackBackend(t, func(r *http.Request, _ int) (int, string) {
			if r.URL.Path == "/select/logsql/stats_query_range" && strings.Contains(r.Form.Get("query"), "filter app:*") {
				return http.StatusUnprocessableEntity, vlBodyParseRegexStats
			}
			return 0, ""
		})
		p := newTestProxy(t, vl.URL)
		w := httptest.NewRecorder()
		p.handleVolumeRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/index/volume_range", map[string]string{"query": `{app="billing"}`, "step": "60", "targetLabels": "app"}), nil))
		if w.Code != http.StatusOK || len(vl.calls) != 2 || vl.called("/select/logsql/hits") {
			t.Fatalf("want 200 from the plain stats fallback, got %d calls=%v body=%s", w.Code, vl.calls, w.Body.String())
		}
	})
}

// TestUpstreamBadRequest_InvalidQueryFailsAfterFallbacks: a genuinely invalid
// query is rejected on every path, so the fallbacks run and the exact path's
// rejection is answered with Loki's 400.
func TestUpstreamBadRequest_InvalidQueryFailsAfterFallbacks(t *testing.T) {
	t.Run("volume_range_optimised_then_plain_stats", func(t *testing.T) {
		vl := newVLBadRequestBackend(t)
		p := newTestProxy(t, vl.URL)
		w := httptest.NewRecorder()
		p.handleVolumeRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/index/volume_range", map[string]string{"query": `{app="billing"}`, "step": "60", "targetLabels": "app"}), nil))
		assertLokiBadData(t, w)
		if calls := vl.calls(); len(calls) != 2 || calls[0] != "/select/logsql/stats_query_range" || calls[1] != "/select/logsql/stats_query_range" {
			t.Fatalf("want the optimised stats query and the plain stats fallback, got %v", calls)
		}
	})
	t.Run("tryBareParserLogRangeBuckets_hits_stats_then_raw", func(t *testing.T) {
		vl := newVLBadRequestBackend(t)
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
		p.declaredLabelFields = []string{"app"}
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `count_over_time({app="billing"} | logfmt [5m])`, "step": "60"}), nil))
		assertLokiBadData(t, w)
		calls := strings.Join(vl.calls(), " ")
		for _, path := range []string{"/select/logsql/hits", "/select/logsql/stats_query_range", "/select/logsql/query"} {
			if !strings.Contains(calls, path) {
				t.Fatalf("want %s in the fallback chain, got %v", path, vl.calls())
			}
		}
	})
	t.Run("tryUnwrapViaStatsFastPath_stats_then_raw", func(t *testing.T) {
		vl := newVLBadRequestBackend(t)
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `sum_over_time({app="billing"} | logfmt | unwrap latency [5m])`, "step": "60"}), nil))
		assertLokiBadData(t, w)
		calls := strings.Join(vl.calls(), " ")
		if !strings.Contains(calls, "/select/logsql/stats_query_range") || !strings.Contains(calls, "/select/logsql/query") {
			t.Fatalf("want the stats fast path and the raw fallback, got %v", vl.calls())
		}
	})
}

// Real VictoriaLogs v1.50.0 error bodies, captured from the e2e stack or built
// from the format strings cited in classifyVLError's marker lists.
const (
	// Plain-text 400 (httpserver.Errorf) for a query that does not parse.
	vlBodyParsePattern = "cannot parse `query` arg: cannot parse \"extract\" pipe: cannot parse 'pattern' \"<_>\": pattern \"<_>\" must contain at least a single named field in the form <field_name>; context: [app:=\"billing-secret-value\" | extract \"<_>\"]; query=app:=\"billing-secret-value\" | extract \"<_>\""
	// JSON 422 (httpserver.SendPrometheusError) from stats_query_range.
	vlBodyParseRegexStats = `{"status":"error","errorType":"422","error":"cannot parse ` + "`query`" + ` arg: invalid regexp \"app\":\"(billing-secret-value\": error parsing regexp: missing closing ): ` + "`(billing-secret-value`" + `; context: [app:~\"(billing-secret-value\" |]; query=app:~\"(billing-secret-value\" | stats count() c"}`
	vlBodyEmptyQuery      = "`query` arg cannot be empty"
	vlBodyZeroStep        = `{"status":"error","errorType":"422","error":"'step' must be bigger than zero"}`
	vlBodyBadStepArg      = `{"status":"error","errorType":"422","error":"cannot parse duration from the arg 'step=abc'"}`
	vlBodyBadStart        = `cannot parse start=not-a-time: cannot parse duration "not-a-time"`
	vlBodyMissingField    = "missing 'field' query arg"
	// Limit and execution failures (lib/logstorage/pipe_stats.go:1123,
	// pipe_stream_context.go:181, storage_search.go:427, app/vlselect/logsql/logsql.go:113, 1577).
	vlBodyStatsMemory     = `{"status":"error","errorType":"422","error":"cannot execute query [app:=\"billing-secret-value\" | stats by (trace_id) count(*) as c]: cannot calculate [stats by (trace_id) count(*) as c], since it requires more than 1024MB of memory"}`
	vlBodyContextMemory   = "cannot execute query [app:=\"billing\" | stream_context before 10]: more than 1024MB of memory is needed for fetching the surrounding logs for 5000 matching logs"
	vlBodyRowsMemory      = "cannot execute query [app:=\"billing\"]: cannot load rows for [app:=\"billing\"] because they occupy more than 512MB of memory"
	vlBodyMaxQueryLen     = "the `query` arg length cannot exceed -search.maxQueryLen=16384 bytes; the current query length is 17000 bytes; query=xxxx"
	vlBodyMaxTimeRange    = "too big time range selected: [2026-09-01T00:00:00Z, 2026-09-15T00:00:00Z]; it cannot exceed -search.maxQueryTimeRange=1d; see https://docs.victoriametrics.com/victorialogs/querying/#resource-usage-limits"
	vlBodyMetadataFailure = "cannot obtain field names with filter=\"\": cannot read index: unexpected EOF"
	// A parse error whose echoed query quotes a limit marker is still a parse error.
	vlBodyParseEchoingLimit = "cannot parse `query` arg: unexpected token after [app:=\"x\"]: \")\"; expecting '|' or ')'; query=app:=\"since it requires more than\")"
	vlBodyUnsupportedPath   = `unsupported path requested: "/select/logsql/stream_field_names"`
	vlBodyUnknown           = "error from storage node: unexpected response"
)

func TestClassifyVLError_RealVictoriaLogsTexts(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   vlErrorClass
	}{
		{"parse_pattern_400", http.StatusBadRequest, vlBodyParsePattern, vlErrorQueryRejected},
		{"parse_regex_stats_422", http.StatusUnprocessableEntity, vlBodyParseRegexStats, vlErrorQueryRejected},
		{"empty_query", http.StatusBadRequest, vlBodyEmptyQuery, vlErrorQueryRejected},
		{"zero_step", http.StatusUnprocessableEntity, vlBodyZeroStep, vlErrorQueryRejected},
		{"bad_step_arg", http.StatusUnprocessableEntity, vlBodyBadStepArg, vlErrorQueryRejected},
		{"bad_start_arg", http.StatusBadRequest, vlBodyBadStart, vlErrorQueryRejected},
		{"missing_field_arg", http.StatusBadRequest, vlBodyMissingField, vlErrorQueryRejected},
		{"parse_echoing_limit_text", http.StatusBadRequest, vlBodyParseEchoingLimit, vlErrorQueryRejected},
		{"stats_memory_limit", http.StatusUnprocessableEntity, vlBodyStatsMemory, vlErrorResourceLimit},
		{"stream_context_memory", http.StatusBadRequest, vlBodyContextMemory, vlErrorResourceLimit},
		{"rows_memory", http.StatusBadRequest, vlBodyRowsMemory, vlErrorResourceLimit},
		{"max_query_len", http.StatusBadRequest, vlBodyMaxQueryLen, vlErrorResourceLimit},
		{"max_time_range", http.StatusBadRequest, vlBodyMaxTimeRange, vlErrorResourceLimit},
		{"metadata_execution_failure", http.StatusBadRequest, vlBodyMetadataFailure, vlErrorResourceLimit},
		{"unsupported_path", http.StatusBadRequest, vlBodyUnsupportedPath, vlErrorUnsupportedPath},
		{"unknown_400", http.StatusBadRequest, vlBodyUnknown, vlErrorUnclassified},
		{"parse_text_on_5xx_is_not_classified", http.StatusBadGateway, vlBodyParsePattern, vlErrorUnclassified},
	}
	p := newTestProxy(t, "http://unused")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := p.redactedBackendStatusError("", tc.status, []byte(tc.body))
			var typed *upstreamStatusError
			if !errors.As(err, &typed) {
				t.Fatalf("expected upstreamStatusError, got %T", err)
			}
			if typed.class != tc.want {
				t.Fatalf("class = %d, want %d for %q", typed.class, tc.want, tc.body)
			}
			// Classification must not depend on unredacted text reaching clients.
			if strings.Contains(err.Error(), "billing-secret-value") {
				t.Fatalf("redaction lost: %s", err.Error())
			}
		})
	}
}

// TestUpstreamBadRequest_GrafanaStatsQueries: a query VictoriaLogs rejects is
// Loki's 400 for every client, Grafana included. A resource-limit failure or an
// unrecognised 400 keeps the pre-existing handling: the partial-results reply
// for Grafana-sourced stats queries, the backend status for direct API clients
// on the stats paths, and 502 on the manual parser-metric path.
func TestUpstreamBadRequest_GrafanaStatsQueries(t *testing.T) {
	clients := []struct {
		name    string
		headers map[string]string
		grafana bool
	}{
		{"api", nil, false},
		{"grafana_explore", map[string]string{"User-Agent": "Grafana/12.1.0", "X-Grafana-Org-Id": "1", "X-Query-Tags": "Source=grafana-explore"}, true},
		{"grafana_drilldown", map[string]string{"User-Agent": "Grafana/12.1.0", "X-Grafana-Org-Id": "1", "X-Query-Tags": "Source=grafana-lokiexplore-app"}, true},
	}
	bodies := []struct {
		name     string
		status   int
		body     string
		rejected bool
	}{
		{"parse_400", http.StatusBadRequest, vlBodyParsePattern, true},
		{"parse_422", http.StatusUnprocessableEntity, vlBodyParseRegexStats, true},
		{"memory_limit_422", http.StatusUnprocessableEntity, vlBodyStatsMemory, false},
		{"rows_memory_400", http.StatusBadRequest, vlBodyRowsMemory, false},
		{"storage_eof_400", http.StatusBadRequest, vlBodyMetadataFailure, false},
		{"unknown_400", http.StatusBadRequest, vlBodyUnknown, false},
	}
	queries := []struct {
		query string
		// partial reports whether the path has the Grafana partial-results
		// reply; the manual parser-metric path never had one.
		partial bool
	}{
		{`sum(count_over_time({app="billing"}[1m]))`, true},
		{`sum by (level) (count_over_time({app="billing"}[1m]))`, true},
		{`count_over_time({app="billing"}[1m])`, true},
		{`sum by (level) (count_over_time({app="billing"} | json [1m]))`, false},
	}
	for _, body := range bodies {
		for _, query := range queries {
			for _, client := range clients {
				t.Run(body.name+"/"+client.name+"/"+query.query, func(t *testing.T) {
					vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if strings.HasPrefix(strings.TrimSpace(body.body), "{") {
							w.Header().Set("Content-Type", "application/json")
						}
						w.WriteHeader(body.status)
						_, _ = w.Write([]byte(body.body))
					}))
					defer vl.Close()
					p := newTestProxy(t, vl.URL)
					// A known v1.45+ backend: tumbling stats buckets for any start.
					p.storeBackendVersion("v1.50.0", "v1.50.0")
					req := httptest.NewRequest(http.MethodGet, badRequestURL("/loki/api/v1/query_range", map[string]string{"query": query.query, "step": "60"}), nil)
					for k, v := range client.headers {
						req.Header.Set(k, v)
					}
					w := httptest.NewRecorder()
					p.handleQueryRange(w, req)
					switch {
					case body.rejected:
						assertLokiBadData(t, w)
						return
					case !query.partial:
						if w.Code != http.StatusBadGateway {
							t.Fatalf("status = %d, want 502 as before; body=%s", w.Code, w.Body.String())
						}
						return
					case !client.grafana:
						if w.Code != body.status {
							t.Fatalf("status = %d, want backend status %d as before; body=%s", w.Code, body.status, w.Body.String())
						}
						return
					}
					if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"warnings"`) {
						t.Fatalf("expected partial-results reply, got %d body=%s", w.Code, w.Body.String())
					}
					if strings.Contains(w.Body.String(), "billing-secret-value") {
						t.Fatalf("backend query echo leaked in partial reply: %s", w.Body.String())
					}
				})
			}
		}
	}
}

func TestStatusFromUpstreamErr_ClientMistakes(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	vlErr := func(status int, body string) error {
		return p.redactedBackendStatusError("", status, []byte(body))
	}
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"backend_400", vlErr(http.StatusBadRequest, vlBodyParsePattern), http.StatusBadRequest},
		{"backend_422", vlErr(http.StatusUnprocessableEntity, vlBodyParseRegexStats), http.StatusBadRequest},
		// Limit, execution and unrecognised 400s are backend failures, not client mistakes.
		{"backend_400_limit", vlErr(http.StatusBadRequest, vlBodyMaxTimeRange), http.StatusBadGateway},
		{"backend_422_memory", vlErr(http.StatusUnprocessableEntity, vlBodyStatsMemory), http.StatusBadGateway},
		{"backend_400_storage_eof", vlErr(http.StatusBadRequest, vlBodyMetadataFailure), http.StatusBadGateway},
		{"backend_400_unknown", vlErr(http.StatusBadRequest, vlBodyUnknown), http.StatusBadGateway},
		{"wrapped_backend_400", fmt.Errorf("left query: %w", vlErr(http.StatusBadRequest, vlBodyEmptyQuery)), http.StatusBadRequest},
		{"translator_parse_error", &translator.ParseError{Msg: "unmatched '{' in stream selector", Pos: -1}, http.StatusBadRequest},
		// Valid LogQL the translator cannot express (Loki accepts it): not a client mistake.
		{"translator_unsupported", &translator.UnsupportedError{Msg: "unsupported"}, http.StatusBadGateway},
		// Unchanged mappings. Older VictoriaLogs answers a missing endpoint with 400.
		{"backend_unsupported_path", vlErr(http.StatusBadRequest, vlBodyUnsupportedPath), http.StatusBadGateway},
		{"backend_500", vlErr(http.StatusInternalServerError, "boom"), http.StatusBadGateway},
		{"backend_503", vlErr(http.StatusServiceUnavailable, "too many concurrent queries"), http.StatusBadGateway},
		{"backend_429", vlErr(http.StatusTooManyRequests, "slow down"), http.StatusBadGateway},
		{"backend_timeout_text", vlErr(http.StatusServiceUnavailable, "timeout exceeded"), http.StatusGatewayTimeout},
		{"circuit_breaker", errors.New("circuit breaker open — backend unavailable"), http.StatusServiceUnavailable},
		{"transport", errors.New("dial tcp: connection refused"), http.StatusBadGateway},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusFromUpstreamErr(tc.err); got != tc.want {
				t.Fatalf("statusFromUpstreamErr(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
	if !isUpstreamQueryRejected(vlErr(http.StatusUnprocessableEntity, vlBodyParseRegexStats)) ||
		isUpstreamQueryRejected(vlErr(http.StatusUnprocessableEntity, vlBodyStatsMemory)) ||
		isUpstreamQueryRejected(vlErr(http.StatusBadRequest, vlBodyUnknown)) {
		t.Fatal("isUpstreamQueryRejected must match only parse and argument errors")
	}
	if shouldFallbackToGenericMetadata(vlErr(http.StatusBadRequest, vlBodyParsePattern)) {
		t.Fatal("a rejected query must not be retried on the generic metadata endpoint")
	}
	if !shouldFallbackToGenericMetadata(vlErr(http.StatusBadRequest, vlBodyUnsupportedPath)) {
		t.Fatal("an endpoint missing on older VictoriaLogs must still fall back to the generic metadata endpoint")
	}
	if shouldRecordBreakerFailure(context.Background(), vlErr(http.StatusBadRequest, vlBodyParsePattern)) {
		t.Fatal("a backend 400 must not count as a circuit-breaker failure")
	}
	if shouldRetryQueryRangeWindow(vlErr(http.StatusBadRequest, vlBodyParsePattern)) {
		t.Fatal("a backend 400 must not be retried")
	}
}

func TestRedactedBackendStatusError_IsTypedAndRedacted(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	err := p.redactedBackendStatusError("stats_query_range", http.StatusBadRequest, []byte(vlParseErrorBody))
	var typed *upstreamStatusError
	if !errors.As(err, &typed) || typed.StatusCode() != http.StatusBadRequest {
		t.Fatalf("expected typed upstream status error with 400, got %T %v", err, err)
	}
	if strings.Contains(err.Error(), "billing-secret-value") {
		t.Fatalf("redaction lost: %s", err.Error())
	}
	if !strings.HasPrefix(err.Error(), "stats_query_range 400: ") {
		t.Fatalf("message format changed: %s", err.Error())
	}
}

// TestUpstreamBadRequest_LimitErrorsKeepBackendFailureHandling: VictoriaLogs
// answers resource limits and storage failures with the same 400 as a parse
// error. Those must keep the backend-failure behaviour: fast-path fallbacks
// run, stale answers are served, and they never become a client 400.
func TestUpstreamBadRequest_LimitErrorsKeepBackendFailureHandling(t *testing.T) {
	t.Run("hits_memory_limit_falls_back_to_stats_and_raw_fetch", func(t *testing.T) {
		var mu sync.Mutex
		var calls []string
		vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls = append(calls, r.URL.Path)
			mu.Unlock()
			switch r.URL.Path {
			case "/select/logsql/hits", "/select/logsql/stats_query_range":
				http.Error(w, vlBodyRowsMemory, http.StatusBadRequest)
			case "/select/logsql/query":
				w.Header().Set("Content-Type", "application/stream+json")
			default:
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"values":[]}`))
			}
		}))
		defer vl.Close()
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
		p.declaredLabelFields = []string{"app"}
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet,
			badRequestURL("/loki/api/v1/query_range", map[string]string{"query": `count_over_time({app="billing"} | logfmt [5m])`, "step": "60"}), nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want the raw-fetch fallback to answer; body=%s", w.Code, w.Body.String())
		}
		mu.Lock()
		defer mu.Unlock()
		seen := map[string]bool{}
		for _, path := range calls {
			seen[path] = true
		}
		for _, path := range []string{"/select/logsql/hits", "/select/logsql/stats_query_range", "/select/logsql/query"} {
			if !seen[path] {
				t.Fatalf("fallback chain skipped %s after a limit 400; calls %v", path, calls)
			}
		}
	})

	t.Run("limit_error_serves_stale_cache", func(t *testing.T) {
		vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, vlBodyRowsMemory, http.StatusBadRequest)
		}))
		defer vl.Close()
		p := newTestProxy(t, vl.URL)
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/stats?query="+url.QueryEscape(`{app="billing"}`)+"&start=1&end=2", nil)
		cacheKey := p.canonicalReadCacheKey("index_stats", "", req)
		const cached = `{"streams":1,"chunks":1,"bytes":100,"entries":1}`
		p.setEndpointReadCacheWithTTL("index_stats", cacheKey, []byte(cached), time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		w := httptest.NewRecorder()
		p.handleIndexStats(w, req)
		if w.Code != http.StatusOK || w.Body.String() != cached {
			t.Fatalf("expected the stale answer after a limit 400, got %d %s", w.Code, w.Body.String())
		}
	})

	t.Run("storage_eof_is_not_a_client_error", func(t *testing.T) {
		vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, vlBodyMetadataFailure, http.StatusBadRequest)
		}))
		defer vl.Close()
		p := newTestProxy(t, vl.URL)
		for _, uri := range []string{
			badRequestURL("/loki/api/v1/labels", map[string]string{"query": `{app="billing"}`}),
			badRequestURL("/loki/api/v1/label/app/values", map[string]string{"query": `{app="billing"}`}),
			badRequestURL("/loki/api/v1/index/volume", map[string]string{"query": `{app="billing"}`}),
		} {
			req := httptest.NewRequest(http.MethodGet, uri, nil)
			w := httptest.NewRecorder()
			if strings.Contains(uri, "/label/") {
				p.handleLabelValues(w, req)
			} else if strings.Contains(uri, "/volume") {
				p.handleVolume(w, req)
			} else {
				p.handleLabels(w, req)
			}
			if w.Code != http.StatusBadGateway {
				t.Fatalf("%s: status = %d, want 502 for a storage failure; body=%s", uri, w.Code, w.Body.String())
			}
		}
	})

	t.Run("multi_tenant_limit_on_one_tenant_fails_whole_request", func(t *testing.T) {
		vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Scope-OrgID") == "tenant-b" {
				http.Error(w, vlBodyRowsMemory, http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"billing\"}","hits":1}]}`))
		}))
		defer vl.Close()
		p := newTestProxy(t, vl.URL)
		p.forwardTenantHeader = true
		serve := func(orgID string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, badRequestURL("/loki/api/v1/series", map[string]string{"match[]": `{app="billing"}`}), nil)
			req.Header.Set("X-Scope-OrgID", orgID)
			_ = req.ParseForm()
			w := httptest.NewRecorder()
			p.handleSeries(w, req)
			return w
		}
		single := serve("tenant-b")
		if single.Code < http.StatusBadRequest {
			t.Fatalf("single-tenant limit failure answered %d %s", single.Code, single.Body.String())
		}
		// Loki fails the whole multi-tenant request when one tenant fails.
		w := serve("tenant-a|tenant-b")
		if want := multiTenantFailureStatus(single.Code); w.Code != want || strings.Contains(w.Body.String(), "billing\"}") {
			t.Fatalf("expected the whole request to fail with %d, got %d body=%s", want, w.Code, w.Body.String())
		}
	})
}

func TestRedactBackendError_DropsVictoriaLogsQueryEchoes(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	cases := []struct {
		name string
		body string
		keep string
	}{
		{"execute_and_calculate", `cannot execute query [app:="billing" AND user_email:="bob@corp.io" | stats by (trace_id) count(*) as c]: cannot calculate [stats by (trace_id) count(*) as c], since it requires more than 1024MB of memory`, "since it requires more than 1024MB of memory"},
		{"execute_json_422", `{"status":"error","errorType":"422","error":"cannot execute query [app:=\"billing\" AND user_email:=\"bob@corp.io\" | stats count() c]: cannot calculate [stats by (user_email) count(*) as c], since it requires more than 512MB of memory"}`, "since it requires more than 512MB of memory"},
		{"load_rows", `cannot execute query [app:="billing" AND user_email:="bob@corp.io"]: cannot load rows for [app:="billing" AND user_email:="bob@corp.io"] because they occupy more than 512MB of memory`, "because they occupy more than 512MB of memory"},
		{"metadata_filter", `cannot obtain field names with filter="app:=\"billing\" AND user_email:=\"bob@corp.io\"": cannot read index: unexpected EOF`, "unexpected EOF"},
		{"values_filter", `cannot obtain values for field "user_email" with filter "app:=\"billing\" AND user_email:=\"bob@corp.io\"": cannot read index: unexpected EOF`, "unexpected EOF"},
		{"tail", `the query [app:="billing" AND user_email:="bob@corp.io" | stats count()] cannot be used in live tailing; see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing for details`, "cannot be used in live tailing"},
		// The echoed query contains the terminators themselves.
		{"line_filter_bracket_colon", `cannot execute query [app:="billing" AND "[ERROR]: " AND user_email:="bob@corp.io" | stats count() c]: cannot calculate [stats count(*) as c], since it requires more than 512MB of memory`, "since it requires more than 512MB of memory"},
		{"line_filter_bracket_comma", `cannot execute query [app:="billing" AND "[WARN], " AND user_email:="bob@corp.io" | stats count() c]: cannot calculate [stats count(*) as c], since it requires more than 512MB of memory`, "since it requires more than 512MB of memory"},
		{"line_filter_bracket_because", `cannot execute query [app:="billing" AND "[x] because" AND user_email:="bob@corp.io"]: cannot load rows for [app:="billing" AND "[x] because" AND user_email:="bob@corp.io"] because they occupy more than 512MB of memory`, "because they occupy more than 512MB of memory"},
		{"nested_brackets_in_quotes", `cannot execute query [app:="billing" AND "a[b[c]]: d]," AND user_email:="bob@corp.io" | stats by (x) count() c]: cannot calculate [stats by (x) count(*) as c], since it requires more than 256MB of memory`, "since it requires more than 256MB of memory"},
		{"pipe_echo_with_bracket_colon", `cannot execute query [app:="billing" | filter user_email:="bob@corp.io"]: cannot calculate [filter "[ERROR]: " user_email:="bob@corp.io" | stats count(*) as c], since it requires more than 128MB of memory`, "since it requires more than 128MB of memory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := p.redactBackendError([]byte(tc.body))
			for _, leak := range []string{"billing", "bob@corp.io", "user_email:="} {
				if strings.Contains(got, leak) {
					t.Fatalf("redacted message still leaks %q: %s", leak, got)
				}
			}
			if !strings.Contains(got, tc.keep) {
				t.Fatalf("redaction removed the failure reason %q: %s", tc.keep, got)
			}
		})
	}
}
