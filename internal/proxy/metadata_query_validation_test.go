package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

const lokiEmptyCompatibleMsg = `parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~".*" does not meet this requirement, but app=~".+" will`

// Loki 3.7.1 answers these metadata requests with HTTP 400 bad_data before
// touching storage; the proxy must do the same before cache, fan-out or any
// VictoriaLogs call.
func TestMetadataQueryValidation_RejectsLikeLoki(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	type probe struct {
		path, param, value, want string
	}
	var cases []probe
	matcherEndpoints := []struct{ path, param string }{
		{"/loki/api/v1/labels", "query"},
		{"/loki/api/v1/label/pod/values", "query"},
		{"/loki/api/v1/series", "match[]"},
		{"/loki/api/v1/index/stats", "query"},
		{"/loki/api/v1/index/volume", "query"},
		{"/loki/api/v1/index/volume_range", "query"},
		{"/loki/api/v1/patterns", "query"},
		{"/loki/api/v1/detected_labels", "query"},
	}
	for _, ep := range matcherEndpoints {
		for _, sel := range []string{`{app=""}`, `{app=~""}`, `{app=~".*"}`, `{app!="x"}`, `{app!~"x"}`} {
			cases = append(cases, probe{ep.path, ep.param, sel, lokiEmptyCompatibleMsg})
		}
		for _, q := range []string{`{app="a"} |= "foo"`, `{app="a"} | logfmt`, `count_over_time({app="a"}[5m])`} {
			cases = append(cases, probe{ep.path, ep.param, q, "only label matchers are supported"})
		}
	}
	for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label/pod/values", "/loki/api/v1/index/stats", "/loki/api/v1/patterns", "/loki/api/v1/detected_labels"} {
		cases = append(cases, probe{path, "query", `{}`, lokiEmptyCompatibleMsg})
	}
	for _, path := range []string{"/loki/api/v1/index/volume", "/loki/api/v1/index/volume_range"} {
		cases = append(cases, probe{path, "query", `{ }`, lokiEmptyCompatibleMsg})
	}
	for _, path := range []string{"/loki/api/v1/index/stats", "/loki/api/v1/index/volume", "/loki/api/v1/index/volume_range", "/loki/api/v1/patterns", "/loki/api/v1/detected_fields", "/loki/api/v1/detected_field/pod/values"} {
		cases = append(cases, probe{path, "", "", "parse error : syntax error: unexpected $end"})
	}
	for _, path := range []string{"/loki/api/v1/detected_fields", "/loki/api/v1/detected_field/pod/values"} {
		for _, sel := range []string{`{app=""}`, `{app=~".*"}`, `{}`} {
			cases = append(cases, probe{path, "query", sel, lokiEmptyCompatibleMsg})
		}
		cases = append(cases,
			probe{path, "query", `count_over_time({app="a"}[5m])`, "only log selector is supported"},
			probe{path, "query", `{app="a"} | json foo="bar["`, `parse error : stage '| json foo="bar["' : cannot parse expression [bar[]: syntax error: unexpected $end, expecting STRING or INDEX`},
		)
	}
	for _, sel := range []string{`{app=""}`, `{app=~".*"}`, `{app!="x"}`, `{}`} {
		cases = append(cases, probe{"/loki/api/v1/tail", "query", sel, lokiEmptyCompatibleMsg})
	}
	// Loki reads `match` as well as `match[]` on /series.
	cases = append(cases,
		probe{"/loki/api/v1/series", "match", `{app=""}`, lokiEmptyCompatibleMsg},
		probe{"/loki/api/v1/series", "match", `{app="a"} |= "x"`, "only label matchers are supported"},
	)

	// {} is "all series" only as the single match; next to another group it is
	// Loki's "0 matchers in group" (logql.MatchForSeriesRequest).
	multi := url.Values{"start": {"1700000000"}, "end": {"1700003600"}, "match[]": {`{ }`, `{app="a"}`}}
	w := doCompatProxyRequest(p, "/loki/api/v1/series?"+multi.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if !strings.Contains(w.Body.String(), `"error":"0 matchers in group: { }"`) || w.Code != http.StatusBadRequest {
		t.Fatalf("series {} next to another group: status=%d body=%s", w.Code, w.Body)
	}

	for _, tc := range cases {
		params := url.Values{"start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}
		if tc.param != "" {
			params.Set(tc.param, tc.value)
		}
		target := tc.path + "?" + params.Encode()
		t.Run(target, func(t *testing.T) {
			w := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
			var body struct{ Status, ErrorType, Error string }
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusBadRequest || body.ErrorType != "bad_data" || body.Error != tc.want {
				t.Fatalf("status=%d body=%s, want 400 bad_data %q", w.Code, w.Body, tc.want)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected metadata requests reached the backend %d times", calls.Load())
	}
}

// Near misses Loki accepts must pass validation unchanged, including the
// selectors Grafana Logs Drilldown sends ({service_name=~".+"}).
func TestMetadataQueryValidation_AcceptsLikeLoki(t *testing.T) {
	cases := []struct {
		endpoint string
		params   url.Values
	}{
		{"labels", url.Values{}},
		{"labels", url.Values{"query": {`{app=~".+"}`}}},
		{"labels", url.Values{"query": {`{app="", pod="a"}`}}},
		{"labels", url.Values{"query": {`*`}}},
		{"label_values", url.Values{"query": {`{service_name=~".+"}`}}},
		{"detected_labels", url.Values{}},
		{"detected_labels", url.Values{"query": {`{service_name="api", pod=~".*"}`}}},
		{"series", url.Values{}},
		{"series", url.Values{"match[]": {`{}`}}},
		{"series", url.Values{"match[]": {`{ }`}}},
		{"series", url.Values{"match[]": {`{}`, `{}`}}},
		{"series", url.Values{"match[]": {`{app="a"}`}, "match": {`{pod="b"}`}}},
		{"series", url.Values{"match": {`{app="", pod="b"}`}}},
		{"index_stats", url.Values{"query": {`{service_name=~".+"}`}}},
		{"volume", url.Values{"query": {`{}`}}},
		{"volume_range", url.Values{"query": {`{service_name=~".+"}`}}},
		{"patterns", url.Values{"query": {`{service_name="api"}`}}},
		{"detected_fields", url.Values{"query": {`{service_name="api"} | json | logfmt | drop __error__, __error_details__`}}},
		{"detected_field_values", url.Values{"query": {`{service_name="api"} |= "foo"`}}},
		{"tail", url.Values{"query": {`{app="a"} | json foo="a.b"`}}},
		{"query_range", url.Values{}},
	}
	for _, tc := range cases {
		t.Run(tc.endpoint+"?"+tc.params.Encode(), func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/?"+tc.params.Encode(), nil)
			rule := lokiQueryParamRules[tc.endpoint]
			if msg := lokiQueryParamError(rule, r, DefaultMaxQueryLengthBytes); msg != "" {
				t.Fatalf("rejected: %s", msg)
			}
		})
	}
}

// Oversized selector parameters are rejected before they are parsed, cached or
// sent upstream: Loki's "input size too long" at or above 128 KiB, the
// proxy's 64 KiB query limit below that. Echoed query text is truncated.
func TestMetadataQueryValidation_OversizedInputs(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	// Loki parses anything below syntax.maxInputSize (128 KiB), so the proxy
	// rejects only what Loki rejects. -max-query-length-bytes can lower that;
	// TestQueryLength_ConfiguredLimitRejectsWhatLokiWouldParse covers it.
	huge := `{app="` + strings.Repeat("x", 131072) + `"}`
	hugeMsg := "parse error : input size too long (131080 > 131072)"
	cases := []struct{ path, param, value, want string }{
		{"/loki/api/v1/labels", "query", huge, hugeMsg},
		{"/loki/api/v1/label/app/values", "query", huge, hugeMsg},
		{"/loki/api/v1/series", "match[]", huge, hugeMsg},
		{"/loki/api/v1/series", "match", huge, hugeMsg},
		{"/loki/api/v1/index/volume", "query", huge, hugeMsg},
		{"/loki/api/v1/detected_fields", "query", huge, hugeMsg},
		{"/loki/api/v1/tail", "query", huge, hugeMsg},
		{"/loki/api/v1/query_range", "query", huge, hugeMsg},
		{"/loki/api/v1/query", "query", huge, hugeMsg},
	}
	before := validationCacheSize.Load()
	for _, tc := range cases {
		t.Run(tc.path+"/"+tc.param+"/"+tc.want, func(t *testing.T) {
			// POST form bodies carry inputs that do not fit in a URL.
			body := url.Values{tc.param: {tc.value}, "start": {"1700000000"}, "end": {"1700003600"}}.Encode()
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("X-Scope-OrgID", "0")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			var got struct{ ErrorType, Error string }
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusBadRequest || got.ErrorType != "bad_data" || got.Error != tc.want {
				t.Fatalf("status=%d body=%.300s, want 400 %q", w.Code, w.Body, tc.want)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("oversized inputs reached the backend %d times", calls.Load())
	}
	if after := validationCacheSize.Load(); after != before {
		t.Fatalf("oversized inputs were cached: cache size %d -> %d", before, after)
	}
}

// Validation results are cached only for short queries, so the cache stays
// bounded to validationCacheMaxSize entries of at most
// validationCacheMaxQueryBytes of query text each.
func TestValidationCacheSkipsLongQueries(t *testing.T) {
	long := `{app="` + strings.Repeat("y", validationCacheMaxQueryBytes) + `"} |= "a"`
	key := "\x00matchers\x00" + long
	if msg := cachedSelectorValidation("matchers", long, func(string) string { return "rejected" }); msg != "rejected" {
		t.Fatalf("msg=%q", msg)
	}
	if _, ok := validationCache.Load(key); ok {
		t.Fatal("long selector validation was cached")
	}
	if validateLogQLSyntax(long) != "" {
		t.Fatal("valid long query rejected")
	}
	if _, ok := validationCache.Load(long); ok {
		t.Fatal("long query validation was cached")
	}
	short := `{app="cache-short-probe"}`
	_ = cachedSelectorValidation("matchers", short, func(string) string { return "" })
	if _, ok := validationCache.Load("\x00matchers\x00" + short); !ok && validationCacheSize.Load() < validationCacheMaxSize {
		t.Fatal("short selector validation was not cached")
	}
}

// A validation error that echoes a large part of the query is truncated in
// the response body.
func TestMetadataQueryValidation_TruncatesEchoedQuery(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	template := strings.Repeat("{{ .x }}", 2000) + "{{"
	params := url.Values{"query": {`{app="a"} | label_format y="` + template + `"`}, "limit": {"10"}}
	w := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	var got struct{ Error string }
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%.300s", w.Code, w.Body)
	}
	if len(got.Error) > maxEchoedErrorBytes+len("... (truncated)") || !strings.HasSuffix(got.Error, "... (truncated)") {
		t.Fatalf("error not truncated: %d bytes", len(got.Error))
	}
}

// The proxy accepts every query Loki parses; an operator who wants a tighter
// bound sets -max-query-length-bytes, and the error names the flag.
func TestQueryLength_ConfiguredLimitRejectsWhatLokiWouldParse(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	defer backend.Close()

	large := `{app="` + strings.Repeat("x", 70000) + `"}`
	defaults, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Millisecond, 10), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = defaults.Shutdown(context.Background()) })
	if msg := queryLengthError(large, defaults.limits().QueryLengthBytes); msg != "" {
		t.Fatalf("default limit rejected a query Loki parses: %s", msg)
	}

	limited, err := New(Config{
		BackendURL:      backend.URL,
		Cache:           cache.New(time.Millisecond, 10),
		LogLevel:        "error",
		ExecutionLimits: ExecutionLimitsConfig{MaxQueryLengthBytes: 65536},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = limited.Shutdown(context.Background()) })
	msg := queryLengthError(large, limited.limits().QueryLengthBytes)
	if !strings.Contains(msg, "query exceeds max length (70008 > 65536)") || !strings.Contains(msg, "-max-query-length-bytes") {
		t.Fatalf("configured limit error = %q", msg)
	}
}
