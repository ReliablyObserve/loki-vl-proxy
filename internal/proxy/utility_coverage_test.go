package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestParseInstantVectorTimeVariants(t *testing.T) {
	rfc := "2026-04-06T20:21:22.123456789Z"
	if got := parseInstantVectorTime(rfc); got != time.Date(2026, 4, 6, 20, 21, 22, 123456789, time.UTC).UnixNano() {
		t.Fatalf("unexpected RFC3339Nano timestamp: %d", got)
	}
	if got := parseInstantVectorTime("1712434882"); got != 1712434882*int64(time.Second) {
		t.Fatalf("unexpected integer seconds timestamp: %d", got)
	}
	if got := parseInstantVectorTime("1712434882.5"); got != int64(1712434882.5*float64(time.Second)) {
		t.Fatalf("unexpected float seconds timestamp: %d", got)
	}
	if got := parseInstantVectorTime("1712434882123456789"); got != 1712434882123456789 {
		t.Fatalf("unexpected nanoseconds timestamp: %d", got)
	}
}

func TestApplyScalarOpReverse(t *testing.T) {
	body := []byte(`{"results":[{"metric":{"app":"api"},"values":[[1,"2"],[2,"4"]]}]}`)
	out := applyScalarOpReverse(body, "-", 10, "matrix")

	var resp struct {
		Data struct {
			Results []struct {
				Values [][]interface{} `json:"values"`
			} `json:"results"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := resp.Data.Results[0].Values[0][1]; got != "8" {
		t.Fatalf("unexpected first reversed value: %#v", got)
	}
	if got := resp.Data.Results[0].Values[1][1]; got != "6" {
		t.Fatalf("unexpected second reversed value: %#v", got)
	}
}

func TestStreamStringMapVariants(t *testing.T) {
	if got := streamStringMap(map[string]string{"app": "api"}); got["app"] != "api" {
		t.Fatalf("unexpected direct map conversion: %#v", got)
	}
	got := streamStringMap(map[string]interface{}{"app": "api", "count": 3})
	if got["app"] != "api" {
		t.Fatalf("unexpected interface map conversion: %#v", got)
	}
	if _, ok := got["count"]; ok {
		t.Fatalf("expected non-string field to be dropped: %#v", got)
	}
	if got := streamStringMap(42); got != nil {
		t.Fatalf("expected nil for unsupported type, got %#v", got)
	}
}

func TestFormatEntryTimestampVariants(t *testing.T) {
	if got, ok := formatEntryTimestamp("2026-04-06T20:21:22Z"); !ok || got != strconv.FormatInt(time.Date(2026, 4, 6, 20, 21, 22, 0, time.UTC).UnixNano(), 10) {
		t.Fatalf("unexpected RFC3339 timestamp result: %q %v", got, ok)
	}
	if got, ok := formatEntryTimestamp("2026-04-06T20:21:22.123456789Z"); !ok || got != strconv.FormatInt(time.Date(2026, 4, 6, 20, 21, 22, 123456789, time.UTC).UnixNano(), 10) {
		t.Fatalf("unexpected RFC3339Nano timestamp result: %q %v", got, ok)
	}
	if got, ok := formatEntryTimestamp("1712434882123456789"); !ok || got != "1712434882123456789" {
		t.Fatalf("unexpected raw numeric timestamp result: %q %v", got, ok)
	}
	if _, ok := formatEntryTimestamp("not-a-time"); ok {
		t.Fatal("expected invalid timestamp to fail")
	}
}

func TestApplyWithoutMatrix(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api","pod":"a"},"values":[[1,"2"]]}]}}`)
	out := applyWithoutMatrix(body, map[string]bool{"pod": true})

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := resp.Data.Result[0].Metric["pod"]; ok {
		t.Fatalf("expected excluded label to be removed: %#v", resp.Data.Result[0].Metric)
	}
}

func TestPostprocessHelpers(t *testing.T) {
	if got := titleCase("hello world"); got != "Hello World" {
		t.Fatalf("unexpected titleCase result: %q", got)
	}
	if !isIPLike("10.20.30.40") {
		t.Fatal("expected dotted quad to be recognized")
	}
	if isIPLike("10.20.30") {
		t.Fatal("expected incomplete IP to be rejected")
	}
}

func TestCombineMetricResults(t *testing.T) {
	left := []byte(`{"results":[{"metric":{"app":"api"},"values":[["1","4"],["2","8"]]}]}`)
	right := []byte(`{"results":[{"metric":{"app":"api"},"values":[["1","2"],["3","10"]]}]}`)
	out := combineMetricResults(left, right, "/", "matrix")

	var resp struct {
		Data struct {
			Results []struct {
				Values [][]interface{} `json:"values"`
			} `json:"results"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := resp.Data.Results[0].Values[0][1]; got != "2" {
		t.Fatalf("expected matched point to be divided, got %#v", got)
	}
	if got := resp.Data.Results[0].Values[1][1]; got != "8" {
		t.Fatalf("expected unmatched point to stay unchanged, got %#v", got)
	}
}

func TestDrilldownHelpers(t *testing.T) {
	if _, ok := parseVolumeBoundary(""); ok {
		t.Fatal("expected empty boundary to fail")
	}
	if got, ok := parseVolumeBoundary("1712434882123456789"); !ok || got.UnixNano() != 1712434882123456789 {
		t.Fatalf("unexpected absolute boundary: %v %v", got, ok)
	}
	if got, ok := parseVolumeBoundary("now-1m"); !ok || time.Since(got) < 50*time.Second || time.Since(got) > 70*time.Second {
		t.Fatalf("unexpected relative boundary: %v %v", got, ok)
	}
	if got, ok := parseEntryTime("2026-04-06T20:21:22Z"); !ok || got.UTC().Format(time.RFC3339) != "2026-04-06T20:21:22Z" {
		t.Fatalf("unexpected parsed entry time: %v %v", got, ok)
	}
	floatTS := float64(1712434882123456789)
	if got, ok := parseEntryTime(floatTS); !ok || got.UnixNano() != int64(floatTS) {
		t.Fatalf("unexpected numeric entry time: %v %v", got, ok)
	}
	if got := unifyDetectedType("", "int"); got != "int" {
		t.Fatalf("unexpected unified type: %q", got)
	}
	if got := unifyDetectedType("int", "float"); got != "float" {
		t.Fatalf("unexpected float promotion: %q", got)
	}
	if got := unifyDetectedType("boolean", "string"); got != "string" {
		t.Fatalf("unexpected fallback to string: %q", got)
	}
	if got := formatDetectedValue("ok"); got != "ok" {
		t.Fatalf("unexpected formatted string value: %q", got)
	}
	if got := formatDetectedValue(12.5); got != "12.5" {
		t.Fatalf("unexpected formatted float value: %q", got)
	}
	if got := formatDetectedValue(true); got != "true" {
		t.Fatalf("unexpected formatted bool value: %q", got)
	}
	if got := formatDetectedValue(map[string]interface{}{"path": "/ready"}); got != `{"path":"/ready"}` {
		t.Fatalf("unexpected formatted object value: %q", got)
	}
	if string(mustJSON(map[string]string{"app": "api"})) != `{"app":"api"}` {
		t.Fatalf("unexpected mustJSON output: %s", string(mustJSON(map[string]string{"app": "api"})))
	}
}

func TestInferDetectedTypeVariants(t *testing.T) {
	tests := []struct {
		name string
		in   interface{}
		want string
	}{
		{name: "bool", in: true, want: "boolean"},
		{name: "int_like_float", in: 12.0, want: "int"},
		{name: "float", in: 12.5, want: "float"},
		{name: "duration", in: "5m", want: "duration"},
		{name: "string", in: "hello", want: "string"},
		{name: "fallback", in: map[string]string{"k": "v"}, want: "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inferDetectedType(tt.in); got != tt.want {
				t.Fatalf("inferDetectedType(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestDetectedFieldsCacheRoundTripAndInvalidPayload(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	ctx := context.WithValue(context.Background(), orgIDKey, "tenant-a")
	fields := []map[string]interface{}{
		{"label": "method", "type": "string", "cardinality": 2},
	}
	values := map[string][]string{"method": {"GET", "POST"}}

	p.setCachedDetectedFields(ctx, `{app="api"}`, "1", "2", 50, fields, values)
	gotFields, gotValues, ok := p.getCachedDetectedFields(ctx, `{app="api"}`, "1", "2", 50)
	if !ok {
		t.Fatal("expected detected fields cache hit")
	}
	if len(gotFields) != 1 || gotFields[0]["label"] != "method" {
		t.Fatalf("unexpected cached fields: %#v", gotFields)
	}
	if len(gotValues["method"]) != 2 || gotValues["method"][0] != "GET" {
		t.Fatalf("unexpected cached values: %#v", gotValues)
	}

	cacheKey := p.detectedFieldsCacheKey(ctx, `{app="api"}`, "1", "2", 50)
	p.cache.Set(cacheKey, []byte("{invalid-json"))
	if gotFields, gotValues, ok := p.getCachedDetectedFields(ctx, `{app="api"}`, "1", "2", 50); ok || gotFields != nil || gotValues != nil {
		t.Fatalf("expected invalid payload to miss cache, got %#v %#v %v", gotFields, gotValues, ok)
	}
}

func TestDetectedFieldsCache_NoCacheConfigured(t *testing.T) {
	p := &Proxy{}
	ctx := context.Background()
	if gotFields, gotValues, ok := p.getCachedDetectedFields(ctx, "*", "1", "2", 10); ok || gotFields != nil || gotValues != nil {
		t.Fatalf("expected miss without cache, got %#v %#v %v", gotFields, gotValues, ok)
	}
	p.setCachedDetectedFields(ctx, "*", "1", "2", 10, nil, nil)
}

func TestWriteEmptyLegacyRules(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	p.writeEmptyLegacyRules(w)

	if got := w.Code; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	if got := w.Header().Get("Content-Type"); got != "application/yaml" {
		t.Fatalf("unexpected content type %q", got)
	}
	if body := w.Body.String(); body != "{}\n" {
		t.Fatalf("unexpected yaml body %q", body)
	}
}

func TestPeerCacheMiddleware(t *testing.T) {
	t.Run("allows_configured_peer_host", func(t *testing.T) {
		pc := cache.NewPeerCache(cache.PeerConfig{
			SelfAddr:      "10.0.0.1:3100",
			DiscoveryType: "static",
			StaticPeers:   "10.0.0.2:3100",
			Timeout:       50 * time.Millisecond,
		})
		defer pc.Close()
		p := newTestProxy(t, "http://unused")
		p.peerCache = pc

		var called bool
		handler := p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusNoContent)
		}))
		req := httptest.NewRequest(http.MethodGet, "/_cache/get?key=x", nil)
		req.RemoteAddr = "10.0.0.2:1234"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if !called || w.Code != http.StatusNoContent {
			t.Fatalf("expected known peer to be allowed, called=%v code=%d", called, w.Code)
		}
	})

	t.Run("rejects_unknown_peer_host", func(t *testing.T) {
		pc := cache.NewPeerCache(cache.PeerConfig{
			SelfAddr:      "10.0.0.1:3100",
			DiscoveryType: "static",
			StaticPeers:   "10.0.0.2:3100",
			Timeout:       50 * time.Millisecond,
		})
		defer pc.Close()
		p := newTestProxy(t, "http://unused")
		p.peerCache = pc
		handler := p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("unexpected next handler call")
		}))
		req := httptest.NewRequest(http.MethodGet, "/_cache/get?key=x", nil)
		req.RemoteAddr = "10.0.0.9:1234"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", w.Code)
		}
	})

	t.Run("peer_token_overrides_host_check", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		p.peerAuthToken = "shared-secret"
		var called bool
		handler := p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusNoContent)
		}))
		req := httptest.NewRequest(http.MethodGet, "/_cache/get?key=x", nil)
		req.Header.Set("X-Peer-Token", "shared-secret")
		req.RemoteAddr = "10.0.0.99:1234"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if !called || w.Code != http.StatusNoContent {
			t.Fatalf("expected authenticated peer to be allowed, called=%v code=%d", called, w.Code)
		}
	})

	t.Run("rejects_missing_peer_token", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		p.peerAuthToken = "shared-secret"
		handler := p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("unexpected next handler call")
		}))
		req := httptest.NewRequest(http.MethodGet, "/_cache/get?key=x", nil)
		req.RemoteAddr = "10.0.0.2:1234"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
}

func TestNormalizeMetadataPairs_Variants(t *testing.T) {
	if got := normalizeMetadataPairs(nil); got != nil {
		t.Fatalf("expected nil for nil input, got %#v", got)
	}

	fromStringMap := normalizeMetadataPairs(map[string]string{
		" service.name ": "api",
		"":               "drop-me",
	})
	if len(fromStringMap) != 1 || fromStringMap["service.name"] != "api" {
		t.Fatalf("unexpected map[string]string normalization: %#v", fromStringMap)
	}

	fromInterfaceMap := normalizeMetadataPairs(map[string]interface{}{
		" host.id ": "i-1",
		"count":     3,
		"":          "drop-me",
	})
	if len(fromInterfaceMap) != 2 || fromInterfaceMap["host.id"] != "i-1" || fromInterfaceMap["count"] != "3" {
		t.Fatalf("unexpected map[string]interface{} normalization: %#v", fromInterfaceMap)
	}

	fromPairs := normalizeMetadataPairs([]interface{}{
		[]interface{}{"k8s.cluster.name", "prod"},
		[]interface{}{"", "drop-me"},
		[]string{"host.id", "i-2"},
		map[string]interface{}{"name": "service.name", "value": "api"},
		map[string]interface{}{"name": "", "value": "drop-me"},
	})
	if len(fromPairs) != 3 {
		t.Fatalf("unexpected []interface{} normalization cardinality: %#v", fromPairs)
	}
	if fromPairs["k8s.cluster.name"] != "prod" || fromPairs["host.id"] != "i-2" || fromPairs["service.name"] != "api" {
		t.Fatalf("unexpected []interface{} normalization values: %#v", fromPairs)
	}

	if got := normalizeMetadataPairs(struct{ Name string }{Name: "x"}); got != nil {
		t.Fatalf("expected nil for unsupported input, got %#v", got)
	}
}

func TestNormalizeMetadataPairTuples_AndCategorizedDetection(t *testing.T) {
	values := [][]interface{}{
		{"1700000000000000000", "line-one", map[string]interface{}{
			"structuredMetadata": []interface{}{
				[]interface{}{"host.id", "i-1"},
				map[string]interface{}{"name": "service.name", "value": "api"},
			},
			"parsed": []interface{}{
				[]string{"k8s.cluster.name", "cluster-a"},
			},
		}},
		{"1700000001000000000", "line-two", map[string]interface{}{
			"structuredMetadata": []interface{}{
				[]interface{}{"", "drop-me"},
			},
			"parsed": []interface{}{
				map[string]interface{}{"name": "", "value": "drop-me"},
			},
		}},
		{"1700000002000000000", "line-three"},
	}

	if !streamValuesHaveCategorizedMetadata(values) {
		t.Fatalf("expected categorized metadata to be detected before normalization")
	}
	normalizeMetadataPairTuples(values)

	meta0, _ := values[0][2].(map[string]interface{})
	structured0, _ := meta0["structuredMetadata"].(map[string]string)
	parsed0, _ := meta0["parsed"].(map[string]string)
	if len(structured0) != 2 || structured0["host.id"] != "i-1" || structured0["service.name"] != "api" {
		t.Fatalf("unexpected normalized structured metadata: %#v", structured0)
	}
	if len(parsed0) != 1 || parsed0["k8s.cluster.name"] != "cluster-a" {
		t.Fatalf("unexpected normalized parsed metadata: %#v", parsed0)
	}

	meta1, _ := values[1][2].(map[string]interface{})
	if _, ok := meta1["structuredMetadata"]; ok {
		t.Fatalf("expected empty structuredMetadata to be removed, got %#v", meta1["structuredMetadata"])
	}
	if _, ok := meta1["parsed"]; ok {
		t.Fatalf("expected empty parsed metadata to be removed, got %#v", meta1["parsed"])
	}

	if streamValuesHaveCategorizedMetadata([][]interface{}{{"1700000003000000000", "line-four"}}) {
		t.Fatalf("expected false when tuples do not contain categorized metadata object")
	}
}

func TestVLErrorHelpers(t *testing.T) {
	blank := (&vlAPIError{status: http.StatusBadGateway, body: "   "}).Error()
	if blank != "victorialogs api error: status 502" {
		t.Fatalf("unexpected blank-body error text: %q", blank)
	}
	trimmed := (&vlAPIError{status: http.StatusBadGateway, body: " backend overloaded  "}).Error()
	if trimmed != "backend overloaded" {
		t.Fatalf("unexpected trimmed-body error text: %q", trimmed)
	}

	if !shouldFallbackToGenericMetadata(&vlAPIError{status: http.StatusNotFound}) {
		t.Fatalf("expected 4xx vl api error to trigger generic metadata fallback")
	}
	if shouldFallbackToGenericMetadata(&vlAPIError{status: http.StatusInternalServerError}) {
		t.Fatalf("expected 5xx vl api error to skip generic metadata fallback")
	}
	if shouldFallbackToGenericMetadata(errors.New("plain error")) {
		t.Fatalf("expected non-vl error to skip generic metadata fallback")
	}
}

func TestCompatCacheResponseAllowedAndBreakerFailure(t *testing.T) {
	if compatCacheResponseAllowed(nil) {
		t.Fatalf("nil recorder should not be cacheable")
	}
	badStatus := httptest.NewRecorder()
	badStatus.WriteHeader(http.StatusBadGateway)
	if compatCacheResponseAllowed(badStatus) {
		t.Fatalf("non-200 response should not be cacheable")
	}
	flushed := httptest.NewRecorder()
	flushed.WriteHeader(http.StatusOK)
	flushed.Flush()
	if compatCacheResponseAllowed(flushed) {
		t.Fatalf("flushed response should not be cacheable")
	}
	withCookie := httptest.NewRecorder()
	http.SetCookie(withCookie, &http.Cookie{Name: "sid", Value: "abc"})
	withCookie.WriteHeader(http.StatusOK)
	if compatCacheResponseAllowed(withCookie) {
		t.Fatalf("response with cookies should not be cacheable")
	}
	withText := httptest.NewRecorder()
	withText.Header().Set("Content-Type", "text/plain")
	withText.WriteHeader(http.StatusOK)
	if compatCacheResponseAllowed(withText) {
		t.Fatalf("non-json content type should not be cacheable")
	}
	withJSON := httptest.NewRecorder()
	withJSON.Header().Set("Content-Type", "application/json; charset=utf-8")
	withJSON.WriteHeader(http.StatusOK)
	if !compatCacheResponseAllowed(withJSON) {
		t.Fatalf("json response should be cacheable")
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: false},
		{name: "net timeout", err: &net.DNSError{IsTimeout: true}, want: false},
		{name: "string timeout", err: errors.New("upstream timeout"), want: false},
		{name: "non-timeout", err: errors.New("connection reset by peer"), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRecordBreakerFailure(tc.err); got != tc.want {
				t.Fatalf("shouldRecordBreakerFailure(%v)=%v want %v", tc.err, got, tc.want)
			}
		})
	}
}

type hijackRecorder struct {
	http.ResponseWriter
	hijacked bool
}

func (h *hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

func TestStatusCaptureHijack(t *testing.T) {
	scNoHijack := &statusCapture{ResponseWriter: httptest.NewRecorder()}
	if _, _, err := scNoHijack.Hijack(); err == nil {
		t.Fatalf("expected hijack to fail when wrapped writer has no hijacker")
	}

	h := &hijackRecorder{ResponseWriter: httptest.NewRecorder()}
	scHijack := &statusCapture{ResponseWriter: h}
	if _, _, err := scHijack.Hijack(); err != nil {
		t.Fatalf("expected hijack to pass through, got error: %v", err)
	}
	if !h.hijacked {
		t.Fatalf("expected wrapped hijacker to be called")
	}
}

func TestHandleReady_WarmingAndOpenBreakerBackendError(t *testing.T) {
	pWarm := newTestProxy(t, "http://unused")
	pWarm.labelValuesIndexedCache = true
	pWarm.labelValuesIndexWarmReady.Store(false)
	wWarm := httptest.NewRecorder()
	rWarm := httptest.NewRequest(http.MethodGet, "/ready", nil)
	pWarm.handleReady(wWarm, rWarm)
	if wWarm.Code != http.StatusServiceUnavailable || wWarm.Body.String() != "label values index warming" {
		t.Fatalf("expected warming readiness response, got code=%d body=%q", wWarm.Code, wWarm.Body.String())
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer vlBackend.Close()

	pOpen := newTestProxy(t, vlBackend.URL)
	for i := 0; i < 10; i++ {
		pOpen.breaker.RecordFailure()
	}
	wOpen := httptest.NewRecorder()
	rOpen := httptest.NewRequest(http.MethodGet, "/ready", nil)
	pOpen.handleReady(wOpen, rOpen)
	if wOpen.Code != http.StatusServiceUnavailable || wOpen.Body.String() != "backend not ready" {
		t.Fatalf("expected open-breaker readiness failure to surface as backend not ready, got code=%d body=%q", wOpen.Code, wOpen.Body.String())
	}
}

func TestLabelValuesWindowAndDefaultLimit(t *testing.T) {
	values := []string{"Alpha", "beta", "gamma", "alphabet"}
	out := selectLabelValuesWindow(values, "  AL  ", -5, 0)
	if len(out) != 2 || out[0] != "Alpha" || out[1] != "alphabet" {
		t.Fatalf("unexpected search+offset+limit default window: %#v", out)
	}

	out = selectLabelValuesWindow(values, "", 1, 2)
	if len(out) != 2 || out[0] != "beta" || out[1] != "gamma" {
		t.Fatalf("unexpected paged window: %#v", out)
	}

	p := &Proxy{labelValuesHotLimit: 50}
	if got := p.defaultLabelValuesLimit("25"); got != 25 {
		t.Fatalf("explicit limit should win, got %d", got)
	}
	if got := p.defaultLabelValuesLimit("0"); got != 50 {
		t.Fatalf("invalid explicit limit should fall back to hot limit, got %d", got)
	}
	if got := p.defaultLabelValuesLimit("999999"); got != maxLimitValue {
		t.Fatalf("explicit limit should be clamped to max, got %d", got)
	}

	p.labelValuesHotLimit = 0
	if got := p.defaultLabelValuesLimit(""); got != 200 {
		t.Fatalf("empty limit with unset hot limit should default to 200, got %d", got)
	}
	p.labelValuesHotLimit = maxLimitValue + 100
	if got := p.defaultLabelValuesLimit(""); got != maxLimitValue {
		t.Fatalf("hot limit should be clamped to max, got %d", got)
	}
}

func TestFetchVLFieldValues_ErrorPaths(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"a","hits":1},{"value":"b","hits":2}]}`))
		case "/bad":
			http.Error(w, "backend failed", http.StatusBadGateway)
		case "/invalid":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{not-json`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	ctx := context.Background()

	got, err := p.fetchVLFieldValues(ctx, "/ok", url.Values{})
	if err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected decoded values, got values=%#v err=%v", got, err)
	}

	if _, err := p.fetchVLFieldValues(ctx, "/bad", url.Values{}); err == nil {
		t.Fatalf("expected API status error from /bad")
	} else if _, ok := err.(*vlAPIError); !ok {
		t.Fatalf("expected vlAPIError from /bad, got %T", err)
	}

	if _, err := p.fetchVLFieldValues(ctx, "/invalid", url.Values{}); err == nil {
		t.Fatalf("expected decode error from /invalid")
	}

	for i := 0; i < 10; i++ {
		p.breaker.RecordFailure()
	}
	if _, err := p.fetchVLFieldValues(ctx, "/ok", url.Values{}); err == nil {
		t.Fatalf("expected breaker-open transport error")
	}
}

func TestNormalizeMetadataStringValue(t *testing.T) {
	if got := normalizeMetadataStringValue(nil); got != "" {
		t.Fatalf("nil metadata value should normalize to empty string, got %q", got)
	}
	if got := normalizeMetadataStringValue(42); got != "42" {
		t.Fatalf("numeric metadata value should stringify, got %q", got)
	}
}

func TestProxyStatsQueryAndRangeBranches(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"resultType":"vector","result":[{"metric":{"service.name":"api"},"value":[1712434882,"5"]}]}}`))
		case "/select/logsql/stats_query_range":
			http.Error(w, "backend blew up", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	wQuery := httptest.NewRecorder()
	rQuery := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query?query=sum(rate({app="api"}[1m]))&time=1712434882`, nil)
	p.proxyStatsQuery(wQuery, rQuery, `sum(rate({app="api"}[1m]))`)
	if wQuery.Code != http.StatusOK {
		t.Fatalf("expected query stats proxy success, got %d body=%s", wQuery.Code, wQuery.Body.String())
	}
	if ct := wQuery.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json content type, got %q", ct)
	}
	if body := wQuery.Body.String(); !strings.Contains(body, `"resultType":"vector"`) || !strings.Contains(body, `"status":"success"`) {
		t.Fatalf("expected Loki-wrapped stats query response, got %s", body)
	}

	wRange := httptest.NewRecorder()
	rRange := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query_range?query=sum(rate({app="api"}[1m]))&start=1712434800&end=1712438400&step=60`, nil)
	p.proxyStatsQueryRange(wRange, rRange, `sum(rate({app="api"}[1m]))`)
	if wRange.Code != http.StatusInternalServerError {
		t.Fatalf("expected stats query_range backend status passthrough, got %d body=%s", wRange.Code, wRange.Body.String())
	}
	if body := wRange.Body.String(); !strings.Contains(body, "backend blew up") {
		t.Fatalf("expected backend error body in query_range response, got %s", body)
	}

	for i := 0; i < 10; i++ {
		p.breaker.RecordFailure()
	}
	wQueryErr := httptest.NewRecorder()
	rQueryErr := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query?query=sum(rate({app="api"}[1m]))`, nil)
	p.proxyStatsQuery(wQueryErr, rQueryErr, `sum(rate({app="api"}[1m]))`)
	if wQueryErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected circuit breaker open to map to 503, got %d body=%s", wQueryErr.Code, wQueryErr.Body.String())
	}
	if body := wQueryErr.Body.String(); !strings.Contains(strings.ToLower(body), "circuit breaker open") {
		t.Fatalf("expected circuit breaker error in query response, got %s", body)
	}
}
