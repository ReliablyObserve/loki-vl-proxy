package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
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
