package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func postReadCacheRequest(path string, params url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(params.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func TestCanonicalReadCacheKeyPOSTFormMatchesGET(t *testing.T) {
	for _, endpoint := range []string{"labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels"} {
		t.Run(endpoint, func(t *testing.T) {
			params := url.Values{"query": {`{app="api"}`}, "from": {"1790871720000000000"}, "to": {"1790875320000000000"}, "q": {"svc"}, "line_limit": {"1000"}, "step": {"60"}, "match[]": {`{app="api"}`, `{env="prod"}`}}
			p := &Proxy{}
			get := httptest.NewRequest(http.MethodGet, "/?"+params.Encode(), nil)
			post := postReadCacheRequest("/", params)
			if got, want := p.canonicalReadCacheKey(endpoint, "tenant-a", post), p.canonicalReadCacheKey(endpoint, "tenant-a", get); got != want {
				t.Fatalf("POST form and GET params differ: got %q want %q", got, want)
			}
		})
	}
}

func TestCanonicalReadCacheKeyPOSTFormPrecedenceAndImmutable(t *testing.T) {
	body := url.Values{"query": {`{app="body"}`, `{app="second"}`}, "from": {"1790871720000000000"}, "to": {"1790875320000000000"}, "q": {" svc "}, "match[]": {`{env="prod"}`, `{env="staging"}`}}
	r := postReadCacheRequest("/?query=%7Bapp%3D%22url%22%7D&from=1&to=2&match%5B%5D=%7Benv%3D%22url%22%7D", body)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	before, postBefore := r.Form.Encode(), r.PostForm.Encode()
	p := &Proxy{}
	got := p.canonicalReadCacheKey("labels", "tenant-a", r)
	expected := url.Values{"query": {`{app="body"}`}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}, "search": {"svc"}, "match[]": {`{env="prod"}`, `{env="staging"}`, `{env="url"}`}}
	want := p.canonicalReadCacheKey("labels", "tenant-a", httptest.NewRequest(http.MethodGet, "/?"+expected.Encode(), nil))
	if got != want {
		t.Errorf("POST must use body precedence and retain multiple values: got %q want %q", got, want)
	}
	if r.Form.Encode() != before || r.PostForm.Encode() != postBefore {
		t.Fatalf("cache key normalization mutated request form: Form=%v PostForm=%v", r.Form, r.PostForm)
	}
	if !reflect.DeepEqual(r.Form["query"], []string{`{app="body"}`, `{app="second"}`, `{app="url"}`}) {
		t.Fatalf("request query values changed: %v", r.Form["query"])
	}
}

func TestCanonicalReadCacheKeyPOSTFormSeparatesSelectorsAndWindows(t *testing.T) {
	p := &Proxy{}
	cases := []url.Values{
		{"query": {`{app="a"}`}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}},
		{"query": {`{app="b"}`}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}},
		{"query": {`{app="a"}`}, "start": {"1790785320000000000"}, "end": {"1790875320000000000"}},
		{"query": {`{app="a"}`}, "start": {"1790871720000000000"}, "end": {"1790961720000000000"}},
	}
	seen := map[string]bool{}
	for _, params := range cases {
		key := p.canonicalReadCacheKey("labels", "", postReadCacheRequest("/loki/api/v1/labels", params))
		if seen[key] {
			t.Errorf("POST form identity collided: %v -> %q", params, key)
		}
		seen[key] = true
		if again := p.canonicalReadCacheKey("labels", "", postReadCacheRequest("/loki/api/v1/labels", params)); again != key {
			t.Errorf("repeat identity changed: %q != %q", again, key)
		}
	}
}

func TestCanonicalReadCacheKeyPOSTPartialFormPreservesEffectiveParameters(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/?query=%7Bapp%3D%22url%22%7D", strings.NewReader("query=%7Bapp%3D%22body%22%7D&invalid=%zz&start=1790871720000000000&end=1790875320000000000"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := r.ParseForm(); err == nil {
		t.Fatal("fixture must contain a malformed form value")
	}
	before := r.Form.Encode()
	p := &Proxy{}
	got := p.canonicalReadCacheKey("labels", "tenant-a", r)
	params := url.Values{"query": {`{app="body"}`}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}}
	want := p.canonicalReadCacheKey("labels", "tenant-a", httptest.NewRequest(http.MethodGet, "/?"+params.Encode(), nil))
	if got != want || r.Form.Encode() != before {
		t.Fatalf("partial form identity or original form changed: got=%q want=%q form=%v", got, want, r.Form)
	}
}

// previousPOSTReadCacheKey reconstructs the namespace used by binaries whose
// URL-only POST keys could store a filtered answer under a wildcard identity.
func previousPOSTReadCacheKey(endpoint, key string) string {
	previous := map[string]string{
		"labels": "@full-range-v2", "label_values": "@full-range-v2", "label_inventory": "@full-range-v2",
		"detected_fields": "@detected-level-v1", "detected_field_values": "@detected-level-v1", "detected_labels": "@detected-level-v1",
		"volume": "@bytes-v1", "volume_range": "@bytes-v1", "index_stats": "",
	}
	if version := readCacheKeyVersion(endpoint); version != "" {
		replacement := ":"
		if previous[endpoint] != "" {
			replacement += previous[endpoint] + ":"
		}
		return strings.Replace(key, ":"+version+":", replacement, 1)
	}
	return key
}

func TestCanonicalReadCacheKeyPOSTMigrationRejectsPriorNamespace(t *testing.T) {
	for _, endpoint := range []string{"labels", "label_values", "label_inventory", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels"} {
		t.Run(endpoint, func(t *testing.T) {
			c := cache.New(time.Hour, 10)
			t.Cleanup(c.Close)
			p := &Proxy{cache: c}
			req := httptest.NewRequest(http.MethodGet, "/?query=%2A", nil)
			key := p.canonicalReadCacheKey(endpoint, "tenant-a", req)
			previous := previousPOSTReadCacheKey(endpoint, key)
			c.SetWithTTL(previous, []byte(`{"status":"success","data":["poisoned-selector"]}`), time.Hour)
			if body, _, _, hit := p.endpointReadCacheEntry(endpoint, key); hit {
				t.Fatalf("current GET reused a prior POST namespace: endpoint=%s key=%q body=%s", endpoint, key, body)
			}
			c.SetWithTTL(key, []byte("current-answer"), time.Hour)
			if body, _, _, hit := p.endpointReadCacheEntry(endpoint, key); !hit || string(body) != "current-answer" {
				t.Fatalf("current namespace did not retain its own entry: hit=%v body=%s", hit, body)
			}
		})
	}
}
