package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// newDefaultLengthProxy is a proxy with the shipped -default-max-query-length
// (or maxLen when it is not negative) behind the shipped middleware chain, in
// front of a VictoriaLogs stand-in that answers every call empty and counts
// them.
func newDefaultLengthProxy(t *testing.T, maxLen time.Duration, tenantDefaults map[string]any) (string, *errLayerBackend) {
	t.Helper()
	backend := &errLayerBackend{failSig: "\x00none", okBody: "empty"}
	vl := httptest.NewServer(backend)
	t.Cleanup(vl.Close)
	c := cache.New(time.Minute, 1000)
	t.Cleanup(c.Close)
	if maxLen < 0 {
		maxLen = DefaultMaxQueryLength
	}
	p, err := New(Config{
		BackendURL:            vl.URL,
		Cache:                 c,
		LogLevel:              "error",
		BackendTimeout:        5 * time.Second,
		DefaultMaxQueryLength: maxLen,
		TenantDefaultLimits:   tenantDefaults,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(shippedChain(mux))
	t.Cleanup(front.Close)
	return front.URL, backend
}

// TestDefaultMaxQueryLength_IsLokis pins the shipped default to Loki's
// documented max_query_length default (limits_config: 30d1h).
//
// conformance: limits/max-query-length-default, tenant-query-limits
func TestDefaultMaxQueryLength_IsLokis(t *testing.T) {
	if DefaultMaxQueryLength != 721*time.Hour {
		t.Fatalf("DefaultMaxQueryLength = %s, Loki's max_query_length default is 721h", DefaultMaxQueryLength)
	}
	if got := formatLokiDuration(DefaultMaxQueryLength); got != "30d1h" {
		t.Fatalf("published as %q, Loki publishes 30d1h", got)
	}
}

// TestDefaultMaxQueryLength_RecordedLokiAnswers replays every recorded Loki
// answer to a range over its default max_query_length (the recorded stack
// sets no max_query_length) against a proxy with no length configured but the
// shipped default: each route answers Loki's 400, byte for byte, before any
// VictoriaLogs call.
//
// conformance: limits/max-query-length-default, limits/max-query-length-error, tenant-query-limits
func TestDefaultMaxQueryLength_RecordedLokiAnswers(t *testing.T) {
	base, backend := newDefaultLengthProxy(t, -1, nil)
	addr := strings.TrimPrefix(base, "http://")
	routes := map[string]bool{}
	for _, row := range readLokiErrorAnswers(t) {
		if row.NotComparable != "" || !strings.HasPrefix(row.Loki.Body, "the query time range exceeds the limit (") {
			continue
		}
		before := backend.hits.Load()
		if reply := row.replay(t, addr); reply != row.Loki {
			t.Errorf("%s %s: proxy %s, Loki %s", row.ID, row.Route, summarizeReply(reply), summarizeReply(row.Loki))
		}
		if calls := backend.hits.Load() - before; calls != 0 {
			t.Errorf("%s %s: %d VictoriaLogs calls before the rejection, want 0", row.ID, row.Route, calls)
		}
		routes[row.Route] = true
	}
	// Every route Loki's recorded answers cover: the limits middleware's and
	// the detected fields and volume routes'.
	for _, route := range []string{"labels", "label_values", "series", "index_stats", "index_volume", "detected_fields", "detected_field_values", "detected_labels"} {
		if !routes[route] {
			t.Errorf("no recorded Loki max_query_length answer replayed for %s", route)
		}
	}
}

// TestDefaultMaxQueryLength_Boundary: under the shipped default a range of
// exactly 721h runs and one millisecond more fails with Loki's text (the
// length as a Go duration, the limit as Loki publishes it); 0 on the flag, or
// "0s" as a tenant limit, turns the limit off, as Loki's max_query_length: 0.
//
// conformance: limits/max-query-length-default, limits/max-query-length-error, limits/effective-limit-resolution, tenant-query-limits
func TestDefaultMaxQueryLength_Boundary(t *testing.T) {
	end := time.Unix(1794570675, 0)
	type route struct {
		name, path string
		params     url.Values
	}
	sel := `{app="api-gateway"}`
	routes := []route{
		{"query_range", "/loki/api/v1/query_range", url.Values{"query": {sel}, "limit": {"10"}, "step": {"3600"}}},
		{"labels", "/loki/api/v1/labels", url.Values{}},
		{"label_values", "/loki/api/v1/label/app/values", url.Values{}},
		{"series", "/loki/api/v1/series", url.Values{"match[]": {sel}}},
		{"detected_labels", "/loki/api/v1/detected_labels", url.Values{"query": {sel}}},
		{"index_stats", "/loki/api/v1/index/stats", url.Values{"query": {sel}}},
		{"index_volume", "/loki/api/v1/index/volume", url.Values{"query": {sel}}},
	}
	get := func(t *testing.T, base string, r route, length time.Duration) (int, string) {
		t.Helper()
		params := url.Values{}
		for k, v := range r.params {
			params[k] = v
		}
		params.Set("start", strconv.FormatInt(end.Add(-length).UnixNano(), 10))
		params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		resp, err := http.Get(base + r.path + "?" + params.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, strings.TrimSpace(string(body))
	}
	const over = 721*time.Hour + time.Millisecond
	const want = "the query time range exceeds the limit (query length: 721h0m0.001s, limit: 30d1h)"

	t.Run("shipped default", func(t *testing.T) {
		base, backend := newDefaultLengthProxy(t, -1, nil)
		for _, r := range routes {
			if status, body := get(t, base, r, 721*time.Hour); status != http.StatusOK {
				t.Errorf("%s at exactly 721h: %d %.200s, want 200 as Loki", r.name, status, body)
			}
			before := backend.hits.Load()
			status, body := get(t, base, r, over)
			if status != http.StatusBadRequest || body != want {
				t.Errorf("%s at 721h+1ms: %d %q, want 400 %q", r.name, status, body, want)
			}
			if calls := backend.hits.Load() - before; calls != 0 {
				t.Errorf("%s at 721h+1ms: %d VictoriaLogs calls, want 0", r.name, calls)
			}
		}
	})
	// A request with only a start (Loki reads the missing end as now) or only an end (a missing start is the
	// route's default window before it) is held to the limit too: start=1 with no end is Loki's recorded r1387.
	t.Run("one bound missing", func(t *testing.T) {
		base, backend := newDefaultLengthProxy(t, -1, nil)
		lengthRE := regexp.MustCompile(`^the query time range exceeds the limit \(query length: [0-9hms.]+, limit: 30d1h\)$`)
		for _, r := range routes {
			params := url.Values{}
			for k, v := range r.params {
				params[k] = v
			}
			params.Del("step") // r1387 sends none: the step Loki derives from the range never exceeds its resolution
			params.Set("start", "1")
			before := backend.hits.Load()
			resp, err := http.Get(base + r.path + "?" + params.Encode())
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if got := strings.TrimSpace(string(body)); resp.StatusCode != http.StatusBadRequest || !lengthRE.MatchString(got) {
				t.Errorf("%s with start=1 and no end: %d %.200q, want 400 with Loki's length text", r.name, resp.StatusCode, got)
			}
			if calls := backend.hits.Load() - before; calls != 0 {
				t.Errorf("%s with start=1 and no end: %d VictoriaLogs calls, want 0", r.name, calls)
			}
		}
	})
	t.Run("flag 0 turns it off", func(t *testing.T) {
		base, _ := newDefaultLengthProxy(t, 0, nil)
		for _, r := range routes {
			if status, body := get(t, base, r, 3000*time.Hour); status != http.StatusOK {
				t.Errorf("%s over 3000h with -default-max-query-length=0: %d %.200s, want 200", r.name, status, body)
			}
		}
	})
	t.Run("tenant default 0s turns it off", func(t *testing.T) {
		base, _ := newDefaultLengthProxy(t, -1, map[string]any{"max_query_length": "0s"})
		for _, r := range routes {
			if status, body := get(t, base, r, 3000*time.Hour); status != http.StatusOK {
				t.Errorf("%s over 3000h with max_query_length 0s: %d %.200s, want 200", r.name, status, body)
			}
		}
	})
}

// TestDefaultMaxQueryLength_Published: with nothing configured, both limits
// endpoints publish Loki's default max_query_length, the value enforced.
//
// conformance: limits/max-query-length-default, limits/published-equals-enforced, tenant-query-limits
func TestDefaultMaxQueryLength_Published(t *testing.T) {
	base, _ := newDefaultLengthProxy(t, -1, nil)
	read := func(path string) []byte {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
		}
		return body
	}
	var drilldown struct {
		Limits map[string]any `json:"limits"`
	}
	if err := json.Unmarshal(read("/loki/api/v1/drilldown-limits"), &drilldown); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(read("/config/tenant/v1/limits"), &config); err != nil {
		t.Fatal(err)
	}
	if got := drilldown.Limits["max_query_length"]; got != "30d1h" {
		t.Errorf("drilldown-limits max_query_length = %v, want 30d1h", got)
	}
	if got := config["max_query_length"]; got != "30d1h" {
		t.Errorf("/config/tenant/v1/limits max_query_length = %v, want 30d1h", got)
	}
}
