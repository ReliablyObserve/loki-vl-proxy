package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// fullRangeVL is a fake VictoriaLogs whose metadata answers depend on the
// requested window, like the real backend: a value only exists when the
// requested start reaches back far enough to cover the data holding it.
type fullRangeVL struct {
	mu    sync.Mutex
	calls []string // path?start..end for every non-health call
	count atomic.Int64
}

// windowCovers reports whether [start, end] reaches back at least age before end.
func windowCovers(r *http.Request, age time.Duration) bool {
	startNs, ok1 := parseLokiTimeToUnixNano(r.URL.Query().Get("start"))
	endNs, ok2 := parseLokiTimeToUnixNano(r.URL.Query().Get("end"))
	return ok1 && ok2 && endNs-startNs > int64(age)
}

func (f *fullRangeVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		f.count.Add(1)
		f.mu.Lock()
		f.calls = append(f.calls, r.URL.Path+"?field="+r.URL.Query().Get("field")+"&start="+r.URL.Query().Get("start")+"&end="+r.URL.Query().Get("end"))
		f.mu.Unlock()
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			hits := []fieldHit{{"app", 10}}
			// Stream label only present 1-2h before the range end.
			if windowCovers(r, time.Hour) {
				hits = append(hits, fieldHit{"early_label", 1})
			}
			writeVLFieldNames(w, hits)
		case "/select/logsql/field_names":
			hits := []fieldHit{{"app", 10}, {"_msg", 10}, {"_time", 10}, {"_stream", 10}, {"trace_id", 10}, {"service_name", 10}}
			if windowCovers(r, time.Hour) {
				hits = append(hits, fieldHit{"early_label", 1})
			}
			writeVLFieldNames(w, hits)
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			switch r.URL.Query().Get("field") {
			case "app":
				values := []fieldHit{{"recent-app", 10}}
				// Value only present more than 6h before the range end.
				if windowCovers(r, 7*time.Hour) {
					values = append(values, fieldHit{"old-app", 1})
				}
				writeVLFieldValues(w, values)
			case "service_name":
				values := []fieldHit{{"recent-svc", 10}}
				if windowCovers(r, time.Hour) {
					values = append(values, fieldHit{"old-svc", 1})
				}
				writeVLFieldValues(w, values)
			case "early_label":
				if windowCovers(r, time.Hour) {
					writeVLFieldValues(w, []fieldHit{{"early-value", 1}})
					return
				}
				writeVLFieldValues(w, nil)
			default:
				writeVLFieldValues(w, nil)
			}
		default:
			writeVLFieldNames(w, nil)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func serveLokiStrings(t *testing.T, mux *http.ServeMux, path string) []string {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s returned %d: %s", path, rec.Code, rec.Body.String())
	}
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s: %v (%s)", path, err, rec.Body.String())
	}
	if resp.Status != "success" {
		t.Fatalf("%s status %q", path, resp.Status)
	}
	return resp.Data
}

func fullRangeWindowPath(endpoint string, window time.Duration) string {
	end := perfBaseTimeNs
	return fmt.Sprintf("%s?start=%d&end=%d", endpoint, end-int64(window), end)
}

// Loki returns every label name with data in [start, end]. The first /labels
// response for a 24h range must already include a label that only has data
// 1-2h before the range end.
func TestLabelsFullRange_FirstResponseIncludesLabelOutsideRecentWindow(t *testing.T) {
	fake := &fullRangeVL{}
	srv := fake.server(t)
	_, mux := newBehaviorProxy(t, srv.URL, 0)

	got := serveLokiStrings(t, mux, fullRangeWindowPath("/loki/api/v1/labels", 24*time.Hour))
	for _, want := range []string{"app", "early_label", "service_name"} {
		if !contains(got, want) {
			t.Fatalf("first /labels response missing %q: %v (backend calls %v)", want, got, fake.calls)
		}
	}
	// trace_id is a non-stream field: Loki never reports it as a label.
	if contains(got, "trace_id") {
		t.Fatalf("non-stream field leaked into /labels: %v", got)
	}
}

// A background refresh must use the same stream-label source as the handler:
// field_names would add message/structured fields Loki never lists as labels.
func TestLabelsFullRange_BackgroundRefreshKeepsStreamLabelSemantics(t *testing.T) {
	fake := &fullRangeVL{}
	srv := fake.server(t)
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	end := perfBaseTimeNs
	start := end - int64(24*time.Hour)
	p.refreshLabelsCacheAsync("", "labels:refresh-semantics", "", fmt.Sprint(start), fmt.Sprint(end), "", nil)
	raw := waitForCachedKey(t, c, "labels:refresh-semantics")
	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if !contains(resp.Data, "early_label") || contains(resp.Data, "trace_id") {
		t.Fatalf("refresh must list stream labels over the full range only, got %v", resp.Data)
	}
}

// Loki returns every value with data in [start, end]; a value only present
// more than 6h before the range end must be included on the first response.
func TestLabelValuesFullRange_IncludesValuesOlderThanSixHours(t *testing.T) {
	for _, tc := range []struct {
		name, label, want string
		window            time.Duration
	}{
		{name: "plain label older than 6h", label: "app", want: "old-app", window: 24 * time.Hour},
		{name: "label only present early in range", label: "early_label", want: "early-value", window: 24 * time.Hour},
		{name: "service_name older than 5m", label: "service_name", want: "old-svc", window: 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fullRangeVL{}
			srv := fake.server(t)
			_, mux := newBehaviorProxy(t, srv.URL, 0)
			path := fullRangeWindowPath("/loki/api/v1/label/"+tc.label+"/values", tc.window)
			got := serveLokiStrings(t, mux, path)
			if !contains(got, tc.want) {
				t.Fatalf("first %s response missing %q: %v (backend calls %v)", path, tc.want, got, fake.calls)
			}
		})
	}
}

// Loki answers /labels for a window without data with no label names (verified
// on Loki 3.7.1: {"status":"success"}). The proxy must not invent service_name
// or operator-declared fields for it.
func TestLabelsFullRange_EmptyWindowHasNoSyntheticLabels(t *testing.T) {
	calls := new(atomic.Int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		calls.Add(1)
		writeVLFieldNames(w, nil)
	}))
	t.Cleanup(srv.Close)
	p, err := New(Config{
		BackendURL:       srv.URL,
		Cache:            cache.New(60*time.Second, 1000),
		LogLevel:         "error",
		StreamFields:     []string{"app", "namespace"},
		ExtraLabelFields: []string{"cluster"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	path := fullRangeWindowPath("/loki/api/v1/labels", 24*time.Hour)
	if got := serveLokiStrings(t, mux, path); len(got) != 0 {
		t.Fatalf("empty window must return no label names, got %v", got)
	}
	// The empty answer is cached only briefly (negative TTL) and never with the
	// metadata TTL, so polling does not rescan while later data still shows up.
	req := httptest.NewRequest(http.MethodGet, path, nil)
	_, remaining, _, ok := p.endpointReadCacheEntry("labels", p.canonicalReadCacheKey("labels", "", req))
	if !ok || remaining <= 0 || remaining > metadataNegativeCacheTTL {
		t.Fatalf("empty /labels answer must be cached for at most %v, got ok=%v remaining=%v", metadataNegativeCacheTTL, ok, remaining)
	}
	before := calls.Load()
	_ = serveLokiStrings(t, mux, path)
	if calls.Load() != before {
		t.Fatalf("repeated empty /labels request within the negative TTL rescanned the backend")
	}
}

// emptyThenDataVL answers every metadata call with nothing until hasData is set,
// then with one label (app) and one value (late-app).
func emptyThenDataVL(t *testing.T, hasData *atomic.Bool, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		calls.Add(1)
		switch r.URL.Path {
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			if hasData.Load() {
				writeVLFieldValues(w, []fieldHit{{"late-app", 1}})
				return
			}
			writeVLFieldValues(w, nil)
		default:
			if hasData.Load() {
				writeVLFieldNames(w, []fieldHit{{"app", 1}})
				return
			}
			writeVLFieldNames(w, nil)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Empty name and value lists are negatively cached in every layer (endpoint
// cache, compatibility edge cache, field-name cache) for metadataNegativeCacheTTL:
// repeated polls inside it make no backend calls, schedule no background refresh,
// and data ingested meanwhile is returned once it expires.
func TestLabelsNegativeCache_EmptyListsExpireAndDataAppears(t *testing.T) {
	var hasData atomic.Bool
	calls := new(atomic.Int64)
	srv := emptyThenDataVL(t, &hasData, calls)
	p, err := New(Config{
		BackendURL:  srv.URL,
		Cache:       cache.New(60*time.Second, 1000),
		CompatCache: cache.New(60*time.Second, 100),
		LogLevel:    "error",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	labelsPath := fullRangeWindowPath("/loki/api/v1/labels", 24*time.Hour)
	valuesPath := fullRangeWindowPath("/loki/api/v1/label/app/values", 24*time.Hour) + "&query=" + "%7Bapp%3D~%22.%2B%22%7D"
	if got := serveLokiStrings(t, mux, labelsPath); len(got) != 0 {
		t.Fatalf("labels before data = %v, want empty", got)
	}
	if got := serveLokiStrings(t, mux, valuesPath); len(got) != 0 {
		t.Fatalf("values before data = %v, want empty", got)
	}
	waitForBackendIdle(t, calls)

	hasData.Store(true)
	before := calls.Load()
	for i := 0; i < 3; i++ {
		_ = serveLokiStrings(t, mux, labelsPath)
		_ = serveLokiStrings(t, mux, valuesPath)
	}
	waitForBackendIdle(t, calls)
	if extra := calls.Load() - before; extra != 0 {
		t.Fatalf("polls inside the negative TTL made %d backend call(s)", extra)
	}

	restore := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + time.Second)
	defer restore()
	if got := serveLokiStrings(t, mux, labelsPath); !contains(got, "app") {
		t.Fatalf("labels after the negative TTL = %v, want app", got)
	}
	if got := serveLokiStrings(t, mux, valuesPath); !contains(got, "late-app") {
		t.Fatalf("values after the negative TTL = %v, want late-app", got)
	}
}

func TestMetadataListPayloadEmpty(t *testing.T) {
	for body, want := range map[string]bool{
		string(lokiLabelsResponse([]string{})):      true,
		string(lokiLabelsResponse(nil)):             true,
		`{"status":"success"}`:                      false,
		string(lokiLabelsResponse([]string{"app"})): false,
	} {
		if got := metadataListPayloadEmpty([]byte(body)); got != want {
			t.Errorf("metadataListPayloadEmpty(%s) = %v, want %v", body, got, want)
		}
	}
}

// When the full-range VictoriaLogs call fails, /labels and /label/{name}/values
// serve the last cached answer for the same request (like the detected-field
// endpoints). Without one they return the error; they never fall back to a
// capped query or a partial list.
func TestLabelsFullRange_BackendErrorServesStaleOrError(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, label, path string
	}{
		{name: "labels", endpoint: "labels", path: "/loki/api/v1/labels"},
		{name: "label_values", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values"},
		{name: "service_name values", endpoint: "label_values", label: "service_name", path: "/loki/api/v1/label/service_name/values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var backendRanges []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				mu.Lock()
				backendRanges = append(backendRanges, r.URL.Query().Get("start")+"/"+r.URL.Query().Get("end"))
				mu.Unlock()
				http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			}))
			t.Cleanup(srv.Close)
			p, mux := newBehaviorProxy(t, srv.URL, 0)

			end := perfBaseTimeNs
			start := end - int64(24*time.Hour)
			path := fmt.Sprintf("%s?start=%d&end=%d", tc.path, start, end)

			// No stale entry: the error is returned.
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code < 400 {
				t.Fatalf("without a cached answer want an error status, got %d: %s", rec.Code, rec.Body.String())
			}
			mu.Lock()
			for _, got := range backendRanges {
				if got != fmt.Sprintf("%d/%d", start, end) {
					mu.Unlock()
					t.Fatalf("backend call used range %s, want only the full range %d/%d", got, start, end)
				}
			}
			mu.Unlock()

			// An expired last known-good answer is served instead of the error.
			req := httptest.NewRequest(http.MethodGet, path, nil)
			var key string
			if tc.label != "" {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req, tc.label)
			} else {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req)
			}
			p.setEndpointReadCacheWithTTL(tc.endpoint, key, lokiLabelsResponse([]string{"cached-a", "cached-b"}), time.Millisecond)
			time.Sleep(5 * time.Millisecond)
			if got := serveLokiStrings(t, mux, path); len(got) != 2 || got[0] != "cached-a" || got[1] != "cached-b" {
				t.Fatalf("stale-on-error response = %v, want the cached answer", got)
			}
		})
	}
}

// The keep-warm loop must use the same labels TTL base the handlers scale and
// compare against (-labels-cache-ttl), not the built-in default.
func TestLabelKeepWarmSchedule_UsesConfiguredLabelsTTL(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	for _, tc := range []struct {
		configured, want time.Duration
	}{
		{configured: 2 * time.Minute, want: 2 * time.Minute},
		{configured: 0, want: CacheTTLs["labels"]},
	} {
		p, _ := newBehaviorProxy(t, srv.URL, tc.configured)
		ttl, interval, skip := p.labelKeepWarmSchedule()
		if ttl != tc.want || interval != tc.want*3/4 || skip != tc.want*2/5 {
			t.Fatalf("-labels-cache-ttl=%v: schedule ttl=%v interval=%v skip=%v, want ttl=%v interval=%v skip=%v",
				tc.configured, ttl, interval, skip, tc.want, tc.want*3/4, tc.want*2/5)
		}
		if ttl != p.cacheTTLLabels && tc.configured != 0 {
			t.Fatalf("keep-warm base %v differs from handler base %v", ttl, p.cacheTTLLabels)
		}
	}
}

// Background refreshers must write entries back with the window-scaled TTL the
// handlers compare remaining TTL against. With the base TTL, every later hit on
// a wide window looks stale and schedules another full-range refresh.
func TestMetadataRefresh_WritesWindowScaledTTL_NoRefreshOnNextHit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
		label    string
		path     string
	}{
		{name: "labels", endpoint: "labels", path: "/loki/api/v1/labels"},
		{name: "label_values", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fullRangeVL{}
			srv := fake.server(t)
			p, mux := newBehaviorProxy(t, srv.URL, 5*time.Minute)

			end := perfBaseTimeNs
			start := end - int64(24*time.Hour)
			startRaw, endRaw := fmt.Sprint(start), fmt.Sprint(end)
			path := fmt.Sprintf("%s?start=%s&end=%s", tc.path, startRaw, endRaw)
			req := httptest.NewRequest(http.MethodGet, path, nil)
			var cacheKey string
			if tc.label != "" {
				cacheKey = p.canonicalReadCacheKey(tc.endpoint, "", req, tc.label)
				p.refreshLabelValuesCacheAsync("", cacheKey, tc.label, "", startRaw, endRaw, "", "", nil)
			} else {
				cacheKey = p.canonicalReadCacheKey(tc.endpoint, "", req)
				p.refreshLabelsCacheAsync("", cacheKey, "", startRaw, endRaw, "", nil)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, _, _, ok := p.endpointReadCacheEntry(tc.endpoint, cacheKey); ok {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("refresh did not populate %s", cacheKey)
				}
				time.Sleep(10 * time.Millisecond)
			}
			waitForBackendIdle(t, &fake.count)

			_, remaining, _, _ := p.endpointReadCacheEntry(tc.endpoint, cacheKey)
			scaled := metadataWindowTTL(startRaw, endRaw, 5*time.Minute)
			if p.shouldRefreshLabelsInBackground(remaining, scaled) {
				t.Fatalf("refresher wrote remaining TTL %v; handler (TTL %v) would refresh on every hit", remaining, scaled)
			}

			before := fake.count.Load()
			_ = serveLokiStrings(t, mux, path)
			waitForBackendIdle(t, &fake.count)
			if extra := fake.count.Load() - before; extra != 0 {
				fake.mu.Lock()
				defer fake.mu.Unlock()
				t.Fatalf("cache hit after refresh scheduled %d backend call(s): %v", extra, fake.calls)
			}
		})
	}
}

// The detected_* refreshers share the same staleness comparison
// (metadataWindowTTL in their handlers) and must write back the scaled TTL too.
func TestMetadataRefresh_DetectedEndpointsWriteWindowScaledTTL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/stream+json")
			_, _ = w.Write([]byte(`{"_time":"2024-01-01T00:00:00Z","_msg":"{\"k\":\"v\"}","_stream":"{app=\"a\"}","app":"a","level":"info"}` + "\n"))
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"app", 1}, {"level", 1}})
		default:
			writeVLFieldValues(w, []fieldHit{{"a", 1}})
		}
	}))
	t.Cleanup(srv.Close)
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	end := perfBaseTimeNs
	startRaw, endRaw := fmt.Sprint(end-int64(24*time.Hour)), fmt.Sprint(end)
	query := `{app="a"}`
	// One refresher at a time: each waits for its own cache write.
	for _, tc := range []struct {
		endpoint, key string
		refresh       func()
	}{
		{"detected_fields", "df:scaled", func() { p.refreshDetectedFieldsCacheAsync("", "df:scaled", query, startRaw, endRaw, 100, nil) }},
		{"detected_labels", "dl:scaled", func() { p.refreshDetectedLabelsCacheAsync("", "dl:scaled", query, startRaw, endRaw, 100, nil) }},
		{"detected_field_values", "dfv:scaled", func() {
			p.refreshDetectedFieldValuesCacheAsync("", "dfv:scaled", "app", query, startRaw, endRaw, 100, nil)
		}},
	} {
		tc.refresh()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, _, _, ok := p.endpointReadCacheEntry(tc.endpoint, tc.key); ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s refresh did not populate cache", tc.endpoint)
			}
			time.Sleep(10 * time.Millisecond)
		}
		body, remaining, _, _ := p.endpointReadCacheEntry(tc.endpoint, tc.key)
		scaled := metadataWindowTTL(startRaw, endRaw, CacheTTLs[tc.endpoint])
		if p.shouldRefreshLabelsInBackground(remaining, scaled) {
			t.Fatalf("%s refresher wrote remaining TTL %v; handler compares against %v", tc.endpoint, remaining, scaled)
		}
		if tc.endpoint == "detected_fields" {
			var payload struct {
				Limit int `json:"limit"`
			}
			if err := json.Unmarshal(body, &payload); err != nil || payload.Limit != 1000 {
				t.Fatalf("detected_fields refresh must keep Loki's limit=1000 like the handler, got %s", body)
			}
		}
	}
}

// Only one backend round-trip per cold /labels request: the synchronous fetch
// already covers the full range, so no follow-up refresh is scheduled.
func TestLabelsFullRange_ColdMissMakesSingleFullRangeCall(t *testing.T) {
	fake := &fullRangeVL{}
	srv := fake.server(t)
	_, mux := newBehaviorProxy(t, srv.URL, 0)

	end := perfBaseTimeNs
	start := end - int64(7*24*time.Hour)
	_ = serveLokiStrings(t, mux, fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", start, end))
	waitForBackendIdle(t, &fake.count)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := fmt.Sprintf("/select/logsql/stream_field_names?field=&start=%d&end=%d", start, end)
	calls := append([]string(nil), fake.calls...)
	sort.Strings(calls)
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("expected exactly one full-range call %q, got %v", want, calls)
	}
}
