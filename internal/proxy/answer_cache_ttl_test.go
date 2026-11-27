package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// answerTTLBuiltins are the built-in TTLs the settings may only shorten, as
// they stood before the settings existed.
var answerTTLBuiltins = map[string]time.Duration{
	"query_range": 5 * time.Minute, "query": 5 * time.Minute,
	"labels": 5 * time.Minute, "label_values": 5 * time.Minute, "series": 30 * time.Second,
	"detected_fields": 90 * time.Second, "detected_field_values": 90 * time.Second, "detected_labels": 90 * time.Second,
	"index_stats": 10 * time.Second, "volume": 10 * time.Second, "volume_range": 10 * time.Second,
}

func newAnswerTTLProxy(t *testing.T, ttls map[string]time.Duration, opts ...testProxyOption) *Proxy {
	t.Helper()
	p := newTestProxyWithOptions(t, append([]testProxyOption{func(c *Config) { c.AnswerCacheTTLs = ttls }}, opts...)...)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// A setting may keep or shorten a class's TTL, never lengthen it; patterns
// and the classes with their own flags are no such class.
//
// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLSettingsOnlyShorten(t *testing.T) {
	if len(AnswerCacheTTLClasses) != len(answerTTLBuiltins) {
		t.Fatalf("classes %v, want %d", AnswerCacheTTLClasses, len(answerTTLBuiltins))
	}
	for _, class := range AnswerCacheTTLClasses {
		builtin := answerTTLBuiltins[class]
		if got, ok := AnswerCacheTTLMax(class); !ok || got != builtin {
			t.Fatalf("%s: max %s %v, want the built-in %s", class, got, ok, builtin)
		}
		for _, ok := range []time.Duration{0, time.Nanosecond, time.Second, builtin} {
			if err := ValidateAnswerCacheTTL(class, ok); err != nil {
				t.Fatalf("%s=%s refused: %v", class, ok, err)
			}
		}
		for _, bad := range []time.Duration{-time.Second, builtin + time.Nanosecond, 24 * time.Hour} {
			err := ValidateAnswerCacheTTL(class, bad)
			if err == nil {
				t.Fatalf("%s=%s accepted", class, bad)
			}
			if !strings.Contains(err.Error(), builtin.String()) {
				t.Fatalf("%s=%s: error %q does not name the built-in %s", class, bad, err, builtin)
			}
			if _, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 10), LogLevel: "error", AnswerCacheTTLs: map[string]time.Duration{class: bad}}); err == nil {
				t.Fatalf("New accepted %s=%s", class, bad)
			}
		}
	}
	for _, other := range []string{"patterns", "label_inventory", "labels_handler", "metadata_negative", "query_range_extent", ""} {
		if err := ValidateAnswerCacheTTL(other, time.Second); err == nil {
			t.Fatalf("%q accepted as an answer class", other)
		}
	}
}

func answerTTLKeyRequests() map[string]*http.Request {
	old := time.Now().Add(-72 * time.Hour)
	window := "start=" + strconv.FormatInt(old.UnixNano(), 10) + "&end=" + strconv.FormatInt(old.Add(time.Hour).UnixNano(), 10)
	q := "query=" + url.QueryEscape(`{app="api"}`)
	return map[string]*http.Request{
		"query_range":           httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q+"&"+window+"&step=60", nil),
		"query":                 httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query="+url.QueryEscape(`count_over_time({app="api"}[5m])`), nil),
		"labels":                httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?"+window, nil),
		"label_values":          httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values?"+window, nil),
		"series":                httptest.NewRequest(http.MethodGet, "/loki/api/v1/series?match[]="+url.QueryEscape(`{app="api"}`)+"&"+window, nil),
		"detected_fields":       httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_fields?"+q+"&"+window, nil),
		"detected_field_values": httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_field/level/values?"+q+"&"+window, nil),
		"detected_labels":       httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_labels?"+q+"&"+window, nil),
		"index_stats":           httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/stats?"+q+"&"+window, nil),
		"volume":                httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume?"+q+"&"+window, nil),
		"volume_range":          httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume_range?"+q+"&"+window+"&step=60", nil),
	}
}

// answerTTLKeys are the shared-tier keys of a class: the single-tenant read
// cache key, the multi-tenant merge key and the field or label detection
// cache key.
func answerTTLKeys(p *Proxy, class string, r *http.Request) []string {
	keys := []string{p.canonicalReadCacheKey(class, "team-a", r)}
	mt := r.Clone(r.Context())
	mt.Header.Set("X-Scope-OrgID", "team-a|team-b")
	if key, ok := p.multiTenantCacheKey(mt, class); ok {
		keys = append(keys, key)
	}
	switch class {
	case "detected_fields":
		keys = append(keys, p.detectedFieldsCacheKey(context.Background(), `{app="api"}`, "1", "2", 1000))
	case "detected_labels":
		keys = append(keys, p.detectedLabelsCacheKey(context.Background(), `{app="api"}`, "1", "2", 1000))
	}
	return keys
}

// Unset, and set to the built-in value, every class keeps its TTLs and the
// keys of a proxy without the settings.
//
// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLUnsetKeepsTTLsAndKeys(t *testing.T) {
	unset := newAnswerTTLProxy(t, nil)
	builtin := map[string]time.Duration{}
	zero := map[string]time.Duration{}
	for class, ttl := range answerTTLBuiltins {
		builtin[class], zero[class] = ttl, 0
	}
	for name, ttls := range map[string]map[string]time.Duration{"zero": zero, "built-in": builtin} {
		p := newAnswerTTLProxy(t, ttls)
		if len(p.answerCacheTTLs) != 0 {
			t.Fatalf("%s: kept %v", name, p.answerCacheTTLs)
		}
		for class, r := range answerTTLKeyRequests() {
			if got := p.compatAnswerTTL(class); got != answerTTLBuiltins[class] {
				t.Fatalf("%s %s: answer cache TTL %s, want %s", name, class, got, answerTTLBuiltins[class])
			}
			if got := p.endpointTTL(class); got != CacheTTLs[class] {
				t.Fatalf("%s %s: read cache TTL %s, want %s", name, class, got, CacheTTLs[class])
			}
			want, got := answerTTLKeys(unset, class, r), answerTTLKeys(p, class, r)
			for i := range want {
				if got[i] != want[i] || strings.Contains(got[i], "@ttl=") {
					t.Fatalf("%s %s: key %q, want %q", name, class, got[i], want[i])
				}
			}
		}
	}
	if unset.endpointTTL("patterns") != patternsCacheRetention || unset.answerTTLKeyFence("patterns") != "" {
		t.Fatal("patterns changed")
	}
}

// A shortened class is stored with the shorter TTL by every cache of the
// class, the TTL gauge states it, and its shared-tier keys carry it; the
// other classes are untouched.
//
// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLShortensEveryCacheOfTheClass(t *testing.T) {
	for _, class := range AnswerCacheTTLClasses {
		t.Run(class, func(t *testing.T) {
			unset := newAnswerTTLProxy(t, nil)
			p := newAnswerTTLProxy(t, map[string]time.Duration{class: time.Second})
			for _, other := range AnswerCacheTTLClasses {
				wantCompat, wantRead, wantFence := answerTTLBuiltins[other], CacheTTLs[other], ""
				if other == class {
					wantCompat, wantRead, wantFence = time.Second, time.Second, "@ttl=1s"
				}
				if got := p.compatAnswerTTL(other); got != wantCompat {
					t.Fatalf("%s: answer cache TTL %s, want %s", other, got, wantCompat)
				}
				if got := p.endpointTTL(other); got != wantRead {
					t.Fatalf("%s: read cache TTL %s, want %s", other, got, wantRead)
				}
				if got := p.answerTTLKeyFence(other); got != wantFence {
					t.Fatalf("%s: fence %q, want %q", other, got, wantFence)
				}
			}
			gauge := map[string]time.Duration{}
			for _, entry := range p.cacheTTLs() {
				gauge[entry.Class] = entry.TTL
			}
			if gauge[class] != time.Second || gauge["patterns"] != patternsCacheRetention || gauge["labels_handler"] != CacheTTLs["labels"] {
				t.Fatalf("gauge %v", gauge)
			}
			r := answerTTLKeyRequests()[class]
			want, got := answerTTLKeys(unset, class, r), answerTTLKeys(p, class, r)
			for i := range want {
				fenced := strings.Contains(got[i], "@ttl=1s")
				// The label handlers' own keys keep -labels-cache-ttl's class;
				// only their multi-tenant merge is stored with this TTL.
				handlerKey := i == 0 && (class == "labels" || class == "label_values" || class == "series" || class == "query_range" || class == "query")
				if fenced == handlerKey || (got[i] == want[i]) == fenced {
					t.Fatalf("key %d %q (unset %q): fenced=%v", i, got[i], want[i], fenced)
				}
			}
			if strings.Count(got[len(got)-1], "@ttl=") > 1 {
				t.Fatalf("key fenced twice: %q", got[len(got)-1])
			}
		})
	}
	// The multi-tenant query_range merge keeps the shorter of the setting and
	// its 10 s, and its key changes only when its TTL does.
	p := newAnswerTTLProxy(t, map[string]time.Duration{"query_range": 30 * time.Second})
	if p.compatAnswerTTL("query_range") != 30*time.Second || p.endpointTTL("query_range") != 10*time.Second || p.answerTTLKeyFence("query_range") != "" {
		t.Fatalf("query_range 30s: answer %s merge %s fence %q", p.compatAnswerTTL("query_range"), p.endpointTTL("query_range"), p.answerTTLKeyFence("query_range"))
	}
	// An empty merged label list keeps the negative TTL unless the class is
	// shorter.
	empty := []byte(`{"status":"success","data":[]}`)
	for _, tc := range []struct {
		ttls map[string]time.Duration
		want time.Duration
	}{{nil, metadataNegativeCacheTTL}, {map[string]time.Duration{"labels": 10 * time.Second}, 10 * time.Second}, {map[string]time.Duration{"labels": 4 * time.Minute}, metadataNegativeCacheTTL}} {
		p := newAnswerTTLProxy(t, tc.ttls)
		p.setMultiTenantMergeCache("labels", "mt:empty", empty)
		if _, remaining, ok := p.cache.GetWithTTL("mt:empty"); !ok || remaining > tc.want || remaining < tc.want-time.Second {
			t.Fatalf("%v: empty merged labels kept %s (%v), want %s", tc.ttls, remaining, ok, tc.want)
		}
	}
	// The label detection cache under /detected_labels keeps its 30 s unless
	// the setting is shorter, and its key changes only then.
	for _, tc := range []struct {
		setting, want time.Duration
		fenced        bool
	}{{60 * time.Second, 30 * time.Second, false}, {10 * time.Second, 10 * time.Second, true}} {
		p := newAnswerTTLProxy(t, map[string]time.Duration{"detected_labels": tc.setting})
		if got := p.shortenedTTL("detected_labels", detectedLabelsDetectionTTL); got != tc.want {
			t.Fatalf("detected_labels %s: detection TTL %s, want %s", tc.setting, got, tc.want)
		}
		if key := p.detectedLabelsCacheKey(context.Background(), "{}", "1", "2", 1); strings.Contains(key, "@ttl=") != tc.fenced {
			t.Fatalf("detected_labels %s: detection key %q", tc.setting, key)
		}
	}
	// The window-scaled metadata TTLs scale the shortened base.
	start, end := strconv.FormatInt(time.Now().Add(-24*time.Hour).UnixNano(), 10), strconv.FormatInt(time.Now().UnixNano(), 10)
	p = newAnswerTTLProxy(t, map[string]time.Duration{"detected_fields": 9 * time.Second})
	if got, base := metadataWindowTTL(start, end, p.endpointTTL("detected_fields")), metadataWindowTTL(start, end, CacheTTLs["detected_fields"]); got*10 != base {
		t.Fatalf("24h detected_fields TTL %s, built-in %s", got, base)
	}
}

type countingIndexStatsVL struct{ calls atomic.Int64 }

func (f *countingIndexStatsVL) server(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/query" || !strings.Contains(r.Form.Get("query"), "| stats count() entries") {
			http.Error(w, "unexpected request "+r.URL.Path, http.StatusTeapot)
			return
		}
		f.calls.Add(1)
		_, _ = w.Write([]byte(`{"entries":"3","streams":"1"}` + "\n"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func answerTTLIndexStatsTarget() string {
	old := time.Now().Add(-72 * time.Hour).Truncate(time.Hour)
	return "/loki/api/v1/index/stats?" + url.Values{
		"query": {`{app="api"}`}, "start": {strconv.FormatInt(old.UnixNano(), 10)}, "end": {strconv.FormatInt(old.Add(time.Hour).UnixNano(), 10)},
	}.Encode()
}

func getAnswerTTLIndexStats(t *testing.T, p *Proxy) {
	t.Helper()
	if w := doCompatProxyRequest(p, answerTTLIndexStatsTarget(), map[string]string{"X-Scope-OrgID": "0"}); w.Code != http.StatusOK {
		t.Fatalf("index/stats status=%d body=%s", w.Code, w.Body)
	}
}

// Through the handler: a shortened TTL expires the answer when it says, the
// built-in TTL keeps serving it.
//
// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLExpiresAtTheShortenedTTL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ttls      map[string]time.Duration
		wantCalls int64
	}{
		{"unset", nil, 1},
		{"index_stats 4s", map[string]time.Duration{"index_stats": 4 * time.Second}, 2},
		{"another class", map[string]time.Duration{"volume": 4 * time.Second}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := &countingIndexStatsVL{}
			p := newAnswerTTLProxy(t, tc.ttls, func(c *Config) { c.BackendURL = vl.server(t).URL })
			getAnswerTTLIndexStats(t, p)
			getAnswerTTLIndexStats(t, p)
			if got := vl.calls.Load(); got != 1 {
				t.Fatalf("repeat within the TTL made %d VictoriaLogs calls, want 1", got)
			}
			restore := cache.AdvanceClockForTesting(5 * time.Second)
			defer restore()
			getAnswerTTLIndexStats(t, p)
			if got := vl.calls.Load(); got != tc.wantCalls {
				t.Fatalf("after 5s: %d VictoriaLogs calls, want %d", got, tc.wantCalls)
			}
		})
	}
	// The same through the Loki answer cache (series, built-in 30s).
	for _, tc := range []struct {
		name      string
		ttls      map[string]time.Duration
		wantCalls int
	}{{"unset", nil, 1}, {"series 5s", map[string]time.Duration{"series": 5 * time.Second}, 2}} {
		t.Run("compat "+tc.name, func(t *testing.T) {
			var calls int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\"}","hits":1}]}`))
			}))
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 100), CompatCache: cache.New(time.Minute, 100), LogLevel: "error", AnswerCacheTTLs: tc.ttls})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			old := time.Now().Add(-72 * time.Hour).Truncate(time.Hour)
			target := "/loki/api/v1/series?" + url.Values{"match[]": {`{app="api"}`}, "start": {strconv.FormatInt(old.Unix(), 10)}, "end": {strconv.FormatInt(old.Add(time.Hour).Unix(), 10)}}.Encode()
			doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
			doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
			if calls != 1 {
				t.Fatalf("repeat within the TTL made %d backend calls, want 1", calls)
			}
			restore := cache.AdvanceClockForTesting(6 * time.Second)
			defer restore()
			if w := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); w.Code != http.StatusOK {
				t.Fatalf("status=%d", w.Code)
			}
			if calls != tc.wantCalls {
				t.Fatalf("after 6s: %d backend calls, want %d", calls, tc.wantCalls)
			}
		})
	}
}

// Replicas, or a restart, with different settings on one disk tier: an
// entry written under the built-in TTL is never served by a process with a
// shorter one (it could be older than the shorter TTL), and processes with
// the same setting share entries again. Peers address entries by the same
// keys, so the same holds across replicas.
//
// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLKeysFenceTheSharedTiers(t *testing.T) {
	disk, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "l2.db"), MinTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = disk.Close() }()
	vl := &countingIndexStatsVL{}
	backend := vl.server(t).URL
	process := func(ttls map[string]time.Duration) *Proxy {
		l1 := cache.New(time.Minute, 100)
		l1.SetL2(disk)
		return newAnswerTTLProxy(t, ttls, func(c *Config) { c.BackendURL = backend; c.Cache = l1 })
	}
	shortened := map[string]time.Duration{"index_stats": 5 * time.Second}
	for i, step := range []struct {
		name      string
		ttls      map[string]time.Duration
		wantCalls int64
	}{
		{"built-in TTL writes", nil, 1},
		{"shortened TTL does not read the built-in entry", shortened, 2},
		{"same shortened TTL reads it from disk", shortened, 2},
		{"built-in TTL still reads its own entry", nil, 2},
		{"another shortened TTL reads neither", map[string]time.Duration{"index_stats": 3 * time.Second}, 3},
	} {
		getAnswerTTLIndexStats(t, process(step.ttls))
		disk.Flush()
		if got := vl.calls.Load(); got != step.wantCalls {
			t.Fatalf("step %d (%s): %d VictoriaLogs calls, want %d", i, step.name, got, step.wantCalls)
		}
	}
}

// The same across replicas through the real peer protocol: every key is owned
// by one peer, the replicas push their answers to it (write-through) and read
// it on a memory miss. A replica with a shortened TTL never reads the entry a
// replica with the built-in TTL pushed, and replicas with the same setting
// share theirs.
//
// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLKeysFenceThePeerTier(t *testing.T) {
	owner := cache.New(time.Minute, 100)
	t.Cleanup(owner.Close)
	server := cache.NewPeerCache(cache.PeerConfig{})
	t.Cleanup(server.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { server.ServeHTTP(w, r, owner) }))
	t.Cleanup(srv.Close)
	vl := &countingIndexStatsVL{}
	backend := vl.server(t).URL
	replica := func(i int, ttls map[string]time.Duration) *Proxy {
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "replica-" + strconv.Itoa(i) + ":3100", DiscoveryType: "static", StaticPeers: strings.TrimPrefix(srv.URL, "http://"),
			Timeout: 5 * time.Second, WriteThrough: true, WriteThroughMinTTL: time.Second})
		t.Cleanup(pc.Close)
		l1 := cache.New(time.Minute, 100)
		l1.SetL3(pc)
		return newAnswerTTLProxy(t, ttls, func(c *Config) { c.BackendURL = backend; c.Cache = l1; c.PeerCache = pc })
	}
	r := httptest.NewRequest(http.MethodGet, answerTTLIndexStatsTarget(), nil)
	r.Header.Set("X-Scope-OrgID", "0")
	waitOwner := func(p *Proxy) {
		t.Helper()
		key := p.canonicalReadCacheKey("index_stats", "0", r)
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if _, ok := owner.Peek(key); ok {
				return
			}
		}
		t.Fatalf("the owner never received %q", key)
	}
	// A peer serves an entry with at least 5 s left, so the shortened TTL is 8 s.
	shortened := map[string]time.Duration{"index_stats": 8 * time.Second}
	for i, step := range []struct {
		name      string
		ttls      map[string]time.Duration
		wantCalls int64
		pushes    bool
	}{
		{"built-in TTL pushes to the owner", nil, 1, true},
		{"shortened TTL does not read the pushed entry", shortened, 2, true},
		{"same shortened TTL reads it from the owner", shortened, 2, false},
		{"built-in TTL still reads its own", nil, 2, false},
	} {
		p := replica(i, step.ttls)
		getAnswerTTLIndexStats(t, p)
		if step.pushes {
			waitOwner(p)
		}
		if got := vl.calls.Load(); got != step.wantCalls {
			t.Fatalf("step %d (%s): %d VictoriaLogs calls, want %d", i, step.name, got, step.wantCalls)
		}
	}
}
