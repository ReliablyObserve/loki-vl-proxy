package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: loki_api_v1_query_range, parser-error-and-label-collision
func TestOrderedJSONResultCacheExactRepeat(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	for _, shape := range dlvBaselineShapes {
		t.Run(shape.name, func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, false)
			from, to := base.Add(time.Minute), base.Add(3*time.Minute)
			target, headers := dlvBaselineRequest(shape, from, to)
			want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
			first := doCompatProxyRequest(p, target, headers)
			if got := dlvBaselineReadMatrix(t, first); len(want) == 0 || !reflect.DeepEqual(got, want) {
				t.Fatalf("cold response: got %v want %v", got, want)
			}
			before := fixture.calls.Load()
			layerBefore := p.cache.LayerStats()[cache.LayerLoki].L1
			second := doCompatProxyRequest(p, target, headers)
			if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) || first.Code != second.Code {
				t.Fatalf("exact repeat changed response: first=%d %s second=%d %s", first.Code, first.Body, second.Code, second.Body)
			}
			if after := fixture.calls.Load(); after != before {
				t.Fatalf("exact repeat read backend again: calls %d -> %d", before, after)
			}
			layerAfter := p.cache.LayerStats()[cache.LayerLoki].L1
			if layerAfter.Requests-layerBefore.Requests != 1 || layerAfter.Hits-layerBefore.Hits != 1 || layerAfter.ReadBytes-layerBefore.ReadBytes != int64(first.Body.Len()) {
				t.Fatalf("Loki L1 repeat metrics: before=%+v after=%+v", layerBefore, layerAfter)
			}
		})
	}
}

func orderedJSONResultKey(p *Proxy, target string, headers map[string]string) string {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return p.queryRangeCacheKey(r, r.FormValue("query"))
}

// conformance: tenant-numeric-routing, data-probing-and-freshness
func TestOrderedJSONResultCacheIsolationAndExpiry(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	from, to := base.Add(time.Minute), base.Add(3*time.Minute)
	target, _ := dlvBaselineRequest(shape, from, to)
	keys := map[string]bool{}
	for _, identity := range []struct{ tenant, account, auth, tag string }{
		{"team-a", "10", "alpha", ""},
		{"team-b", "20", "alpha", ""},
		{"team-a", "10", "beta", ""},
		{"team-a", "10", "alpha", "Source=grafana-lokiexplore-app"},
	} {
		headers := map[string]string{"X-Scope-OrgID": identity.tenant, "X-Auth-Token": identity.auth, "X-Query-Tags": identity.tag}
		key := orderedJSONResultKey(p, target, headers)
		if keys[key] {
			t.Fatalf("tenant/auth/profile cache collision for %+v", identity)
		}
		keys[key] = true
		want := dlvBaselineWant(fixture.rows, shape, from, to, identity.account, identity.auth)
		cold := doCompatProxyRequest(p, target, headers)
		if got := dlvBaselineReadMatrix(t, cold); !reflect.DeepEqual(got, want) {
			t.Fatalf("identity %+v: got %v want %v", identity, got, want)
		}
		body, remaining, ok := p.cache.GetWithTTL(key)
		if !ok || !bytes.Equal(body, cold.Body.Bytes()) || remaining <= 4*time.Minute || remaining > 5*time.Minute {
			t.Fatalf("missing/incorrect response entry: found=%t ttl=%s body=%s", ok, remaining, body)
		}
		before := fixture.calls.Load()
		warm := doCompatProxyRequest(p, target, headers)
		if !bytes.Equal(warm.Body.Bytes(), cold.Body.Bytes()) || fixture.calls.Load() != before {
			t.Fatalf("identity %+v did not reuse its own complete reply", identity)
		}
	}
	// A different caller has reused the pooled capture storage since this entry
	// was written. Its bytes must remain owned by the first cached response.
	firstHeaders := map[string]string{"X-Scope-OrgID": "team-a", "X-Auth-Token": "alpha"}
	firstWant := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
	beforeFirst := fixture.calls.Load()
	if got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, firstHeaders)); !reflect.DeepEqual(got, firstWant) || fixture.calls.Load() != beforeFirst {
		t.Fatal("capture-pool reuse changed the first caller's cached answer")
	}
	// Expiry is checked by the real cache, without sleeping or changing query bounds.
	headers := map[string]string{"X-Scope-OrgID": "team-a", "X-Auth-Token": "alpha"}
	before := fixture.calls.Load()
	restore := cache.AdvanceClockForTesting(6 * time.Minute)
	defer restore()
	got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
	want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
	if !reflect.DeepEqual(got, want) || fixture.calls.Load() <= before {
		t.Fatal("expired response was not fetched again correctly")
	}
}

type orderedJSONCancelWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *orderedJSONCancelWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(b)
	w.cancel()
	return n, err
}

// conformance: backend-deadlines-and-cancellation
func TestOrderedJSONResultCacheCancellationAfterEvaluation(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	from, to := base.Add(time.Minute), base.Add(3*time.Minute)
	target, headers := dlvBaselineRequest(shape, from, to)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := &orderedJSONCancelWriter{httptest.NewRecorder(), cancel}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)
	want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
	if got := dlvBaselineReadMatrix(t, w.ResponseRecorder); !reflect.DeepEqual(got, want) || ctx.Err() == nil {
		t.Fatal("fixture did not finish evaluation and cancel at response write")
	}
	if _, _, ok := p.cache.GetWithTTL(orderedJSONResultKey(p, target, headers)); ok {
		t.Fatal("cancelled request published a response-cache entry")
	}
}

func TestOrderedJSONResultCacheOffsetBypass(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	from, to := base.Add(2*time.Minute), base.Add(4*time.Minute)
	target, headers := dlvBaselineRequest(shape, from, to)
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("query", strings.Replace(q.Get("query"), "[1m]", "[1m] offset 1m", 1))
	u.RawQuery = q.Encode()
	target = u.String()
	want := dlvBaselineWant(fixture.rows, shape, from.Add(-time.Minute), to.Add(-time.Minute), "10", "alpha")
	for level, points := range want {
		shifted := map[int64]string{}
		for ts, value := range points {
			shifted[ts+60] = value
		}
		want[level] = shifted
	}
	for range 2 {
		before := fixture.calls.Load()
		got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
		if !reflect.DeepEqual(got, want) || fixture.calls.Load() <= before {
			t.Fatalf("offset response: got %v want %v", got, want)
		}
	}
	if _, _, ok := p.cache.GetWithTTL(orderedJSONResultKey(p, target, headers)); ok {
		t.Fatal("offset admitted pre-rewrite timestamps into result cache")
	}
}

func TestOrderedJSONResultCachePreservesSeriesWarning(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	p.maxStatsQuerySeries = 1
	target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(3*time.Minute))
	headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
	cold := doCompatProxyRequest(p, target, headers)
	var body struct {
		Warnings []string
		Data     struct{ Result []json.RawMessage }
	}
	if cold.Code != http.StatusOK || json.Unmarshal(cold.Body.Bytes(), &body) != nil || len(body.Warnings) != 1 || len(body.Data.Result) != 1 {
		t.Fatalf("expected intentional capped result with warning: %d %s", cold.Code, cold.Body)
	}
	before := fixture.calls.Load()
	warm := doCompatProxyRequest(p, target, headers)
	if warm.Code != cold.Code || !bytes.Equal(cold.Body.Bytes(), warm.Body.Bytes()) || fixture.calls.Load() != before {
		t.Fatalf("cached capped result changed or lost its warning: cold=%s warm=%s", cold.Body, warm.Body)
	}
}

func TestOrderedJSONResultCacheKeepsLocalStoragePolicy(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	from, to := base.Add(time.Minute), base.Add(3*time.Minute)
	target, headers := dlvBaselineRequest(shape, from, to)
	key := orderedJSONResultKey(p, target, headers)
	c, disk, peer, remoteSets := newLocalOnlyCacheFixture(t, key)
	p.cache = c
	want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
	if got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers)); !reflect.DeepEqual(got, want) {
		t.Fatalf("cold: got %v want %v", got, want)
	}
	if _, _, ok := c.GetWithTTL(key); !ok {
		t.Fatal("complete result missing from local RAM")
	}
	if _, ok := disk.Get(key); ok || remoteSets.Load() != 0 || peer.WTPushes.Load() != 0 {
		t.Fatal("full response changed its existing local-only storage policy")
	}
	before := fixture.calls.Load()
	if got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers)); !reflect.DeepEqual(got, want) || fixture.calls.Load() != before {
		t.Fatal("local-only result was not reusable")
	}
}

// conformance: data-probing-and-freshness
func TestOrderedJSONResultCacheNearNowRefresh(t *testing.T) {
	for _, maxStaleness := range []time.Duration{time.Second, 15 * time.Second} {
		t.Run(maxStaleness.String(), func(t *testing.T) {
			testOrderedJSONResultCacheNearNowRefresh(t, maxStaleness)
		})
	}
}

func testOrderedJSONResultCacheNearNowRefresh(t *testing.T, maxStaleness time.Duration) {
	t.Helper()
	base := time.Now().UTC().Truncate(time.Minute).Add(-3 * time.Minute)
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	p.recentTailRefreshEnabled = true
	p.recentTailRefreshMaxStaleness = maxStaleness
	from, to := base.Add(time.Minute), base.Add(3*time.Minute)
	target, headers := dlvBaselineRequest(shape, from, to)
	cold := doCompatProxyRequest(p, target, headers)
	dlvBaselineReadMatrix(t, cold)
	before := fixture.calls.Load()
	warm := doCompatProxyRequest(p, target, headers)
	if !bytes.Equal(cold.Body.Bytes(), warm.Body.Bytes()) || fixture.calls.Load() != before {
		t.Fatal("fresh near-now response was not reused")
	}
	// Leave room for real handler time on loaded/race CI workers: the test
	// clock advances wall time rather than freezing it at an exact instant.
	func() {
		restore := cache.AdvanceClockForTesting(maxStaleness / 2)
		defer restore()
		stillFresh := doCompatProxyRequest(p, target, headers)
		if !bytes.Equal(cold.Body.Bytes(), stillFresh.Body.Bytes()) || fixture.calls.Load() != before {
			t.Fatal("entry below the configured max-staleness was not reused")
		}
	}()
	restore := cache.AdvanceClockForTesting(maxStaleness + time.Second)
	defer restore()
	refreshed := doCompatProxyRequest(p, target, headers)
	if !bytes.Equal(cold.Body.Bytes(), refreshed.Body.Bytes()) || fixture.calls.Load() <= before {
		t.Fatal("near-now response exceeding max-staleness was not fetched again")
	}
}

func TestOrderedJSONResultCacheExclusions(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	for _, mode := range []string{"streaming", "oversized", "disabled", "tier0"} {
		t.Run(mode, func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, mode == "tier0")
			switch mode {
			case "streaming":
				p.streamResponse = true
			case "oversized":
				p.cache = cache.NewWithMaxBytes(time.Hour, 10, 128)
				t.Cleanup(p.cache.Close)
			case "disabled":
				p.cache = cache.NewDisabled()
			}
			from, to := base.Add(time.Minute), base.Add(3*time.Minute)
			target, headers := dlvBaselineRequest(shape, from, to)
			first := doCompatProxyRequest(p, target, headers)
			want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
			if got := dlvBaselineReadMatrix(t, first); !reflect.DeepEqual(got, want) {
				t.Fatalf("cold response: got %v want %v", got, want)
			}
			before := fixture.calls.Load()
			second := doCompatProxyRequest(p, target, headers)
			if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatal("uncacheable/reused reply changed")
			}
			if _, _, ok := p.cache.GetWithTTL(orderedJSONResultKey(p, target, headers)); ok {
				t.Fatal("response unexpectedly admitted into primary cache")
			}
			if mode == "tier0" {
				if fixture.calls.Load() != before {
					t.Fatal("Tier0 repeat read backend")
				}
			} else if fixture.calls.Load() <= before {
				t.Fatal("uncacheable response bypassed backend")
			}
		})
	}
}

func TestOrderedJSONResultCacheFailureRecovery(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	for _, mode := range []string{"backend", "row-limit", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, false)
			from, to := base.Add(time.Minute), base.Add(3*time.Minute)
			target, headers := dlvBaselineRequest(shape, from, to)
			key := orderedJSONResultKey(p, target, headers)
			var failed *httptest.ResponseRecorder
			switch mode {
			case "backend":
				fixture.fail.Store(true)
				failed = doCompatProxyRequest(p, target, headers)
				fixture.fail.Store(false)
			case "row-limit":
				p.rangeMetricRowLimit = 1
				failed = doCompatProxyRequest(p, target, headers)
				p.rangeMetricRowLimit = 100
			case "cancelled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
				for k, v := range headers {
					r.Header.Set(k, v)
				}
				failed = httptest.NewRecorder()
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				mux.ServeHTTP(failed, r)
			}
			if failed.Code < http.StatusBadRequest {
				t.Fatalf("failure became success: %d %s", failed.Code, failed.Body)
			}
			if _, _, ok := p.cache.GetWithTTL(key); ok {
				t.Fatal("failed answer entered result cache")
			}
			good := doCompatProxyRequest(p, target, headers)
			want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
			if got := dlvBaselineReadMatrix(t, good); !reflect.DeepEqual(got, want) {
				t.Fatalf("recovery: got %v want %v", got, want)
			}
			before := fixture.calls.Load()
			warm := doCompatProxyRequest(p, target, headers)
			if !bytes.Equal(good.Body.Bytes(), warm.Body.Bytes()) || fixture.calls.Load() != before {
				t.Fatal("recovered response did not become reusable")
			}
		})
	}
}
