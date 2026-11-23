package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOrderedJSONVectorResponseVersionScope(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/?start=1767225600&end=1767225900&step=60", nil)
	for _, query := range []string{
		`max(sum by(cluster)(rate({app="a"}|json[1m])))`,
		`topk(1,sum by(cluster)(count_over_time({app="a"}|json[1m])))`,
		`avg(sum by(cluster)(rate({app="a"}|json[$__auto])))`,
		`max(sum by(cluster)(rate({app="a"}|json[1m] offset 1h)))`,
		`max(sum by(cluster)(rate({app="a"}|json[1m]))) + 1`,
		`sum(max(sum by(cluster)(rate({app="a"}|json[1m]))) + 1)`,
		`sort_desc(max(sum by(cluster)(rate({app="a"}|json[1m]))))`,
		`stddev(max(sum by(cluster)(rate({app="a"}|json[1m]))))`,
		`max(sum by(cluster)(rate({app="a"}|json[1m] offset 0s)))`,
	} {
		if got := orderedJSONVectorResponseKeyVersion(r, query); got != orderedJSONVectorResponseVersion {
			t.Fatalf("unfenced nested expression: %s version=%q", query, got)
		}
		if queryStepsShape(query, 2*time.Minute) {
			t.Fatalf("affected expression admitted to old generic fragments: %s", query)
		}
	}
	for _, query := range []string{
		`{app="a"}`, `sum by(cluster)(rate({app="a"}|json[1m]))`,
		`max(sum by(cluster)(rate({app="a"}|logfmt[1m])))`,
		`max(sum by(cluster)(rate({app="a"}[1m])))`,
	} {
		if got := orderedJSONVectorResponseKeyVersion(r, query); got != "" {
			t.Fatalf("unrelated key would change: %s version=%q", query, got)
		}
	}
}

// Old empty answers from the parser-stripping burst path must not be reused
// through the exact-response caches or the settled-step cache after upgrade.
// conformance: semantics/logfmt-existence-filter-ungrouped-sum
func TestLogfmtPresenceResponseVersionScope(t *testing.T) {
	query := `sum(count_over_time({app="a"} | logfmt | op!="" [5m]))`
	params := url.Values{"query": {query}, "start": {"1767225600"}, "end": {"1767225900"}, "step": {"60"}}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	if got := logfmtPresenceResponseKeyVersion(r, query); got != logfmtPresenceResponseVersion {
		t.Fatalf("affected query has version %q", got)
	}
	p := newCompatTestProxy(t, "http://unused")
	if key := p.queryRangeCacheKey(r, query); !strings.Contains(key, logfmtPresenceResponseVersion) {
		t.Fatalf("exact and settled-step cache key is not fenced: %s", key)
	}
	if key, ok := p.compatCacheKey("query_range", r); !ok || !strings.Contains(key, logfmtPresenceResponseVersion) {
		t.Fatalf("compat cache key is not fenced: %s", key)
	}
	for _, other := range []string{
		`sum by(op)(count_over_time({app="a"} | logfmt | op!="" [5m]))`,
		`sum(count_over_time({app="a"} | json | op!="" [5m]))`,
		`sum(count_over_time({app="a"} | logfmt [5m]))`,
	} {
		if got := logfmtPresenceResponseKeyVersion(r, other); got != "" {
			t.Fatalf("unrelated query %q has version %q", other, got)
		}
	}
}

// An old exact reply can already be in L1 after peer hot read-ahead, even
// though the response handler itself only looks in L1. The namespace, rather
// than TTL or placement, must stop that reply from bypassing the fixed parser.
// conformance: semantics/pipeline-error-text-parity
func TestOrderedJSONVectorResponseCacheFencesOldCopies(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShape{name: "nested-valid", pipeline: `| json | __error__=""`}
	for _, setup := range []struct {
		mode     string
		tier0    bool
		offset   time.Duration
		addition bool
		mixed    bool
	}{
		{"query_range", false, 0, false, false}, {"query_range", true, 0, false, false}, {"query", true, 0, false, false},
		{"query_range", false, time.Minute, false, false}, {"query_range", true, time.Minute, false, false}, {"query", true, time.Minute, false, false},
		{"query_range", true, 0, true, false}, {"query", true, 0, true, false},
		{"query_range", false, 0, false, true}, {"query_range", true, 0, true, true}, {"query", true, 0, true, true},
	} {
		t.Run(setup.mode+"/tier0="+strconv.FormatBool(setup.tier0)+"/offset="+setup.offset.String()+"/addition="+strconv.FormatBool(setup.addition)+"/mixed="+strconv.FormatBool(setup.mixed), func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, setup.tier0)
			// The hazard probe runs beside the foreign-level probe, whose verdict
			// can end the pushdown before the hazard probe is answered.
			origins := &vlCallOrigins{inner: p.client.Transport, hold: 30 * time.Millisecond, holdIf: func(query string) bool {
				return strings.Contains(query, jsonLokiRejectHazard)
			}}
			p.client.Transport = origins
			seen := map[string]bool{}
			for n, identity := range []struct{ tenant, account, credential string }{{"team-a", "10", "alpha"}, {"team-b", "20", "alpha"}, {"team-a", "10", "beta"}} {
				from, to := base.Add(time.Minute), base.Add(3*time.Minute)
				query := `max(sum by(detected_level)(count_over_time({app="api"}|json|__error__=""[1m])))`
				if setup.mixed {
					query = strings.Replace(query, "|json|", "|json|logfmt|", 1)
				}
				if setup.offset != 0 {
					query = strings.Replace(query, "[1m]", "[1m] offset "+strconv.FormatInt(int64(setup.offset/time.Second), 10)+"s", 1)
				}
				if setup.addition {
					query += " + 1"
				}
				params := url.Values{"query": {query}, "start": {strconv.FormatInt(from.UnixNano(), 10)}, "end": {strconv.FormatInt(to.UnixNano(), 10)}, "step": {"60"}, "limit": {"100"}}
				if setup.mode == "query" {
					from = to
					params.Set("time", strconv.FormatInt(to.UnixNano(), 10))
				}
				target := "/loki/api/v1/" + setup.mode + "?" + params.Encode()
				headers := map[string]string{"X-Scope-OrgID": identity.tenant, "X-Auth-Token": identity.credential}
				r := httptest.NewRequest(http.MethodGet, target, nil)
				for key, value := range headers {
					r.Header.Set(key, value)
				}
				key := p.queryRangeCacheKey(r, query)
				store := p.cache
				if setup.tier0 {
					var ok bool
					key, ok = p.compatCacheKey(setup.mode, r)
					if !ok {
						t.Fatal("Tier0 not active")
					}
					store = p.compatCache
				}
				if !strings.Contains(key, orderedJSONVectorResponseVersion) || seen[key] {
					t.Fatalf("missing version or tenant/auth collision: %s", key)
				}
				seen[key] = true
				legacy := strings.Replace(key, orderedJSONVectorResponseVersion, "", 1)
				if setup.mixed {
					if !strings.Contains(key, mixedParserMetricResponseVersion) {
						t.Fatal("missing mixed-parser response fence")
					}
					legacy = strings.Replace(legacy, mixedParserMetricResponseVersion, "", 1)
				}
				bad := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000460,"999999"]]}]}}`)
				if setup.mode == "query" {
					bad = []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000580,"999999"]}]}}`)
				}
				store.SetLocalOnlyWithTTL(legacy, bad, 5*time.Minute)
				before := fixture.calls.Load()
				// Each client request is tagged, so the calls made for it are
				// counted apart from the ones an earlier request left in flight.
				firstOrigin, repeatOrigin := "first-"+strconv.Itoa(n), "repeat-"+strconv.Itoa(n)
				first := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, firstOrigin))
				if first.Code != 200 || bytes.Equal(first.Body.Bytes(), bad) || fixture.calls.Load() <= before || origins.made(firstOrigin) == 0 {
					t.Fatalf("old reply bypassed evaluator: status=%d body=%s", first.Code, first.Body)
				}
				var response struct {
					Status string
					Data   struct {
						ResultType string
						Result     []struct {
							Metric map[string]string
							Value  []json.RawMessage
							Values [][]json.RawMessage
						}
					}
				}
				if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.Status != "success" || len(response.Data.Result) != 1 || len(response.Data.Result[0].Metric) != 0 {
					t.Fatalf("unexpected result: %s", first.Body)
				}
				want := map[int64]int{}
				for _, points := range dlvBaselineWant(fixture.rows, shape, from.Add(-setup.offset), to.Add(-setup.offset), identity.account, identity.credential) {
					for stamp, raw := range points {
						stamp += int64(setup.offset / time.Second)
						n, err := strconv.Atoi(raw)
						if err != nil {
							t.Fatal(err)
						}
						want[stamp] = max(want[stamp], n)
					}
				}
				if setup.addition {
					for stamp, n := range want {
						want[stamp] = n + 1
					}
				}
				points := response.Data.Result[0].Values
				if setup.mode == "query" {
					points = [][]json.RawMessage{response.Data.Result[0].Value}
				}
				if len(points) != len(want) {
					t.Fatalf("wrong points: %s want=%v", first.Body, want)
				}
				for _, point := range points {
					var stamp int64
					var raw string
					if len(point) != 2 || json.Unmarshal(point[0], &stamp) != nil || json.Unmarshal(point[1], &raw) != nil {
						t.Fatalf("invalid point: %s", point)
					}
					n, err := strconv.Atoi(raw)
					if err != nil || n != want[stamp] {
						t.Fatalf("wrong aggregate point %s want=%v", point, want)
					}
				}
				if setup.offset != 0 && !setup.tier0 {
					// The range handler disables primary exact-cache writes after
					// shifting the axis; its earlier lookup must still be fenced.
					continue
				}
				if stored, _, ok := store.GetWithTTL(key); !ok || !bytes.Equal(stripQueryStats(stored), first.Body.Bytes()) {
					t.Fatal("correct new reply not retained under new identity")
				}
				served := len(fixture.requestsSince(0))
				// A probe the first request abandoned may be served while the
				// repeat runs; it is the first request's call, not the repeat's.
				origins.deliverAbandoned()
				second := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, repeatOrigin))
				if second.Code != 200 || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) || origins.made(repeatOrigin) != 0 {
					t.Fatalf("new exact reply not reused: first=%s second=%s repeat made %d backend calls; backend served since the first: %q", first.Body, second.Body, origins.made(repeatOrigin), fixture.requestsSince(served))
				}
				if !setup.tier0 {
					// The control: with the one copy gone, the same count sees the
					// request evaluated again.
					store.Invalidate(key)
					control := "control-" + strconv.Itoa(n)
					third := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, control))
					if third.Code != 200 || !bytes.Equal(third.Body.Bytes(), first.Body.Bytes()) || origins.made(control) == 0 {
						t.Fatalf("evaluated again: status=%d body=%s backend calls %d", third.Code, third.Body, origins.made(control))
					}
				}
			}
			if untagged := origins.made(""); untagged != 0 {
				t.Fatalf("%d backend calls were made for no client request", untagged)
			}
		})
	}
}

// vlCallOriginHeader names, on a client request of a test, which request it
// is; vlCallOrigins reads it back from the VictoriaLogs calls the proxy makes
// for that request.
const vlCallOriginHeader = "X-Test-Request-Seq"

// vlCallOrigins counts the VictoriaLogs calls the proxy makes per client
// request, by the vlCallOriginHeader of the request each call is made for, so
// a call that reaches the backend after its own request has answered is not
// charged to the next one. With hold set, a call holdIf picks that its request
// gives up on within hold is kept and delivered by
// deliverAbandoned: a probe already written to the connection when it is
// cancelled is still read and served by the backend, possibly after the
// response that abandoned it.
type vlCallOrigins struct {
	inner     http.RoundTripper
	hold      time.Duration
	holdIf    func(query string) bool
	mu        sync.Mutex
	calls     map[string]int
	abandoned []*http.Request
}

func (o *vlCallOrigins) RoundTrip(req *http.Request) (*http.Response, error) {
	origin := ""
	if original, ok := req.Context().Value(origRequestKey).(*http.Request); ok && original != nil {
		origin = original.Header.Get(vlCallOriginHeader)
	}
	o.mu.Lock()
	if o.calls == nil {
		o.calls = map[string]int{}
	}
	o.calls[origin]++
	o.mu.Unlock()
	if o.hold <= 0 || req.GetBody == nil {
		return o.inner.RoundTrip(req)
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	raw, _ := io.ReadAll(body)
	form, _ := url.ParseQuery(string(raw))
	if o.holdIf == nil || !o.holdIf(form.Get("query")) {
		return o.inner.RoundTrip(req)
	}
	timer := time.NewTimer(o.hold)
	defer timer.Stop()
	select {
	case <-timer.C:
		return o.inner.RoundTrip(req)
	case <-req.Context().Done():
		late := req.Clone(context.WithoutCancel(req.Context()))
		late.Body, _ = req.GetBody()
		o.mu.Lock()
		o.abandoned = append(o.abandoned, late)
		o.mu.Unlock()
		return nil, context.Cause(req.Context())
	}
}

// deliverAbandoned has the backend serve the probes their requests gave up
// on, and reports how many there were.
func (o *vlCallOrigins) deliverAbandoned() int {
	o.mu.Lock()
	late := o.abandoned
	o.abandoned = nil
	o.mu.Unlock()
	for _, req := range late {
		if resp, err := o.inner.RoundTrip(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
	return len(late)
}

// withHeader is headers with name set to value, headers left as they were.
func withHeader(headers map[string]string, name, value string) map[string]string {
	out := make(map[string]string, len(headers)+1)
	for key, v := range headers {
		out[key] = v
	}
	out[name] = value
	return out
}

// traceVLCallOrigins counts p's VictoriaLogs calls per client request
// (vlCallOrigins) from here on.
func traceVLCallOrigins(p *Proxy) *vlCallOrigins {
	origins := &vlCallOrigins{inner: p.client.Transport}
	p.client.Transport = origins
	return origins
}

// made is the calls made for the client request tagged origin.
func (o *vlCallOrigins) made(origin string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[origin]
}

// conformance: semantics/two-parsers-keep-one, cache-response-key-versions
func TestOrderedMixedParserTrailingKeepOldCopies(t *testing.T) {
	testOrderedMixedParserOldCopies(t, `count_over_time({app="api"} | logfmt | json | keep detected_level [1m])`)
}

// conformance: semantics/two-parsers-keep-one, cache-response-key-versions
func TestOrderedMixedParserTransformedBinaryOldCopies(t *testing.T) {
	const mixed = `sum by (message) (count_over_time({app="api"} | json message="[\"message\"]" | logfmt | drop __error__, __error_details__ | message!="" [1m]))`
	testOrderedMixedParserOldCopies(t, `sort(label_replace(`+mixed+` + 1, "copy", "$1", "message", "(.+)"))`)
}

func testOrderedMixedParserOldCopies(t *testing.T, q string) {
	t.Helper()
	at := time.Unix(1700000400, 0).UTC()
	for _, endpoint := range []string{"query", "query_range"} {
		t.Run(endpoint, func(t *testing.T) {
			backend, _ := newDLVBaselineBackend(t, dlvBaselineShape{}, at)
			p := newDLVBaselineProxy(t, backend.URL, true)
			origins := traceVLCallOrigins(p)
			params := url.Values{"query": {q}, "limit": {"100"}}
			if endpoint == "query" {
				params.Set("time", strconv.FormatInt(at.Add(time.Minute).UnixNano(), 10))
			} else {
				params.Set("start", strconv.FormatInt(at.UnixNano(), 10))
				params.Set("end", strconv.FormatInt(at.Add(time.Minute).UnixNano(), 10))
				params.Set("step", "60")
			}
			headers := map[string]string{"X-Scope-OrgID": "team-a", "X-Auth-Token": "alpha"}
			target := "/loki/api/v1/" + endpoint + "?" + params.Encode()
			r := httptest.NewRequest(http.MethodGet, target, nil)
			for k, v := range headers {
				r.Header.Set(k, v)
			}
			key, ok := p.compatCacheKey(endpoint, r)
			if !ok || !strings.Contains(key, mixedParserMetricResponseVersion) {
				t.Fatalf("unfenced original request: %s", key)
			}
			stale := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"stale":"true"},"value":[1700000400,"999"]}]}}`)
			p.compatCache.SetLocalOnlyWithTTL(strings.Replace(key, mixedParserMetricResponseVersion, "", 1), stale, 5*time.Minute)
			cold := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "cold"))
			if cold.Code != 200 || origins.made("cold") == 0 || bytes.Contains(cold.Body.Bytes(), []byte("stale")) {
				t.Fatalf("original cache entry reused: %d %s", cold.Code, cold.Body)
			}
			warm := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "warm"))
			if warm.Code != 200 || !bytes.Equal(cold.Body.Bytes(), warm.Body.Bytes()) || origins.made("warm") != 0 {
				t.Fatalf("no exact original-key reuse: %d %s", warm.Code, warm.Body)
			}
		})
	}
}

// conformance: semantics/two-parsers-keep-one, cache-response-key-versions
func TestOrderedJSONEvaluatorRejectsPreviousV1Copies(t *testing.T) {
	const plain = `sum by (message) (count_over_time({app="api"} | json | drop __error__, __error_details__ | message!="" [1m]))`
	const mixed = `sum by (message) (count_over_time({app="api"} | json message="[\"message\"]" | logfmt | drop __error__, __error_details__ | message!="" [1m]))`
	for _, query := range []string{plain, mixed, "max(" + plain + ")"} {
		for _, site := range []struct {
			endpoint string
			compat   bool
		}{{"query", true}, {"query_range", true}, {"query_range", false}} {
			t.Run(query+"/"+site.endpoint+"/"+strconv.FormatBool(site.compat), func(t *testing.T) {
				at := time.Unix(1700000400, 0).UTC()
				backend, _ := newDLVBaselineBackend(t, dlvBaselineShape{}, at)
				p := newDLVBaselineProxy(t, backend.URL, true)
				origins := traceVLCallOrigins(p)
				params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Add(time.Minute).UnixNano(), 10)},
					"start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(time.Minute).UnixNano(), 10)}, "step": {"60"}}
				target := "/loki/api/v1/" + site.endpoint + "?" + params.Encode()
				headers := map[string]string{"X-Scope-OrgID": "team-a", "X-Auth-Token": "alpha"}
				r := httptest.NewRequest(http.MethodGet, target, nil)
				for k, v := range headers {
					r.Header.Set(k, v)
				}
				key, store := p.queryRangeCacheKey(r, query), p.cache
				if site.compat {
					var ok bool
					key, ok = p.compatCacheKey(site.endpoint, r)
					if !ok {
						t.Fatal("compat cache disabled")
					}
					store = p.compatCache
				}
				if !strings.Contains(key, orderedJSONEvaluatorResponseVersion) {
					t.Fatalf("missing evaluator fence: %s", key)
				}
				// Preserve every prior v1 fence, including the mixed token;
				// only the newly added semantic fence is absent in this copy.
				legacy := strings.Replace(key, orderedJSONEvaluatorResponseVersion, "", 1)
				if strings.Contains(query, "logfmt") && !strings.Contains(legacy, "@ordered-mixed-parser-metric-v1:") {
					t.Fatal("legacy seed did not retain the actual previous mixed v1 identity")
				}
				stale := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"stale":"previous-v1"},"value":[1700000400,"999"]}]}}`)
				store.SetLocalOnlyWithTTL(legacy, stale, 5*time.Minute)
				cold := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "cold"))
				if cold.Code != http.StatusOK || origins.made("cold") == 0 || bytes.Contains(cold.Body.Bytes(), []byte("previous-v1")) {
					t.Fatalf("previous v1 cache bypassed evaluator: %d %s", cold.Code, cold.Body)
				}
				warm := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "warm"))
				if warm.Code != http.StatusOK || !bytes.Equal(cold.Body.Bytes(), warm.Body.Bytes()) || origins.made("warm") != 0 {
					t.Fatalf("new semantic identity did not retain exact reuse: %d %s", warm.Code, warm.Body)
				}
			})
		}
	}
}
