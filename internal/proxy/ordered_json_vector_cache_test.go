package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
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
	}{{"query_range", false, 0, false}, {"query_range", true, 0, false}, {"query", true, 0, false}, {"query_range", false, time.Minute, false}, {"query_range", true, time.Minute, false}, {"query", true, time.Minute, false}, {"query_range", true, 0, true}, {"query", true, 0, true}} {
		t.Run(setup.mode+"/tier0="+strconv.FormatBool(setup.tier0)+"/offset="+setup.offset.String()+"/addition="+strconv.FormatBool(setup.addition), func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, setup.tier0)
			seen := map[string]bool{}
			for _, identity := range []struct{ tenant, account, credential string }{{"team-a", "10", "alpha"}, {"team-b", "20", "alpha"}, {"team-a", "10", "beta"}} {
				from, to := base.Add(time.Minute), base.Add(3*time.Minute)
				query := `max(sum by(detected_level)(count_over_time({app="api"}|json|__error__=""[1m])))`
				if setup.offset != 0 {
					query = strings.Replace(query, "[1m]", "[1m] offset "+strconv.FormatInt(int64(setup.offset/time.Second), 10)+"s", 1)
				}
				if setup.addition {
					query += " + 1"
				}
				params := url.Values{"query": {query}, "start": {strconv.FormatInt(from.UnixNano(), 10)}, "end": {strconv.FormatInt(to.UnixNano(), 10)}, "step": {"60"}}
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
				bad := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000460,"999999"]]}]}}`)
				if setup.mode == "query" {
					bad = []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000580,"999999"]}]}}`)
				}
				store.SetLocalOnlyWithTTL(legacy, bad, 5*time.Minute)
				before := fixture.calls.Load()
				first := doCompatProxyRequest(p, target, headers)
				if first.Code != 200 || bytes.Equal(first.Body.Bytes(), bad) || fixture.calls.Load() <= before {
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
				if stored, _, ok := store.GetWithTTL(key); !ok || !bytes.Equal(stored, first.Body.Bytes()) {
					t.Fatal("correct new reply not retained under new identity")
				}
				before = fixture.calls.Load()
				second := doCompatProxyRequest(p, target, headers)
				if second.Code != 200 || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) || fixture.calls.Load() != before {
					t.Fatalf("new exact reply not reused: first=%s second=%s", first.Body, second.Body)
				}
			}
		})
	}
}
