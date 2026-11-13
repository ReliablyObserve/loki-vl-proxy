package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The tree checks (queryplan.Validate) on every route that validates a query, before the cache, the plan and any
// VictoriaLogs call. Loki validates the whole expression before a metadata route requires a selector, as its
// recorded answers for `topk(0, …)` show (`invalid parameter (must be greater than 0) topk(0` on /labels,
// /patterns, /detected_labels and /detected_fields; Loki 3.7.9, 2026-11-13), except on /series, which reads its
// groups without that validation (`only label matchers are supported` for the same query).
//
// conformance: semantics/sort-grouping-accepted, semantics/quantile-over-time-parameter-required, semantics/set-operation-folded-literal-operand, semantics/label-replace-over-log-selector-accepted, semantics/metadata-routes-validate-before-selector-rule
func TestQueryTreeValidationOnEveryRoute(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	const (
		onlyMatchers = "only label matchers are supported"
		onlySelector = "only log selector is supported"
	)
	rejected := []struct{ q, want string }{
		{`sort by (a) (rate({app="a"}[5m]))`, "parse error : sort and sort_desc doesn't allow grouping by "},
		{`quantile_over_time({app="a"} | unwrap c [5m])`, "parse error : parameter required for operation quantile_over_time"},
		{`rate({app="a"}[5m]) and 1 + 2`, "parse error : unexpected literal for right leg of logical/set binary operation (and): 3.000000"},
		{`label_replace({app="a"}, "d", "x", "a", "(.*)")`, "parse error : syntax error: unexpected ,"},
	}
	// The neighbours Loki accepts as queries meet each route's own rule.
	accepted := []string{`sort by () (rate({app="a"}[5m]))`, `quantile_over_time(0.5, {app="a"} | unwrap c [5m])`, `rate({app="a"}[5m]) and vector(1)`}

	type probe struct{ path, param, value, want string }
	var cases []probe
	for _, path := range []string{"/loki/api/v1/query_range", "/loki/api/v1/query"} {
		for _, tc := range rejected {
			cases = append(cases, probe{path, "query", tc.q, tc.want})
		}
	}
	for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label/pod/values", "/loki/api/v1/index/stats", "/loki/api/v1/index/volume", "/loki/api/v1/index/volume_range", "/loki/api/v1/patterns", "/loki/api/v1/detected_labels"} {
		for _, tc := range rejected {
			cases = append(cases, probe{path, "query", tc.q, tc.want})
		}
		for _, q := range accepted {
			cases = append(cases, probe{path, "query", q, onlyMatchers})
		}
	}
	for _, path := range []string{"/loki/api/v1/detected_fields", "/loki/api/v1/detected_field/pod/values"} {
		for _, tc := range rejected {
			cases = append(cases, probe{path, "query", tc.q, tc.want})
		}
		for _, q := range accepted {
			cases = append(cases, probe{path, "query", q, onlySelector})
		}
	}
	for _, tc := range rejected {
		cases = append(cases, probe{"/loki/api/v1/series", "match[]", tc.q, onlyMatchers})
	}
	for _, tc := range cases {
		params := url.Values{"start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}}
		params.Set(tc.param, tc.value)
		target := tc.path + "?" + params.Encode()
		t.Run(target, func(t *testing.T) {
			w := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
			if w.Code != http.StatusBadRequest || w.Body.String() != tc.want {
				t.Fatalf("status=%d body=%q, want 400 %q", w.Code, w.Body, tc.want)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected requests reached the backend %d times", calls.Load())
	}
}

// TestQueryTreeValidationIsCached: a query is validated once, and the tree check's answer is held with the
// parser's in the same cache (a repeat reads the cache and neither parses nor walks the tree again).
func TestQueryTreeValidationIsCached(t *testing.T) {
	const q = `sort_desc by (pod) (rate({app="cached-tree-check"}[5m]))`
	first := cachedQueryValidation(q)
	if !first.tree || first.msg != "parse error : sort and sort_desc doesn't allow grouping by " || first.sample {
		t.Fatalf("first validation: %+v", first)
	}
	v, ok := validationCache.Load(q)
	if !ok {
		t.Skip("the validation cache is full (other tests of the package filled it)")
	}
	if v.(queryValidation) != first {
		t.Fatalf("the cache holds %+v", v)
	}
	if n := testing.AllocsPerRun(20, func() { _ = cachedQueryValidation(q) }); n != 0 {
		t.Fatalf("a cached validation allocates %v times", n)
	}
}

// FuzzMetadataValidationKeepsTheRouteRules: on the selector routes and on /detected_fields, the answer is the
// parser's rules' (logql.ValidateMatchersQuery, logql.ValidateLogSelectorQuery) for every query but one the tree
// checks reject, which gets the tree check's text.
//
// conformance: semantics/metadata-routes-validate-before-selector-rule
func FuzzMetadataValidationKeepsTheRouteRules(f *testing.F) {
	for _, q := range []string{
		`{app="a"}`, `{app=""}`, `{app=~"("}`, `{}`, `*`, ``, `{`, `foo`, `1 + 1`, `vector(1)`, `{app="a"} |= "x"`,
		`{app="a"} | regexp "("`, `{app="a"} |~ "("`, `{app="a"} | line_format "{{"`, `{app="a"} | unwrap c`,
		`rate({app="a"}[5m])`, `topk(0, rate({app="a"}[5m]))`, `sort by (a) (rate({app="a"}[5m]))`,
		`quantile_over_time({app="a"} | unwrap c [5m])`, `rate({app="a"}[5m]) and 1 + 2`,
		`label_replace({app="a"}, "d", "x", "a", "(.*)")`, `rate({app=~".*"}[5m]) and 1 + 2`,
		`{app="a"} | json | a="b"`, `{app="a", app="b"}`, `{app.name="a"}`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 {
			return
		}
		tree := cachedQueryValidation(q).tree
		for _, route := range []struct {
			name      string
			got, want string
		}{
			{"matchers", validateMatchersQuery(q), logqlpkg.ValidateMatchersQuery(q)},
			{"log selector", validateLogSelectorQuery(q), logqlpkg.ValidateLogSelectorQuery(q)},
		} {
			if tree {
				if route.got != cachedQueryValidation(q).msg {
					t.Fatalf("%s %q: %q, the tree check answers %q", route.name, q, route.got, cachedQueryValidation(q).msg)
				}
				continue
			}
			if route.got != route.want {
				t.Fatalf("%s %q: %q, the route rule answers %q", route.name, q, route.got, route.want)
			}
		}
	})
}
