package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// dynamicSeriesHashPipe is the pipe that ends the identity pipes of a dynamic identity.
const dynamicSeriesHashPipe = "| hash(__lvp_sid) as __lvp_sid"

// A range function over an unwrapped label after a json or logfmt parser with no keep, under sum, max, min or avg, is
// translated by the typed lowering: its stats group by a hash of the stream and each line's parsed labels. With
// -query-plan-lowering=false, and for every other query, the string translator translates it as before.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/stdvar-grouped-pooled-variance
func TestDynamicIdentityServedFromPlan(t *testing.T) {
	newProxy := func(disable bool) *Proxy {
		p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error", DisableQueryPlanLowering: disable})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	on, off := newProxy(false), newProxy(true)
	for q, want := range map[string]string{
		`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`:                dynamicSeriesHashPipe + " | stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)",
		`sum(min_over_time({app="a"} | json | n > 1 | unwrap n [5m]))`:        dynamicSeriesHashPipe + " | stats by (__lvp_sid) min(__lvp_v) as __lvp_inner",
		`max by (app) (stdvar_over_time({app="a"} | logfmt | unwrap n [5m]))`: "| math __lvp_inner*__lvp_inner as __lvp_inner_var | stats by (app) max(__lvp_inner_var)",
	} {
		got, err := on.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !strings.Contains(got, want) || !strings.Contains(got, dynamicSeriesHashPipe) {
			t.Errorf("%s: served %s, want it to hold %s", q, got, want)
		}
		legacy, err := off.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if strings.Contains(legacy, "__lvp_sid") {
			t.Errorf("%s: with the lowering off, translated %s", q, legacy)
		}
	}
	// Queries the lowering does not serve this way keep the string translator's text.
	prof := loweringProfile(translator.WithStreamLabels(on.labelTranslator.ToVL, nil), nil, on.logsqlCapabilities(), false)
	for _, q := range []string{
		``,
		`{app="a"} | json`,
		`sum(rate({app="a"} | json | unwrap n [5m]))`,
		`sum(sum_over_time({app="a"} | json | unwrap n [5m]))`,
		`sum by (app) (quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`,
		`sum(rate_counter({app="a"} | json | unwrap n [5m]))`,
		`avg_over_time({app="a"} | json | unwrap n [5m])`,
		`sum(avg_over_time({app="a"} | json | keep app, n | unwrap n [5m]))`,
		`sum(avg_over_time({app="a"} | unwrap n [5m]))`,
		`json unwrap (`,
	} {
		if text, ok := on.lowerDynamicIdentity(context.Background(), q, prof); ok {
			t.Errorf("%q: served from the lowering: %s", q, text)
		}
		if on.dynamicUnwrapIdentity(q) {
			t.Errorf("%q: the handlers read a dynamic identity", q)
		}
	}
}

// The handlers read the identity only where the lowering serves the query, and only with the lowering on, and a
// range longer than the step keeps the stream's series: the request is marked before it is translated, at its root
// or in an operand.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityHandlersReadThePlan(t *testing.T) {
	const q = `sum(avg_over_time({app="a"} | json | unwrap n [5m]))`
	p := newTestProxy(t, "http://127.0.0.1:1")
	prof := loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, nil), nil, p.logsqlCapabilities(), false)
	for _, s := range []string{q, `sum(avg_over_time({app="a"} | logfmt | unwrap n [5m]))`, `sum(avg_over_time({app="a"} |unwrap n | json [5m]))`} {
		_, served := p.lowerDynamicIdentity(context.Background(), s, prof)
		if served != p.dynamicUnwrapIdentity(s) {
			t.Errorf("%s: served from the lowering %v, the handlers read the identity %v", s, served, !served)
		}
	}
	for _, tc := range []struct {
		q, step string
		marked  bool
	}{
		{q, "60", true},
		{q, "300", false},
		{q, "900", false},
		{q, "", false},
		{`topk(1, sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])))`, "60", true},
		{`sum(avg_over_time({app="a"} | json | unwrap n [5m])) * 2`, "60", true},
		{`topk(1, sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])))`, "300", false},
		{`sum(sum_over_time({app="a"} | json | unwrap n [5m]))`, "60", false},
		{`sum by (app) (quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`, "60", false},
	} {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?step="+tc.step, nil)
		out := p.dynamicIdentityWindows(r, tc.q)
		if streamIdentityOnly(out.Context()) != tc.marked {
			t.Errorf("%s step %q: marked %v, want %v", tc.q, tc.step, !tc.marked, tc.marked)
		}
		if tc.marked && p.dynamicIdentityFor(out.Context(), q) {
			t.Errorf("%s: a marked request reads the identity", tc.q)
		}
		// A marked request is not decided again.
		if again := p.dynamicIdentityWindows(out, tc.q); again != out {
			t.Errorf("%s: decided twice", tc.q)
		}
	}
	for qq, want := range map[string]time.Duration{
		q: 5 * time.Minute,
		`max by (app) (stdvar_over_time({app="a"} | logfmt | unwrap n [1m]))`: time.Minute,
		`sum(quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`:      0,
		`avg_over_time({app="a"} | json | unwrap n [5m])`:                     0,
		`sum(vector(1))`: 0,
		`sum(`:           0,
	} {
		if got, ok := dynamicWindowRange(qq); got != want || ok != (want > 0) {
			t.Errorf("%s: range %s %v, want %s", qq, got, ok, want)
		}
	}
	// A long query is decided without the memo, and the lowering turns away a query that does not parse.
	long := `sum(avg_over_time({app="` + strings.Repeat("a", validationCacheMaxQueryBytes) + `"} | json | unwrap n [5m]))`
	if !p.dynamicUnwrapIdentity(long) {
		t.Error("a long dynamic query is not decided")
	}
	if _, ok := p.lowerDynamicIdentity(context.Background(), `sum(avg_over_time({app="a"} | json | unwrap n [5m]`, prof); ok {
		t.Error("a query that does not parse was lowered")
	}
	v := &boundedVerdicts{entries: map[string]bool{}, max: 2}
	v.put("a", true)
	v.put("b", false)
	if got, ok := v.get("a"); !ok || !got {
		t.Errorf("a: %v %v", got, ok)
	}
	v.put("c", true) // full: starts over
	if _, ok := v.get("a"); ok || len(v.entries) != 1 {
		t.Errorf("a full map kept %d entries", len(v.entries))
	}
	// With the lowering off the string translator serves the query, and the handlers read no identity.
	off, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error", DisableQueryPlanLowering: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = off.Shutdown(context.Background()) })
	if off.dynamicUnwrapIdentity(q) {
		t.Error("the handlers read the dynamic identity with the lowering off")
	}
}

// The answers cached before the fix are not served after it: a query with the identity carries the fence, every
// other query keeps its key.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityCacheKeysFenced(t *testing.T) {
	for q, want := range map[string]string{
		`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`:                 dynamicUnwrapIdentityResponseVersion,
		` max by (app) (stddev_over_time({app="a"} | logfmt | unwrap n [5m]))`: dynamicUnwrapIdentityResponseVersion,
		`sum(sum_over_time({app="a"} | json | unwrap n [5m]))`:                 "",
		`sum(avg_over_time({app="a"} | json | keep app, n | unwrap n [5m]))`:   "",
		`sum(avg_over_time({app="a"} | unwrap n [5m]))`:                        "",
		`{app="a"} | json`: "",
	} {
		if got := dynamicUnwrapIdentityResponseKeyVersion(q); got != want {
			t.Errorf("%s: fence %q, want %q", q, got, want)
		}
		if got := responseKeyFences(httptest.NewRequest("GET", "/loki/api/v1/query_range?step=60", nil), q, responseKeyInnerQueryRange); !strings.Contains(got, want) || want == "" && strings.Contains(got, dynamicUnwrapIdentityResponseVersion) {
			t.Errorf("%s: response key fences %q", q, got)
		}
	}
}
