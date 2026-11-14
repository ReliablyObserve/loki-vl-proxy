package proxy

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A range function over an unwrapped label whose series a keep before the unwrap decides is translated by the typed
// lowering, whose stats group by the labels the keep leaves; with -query-plan-lowering=false, and for every other
// query, the string translator translates it as before.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepIdentityServedFromPlan(t *testing.T) {
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
		`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`:                      "| stats by (app) row_min(",
		`sum(quantile_over_time(0.9, {app="a"} | json | keep app, n | unwrap n [5m]))`:         "| stats by (app) quantile(0.9, __lvp_v) as __lvp_inner | stats sum(__lvp_inner)",
		`sum by (app) (stddev_over_time({app="a"} | json | keep app, pod, n | unwrap n [5m]))`: "| stats by (app, pod) stddev(__lvp_v) as __lvp_inner | stats by (app) sum(__lvp_inner)",
	} {
		got, err := on.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !strings.Contains(got, want) {
			t.Errorf("%s: served %s, want it to hold %s", q, got, want)
		}
		legacy, err := off.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if strings.Contains(legacy, want) || !strings.Contains(legacy, "_stream") {
			t.Errorf("%s: with the lowering off, translated %s", q, legacy)
		}
	}
	// Queries the lowering does not serve this way keep the string translator's text.
	prof := loweringProfile(translator.WithStreamLabels(on.labelTranslator.ToVL, nil), nil, on.logsqlCapabilities(), false)
	for _, q := range []string{
		``,
		`{app="a"} | json | keep app`,
		`rate({app="a"}[5m])`,
		`sum(rate({app="a"} | json | keep app [5m]))`,
		`first_over_time({app="a"} | json | unwrap n [5m])`,
		`first_over_time({app="a"} | json | keep app | json | unwrap n [5m])`,
		`first_over_time({app="a"} | json | keep app, n | unwrap n [5m]) without (app)`,
		`keep unwrap (`,
	} {
		if text, ok := on.lowerKeepIdentity(context.Background(), q, prof); ok {
			t.Errorf("%q: served from the lowering: %s", q, text)
		}
	}
}

// The handlers read the same identity: the bare-parser raw-row evaluator leaves a keep-decided query to the stats
// route, and the quantile evaluator groups by the labels the keep leaves, as a range by would.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepIdentityHandlersReadThePlan(t *testing.T) {
	const kept = `quantile_over_time(0.9, {app="a"} | json | keep app, pod, n | unwrap n [5m])`
	p := newTestProxy(t, "http://127.0.0.1:1")
	if _, ok := p.bareParserMetricSpec(kept); ok {
		t.Errorf("%s: answered from raw rows", kept)
	}
	if _, ok := p.bareParserMetricSpec(`quantile_over_time(0.9, {app="a"} | json | unwrap n [5m])`); !ok {
		t.Error("a query without a keep left the raw-row evaluator")
	}
	for q, want := range map[string]bool{kept: true, `sum(` + kept + `)`: true, `rate({app="a"} | json | keep app [5m])`: false, `quantile_over_time(0.9, {app="a"} | keep x unwrap`: false} {
		if _, ok := p.keepUnwrapIdentity(q); ok != want {
			t.Errorf("%s: keep identity %v, want %v", q, ok, want)
		}
	}
	// With the lowering off the string translator serves the query, and the handlers read no identity.
	off, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error", DisableQueryPlanLowering: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = off.Shutdown(context.Background()) })
	if _, ok := off.keepUnwrapIdentity(kept); ok {
		t.Error("the handlers read the keep identity with the lowering off")
	}
	if _, ok := off.bareParserMetricSpec(kept); !ok {
		t.Error("with the lowering off the raw-row evaluator does not answer the query as before")
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/query", nil)
	for _, tc := range []struct {
		q          string
		by, orig   []string
		byExplicit bool
	}{
		{kept, []string{"app", "pod"}, []string{"app", "pod"}, false},
		{`sum(` + kept + `)`, []string{"app", "pod"}, []string{"app", "pod"}, false},
		{`quantile_over_time(0.9, {app="a"} | json | keep n | unwrap n [5m])`, []string{}, []string{}, true},
		{`quantile_over_time(0.9, {app="a"} | json | unwrap n [5m])`, nil, nil, false},
		{`quantile_over_time(0.9, {app="a"} | json | keep app, n | unwrap n [5m]) by (pod)`, []string{"pod"}, []string{"pod"}, false},
	} {
		_, spec, ok := p.withQuantileShape(r, tc.q, statsCompatSpec{GroupBy: []string{"pod"}, OrigGroupBy: []string{"pod"}})
		if !ok {
			t.Fatalf("%s: no quantile shape", tc.q)
		}
		if !reflect.DeepEqual(spec.GroupBy, tc.by) || !reflect.DeepEqual(spec.OrigGroupBy, tc.orig) || spec.ByExplicit != tc.byExplicit {
			t.Errorf("%s: grouping %v %v %v, want %v %v %v", tc.q, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, tc.by, tc.orig, tc.byExplicit)
		}
	}
}

// The handlers read the keep identity only where the lowering serves the query: a spelling the text screen leaves
// to the string translator keeps the raw-row route and the quantile evaluator's per-stream series.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepIdentityHandlersFollowTheFlip(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	prof := loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, nil), nil, p.logsqlCapabilities(), false)
	for _, q := range []string{
		`first_over_time({app="a"} | json | keep app, n |unwrap n [5m])`,
		`quantile_over_time(0.50, {app="a"} | json | keep app, n | unwrap n [5m])`,
		`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`,
	} {
		_, served := p.lowerKeepIdentity(context.Background(), q, prof)
		_, read := p.keepUnwrapIdentity(q)
		if served != read {
			t.Errorf("%s: served from the lowering %v, the handlers read the identity %v", q, served, read)
		}
		if _, raw := p.bareParserMetricSpec(q); raw == served {
			t.Errorf("%s: served from the lowering %v and from raw rows %v", q, served, raw)
		}
	}
}

// The answers cached before the fix are not served after it: a query with a keep and an unwrap carries the fence,
// every other query keeps its key.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepIdentityCacheKeysFenced(t *testing.T) {
	for q, want := range map[string]string{
		`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`: keepUnwrapIdentityResponseVersion,
		`sum(rate({app="a"} | json | keep app [5m]))`:                     "",
		`first_over_time({app="a"} | json | unwrap n [5m])`:               "",
	} {
		if got := keepUnwrapIdentityResponseKeyVersion(q); got != want {
			t.Errorf("%s: fence %q, want %q", q, got, want)
		}
		if got := responseKeyFences(httptest.NewRequest("GET", "/loki/api/v1/query_range?step=60", nil), q, responseKeyInnerQueryRange); !strings.Contains(got, want) || want == "" && strings.Contains(got, keepUnwrapIdentityResponseVersion) {
			t.Errorf("%s: response key fences %q", q, got)
		}
	}
}
