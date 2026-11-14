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

// dynamicSeriesHashPipe is the pipe that ends the identity pipes of a dynamic identity.
const dynamicSeriesHashPipe = "| hash(__lvp_sid) as __lvp_sid"

// A range function over an unwrapped label after a json or logfmt parser with no keep, under sum, max, min or avg, is
// translated by the typed lowering: its stats group by the stream and the hash of each line's parsed labels. With
// -query-plan-lowering=false, and for every other query, the string translator translates it as before.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-quantile-sum-by-group-quantile, semantics/stdvar-grouped-pooled-variance
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
		`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`:                     dynamicSeriesHashPipe + " | stats by (_stream, __lvp_sid) avg(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)",
		`sum by (app) (quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`: dynamicSeriesHashPipe + " | stats by (app, _stream, __lvp_sid) quantile(0.9, __lvp_v) as __lvp_inner",
		`sum(min_over_time({app="a"} | json | n > 1 | unwrap n [5m]))`:             dynamicSeriesHashPipe + " | stats by (_stream, __lvp_sid) min(__lvp_v) as __lvp_inner",
		`max by (app) (stdvar_over_time({app="a"} | logfmt | unwrap n [5m]))`:      "| math __lvp_inner*__lvp_inner as __lvp_inner_var | stats by (app) max(__lvp_inner_var)",
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

// The handlers read the identity only where the lowering serves the query, and only with the lowering on: the
// instant handler leaves the functions VictoriaLogs takes per series to the stats route, the quantile evaluator
// groups by the stream and the series hash, a sum folds the series of the raw-row evaluator, and a window longer
// than the step reads the rows.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-quantile-sum-by-group-quantile
func TestDynamicIdentityHandlersReadThePlan(t *testing.T) {
	const q = `sum by (app) (quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`
	p := newTestProxy(t, "http://127.0.0.1:1")
	prof := loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, nil), nil, p.logsqlCapabilities(), false)
	for _, s := range []string{q, `sum(avg_over_time({app="a"} | logfmt | unwrap n [5m]))`, `sum(avg_over_time({app="a"} |unwrap n | json [5m]))`} {
		_, served := p.lowerDynamicIdentity(context.Background(), s, prof)
		if served != p.dynamicUnwrapIdentity(s) {
			t.Errorf("%s: served from the lowering %v, the handlers read the identity %v", s, served, !served)
		}
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/query", nil)
	_, spec, ok := p.withQuantileShape(r, q, statsCompatSpec{GroupBy: []string{"app"}, OrigGroupBy: []string{"app"}})
	if !ok || !reflect.DeepEqual(spec.GroupBy, dynamicSeriesGroupBy) || !reflect.DeepEqual(spec.OrigGroupBy, dynamicSeriesGroupBy) || spec.ByExplicit {
		t.Errorf("quantile grouping %v %v %v %v", spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, ok)
	}
	orig, _ := parseOriginalRangeMetricSpec(`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`)
	for _, tc := range []struct {
		fn   string
		step time.Duration
		want bool
	}{
		{"avg", time.Minute, true},
		{"stdvar", time.Minute, true},
		{"avg", 5 * time.Minute, false},
		{"avg", 15 * time.Minute, false},
		{"quantile", time.Minute, false},
		{"rate_counter", time.Minute, false},
	} {
		if got := p.dynamicIdentityRows(`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`, orig, tc.fn, tc.step); got != tc.want {
			t.Errorf("%s step %s: rows %v, want %v", tc.fn, tc.step, got, tc.want)
		}
	}
	if p.dynamicIdentityRows(`sum(sum_over_time({app="a"} | json | unwrap n [5m]))`, orig, "avg", time.Minute) {
		t.Error("a query without the identity reads the rows")
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
	if _, spec, _ := off.withQuantileShape(r, q, statsCompatSpec{GroupBy: []string{"app"}}); spec.GroupBy != nil {
		t.Errorf("with the lowering off the quantile evaluator groups by %v", spec.GroupBy)
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
