package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`:                     dynamicSeriesHashPipe + " | stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)",
		`sum by (app) (quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`: dynamicSeriesHashPipe + " | stats by (app, __lvp_sid) quantile(0.9, __lvp_v) as __lvp_inner",
		`sum(min_over_time({app="a"} | json | n > 1 | unwrap n [5m]))`:             dynamicSeriesHashPipe + " | stats by (__lvp_sid) min(__lvp_v) as __lvp_inner",
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
// instant handler leaves the functions VictoriaLogs takes per series to the stats route, and the quantile evaluator
// groups by the series hash.
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

// The closed form of one-sample series: the range function of the outer aggregation's name grouped by its clause, and
// 0 wherever a line is for the functions whose value over one sample is 0; nothing for a shape the proof cannot read.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-quantile-sum-by-group-quantile
func TestDynamicIdentityClosedForm(t *testing.T) {
	const j = `{app="a"} | json | unwrap n [5m]`
	for q, want := range map[string]string{
		`sum by (app) (quantile_over_time(0.9, ` + j + `))`: `sum by (app) (sum_over_time({app="a"} | json | unwrap n[5m]))`,
		`sum(avg_over_time(` + j + `))`:                     `sum (sum_over_time({app="a"} | json | unwrap n[5m]))`,
		`max by (app, pod) (max_over_time(` + j + `))`:      `max_over_time({app="a"} | json | unwrap n[5m]) by (app, pod)`,
		`min(min_over_time(` + j + `))`:                     `min_over_time({app="a"} | json | unwrap n[5m]) by ()`,
		`avg by (app) (avg_over_time(` + j + `))`:           `avg_over_time({app="a"} | json | unwrap n[5m]) by (app)`,
		`sum by (app) (stddev_over_time(` + j + `))`:        `sum by (app) (sum_over_time({app="a"} | json | unwrap n[5m])) * 0`,
		`max(stdvar_over_time(` + j + `))`:                  `sum (sum_over_time({app="a"} | json | unwrap n[5m])) * 0`,
	} {
		got, ok := dynamicClosedForm(q)
		if !ok {
			t.Fatalf("%s: no closed form", q)
		}
		if strings.ReplaceAll(got, " [", "[") != want {
			t.Errorf("%s: closed form %s, want %s", q, got, want)
		}
		if _, err := newTestProxy(t, "http://127.0.0.1:1").translateQuery(got); err != nil {
			t.Errorf("%s: the closed form %s does not translate: %v", q, got, err)
		}
	}
	for _, q := range []string{
		`sum(avg_over_time({app="a"} | json | unwrap n [5m] offset 1m))`,
		`sum without (app) (avg_over_time(` + j + `))`,
		`sum(avg_over_time(` + j + `) by (app))`,
		`count(avg_over_time(` + j + `))`,
		`sum(sum_over_time(` + j + `))`,
		`sum(rate_counter(` + j + `))`,
		`avg_over_time(` + j + `)`,
		`sum(`,
	} {
		if got, ok := dynamicClosedForm(q); ok {
			t.Errorf("%s: closed form %s", q, got)
		}
	}
}

// The functions and windows the one-sample proof serves, and the bounded memory of the verdicts.
func TestDynamicIdentityProvesAndVerdicts(t *testing.T) {
	long, _ := parseOriginalRangeMetricSpec(`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`)
	for _, tc := range []struct {
		fn      string
		step    time.Duration
		instant bool
		want    bool
	}{
		{"quantile", 5 * time.Minute, false, true},
		{"rate_counter", 0, true, false},
		{"avg", time.Minute, false, false},
		{"stdvar", time.Minute, false, false},
		{"avg", 5 * time.Minute, false, false},
		{"max", 0, true, false},
		{"sum", time.Minute, false, false},
	} {
		if got := dynamicIdentityProves(tc.fn, long, tc.step, tc.instant); got != tc.want {
			t.Errorf("%s step %s instant %v: %v, want %v", tc.fn, tc.step, tc.instant, got, tc.want)
		}
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
}

// serveDynamicIdentity answers one-sample series from the closed form, fails a query whose series outnumber the
// series limit with Loki's error, and leaves the others (and a proof VictoriaLogs does not answer) to the per-series
// evaluators.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityProofRoutes(t *testing.T) {
	const q = `sum by (app) (quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`
	for _, tc := range []struct {
		name   string
		proof  string
		served bool
		status int
		sent   string
	}{
		{"one sample per series", `{"n":"7","u":"7"}`, true, 200, "| limit "},
		{"more series than the limit", `{"n":"900","u":"600"}`, true, 400, ""},
		{"series of several samples", `{"n":"9","u":"3"}`, false, 0, ""},
		{"no verdict", `not json`, false, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sent []string
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				sent = append(sent, r.Form.Get("query"))
				if strings.Contains(r.Form.Get("query"), "count_uniq_hash(") {
					_, _ = w.Write([]byte(tc.proof))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"a"},"value":[1800000600,"3"]}]}}`))
			}))
			defer backend.Close()
			p := newTestProxy(t, backend.URL)
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {q}, "time": {"1800000600"}}.Encode(), nil)
			r.Header.Set("X-Scope-OrgID", "0")
			rec := httptest.NewRecorder()
			served := p.serveDynamicIdentity(rec, r, q, `app:="a" | unpack_json`, time.Unix(1800000300, 0), time.Unix(1800000600, 0), true)
			if served != tc.served || served && rec.Code != tc.status {
				t.Fatalf("served %v status %d, want %v %d: %s", served, rec.Code, tc.served, tc.status, rec.Body)
			}
			// The closed form's own read follows the proof (a sum over sum_over_time of an instant reads the rows).
			if tc.sent != "" && (len(sent) < 2 || !strings.Contains(sent[len(sent)-1], tc.sent) || strings.Contains(sent[len(sent)-1], "__lvp_sid")) {
				t.Errorf("the closed form was not asked: %v", sent)
			}
		})
	}
	// A query without the identity asks nothing.
	p := newTestProxy(t, "http://127.0.0.1:1")
	if p.serveDynamicIdentity(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), `sum(rate({a="b"}[5m]))`, "", time.Time{}, time.Time{}, true) {
		t.Error("a query without the identity was served")
	}
}
