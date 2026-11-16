package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// PlanServedText answers what the translation serves from the query plan, with the translation's decisions: the
// plan's text for a query the plan serves (a filter-only log query, a flipped metric construct, a keep before an
// unwrap, a label named like a field VictoriaLogs reserves), false for one the string translator translates, and
// false for every query when -query-plan-lowering is off.
// conformance: architecture-syntax-package-layering, semantics/keep-before-unwrap-series-identity, semantics/translator-writes-reserved-field-names
func TestPlanServedTextIsWhatTheTranslationServes(t *testing.T) {
	newProxy := func(disable bool) *Proxy {
		p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
			DisableQueryPlanLowering: disable})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	on, off := newProxy(false), newProxy(true)
	prof := loweringProfile(translator.WithStreamLabels(on.labelTranslator.ToVL, nil), nil, on.logsqlCapabilities(), false)
	for _, tc := range []struct {
		query  string
		served bool
	}{
		{`{app="a"} |= "x"`, true},
		{`{app="a", _time="a"}`, true},
		{`sum by (app) (rate({app="a"}[5m]))`, true},
		{`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`, true},
		{`sum by( ) (rate({app="a"}[5m]))`, true},
		{`sum without () (count_over_time({app="a"}[5m]))`, false},
		{`stddev(count_over_time({app="a"}[5m]))`, false},
		{`vector(1)`, false},
	} {
		text, ok := on.PlanServedText(tc.query, prof)
		if ok != tc.served {
			t.Errorf("%q: served from the plan %v, want %v", tc.query, ok, tc.served)
			continue
		}
		if ok {
			if want, err := on.translateQuery(tc.query); err != nil || text != want {
				t.Errorf("%q: PlanServedText %q, the translation serves %q (%v)", tc.query, text, want, err)
			}
		}
		if _, ok := off.PlanServedText(tc.query, prof); ok {
			t.Errorf("%q: served from the plan with -query-plan-lowering off", tc.query)
		}
	}
}
