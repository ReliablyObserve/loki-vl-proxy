package proxy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// count by labels over stdvar_over_time is served from the plan, which counts the series of each group; with
// -query-plan-lowering=false the string translator's variance stays, and the response key fence keeps the answers
// cached before the fix apart whatever the flag.
//
// conformance: semantics/count-over-stdvar-answers-variance
func TestCountOverStdvarServedFromPlan(t *testing.T) {
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
		`count by (ns) (stdvar_over_time({app="a"} | logfmt n | unwrap n [5m]))`:          "| stats by (ns, _stream) stddev(__lvp_v) as __lvp_inner | stats by (ns) count()",
		`count by (ns) (stdvar_over_time({app="a"} | unwrap n [5m]))`:                     "| stats by (ns, _stream, level, detected_level) stddev(__lvp_v) as __lvp_inner | stats by (ns) count()",
		`COUNT BY (ns, pod) (STDVAR_OVER_TIME({app="a"} | JSON | UNWRAP n [1m]))`:         "| stats by (ns, pod, _stream) stddev(__lvp_v) as __lvp_inner | stats by (ns, pod) count()",
		`count by (ns) (stdvar_over_time({app="a"} | json | keep ns, n | unwrap n [5m]))`: "| stats by (ns) stddev(__lvp_v) as __lvp_inner | stats by (ns) count()",
	} {
		got, err := on.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !strings.Contains(got, want) || strings.Contains(got, "__binary__") {
			t.Errorf("%s: served %s, want it to hold %s and no power", q, got, want)
		}
		if countVarianceResponseKeyVersion(q) != countVarianceResponseVersion {
			t.Errorf("%s: the response key carries no fence", q)
		}
		legacy, err := off.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !strings.HasPrefix(legacy, "__binary__:^:") {
			t.Errorf("%s: with the lowering off, translated %s", q, legacy)
		}
	}
	// The other aggregations over stdvar_over_time, and count over another function, keep their translation and key.
	for _, q := range []string{
		`sum by (ns) (stdvar_over_time({app="a"} | unwrap n [5m]))`,
		`count(stdvar_over_time({app="a"} | unwrap n [5m]))`,
		`count by (ns) (stddev_over_time({app="a"} | unwrap n [5m]))`,
		`count without (pod) (stdvar_over_time({app="a"} | unwrap n [5m]))`,
	} {
		got, err := on.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		legacy, err := off.translateQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got != legacy {
			t.Errorf("%s: served %s, the translator writes %s", q, got, legacy)
		}
		if countVarianceResponseKeyVersion(q) != "" {
			t.Errorf("%s: fenced", q)
		}
	}
}
