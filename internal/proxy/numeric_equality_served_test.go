package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// A numeric, duration or bytes equality in a label filter after a json or logfmt parser is served from the plan, in
// a log query and under a metric: the value is compared as a number (a range pair, its negation for !=), where the
// string translator writes the literal's exact text (semantics/label-filter-numeric-equality; the answers are held to
// Loki by TestNumericEqualityAfterParserMatchesLoki in internal/queryplan/lower). With -query-plan-lowering off the
// translator's text is sent, and so is it for a comparison with a signed number, which the plan does not serve yet.
//
// conformance: semantics/label-filter-numeric-equality
func TestNumericEqualityServedFromPlan(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	ctx := context.Background()
	on := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	off := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores; c.DisableQueryPlanLowering = true }, "")
	for _, c := range []struct{ q, served string }{
		{`{app="a"} | json | status == 500`, `filter (status:>=500 status:<=500)`},
		{`{app="a"} | logfmt | status = 500`, `filter (status:>=500 status:<=500)`},
		{`{app="a"} | json | status != 200 | status != 404`, `status:>=200 status:<=200`},
		{`sum(count_over_time({app="a"} | json | status == 500 [5m]))`, `status:>=500 status:<=500`},
		{`sum by (app) (rate({app="a"} | logfmt | d = 5s [1m]))`, `d:>=5000000000 d:<=5000000000`},
	} {
		got, err := on.translateLogResponseQuery(ctx, c.q)
		if err != nil || !strings.Contains(got, c.served) {
			t.Errorf("%q: served %q %v, want the plan's numeric comparison %q", c.q, got, err, c.served)
		}
		legacy, err := off.translateLogResponseQuery(ctx, c.q)
		if err != nil || legacy == got || strings.Contains(legacy, c.served) {
			t.Errorf("%q: with the lowering off %q %v, want the translator's text", c.q, legacy, err)
		}
	}
	// A signed number is a class of its own (semantics/translator-writes-signed-number-with-blank): not served here.
	q := `{app="a"} | json | status == 500 | v = -5`
	got, _ := on.translateLogResponseQuery(ctx, q)
	legacy, _ := off.translateLogResponseQuery(ctx, q)
	if got != legacy {
		t.Errorf("%q: served %q, want the translator's %q", q, got, legacy)
	}
}
