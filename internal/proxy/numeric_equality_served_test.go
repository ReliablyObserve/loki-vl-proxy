package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
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
		{`{app="a"} | json | status == 500`, `status:>=500 status:<=500 status:~`},
		{`{app="a"} | logfmt | status = 500`, `status:>=500 status:<=500 status:~`},
		{`{app="a"} | json | status != 200 | status != 404`, `status:>=200 status:<=200`},
		{`sum(count_over_time({app="a"} | json | status == 500 [5m]))`, `status:>=500 status:<=500`},
		{`sum by (app) (rate({app="a"} | logfmt | d = 5s [1m]))`, `d:>=5000000000 d:<=5000000000`},
	} {
		t.Run(c.q, func(t *testing.T) {
			got, err := on.translateLogResponseQuery(ctx, c.q)
			if err != nil || !strings.Contains(got, c.served) {
				t.Errorf("served %q %v, want the plan's numeric comparison %q", got, err, c.served)
			}
			legacy, err := off.translateLogResponseQuery(ctx, c.q)
			if err != nil || legacy == got || strings.Contains(legacy, c.served) {
				t.Errorf("with the lowering off %q %v, want the translator's text", legacy, err)
			}
		})
	}
	// A signed number is a class of its own (semantics/translator-writes-signed-number-with-blank): not served here.
	q := `{app="a"} | json | status == 500 | v = -5`
	got, _ := on.translateLogResponseQuery(ctx, q)
	legacy, _ := off.translateLogResponseQuery(ctx, q)
	if got != legacy {
		t.Errorf("%q: served %q, want the translator's %q", q, got, legacy)
	}
}

// The numeric equality fence fires on every query whose plan writes a numeric equality after a parser, whatever the
// spelling: its text screen (equalityBeforeNumber, a json or logfmt word) never turns one away.
func FuzzNumericEqualityFence(f *testing.F) {
	for _, q := range []string{
		`{app="x"} | json | status = 200`, `{app="x"} | JSON | status == 2e2`, `{app="x"} | logfmt | d != 5s`,
		`sum(count_over_time({app="x"} | logfmt | size = 1KB [5m]))`, "{app=\"x\"} |#c\n json | status=\t.5",
		`{app="x"} | json | status >= 200`, `{app="x"} | json | a="=5" | b = -3`, `{app="x"} | logfmt | n==+1`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 {
			return
		}
		plan := resolvedPlan(fenceVariablesResolved(dynamicQuery(q)))
		if plan == nil || !lower.WritesNumericEquality(plan) {
			return
		}
		if numericEqualityResponseKeyVersion(q) == "" {
			t.Fatalf("%q: the plan writes a numeric equality, the fence does not fire", q)
		}
	})
}
