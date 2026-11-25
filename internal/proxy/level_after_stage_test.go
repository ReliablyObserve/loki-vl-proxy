package proxy

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// A filter on detected_level after a stage is served from the plan, written ahead of the stages, in a log query and
// in a metric query whose route derives detected_level anyway; a metric query that would gain a VictoriaLogs request
// stays with the string translator, as every query does with the lowering off.
//
// conformance: semantics/level-filter-after-stage
func TestLevelAfterStageServedFromPlan(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	ctx := context.Background()
	on := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	off := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores; c.DisableQueryPlanLowering = true }, "")
	for _, c := range []struct {
		q      string
		served bool
	}{
		{`{app="a"} | json | detected_level="error"`, true},
		{`{app="a"} | logfmt | detected_level!="info"`, true},
		{`{app="a"} | drop level | decolorize | detected_level="error"`, true},
		{`{app="a"} | keep app | detected_level="error"`, true},
		{`{app="a"} | json | detected_level=""`, true},
		{`sum by (detected_level) (count_over_time({app="a"} | logfmt | detected_level="error" [1m]))`, true},
		{`count_over_time({app="a"} | logfmt | detected_level="error" [1m])`, false},
		{`sum by (app) (rate({app="a"} | logfmt | detected_level="error" [1m]))`, false},
		{`{app="a"} | label_format detected_level="x" | detected_level="x"`, false},
		{`{app="a"} | drop detected_level | json | detected_level="x"`, false},
	} {
		t.Run(c.q, func(t *testing.T) {
			got, err := on.translateLogResponseQuery(ctx, c.q)
			if err != nil {
				t.Fatal(err)
			}
			legacy, err := off.translateLogResponseQuery(ctx, c.q)
			if err != nil {
				t.Fatal(err)
			}
			if (got != legacy) != c.served {
				t.Errorf("served from the plan %v, want %v\n plan %s\n text %s", got != legacy, c.served, got, legacy)
			}
			if c.served && (strings.Contains(got, "unpack_logfmt | filter level") || strings.Contains(got, "unpack_json | filter level")) {
				t.Errorf("served a filter on the parsed level: %s", got)
			}
		})
	}
}

// The fence of a filter on detected_level after a stage fires on every query whose plan writes one, whatever the
// spelling: its text screen never turns one away.
//
// conformance: semantics/level-filter-after-stage, cache-response-key-versions
func FuzzLevelAfterStageFence(f *testing.F) {
	for _, q := range []string{
		`{app="x"} | json | detected_level="error"`, `{app="x"} | JSON | detected_level!~"e.*"`,
		`{app="x"} | Drop level | detected_level="x"`, "{app=\"x\"} |#c\n decolorize | detected_level=`x`",
		`sum by (detected_level) (count_over_time({app="x"} | LOGFMT | detected_level="e" [5m]))`,
		`{app="x"} | keep app | detected_level=""`, `{app="x"} | line_format "x" | detected_level="x"`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 {
			return
		}
		r := httptest.NewRequest("GET", "/", nil)
		plan := resolvedPlan(fenceVariablesResolved(dynamicQuery(q)))
		if plan == nil || !lower.WritesLevelAfterStage(plan) {
			return
		}
		for _, site := range []responseKeySite{responseKeyLogWindowShape, responseKeyCompatQuery, responseKeyCompatQueryRange, responseKeyInnerQueryRange} {
			if !strings.Contains(responseKeyFences(r, q, site), levelAfterStageResponseVersion) {
				t.Fatalf("%q: the plan writes a level filter after a stage, the fence does not fire at site %d", q, site)
			}
		}
	})
}
