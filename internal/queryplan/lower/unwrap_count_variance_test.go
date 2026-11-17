package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// count by labels over stdvar_over_time counts the series of each group, as count over stddev_over_time does: the
// lowered text is the same, one value per series and their count, with no power the proxy takes (Loki answers the
// number of series whatever their variance; the string translator answered the group's pooled variance).
//
// conformance: semantics/count-over-stdvar-answers-variance
func TestCountOverStdvarCountsSeries(t *testing.T) {
	for _, pipeline := range []string{
		`{app="a"} | unwrap n [5m]`,
		`{app="a"} | logfmt | unwrap n [5m]`,
		`{app="a"} | json n, m | unwrap n [5m]`,
		`{app="a"} | logfmt n | n > 1 | unwrap duration(n) [1m]`,
		`{app="a"} | json | keep app, n | unwrap n [5m]`,
		`{app="a"} | json | keep pod, n | unwrap n [5m]`,
	} {
		for _, clause := range []string{"by (app)", "by (app, pod)"} {
			q := `count ` + clause + ` (stdvar_over_time(` + pipeline + `))`
			p := rangeAggPlan(q)
			if !CountsVarianceSeries(p) || !WritesCountVarianceSeries(p) {
				t.Errorf("%s: not a count over stdvar_over_time", q)
			}
			op, err := PlanQuery(q, p, Profile{StreamIdentity: true})
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			vq, isQuery := op.(physical.VLQuery)
			if !isQuery {
				t.Fatalf("%s: lowered to %T, not one query (a power the proxy takes)", q, op)
			}
			ref := strings.Replace(q, "stdvar_over_time", "stddev_over_time", 1)
			want, err := PlanQuery(ref, rangeAggPlan(ref), Profile{StreamIdentity: true})
			if err != nil {
				t.Fatalf("%s: %v", ref, err)
			}
			// A keep whose labels the clause does not hold already counted the series of the squared deviation (the
			// square changes no count): that text stays.
			got := strings.Replace(vq.Text, " | math __lvp_inner*__lvp_inner as __lvp_inner_var", "", 1)
			if got != physical.Encode(want) {
				t.Errorf("%s:\n got %s\nwant %s (count over stddev_over_time)", q, vq.Text, physical.Encode(want))
			}
			if !strings.HasSuffix(vq.Text, `| stats by (`+strings.TrimSuffix(strings.TrimPrefix(clause, "by ("), ")")+`) count()`) {
				t.Errorf("%s: %s does not end in the count of the group's series", q, vq.Text)
			}
			if _, err := logsql.Parse(vq.Text); err != nil {
				t.Errorf("%s: %s does not parse: %v", q, vq.Text, err)
			}
		}
	}
	// The other shapes keep their lowering: an ungrouped count already counts the squared deviations, a count
	// without a clause reads the series it folds, and another aggregation reads the variance.
	for q, want := range map[string]bool{
		`count(stdvar_over_time({app="a"} | unwrap n [5m]))`:                    false,
		`count without (pod) (stdvar_over_time({app="a"} | unwrap n [5m]))`:     false,
		`count by () (stdvar_over_time({app="a"} | unwrap n [5m]))`:             false,
		`sum by (app) (stdvar_over_time({app="a"} | unwrap n [5m]))`:            false,
		`count by (app) (stddev_over_time({app="a"} | unwrap n [5m]))`:          false,
		`count by (app) (stdvar_over_time({app="a"} | unwrap n [5m]) by (pod))`: false,
		`count by (app) (count_over_time({app="a"} [5m]))`:                      false,
		`count by (app) (stdvar_over_time({app="a"} | unwrap n [5m])) * 2`:      false,
	} {
		if got := CountsVarianceSeries(rangeAggPlan(q)); got != want {
			t.Errorf("%s: CountsVarianceSeries %v, want %v", q, got, want)
		}
	}
	// At any depth: under a binary operation, a ranking, a label_replace, another aggregation.
	for _, q := range []string{
		`count by (app) (stdvar_over_time({app="a"} | unwrap n [5m])) * 2`,
		`topk(1, count by (app) (stdvar_over_time({app="a"} | unwrap n [5m])))`,
		`label_replace(count by (app) (stdvar_over_time({app="a"} | unwrap n [5m])), "x", "$1", "app", "(.*)")`,
		`sum(count by (app) (stdvar_over_time({app="a"} | unwrap n [5m])))`,
	} {
		if !WritesCountVarianceSeries(rangeAggPlan(q)) {
			t.Errorf("%s: the nested count over stdvar_over_time is not found", q)
		}
	}
	if WritesCountVarianceSeries(rangeAggPlan(`sum by (app) (stdvar_over_time({app="a"} | unwrap n [5m])) * 2`)) {
		t.Error("a sum over stdvar_over_time is a count")
	}
}
