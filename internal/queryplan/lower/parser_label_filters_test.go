package lower

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A label filter or a drop after a json or logfmt parser, in a metric query whose grouping or unwrapped label the
// lowering resolves under its other spellings (a label with an underscore), or in a `without` fold over the parser
// or behind a trailing keep, is written by the lowering. Each shape is the string translator's text under every
// profile; where the plan writes Loki's series instead (a parser's dynamic identity) the next test holds it.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersParserLabelFilters(t *testing.T) {
	for _, q := range []string{
		// a label filter after the parser, under a resolved grouping label
		`sum by (a_b) (count_over_time({app="a"} | json | status >= 500 [5m]))`,
		`sum by (service_name) (sum_over_time({env="production"} | json | unwrap duration_ms | __error__="" [1m]))`,
		`sum by (service_name) (sum_over_time({service_name="cache-redis"} | logfmt | unwrap bytes(size_bytes) | __error__="" [5m]))`,
		`sum by (service_name) (rate({service_name="api-gateway"} | json | unwrap duration_ms | __error__="" [1m]))`,
		`sum(rate({service_name="api-gateway"} | json | __error__="" | unwrap duration_ms [5m]))`,
		`sum_over_time({app="a"} | json | status >= 500 | unwrap duration_ms [5m])`,
		`avg_over_time({service_name="api-gateway"} | json | unwrap duration_ms | __error__="" [5m]) by (service_name)`,
		// a drop after the parser
		`sum by (service_version) (count_over_time({app="a"} | json | drop __error__ [5m]))`,
		`sum by (level_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`,
		`sum by (user_id) (count_over_time({app="api"} | json | drop __error__ [1m]))`,
		`first_over_time({app="api-gateway"} | json duration_ms | drop pod | unwrap duration_ms [5m])`,
		// a fold over a parser and a trailing keep, with a filter or a drop after the parser
		`sum without (pod) (count_over_time({app="a"} | logfmt | a="x" [5m]))`,
		`count without (pod) (count_over_time({app="a"} | logfmt a, b | drop c [5m]))`,
		`sum without (pod) (sum_over_time({app="a"} | logfmt a, n | a!="x" | unwrap n [5m]))`,
		`rate({app="a"} | json | a="x" | keep a, b_c [5m])`,
		`min by (x) (count_over_time({app="a"} | logfmt | b="y" | keep a [5m]))`,
	} {
		for _, prof := range rangeProfiles() {
			lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
			switch {
			case !ok:
				t.Errorf("%s: %s: the string translator rejects it", prof.name, q)
			case delegated:
				t.Errorf("%s: %s: not lowered", prof.name, q)
			case !same:
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	}
}

// The same filters under a sum, max, min or avg over a function taken per series of an unwrapped label after a bare
// parser: the series are the stream and every parsed label (lower.DynamicUnwrapIdentity), which the lowering names
// with the series hash where the translator groups by the stream and each line; the filter after the parser, before
// or after the unwrap, is one more stage of that query.
//
// conformance: architecture-syntax-package-layering, semantics/unwrap-per-series-identity-raw-rows
func TestPlanLowersParserLabelFiltersWithDynamicIdentity(t *testing.T) {
	for _, q := range []string{
		`max by (service_name) (max_over_time({env="production"} | logfmt | unwrap latency_ms | __error__="" [1m]))`,
		`max by (service_name) (max_over_time({service_name="api-gateway"} | json | unwrap duration_ms | __error__="" [1m]))`,
		`sum(avg_over_time({service_name="api-gateway"} | json | unwrap duration_ms | __error__="" [5m]))`,
		`sum(stddev_over_time({app="api-gateway"} | json | __error__="" | unwrap duration_ms [5m]))`,
	} {
		p := rangeAggPlan(q)
		if p == nil || !DynamicUnwrapIdentity(p) {
			t.Fatalf("%s: not a dynamic identity", q)
		}
		for _, label := range []func(string) string{nil, benchStreamLabels} {
			op, err := PlanQuery(q, p, Profile{Label: label})
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			text := physical.Encode(op)
			if !strings.Contains(text, dynamicSeriesField) || !strings.Contains(text, `filter __error__:=""`) {
				t.Errorf("label=%v: %s: want the series hash and the error filter, got %s", label != nil, q, text)
			}
		}
	}
}

// What stays with the string translator among these shapes: a rate that sums a label it counts behind the label's
// existence check (the translator resolves the label in the middle of the query, `parser label filter`).
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesParserLabelExistenceInRate(t *testing.T) {
	q := `sum by (a_b) (rate({app="a"} | json | a_b != "" [5m]))`
	_, err := Plan(rangeAggPlan(q), Profile{})
	var nl *NotLoweredError
	if !errors.As(err, &nl) || nl.Construct != "VectorAgg parser label filter" {
		t.Fatalf("%s: got %v, want not lowered: VectorAgg parser label filter", q, err)
	}
}
