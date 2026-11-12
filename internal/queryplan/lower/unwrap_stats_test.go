package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A vector aggregation over a function of an unwrapped label hands back its stats typed the way the vector
// aggregations over counts do: whether a stats pipe groups by a key at all, whether the last one groups by nothing
// (VictoriaLogs then answers one row over no input), and whether the query derives detected_level for the
// grouping. The text is the one the lowering writes, whichever way it is asked for.
//
// conformance: architecture-syntax-package-layering
func TestUnwrapVectorReturnsTypedStats(t *testing.T) {
	for _, c := range []struct {
		q                  string
		chain              bool
		grouped, ungrouped bool
	}{
		// with no clause (or `by ()`) the stats read a value per stream first and then aggregate it: the last pipe
		// groups by nothing
		{q: `sum(sum_over_time({app="a"} | unwrap n [5m]))`, grouped: true, ungrouped: true},
		{q: `avg(avg_over_time({app="a"} | unwrap n [5m]))`, grouped: true, ungrouped: true},
		{q: `sum by () (max_over_time({app="a"} | unwrap n [5m]))`, grouped: true, ungrouped: true},
		// a sum by labels reads the rows, grouped by the clause (and the level chain, when it names the level)
		{q: `sum by (pod) (sum_over_time({app="a"} | unwrap n [5m]))`, grouped: true},
		{q: `sum by (detected_level) (sum_over_time({app="a"} | unwrap n [5m]))`, grouped: true, chain: true},
		// the others read a value per series: the stream and its level (and the derived level) next to the clause
		{q: `count by (pod) (min_over_time({app="a"} | unwrap n [5m]))`, grouped: true, chain: true},
		{q: `max by (pod, zone) (max_over_time({app="a"} | unwrap n [5m]))`, grouped: true, chain: true},
		// a without clause keeps a row per stream for the proxy to fold
		{q: `max without (pod) (max_over_time({app="a"} | unwrap n [5m]))`, grouped: true},
		{q: `sum without (pod) (sum_over_time({app="a"} | unwrap n [5m]))`, grouped: true},
		// the other functions
		{q: `sum(first_over_time({app="a"} | unwrap n [5m]))`, grouped: true, ungrouped: true},
		{q: `max by (pod) (quantile_over_time(0.5, {app="a"} | unwrap n [5m]))`, grouped: true},
		{q: `sum by (pod) (rate({app="a"} | unwrap n [5m]))`, grouped: true},
		{q: `avg(rate({app="a"} | unwrap n [5m]))`, grouped: true, ungrouped: true, chain: true},
		{q: `sum by (pod) (stdvar_over_time({app="a"} | unwrap n [5m]))`, grouped: true},
		// over a parser the series are the stream and each line
		{q: `max(sum_over_time({app="a"} | json | unwrap n [5m]))`, grouped: true, ungrouped: true},
	} {
		t.Run(c.q, func(t *testing.T) {
			p := planOf(t, c.q)
			va := p.Root.(*queryplan.VectorAgg)
			ra := va.In.(*queryplan.RangeAgg)
			op, view, err := unwrapVectorStats(va, ra, Profile{})
			if err != nil {
				t.Fatal(err)
			}
			if view.chain != c.chain || view.shape.grouped != c.grouped || view.shape.ungrouped != c.ungrouped {
				t.Errorf("chain %v grouped %v ungrouped %v, want %v %v %v", view.chain, view.shape.grouped, view.shape.ungrouped, c.chain, c.grouped, c.ungrouped)
			}
			text, err := unwrapVector(va, ra, Profile{})
			if err != nil {
				t.Fatal(err)
			}
			planned, err := Plan(p, Profile{})
			if err != nil {
				t.Fatal(err)
			}
			if physical.Encode(op) != physical.Encode(text) || physical.Encode(op) != physical.Encode(planned) {
				t.Errorf("text differs:\n stats   %s\n text    %s\n planned %s", physical.Encode(op), physical.Encode(text), physical.Encode(planned))
			}
		})
	}
}

// The stats a vector aggregation over counts hands back (builtVector.view) read the same way: the shape is the stats
// pipes', typed.
//
// conformance: architecture-syntax-package-layering
func TestBuiltVectorViewReadsItsStats(t *testing.T) {
	for _, c := range []struct {
		q                  string
		chain              bool
		grouped, ungrouped bool
	}{
		{q: `sum(count_over_time({app="a"}[5m]))`, ungrouped: true},
		{q: `sum by (pod) (count_over_time({app="a"}[5m]))`, grouped: true},
		{q: `max(count_over_time({app="a"}[5m]))`, grouped: true, ungrouped: true, chain: true},
		{q: `count by (pod) (rate({app="a"}[5m]))`, grouped: true, chain: true},
	} {
		t.Run(c.q, func(t *testing.T) {
			va := planOf(t, c.q).Root.(*queryplan.VectorAgg)
			ra := va.In.(*queryplan.RangeAgg)
			b, err := buildVectorAgg(va, ra, ra.In, Profile{})
			if err != nil {
				t.Fatal(err)
			}
			if v := b.view(); v.chain != c.chain || v.shape.grouped != c.grouped || v.shape.ungrouped != c.ungrouped {
				t.Errorf("chain %v grouped %v ungrouped %v, want %v %v %v", v.chain, v.shape.grouped, v.shape.ungrouped, c.chain, c.grouped, c.ungrouped)
			}
		})
	}
}
