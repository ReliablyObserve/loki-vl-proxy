package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// lokiFactsDir is the Loki parse oracle (test/oracle/loki), a module of its own.
const lokiFactsDir = "../../test/oracle/loki"

// TestVectorAggGroupingMatchesLoki holds the grouping the lowering writes for a vector aggregation to Loki's own
// syntax tree: the aggregation Loki reads, and the labels of its by or without clause. A without clause is the
// labels of the fold, as written; a by clause is the labels the last stats pipe groups by (each once, in order);
// no clause (and `by ()`) is a last stats pipe with no grouping. LVP_RUN_ORACLES=1 runs it (it builds the oracle).
// conformance: architecture-syntax-package-layering
func TestVectorAggGroupingMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	t.Cleanup(querycorpus.PauseRecording())
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiFactsDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		q   string
		op  physical.Op
		agg logql.VectorOp
		// nested says the aggregation is over another one: Loki's tree gives the clause the outer level wrote, and
		// the lowering writes the labels it keeps of the level below (translate.LevelKeeps).
		nested bool
	}
	var rows []row
	in := filepath.Join(work, "facts-in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	identity := func(s string) string { return s }
	prof := lower.Profile{Caps: logsql.CapabilitiesFor("v1.53.0"), Label: identity}
	for _, q := range append(vectorAggGrid(), vectorAggNestedGrid()...) {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		va, ok := p.Root.(*queryplan.VectorAgg)
		if !ok {
			continue
		}
		queryplan.Resolve(p)
		op, err := lower.Plan(p, prof)
		if err != nil {
			continue
		}
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(rows)), "q": q})
		w.Write(append(b, '\n'))
		_, nested := va.In.(*queryplan.VectorAgg)
		rows = append(rows, row{q, op, va.Op, nested})
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	out := filepath.Join(work, "facts.jsonl")
	if err := oraclerun.Run(bin, "facts", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	compared := 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf struct {
			ID       string `json:"id"`
			Accept   bool   `json:"accept"`
			Outer    string `json:"outer"`
			Grouping *struct {
				Groups  []string `json:"groups"`
				Without bool     `json:"without"`
			} `json:"grouping"`
		}
		if err := json.Unmarshal([]byte(line), &lf); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(lf.ID)
		r := rows[i]
		if !lf.Accept {
			continue
		}
		compared++
		if lf.Outer != string(r.agg) {
			t.Errorf("%s: Loki reads %q, the lowering %q", r.q, lf.Outer, r.agg)
		}
		if r.nested {
			// The outer operation is the one Loki reads; its clause is cut to the labels below (the fold of a
			// nest is the innermost level's clause with the top operator).
			if fold, ok := r.op.(physical.Fold); ok && fold.Levels[0].Op != string(r.agg) {
				t.Errorf("%s: the nest's fold has operator %s, Loki's outer operation is %s", r.q, fold.Levels[0].Op, r.agg)
			}
			continue
		}
		var want []string
		without := false
		if lf.Grouping != nil {
			want, without = lf.Grouping.Groups, lf.Grouping.Without
		}
		core, _ := physical.Split(r.op)
		var got []string
		if fold, ok := r.op.(physical.Fold); ok {
			if !without || fold.Levels[0].Op != string(r.agg) || !slices.Equal(fold.Levels[0].Labels, want) {
				t.Errorf("%s: fold %+v, Loki's without %v %v", r.q, fold.Levels[0], without, want)
			}
			continue
		}
		sa, evaluated := core.(physical.SeriesAgg)
		if without && len(want) > 0 && !evaluated {
			t.Errorf("%s: Loki reads a without clause %v, the lowering writes no fold", r.q, want)
			continue
		}
		if evaluated {
			// A level the proxy evaluates (stddev, stdvar, a ranking) carries its clause on the operator.
			if sa.By != nil {
				// A label named twice groups as once, as Loki's tree reads it.
				for _, l := range sa.By.Labels {
					if !slices.Contains(got, l) {
						got = append(got, l)
					}
				}
			}
			if (sa.By != nil && sa.By.Without) != without {
				t.Errorf("%s: series aggregation %+v, Loki's without %v %v", r.q, sa.By, without, want)
				continue
			}
		} else {
			parsed, err := logsql.Parse(core.(physical.VLQuery).Text)
			if err != nil {
				t.Fatalf("%s: %v", r.q, err)
			}
			last, ok := parsed.Pipes[len(parsed.Pipes)-1].(logsql.PipeStats)
			if !ok {
				t.Fatalf("%s: the last pipe is %T", r.q, parsed.Pipes[len(parsed.Pipes)-1])
			}
			for _, k := range last.By {
				got = append(got, k.Field)
			}
		}
		var uniq []string
		for _, l := range want {
			if !slices.Contains(uniq, l) {
				uniq = append(uniq, l)
			}
		}
		// A sum of a rate with a without clause groups by the series (checked above); here every other row groups
		// by exactly the clause's labels.
		if !slices.Equal(got, uniq) {
			t.Errorf("%s: groups by %v, Loki's clause %v", r.q, got, uniq)
		}
	}
	if compared < 100 {
		t.Fatalf("only %d queries compared", compared)
	}
	t.Logf("%d vector aggregations: operation and grouping equal Loki's tree", compared)
}
