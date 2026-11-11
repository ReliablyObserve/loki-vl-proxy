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

// TestRankMatchesLoki holds what the lowering writes for a ranking to Loki's own syntax tree: the outermost ranking
// Loki reads (topk, bottomk, sort or sort_desc), its k and its by or without clause, for the rankings the lowering
// writes natively (a RankStep at the top), and, where the ranking is over a range aggregation, that range function.
// The levels below the outermost one (a ranking under a ranking, the sum, avg, min, max and count layers between
// them) are not covered: Loki's facts give the outermost aggregation only (the report counts them). A row Loki's
// parser rejects is not compared: the grids spell k, the names and the clauses in ways Loki does not accept.
// LVP_RUN_ORACLES=1 runs it (it builds the oracle).
// conformance: architecture-syntax-package-layering
func TestRankMatchesLoki(t *testing.T) {
	holdEvaluatedLevelsToLoki(t, append(rankGrid(), rankStepGrid()...), func(n queryplan.Node) bool {
		_, ok := n.(*queryplan.Rank)
		return ok
	})
}

// TestSeriesAggMatchesLoki holds what the lowering writes for a stddev or stdvar at the top (a SeriesAgg) to Loki's
// own syntax tree: the aggregation Loki reads, and its by or without clause, and the range function under it. The
// levels below the outermost are not in Loki's facts. LVP_RUN_ORACLES=1 runs it (it builds the oracle).
// conformance: architecture-syntax-package-layering
func TestSeriesAggMatchesLoki(t *testing.T) {
	holdEvaluatedLevelsToLoki(t, seriesAggGrid(), func(n queryplan.Node) bool {
		va, ok := n.(*queryplan.VectorAgg)
		return ok && (va.Op == logql.VectorStddev || va.Op == logql.VectorStdvar)
	})
}

// holdEvaluatedLevelsToLoki lowers the rows of grid whose plan root is the kind isRoot says, and compares the level
// at the top of each with Loki's tree: the operation, k and the clause, and the range function under it.
func holdEvaluatedLevelsToLoki(t *testing.T, grid []string, isRoot func(queryplan.Node) bool) {
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
		q     string
		top   physical.Level
		below queryplan.Node
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
	nested := 0
	for _, q := range grid {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		if !isRoot(p.Root) {
			continue
		}
		queryplan.Resolve(p)
		op, err := lower.PlanQuery(q, p, prof)
		if err != nil {
			continue
		}
		levels, _ := physical.SplitEval(op)
		if len(levels) == 0 {
			t.Fatalf("%s: lowered to %T, not a level the proxy evaluates", q, op)
		}
		if len(levels) > 1 {
			nested++
		}
		var below queryplan.Node
		switch v := p.Root.(type) {
		case *queryplan.Rank:
			below = v.In
		case *queryplan.VectorAgg:
			below = v.In
		}
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(rows)), "q": q})
		w.Write(append(b, '\n'))
		// SplitEval lists the levels innermost first: the outermost is the last.
		rows = append(rows, row{q, levels[len(levels)-1], below})
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
	compared, operands := 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf struct {
			ID       string `json:"id"`
			Accept   bool   `json:"accept"`
			Outer    string `json:"outer"`
			Param    int    `json:"param"`
			Operand  string `json:"operand"`
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
		if lf.Outer != r.top.Op {
			t.Errorf("%s: Loki reads %q, the plan %q", r.q, lf.Outer, r.top.Op)
		}
		if lf.Param != r.top.K {
			t.Errorf("%s: Loki reads k %d, the plan %d", r.q, lf.Param, r.top.K)
		}
		// by () and no clause are one group in Loki's tree as in the proxy's.
		var want physical.Grouping
		if g := lf.Grouping; g != nil {
			want = physical.Grouping{Labels: g.Groups, Without: g.Without}
		}
		var got physical.Grouping
		if g := r.top.By; g != nil {
			got = *g
		}
		if got.Without != want.Without || !slices.Equal(got.Labels, want.Labels) {
			t.Errorf("%s: Loki reads the clause %+v, the plan %+v", r.q, want, got)
		}
		if ra, ok := r.below.(*queryplan.RangeAgg); ok {
			operands++
			if lf.Operand != string(ra.Op) {
				t.Errorf("%s: Loki reads the operand %q, the lowering %q", r.q, lf.Operand, ra.Op)
			}
		}
	}
	if compared < 100 {
		t.Fatalf("only %d queries compared", compared)
	}
	t.Logf("%d of %d lowered rows: outermost level, k and clause equal Loki's tree (%d with the range function under them); %d of the lowered rows have levels below the outermost, which the facts do not cover", compared, len(rows), operands, nested)
}
