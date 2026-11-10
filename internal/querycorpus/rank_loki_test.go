package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestRankMatchesLoki holds what the lowering writes for a ranking to Loki's own syntax tree: the ranking Loki reads
// (topk, bottomk, sort or sort_desc), its k, the absence of a grouping, and the range aggregation under it, for the
// grid rows the lowering writes natively. The ranking itself is not in the lowered text (the proxy ranks each step of
// the answer from the LogQL), so this holds the plan the lowering lowered to Loki's reading of the query. A row Loki's
// parser rejects is not compared: the grid spells k and the names in ways Loki does not accept. LVP_RUN_ORACLES=1
// runs it (it builds the oracle).
// conformance: architecture-syntax-package-layering
func TestRankMatchesLoki(t *testing.T) {
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
		q    string
		rank *queryplan.Rank
		fn   logql.RangeOp
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
	for _, q := range rankGrid() {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		rk, ok := p.Root.(*queryplan.Rank)
		if !ok {
			continue
		}
		queryplan.Resolve(p)
		if _, err := lower.PlanQuery(q, p, prof); err != nil {
			continue
		}
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(rows)), "q": q})
		w.Write(append(b, '\n'))
		rows = append(rows, row{q, rk, rk.In.(*queryplan.RangeAgg).Op})
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
		if lf.Outer != string(r.rank.Op) {
			t.Errorf("%s: Loki reads %q, the plan %q", r.q, lf.Outer, r.rank.Op)
		}
		if lf.Param != r.rank.K {
			t.Errorf("%s: Loki reads k %d, the plan %d", r.q, lf.Param, r.rank.K)
		}
		if lf.Operand != string(r.fn) {
			t.Errorf("%s: Loki reads the operand %q, the lowering %q", r.q, lf.Operand, r.fn)
		}
		if g := lf.Grouping; g != nil && (g.Without || len(g.Groups) > 0) {
			t.Errorf("%s: Loki reads a grouping %v, the lowering writes none", r.q, g)
		}
	}
	if compared < 100 {
		t.Fatalf("only %d queries compared", compared)
	}
	t.Logf("%d of %d lowered rankings: ranking, k, operand and grouping equal Loki's tree", compared, len(rows))
}
