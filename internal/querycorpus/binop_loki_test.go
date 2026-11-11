package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"fmt"
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

// TestBinOpLoweringMatchesLoki holds what the lowering writes for a binary operation and for label_replace calls to
// Loki's own syntax tree, on every grid row the lowering writes natively (binOpGrid, and the rows of binOpMoreGrid with
// a without clause, which it holds to the proxy's binary route): the clause of each vector aggregation level of each
// operand in the plan, an operand folded by its own without clause (the fold Loki applies to the operand), the operator, no bool modifier (the
// lowering leaves bool to the translator), the vector matching (on or ignoring and its labels, the cardinality
// group_left and group_right give and their include labels; Loki's tree holds a one-to-one matching with no labels
// when the query has none, and ignoring() reads the same), each operand a number with Loki's value or a query of a
// range or vector aggregation, and the label_replace arguments, innermost first. Rows Loki's parser rejects (a dotted
// grouping label, an invalid label_replace regexp) are counted, not compared. LVP_RUN_ORACLES=1 runs it (it builds
// the oracle).
// conformance: architecture-syntax-package-layering
func TestBinOpLoweringMatchesLoki(t *testing.T) {
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
		op   physical.Op
		plan *queryplan.Plan
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
	// The grid of the binary operations held to the translator, and the rows of the bool and without grid the
	// lowering holds to the proxy's binary route (an operand with a without clause).
	queries := binOpGrid()
	for _, q := range binOpMoreGrid() {
		if strings.Contains(q, "without") {
			queries = append(queries, q)
		}
	}
	for _, q := range queries {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(p)
		op, err := lower.PlanQuery(q, p, prof)
		if err != nil {
			continue
		}
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(rows)), "q": q})
		w.Write(append(b, '\n'))
		rows = append(rows, row{q, op, p})
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
	type lokiBinary struct {
		Op         string   `json:"op"`
		ReturnBool bool     `json:"return_bool"`
		On         bool     `json:"on"`
		Labels     []string `json:"labels"`
		Card       string   `json:"card"`
		Include    []string `json:"include"`
		Left       string   `json:"left"`
		Right      string   `json:"right"`
		LeftValue  float64  `json:"left_value"`
		RightValue float64  `json:"right_value"`
		// The clauses of each operand's vector aggregation levels, outermost first.
		LeftGroupings  []*lokiGrouping `json:"left_groupings"`
		RightGroupings []*lokiGrouping `json:"right_groupings"`
	}
	type lokiLabelReplace struct {
		Dst, Replacement, Src, Regex string
	}
	compared, binaries, rejected, folded := 0, 0, 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf struct {
			ID           string             `json:"id"`
			Accept       bool               `json:"accept"`
			Error        string             `json:"error"`
			Binary       *lokiBinary        `json:"binary"`
			LabelReplace []lokiLabelReplace `json:"label_replace"`
		}
		if err := json.Unmarshal([]byte(line), &lf); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(lf.ID)
		r := rows[i]
		if !lf.Accept {
			// Loki rejects a dotted grouping label and an invalid label_replace regexp when it parses; the proxy
			// accepts the first and answers the second with Loki's error when it reads the markers, as the
			// translator does: not part of what is compared here. A label list that ends in a comma (`without
			// (pod,)`) Loki rejects too, and the proxy's parser accepts it (semantics/label-list-trailing-comma).
			trailingComma := strings.Contains(lf.Error, "unexpected ), expecting IDENTIFIER") && strings.Contains(strings.ReplaceAll(r.q, " ", ""), ",)")
			if !strings.Contains(lf.Error, "unexpected .") && !strings.Contains(lf.Error, "invalid regex in label_replace") && !trailingComma {
				t.Errorf("%s: lowered, Loki rejects it: %s", r.q, lf.Error)
			}
			rejected++
			continue
		}
		compared++
		op := r.op
		if lm, ok := op.(physical.LabelMap); ok {
			var got []lokiLabelReplace
			for i := len(lm.Replace) - 1; i >= 0; i-- {
				s := lm.Replace[i]
				got = append(got, lokiLabelReplace{s.DstLabel, s.Replacement, s.SrcLabel, s.Regex})
			}
			if !slices.Equal(got, lf.LabelReplace) {
				t.Errorf("%s: label_replace %+v, Loki's %+v", r.q, got, lf.LabelReplace)
			}
			op = lm.In
		} else if len(lf.LabelReplace) > 0 {
			t.Errorf("%s: Loki reads label_replace %+v, the lowering none", r.q, lf.LabelReplace)
		}
		bin, ok := op.(physical.Binary)
		if !ok {
			if lf.Binary != nil {
				t.Errorf("%s: Loki reads a binary operation, the lowering %T", r.q, op)
			}
			continue
		}
		binaries++
		lb := lf.Binary
		if lb == nil {
			t.Errorf("%s: the lowering writes a binary operation, Loki reads none", r.q)
			continue
		}
		if bin.Op != lb.Op || bin.Bool || lb.ReturnBool {
			t.Errorf("%s: operator %q bool %v, Loki's %q bool %v", r.q, bin.Op, bin.Bool, lb.Op, lb.ReturnBool)
		}
		m := bin.Match
		if m == nil {
			m = &physical.Matching{}
		}
		labels := m.Ignoring
		if m.MatchOn {
			labels = m.On
		}
		card, include := "one-to-one", []string(nil)
		switch m.GroupSide {
		case "group_left":
			card, include = "many-to-one", m.GroupLeft
		case "group_right":
			card, include = "one-to-many", m.GroupRight
		}
		if m.MatchOn != lb.On || !slices.Equal(labels, lb.Labels) || card != lb.Card || !slices.Equal(include, lb.Include) {
			t.Errorf("%s: matching %+v, Loki's on %v labels %v card %s include %v", r.q, *m, lb.On, lb.Labels, lb.Card, lb.Include)
		}
		planBin := planBinOp(r.plan.Root)
		for side, o := range [2]physical.Op{bin.L, bin.R} {
			kind, value, groupings := lb.Left, lb.LeftValue, lb.LeftGroupings
			planOperand := planBin.L
			if side == 1 {
				kind, value, groupings, planOperand = lb.Right, lb.RightValue, lb.RightGroupings, planBin.R
			}
			// The plan's clause of each vector aggregation level of the operand is Loki's (the lowering writes each
			// operand alone from them, a without clause as the fold on the operand).
			if got := planGroupings(planOperand); !sameGroupings(got, groupings) {
				t.Errorf("%s: operand %d clauses %s, Loki's %s", r.q, side, groupingText(got), groupingText(groupings))
			}
			if f, ok := o.(physical.Fold); ok {
				folded++
				if kind != "*syntax.VectorAggregationExpr" || len(f.Levels) == 0 || !f.Levels[0].Without || !anyWithout(groupings, f.Levels[0].Labels) {
					t.Errorf("%s: operand %d folds %+v, Loki's %s with clauses %v", r.q, side, f.Levels, kind, groupings)
				}
				continue
			}
			switch v := o.(type) {
			case physical.ScalarOp:
				if kind != "*syntax.LiteralExpr" || v.V != value {
					t.Errorf("%s: operand %d is the number %v, Loki's %s %v", r.q, side, v.V, kind, value)
				}
			case physical.VLQuery:
				if kind != "*syntax.RangeAggregationExpr" && kind != "*syntax.VectorAggregationExpr" {
					t.Errorf("%s: operand %d is a query, Loki's %s", r.q, side, kind)
				}
			default:
				t.Errorf("%s: operand %d is %T", r.q, side, o)
			}
		}
	}
	t.Logf("%d lowered rows compared with Loki's tree, %d binary operations (%d operands folded by their without clause); %d rows Loki's parser rejects", compared, binaries, folded, rejected)
	if binaries == 0 || folded == 0 {
		t.Fatalf("binary operations compared %d, folded operands %d", binaries, folded)
	}
}

// lokiGrouping is a by or without clause as the Loki oracle reports it.
type lokiGrouping struct {
	Groups  []string `json:"groups"`
	Without bool     `json:"without"`
}

// planBinOp is the binary operation at the top of the plan, under its label_replace calls.
func planBinOp(n queryplan.Node) *queryplan.BinOp {
	for {
		lr, ok := n.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		n = lr.In
	}
	b, _ := n.(*queryplan.BinOp)
	return b
}

// planGroupings is the clause of each vector aggregation level of n, outermost first, as the oracle reports Loki's.
func planGroupings(n queryplan.Node) []*lokiGrouping {
	var out []*lokiGrouping
	for v, _ := n.(*queryplan.VectorAgg); v != nil; v, _ = v.In.(*queryplan.VectorAgg) {
		var g *lokiGrouping
		if v.By != nil {
			g = &lokiGrouping{Groups: v.By.Labels, Without: v.By.Without}
		}
		out = append(out, g)
	}
	return out
}

// sameGroupings reports whether two lists of clauses read alike: Loki's tree holds an empty by clause for an
// aggregation written with none, so no clause, `by ()` and an empty clause are one.
func sameGroupings(a, b []*lokiGrouping) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x == nil {
			x = &lokiGrouping{}
		}
		if y == nil {
			y = &lokiGrouping{}
		}
		if x.Without != y.Without || len(x.Groups) != len(y.Groups) || (len(x.Groups) > 0 && !slices.Equal(x.Groups, y.Groups)) {
			return false
		}
	}
	return true
}

// anyWithout reports whether one of the clauses is a without clause of these labels.
func anyWithout(groupings []*lokiGrouping, labels []string) bool {
	for _, g := range groupings {
		if g != nil && g.Without && slices.Equal(g.Groups, labels) {
			return true
		}
	}
	return false
}

func groupingText(gs []*lokiGrouping) string {
	var b strings.Builder
	for _, g := range gs {
		if g == nil {
			b.WriteString("[none]")
			continue
		}
		fmt.Fprintf(&b, "[without=%v %v]", g.Without, g.Groups)
	}
	return b.String()
}
