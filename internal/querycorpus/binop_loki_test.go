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

// TestBinOpLoweringMatchesLoki holds what the lowering writes for a binary operation and for label_replace calls to
// Loki's own syntax tree, on every grid row the lowering writes natively: the operator, no bool modifier (the
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
		q  string
		op physical.Op
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
	for _, q := range binOpGrid() {
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
		rows = append(rows, row{q, op})
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
	}
	type lokiLabelReplace struct {
		Dst, Replacement, Src, Regex string
	}
	compared, binaries, rejected := 0, 0, 0
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
			// translator does: not part of what is compared here.
			if !strings.Contains(lf.Error, "unexpected .") && !strings.Contains(lf.Error, "invalid regex in label_replace") {
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
		for side, o := range [2]physical.Op{bin.L, bin.R} {
			kind, value := lb.Left, lb.LeftValue
			if side == 1 {
				kind, value = lb.Right, lb.RightValue
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
	t.Logf("%d lowered rows compared with Loki's tree, %d binary operations; %d rows Loki's parser rejects", compared, binaries, rejected)
	if binaries == 0 {
		t.Fatal("no binary operation compared")
	}
}
