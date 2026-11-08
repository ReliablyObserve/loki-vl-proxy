package queryplan

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The resolver against Loki's own tree: for every plan-corpus query both parsers accept, the test oracle under
// test/oracle/loki (a module of its own; Loki is AGPL-3.0 and only that module imports it) writes what Loki's
// syntax tree holds, and the resolved plan must say the same: whether the outermost vector aggregation is
// ungrouped, the offset of each range aggregation, and the parser and drop stages of the pipeline under the
// aggregations, with whether the error label is dropped after the last parser. LVP_RUN_ORACLES=1 runs it (it
// builds the oracle); the PR quality report runs it with the other oracles.

const lokiOracleDir = "../../test/oracle/loki"

type lokiStage struct {
	Kind        string   `json:"kind"`
	Names       []string `json:"names"`
	Conditional bool     `json:"conditional"`
}

type lokiFacts struct {
	ID       string `json:"id"`
	Accept   bool   `json:"accept"`
	Error    string `json:"error"`
	Outer    string `json:"outer"`
	Grouping *struct {
		Groups  []string `json:"groups"`
		Without bool     `json:"without"`
	} `json:"grouping"`
	Offsets     []int64     `json:"offsets"`
	Pipeline    []lokiStage `json:"pipeline"`
	HasPipeline bool        `json:"has_pipeline"`
}

var parserKinds = map[logql.ParserType]string{
	logql.ParserJSON: "json", logql.ParserLogfmt: "logfmt", logql.ParserRegexp: "regexp",
	logql.ParserPattern: "pattern", logql.ParserUnpack: "unpack",
}

// significant is the parser and drop stages of a pipeline, in order: what the error policy reads. A drop is
// summarised as Loki's tree exposes it: the names it lists and whether it carries a condition.
func significant(stages []lokiStage) []lokiStage {
	out := []lokiStage{}
	for _, s := range stages {
		if s.Kind != "other" {
			if s.Names == nil {
				s.Names = []string{}
			}
			out = append(out, s)
		}
	}
	return out
}

func ourStages(s *LogSelect) []lokiStage {
	out := []lokiStage{}
	for _, st := range s.Stages {
		switch x := st.(type) {
		case *Parse:
			out = append(out, lokiStage{Kind: parserKinds[x.Type], Names: []string{}})
		case *Drop:
			names := append([]string{}, x.Labels...)
			for _, m := range x.Matchers {
				names = append(names, m.Name)
			}
			out = append(out, lokiStage{Kind: "drop", Names: names, Conditional: len(x.Matchers) > 0})
		}
	}
	return out
}

// lokiDropsErrorLabel applies the resolver's definition to Loki's stages: a drop after the last parser lists
// __error__ unconditionally. A conditional drop that also lists __error__ cannot be told apart in Loki's tree
// (Names lists the condition's names too), so ok is false for it.
func lokiDropsErrorLabel(stages []lokiStage) (parsers int, dropped, ok bool) {
	last := -1
	for i, s := range stages {
		if s.Kind != "drop" {
			parsers++
			last = i
		}
	}
	for _, s := range stages[last+1:] {
		for _, n := range s.Names {
			if n == "__error__" {
				if s.Conditional {
					return parsers, false, false
				}
				dropped = true
			}
		}
	}
	return parsers, dropped && parsers > 0, true
}

func ourOffsets(n Node, out *[]int64) {
	switch x := n.(type) {
	case *RangeAgg:
		*out = append(*out, int64(x.Offset))
	case *VectorAgg:
		ourOffsets(x.In, out)
	case *Rank:
		ourOffsets(x.In, out)
	case *LabelReplace:
		ourOffsets(x.In, out)
	case *BinOp:
		ourOffsets(x.L, out)
		ourOffsets(x.R, out)
	}
}

func ourPipeline(n Node) *LogSelect {
	for {
		switch x := n.(type) {
		case *LogSelect:
			return x
		case *LabelReplace:
			n = x.In
		case *VectorAgg:
			n = x.In
		case *Rank:
			n = x.In
		case *RangeAgg:
			return x.In
		default:
			return nil
		}
	}
}

var vectorOps = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true, "stddev": true, "stdvar": true}

func TestPlanMatchesLokiOracle(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	rows, err := plancorpus.Queries("../..")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiOracleDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		q string
		p *Plan
	}
	var ours []row
	in := filepath.Join(work, "facts-in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for _, q := range rows {
		expr, msg := logql.ValidateLogQLExpr(q)
		if msg != "" || expr == nil {
			continue
		}
		p, err := Build(expr, Inputs{})
		if err != nil {
			continue
		}
		Resolve(p)
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(ours)), "q": q})
		w.Write(append(b, '\n'))
		ours = append(ours, row{q, p})
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
	var problems []string
	compared, lokiRejects, ambiguous := 0, 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf lokiFacts
		if err := json.Unmarshal([]byte(line), &lf); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(lf.ID)
		r := ours[i]
		if !lf.Accept {
			lokiRejects++
			continue
		}
		compared++
		// Loki's tree holds no clause and `by ()` alike: an empty grouping.
		wantUngrouped := vectorOps[lf.Outer] && (lf.Grouping == nil || (!lf.Grouping.Without && len(lf.Grouping.Groups) == 0))
		if r.p.Sem.Ungrouped != wantUngrouped {
			problems = append(problems, fmt.Sprintf("%q: ungrouped %v, Loki's tree: %s %+v", r.q, r.p.Sem.Ungrouped, lf.Outer, lf.Grouping))
		}
		offs := []int64{}
		ourOffsets(r.p.Root, &offs)
		if !reflect.DeepEqual(offs, lf.Offsets) {
			problems = append(problems, fmt.Sprintf("%q: offsets %v, Loki's tree %v", r.q, offs, lf.Offsets))
		}
		sel := ourPipeline(r.p.Root)
		if (sel != nil) != lf.HasPipeline {
			problems = append(problems, fmt.Sprintf("%q: pipeline found %v, Loki's tree %v", r.q, sel != nil, lf.HasPipeline))
			continue
		}
		if sel == nil {
			continue
		}
		mine, loki := ourStages(sel), significant(lf.Pipeline)
		if !reflect.DeepEqual(mine, loki) {
			problems = append(problems, fmt.Sprintf("%q: parser and drop stages %+v, Loki's tree %+v", r.q, mine, loki))
			continue
		}
		parsers, dropped, ok := lokiDropsErrorLabel(loki)
		if !ok {
			ambiguous++
			continue
		}
		if sel.Sem.Parsers != parsers || (sel.Sem.ErrorPolicy == ErrorDroppedLabel) != dropped {
			problems = append(problems, fmt.Sprintf("%q: %d parsers, error policy %d; Loki's tree: %d parsers, error label dropped %v", r.q, sel.Sem.Parsers, sel.Sem.ErrorPolicy, parsers, dropped))
		}
	}
	t.Logf("%d queries compared with Loki's tree, %d the proxy accepts and Loki rejects (the parse oracle reports them), %d conditional drops of __error__ left out", compared, lokiRejects, ambiguous)
	if compared < 1000 {
		t.Fatalf("only %d queries compared", compared)
	}
	if len(problems) > 0 {
		t.Fatalf("%d differences from Loki's tree:\n%s", len(problems), strings.Join(problems, "\n"))
	}
}
