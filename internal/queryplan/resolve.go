package queryplan

import (
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The resolver is one pass over a built plan. It derives the semantics the
// handlers read today from the query text (which parsers run, how parser
// errors surface, whether the outermost aggregation groups, how offsets apply)
// from the typed nodes alone, and records the Loki rules it applied. The
// physical planner and the evaluators will read these attributes, never
// Inputs.Compat: a rule that differs between Loki versions is chosen here.

// RuleID names a Loki semantics rule the resolver applied.
type RuleID string

// The Loki 3.7 rules.
const (
	// RuleParserErrorLabel: a line a parser fails on carries __error__ and
	// __error_details__ to the end of the pipeline.
	RuleParserErrorLabel RuleID = "loki-3.7/parser-error-label"
	// RuleDropErrorKeepsLine: `| drop __error__` after the last parser keeps
	// the lines a parser failed on, without the error label.
	RuleDropErrorKeepsLine RuleID = "loki-3.7/drop-error-keeps-line"
)

// OffsetKind says how the range aggregations of a query carry offsets.
type OffsetKind uint8

const (
	// OffsetNone: no range aggregation has an offset.
	OffsetNone OffsetKind = iota
	// OffsetUniform: every range aggregation has the same offset, PlanSem.Offset.
	OffsetUniform
	// OffsetMixed: some range aggregations have an offset and others have none.
	OffsetMixed
	// OffsetDistinct: every range aggregation has an offset, not all the same.
	OffsetDistinct
)

// PlanSem is what Resolve derives from the whole tree.
type PlanSem struct {
	// Resolved is true once Resolve has run.
	Resolved bool
	// Ungrouped is true when the outermost aggregation, under any
	// label_replace, is a vector aggregation or a rank without a by or
	// without clause: Loki answers it as one series per step (topk, sort: one
	// set) with no labels from the grouping.
	Ungrouped bool
	// OffsetKind and Offset are how the range aggregations shift their windows.
	OffsetKind OffsetKind
	Offset     time.Duration
	// Rules are the rules applied, in the order the walk met them, without duplicates.
	Rules []RuleID
}

// Resolve fills p.Sem and the PipelineSem of every LogSelect. It reads only
// typed fields and is idempotent. Build rejects every Compat value but 3.7,
// so the 3.7 rules are the only ones.
func Resolve(p *Plan) {
	if p.Sem.Resolved {
		return
	}
	sem := &p.Sem
	sem.Resolved = true
	sem.Ungrouped = ungrouped(p.Root)
	first := true
	// An explicit stack: a plan nests as deep as Build allows (maxDepth).
	stack := []Node{p.Root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch x := n.(type) {
		case *LogSelect:
			resolvePipeline(x, sem)
		case *RangeAgg:
			switch {
			case first:
				first = false
				if x.HasOffset {
					sem.OffsetKind, sem.Offset = OffsetUniform, x.Offset
				}
			case (sem.OffsetKind == OffsetNone) != !x.HasOffset:
				sem.OffsetKind, sem.Offset = OffsetMixed, 0
			case sem.OffsetKind == OffsetUniform && x.Offset != sem.Offset:
				sem.OffsetKind, sem.Offset = OffsetDistinct, 0
			}
			if x.In != nil {
				stack = append(stack, x.In)
			}
		case *VectorAgg:
			stack = append(stack, x.In)
		case *Rank:
			stack = append(stack, x.In)
		case *LabelReplace:
			stack = append(stack, x.In)
		case *BinOp:
			// Right first, so the left operand is met first.
			stack = append(stack, x.R, x.L)
		}
	}
}

// ungrouped reports a vector aggregation or a rank without a by or without
// clause at the top of n, under any label_replace.
func ungrouped(n Node) bool {
	for {
		switch x := n.(type) {
		case *LabelReplace:
			n = x.In
		case *VectorAgg:
			return x.By == nil
		case *Rank:
			return x.By == nil
		default:
			return false
		}
	}
}

// resolvePipeline fills the parser and error attributes of a pipeline.
func resolvePipeline(s *LogSelect, sem *PlanSem) {
	ps := &PipelineSem{ErrorAt: -1}
	last := -1
	for i, st := range s.Stages {
		if _, ok := st.(*Parse); ok {
			ps.Parsers++
			last = i
		}
	}
	if ps.Parsers > 0 {
		ps.ErrorPolicy = ErrorPropagate
		if last == len(s.Stages)-2 && dropsOnlyErrorLabel(s.Stages[last+1]) {
			ps.ErrorPolicy, ps.ErrorAt = ErrorDroppedLabel, last+1
			sem.rule(RuleDropErrorKeepsLine)
		} else {
			sem.rule(RuleParserErrorLabel)
		}
	}
	s.Sem = ps
}

// dropsOnlyErrorLabel reports `| drop __error__` or `| drop __error__,
// __error_details__` (either order), with no condition.
func dropsOnlyErrorLabel(st Stage) bool {
	d, ok := st.(*logql.DropStage)
	if !ok || len(d.Matchers) != 0 {
		return false
	}
	switch len(d.Labels) {
	case 1:
		return d.Labels[0] == "__error__"
	case 2:
		return (d.Labels[0] == "__error__" && d.Labels[1] == "__error_details__") ||
			(d.Labels[0] == "__error_details__" && d.Labels[1] == "__error__")
	}
	return false
}

func (s *PlanSem) rule(id RuleID) {
	for _, r := range s.Rules {
		if r == id {
			return
		}
	}
	s.Rules = append(s.Rules, id)
}
