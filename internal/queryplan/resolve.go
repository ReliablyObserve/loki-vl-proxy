package queryplan

import "time"

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
	// label_replace, is a vector aggregation (sum, avg, min, max, count,
	// stddev, stdvar) without a by or without clause: Loki answers it as one
	// series without labels. A rank (topk, bottomk, sort, sort_desc) keeps
	// its series and their labels, so it is not ungrouped.
	Ungrouped bool
	// OffsetKind and Offset are how the range aggregations shift their windows.
	OffsetKind OffsetKind
	Offset     time.Duration
	// Rules are the rules applied, in the order the walk met them, without duplicates.
	Rules []RuleID
}

// Resolve fills p.Sem and the PipelineSem of every LogSelect. It reads only
// typed fields and is idempotent. It writes into the plan, so it must run
// before the plan is shared (memoised, read by other goroutines): it is not
// safe for concurrent use. Build rejects every Compat value but 3.7, so the
// 3.7 rules are the only ones.
func Resolve(p *Plan) {
	if p.Sem.Resolved {
		return
	}
	sem := &p.Sem
	sem.Ungrouped = ungrouped(p.Root)
	first := true
	// An explicit stack: a plan nests as deep as Build allows (maxDepth). The
	// array holds the usual depth without an allocation.
	var buf [16]Node
	stack := append(buf[:0], p.Root)
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
	// Last: a plan reads as resolved only once every pipeline is.
	sem.Resolved = true
}

// ungrouped reports a vector aggregation without a by or without clause at
// the top of n, under any label_replace.
func ungrouped(n Node) bool {
	for {
		switch x := n.(type) {
		case *LabelReplace:
			n = x.In
		case *VectorAgg:
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
		for i := last + 1; i < len(s.Stages); i++ {
			if dropsErrorLabel(s.Stages[i]) {
				ps.ErrorPolicy, ps.ErrorAt = ErrorDroppedLabel, i
				break
			}
		}
		if ps.ErrorPolicy == ErrorDroppedLabel {
			sem.rule(RuleDropErrorKeepsLine)
		} else {
			sem.rule(RuleParserErrorLabel)
		}
	}
	s.Sem = ps
}

// dropsErrorLabel reports a drop stage that names __error__ without a
// condition (`| drop __error__`, `| drop a, __error__`); `| drop
// __error__="x"` drops it only from the lines it matches.
func dropsErrorLabel(st Stage) bool {
	d, ok := st.(*Drop)
	if !ok {
		return false
	}
	for _, l := range d.Labels {
		if l == "__error__" {
			return true
		}
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
