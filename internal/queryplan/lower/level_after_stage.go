package lower

import (
	"regexp"
	"slices"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A filter on detected_level after a stage (semantics/level-filter-after-stage). Loki attaches detected_level to
// every entry at ingest, so a later parser never writes it: a key or capture of that name becomes
// detected_level_extracted. Line filters, line_format, decolorize, parsers, and a drop, keep or label_format that
// does not name the label leave it as ingested, and a filter on it selects the same entries ahead of those stages,
// where the lowering writes it. A drop of the name, or a keep without it, removes the label: a filter that follows
// with no parser in between compares "". Any other stage that touches the label (a drop or keep matcher on it, a
// label_format from or to it, a parser after it was removed, unpack) leaves the filter to the string translator.

// levelState is what detected_level holds at a stage of a pipeline.
type levelState uint8

const (
	// levelIngest is the value Loki attached at ingest.
	levelIngest levelState = iota
	// levelAbsent is no label.
	levelAbsent
	// levelOther is a value the lowering does not follow.
	levelOther
)

// levelTransition is the state after the stage st, and whether a filter on detected_level may not stay behind st as
// the string translator writes it (every stage but a line filter, a label filter and a drop of the parser error
// labels alone).
func levelTransition(state levelState, st queryplan.Stage) (levelState, bool) {
	switch s := st.(type) {
	case *queryplan.LineFilter, *queryplan.LabelFilter:
		return state, false
	case *queryplan.Drop:
		if dropsOnlyErrorLabels(s) {
			return state, false
		}
		switch {
		case matchesName(s.Matchers, translate.DetectedLevelLabel):
			return levelOther, true
		case slices.Contains(s.Labels, translate.DetectedLevelLabel) && state != levelOther:
			return levelAbsent, true
		}
		return state, true
	case *queryplan.Keep:
		switch {
		case matchesName(s.Matchers, translate.DetectedLevelLabel):
			return levelOther, true
		case !slices.Contains(s.Labels, translate.DetectedLevelLabel) && state != levelOther:
			return levelAbsent, true
		}
		return state, true
	case *queryplan.LabelFormat:
		for _, f := range s.Formats {
			if f.Name == translate.DetectedLevelLabel || f.Rename && f.Value == translate.DetectedLevelLabel {
				return levelOther, true
			}
		}
		return state, true
	case *queryplan.LineFormat, *queryplan.Decolorize:
		return state, true
	case *queryplan.Parse:
		switch s.Type {
		case logql.ParserJSON, logql.ParserLogfmt, logql.ParserRegexp, logql.ParserPattern:
			if state == levelIngest {
				return state, true
			}
		}
		return levelOther, true
	}
	return levelOther, true
}

// matchesName reports a drop or keep matcher on the label name.
func matchesName(matchers []logql.DropMatcher, name string) bool {
	return slices.ContainsFunc(matchers, func(m logql.DropMatcher) bool { return m.Name == name })
}

// The plans of a filter every entry passes and of one none passes.
var (
	levelAllRows = &levelFilterNodes{}
	levelNoRows  = &levelFilterNodes{Never: true}
)

// isConstantLevelPlan reports a plan that does not read the label: it may stand anywhere.
func isConstantLevelPlan(p *levelFilterNodes) bool { return p == levelAllRows || p == levelNoRows }

// checkLevelAfterStage is the plan of a label filter that names detected_level after a stage, in the state the label
// is in there, or the construct of one the lowering does not write. A filter on the ingest value is written as one no
// stage precedes (levelStage); a comparison with "" there, and any filter on a removed label, passes every entry or
// none.
func checkLevelAfterStage(e *logql.LabelFilterExpr, state levelState, prof Profile) (*levelFilterNodes, string) {
	if state == levelOther {
		return nil, levelAfterStageConstruct
	}
	if !e.IsLeaf() {
		return nil, levelTreeConstruct
	}
	if e.Name != translate.DetectedLevelLabel {
		return nil, levelSpellingConstruct
	}
	op := e.Cmp
	if op == "==" {
		op = "="
	}
	stringFilter := e.Kind == logql.LabelFilterValueString && (op == "=" || op == "!=" || op == "=~" || op == "!~")
	switch {
	case !stringFilter:
		return nil, levelFormConstruct
	case state == levelAbsent:
		return constantLevelPlan(op, e.Value, "")
	case e.Value == "" && (op == "=" || op == "!="):
		// Every entry has a level at ingest: any value that is not "" answers for it.
		return constantLevelPlan(op, e.Value, "unknown")
	}
	return levelStage(e, prof)
}

// constantLevelPlan is the plan of the matcher op value on a label that holds label on every entry.
func constantLevelPlan(op, value, label string) (*levelFilterNodes, string) {
	var match bool
	switch op {
	case "=", "!=":
		match = label == value
	default:
		re, err := regexp.Compile("^(?:" + translate.LokiLabelFilterPattern(value) + ")$")
		if err != nil {
			return nil, levelFormConstruct
		}
		match = re.MatchString(label)
	}
	if op == "!=" || op == "!~" {
		match = !match
	}
	if match {
		return levelAllRows, ""
	}
	return levelNoRows, ""
}

// hoistLevelFilters is sel with the filters on the ingest value after the stage at index first moved ahead of it,
// in their order: the pipeline Loki answers the same for, since the filters read the value no stage between changes.
// holds says the label is still the ingest value at the end of the pipeline.
func hoistLevelFilters(sel *queryplan.LogSelect, levels *levelStages, first int, holds bool) *queryplan.LogSelect {
	var moved, rest []queryplan.Stage
	// The plans follow the stage order: those of the moved filters come ahead of the constant ones left behind.
	var ahead, behind []*levelFilterNodes
	k, parser := 0, false
	for i, st := range sel.Stages {
		if f, ok := st.(*queryplan.LabelFilter); ok && isLevelLeaf(f.Expr) {
			p := levels.plan(k)
			k++
			switch {
			case i < first:
				ahead = append(ahead, p)
			case isConstantLevelPlan(p):
				behind = append(behind, p)
			default:
				ahead = append(ahead, p)
				moved = append(moved, st)
				continue
			}
		}
		if i < first {
			continue
		}
		_, isParser := st.(*queryplan.Parse)
		parser = parser || isParser
		rest = append(rest, st)
	}
	stages := append(append(append(make([]queryplan.Stage, 0, len(sel.Stages)), sel.Stages[:first]...), moved...), rest...)
	ordered := levelStages{any: levels.any, mode: levels.mode,
		keepsAcrossParsers: holds && parser && levels.mode == translate.DetectedLevelFilterMetric}
	for _, p := range append(ahead, behind...) {
		ordered.add(p)
	}
	*levels = ordered
	return &queryplan.LogSelect{Matchers: sel.Matchers, Stages: stages, Sem: sel.Sem}
}

// levelKeepField holds the derived detected_level of a metric row across its parsers; the chain's own deletes
// (__dl_*) run ahead of it.
const levelKeepField = "__dl_keep"

// hold saves the derived detected_level of a metric row ahead of the first parser behind a filter moved ahead of it.
func (w *levelWriter) hold(q *queryParts) {
	if w.levels.keepsAcrossParsers && !w.held && w.wrote {
		w.held = true
		q.pipe(logsql.PipeFormat{Template: "<" + translate.DetectedLevelLabel + ">", ResultField: levelKeepField})
	}
}

// restore puts a held detected_level back once every stage is written.
func (w *levelWriter) restore(q *queryParts) {
	if w.held {
		q.pipes = append(q.pipes, logsql.PipeRename{Pairs: [][2]string{{levelKeepField, translate.DetectedLevelLabel}}})
	}
}

// WritesLevelAfterStage reports a plan with a filter on detected_level after a stage that the lowering writes as
// Loki reads it, where the string translator filters the field a parser, a drop or a formatting stage leaves
// (semantics/level-filter-after-stage).
func WritesLevelAfterStage(p *queryplan.Plan) bool { return nodeWritesLevelAfterStage(p.Root) }

func nodeWritesLevelAfterStage(n queryplan.Node) bool {
	switch n := n.(type) {
	case *queryplan.LogSelect:
		return selectWritesLevelAfterStage(n)
	case *queryplan.RangeAgg:
		return n.In != nil && selectWritesLevelAfterStage(n.In)
	case *queryplan.VectorAgg:
		return nodeWritesLevelAfterStage(n.In)
	case *queryplan.Rank:
		return nodeWritesLevelAfterStage(n.In)
	case *queryplan.LabelReplace:
		return nodeWritesLevelAfterStage(n.In)
	case *queryplan.BinOp:
		return nodeWritesLevelAfterStage(n.L) || nodeWritesLevelAfterStage(n.R)
	}
	return false
}

func selectWritesLevelAfterStage(sel *queryplan.LogSelect) bool {
	state, staged, found := levelIngest, false, false
	for _, st := range sel.Stages {
		if f, ok := st.(*queryplan.LabelFilter); ok {
			if staged && f.Expr != nil && namesLevel(f.Expr) {
				if _, c := checkLevelAfterStage(f.Expr, state, Profile{}); c != "" {
					return false
				}
				found = true
			}
			continue
		}
		if next, changes := levelTransition(state, st); changes {
			state, staged = next, true
		}
	}
	return found
}
