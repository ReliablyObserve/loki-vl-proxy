package lower

import (
	"strings"
	"sync"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// Label filters on detected_level. Loki attaches detected_level at ingest; the proxy derives it on the read path,
// so the string translator writes a filter on it that no parser, line rewrite or label stage precedes as the
// derivation plus the filter (translate.BuildDetectedLevelFilter, built typed: detected_level_ast.go): a prefilter the rows go through first (a filter
// next to the query's filters, or a filter pipe of its own after a pipe), and the pipes that derive the value and
// filter it, held back until the next stage that is not such a filter so that the line filters in between stay
// filters of the query and consecutive filters share one chain (translate.JoinDetectedLevelFilters). A log query
// derives the value into a scratch field it deletes again (DetectedLevelFilterLogs, or the stored level fields alone
// under -detected-level-body-scan=false: DetectedLevelFilterStored); a metric row writes it into detected_level
// with the grouping chain (DetectedLevelFilterMetric), which a grouping by detected_level then reuses (metricLog).
//
// Every other filter on detected_level is left to the string translator under a construct name: one after a parser,
// a drop (other than of the parser error labels), a keep or a formatting stage, which the translator reads as the
// stored or parsed level field (an open Loki difference, severity/detected-level-stage-side-effects); a comparison
// or ip() filter, an invalid regexp and a log query's empty raw string, which take the same path; an and/or tree
// holding one; a label name the translator reads as detected_level only after sanitizing it; and `detected_level=""`
// and `!=""`, which a log query reads as the presence of the label (no row and every row before a stage that drops,
// keeps, renames or parses labels) and a metric row as no row and every row: the translator answers them without a
// chain, for fewer allocations than the plan's parse costs. An unwrap over such a filter is left to it too: the
// translator reads the unpack pipes of the chain as the parser of the unwrapped label.

// levelStages are the detected_level filters of a log query the lowering writes: their form and their plans, in
// stage order (the first in room, the rest in more). Once detectedLevelStages accepted the query, every label filter
// stage that is a leaf named detected_level is one.
type levelStages struct {
	any  bool
	mode translate.DetectedLevelFilterMode
	n    int
	room [4]*levelFilterNodes
	more []*levelFilterNodes
}

// add records the plan of the next level filter.
func (l *levelStages) add(p *levelFilterNodes) {
	if l.n < len(l.room) {
		l.room[l.n] = p
	} else {
		l.more = append(l.more, p)
	}
	l.n++
}

// plan is the plan of the k-th level filter.
func (l *levelStages) plan(k int) *levelFilterNodes {
	if k < len(l.room) {
		return l.room[k]
	}
	return l.more[k-len(l.room)]
}

// The constructs the filters on detected_level the lowering does not write are delegated as (letters and spaces, at
// most 48 bytes with the owner in front: the live shadow's label).
const (
	levelAfterStageConstruct = "LogSelect level filter after stage"
	levelFormConstruct       = "LogSelect level filter form"
	levelTreeConstruct       = "LogSelect level filter tree"
	levelSpellingConstruct   = "LogSelect level filter spelling"
	levelPresenceConstruct   = "LogSelect level filter presence"
	levelUnwrapConstruct     = "LogSelect level filter unwrap"
)

// levelFilterMode is the form of a detected_level filter in a query (the translator's detectedLevelFilterMode): a
// metric row's log query is written with no capabilities, so the body is always read there.
func levelFilterMode(prof Profile) translate.DetectedLevelFilterMode {
	switch {
	case prof.metric:
		return translate.DetectedLevelFilterMetric
	case prof.Caps.NoDetectedLevelBodyScan:
		return translate.DetectedLevelFilterStored
	}
	return translate.DetectedLevelFilterLogs
}

// detectedLevelStages checks the label filter stages of sel that filter on detected_level, and answers the construct
// of the first one the lowering does not write.
func detectedLevelStages(sel *queryplan.LogSelect, prof Profile) (levelStages, string) {
	var res levelStages
	// intact holds while every stage so far left the line and the stored fields as VictoriaLogs stored them: label
	// filters and a drop of the parser error labels (line filters are not stages here).
	intact := true
	for _, st := range sel.Stages {
		switch s := st.(type) {
		case *queryplan.LineFilter:
			continue
		case *queryplan.Drop:
			intact = intact && dropsOnlyErrorLabels(s)
			continue
		case *queryplan.LabelFilter:
			if s.Expr == nil {
				continue
			}
			plan, c := checkLevelStage(s.Expr, intact, prof)
			if c != "" {
				return levelStages{}, c
			}
			if plan != nil {
				res.any, res.mode = true, levelFilterMode(prof)
				res.add(plan)
			}
			continue
		}
		intact = false
	}
	return res, ""
}

// checkLevelStage is the plan of a label filter on detected_level the lowering writes (levelStage), or the construct
// of one it does not; nil and "" for a filter on another label.
func checkLevelStage(e *logql.LabelFilterExpr, intact bool, prof Profile) (*levelFilterNodes, string) {
	if !e.IsLeaf() {
		if namesLevel(e) {
			return nil, levelTreeConstruct
		}
		return nil, ""
	}
	if e.Name != translate.DetectedLevelLabel {
		if strings.Contains(e.Name, translate.DetectedLevelLabel) && translate.SanitizeFieldIdentifier(e.Name) == translate.DetectedLevelLabel {
			return nil, levelSpellingConstruct
		}
		return nil, ""
	}
	if !intact {
		return nil, levelAfterStageConstruct
	}
	return levelStage(e, prof)
}

// isLevelLeaf reports a label filter stage that is one filter on detected_level, as written.
func isLevelLeaf(e *logql.LabelFilterExpr) bool {
	return e != nil && e.IsLeaf() && e.Name == translate.DetectedLevelLabel
}

// namesLevel reports whether a leaf of the tree e names detected_level, as written or once sanitized.
func namesLevel(e *logql.LabelFilterExpr) bool {
	if !e.IsLeaf() {
		return namesLevel(e.Left) || namesLevel(e.Right)
	}
	return strings.Contains(e.Name, translate.DetectedLevelLabel) && translate.SanitizeFieldIdentifier(e.Name) == translate.DetectedLevelLabel
}

// levelStage is the plan of a detected_level leaf that intact stages precede, or the construct of a form the
// translator does not read as a filter on Loki's value (detectedLevelStageFilter): an operator other than =, ==, !=,
// =~ and !~, a value that is not a string, a regexp that does not compile, and a log query's empty value (the
// presence of the label for a double-quoted one, the stored level otherwise).
func levelStage(e *logql.LabelFilterExpr, prof Profile) (*levelFilterNodes, string) {
	op := e.Cmp
	switch op {
	case "==":
		op = "="
	case "=", "!=", "=~", "!~":
	default:
		return nil, levelFormConstruct
	}
	if e.Kind != logql.LabelFilterValueString {
		return nil, levelFormConstruct
	}
	if e.Value == "" && (op == "=" || op == "!=") {
		// A log query reads a double-quoted empty value as the presence of the label (detectedLevelEmptyFilterRE)
		// and a raw one as the stored level; a metric row matches no row or every row. The translator writes each
		// without a chain, for fewer allocations than the plan's parse costs.
		if prof.metric || strings.HasSuffix(e.Leaf, `""`) {
			return nil, levelPresenceConstruct
		}
		return nil, levelFormConstruct
	}
	plan, ok := levelFilterPlan(op, e.Value, levelFilterMode(prof))
	if !ok {
		return nil, levelFormConstruct
	}
	return plan, ""
}

// levelPlanKey is what a detected_level filter's plan is built from (the field list is always
// translate.DetectedLevelFieldNames).
type levelPlanKey struct {
	op, value string
	mode      translate.DetectedLevelFilterMode
}

// levelPlans holds the plans built so far (nil for a filter the translator does not read as one), by levelPlanKey.
// Grafana sends the same few level filters over and over, and a plan is most of the cost of lowering one: it is
// built once per key. The memo is bounded: past maxLevelPlans keys, or for a value longer than maxLevelPlanValue
// bytes, a plan is built each time and not kept. A map under a lock, so a lookup copies nothing.
var levelPlans = struct {
	sync.RWMutex
	m map[levelPlanKey]*levelFilterNodes
}{m: make(map[levelPlanKey]*levelFilterNodes, maxLevelPlans)}

const (
	maxLevelPlans     = 256
	maxLevelPlanValue = 128
)

// levelFilterPlan is the typed translate.BuildDetectedLevelFilter over every level field for op, value and mode
// (buildLevelFilterNodes), through the memo; false when it fails (an invalid regexp).
func levelFilterPlan(op, value string, mode translate.DetectedLevelFilterMode) (*levelFilterNodes, bool) {
	key := levelPlanKey{op: op, value: value, mode: mode}
	levelPlans.RLock()
	plan, ok := levelPlans.m[key]
	levelPlans.RUnlock()
	if ok {
		return plan, plan != nil
	}
	plan = nil
	if built, err := buildLevelFilterNodes(op, value, mode); err == nil {
		plan = &built
	}
	if len(value) <= maxLevelPlanValue {
		levelPlans.Lock()
		if len(levelPlans.m) < maxLevelPlans {
			levelPlans.m[key] = plan
		}
		levelPlans.Unlock()
	}
	return plan, plan != nil
}

// resetLevelPlans empties the memo (tests measure the first plan of a key with it).
func resetLevelPlans() {
	levelPlans.Lock()
	clear(levelPlans.m)
	levelPlans.Unlock()
}

// dropsOnlyErrorLabels reports whether d drops the parser error labels alone, which leaves the level inputs as
// stored.
func dropsOnlyErrorLabels(d *queryplan.Drop) bool {
	if len(d.Matchers) > 0 {
		return false
	}
	for _, l := range d.Labels {
		if l != "__error__" && l != "__error_details__" {
			return false
		}
	}
	return true
}

// levelWriter writes the detected_level filters of a log query into its parts: the prefilter where the stage stands,
// the pipes held back until the next stage that is not a level filter (or the end), one chain for those in a row.
type levelWriter struct {
	levels levelStages
	// next is the index of the next level filter's plan in levels.
	next    int
	pending []*levelFilterNodes
	// wrote says the pipes of a level filter were written.
	wrote bool
}

// write takes stage st when it is a level filter (and reports it), and writes the pipes held so far before any
// other stage but a line filter.
func (w *levelWriter) write(st queryplan.Stage, q *queryParts) bool {
	switch s := st.(type) {
	case *queryplan.LineFilter:
		return false
	case *queryplan.LabelFilter:
		if w.levels.any && isLevelLeaf(s.Expr) {
			w.stage(q)
			return true
		}
	}
	w.flush(q)
	return false
}

// stage writes the next level filter: its prefilter, bare or, after a pipe, in a filter pipe of its own, as the
// translator writes it; its pipes wait.
func (w *levelWriter) stage(q *queryParts) {
	plan := w.levels.plan(w.next)
	w.next++
	if f := levelPrefilter(plan); f != nil {
		if q.lastPipe {
			q.own(f)
		} else {
			q.add(f)
		}
	}
	if !plan.Never {
		w.pending = append(w.pending, plan)
	}
}

// flush writes the pipes of the level filters held so far, after the filters before them.
func (w *levelWriter) flush(q *queryParts) {
	if len(w.pending) == 0 {
		return
	}
	if pipes := joinLevelFilters(w.pending, w.levels.mode); len(pipes) > 0 {
		q.pipe(pipes...)
		w.wrote = true
	}
	w.pending = w.pending[:0]
}

// levelPrefilter is the filter a level stage puts in front of its pipes: the prefilter, the filter no row passes
// when the matcher accepts none, or nil.
func levelPrefilter(plan *levelFilterNodes) logsql.FilterExpr {
	switch {
	case plan.Never:
		return translate.MatchNoRows()
	}
	return plan.Prefilter
}
