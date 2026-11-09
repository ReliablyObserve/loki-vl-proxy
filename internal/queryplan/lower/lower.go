// Package lower lowers a logical plan (internal/queryplan) to the physical
// operators the proxy runs (internal/queryplan/physical) under a Profile,
// building the VictoriaLogs filters with the builders it shares with the
// string translator (internal/translate). It sits above them and below the
// translator and the proxy, which it never imports. A construct is lowered natively once its lowering lands, held to
// the string translator's operators by the shadow harness; until then Plan
// answers ErrNotLowered and the proxy uses the string translator.
package lower

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// Profile is what changes the VictoriaLogs query a plan lowers to: the
// capabilities of the VictoriaLogs version, the client-to-VictoriaLogs label
// names, the stream fields, and whether a log query keeps its stored line
// aside for a later line_format. The physical inputs never enter the logical
// plan or its key.
type Profile struct {
	Caps         logsql.Capabilities
	Label        func(string) string
	StreamFields map[string]bool
	KeepLine     bool
}

// ErrNotLowered is the error of a plan whose construct has no native lowering
// yet: the proxy answers it with the string translator.
var ErrNotLowered = errors.New("lower: not lowered")

// NotLoweredError names the construct that is not lowered; it is ErrNotLowered.
type NotLoweredError struct {
	Construct string
}

func (e *NotLoweredError) Error() string {
	return fmt.Sprintf("lower: %s is not lowered", e.Construct)
}

// Is reports ErrNotLowered.
func (e *NotLoweredError) Is(target error) bool { return target == ErrNotLowered }

// Plan lowers a resolved logical plan to physical operators under prof. Each
// construct is lowered natively once its lowering lands, and the shadow
// harness holds it to the string translator's operators on the whole corpus
// under every profile (internal/querycorpus TestPlanShadow); until then Plan
// answers ErrNotLowered naming the construct.
func Plan(p *queryplan.Plan, prof Profile) (physical.Op, error) {
	switch n := p.Root.(type) {
	case *queryplan.LogSelect:
		return logSelect(n, prof)
	case *queryplan.RangeAgg:
		return rangeAgg(n, prof)
	}
	return nil, &NotLoweredError{Construct: Construct(p.Root)}
}

// logSelect lowers a log query: its stream filter (translate.StreamSelectorFilter, the filter the string
// translator writes; keeping the line aside adds nothing without a line_format), its line filters
// (translate.LineFilter, and translate.PatternLineFilter for |> and !>) and its label filters without a parser
// (labelFilter), in order. A pattern filter that needs the extract pipe adds pipes; the filters after a pipe go
// into one filter pipe up to the next pipe, as the string translator writes them, except that a label filter
// that is a field filter and directly follows a pipe or a label filter in a filter pipe opens a filter pipe of
// its own (the translator wraps it as it translates it, where a line filter is wrapped once the query is
// assembled). Any other stage is not lowered yet.
func logSelect(sel *queryplan.LogSelect, prof Profile) (physical.Op, error) {
	q, err := logSelectQuery(sel, prof)
	if err != nil {
		return nil, err
	}
	return physical.VLQuery{Text: q.String()}, nil
}

// logSelectQuery is the typed query logSelect prints: a range aggregation appends its stats pipe to it.
func logSelectQuery(sel *queryplan.LogSelect, prof Profile) (logsql.Query, error) {
	built, err := labelFilters(sel, prof)
	if err != nil {
		return logsql.Query{}, err
	}
	var filters, after logsql.AllOf
	var pipes []logsql.Pipe
	flush := func() {
		switch len(after) {
		case 0:
		case 1:
			pipes = append(pipes, logsql.PipeFilter{Expr: after[0]})
		default:
			pipes = append(pipes, logsql.PipeFilter{Expr: after})
		}
		after = nil
	}
	add := func(fs ...logsql.FilterExpr) {
		if len(pipes) == 0 {
			filters = append(filters, fs...)
		} else {
			after = append(after, fs...)
		}
	}
	// seq numbers the pattern alternatives that extract, across the query.
	seq := 0
	// lastPipe is whether the last part the string translator holds starts with a pipe: a pipe, or a label
	// filter it wrapped in a filter pipe.
	lastPipe := false
	for i, st := range sel.Stages {
		if _, ok := st.(*queryplan.LabelFilter); ok {
			f := built[i]
			if lastPipe && translate.IsFieldFilter(f.String()) {
				flush()
				after = logsql.AllOf{f}
			} else {
				add(f)
				lastPipe = false
			}
			continue
		}
		lf, ok := st.(*queryplan.LineFilter)
		if !ok {
			return logsql.Query{}, &NotLoweredError{Construct: "LogSelect pipeline"}
		}
		if lf.Op == logql.LineFilterContainsPat || lf.Op == logql.LineFilterExcludePat {
			values := []string{lf.Value}
			for _, alt := range lf.Or {
				values = append(values, alt.Value)
			}
			fs, ps := translate.PatternLineFilter(values, lf.Op == logql.LineFilterExcludePat, &seq)
			add(fs...)
			if len(ps) > 0 {
				flush()
				pipes = append(pipes, ps...)
			}
			lastPipe = len(ps) > 0
			continue
		}
		f, ok := translate.LineFilter(lf)
		if !ok {
			return logsql.Query{}, &NotLoweredError{Construct: "LogSelect line filter"}
		}
		if f != nil {
			add(f)
			lastPipe = false
		}
	}
	flush()
	f, err := translate.StreamSelectorFilter(sel.Matchers, prof.Label, prof.StreamFields)
	if err != nil {
		return logsql.Query{}, err
	}
	q := logsql.Query{Filter: f, Pipes: pipes}
	if len(filters) > 0 {
		// A selector that keeps every row (`{detected_level!="info"}`) is no filter next to the others, as the
		// string translator writes it.
		if _, all := f.(logsql.Wildcard); all {
			q.Filter = filters
		} else {
			q.Filter = append(logsql.AllOf{f}, filters...)
		}
	}
	return q, nil
}

// labelFilters builds the filter of each label filter stage of the query, by stage index (nil elsewhere), or
// answers NotLoweredError for a query whose label filters the lowering does not write: a filter on
// detected_level or service_name (the string translator derives them), a value that is not a valid literal, and a
// label that translates to several parsed keys or to no field.
func labelFilters(sel *queryplan.LogSelect, prof Profile) ([]logsql.FilterExpr, error) {
	var built []logsql.FilterExpr
	for i, st := range sel.Stages {
		lbl, ok := st.(*queryplan.LabelFilter)
		if !ok {
			continue
		}
		if lbl.Expr == nil {
			return nil, &NotLoweredError{Construct: "LogSelect label filter"}
		}
		if c := checkLabelFilterExpr(lbl.Expr, prof); c != "" {
			return nil, &NotLoweredError{Construct: c}
		}
		if built == nil {
			built = make([]logsql.FilterExpr, len(sel.Stages))
		}
		built[i] = labelFilter(lbl.Expr, prof.Label, prof.Caps)
	}
	return built, nil
}

// checkLabelFilterExpr names the construct of the first leaf the lowering does not write, "" when it writes all.
func checkLabelFilterExpr(e *logql.LabelFilterExpr, prof Profile) string {
	if !e.IsLeaf() {
		if c := checkLabelFilterExpr(e.Left, prof); c != "" {
			return c
		}
		return checkLabelFilterExpr(e.Right, prof)
	}
	if e.Kind == logql.LabelFilterValueNone {
		return "LogSelect label filter value"
	}
	label := translate.SanitizeFieldIdentifier(e.Name)
	switch label {
	case "":
		return "LogSelect label filter name"
	case "detected_level", "service_name":
		return "LogSelect label filter " + label
	}
	if prof.Label != nil {
		translated := prof.Label(label)
		if strings.Contains(translated, translate.ParsedKeySep) {
			return "LogSelect label filter parsed keys"
		}
		if translate.SanitizeFieldIdentifier(translated) == "" {
			return "LogSelect label filter name"
		}
	}
	if e.Kind == logql.LabelFilterValueIP && !strings.HasSuffix(e.Leaf, `ip("`+e.Value+`")`) {
		return "LogSelect label filter ip form"
	}
	if !translate.KnownLabelFilterCmp(e.Cmp) {
		return "LogSelect label filter operator"
	}
	return ""
}

// labelFilter is the filter of a label filter stage that checkLabelFilters accepted: the leaf, or the and/or
// tree of leaves in parentheses, each translated like a stand-alone stage (translateLabelFilterExpr).
func labelFilter(e *logql.LabelFilterExpr, label func(string) string, caps logsql.Capabilities) logsql.FilterExpr {
	if e.IsLeaf() {
		return labelFilterLeaf(e, label, caps)
	}
	var operands []logsql.FilterExpr
	collectLabelFilter(e, e.Op, &operands, label, caps)
	var tree logsql.FilterExpr
	for i, o := range operands {
		switch {
		case i == 0:
			tree = o
		case e.Op == logql.LabelFilterOr:
			tree = logsql.OrExpr{Left: tree, Right: o}
		default:
			tree = logsql.AndExpr{Left: tree, Right: o}
		}
	}
	return logsql.Group{Expr: tree}
}

// collectLabelFilter flattens a chain of one operator, parenthesizing a leaf that holds a top-level space and a
// nested chain of the other operator (LabelFilterExpr.Render).
func collectLabelFilter(e *logql.LabelFilterExpr, op logql.LabelFilterOp, out *[]logsql.FilterExpr, label func(string) string, caps logsql.Capabilities) {
	switch {
	case e.IsLeaf():
		f := labelFilterLeaf(e, label, caps)
		if translate.HasTopLevelSpace(f.String()) {
			f = logsql.Group{Expr: f}
		}
		*out = append(*out, f)
	case e.Op != op:
		*out = append(*out, labelFilter(e, label, caps))
	default:
		collectLabelFilter(e.Left, op, out, label, caps)
		collectLabelFilter(e.Right, op, out, label, caps)
	}
}

// labelFilterLeaf is translateSingleLabelFilter's generic path for one leaf: the label named like the selector
// names it, quoted where LogsQL needs it, and the filter translate.LabelFilterLeaf builds for its value.
func labelFilterLeaf(e *logql.LabelFilterExpr, labelFn func(string) string, caps logsql.Capabilities) logsql.FilterExpr {
	label := translate.SanitizeFieldIdentifier(e.Name)
	if labelFn != nil {
		label = translate.SanitizeFieldIdentifier(labelFn(label))
	}
	if strings.ContainsAny(label, ".-") {
		label = `"` + label + `"`
	}
	f, _ := translate.LabelFilterLeaf(label, e.Cmp, e.Value, e.Kind == logql.LabelFilterValueIP, caps) // the operator is known
	return f
}

// Construct is the kind of the node at the top of a plan, the unit the shadow
// harness counts delegations by: LogSelect, RangeAgg, VectorAgg, Rank, BinOp,
// LabelReplace, VectorLit, Scalar or Opaque.
func Construct(n queryplan.Node) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", n), "*queryplan.")
}
