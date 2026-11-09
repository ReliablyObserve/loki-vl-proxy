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
	if sel, ok := p.Root.(*queryplan.LogSelect); ok {
		return logSelect(sel, prof)
	}
	return nil, &NotLoweredError{Construct: Construct(p.Root)}
}

// logSelect lowers a log query: its stream filter (translate.StreamSelectorFilter, the filter the string
// translator writes; keeping the line aside adds nothing without a line_format) and its line filters
// (translate.LineFilter, and translate.PatternLineFilter for |> and !>), in order. A pattern filter that needs
// the extract pipe adds pipes; the filters after a pipe go into one filter pipe up to the next pipe, as the
// string translator writes them. Any other stage is not lowered yet.
func logSelect(sel *queryplan.LogSelect, prof Profile) (physical.Op, error) {
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
	for _, st := range sel.Stages {
		lf, ok := st.(*queryplan.LineFilter)
		if !ok {
			return nil, &NotLoweredError{Construct: "LogSelect pipeline"}
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
			continue
		}
		f, ok := translate.LineFilter(lf)
		if !ok {
			return nil, &NotLoweredError{Construct: "LogSelect line filter"}
		}
		if f != nil {
			add(f)
		}
	}
	flush()
	f, err := translate.StreamSelectorFilter(sel.Matchers, prof.Label, prof.StreamFields)
	if err != nil {
		return nil, err
	}
	q := logsql.Query{Filter: f, Pipes: pipes}
	if len(filters) > 0 {
		q.Filter = append(logsql.AllOf{f}, filters...)
	}
	return physical.VLQuery{Text: q.String()}, nil
}

// Construct is the kind of the node at the top of a plan, the unit the shadow
// harness counts delegations by: LogSelect, RangeAgg, VectorAgg, Rank, BinOp,
// LabelReplace, VectorLit, Scalar or Opaque.
func Construct(n queryplan.Node) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", n), "*queryplan.")
}
