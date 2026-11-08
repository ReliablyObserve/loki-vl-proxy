// Package lower lowers a logical plan (internal/queryplan) to the physical
// operators the proxy runs (internal/queryplan/physical) under a Profile. It
// sits above both and below the translator and the proxy, which it never
// imports. A construct is lowered natively once its lowering lands, held to
// the string translator's operators by the shadow harness; until then Plan
// answers ErrNotLowered and the proxy uses the string translator.
package lower

import (
	"errors"
	"fmt"
	"strings"

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

// logSelect lowers a log query. A bare selector is its stream filter (translate.StreamSelectorFilter, the filter
// the string translator writes; keeping the line aside adds nothing without a line_format); a pipeline is not
// lowered yet.
func logSelect(sel *queryplan.LogSelect, prof Profile) (physical.Op, error) {
	if len(sel.Stages) > 0 {
		return nil, &NotLoweredError{Construct: "LogSelect pipeline"}
	}
	f, err := translate.StreamSelectorFilter(sel.Matchers, prof.Label, prof.StreamFields)
	if err != nil {
		return nil, err
	}
	return physical.VLQuery{Text: f.String()}, nil
}

// Construct is the kind of the node at the top of a plan, the unit the shadow
// harness counts delegations by: LogSelect, RangeAgg, VectorAgg, Rank, BinOp,
// LabelReplace, VectorLit, Scalar or Opaque.
func Construct(n queryplan.Node) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", n), "*queryplan.")
}
