package lower

import (
	"slices"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A keep stage with matchers leaves a label a matcher names only when one of the stage's matchers on that name
// matches its value. VictoriaLogs' fields projection keeps the field whatever its value, so a later stage that reads
// the name would see a label Loki has removed: the field is blanked, which VictoriaLogs reads as a missing label,
// where no matcher matches (semantics/keep-names-and-matchers). The proxy removes the label from the answer itself,
// so a log query's keep whose matcher names no later stage reads writes no pipe. A metric query always reads them:
// its series are the labels the keep leaves.

// keepStagePipes are the pipes of the keep at stage i: its projection (a metric row's also deletes the level the
// derived-level chain needs), then the blanking of the matcher-named fields that are read.
//
// After a parser of a log query, copies are the stream labels the query reads from their stored value
// (translate.StreamValueScratch): the projection keeps the copy of each one the stage names, and a matcher reads and
// blanks the copy, as Loki's keep reads the stream label there and not the parsed key.
func keepStagePipes(sel *queryplan.LogSelect, i int, st *queryplan.Keep, label func(string) string, metric bool, copies []string) []logsql.Pipe {
	var ps []logsql.Pipe
	if metric {
		ps = metricKeepPipes(st, label)
	} else {
		ps = keepPipes(st, label)
	}
	if fields, ok := ps[0].(logsql.PipeFields); ok {
		for _, n := range copies {
			if keepNames(st, n) {
				fields.Labels = append(fields.Labels, translate.StreamValueScratch+n)
				ps[0] = fields
			}
		}
	}
	if len(st.Matchers) == 0 {
		return ps
	}
	return keepBlankPipes(ps, sel, i, st, label, metric, copies)
}

// keepNames reports whether the keep names n, bare or with a matcher.
func keepNames(st *queryplan.Keep, n string) bool {
	return slices.Contains(st.Labels, n) ||
		slices.ContainsFunc(st.Matchers, func(m logql.DropMatcher) bool { return m.Name == n })
}

// keepStreamReads are the stream labels among names that a matcher of a keep behind a parser names and a later stage
// of the log query reads: the lowering reads their stored values.
func keepStreamReads(sel *queryplan.LogSelect, names []string) []string {
	var out []string
	parsed := false
	for i, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Parse:
			parsed = true
		case *queryplan.Keep:
			if !parsed {
				continue
			}
			for j, m := range st.Matchers {
				if slices.Contains(names, m.Name) && !slices.Contains(out, m.Name) && blanksKeepName(sel, i, st, j, false) &&
					stageReads(sel.Stages[i+1:], m.Name) {
					out = append(out, m.Name)
				}
			}
		}
	}
	return out
}

// withNames are the names of all that read or more holds, in the order of all.
func withNames(all, read, more []string) []string {
	if len(more) == 0 {
		return read
	}
	var out []string
	for _, n := range all {
		if slices.Contains(read, n) || slices.Contains(more, n) {
			out = append(out, n)
		}
	}
	return out
}

// WritesKeepStreamValue reports a plan with a log query whose keep behind a parser names one of the profile's stream
// labels: where the query reads that label, the plan keeps and tests its stored value, which the string translator's
// projection drops (semantics/keep-names-and-matchers).
func WritesKeepStreamValue(p *queryplan.Plan, prof Profile) bool {
	if p == nil {
		return false
	}
	sel, ok := p.Root.(*queryplan.LogSelect)
	if !ok {
		return false
	}
	names := translate.StreamLabelNames(prof.Label)
	parsed := false
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Parse:
			parsed = true
		case *queryplan.Keep:
			if parsed && slices.ContainsFunc(names, func(n string) bool { return keepNames(st, n) }) {
				return true
			}
		}
	}
	return false
}

// keepBlankPipes appends to out the pipes that blank, behind the projection of the keep at stage i, the
// matcher-named fields later stages read (every one in a metric query).
func keepBlankPipes(out []logsql.Pipe, sel *queryplan.LogSelect, i int, st *queryplan.Keep, label func(string) string, metric bool, copies []string) []logsql.Pipe {
	for j, m := range st.Matchers {
		name := m.Name
		if !blanksKeepName(sel, i, st, j, metric) || !metric && !stageReads(sel.Stages[i+1:], name) {
			continue
		}
		// A stream label read from its copy: the copy holds the value Loki's keep reads and later stages filter on.
		field, fieldLabel := name, label
		if slices.Contains(copies, name) {
			field, fieldLabel = translate.StreamValueScratch+name, nil
		}
		// A metric query's detected_level and service_name are the fields the derivations ahead of the pipeline set.
		computed := metric && derivedName(name)
		if computed {
			fieldLabel = nil
		}
		var rejected logsql.FilterExpr
		for _, other := range st.Matchers[j:] {
			if other.Name != name {
				continue
			}
			negate, isRe := other.Op == "!=" || other.Op == "!~", other.Op == "=~" || other.Op == "!~"
			var f logsql.FilterExpr
			if computed {
				f = translate.ComputedFieldFilter(field, negate, isRe, other.Value)
			} else {
				f = translate.MatcherFieldFilter(field, negate, isRe, other.Value, fieldLabel)
			}
			if f == nil {
				return out
			}
			if rejected == nil {
				rejected = logsql.Not(f)
			} else {
				rejected = logsql.And(rejected, logsql.Not(f))
			}
		}
		out = append(out, logsql.PipeFormat{If: rejected, ResultField: field})
		if fieldLabel != nil {
			if mapped := fieldLabel(name); mapped != "" && mapped != name {
				out = append(out, logsql.PipeFormat{If: rejected, ResultField: mapped})
			}
		}
	}
	return out
}

// blanksKeepName reports whether the j-th matcher of the keep at stage i is the first on a name whose field the
// stage may remove: not listed bare, and one the selector does not already hold to a value a matcher accepts. A log
// query blanks stored fields only (detected_level and service_name are derived on the read path); a metric query also
// blanks those two, whose derivations the lowering writes ahead of the pipeline so the series group by what is left.
func blanksKeepName(sel *queryplan.LogSelect, i int, st *queryplan.Keep, j int, metric bool) bool {
	name := st.Matchers[j].Name
	if !metric && derivedName(name) || slices.Contains(st.Labels, name) {
		return false
	}
	return !slices.ContainsFunc(st.Matchers[:j], func(m logql.DropMatcher) bool { return m.Name == name }) &&
		!selectorHolds(sel, i, st, name)
}

// derivedName reports a label the read path derives.
func derivedName(name string) bool {
	return name == translate.DetectedLevelLabel || name == "service_name"
}

// selectorHolds reports a keep whose matchers on name accept every selected entry: the selector asks for one value of
// the label, one of the keep's matchers accepts it, and no stage before the keep writes the label.
func selectorHolds(sel *queryplan.LogSelect, i int, st *queryplan.Keep, name string) bool {
	for _, before := range sel.Stages[:i] {
		switch before.(type) {
		case *queryplan.LineFilter, *queryplan.LabelFilter, *queryplan.Keep, *queryplan.Drop:
		default:
			return false
		}
	}
	for _, s := range sel.Matchers {
		if s.Name != name || s.Op != logql.MatchEq || s.Value == "" {
			continue
		}
		return slices.ContainsFunc(st.Matchers, func(m logql.DropMatcher) bool {
			if m.Name != name {
				return false
			}
			cond, err := translate.NewDropCondition(m.Name, m.Op, m.Value)
			return err == nil && cond.Matches(s.Value)
		})
	}
	return false
}

// stageReads reports whether one of the stages reads the label name: a label filter, a drop or keep matcher, a
// line_format or label_format template, or an unwrap.
func stageReads(stages []queryplan.Stage, name string) bool {
	matched := func(ms []logql.DropMatcher) bool {
		return slices.ContainsFunc(ms, func(m logql.DropMatcher) bool { return m.Name == name })
	}
	for _, st := range stages {
		switch st := st.(type) {
		case *queryplan.LabelFilter:
			if st.Expr == nil && strings.Contains(st.Raw, name) || st.Expr != nil && namesDerivedLabel(st.Expr, name) {
				return true
			}
		case *queryplan.Drop:
			if matched(st.Matchers) {
				return true
			}
		case *queryplan.Keep:
			if matched(st.Matchers) {
				return true
			}
		case *queryplan.LineFormat:
			if strings.Contains(st.Template, name) {
				return true
			}
		case *queryplan.LabelFormat:
			for _, f := range st.Formats {
				if strings.Contains(f.Value, name) {
					return true
				}
			}
		case *queryplan.Unwrap:
			if st.Label == name {
				return true
			}
		}
	}
	return false
}

// WritesKeepBlank reports a plan with a keep or drop stage the string translator writes otherwise: one whose field the
// plan blanks (a metric query's keep, or one a later stage reads), where the translator reads the stored field
// (WritesDropBlank), or a keep with a matcher value holding a quote or an operator character, which the translator
// reads as part of the keep list.
func WritesKeepBlank(p *queryplan.Plan) bool {
	return p != nil && (writesKeepBlank(p.Root) || writesDropBlank(p.Root))
}

func writesKeepBlank(n queryplan.Node) bool {
	switch n := n.(type) {
	case *queryplan.LogSelect:
		return selectWritesKeepBlank(n, false)
	case *queryplan.RangeAgg:
		return n.In != nil && selectWritesKeepBlank(n.In, true)
	case *queryplan.VectorAgg:
		return writesKeepBlank(n.In)
	case *queryplan.Rank:
		return writesKeepBlank(n.In)
	case *queryplan.LabelReplace:
		return writesKeepBlank(n.In)
	case *queryplan.BinOp:
		return writesKeepBlank(n.L) || writesKeepBlank(n.R)
	}
	return false
}

func selectWritesKeepBlank(sel *queryplan.LogSelect, metric bool) bool {
	for i, st := range sel.Stages {
		k, ok := st.(*queryplan.Keep)
		if !ok {
			continue
		}
		for j := range k.Matchers {
			if strings.ContainsAny(k.Matchers[j].Value, "\"`=~!") {
				return true
			}
			if blanksKeepName(sel, i, k, j, metric) && (metric || stageReads(sel.Stages[i+1:], k.Matchers[j].Name)) {
				return true
			}
		}
	}
	return false
}
