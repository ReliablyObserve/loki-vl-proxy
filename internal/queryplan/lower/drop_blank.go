package lower

import (
	"slices"
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A drop stage removes a label, bare or where one of its matchers matches the value, and the proxy removes it from the
// answer. A later stage of the query must not read it either: the field is blanked behind the drop, which
// VictoriaLogs reads as a missing label (semantics/drop-matcher-later-stage-reads-dropped-value).
//
// A field detected_level is derived from (level and its spellings) is copied aside before it is blanked and put back
// after the last stage, so the entry's detected_level is derived from the stored value, as Loki's is attached before
// any stage runs.

// dropScratch prefixes the scratch copy of a blanked level field.
const dropScratch = "__lvp_drop_"

// dropBlank is one name the drop at stage i blanks.
type dropBlank struct {
	name  string
	level bool
}

// dropBlankNames are the names of the drop at stage i that a later stage reads and whose field the plan blanks: a
// matcher's name, or a bare level field (a bare drop of any other name deletes its field). detected_level and
// service_name are derived on the read path and stay with their own reading. A level field stays as it is where a
// later parser or label_format may write it again, or the query derives detected_level for a filter. A matcher's name
// whose only later reader is a label filter the plan rewrites to read the stored value (dropHoistReader) is not
// blanked.
func dropBlankNames(sel *queryplan.LogSelect, i int, st *queryplan.Drop, metric bool) []dropBlank {
	var out []dropBlank
	add := func(name string, bare bool) {
		if !dropBlankCandidate(sel, i, st, name, bare) || slices.ContainsFunc(out, func(b dropBlank) bool { return b.name == name }) {
			return
		}
		if !bare && !slices.Contains(st.Labels, name) {
			if j, _ := dropHoistReader(sel, i, name, metric); j >= 0 {
				return
			}
		}
		out = append(out, dropBlank{name: name, level: translate.IsLevelSource(name)})
	}
	for _, name := range st.Labels {
		add(name, true)
	}
	for _, m := range st.Matchers {
		add(m.Name, false)
	}
	return out
}

// dropBlankCandidate reports a name of the drop at stage i whose removed value a later stage reads (dropBlankNames).
func dropBlankCandidate(sel *queryplan.LogSelect, i int, st *queryplan.Drop, name string, bare bool) bool {
	level := translate.IsLevelSource(name)
	if derivedName(name) || bare && !level || len(name) > 1 && name[:2] == "__" {
		return false
	}
	return stageReads(sel.Stages[i+1:], name) && (!level || !rewritesLevel(sel, i, name))
}

// dropHoistReader is the label filter stage after the drop at stage i that the plan writes reading the stored value
// of name, in place of blanking the field the drop's matchers remove (-1 for none), and whether the filter accepts
// the empty value, as Loki compares it. The filter is the last stage that reads name, a single string comparison on
// it, and every stage between the drop and it is a line filter, decolorize or a label filter on other labels, so
// nothing reads or writes name in between. A filter F on the blanked value x' (x' is "" where the matchers R remove
// x) reads the same rows as F(x) AND NOT R when F("") is false, and as R OR F(x) when it is true; nothing after F
// reads x, so the blanking changes nothing else. A metric query groups by the fields, so only the first form, which
// keeps no removed value, is written there.
func dropHoistReader(sel *queryplan.LogSelect, i int, name string, metric bool) (int, bool) {
	for j := i + 1; j < len(sel.Stages); j++ {
		switch st := sel.Stages[j].(type) {
		case *queryplan.LineFilter, *queryplan.Decolorize:
			continue
		case *queryplan.LabelFilter:
			if !stageReads(sel.Stages[j:j+1], name) {
				continue
			}
			e := st.Expr
			if e == nil || !e.IsLeaf() || e.Name != name || e.Kind != logql.LabelFilterValueString ||
				stageReads(sel.Stages[j+1:], name) {
				return -1, false
			}
			empty, ok := filterAcceptsEmpty(e)
			if !ok || metric && empty {
				return -1, false
			}
			return j, empty
		}
		return -1, false
	}
	return -1, false
}

// filterAcceptsEmpty reports whether the string comparison e accepts the empty value, as Loki's label filter
// compares it; false in ok for a comparison it does not decide.
func filterAcceptsEmpty(e *logql.LabelFilterExpr) (empty, ok bool) {
	switch e.Cmp {
	case "=":
		return e.Value == "", true
	case "!=":
		return e.Value != "", true
	case "=~", "!~":
		c, err := translate.NewLabelFilterCondition(e.Name, e.Cmp, e.Value)
		if err != nil {
			return false, false
		}
		return c.Matches(""), true
	}
	return false, false
}

// dropHoistedFilter is the filter f of the label filter stage j rewritten to read the stored value of the label a
// matcher drop before it removes (dropHoistReader), or f as it is. It is f alone where f is an equality whose value
// none of the drop's matchers on the label removes (each an equality on another non-empty value), so the plan writes
// the text of a query without the drop.
func dropHoistedFilter(sel *queryplan.LogSelect, j int, f logsql.FilterExpr, label func(string) string, metric bool) logsql.FilterExpr {
	lbl, ok := sel.Stages[j].(*queryplan.LabelFilter)
	if !ok || f == nil || lbl.Expr == nil || !lbl.Expr.IsLeaf() {
		return f
	}
	name := lbl.Expr.Name
	for i := j - 1; i >= 0; i-- {
		d, ok := sel.Stages[i].(*queryplan.Drop)
		if !ok || !slices.ContainsFunc(d.Matchers, func(m logql.DropMatcher) bool { return m.Name == name }) {
			continue
		}
		if slices.Contains(d.Labels, name) || !dropBlankCandidate(sel, i, d, name, false) {
			return f
		}
		at, empty := dropHoistReader(sel, i, name, metric)
		if at != j {
			return f
		}
		removed := dropRemoved(d, name, label)
		if removed == nil {
			return f
		}
		if empty {
			return logsql.Or(removed, f)
		}
		if dropSpares(d, lbl.Expr) {
			return f
		}
		return logsql.And(f, logsql.Not(removed))
	}
	return f
}

// dropSpares reports an equality filter on a non-empty value whose value none of the drop's matchers on its label
// removes, each matcher an equality on another non-empty value: every row the filter keeps keeps the label.
func dropSpares(d *queryplan.Drop, e *logql.LabelFilterExpr) bool {
	if e.Cmp != "=" || e.Value == "" {
		return false
	}
	for _, m := range d.Matchers {
		if m.Name == e.Name && (m.Op != "=" || m.Value == "" || m.Value == e.Value) {
			return false
		}
	}
	return true
}

// dropRemoved is the filter of the rows whose value of name the drop's matchers on it remove (nil when one of them
// has no filter).
func dropRemoved(st *queryplan.Drop, name string, label func(string) string) logsql.FilterExpr {
	var removed logsql.FilterExpr
	for _, m := range st.Matchers {
		if m.Name != name {
			continue
		}
		f := translate.MatcherFieldFilter(name, m.Op == "!=" || m.Op == "!~", m.Op == "=~" || m.Op == "!~", m.Value, label)
		if f == nil {
			return nil
		}
		if removed == nil {
			removed = f
		} else {
			removed = logsql.Or(removed, f)
		}
	}
	return removed
}

// rewritesLevel reports a stage after i that may write the level field name again, or a filter anywhere on
// detected_level, which reads the level fields.
func rewritesLevel(sel *queryplan.LogSelect, i int, name string) bool {
	if stageReads(sel.Stages, translate.DetectedLevelLabel) {
		return true
	}
	return slices.ContainsFunc(sel.Stages[i+1:], func(st queryplan.Stage) bool {
		switch st := st.(type) {
		case *queryplan.Parse:
			return true
		case *queryplan.LabelFormat:
			for _, f := range st.Formats {
				if f.Name == name {
					return true
				}
			}
		}
		return false
	})
}

// dropBlankPipes are the pipes behind the drop at stage i that blank the values it removes from the fields later
// stages read, and the pipes that put the level fields back after the last stage. A metric query groups by the stored
// fields and needs none put back.
func dropBlankPipes(sel *queryplan.LogSelect, i int, st *queryplan.Drop, label func(string) string, metric bool) (blank, restore []logsql.Pipe) {
	for _, b := range dropBlankNames(sel, i, st, metric) {
		var removed logsql.FilterExpr
		if !slices.Contains(st.Labels, b.name) {
			if removed = dropRemoved(st, b.name, label); removed == nil {
				continue
			}
		}
		fields := []string{b.name}
		if label != nil {
			if mapped := label(b.name); mapped != "" && mapped != b.name {
				fields = append(fields, mapped)
			}
		}
		for j, field := range fields {
			if b.level && !metric {
				scratch := dropScratch + strconv.Itoa(i) + "_" + strconv.Itoa(j) + "_" + b.name
				blank = append(blank, logsql.PipeFormat{If: removed, Template: "<" + field + ">", ResultField: scratch})
				restore = append(restore,
					logsql.PipeFormat{If: logsql.FieldFilter{Field: scratch, Op: logsql.FieldOpAny}, Template: "<" + scratch + ">", ResultField: field},
					logsql.PipeDelete{Labels: []string{scratch}})
			}
			// The condition reads the fields as they are before any of them is blanked.
			blank = append(blank, logsql.PipeFormat{If: removed, ResultField: field})
		}
	}
	return blank, restore
}

// WritesDropBlank reports a plan whose drop blanks a field a later stage reads, which the string translator reads
// with the dropped value.
func WritesDropBlank(p *queryplan.Plan) bool { return p != nil && writesDropBlank(p.Root) }

func writesDropBlank(n queryplan.Node) bool {
	switch n := n.(type) {
	case *queryplan.LogSelect:
		return selectWritesDropBlank(n, false)
	case *queryplan.RangeAgg:
		return n.In != nil && selectWritesDropBlank(n.In, true)
	case *queryplan.VectorAgg:
		return writesDropBlank(n.In)
	case *queryplan.Rank:
		return writesDropBlank(n.In)
	case *queryplan.LabelReplace:
		return writesDropBlank(n.In)
	case *queryplan.BinOp:
		return writesDropBlank(n.L) || writesDropBlank(n.R)
	}
	return false
}

func selectWritesDropBlank(sel *queryplan.LogSelect, metric bool) bool {
	for i, st := range sel.Stages {
		d, ok := st.(*queryplan.Drop)
		if !ok {
			continue
		}
		if len(dropBlankNames(sel, i, d, metric)) > 0 {
			return true
		}
		// A filter the plan rewrites to read the stored value is another text than the translator's, unless the
		// drop spares every row it keeps.
		for _, m := range d.Matchers {
			if slices.Contains(d.Labels, m.Name) || !dropBlankCandidate(sel, i, d, m.Name, false) {
				continue
			}
			if j, empty := dropHoistReader(sel, i, m.Name, metric); j >= 0 &&
				(empty || !dropSpares(d, sel.Stages[j].(*queryplan.LabelFilter).Expr)) {
				return true
			}
		}
	}
	return false
}
