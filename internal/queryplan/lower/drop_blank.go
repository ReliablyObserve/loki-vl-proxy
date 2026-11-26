package lower

import (
	"slices"
	"strconv"

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
// later parser or label_format may write it again, or the query derives detected_level for a filter.
func dropBlankNames(sel *queryplan.LogSelect, i int, st *queryplan.Drop) []dropBlank {
	var out []dropBlank
	add := func(name string, bare bool) {
		level := translate.IsLevelSource(name)
		if derivedName(name) || bare && !level || len(name) > 1 && name[:2] == "__" ||
			slices.ContainsFunc(out, func(b dropBlank) bool { return b.name == name }) ||
			!stageReads(sel.Stages[i+1:], name) || level && rewritesLevel(sel, i, name) {
			return
		}
		out = append(out, dropBlank{name: name, level: level})
	}
	for _, name := range st.Labels {
		add(name, true)
	}
	for _, m := range st.Matchers {
		add(m.Name, false)
	}
	return out
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
	for _, b := range dropBlankNames(sel, i, st) {
		var removed logsql.FilterExpr
		bare := slices.Contains(st.Labels, b.name)
		if !bare {
			for _, m := range st.Matchers {
				if m.Name != b.name {
					continue
				}
				f := translate.MatcherFieldFilter(b.name, m.Op == "!=" || m.Op == "!~", m.Op == "=~" || m.Op == "!~",
					m.Value, label)
				if f == nil {
					removed = nil
					break
				}
				if removed == nil {
					removed = f
				} else {
					removed = logsql.Or(removed, f)
				}
			}
			if removed == nil {
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
		return selectWritesDropBlank(n)
	case *queryplan.RangeAgg:
		return n.In != nil && selectWritesDropBlank(n.In)
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

func selectWritesDropBlank(sel *queryplan.LogSelect) bool {
	for i, st := range sel.Stages {
		if d, ok := st.(*queryplan.Drop); ok && len(dropBlankNames(sel, i, d)) > 0 {
			return true
		}
	}
	return false
}
