package lower

import (
	"slices"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// An alias's stream label (-field-mapping-label-mode alias and both, Profile AliasField) is stored in the alias's
// field, or, on a stream that was never renamed, in a stream field named like the alias itself: the value Loki's
// label of that name holds there, which a json or logfmt parser overwrites in VictoriaLogs with the parsed key
// (Loki names that key name_extracted). A query that reads such an alias after a parser keeps the stored value aside
// the way it keeps a stream label's (translate.StreamValueScratch): copied before the first parser, filled from the
// parsed key where the entry stores none (a log query) or put back behind each parser (a metric query). The alias
// filters and groupings then read the alias's field where the stream holds it, else that value.

// withAliasReads sets the aliases the plan's groupings name, for a profile with alias functions.
func (prof Profile) withAliasReads(root queryplan.Node) Profile {
	if prof.AliasField == nil || prof.aliasReads != nil {
		return prof
	}
	var out []string
	add := func(g *queryplan.Grouping) {
		if g == nil || g.Without {
			return
		}
		for _, l := range g.Labels {
			if prof.AliasField(l) != "" && !slices.Contains(out, l) {
				out = append(out, l)
			}
		}
	}
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch n := n.(type) {
		case *queryplan.VectorAgg:
			add(n.By)
			walk(n.In)
		case *queryplan.Rank:
			add(n.By)
			walk(n.In)
		case *queryplan.RangeAgg:
			add(n.By)
		case *queryplan.BinOp:
			walk(n.L)
			walk(n.R)
		case *queryplan.LabelReplace:
			walk(n.In)
		}
	}
	walk(root)
	if out == nil {
		out = []string{} // walked: the plan groups by no alias
	}
	prof.aliasReads = out
	return prof
}

// aliasStreamReads adds to read the aliases a query with a parser reads after it: those its label filters after the first
// parser name, and for a metric query those its groupings name.
func aliasStreamReads(sel *queryplan.LogSelect, prof Profile, read []string) []string {
	if prof.AliasField == nil || !hasParser(sel) {
		return read
	}
	add := func(name string) {
		if prof.AliasField(name) != "" && !slices.Contains(read, name) {
			read = append(read, name)
		}
	}
	if prof.metric {
		for _, a := range prof.aliasReads {
			add(a)
		}
	}
	var leaves func(e *logql.LabelFilterExpr)
	leaves = func(e *logql.LabelFilterExpr) {
		if e == nil {
			return
		}
		if !e.IsLeaf() {
			leaves(e.Left)
			leaves(e.Right)
			return
		}
		if translate.SanitizeFieldIdentifier(e.Name) == e.Name {
			add(e.Name)
		}
	}
	parsed := false
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Parse:
			parsed = true
		case *queryplan.LabelFilter:
			if parsed {
				leaves(st.Expr)
			}
		}
	}
	return read
}
