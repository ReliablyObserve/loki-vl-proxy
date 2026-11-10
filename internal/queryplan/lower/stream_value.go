package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The stream labels a label filter reads after a json or logfmt parser (translate.StreamValueScratch). The
// string translator finds them by scanning the text of the pipeline for `name <operator>` and for `{{ .name }}`
// in any string, so the lowering writes them for a stage that is one filter on a stream label (where the text
// scan is the typed name), and leaves every other reading of a stream label, and any `{{` template text, to the
// translator.

// streamValues are the stream label names the label filters of the query read, in the order the string
// translator lists them (the profile's), and the construct the lowering does not write, "" when it writes all.
// A query that reads no stream label after a parser writes no scratch field.
func streamValues(sel *queryplan.LogSelect, names []string) (read []string, construct string) {
	if len(names) == 0 {
		return nil, ""
	}
	if templateReads(sel, names) {
		return nil, "LogSelect stream label after parser"
	}
	var found []string
	for _, st := range sel.Stages {
		lf, ok := st.(*queryplan.LabelFilter)
		if !ok {
			continue
		}
		if lf.Expr == nil {
			return nil, "LogSelect stream label after parser"
		}
		if lf.Expr.IsLeaf() {
			if n, ok := streamLeaf(lf.Expr, names); ok {
				found = append(found, n)
			} else if namesAny(lf.Expr, names) {
				return nil, "LogSelect stream label after parser"
			}
		} else if namesAny(lf.Expr, names) {
			return nil, "LogSelect stream label in label filter tree"
		}
	}
	for _, n := range names {
		for _, f := range found {
			if f == n {
				read = append(read, n)
				break
			}
		}
	}
	return read, ""
}

// streamLeaf is the stream label name a leaf filters on, written as the name itself.
func streamLeaf(e *logql.LabelFilterExpr, names []string) (string, bool) {
	for _, n := range names {
		if e.Name == n && translate.SanitizeFieldIdentifier(e.Name) == n {
			return n, true
		}
	}
	return "", false
}

// namesAny reports whether a filter names a stream label in any spelling the lowering cannot tell from the
// translator's text scan: the name, or a name that sanitizes to it.
func namesAny(e *logql.LabelFilterExpr, names []string) bool {
	if !e.IsLeaf() {
		return namesAny(e.Left, names) || namesAny(e.Right, names)
	}
	s := translate.SanitizeFieldIdentifier(e.Name)
	for _, n := range names {
		if e.Name == n || s == n {
			return true
		}
	}
	return false
}

// templateReads reports whether a string of the query could hold a template read of a stream label (`{{ .name }}`),
// which the string translator finds anywhere in the pipeline text, in a line filter or a value too.
func templateReads(sel *queryplan.LogSelect, names []string) bool {
	holds := func(s string) bool {
		if !strings.Contains(s, "{{") {
			return false
		}
		for _, n := range names {
			if strings.Contains(s, "."+n) {
				return true
			}
		}
		return false
	}
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.LineFilter:
			if holds(st.Value) {
				return true
			}
			for _, alt := range st.Or {
				if holds(alt.Value) {
					return true
				}
			}
		case *queryplan.LabelFilter:
			if st.Expr == nil || holdsInFilter(st.Expr, holds) {
				return true
			}
		case *queryplan.Parse:
			if holds(st.Param) {
				return true
			}
		case *queryplan.Drop:
			for _, m := range st.Matchers {
				if holds(m.Value) {
					return true
				}
			}
		case *queryplan.Keep:
			for _, m := range st.Matchers {
				if holds(m.Value) {
					return true
				}
			}
		}
	}
	return false
}

func holdsInFilter(e *logql.LabelFilterExpr, holds func(string) bool) bool {
	if !e.IsLeaf() {
		return holdsInFilter(e.Left, holds) || holdsInFilter(e.Right, holds)
	}
	return holds(e.Leaf) || holds(e.Value)
}

// streamValuePipes are the pipes a parser stage gets when the query reads stream labels: the copies of their
// stored values ahead of the first parser, the fills behind each.
func streamValuePipes(ps []logsql.Pipe, read []string, first bool) []logsql.Pipe {
	if len(read) == 0 {
		return ps
	}
	var out []logsql.Pipe
	if first {
		out = append(out, translate.StreamValueCopies(read)...)
	}
	out = append(out, ps...)
	return append(out, translate.StreamValueFills(read)...)
}

// extractedUnpacks puts, behind the pipe of each json and logfmt stage, the pipe that unpacks the keys of the
// name_extracted labels the query's filters read into scratch fields. The string translator writes them for a
// parser that extracts every key; a parser with flags or an extraction list it reads as naming the labels it
// extracts, flags included, and the query stays with it.
func extractedUnpacks(sel *queryplan.LogSelect, staged [][]logsql.Pipe, bases []string) ([][]logsql.Pipe, error) {
	n := 0
	for i, st := range sel.Stages {
		p, ok := st.(*queryplan.Parse)
		if !ok {
			continue
		}
		if p.Strict || p.KeepEmpty || len(p.Fields) > 0 {
			return nil, &NotLoweredError{Construct: "LogSelect label filter _extracted with parser arguments"}
		}
		at := -1
		for k, pipe := range staged[i] {
			switch pipe.(type) {
			case logsql.PipeUnpackJSON, logsql.PipeUnpackLogfmt:
				if at < 0 {
					at = k
				}
			}
		}
		if at < 0 {
			return nil, &NotLoweredError{Construct: "LogSelect unpack parser"}
		}
		ps := make([]logsql.Pipe, 0, len(staged[i])+1)
		ps = append(ps, staged[i][:at+1]...)
		ps = append(ps, translate.ExtractedUnpack(p.Type == logql.ParserLogfmt, n, bases))
		staged[i] = append(ps, staged[i][at+1:]...)
		n++
	}
	return staged, nil
}
