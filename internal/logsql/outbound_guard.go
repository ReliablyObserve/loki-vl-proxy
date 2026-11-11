package logsql

import (
	"reflect"
	"strconv"
)

// GuardReservedFieldCase returns text, a query about to be sent to VictoriaLogs, with every filter on a field named
// _time, _stream or _stream_id in another letter case (ReservedFieldNameOtherCase: _TIME, _Stream, _STREAM_ID, bare
// or quoted) rewritten to the form VictoriaLogs reads back from its own print: the field is copied into a scratch
// field by a copy pipe right ahead of the filter (`| copy _TIME as __lxrf_TIME`), the filter reads the scratch field,
// and a delete pipe drops it right after (not after a stats pipe, whose output does not hold it). A filter of the
// query's first part moves into a filter pipe in front of the other pipes, its other conjuncts staying where they
// are; a filter pipe, and the if condition of any other pipe, keeps its place between the copy and the delete.
//
// VictoriaLogs reads a bare `_TIME:` as its own time filter, and prints a quoted `"_TIME"` bare when it clones the
// query (lib/logstorage Query.Clone, print then parse again), which every query with a limit, the live tail and a
// cluster's remote query go through: the clone's parse fails and VictoriaLogs panics (VictoriaMetrics/VictoriaLogs#1942).
// The copy pipe and the scratch name survive the clone as written (the roundtrip gate of internal/queryplan/lower).
//
// A text that holds no such name anywhere is returned as it is, unread, with no allocation; a text the proxy's
// LogsQL parser does not read, and one whose only such names are not filtered (a pipe that names the field, a
// stream filter, a quoted value), are returned as they are too.
func GuardReservedFieldCase(text string) string {
	if !mayNameReservedOtherCase(text) {
		return text
	}
	q, err := Parse(text)
	if err != nil {
		return text
	}
	guarded, changed := guardQuery(*q)
	if !changed {
		return text
	}
	return guarded.String()
}

// guardQuery is the guarded query and whether a filter was rewritten.
func guardQuery(q Query) (Query, bool) {
	var pipes []Pipe
	changed := false
	if q.Filter != nil {
		keep, moved, names := splitReservedCaseFilter(q.Filter)
		if moved != nil {
			changed = true
			q.Filter = keep
			pipes = appendGuardedStep(pipes, PipeFilter{Expr: moved}, names)
		}
	}
	for _, p := range q.Pipes {
		var names []string
		rewritten, _ := rewriteFilters(reflect.ValueOf(&p).Elem(), false, &names)
		if len(names) == 0 {
			pipes = append(pipes, p)
			continue
		}
		changed = true
		pipes = appendGuardedStep(pipes, rewritten.Interface().(Pipe), names)
	}
	if !changed {
		return q, false
	}
	q.Pipes = pipes
	return q, true
}

// appendGuardedStep appends the copies of names into their scratch fields, the step that reads them, and their
// delete (unless the step is a stats pipe: its output holds the groups and the results only, and a query sent to
// stats_query_range must end with it).
func appendGuardedStep(pipes []Pipe, step Pipe, names []string) []Pipe {
	scratch := make([]string, len(names))
	pairs := make([][2]string, len(names))
	for i, n := range names {
		scratch[i] = ReservedFieldCaseScratch(n)
		pairs[i] = [2]string{n, scratch[i]}
	}
	pipes = append(pipes, PipeCopy{Pairs: pairs}, step)
	if _, stats := step.(PipeStats); !stats {
		pipes = append(pipes, PipeDelete{Labels: scratch})
	}
	return pipes
}

// splitReservedCaseFilter splits the filter of a query's first part into the conjuncts that stay (keep, nil for
// none) and the rewritten conjuncts that move into a filter pipe (moved, nil when no conjunct names such a field),
// with the names the moved ones read.
func splitReservedCaseFilter(f FilterExpr) (keep, moved FilterExpr, names []string) {
	all, isAll := f.(AllOf)
	if !isAll {
		rewritten, _ := rewriteFilters(reflect.ValueOf(&f).Elem(), true, &names)
		if len(names) == 0 {
			return f, nil, nil
		}
		return nil, rewritten.Interface().(FilterExpr), names
	}
	var stay, move AllOf
	for _, c := range all {
		rewritten, changed := rewriteFilters(reflect.ValueOf(&c).Elem(), true, &names)
		if !changed {
			stay = append(stay, c)
			continue
		}
		move = append(move, rewritten.Interface().(FilterExpr))
	}
	if len(move) == 0 {
		return f, nil, nil
	}
	switch len(stay) {
	case 0:
		keep = nil
	case 1:
		keep = stay[0]
	default:
		keep = stay
	}
	if len(move) == 1 {
		return keep, move[0], names
	}
	return keep, move, names
}

var filterExprType = reflect.TypeOf((*FilterExpr)(nil)).Elem()

// rewriteFilters returns v with the Field of every filter node under it (inFilter: v is inside a filter) that names a
// field in another case replaced by its scratch field, and whether it replaced one; it appends each name it replaced
// to names once. A value with nothing to replace is returned as it is.
func rewriteFilters(v reflect.Value, inFilter bool, names *[]string) (reflect.Value, bool) {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v, false
		}
		elem := v.Elem()
		r, changed := rewriteFilters(elem, inFilter || elem.Type().Implements(filterExprType), names)
		if !changed {
			return v, false
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(r)
		return out, true
	case reflect.Struct:
		var out reflect.Value
		for i := 0; i < v.NumField(); i++ {
			sf := v.Type().Field(i)
			if !sf.IsExported() {
				continue
			}
			fv := v.Field(i)
			var r reflect.Value
			if inFilter && sf.Name == "Field" && fv.Kind() == reflect.String {
				name := unquoteName(fv.String())
				if !ReservedFieldNameOtherCase(name) {
					continue
				}
				addName(names, name)
				r = reflect.ValueOf(ReservedFieldCaseScratch(name))
			} else {
				var changed bool
				if r, changed = rewriteFilters(fv, inFilter || sf.Type == filterExprType, names); !changed {
					continue
				}
			}
			if !out.IsValid() {
				out = reflect.New(v.Type()).Elem()
				out.Set(v)
			}
			out.Field(i).Set(r)
		}
		if out.IsValid() {
			return out, true
		}
		return v, false
	case reflect.Slice:
		var out reflect.Value
		for i := 0; i < v.Len(); i++ {
			r, changed := rewriteFilters(v.Index(i), inFilter, names)
			if !changed {
				continue
			}
			if !out.IsValid() {
				out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
				reflect.Copy(out, v)
			}
			out.Index(i).Set(r)
		}
		if out.IsValid() {
			return out, true
		}
		return v, false
	case reflect.Pointer:
		if v.IsNil() {
			return v, false
		}
		r, changed := rewriteFilters(v.Elem(), inFilter, names)
		if !changed {
			return v, false
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(r)
		return out, true
	}
	return v, false
}

// addName appends name to names unless it is there.
func addName(names *[]string, name string) {
	for _, n := range *names {
		if n == name {
			return
		}
	}
	*names = append(*names, name)
}

// unquoteName is a field name as written without its quotes.
func unquoteName(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '`') && s[len(s)-1] == s[0] {
		if u, err := strconv.Unquote(s); err == nil {
			return u
		}
	}
	return s
}
