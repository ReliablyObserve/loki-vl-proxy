package translator

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// evalRow reports whether the row (field name to value; a missing field reads as empty) passes the LogsQL query:
// its filter, then each `| filter` pipe. It reads only the filters the label filter translations write (exact,
// regexp, presence, comparison and their and/or/not), and fails the test on any other construct, so a test that
// compares a translation with Loki's recorded answer cannot pass by skipping something it does not understand.
func evalRow(t testing.TB, query string, row map[string]string) bool {
	t.Helper()
	q, err := logsql.Parse(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if !evalFilter(t, q.Filter, row) {
		return false
	}
	for _, p := range q.Pipes {
		pf, ok := p.(logsql.PipeFilter)
		if !ok {
			t.Fatalf("%s: pipe %T is not evaluated", query, p)
		}
		if !evalFilter(t, pf.Expr, row) {
			return false
		}
	}
	return true
}

func evalFilter(t testing.TB, f logsql.FilterExpr, row map[string]string) bool {
	t.Helper()
	switch f := f.(type) {
	case nil, logsql.Wildcard:
		return true
	case logsql.AllOf:
		for _, x := range f {
			if !evalFilter(t, x, row) {
				return false
			}
		}
		return true
	case logsql.AnyOf:
		for _, x := range f {
			if evalFilter(t, x, row) {
				return true
			}
		}
		return false
	case logsql.AndExpr:
		return evalFilter(t, f.Left, row) && evalFilter(t, f.Right, row)
	case logsql.OrExpr:
		return evalFilter(t, f.Left, row) || evalFilter(t, f.Right, row)
	case logsql.Group:
		return evalFilter(t, f.Expr, row)
	case logsql.Exclude:
		return !evalFilter(t, f.Expr, row)
	case logsql.NotExpr:
		return !evalFilter(t, f.Expr, row)
	case logsql.Regexp:
		return regexp.MustCompile(f.Pattern).MatchString(row["_msg"])
	case logsql.FieldFilter:
		v := row[f.Field]
		var ok bool
		switch f.Op {
		case logsql.FieldOpExact:
			ok = v == f.Value
		case logsql.FieldOpRegexp:
			ok = regexp.MustCompile(f.Value).MatchString(v)
		case logsql.FieldOpAny:
			ok = v != ""
		case logsql.FieldOpEmpty:
			ok = v == ""
		case logsql.FieldOpNotExact:
			ok = v != "" && v != f.Value
		case logsql.FieldOpGT, logsql.FieldOpGTE, logsql.FieldOpLT, logsql.FieldOpLTE:
			x, errx := strconv.ParseFloat(v, 64)
			y, erry := strconv.ParseFloat(f.Value, 64)
			if errx != nil || erry != nil {
				return false
			}
			ok = map[logsql.FieldOp]bool{logsql.FieldOpGT: x > y, logsql.FieldOpGTE: x >= y, logsql.FieldOpLT: x < y, logsql.FieldOpLTE: x <= y}[f.Op]
		default:
			t.Fatalf("field filter operator %d is not evaluated", f.Op)
		}
		return ok != f.Negate
	}
	t.Fatalf("filter %T (%s) is not evaluated", f, fmt.Sprint(f))
	return false
}
