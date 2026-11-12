package physical

import (
	"reflect"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The structure of an operator tree, which two trees are compared by: the operators, with each VictoriaLogs query
// parsed. Only the operators the string translator writes get a form; the text of a binary (Text, Tail) is not
// part of it, its parts are.
type (
	canonFold struct {
		Levels []FoldLevel
		In     any
	}
	canonLabel struct {
		Replace []LabelReplace
		In      any
	}
	canonBin struct {
		Op    string
		Bool  bool
		Match *Matching
		L, R  any
	}
	canonScalar struct{ V float64 }
	// canonVector is a vector(n) operand: its own form, so a vector(2) never equals the number 2 (a vector takes part in
	// vector matching, a number does not).
	canonVector struct{ V float64 }
	// canonLevel is a RankStep (K set) or a SeriesAgg; By is nil for no clause and for `by ()`.
	canonLevel struct {
		Rank bool
		Op   string
		K    int
		By   *canonGrouping
		In   any
	}
	canonGrouping struct {
		Labels  []string
		Without bool
	}
)

// Canon is the structure of op: the parsed LogsQL of each VictoriaLogs query with its default spellings folded
// (`from _msg` reads like no from), and the operator tree around it. Two trees that answer alike have equal Canon
// forms. unparsed is set when a query does not parse; its form is then its text with the whitespace folded.
func Canon(op Op, unparsed *bool) any {
	switch v := op.(type) {
	case Verbatim:
		return Canon(v.In, unparsed)
	case Fold:
		return canonFold{canonLevels(v.Levels), Canon(v.In, unparsed)}
	case LabelMap:
		return canonLabel{v.Replace, Canon(v.In, unparsed)}
	case Binary:
		return canonBin{v.Op, v.Bool, v.Match, Canon(v.L, unparsed), Canon(v.R, unparsed)}
	case ScalarOp:
		return canonScalar{v.V}
	case VectorOp:
		return canonVector{v.V}
	case RankStep:
		return canonLevel{true, v.Op, v.K, canonClause(v.By), Canon(v.In, unparsed)}
	case SeriesAgg:
		return canonLevel{false, v.Op, 0, canonClause(v.By), Canon(v.In, unparsed)}
	case VLQuery:
		q, err := logsql.Parse(v.Text)
		if err != nil {
			*unparsed = true
			return strings.Join(strings.Fields(v.Text), " ")
		}
		foldDefaultFields(reflect.ValueOf(q))
		return q
	}
	return op
}

// canonLevels are the levels of a fold with an empty label list read as none: the `without ()` level keeps every
// series, and an empty list and no list are the same clause. The levels are copied only when one has an empty list.
func canonLevels(levels []FoldLevel) []FoldLevel {
	for i, lv := range levels {
		if lv.Labels != nil && len(lv.Labels) == 0 {
			out := append([]FoldLevel(nil), levels...)
			for j := i; j < len(out); j++ {
				if len(out[j].Labels) == 0 {
					out[j].Labels = nil
				}
			}
			return out
		}
	}
	return levels
}

// canonClause is the clause of a level the proxy evaluates: `by ()` is no clause (it groups every series together,
// as no clause does), a list of labels is kept with its order, and `without ()` stays, since the proxy groups it by
// every label.
func canonClause(g *Grouping) *canonGrouping {
	if g == nil || !g.Without && len(g.Labels) == 0 {
		return nil
	}
	labels := g.Labels
	if len(labels) == 0 {
		labels = nil
	}
	return &canonGrouping{labels, g.Without}
}

// Equal reports whether a and b have the same structure (Canon).
func Equal(a, b Op) bool {
	var ua, ub bool
	return reflect.DeepEqual(Canon(a, &ua), Canon(b, &ub))
}

// foldDefaultFields sets the fields that default to _msg to their empty spelling, so `from _msg` reads like no from.
func foldDefaultFields(v reflect.Value) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			foldDefaultFields(v.Elem())
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			foldDefaultFields(v.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			switch name := v.Type().Field(i).Name; {
			case f.Kind() == reflect.String && f.CanSet() && f.String() == "_msg" && (name == "From" || name == "Field" || name == "ResultField"):
				f.SetString("")
			default:
				foldDefaultFields(f)
			}
		}
	}
}
