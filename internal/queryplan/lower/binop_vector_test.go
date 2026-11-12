package lower

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

func vec(v float64, text string) physical.VectorOp { return physical.VectorOp{V: v, Text: text} }

// A vector(n) operand lowers to the physical.VectorOp the proxy's binary route answers it from: one series without
// labels, n at every step, whatever the operator and the vector matching around it. It is a vector, not a number: it
// takes part in the matching, and a number operand next to it stays a ScalarOp.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersVectorLiteralOperands(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	sum := `sum by (app) (count_over_time({app="b"}[5m]))`
	lowerQuery := func(q string) physical.Op {
		t.Helper()
		op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return op
	}
	one, two := vec(1, "vector(1)"), vec(2, "vector(2)")
	on := &physical.Matching{MatchOn: true}
	for _, c := range []struct {
		q    string
		want physical.Op
	}{
		// either side, every operator class
		{rate + ` + vector(1)`, physical.Binary{Op: "+", L: lowerQuery(rate), R: one}},
		{`vector(1) + ` + rate, physical.Binary{Op: "+", L: one, R: lowerQuery(rate)}},
		{rate + ` / vector(2)`, physical.Binary{Op: "/", L: lowerQuery(rate), R: two}},
		{`vector(2) % ` + sum, physical.Binary{Op: "%", L: two, R: lowerQuery(sum)}},
		{rate + ` ^ vector(2)`, physical.Binary{Op: "^", L: lowerQuery(rate), R: two}},
		{rate + ` > vector(1)`, physical.Binary{Op: ">", L: lowerQuery(rate), R: one}},
		{`vector(1) <= ` + sum, physical.Binary{Op: "<=", L: one, R: lowerQuery(sum)}},
		{rate + ` == vector(2)`, physical.Binary{Op: "==", L: lowerQuery(rate), R: two}},
		{rate + ` != vector(2)`, physical.Binary{Op: "!=", L: lowerQuery(rate), R: two}},
		// integer, decimal, negative, exponent and zero values; the text is the number as Go prints it
		{rate + ` * vector(0)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: vec(0, "vector(0)")}},
		{rate + ` * vector(0.5)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: vec(0.5, "vector(0.5)")}},
		{rate + ` * vector(100.25)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: vec(100.25, "vector(100.25)")}},
		{rate + ` * vector(1e3)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: vec(1000, "vector(1000)")}},
		{rate + ` * vector( 3 )`, physical.Binary{Op: "*", L: lowerQuery(rate), R: vec(3, "vector(3)")}},
		{rate + ` * vector(0x1p4)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: vec(16, "vector(16)")}},
		// blanks the route does not read, around the operator or inside the call, change nothing
		{rate + `  * vector(1 )`, physical.Binary{Op: "*", L: lowerQuery(rate), R: one}},
		// vector matching around it
		{`sum by()(count_over_time({app="a"}[5m])) + on() vector(0)`, physical.Binary{Op: "+", L: lowerQuery(`sum by()(count_over_time({app="a"}[5m]))`), R: vec(0, "vector(0)"),
			Tail: "@@@on:", Match: on}},
		{rate + ` * on(app) vector(2)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: two, Tail: "@@@on:app",
			Match: &physical.Matching{MatchOn: true, On: []string{"app"}}}},
		{`vector(2) - ignoring(pod) ` + sum, physical.Binary{Op: "-", L: two, R: lowerQuery(sum), Tail: "@@@ignoring:pod",
			Match: &physical.Matching{Ignoring: []string{"pod"}}}},
		{rate + ` + ignoring() vector(1)`, physical.Binary{Op: "+", L: lowerQuery(rate), R: one, Tail: "@@@ignoring:", Match: &physical.Matching{}}},
		{rate + ` * on(app) group_left(zone) vector(2)`, physical.Binary{Op: "*", L: lowerQuery(rate), R: two, Tail: "@@@on:app@@@group_left:zone",
			Match: &physical.Matching{MatchOn: true, On: []string{"app"}, GroupSide: "group_left", GroupLeft: []string{"zone"}}}},
		{`vector(2) / ignoring(pod) group_right() ` + sum, physical.Binary{Op: "/", L: two, R: lowerQuery(sum), Tail: "@@@ignoring:pod@@@group_right:",
			Match: &physical.Matching{Ignoring: []string{"pod"}, GroupSide: "group_right"}}},
		// an operand folded by its without clause next to it is lowered alone, as without a vector
		{`sum without (pod) (` + rate + `) + vector(1)`, physical.Binary{Op: "+", L: lowerQuery(`sum without (pod) (` + rate + `)`), R: one}},
	} {
		if got := lowerQuery(c.q); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %#v\nwant %#v", c.q, got, c.want)
		}
		if !RoutedBinary(rangeAggPlan(c.q).Root) {
			t.Errorf("%s: a vector(n) operand is held to the proxy's binary route", c.q)
		}
	}
	// A vector(n) is never a number: the same value as a ScalarOp is another operand.
	if physical.Equal(physical.Binary{Op: "+", L: one, R: one}, physical.Binary{Op: "+", L: physical.ScalarOp{V: 1}, R: one}) {
		t.Error("vector(1) equals the number 1")
	}
}

// What stays with the string translator, by name: the set operators and bool (the translator drops the modifier and
// writes the left operand alone), an operand that is a binary operation, a without clause with no label, a vector(n)
// spelled so the proxy's route does not read it as one, and a vector(n) under the kept line option, which holds log
// queries only.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesVectorLiteralOperands(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		rate + ` and vector(1)`:                     "BinOp set operator",
		`vector(1) or ` + rate:                      "BinOp set operator",
		`vector(1) unless vector(2)`:                "BinOp set operator",
		rate + ` > bool vector(1)`:                  "BinOp bool",
		`vector(1) == bool vector(2)`:               "BinOp bool",
		rate + ` + (vector(1) * 2)`:                 "BinOp operand BinOp",
		`vector(1) + vector(2) * vector(3)`:         "BinOp operand BinOp",
		`sum without () (` + rate + `) + vector(1)`: "BinOp operand without empty",
		`vector(1) + sum without () (` + rate + `)`: "BinOp operand without empty",
		rate + ` + vector (1)`:                      "BinOp operand spelling",
		`vector (1) + ` + rate:                      "BinOp operand spelling",
		`vector(1) + vector(2)`:                     "BinOp constant operands",
		`vector(1) > on() vector(2)`:                "BinOp constant operands",
		`2 * vector(1)`:                             "BinOp constant operands",
		`vector(1) - 0.5`:                           "BinOp constant operands",
		`1 + 2`:                                     "BinOp scalar operands",
		rate + ` + vector(-1)`:                      "BinOp operand Opaque",
		rate + ` + vector(a)`:                       "BinOp operand Opaque",
		rate + `+ vector(1)`:                        "BinOp operator spelling",
		"vector(1)\n+ " + rate:                      "BinOp operator spelling",
		`label_replace(` + rate + ` + vector(1), "a", "b", "c", "d")`: "LabelReplace operand BinOp",
		`label_replace(vector(1) + vector(2), "a", "b", "c", "d")`:    "LabelReplace operand BinOp",
		`topk(2, ` + rate + `) + vector(1)`:                           "BinOp operand Rank",
		`vector(1) + {app="a"}`:                                       "BinOp operand LogSelect",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}

// The text the proxy's binary route reads as a vector(n): the call with its parenthesis right after the name, one
// number inside, blanks around the number and after the call. Anything else is a query to it.
//
// conformance: architecture-syntax-package-layering
func TestVectorLiteralIsWhatTheRouteReads(t *testing.T) {
	for text, want := range map[string]float64{
		`vector(1)`:      1,
		`vector(0)`:      0,
		`vector(-1)`:     -1,
		`vector(0.25)`:   0.25,
		`vector(.5)`:     0.5,
		`vector(1e3)`:    1000,
		`vector(2E-2)`:   0.02,
		` vector( 3 ) `:  3,
		"\tvector(\n4)":  4,
		`vector(+7)`:     7,
		`vector(0x1p4)`:  16,
		`vector(Inf)`:    math.Inf(1),
		`vector(1_0.5)`:  10.5,
		`vector(1e400)`:  0,
		`vector(a b)`:    0,
		`vector (1)`:     0,
		`vector(1) + 2`:  0,
		`xvector(1)`:     0,
		`vector()`:       0,
		`vector(1)(2)`:   0,
		`vector((1))`:    0,
		`Vector(1)`:      0,
		`vector(1)    x`: 0,
	} {
		v, ok := VectorLiteral(text)
		wantOK := want != 0 || text == `vector(0)`
		switch text {
		case `vector(1e400)`, `vector(a b)`:
			wantOK = false
		}
		if ok != wantOK || (ok && v != want) {
			t.Errorf("VectorLiteral(%q) = %v, %v; want %v, %v", text, v, ok, want, wantOK)
		}
	}
	// The number is read as strconv reads it, so the text of vectorText reads back as the same value.
	for _, v := range []float64{0, 1, -1, 0.5, 1e3, 123456789.125, 1e-7, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		got, ok := VectorLiteral(vectorText(v))
		if !ok || got != v {
			t.Errorf("VectorLiteral(%q) = %v, %v; want %v", vectorText(v), got, ok, v)
		}
		if vectorText(v) != "vector("+strconv.FormatFloat(v, 'f', -1, 64)+")" {
			t.Errorf("vectorText(%v) = %q", v, vectorText(v))
		}
	}
}
