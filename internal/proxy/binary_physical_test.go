package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// constantAnswer is what evaluateBinaryLogQLOperand answers for an operand as a single-series constant: the value and
// true when the answer is a success with one series without labels, the same value at every sample, and the fake
// VictoriaLogs was never asked.
func constantAnswer(t *testing.T, operand logqlpkg.Expr, resultType string) (value float64, constant bool) {
	t.Helper()
	var hits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	params := url.Values{"query": {operand.String()}}
	path := "/loki/api/v1/query"
	if resultType == "matrix" {
		path = "/loki/api/v1/query_range"
		params.Set("start", "1000")
		params.Set("end", "1180")
		params.Set("step", "60")
	} else {
		params.Set("time", "1000")
	}
	response := p.evaluateBinaryLogQLOperand(httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil), operand, resultType)
	if response.status != http.StatusOK || hits.Load() != 0 {
		return 0, false
	}
	var body struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.body.Bytes(), &body); err != nil || body.Data.ResultType != resultType || len(body.Data.Result) != 1 || len(body.Data.Result[0].Metric) != 0 {
		return 0, false
	}
	samples := body.Data.Result[0].Values
	if resultType == "vector" {
		samples = [][]any{body.Data.Result[0].Value}
	}
	if len(samples) == 0 || (resultType == "matrix" && len(samples) != 4) {
		return 0, false
	}
	for i, sample := range samples {
		text, _ := sample[1].(string)
		v, err := strconv.ParseFloat(text, 64)
		if err != nil || (i > 0 && v != value) {
			return 0, false
		}
		value = v
	}
	return value, true
}

// BinaryOperandsPhysical describes a vector(n) operand as evaluateBinaryLogQLOperand treats it: a VectorOp of the
// constant the handler answers with (one series without labels, the number at every step), with no translation and no
// request to VictoriaLogs; and every operand the handler does not answer from a constant is translated as a query,
// whatever it looks like. Instant and range answers both. An operand that is itself a binary operation (`vector(2) * 3`)
// is a query to the branch under test; the child handler folds its constants, which the plan leaves delegated
// (BinOp operand BinOp), so it is not in the table.
//
// conformance: architecture-syntax-package-layering
func TestBinaryOperandsPhysicalReadsVectorLiteralLikeTheHandler(t *testing.T) {
	const left = `rate({app="a"}[5m])`
	texts := []string{
		`vector(1)`, `vector(0)`, `vector(2)`, `vector(0.5)`, `vector(100.25)`, `vector(1e3)`, `vector(.5)`, `vector( 3 )`, `vector(1_0)`,
		`vector (1)`, `vector(  7  )`, `2`, `rate({app="b"}[5m])`, `sum by (app) (rate({app="b"}[5m]))`, `vector`,
	}
	for _, text := range texts {
		query := left + ` + ` + text
		parsed, err := logqlpkg.Parse(query)
		if err != nil {
			continue
		}
		bin, ok := parsed.(*logqlpkg.BinOpExpr)
		if !ok {
			continue
		}
		var translated []string
		translate := func(q string) (string, error) {
			translated = append(translated, q)
			return `app:="a" | stats count()`, nil
		}
		op, operands, routed, err := BinaryOperandsPhysical(query, translate)
		if err != nil || !routed {
			t.Fatalf("%s: routed %v, %v", query, routed, err)
		}
		right := op.(physical.Binary).R
		for _, resultType := range []string{"matrix", "vector"} {
			value, constant := constantAnswer(t, bin.Right, resultType)
			vec, isVector := right.(physical.VectorOp)
			if isVector != constant {
				t.Errorf("%s (%s): the route describes %T, the handler answers a constant: %v", query, resultType, right, constant)
				continue
			}
			if isVector && vec.V != value {
				t.Errorf("%s (%s): the route's value %v, the handler's %v", query, resultType, vec.V, value)
			}
		}
		if _, isVector := right.(physical.VectorOp); isVector {
			if operands[1] != "" || len(translated) != 1 || translated[0] != bin.Left.String() {
				t.Errorf("%s: a vector(n) is translated or sent: operands %q, translated %q", query, operands, translated)
			}
		} else if _, scalar := right.(physical.ScalarOp); !scalar && (len(translated) == 0 || translated[len(translated)-1] != bin.Right.String()) {
			t.Errorf("%s: an operand the handler sends as a query is not translated: %q", query, translated)
		}
	}
}

// The reference for whole binary operations with a vector(n): each side is a VectorOp, a ScalarOp for a number or the
// translation of the other operand; the matching and the operator are the route's. Nothing is translated for a
// vector(n), so two of them are described without one translation.
//
// conformance: architecture-syntax-package-layering
func TestBinaryOperandsPhysicalWithVectorLiterals(t *testing.T) {
	query := `app:="a" | stats count()`
	count := physical.VLQuery{Text: query}
	cases := []struct {
		q          string
		want       physical.Op
		translated int
	}{
		{`vector(1) + vector(2)`, physical.Binary{Op: "+", L: physical.VectorOp{V: 1}, R: physical.VectorOp{V: 2}}, 0},
		{`2 * vector(3)`, physical.Binary{Op: "*", L: physical.ScalarOp{V: 2}, R: physical.VectorOp{V: 3}}, 0},
		{`vector(3) / 0.5`, physical.Binary{Op: "/", L: physical.VectorOp{V: 3}, R: physical.ScalarOp{V: 0.5}}, 0},
		{`rate({app="a"}[5m]) + vector(1)`, physical.Binary{Op: "+", L: count, R: physical.VectorOp{V: 1}}, 1},
		{`vector(1.5) - rate({app="a"}[5m])`, physical.Binary{Op: "-", L: physical.VectorOp{V: 1.5}, R: count}, 1},
		{`sum by () (count_over_time({app="a"}[5m])) + on() vector(0)`, physical.Binary{Op: "+", L: count, R: physical.VectorOp{V: 0},
			Match: &physical.Matching{MatchOn: true}}, 1},
		{`rate({app="a"}[5m]) * ignoring(pod) group_left(zone) vector(2)`, physical.Binary{Op: "*", L: count, R: physical.VectorOp{V: 2},
			Match: &physical.Matching{Ignoring: []string{"pod"}, GroupSide: "group_left", GroupLeft: []string{"zone"}}}, 1},
		{`vector(1) > bool vector(2)`, physical.Binary{Op: ">", Bool: true, L: physical.VectorOp{V: 1}, R: physical.VectorOp{V: 2}}, 0},
		{`rate({app="a"}[5m]) and vector(1)`, physical.Binary{Op: "and", L: count, R: physical.VectorOp{V: 1}}, 1},
		{`vector(1) unless on(app) rate({app="a"}[5m])`, physical.Binary{Op: "unless", L: physical.VectorOp{V: 1}, R: count,
			Match: &physical.Matching{MatchOn: true, On: []string{"app"}}}, 1},
	}
	for _, c := range cases {
		translated := 0
		op, _, routed, err := BinaryOperandsPhysical(c.q, func(string) (string, error) { translated++; return query, nil })
		if err != nil || !routed {
			t.Fatalf("%s: routed %v, %v", c.q, routed, err)
		}
		if !reflect.DeepEqual(op, c.want) {
			t.Errorf("%s:\n got %#v\nwant %#v", c.q, op, c.want)
		}
		if translated != c.translated {
			t.Errorf("%s: %d translations, want %d", c.q, translated, c.translated)
		}
	}
}

// The route's reading of a vector(n) is the one the lowering screens its operand text with (lower.VectorLiteral): on
// any text the two agree on whether it is a vector(n) and on its number.
//
// conformance: architecture-syntax-package-layering
func TestVectorLiteralReadingsAgree(t *testing.T) {
	for _, text := range []string{`vector(1)`, ` vector( 2.5 ) `, `vector (1)`, `vector(a)`, `vector(1e400)`, `vector(0x1p4)`, `vector(1_0)`, `vector(-1)`, `vector()`, `vector(1)x`, "vector(\n1\n)", `vector( 1  2 )`, ``} {
		checkVectorLiteralReadings(t, text)
	}
}

func checkVectorLiteralReadings(t testing.TB, text string) {
	t.Helper()
	routeV, routeOK := binaryVectorLiteral(text)
	lowerV, lowerOK := lower.VectorLiteral(text)
	if routeOK != lowerOK || (routeOK && routeV != lowerV && routeV == routeV) {
		t.Fatalf("%q: the route reads (%v, %v), the lowering's screen (%v, %v)", text, routeV, routeOK, lowerV, lowerOK)
	}
}

// FuzzVectorLiteralReadings holds the lowering's reading of a vector(n) operand text to the route's on any text.
//
// conformance: architecture-syntax-package-layering
func FuzzVectorLiteralReadings(f *testing.F) {
	for _, s := range []string{`vector(1)`, ` vector( 2.5 ) `, `vector (1)`, `vector(a)`, `vector(1e400)`, `vector(0x1p4)`, `vector(1_0)`, `vector(-1)`, `vector()`, "vector(\n1\n)", `vector(NaN)`, `vector(Inf)`, `vector(+1)`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, text string) { checkVectorLiteralReadings(t, text) })
}

// A binary operation with a vector(n) operand is lowered to a VectorOp and held to the proxy's binary route in the
// corpus shadow; the translator's text for the whole query (a phrase search, or the left side alone) is no reference
// for it, so the live shadow counts it as not lowered, never as a mismatch. A spelling the route does not read as a
// vector(n) is left to the translator by name.
//
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_BinaryWithVectorOperandIsNotCompared(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	queries := []string{
		`rate({app="a"}[5m]) + vector(1)`,
		`sum by () (count_over_time({app="a"}[5m])) + on() vector(0)`,
		`vector(2) * on(app) group_left(zone) sum by (app) (rate({app="a"}[5m]))`,
	}
	for _, q := range queries {
		if _, err := translateAndSettle(t, p, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	got := shadowCounts(t, p)
	if shadowTotal(got, "mismatch") != 0 || got["not_lowered BinOp"] != len(queries) {
		t.Fatalf("each binary operation with a vector(n) operand is not compared: %v", got)
	}
	// A spelling the route does not read as a vector(n), and constants only, are left to the translator by name.
	for q, construct := range map[string]string{
		`rate({app="a"}[5m]) + vector (1)`: "BinOp operand spelling",
		`vector(1) + vector(2)`:            "BinOp constant operands",
		`2 * vector(3)`:                    "BinOp constant operands",
	} {
		if _, err := translateAndSettle(t, p, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got = shadowCounts(t, p); got["not_lowered "+construct] == 0 || shadowTotal(got, "mismatch") != 0 {
			t.Fatalf("%s is left to the translator as %s: %v", q, construct, got)
		}
	}
}

// binaryVectorBenchQueries are binary operations with a vector(n) operand next to a range or vector aggregation, either
// side, under arithmetic and comparison operators and every vector matching modifier: the shapes the lowering writes
// and the binary route describes. They are not the without-operand rows of the corpus benchmarks.
func binaryVectorBenchQueries() []string {
	var qs []string
	rate, sum := `rate({app="a"}[5m])`, `sum by (app) (count_over_time({app="b"}[5m]))`
	for _, v := range []string{`vector(0)`, `vector(1.5)`, `vector(1e3)`} {
		for _, op := range []string{"+", "-", "*", "/", ">", "<="} {
			for _, m := range []string{``, ` on()`, ` on(app)`, ` ignoring(pod)`, ` on(app) group_left(zone)`, ` ignoring(pod) group_right()`} {
				qs = append(qs, rate+` `+op+m+` `+v, v+` `+op+m+` `+sum)
			}
		}
	}
	return qs
}

// The proxy's binary route for them (BinaryOperandsPhysical): its parse of the query, the operand that is a query
// translated alone and decoded, and nothing for a vector(n).
func BenchmarkBinaryVectorRoute(b *testing.B) {
	qs := binaryVectorBenchQueries()
	translate := func(q string) (string, error) {
		return translator.TranslateLogQLWithLabels(logqlpkg.CanonicalizeLogRanges(q), nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, routed, err := BinaryOperandsPhysical(qs[i%len(qs)], translate); err != nil || !routed {
			b.Fatal(routed, err)
		}
	}
}

// The typed plan on the same queries: Parse, Build, Resolve, the text screen and Plan.
func BenchmarkBinaryVectorLowered(b *testing.B) {
	qs := binaryVectorBenchQueries()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logqlpkg.Parse(logqlpkg.CanonicalizeLogRanges(q))
		if err != nil {
			b.Fatal(err)
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(plan)
		if _, err := lower.PlanQuery(q, plan, lower.Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}
