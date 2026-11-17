package proxy

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// Loki's answers for every binary operator over the edge operands (zero, negative zero, NaN, the infinities and
// ordinary numbers), recorded with Loki's engine (test/oracle/loki engine; LVP_RUN_ORACLES=1 records them again).
//
// Each operand is the value of one stream's only line, read back with last_over_time over an unwrap, so an
// operand Loki cannot write as a literal (NaN, +Inf, -Inf) is still a sample. A one-to-one join of two such
// samples (ignoring the stream label) is Loki's answer for a vector operand on each side; a literal on one side
// is recorded wherever the operand has a literal spelling.

const binaryOperatorAnswers = "binary_operator_values"

var binaryOperatorTime = int64(1700000100)

func binaryOperandText(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// binaryOperandLiteral is the operand's LogQL literal, or "" when it has none: NaN and the infinities have no
// literal, and Loki reads "-0" as a duration (a 400 "unexpected DURATION"), so negative zero has none either.
func binaryOperandLiteral(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) || (v == 0 && math.Signbit(v)) {
		return ""
	}
	return binaryOperandText(v)
}

func binaryOperandSample(i int) string {
	return fmt.Sprintf(`last_over_time({k="o%d"} | logfmt | unwrap v [1m])`, i)
}

func binaryComparison(op string) bool { return strings.ContainsAny(op, "=<>") }

// binaryBoolModes are the bool modes Loki parses for op: bool is a comparison modifier only.
func binaryBoolModes(op string) []bool {
	if binaryComparison(op) {
		return []bool{false, true}
	}
	return []bool{false}
}

func binaryOpText(op string, returnBool bool) string {
	if returnBool {
		return op + " bool"
	}
	return op
}

func binaryAnswerID(kind, op string, i, j int, returnBool bool) string {
	return fmt.Sprintf("%s/%s/%d/%d/%v", kind, op, i, j, returnBool)
}

func binaryOperatorDataSet() oraclerun.EngineDataSet {
	set := oraclerun.EngineDataSet{}
	for i, v := range binaryEdgeOperands {
		set.Streams = append(set.Streams, oraclerun.EngineStream{
			Labels:  map[string]string{"k": "o" + strconv.Itoa(i)},
			Entries: []oraclerun.EngineEntry{{TS: (binaryOperatorTime - 30) * 1e9, Line: "v=" + binaryOperandText(v)}},
		})
	}
	instant := func(id, q string) {
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: id, Instant: true, Form: url.Values{"query": {q}, "time": {strconv.FormatInt(binaryOperatorTime, 10)}}.Encode()})
	}
	for i := range binaryEdgeOperands {
		instant(fmt.Sprintf("operand/%d", i), binaryOperandSample(i))
	}
	for _, op := range binaryEdgeOps {
		for _, returnBool := range binaryBoolModes(op) {
			o := binaryOpText(op, returnBool)
			for i, a := range binaryEdgeOperands {
				for j, b := range binaryEdgeOperands {
					instant(binaryAnswerID("vv", op, i, j, returnBool), binaryOperandSample(i)+" "+o+" ignoring(k) "+binaryOperandSample(j))
					if lit := binaryOperandLiteral(a); lit != "" {
						instant(binaryAnswerID("sv", op, i, j, returnBool), lit+" "+o+" "+binaryOperandSample(j))
					}
					if lit := binaryOperandLiteral(b); lit != "" {
						instant(binaryAnswerID("vs", op, i, j, returnBool), binaryOperandSample(i)+" "+o+" "+lit)
					}
					if la, lb := binaryOperandLiteral(a), binaryOperandLiteral(b); la != "" && lb != "" {
						instant(binaryAnswerID("ss", op, i, j, returnBool), la+" "+o+" "+lb)
					}
				}
			}
		}
	}
	return set
}

// binaryLokiValue is one recorded answer read as a sample: the value Loki wrote, whether it kept a sample at all,
// and whether it answered (status 200).
type binaryLokiValue struct {
	value    float64
	text     string
	kept, ok bool
	status   int
	err      string
}

type binaryLokiAnswers map[string]binaryLokiValue

func readBinaryLokiAnswers(t testing.TB) binaryLokiAnswers {
	t.Helper()
	set := binaryOperatorDataSet()
	raw := lokiEngineAnswers(t, binaryOperatorAnswers, []oraclerun.EngineDataSet{set})
	out := binaryLokiAnswers{}
	for id, a := range raw {
		v := binaryLokiValue{status: a.Status, err: a.Error}
		if a.Status == 200 {
			v.ok = true
			s, err := decodeLokiSeries(a.Body)
			if err != nil {
				t.Fatalf("%s: %v", id, err)
			}
			switch {
			case s.Scalar != nil:
				v.text, _ = s.Scalar[1].(string)
				v.kept = true
			case len(s.Points) > 1:
				t.Fatalf("%s: %d series", id, len(s.Points))
			default:
				for _, points := range s.Points {
					for _, text := range points {
						v.text, v.kept = text, true
					}
				}
			}
			if v.kept {
				f, err := strconv.ParseFloat(v.text, 64)
				if err != nil {
					t.Fatalf("%s: value %q", id, v.text)
				}
				v.value = f
			}
		}
		out[id] = v
	}
	return out
}

// get returns a recorded answer; a missing one fails the test.
func (a binaryLokiAnswers) get(t testing.TB, id string) binaryLokiValue {
	t.Helper()
	v, ok := a[id]
	if !ok {
		t.Fatalf("no recorded Loki answer %s", id)
	}
	return v
}

// vector is Loki's answer for a vector operand on each side (one-to-one); an arithmetic operator has one mode,
// whatever returnBool asks.
func (a binaryLokiAnswers) vector(t testing.TB, op string, i, j int, returnBool bool) binaryLokiValue {
	t.Helper()
	v := a.get(t, binaryAnswerID("vv", op, i, j, returnBool && binaryComparison(op)))
	if !v.ok {
		t.Fatalf("Loki answered %d %s for %s", v.status, v.err, binaryAnswerID("vv", op, i, j, returnBool))
	}
	return v
}

// Every operand reads back as itself, so a recorded join is the operator applied to the operands.
//
// conformance: semantics/binary-division-by-zero-nan
func TestBinaryOperatorRecordedOperands(t *testing.T) {
	answers := readBinaryLokiAnswers(t)
	for i, v := range binaryEdgeOperands {
		got := answers.get(t, fmt.Sprintf("operand/%d", i))
		if !got.ok || !got.kept || !sameFloat(got.value, v) || math.Signbit(got.value) != math.Signbit(v) {
			t.Errorf("operand %v reads back as %+v", v, got)
		}
	}
}

// What the recordings show about a literal operand, so the matcher tests below can read a vector join for an
// operand with no literal: with a literal on one side, Loki gives the value and keep of the vector join, except
// that a filtering comparison keeps the vector side's value; two literals give the bool value of the join, for a
// filtering comparison too (1 or 0, never dropped).
//
// conformance: semantics/binary-division-by-zero-nan
func TestBinaryOperatorLiteralSidesMatchVectorJoin(t *testing.T) {
	answers := readBinaryLokiAnswers(t)
	for _, op := range binaryEdgeOps {
		for _, returnBool := range binaryBoolModes(op) {
			filter := !returnBool && binaryComparison(op)
			for i, a := range binaryEdgeOperands {
				for j, b := range binaryEdgeOperands {
					join := answers.vector(t, op, i, j, returnBool)
					what := fmt.Sprintf("%v %s %v", a, binaryOpText(op, returnBool), b)
					if v, ok := answers[binaryAnswerID("vs", op, i, j, returnBool)]; ok {
						if !v.ok || v.kept != join.kept || (v.kept && !sameFloat(v.value, join.value)) {
							t.Errorf("vector %s literal: %+v, join %+v", what, v, join)
						}
					}
					if v, ok := answers[binaryAnswerID("sv", op, i, j, returnBool)]; ok {
						want := join.value
						if filter {
							want = b
						}
						if !v.ok || v.kept != join.kept || (v.kept && !sameFloat(v.value, want)) {
							t.Errorf("literal %s vector: %+v, join %+v", what, v, join)
						}
					}
					if v, ok := answers[binaryAnswerID("ss", op, i, j, returnBool)]; ok {
						want := answers.vector(t, op, i, j, true)
						if !v.ok || !v.kept || !sameFloat(v.value, want.value) {
							t.Errorf("literal %s literal: %+v, bool join %+v", what, v, want)
						}
					}
				}
			}
		}
	}
}
