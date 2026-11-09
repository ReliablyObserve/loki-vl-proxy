package proxy

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// legacyOperand is what the binary handlers read from an operand's text before they read its physical operator:
// a number (translator.IsScalar), a nested binary expression (the __binary__ prefix, read with
// ParseBinaryMetricExprFull), or a query sent as it is.
type legacyOperand struct {
	scalar, binary bool
	op             string
	vm             *translator.VectorMatchInfo
	left, right    string
}

func readLegacyOperand(s string) legacyOperand {
	l := legacyOperand{scalar: translator.IsScalar(s)}
	if !l.scalar && strings.HasPrefix(s, translator.BinaryMetricPrefix) {
		l.binary = true
		l.op, l.left, l.right, l.vm, _ = translator.ParseBinaryMetricExprFull(s)
	}
	return l
}

// checkOperand compares the physical operator of an operand with what the text readers read from text, down
// the nested binary expressions: the same kind, the same text sent to VictoriaLogs or written as the scalar, the
// same operator and the same vector matching (empty, not nil, without modifiers).
func checkOperand(t *testing.T, where, text string, op physical.Op) {
	t.Helper()
	l := readLegacyOperand(text)
	if got := operandText(op); got != text {
		t.Fatalf("%s: operand text %q, translated %q", where, got, text)
	}
	if isScalarOperand(op) != l.scalar {
		t.Fatalf("%s: %q scalar %v, text reader %v", where, text, isScalarOperand(op), l.scalar)
	}
	b, isBinary := op.(physical.Binary)
	if isBinary != l.binary {
		t.Fatalf("%s: %q binary %v, text reader %v", where, text, isBinary, l.binary)
	}
	if !isBinary {
		return
	}
	if b.Op != l.op || !reflect.DeepEqual(binaryMatching(b.Match), l.vm) {
		t.Fatalf("%s: %q op %q matching %+v, text reader %q %+v", where, text, b.Op, binaryMatching(b.Match), l.op, l.vm)
	}
	checkOperand(t, where+" left", l.left, b.L)
	checkOperand(t, where+" right", l.right, b.R)
}

// The binary handlers read each operand's physical operator (translator.DecodePhysical) where they read its text:
// on every plan-corpus binary expression, both sides, nested binaries included, the operator gives what the text
// readers gave, so the requests to VictoriaLogs and the answers do not change.
//
// conformance: architecture-syntax-package-layering
func TestBinaryOperandsReadLikeTheirText(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	p := newTestProxy(t, "http://unused")
	kinds := map[string]int{}
	for _, q := range qs {
		expr, msg := logqlpkg.ValidateLogQLExpr(q)
		if msg != "" {
			continue
		}
		bin, ok := expr.(*logqlpkg.BinOpExpr)
		if !ok {
			continue
		}
		for i, side := range []logqlpkg.Expr{bin.Left, bin.Right} {
			op, err := p.translateBinOpSide(t.Context(), side)
			if err != nil {
				continue
			}
			text := side.String()
			if _, lit := side.(*logqlpkg.LiteralExpr); !lit {
				if text, err = p.translateQueryWithContext(t.Context(), side.String()); err != nil {
					t.Fatal(err)
				}
			}
			kinds[strings.TrimPrefix(reflect.TypeOf(op).String(), "physical.")]++
			checkOperand(t, q+[]string{" left", " right"}[i], text, op)
		}
	}
	t.Logf("operands by kind: %v", kinds)
	for _, k := range []string{"ScalarOp", "Binary", "VLQuery"} {
		if kinds[k] == 0 {
			t.Fatalf("no %s operand in the corpus: %v", k, kinds)
		}
	}
}

// A nested binary expression's matching is empty, not nil, without modifiers (the text reader's answer), and
// carries every modifier with them.
func TestBinaryMatchingOfNestedExpressions(t *testing.T) {
	if vm := binaryMatching(nil); vm == nil || !reflect.DeepEqual(*vm, translator.VectorMatchInfo{}) {
		t.Fatalf("binaryMatching(nil) = %+v", vm)
	}
	m := &physical.Matching{MatchOn: true, GroupSide: "group_left", On: []string{"app"}, GroupLeft: []string{"pod"}}
	want := &translator.VectorMatchInfo{MatchOn: true, GroupSide: "group_left", On: []string{"app"}, GroupLeft: []string{"pod"}}
	if vm := binaryMatching(m); !reflect.DeepEqual(vm, want) {
		t.Fatalf("binaryMatching = %+v, want %+v", vm, want)
	}
}
