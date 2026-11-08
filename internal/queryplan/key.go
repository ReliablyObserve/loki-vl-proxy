package queryplan

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The key is a pre-order binary encoding of the node fields: a tag byte, then
// the fields in declaration order, strings and lists prefixed by their length.
// Every construct has a fixed shape, so no key is a prefix of another and a
// subtree's bytes are the same alone and inside a larger query. Order is kept
// as written (matchers and labels are never sorted), durations are int64
// nanoseconds and floats their IEEE bits. The key is never built from a String
// method or from query text.

// Tags of the node, stage and input encodings. Values are part of the key:
// never renumber them.
const (
	tagLogSelect byte = iota + 1
	tagRangeAgg
	tagVectorAgg
	tagRank
	tagBinOp
	tagScalar
	tagOpaque
	tagLineFilter
	tagParse
	tagLabelFilter
	tagProject
	tagLineFormat
	tagLabelFormat
	tagDecolorize
	tagUnwrap
	tagInputs
)

type encoder struct{ b []byte }

func (e *encoder) tag(t byte) { e.b = append(e.b, t) }

func (e *encoder) bool(v bool) {
	if v {
		e.b = append(e.b, 1)
		return
	}
	e.b = append(e.b, 0)
}

func (e *encoder) uint(v uint64) { e.b = binary.AppendUvarint(e.b, v) }

func (e *encoder) dur(d time.Duration) { e.b = binary.AppendVarint(e.b, int64(d)) }

func (e *encoder) float(v float64) { e.b = binary.LittleEndian.AppendUint64(e.b, math.Float64bits(v)) }

func (e *encoder) str(s string) {
	e.uint(uint64(len(s)))
	e.b = append(e.b, s...)
}

func (e *encoder) strs(l []string) {
	e.uint(uint64(len(l)))
	for _, s := range l {
		e.str(s)
	}
}

// grouping writes a presence byte, so `by ()` and no clause differ.
func (e *encoder) grouping(g *Grouping) {
	e.bool(g != nil)
	if g != nil {
		e.bool(g.Without)
		e.strs(g.Labels)
	}
}

func (e *encoder) matching(m *Matching) {
	e.bool(m != nil)
	if m != nil {
		e.str(m.Card)
		e.strs(m.MatchLabels)
		e.str(m.GroupSide)
		e.strs(m.Include)
	}
}

func (e *encoder) matcher(m Matcher) {
	e.str(m.Name)
	e.b = append(e.b, byte(m.Op))
	e.str(m.Value)
}

func (e *encoder) labelFilter(x *LFExpr) {
	e.b = append(e.b, byte(x.Op))
	if x.Op != logql.LabelFilterLeaf {
		e.labelFilter(x.Left)
		e.labelFilter(x.Right)
		return
	}
	e.str(x.Name)
	e.str(x.Cmp)
	e.str(x.Value)
	e.b = append(e.b, byte(x.Kind))
	if x.Kind == logql.LabelFilterValueNone {
		// A literal logql could not read leaves Value empty: keep the text apart.
		e.str(x.Leaf)
	}
}

// inputs appends the logical inputs after the root's key.
func (e *encoder) inputs(in Inputs) {
	e.tag(tagInputs)
	e.b = append(e.b, byte(in.Kind), byte(in.Compat))
	e.bool(in.Exposure.LokiProfile)
	e.bool(in.Exposure.DottedNames)
	e.strs(in.StreamLabels)
}
