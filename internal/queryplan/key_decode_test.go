package queryplan

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The decoder below reads a key back into a list of tokens, one per field, in key order. describePlan writes the
// same list from the plan and the Expr it was built from, without the encoder. A key that decodes to exactly what
// the plan says determines every encoded field, so no two plans share one, and a decoder that consumes the key to
// its last byte, node by node, shows the encoding is prefix-free and unambiguous.

type keyDecoder struct {
	b     []byte
	i     int
	toks  []string
	spans []keySpan // the key bytes of each node, in pre-order
}

type keySpan struct{ start, end int }

type keyError string

func (e keyError) Error() string { return string(e) }

func (d *keyDecoder) fail(format string, args ...any) { panic(keyError(fmt.Sprintf(format, args...))) }

func (d *keyDecoder) tok(format string, args ...any) {
	d.toks = append(d.toks, fmt.Sprintf(format, args...))
}

func (d *keyDecoder) byte() byte {
	if d.i >= len(d.b) {
		d.fail("key ends at %d", d.i)
	}
	c := d.b[d.i]
	d.i++
	return c
}

func (d *keyDecoder) uint() uint64 {
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 {
		d.fail("bad uvarint at %d", d.i)
	}
	if n != len(binary.AppendUvarint(nil, v)) {
		d.fail("non-minimal uvarint at %d", d.i)
	}
	d.i += n
	return v
}

func (d *keyDecoder) int() int64 {
	v, n := binary.Varint(d.b[d.i:])
	if n <= 0 {
		d.fail("bad varint at %d", d.i)
	}
	if n != len(binary.AppendVarint(nil, v)) {
		d.fail("non-minimal varint at %d", d.i)
	}
	d.i += n
	return v
}

// str reads a length-prefixed string, quoted so a token holds its bytes exactly.
func (d *keyDecoder) str() string {
	n := d.uint()
	if n > uint64(len(d.b)-d.i) {
		d.fail("string of %d bytes at %d runs past the key", n, d.i)
	}
	s := string(d.b[d.i : d.i+int(n)])
	d.i += int(n)
	return strconv.Quote(s)
}

func (d *keyDecoder) bool() bool {
	switch c := d.byte(); c {
	case 0:
		return false
	case 1:
		return true
	default:
		d.fail("bool byte %d at %d", c, d.i-1)
	}
	return false
}

func (d *keyDecoder) float() uint64 {
	if len(d.b)-d.i < 8 {
		d.fail("float at %d runs past the key", d.i)
	}
	v := binary.LittleEndian.Uint64(d.b[d.i:])
	d.i += 8
	return v
}

func (d *keyDecoder) strs() {
	n := d.uint()
	d.tok("n=%d", n)
	for j := uint64(0); j < n; j++ {
		d.tok("%s", d.str())
	}
}

func (d *keyDecoder) grouping() {
	if !d.bool() {
		d.tok("group=none")
		return
	}
	d.tok("group without=%v", d.bool())
	d.strs()
}

func (d *keyDecoder) matching() {
	if !d.bool() {
		d.tok("match=none")
		return
	}
	d.tok("match card=%s", d.str())
	d.strs()
	d.tok("side=%s", d.str())
	d.strs()
}

func (d *keyDecoder) labelFilter() {
	op := d.byte()
	d.tok("lf op=%d", op)
	switch logql.LabelFilterOp(op) {
	case logql.LabelFilterLeaf:
		name, cmp, value := d.str(), d.str(), d.str()
		d.tok("name=%s cmp=%s value=%s", name, cmp, value)
		kind := d.byte()
		d.tok("kind=%d", kind)
		if logql.LabelFilterValueKind(kind) == logql.LabelFilterValueNone {
			d.tok("leaf=%s", d.str())
		}
	case logql.LabelFilterAnd, logql.LabelFilterOr:
		d.labelFilter()
		d.labelFilter()
	default:
		d.fail("label filter op %d", op)
	}
}

func (d *keyDecoder) stage() {
	tag := d.byte()
	d.tok("stage %d", tag)
	switch tag {
	case tagLineFilter:
		d.tok("op=%d", d.byte())
		n := d.uint()
		d.tok("alts=%d", n)
		for j := uint64(0); j < n; j++ {
			v := d.str()
			d.tok("alt %s ip=%v", v, d.bool())
		}
	case tagParse:
		typ := d.byte()
		d.tok("type=%d source=%s", typ, d.str())
		n := d.uint()
		d.tok("fields=%d", n)
		for j := uint64(0); j < n; j++ {
			name := d.str()
			d.tok("field %s=%s", name, d.str())
		}
		if logql.ParserType(typ) != logql.ParserRegexp && logql.ParserType(typ) != logql.ParserPattern && n == 0 {
			d.tok("param=%s", d.str())
		}
	case tagLabelFilter:
		if d.bool() {
			d.tok("typed")
			d.labelFilter()
		} else {
			d.tok("raw=%s", d.str())
		}
	case tagProject:
		d.tok("keep=%v", d.bool())
		d.strs()
		n := d.uint()
		d.tok("conds=%d", n)
		for j := uint64(0); j < n; j++ {
			name, op := d.str(), d.str()
			d.tok("cond %s %s %s", name, op, d.str())
		}
	case tagLineFormat:
		d.tok("template=%s", d.str())
	case tagLabelFormat:
		n := d.uint()
		d.tok("formats=%d", n)
		for j := uint64(0); j < n; j++ {
			name, value := d.str(), d.str()
			d.tok("format %s=%s rename=%v", name, value, d.bool())
		}
	case tagDecolorize:
	case tagUnwrap:
		label := d.str()
		d.tok("unwrap %s conv=%s", label, d.str())
	default:
		d.fail("stage tag %d at %d", tag, d.i-1)
	}
}

func (d *keyDecoder) node() {
	start := d.i
	tag := d.byte()
	d.spans = append(d.spans, keySpan{start: start})
	at := len(d.spans) - 1
	d.tok("node %d", tag)
	switch tag {
	case tagLogSelect:
		n := d.uint()
		d.tok("matchers=%d", n)
		for j := uint64(0); j < n; j++ {
			name := d.str()
			op := d.byte()
			d.tok("matcher %s op=%d value=%s", name, op, d.str())
		}
		stages := d.uint()
		d.tok("stages=%d", stages)
		for j := uint64(0); j < stages; j++ {
			d.stage()
		}
	case tagRangeAgg:
		d.tok("op=%s", d.str())
		rng, off := d.int(), d.int()
		hasOffset := d.bool()
		d.tok("range=%d offset=%d has=%v", rng, off, hasOffset)
		if rng == 0 {
			d.tok("rangetext=%s", d.str())
		}
		if hasOffset && off == 0 {
			d.tok("offsettext=%s", d.str())
		}
		d.tok("phi=%d", d.float())
		d.grouping()
		if d.i >= len(d.b) || d.b[d.i] != tagLogSelect {
			d.fail("range input is not a LogSelect at %d", d.i)
		}
		d.node()
	case tagVectorAgg:
		d.tok("op=%s", d.str())
		d.grouping()
		d.node()
	case tagRank:
		op := d.str()
		d.tok("op=%s k=%d", op, d.uint())
		d.grouping()
		d.node()
	case tagBinOp:
		op := d.str()
		d.tok("op=%s bool=%v", op, d.bool())
		d.matching()
		d.node()
		d.node()
	case tagScalar:
		d.tok("value=%d", d.float())
	case tagOpaque:
		d.tok("raw=%s", d.str())
	default:
		d.fail("node tag %d at %d", tag, start)
	}
	d.spans[at].end = d.i
}

// decodeKey reads a plan key: one node tree, then the inputs, to the last byte.
func decodeKey(key []byte) (toks []string, spans []keySpan, err error) {
	d := &keyDecoder{b: key}
	defer func() {
		if r := recover(); r != nil {
			ke, ok := r.(keyError)
			if !ok {
				panic(r)
			}
			err = ke
		}
	}()
	d.node()
	if tag := d.byte(); tag != tagInputs {
		d.fail("tag %d, want the inputs", tag)
	}
	kind, compat := d.byte(), d.byte()
	d.tok("inputs kind=%d compat=%d", kind, compat)
	lp := d.bool()
	d.tok("exposure %v %v", lp, d.bool())
	d.strs()
	if d.i != len(d.b) {
		d.fail("%d bytes left after the inputs", len(d.b)-d.i)
	}
	return d.toks, d.spans, nil
}

// describer writes the tokens of a plan.
type describer struct {
	toks  []string
	texts [][]string // per RangeAgg in pre-order: the tokens of a range or offset that is not a positive duration
	next  int
}

func (d *describer) tok(format string, args ...any) {
	d.toks = append(d.toks, fmt.Sprintf(format, args...))
}

func (d *describer) strs(l []string) {
	d.tok("n=%d", len(l))
	for _, s := range l {
		d.tok("%s", strconv.Quote(s))
	}
}

func (d *describer) grouping(g *Grouping) {
	if g == nil {
		d.tok("group=none")
		return
	}
	d.tok("group without=%v", g.Without)
	d.strs(g.Labels)
}

func (d *describer) labelFilter(x *logql.LabelFilterExpr) {
	d.tok("lf op=%d", x.Op)
	if x.Op != logql.LabelFilterLeaf {
		d.labelFilter(x.Left)
		d.labelFilter(x.Right)
		return
	}
	d.tok("name=%s cmp=%s value=%s", strconv.Quote(x.Name), strconv.Quote(x.Cmp), strconv.Quote(x.Value))
	d.tok("kind=%d", x.Kind)
	if x.Kind == logql.LabelFilterValueNone {
		d.tok("leaf=%s", strconv.Quote(x.Leaf))
	}
}

func (d *describer) stage(s Stage) {
	switch x := s.(type) {
	case *LineFilter:
		d.tok("stage %d", tagLineFilter)
		d.tok("op=%d", x.Op)
		d.tok("alts=%d", 1+len(x.Or))
		d.tok("alt %s ip=%v", strconv.Quote(x.Value), x.IP)
		for _, a := range x.Or {
			d.tok("alt %s ip=%v", strconv.Quote(a.Value), a.IP)
		}
	case *Parse:
		d.tok("stage %d", tagParse)
		source := ""
		if x.Type == logql.ParserRegexp || x.Type == logql.ParserPattern {
			source = x.Param
		}
		d.tok("type=%d source=%s", x.Type, strconv.Quote(source))
		d.tok("fields=%d", len(x.Fields))
		for _, f := range x.Fields {
			d.tok("field %s=%s", strconv.Quote(f.Name), strconv.Quote(f.Expression))
		}
		if x.Type != logql.ParserRegexp && x.Type != logql.ParserPattern && len(x.Fields) == 0 {
			d.tok("param=%s", strconv.Quote(x.Param))
		}
	case *LabelFilter:
		d.tok("stage %d", tagLabelFilter)
		if x.Expr != nil {
			d.tok("typed")
			d.labelFilter(x.Expr)
		} else {
			d.tok("raw=%s", strconv.Quote(x.Raw))
		}
	case *Drop:
		d.project(false, x.Labels, x.Matchers)
	case *Keep:
		d.project(true, x.Labels, x.Matchers)
	case *LineFormat:
		d.tok("stage %d", tagLineFormat)
		d.tok("template=%s", strconv.Quote(x.Template))
	case *LabelFormat:
		d.tok("stage %d", tagLabelFormat)
		d.tok("formats=%d", len(x.Formats))
		for _, f := range x.Formats {
			d.tok("format %s=%s rename=%v", strconv.Quote(f.Name), strconv.Quote(f.Value), f.Rename)
		}
	case *Decolorize:
		d.tok("stage %d", tagDecolorize)
	case *Unwrap:
		d.tok("stage %d", tagUnwrap)
		d.tok("unwrap %s conv=%s", strconv.Quote(x.Label), strconv.Quote(x.Converter))
	}
}

func (d *describer) project(keep bool, names []string, conds []logql.DropMatcher) {
	d.tok("stage %d", tagProject)
	d.tok("keep=%v", keep)
	d.strs(names)
	d.tok("conds=%d", len(conds))
	for _, c := range conds {
		d.tok("cond %s %s %s", strconv.Quote(c.Name), strconv.Quote(c.Op), strconv.Quote(c.Value))
	}
}

// node describes n and appends the nodes in pre-order to nodes.
func (d *describer) node(n Node, nodes *[]Node) {
	*nodes = append(*nodes, n)
	switch x := n.(type) {
	case *LogSelect:
		d.tok("node %d", tagLogSelect)
		d.tok("matchers=%d", len(x.Matchers))
		for _, m := range x.Matchers {
			d.tok("matcher %s op=%d value=%s", strconv.Quote(m.Name), m.Op, strconv.Quote(m.Value))
		}
		d.tok("stages=%d", len(x.Stages))
		for _, s := range x.Stages {
			d.stage(s)
		}
	case *RangeAgg:
		d.tok("node %d", tagRangeAgg)
		d.tok("op=%s", strconv.Quote(string(x.Op)))
		d.tok("range=%d offset=%d has=%v", x.Range, x.Offset, x.HasOffset)
		d.toks = append(d.toks, d.texts[d.next]...)
		d.next++
		d.tok("phi=%d", math.Float64bits(x.Phi))
		d.grouping(x.By)
		d.node(x.In, nodes)
	case *VectorAgg:
		d.tok("node %d", tagVectorAgg)
		d.tok("op=%s", strconv.Quote(string(x.Op)))
		d.grouping(x.By)
		d.node(x.In, nodes)
	case *Rank:
		d.tok("node %d", tagRank)
		d.tok("op=%s k=%d", strconv.Quote(string(x.Op)), x.K)
		d.grouping(x.By)
		d.node(x.In, nodes)
	case *BinOp:
		d.tok("node %d", tagBinOp)
		d.tok("op=%s bool=%v", strconv.Quote(x.Op), x.Bool)
		if x.Match == nil {
			d.tok("match=none")
		} else {
			d.tok("match card=%s", strconv.Quote(x.Match.Card))
			d.strs(x.Match.MatchLabels)
			d.tok("side=%s", strconv.Quote(x.Match.GroupSide))
			d.strs(x.Match.Include)
		}
		d.node(x.L, nodes)
		d.node(x.R, nodes)
	case *Scalar:
		d.tok("node %d", tagScalar)
		d.tok("value=%d", math.Float64bits(x.V))
	case *Opaque:
		d.tok("node %d", tagOpaque)
		d.tok("raw=%s", strconv.Quote(x.Raw))
	}
}

// rangeTexts lists, for each range aggregation of expr in pre-order, the tokens the key holds for a range or an
// offset that did not read as a positive duration: their text, as written.
func rangeTexts(expr logql.Expr, out [][]string) [][]string {
	switch x := expr.(type) {
	case *logql.RangeAggregation:
		var extra []string
		if x.RangeDuration == 0 {
			extra = append(extra, "rangetext="+strconv.Quote(x.Range))
		}
		if x.HasOffset() && x.OffsetDuration == 0 {
			extra = append(extra, "offsettext="+strconv.Quote(x.Offset))
		}
		return append(out, extra)
	case *logql.VectorAggregation:
		return rangeTexts(x.Inner, out)
	case *logql.BinOpExpr:
		return rangeTexts(x.Right, rangeTexts(x.Left, out))
	}
	return out
}

// describePlan is the token list a plan's key must decode to, and the plan's nodes in pre-order.
func describePlan(p *Plan, expr logql.Expr) (toks []string, nodes []Node) {
	d := &describer{texts: rangeTexts(expr, nil)}
	d.node(p.Root, &nodes)
	d.tok("inputs kind=%d compat=%d", p.In.Kind, p.In.Compat)
	d.tok("exposure %v %v", p.In.Exposure.LokiProfile, p.In.Exposure.DottedNames)
	d.strs(p.In.StreamLabels)
	return d.toks, nodes
}

// checkKey decodes a plan's key and compares it with the plan: the tokens, the end of every node's key, and that a
// node's key is the bytes of the plan key it names, with no spare capacity to append into.
func checkKey(tb testing.TB, p *Plan, expr logql.Expr, q string) {
	tb.Helper()
	got, spans, err := decodeKey(p.Key)
	if err != nil {
		tb.Fatalf("%q: the key does not decode: %v", q, err)
	}
	want, nodes := describePlan(p, expr)
	if len(got) != len(want) {
		tb.Fatalf("%q: the key decodes to %d tokens, the plan has %d\n got %q\nwant %q", q, len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			tb.Fatalf("%q: token %d is %q, the plan has %q", q, i, got[i], want[i])
		}
	}
	if len(spans) != len(nodes) {
		tb.Fatalf("%q: %d nodes in the key, %d in the plan", q, len(spans), len(nodes))
	}
	for i, n := range nodes {
		k, sp := n.Props().Key, spans[i]
		if len(k) == 0 || !bytes.Equal(k, p.Key[sp.start:sp.end]) || &k[0] != &p.Key[sp.start] {
			tb.Fatalf("%q: node %d (%T) has key %x, not the plan key's bytes %d:%d", q, i, n, k, sp.start, sp.end)
		}
		if cap(k) != len(k) {
			tb.Fatalf("%q: node %d key has spare capacity: an append would overwrite the plan key", q, i)
		}
	}
}
