package queryplan

import (
	"fmt"
	"sync"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// UnsupportedError is returned for an Expr or Inputs the plan has no node for.
type UnsupportedError struct {
	Msg string
}

func (e *UnsupportedError) Error() string { return e.Msg }

func unsupported(format string, args ...any) error {
	return &UnsupportedError{Msg: fmt.Sprintf(format, args...)}
}

// scratch is the memory a build reuses: the key as it is written, and where each
// node's part of it is, so the plan's key is allocated once, at its final size.
type scratch struct {
	key   []byte
	seals []seal
}

// seal is the part of the key a node's subtree wrote.
type seal struct {
	p      *Props
	ks, ke uint32
}

var scratchPool = sync.Pool{New: func() any { return &scratch{key: make([]byte, 0, 512), seals: make([]seal, 0, 8)} }}

// maxPooledScratch is the largest key buffer put back in the pool.
const maxPooledScratch = 64 << 10

// Build converts a validated Expr into a logical plan. The conversion is
// structural and total: every Expr the parser produces has a node, and none
// reads or prints query text. The Expr must not change afterwards: the plan
// shares its leaf values and stages, and Build itself never changes it, so
// the same Expr can be built again and from several goroutines.
func Build(expr logql.Expr, in Inputs) (*Plan, error) {
	if in.Compat != Compat37 {
		return nil, unsupported("queryplan: unsupported compat version %d", in.Compat)
	}
	if in.Kind > KindInstant {
		return nil, unsupported("queryplan: unknown kind %d", in.Kind)
	}
	sc := scratchPool.Get().(*scratch)
	p := new(Plan)
	b := builder{e: encoder{b: sc.key[:0]}, plan: p, seals: sc.seals[:0]}
	root, err := b.node(expr)
	if err == nil {
		n := len(b.e.b)
		b.e.inputs(in)
		key := make([]byte, len(b.e.b))
		copy(key, b.e.b)
		p.Root, p.In, p.Key = root, in, key
		for _, s := range b.seals {
			s.p.Key = key[s.ks:s.ke:s.ke]
		}
		root.Props().Key = key[:n:n]
	} else {
		p = nil
	}
	clear(b.seals)
	sc.seals = b.seals[:0]
	sc.key = b.e.b[:0]
	if cap(sc.key) <= maxPooledScratch {
		scratchPool.Put(sc)
	}
	return p, err
}

// builder writes one pre-order key while it builds the nodes. A node's key is
// the part of it its subtree wrote.
type builder struct {
	e     encoder
	plan  *Plan
	seals []seal
	depth int
	// used says which of the plan's inline nodes are taken.
	usedSel, usedRng, usedVec bool
}

func (b *builder) newSelect() *LogSelect {
	if !b.usedSel {
		b.usedSel = true
		return &b.plan.mem.sel
	}
	return new(LogSelect)
}

func (b *builder) newRange() *RangeAgg {
	if !b.usedRng {
		b.usedRng = true
		return &b.plan.mem.rng
	}
	return new(RangeAgg)
}

func (b *builder) newVector() *VectorAgg {
	if !b.usedVec {
		b.usedVec = true
		return &b.plan.mem.vec
	}
	return new(VectorAgg)
}

// maxDepth is how deep a query may nest (aggregations, binary operands, label filter groups). Build recurses
// with the tree, and a Go stack that overflows ends the process, so a tree deeper than any real query is refused
// with an UnsupportedError. The parser reads far deeper trees than this, so the limit is the first to apply.
const maxDepth = 1 << 18

func (b *builder) node(expr logql.Expr) (Node, error) {
	if b.depth >= maxDepth {
		return nil, unsupported("queryplan: query nested deeper than %d", maxDepth)
	}
	b.depth++
	n, err := b.build(expr)
	b.depth--
	return n, err
}

func (b *builder) build(expr logql.Expr) (Node, error) {
	switch x := expr.(type) {
	case *logql.LogQuery:
		return b.logSelect(x.Selector, x.Pipeline)
	case *logql.StreamSelector:
		return b.logSelect(x, nil)
	case *logql.RangeAggregation:
		return b.rangeAgg(x)
	case *logql.VectorAggregation:
		return b.vectorAgg(x)
	case *logql.BinOpExpr:
		return b.binOp(x)
	case *logql.LiteralExpr:
		n := &Scalar{V: x.Value}
		start := len(b.e.b)
		b.e.tag(tagScalar)
		b.e.float(x.Value)
		b.seal(&n.p, start)
		return n, nil
	case *logql.OpaqueMetricExpr:
		if x.Typed && x.Func == "label_replace" && len(x.Args) == 4 {
			return b.labelReplace(x)
		}
		if x.Typed && x.Func == "vector" {
			n := &VectorLit{V: x.Value}
			start := len(b.e.b)
			b.e.tag(tagVectorLit)
			b.e.float(x.Value)
			b.seal(&n.p, start)
			return n, nil
		}
		n := &Opaque{Raw: x.Raw}
		start := len(b.e.b)
		b.e.tag(tagOpaque)
		b.e.str(x.Raw)
		b.seal(&n.p, start)
		return n, nil
	case *logql.BadLiteralExpr:
		// A number literal Go cannot read: the query is rejected by validation (Loki reports it after reading the
		// whole query), so the plan only has to be total over it; it is never lowered.
		n := &Opaque{Raw: x.String()}
		start := len(b.e.b)
		b.e.tag(tagOpaque)
		b.e.str(n.Raw)
		b.seal(&n.p, start)
		return n, nil
	}
	return nil, unsupported("queryplan: no node for %T", expr)
}

// labelReplace builds label_replace from the call's typed parts: the key is its tag, the sample expression and the
// four strings in Loki's argument order.
func (b *builder) labelReplace(x *logql.OpaqueMetricExpr) (Node, error) {
	n := &LabelReplace{Dst: x.Args[0], Replacement: x.Args[1], Src: x.Args[2], Regex: x.Args[3]}
	start := len(b.e.b)
	b.e.tag(tagLabelReplace)
	in, err := b.node(x.Inner)
	if err != nil {
		return nil, err
	}
	n.In = in
	for _, a := range x.Args {
		b.e.str(a)
	}
	b.seal(&n.p, start)
	return n, nil
}

// seal records the part of the key the subtree wrote since start.
func (b *builder) seal(p *Props, start int) {
	b.seals = append(b.seals, seal{p, uint32(start), uint32(len(b.e.b))})
}

func (b *builder) logSelect(sel *logql.StreamSelector, pipeline []logql.Stage) (*LogSelect, error) {
	n := b.newSelect()
	start := len(b.e.b)
	b.e.tag(tagLogSelect)
	if sel != nil {
		n.Matchers = sel.Matchers
	}
	b.e.uint(uint64(len(n.Matchers)))
	for _, m := range n.Matchers {
		b.e.str(m.Name)
		b.e.b = append(b.e.b, byte(m.Op))
		b.e.str(m.Value)
	}
	n.Stages = pipeline
	b.e.uint(uint64(len(pipeline)))
	for _, s := range pipeline {
		if err := b.stage(s); err != nil {
			return nil, err
		}
	}
	b.seal(&n.p, start)
	return n, nil
}

// stage writes a stage into the key. The stages are the AST's: Build only
// refuses one it has no encoding for.
func (b *builder) stage(s logql.Stage) error {
	e := &b.e
	switch x := s.(type) {
	case *logql.LineFilterStage:
		e.tag(tagLineFilter)
		e.b = append(e.b, byte(x.Op))
		e.uint(uint64(1 + len(x.Or)))
		e.str(x.Value)
		e.bool(x.IP)
		for _, a := range x.Or {
			e.str(a.Value)
			e.bool(a.IP)
		}
	case *logql.ParserStage:
		e.tag(tagParse)
		// The logfmt flags ride on the type byte, so a stage without them keeps its key.
		t := byte(x.Type)
		if x.Strict {
			t |= parseFlagStrict
		}
		if x.KeepEmpty {
			t |= parseFlagKeepEmpty
		}
		e.b = append(e.b, t)
		// Param is the source of a regexp or pattern parser; for json and
		// logfmt it is the argument text, which the field list covers.
		source := x.Type == logql.ParserRegexp || x.Type == logql.ParserPattern
		if source {
			e.str(x.Param)
		} else {
			e.str("")
		}
		e.uint(uint64(len(x.Fields)))
		for _, f := range x.Fields {
			e.str(f.Name)
			e.str(f.Expression)
		}
		if !source && len(x.Fields) == 0 {
			e.str(x.Param)
		}
	case *logql.LabelFilterStage:
		e.tag(tagLabelFilter)
		e.bool(x.Expr != nil)
		if x.Expr != nil {
			if !e.labelFilter(x.Expr, 0) {
				return unsupported("queryplan: label filter nested deeper than %d", maxDepth)
			}
		} else {
			e.str(x.Raw)
		}
	case *logql.DropStage:
		e.project(false, x.Labels, x.Matchers)
	case *logql.KeepStage:
		e.project(true, x.Labels, x.Matchers)
	case *logql.LineFormatStage:
		e.tag(tagLineFormat)
		e.str(x.KeyText())
	case *logql.LabelFormatStage:
		e.tag(tagLabelFormat)
		e.uint(uint64(len(x.Formats)))
		for _, f := range x.Formats {
			e.str(f.Name)
			e.str(f.Value)
			e.bool(f.Rename)
		}
	case *logql.DecolorizeStage:
		e.tag(tagDecolorize)
	case *logql.UnwrapStage:
		e.tag(tagUnwrap)
		e.str(x.Label)
		e.str(x.Converter)
	default:
		return unsupported("queryplan: no stage for %T", s)
	}
	return nil
}

func (b *builder) rangeAgg(x *logql.RangeAggregation) (Node, error) {
	n := b.newRange()
	n.Op, n.Range, n.Offset, n.HasOffset, n.By = x.Op, x.RangeDuration, x.OffsetDuration, x.HasOffset(), x.Grouping
	if x.HasParam {
		n.Phi, n.PhiText = x.Param, x.ParamText
	}
	start := len(b.e.b)
	e := &b.e
	e.tag(tagRangeAgg)
	e.str(string(x.Op))
	e.dur(n.Range)
	e.dur(n.Offset)
	e.bool(n.HasOffset)
	// A literal that did not read as a positive duration keeps its text.
	if n.Range == 0 {
		e.str(x.Range)
	}
	if n.HasOffset && n.Offset == 0 {
		e.str(x.Offset)
	}
	e.float(n.Phi)
	e.grouping(n.By)
	var err error
	switch in := x.Inner.(type) {
	case *logql.LogQuery:
		n.In, err = b.logSelect(in.Selector, in.Pipeline)
	case *logql.StreamSelector:
		n.In, err = b.logSelect(in, nil)
	default:
		err = unsupported("queryplan: no range input for %T", x.Inner)
	}
	if err != nil {
		return nil, err
	}
	n.p.Lookback, n.p.Offset = n.Range+n.Offset, n.Offset
	b.seal(&n.p, start)
	return n, nil
}

func (b *builder) vectorAgg(x *logql.VectorAggregation) (Node, error) {
	start := len(b.e.b)
	e := &b.e
	switch x.Op {
	case logql.VectorTopK, logql.VectorBottomK, logql.VectorSort, logql.VectorSortDesc:
		n := &Rank{Op: x.Op, By: x.Grouping}
		if x.HasParam {
			n.K = int(x.Param)
		}
		e.tag(tagRank)
		e.str(string(n.Op))
		e.uint(uint64(n.K))
		e.grouping(n.By)
		in, err := b.node(x.Inner)
		if err != nil {
			return nil, err
		}
		n.In = in
		inherit(&n.p, in)
		b.seal(&n.p, start)
		return n, nil
	}
	n := b.newVector()
	n.Op, n.By = x.Op, x.Grouping
	e.tag(tagVectorAgg)
	e.str(string(n.Op))
	e.grouping(n.By)
	in, err := b.node(x.Inner)
	if err != nil {
		return nil, err
	}
	n.In = in
	inherit(&n.p, in)
	b.seal(&n.p, start)
	return n, nil
}

func (b *builder) binOp(x *logql.BinOpExpr) (Node, error) {
	n := &BinOp{Op: x.Op, Bool: x.ReturnBool, Match: x.VectorMatching}
	start := len(b.e.b)
	b.e.tag(tagBinOp)
	b.e.str(n.Op)
	b.e.bool(n.Bool)
	b.e.matching(n.Match)
	var err error
	if n.L, err = b.node(x.Left); err != nil {
		return nil, err
	}
	if n.R, err = b.node(x.Right); err != nil {
		return nil, err
	}
	inherit(&n.p, n.L)
	inherit(&n.p, n.R)
	b.seal(&n.p, start)
	return n, nil
}

// inherit raises p's lookback and offset to the child's.
func inherit(p *Props, child Node) {
	c := child.Props()
	p.Lookback = max(p.Lookback, c.Lookback)
	p.Offset = max(p.Offset, c.Offset)
}
