package queryplan

import (
	"fmt"

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

// Build converts a validated Expr into a logical plan. The conversion is
// structural and total: every Expr the parser produces has a node, and none
// reads or prints query text. The Expr must not change afterwards: the plan
// shares its leaf values.
func Build(expr logql.Expr, in Inputs) (*Plan, error) {
	if in.Compat != Compat37 {
		return nil, unsupported("queryplan: unsupported compat version %d", in.Compat)
	}
	if in.Kind > KindInstant {
		return nil, unsupported("queryplan: unknown kind %d", in.Kind)
	}
	b := builder{e: encoder{b: make([]byte, 0, 512)}}
	root, err := b.node(expr)
	if err != nil {
		return nil, err
	}
	n := len(b.e.b)
	root.Props().Key = b.e.b[:n:n]
	b.e.inputs(in)
	return &Plan{Root: root, In: in, Key: b.e.b}, nil
}

// builder writes one pre-order key while it builds the nodes. A node's Key is
// the slice of that buffer its subtree wrote.
type builder struct{ e encoder }

func (b *builder) node(expr logql.Expr) (Node, error) {
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
		n := &Opaque{Raw: x.Raw}
		start := len(b.e.b)
		b.e.tag(tagOpaque)
		b.e.str(x.Raw)
		b.seal(&n.p, start)
		return n, nil
	}
	return nil, unsupported("queryplan: no node for %T", expr)
}

// seal sets the key of the subtree written since start.
func (b *builder) seal(p *Props, start int) {
	end := len(b.e.b)
	p.Key = b.e.b[start:end:end]
}

func (b *builder) logSelect(sel *logql.StreamSelector, pipeline []logql.Stage) (*LogSelect, error) {
	n := &LogSelect{}
	start := len(b.e.b)
	b.e.tag(tagLogSelect)
	if sel != nil {
		n.Matchers = make([]Matcher, len(sel.Matchers))
		for i, m := range sel.Matchers {
			n.Matchers[i] = Matcher{Name: m.Name, Value: m.Value, Op: m.Op}
		}
	}
	b.e.uint(uint64(len(n.Matchers)))
	for _, m := range n.Matchers {
		b.e.matcher(m)
	}
	n.Stages = make([]Stage, 0, len(pipeline))
	b.e.uint(uint64(len(pipeline)))
	for _, s := range pipeline {
		st, err := b.stage(s)
		if err != nil {
			return nil, err
		}
		n.Stages = append(n.Stages, st)
	}
	b.seal(&n.p, start)
	return n, nil
}

func (b *builder) stage(s logql.Stage) (Stage, error) {
	e := &b.e
	switch x := s.(type) {
	case *logql.LineFilterStage:
		st := &LineFilter{Op: x.Op, Alts: make([]LineMatch, 1+len(x.Or))}
		st.Alts[0] = LineMatch{Value: x.Value, IP: x.IP}
		for i, a := range x.Or {
			st.Alts[1+i] = LineMatch{Value: a.Value, IP: a.IP}
		}
		e.tag(tagLineFilter)
		e.b = append(e.b, byte(st.Op))
		e.uint(uint64(len(st.Alts)))
		for _, a := range st.Alts {
			e.str(a.Value)
			e.bool(a.IP)
		}
		return st, nil
	case *logql.ParserStage:
		st := &Parse{Kind: x.Type, Fields: x.Fields}
		switch x.Type {
		case logql.ParserRegexp, logql.ParserPattern:
			st.Expr = x.Param
		default:
			st.Param = x.Param
		}
		e.tag(tagParse)
		e.b = append(e.b, byte(st.Kind))
		e.str(st.Expr)
		// The field list is typed; its text is only kept apart when there is no list.
		e.uint(uint64(len(st.Fields)))
		for _, f := range st.Fields {
			e.str(f.Name)
			e.str(f.Expression)
		}
		if len(st.Fields) == 0 {
			e.str(st.Param)
		}
		return st, nil
	case *logql.LabelFilterStage:
		st := &LabelFilter{Expr: x.Expr}
		e.tag(tagLabelFilter)
		e.bool(x.Expr != nil)
		if x.Expr != nil {
			e.labelFilter(x.Expr)
		} else {
			st.Raw = x.Raw
			e.str(st.Raw)
		}
		return st, nil
	case *logql.DropStage:
		return b.project(false, x.Labels, x.Matchers), nil
	case *logql.KeepStage:
		return b.project(true, x.Labels, x.Matchers), nil
	case *logql.LineFormatStage:
		e.tag(tagLineFormat)
		e.str(x.Template)
		return &LineFormat{Raw: x.Template}, nil
	case *logql.LabelFormatStage:
		e.tag(tagLabelFormat)
		e.uint(uint64(len(x.Formats)))
		for _, f := range x.Formats {
			e.str(f.Name)
			e.str(f.Value)
			e.bool(f.Rename)
		}
		return &LabelFormat{Entries: x.Formats}, nil
	case *logql.DecolorizeStage:
		e.tag(tagDecolorize)
		return &Decolorize{}, nil
	case *logql.UnwrapStage:
		e.tag(tagUnwrap)
		e.str(x.Label)
		e.str(x.Converter)
		return &Unwrap{Field: x.Label, Conv: x.Converter}, nil
	}
	return nil, unsupported("queryplan: no stage for %T", s)
}

func (b *builder) project(keep bool, names []string, conds []logql.DropMatcher) Stage {
	st := &Project{Keep: keep, Names: names}
	b.e.tag(tagProject)
	b.e.bool(keep)
	b.e.strs(names)
	if len(conds) > 0 {
		st.Conds = make([]Matcher, len(conds))
	}
	b.e.uint(uint64(len(conds)))
	for i, c := range conds {
		st.Conds[i] = Matcher{Name: c.Name, Value: c.Value, Op: dropOp(c.Op)}
		b.e.str(c.Name)
		b.e.str(c.Op)
		b.e.str(c.Value)
	}
	return st
}

// dropOp reads the operator text of a keep or drop condition.
func dropOp(op string) MatchOp {
	switch op {
	case "!=":
		return logql.MatchNeq
	case "=~":
		return logql.MatchRe
	case "!~":
		return logql.MatchNotRe
	}
	return logql.MatchEq
}

func (b *builder) rangeAgg(x *logql.RangeAggregation) (Node, error) {
	n := &RangeAgg{
		Op: x.Op, Range: x.RangeDuration, Offset: x.OffsetDuration, HasOffset: x.HasOffset(),
		By: x.Grouping,
	}
	if x.HasParam {
		n.Phi = x.Param
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
	n := &VectorAgg{Op: x.Op, By: x.Grouping}
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
