package queryplan

// Failures is the set of failure classes a request that evaluates a plan can
// end in, read from the tree. It decides which failed answers of the request
// may be held and shared (the cache layer's Policy keeps only these classes);
// the TTL of each class is the operator's, not the plan's.
type Failures uint8

const (
	// FailBackend: the plan reads log data, so a failed or overloaded backend
	// can end the request.
	FailBackend Failures = 1 << iota
	// FailLimit: the plan reads log data, so a limit on what it reads or
	// returns (series, lines, response size) can end the request; whether it
	// does depends on the data in the window.
	FailLimit
	// FailPipeline: a metric reads a pipeline whose stages fail on line
	// content, a parser whose error reaches the metric (not dropped or
	// filtered) or an unwrap whose conversion can fail; whether a line fails
	// depends on the data in the window.
	FailPipeline
)

// Has reports whether every class in c is in f.
func (f Failures) Has(c Failures) bool { return f&c == c }

// Failures is the set of failure classes of the plan. Resolve must have run:
// the pipeline error policy is the resolver's.
func (p *Plan) Failures() Failures {
	if p == nil || p.Root == nil {
		return 0
	}
	return failuresOf(p.Root, false, 0)
}

func failuresOf(n Node, metric bool, depth int) Failures {
	if depth > maxDepth {
		// Deeper than any plan Build accepts: claim every class but the ones
		// read from the data, so nothing data-dependent is held for it.
		return FailBackend
	}
	switch x := n.(type) {
	case *LogSelect:
		f := FailBackend | FailLimit
		if metric && pipelineFails(x) {
			f |= FailPipeline
		}
		return f
	case *RangeAgg:
		if x.In == nil {
			return 0
		}
		return failuresOf(x.In, true, depth+1)
	case *VectorAgg:
		return failuresOf(x.In, true, depth+1)
	case *Rank:
		return failuresOf(x.In, true, depth+1)
	case *LabelReplace:
		return failuresOf(x.In, true, depth+1)
	case *BinOp:
		return failuresOf(x.L, true, depth+1) | failuresOf(x.R, true, depth+1)
	case *Opaque:
		// A call the plan keeps as text: its operands are not typed, so any
		// class but the data-dependent pipeline ones.
		return FailBackend | FailLimit
	}
	return 0 // a number or vector(n): no data read
}

// pipelineFails reports a pipeline whose lines can fail a metric: an unwrap
// stage (its conversion), or a parser whose error the resolver says reaches
// the end of the pipeline.
func pipelineFails(s *LogSelect) bool {
	for _, st := range s.Stages {
		if _, ok := st.(*Unwrap); ok {
			return true
		}
	}
	return s.Sem != nil && s.Sem.ErrorPolicy == ErrorPropagate
}
