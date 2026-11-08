package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// The facts command reads each input with Loki's parser and writes the facts of its tree that the proxy's query
// plan resolver derives on its own (internal/queryplan Resolve): the outermost aggregation and its grouping, the
// offset of every range aggregation, and the stages of the log pipeline under the aggregations. The proxy's test
// compares its resolved plan with them; Loki is read through its exported syntax tree only.

// FactsInput is one query.
type FactsInput struct {
	ID    string `json:"id"`
	Query string `json:"q"`
}

// Grouping is a by or without clause as Loki's tree holds it.
type Grouping struct {
	Groups  []string `json:"groups"`
	Without bool     `json:"without"`
}

// Stage is one stage of a log pipeline: its kind (json, logfmt, regexp, pattern, unpack, drop, other) and, for a
// drop, the names it lists and whether it carries a condition.
type Stage struct {
	Kind        string   `json:"kind"`
	Names       []string `json:"names,omitempty"`
	Conditional bool     `json:"conditional,omitempty"`
}

// Facts is what Loki's tree says about a query.
type Facts struct {
	ID     string `json:"id"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
	// Outer is the operation of the outermost aggregation under any label_replace ("" for none), and Grouping its
	// clause (nil for none).
	Outer    string    `json:"outer"`
	Grouping *Grouping `json:"grouping"`
	// Offsets are the offsets of the range aggregations in Loki's tree, left operand first, in nanoseconds.
	Offsets []int64 `json:"offsets"`
	// Pipeline is the stages of the log query, or of the range aggregation under the aggregations; nil for any
	// other shape (a binary operation, a literal).
	Pipeline []Stage `json:"pipeline"`
	// HasPipeline is false when there is no such log query.
	HasPipeline bool `json:"has_pipeline"`
}

func facts(in, out string) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer f.Close()
	o, err := os.Create(out)
	if err != nil {
		return err
	}
	defer o.Close()
	w := bufio.NewWriter(o)
	enc := json.NewEncoder(w)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var x FactsInput
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return err
		}
		if err := enc.Encode(factsOf(x)); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return w.Flush()
}

func factsOf(x FactsInput) (r Facts) {
	r.ID = x.ID
	defer func() {
		if p := recover(); p != nil {
			r = Facts{ID: x.ID, Error: fmt.Sprintf("parser panic: %v", p)}
		}
	}()
	expr, err := syntax.ParseExpr(x.Query)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.Accept = true
	r.Offsets = []int64{}
	// The outermost aggregation.
	top := expr
	for {
		if lr, ok := top.(*syntax.LabelReplaceExpr); ok {
			top = lr.Left
			continue
		}
		break
	}
	switch v := top.(type) {
	case *syntax.VectorAggregationExpr:
		r.Outer = v.Operation
		if v.Grouping != nil {
			r.Grouping = &Grouping{Groups: append([]string{}, v.Grouping.Groups...), Without: v.Grouping.Without}
		}
	case *syntax.RangeAggregationExpr:
		r.Outer = v.Operation
	}
	offsets(expr, &r.Offsets)
	if sel := pipelineUnderAggregations(expr); sel != nil {
		r.HasPipeline = true
		r.Pipeline = []Stage{}
		if p, ok := sel.(*syntax.PipelineExpr); ok {
			for _, st := range p.MultiStages {
				r.Pipeline = append(r.Pipeline, stageOf(st))
			}
		}
	}
	return r
}

// offsets appends the offset of every range aggregation, the left operand of a binary operation first.
func offsets(e syntax.Expr, out *[]int64) {
	switch v := e.(type) {
	case *syntax.RangeAggregationExpr:
		*out = append(*out, int64(v.Left.Offset))
	case *syntax.VectorAggregationExpr:
		offsets(v.Left, out)
	case *syntax.LabelReplaceExpr:
		offsets(v.Left, out)
	case *syntax.BinOpExpr:
		offsets(v.SampleExpr, out)
		offsets(v.RHS, out)
	}
}

// pipelineUnderAggregations is the log selector of a log query, or of the range aggregation under label_replace
// calls and vector aggregations; nil otherwise.
func pipelineUnderAggregations(e syntax.Expr) syntax.LogSelectorExpr {
	for {
		switch v := e.(type) {
		case *syntax.MatchersExpr:
			return v
		case *syntax.PipelineExpr:
			return v
		case *syntax.LabelReplaceExpr:
			e = v.Left
		case *syntax.VectorAggregationExpr:
			e = v.Left
		case *syntax.RangeAggregationExpr:
			return v.Left.Left
		default:
			return nil
		}
	}
}

func stageOf(st syntax.StageExpr) Stage {
	switch v := st.(type) {
	case *syntax.LineParserExpr:
		return Stage{Kind: v.Op}
	case *syntax.LogfmtParserExpr, *syntax.LogfmtExpressionParserExpr:
		return Stage{Kind: "logfmt"}
	case *syntax.JSONExpressionParserExpr:
		return Stage{Kind: "json"}
	case *syntax.DropLabelsExpr:
		return Stage{Kind: "drop", Names: v.Names(), Conditional: v.HasNamedMatchers()}
	}
	return Stage{Kind: "other"}
}
