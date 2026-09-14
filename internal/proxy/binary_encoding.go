package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"unicode/utf8"
)

// Compute encoding/json's quoted size without allocating its escaped copy.
func binaryJSONStringSize(ctx context.Context, value string) (int, error) {
	size := 2
	for i := 0; i < len(value); {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		c := value[i]
		if c < utf8.RuneSelf {
			switch c {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				size += 2
			case '<', '>', '&':
				size += 6
			default:
				if c < 0x20 {
					size += 6
				} else {
					size++
				}
			}
			i++
			continue
		}
		r, n := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && n == 1 {
			// Go 1.27 encoding/json emits the UTF-8 replacement character.
			size += 3
		} else if r == '\u2028' || r == '\u2029' {
			size += 6
		} else {
			size += n
		}
		i += n
	}
	return size, ctx.Err()
}

func checkBinaryOutputLabels(ctx context.Context, labels map[string]string) error {
	state := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
	for key, value := range labels {
		for _, text := range []string{key, value} {
			size, err := binaryJSONStringSize(ctx, text)
			if err != nil {
				return err
			}
			if size > state.budget.outputLabels {
				return fmt.Errorf("binary expression output label budget exceeded")
			}
			state.budget.outputLabels -= size
		}
	}
	return ctx.Err()
}

type binaryJSONEncoder struct {
	w       *binaryOperandResponse
	samples int
}

func (e *binaryJSONEncoder) raw(text string) { _, _ = e.w.Write([]byte(text)) }
func (e *binaryJSONEncoder) quoted(value string) {
	if e.w.err != nil {
		return
	}
	size, err := binaryJSONStringSize(e.w.ctx, value)
	if err != nil {
		e.w.err = err
		return
	}
	if size > e.w.limit-e.w.body.Len() || size > e.w.budget.bytes {
		e.w.err = fmt.Errorf("binary encoded response exceeds byte budget")
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		e.w.err = err
		return
	}
	_, _ = e.w.Write(encoded)
}

func (e *binaryJSONEncoder) metric(labels map[string]string) {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	e.raw("{")
	for i, key := range keys {
		if e.w.err != nil {
			return
		}
		if i > 0 {
			e.raw(",")
		}
		e.quoted(key)
		e.raw(":")
		e.quoted(labels[key])
	}
	e.raw("}")
}

func (e *binaryJSONEncoder) point(point []any) {
	if e.w.err != nil {
		return
	}
	if len(point) != 2 || e.samples >= 1000000 {
		e.w.err = fmt.Errorf("invalid or excessive binary output samples")
		return
	}
	e.samples++
	// Only numeric timestamps are encoded; arbitrary objects cannot allocate
	// an unbounded temporary JSON document here.
	switch point[0].(type) {
	case float64, int, int64:
	default:
		e.w.err = fmt.Errorf("invalid binary output timestamp")
		return
	}
	stamp, err := json.Marshal(point[0])
	if err != nil {
		e.w.err = err
		return
	}
	value, ok := point[1].(string)
	if !ok {
		e.w.err = fmt.Errorf("invalid binary output value")
		return
	}
	e.raw("[")
	_, _ = e.w.Write(stamp)
	e.raw(",")
	e.quoted(value)
	e.raw("]")
}

func encodeBinarySeriesContext(ctx context.Context, series map[string]*binaryMatchedSeries, resultType string, limit int) ([]byte, error) {
	ctx = binaryEvaluationContext(ctx)
	state := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
	w := &binaryOperandResponse{header: make(http.Header), ctx: ctx, budget: state.budget, limit: limit}
	e := binaryJSONEncoder{w: w}
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	e.raw(`{"status":"success","data":{"resultType":`)
	e.quoted(resultType)
	e.raw(`,"result":[`)
	for i, key := range keys {
		if w.err != nil {
			return nil, w.err
		}
		if i > 0 {
			e.raw(",")
		}
		s := series[key]
		e.raw(`{"metric":`)
		e.metric(s.labels)
		if resultType == "matrix" {
			e.raw(`,"values":[`)
			for j, point := range s.points {
				if w.err != nil {
					return nil, w.err
				}
				if j > 0 {
					e.raw(",")
				}
				e.point(point)
			}
			e.raw("]")
		} else {
			if len(s.points) == 0 {
				return nil, fmt.Errorf("missing binary output sample")
			}
			e.raw(`,"value":`)
			e.point(s.points[0])
		}
		e.raw("}")
	}
	e.raw("]}}")
	if w.err != nil {
		return nil, w.err
	}
	return w.body.Bytes(), nil
}
