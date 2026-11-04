package proxy

import (
	"context"
	"sync/atomic"
)

// matrixFold asks the bucket matrix builder (buildHitsRangeMetricMatrixFolded)
// to apply the vector aggregations around a range metric while it builds the
// matrix, instead of returning one series per Loki stream for the caller to
// fold (foldVectorAggregations).
//
// Loki evaluates `count by (g) (count_over_time(...))` per step: every stream's
// window value joins its group's count (pkg/logql/evaluator.go
// VectorAggEvaluator.Next) and the per-stream samples are gone after the step.
// Encoding every stream at every step first made the answer's size the
// streams x steps of the range and not the groups x steps of the answer: a
// `count`, `min`, `max` or `avg` reads one value per stream, so nothing could be
// added up before the window (series_level folds, vector_agg_fold.go), and 48h
// at a 120s step passed -backend-max-buffered-response-bytes with a 200 from
// VictoriaLogs. The sink folds one series at a time, so the builder holds the
// groups of the answer and one series' points.
//
// The result is the one foldLevels yields on the encoded matrix: the series
// are visited in the encoder's order, the points of a series in time order, and
// the accumulator is the one typedBody.fold uses.
type matrixFold struct {
	levels []foldLevel
	// folded is set by a builder that applied every level: the caller must not
	// fold the body again (a second `count` of a folded count answers 1).
	folded bool
}

type matrixFoldKey struct{}

// matrixFoldOff switches the fold in the builder off, for the tests that compare
// it with the fold of the encoded matrix.
var matrixFoldOff atomic.Bool

// withMatrixFold returns ctx carrying the levels a builder may fold, and the
// request state it reports back on.
func withMatrixFold(ctx context.Context, levels []foldLevel) (context.Context, *matrixFold) {
	fold := &matrixFold{levels: levels}
	return context.WithValue(ctx, matrixFoldKey{}, fold), fold
}

func matrixFoldFrom(ctx context.Context) *matrixFold {
	if matrixFoldOff.Load() {
		return nil
	}
	fold, _ := ctx.Value(matrixFoldKey{}).(*matrixFold)
	return fold
}

// matrixFoldSink folds series into the first level as they come; the later
// levels fold the groups, which are small.
type matrixFoldSink struct {
	acc *foldAccumulator
}

// newMatrixFoldSink returns the sink of fold, or nil when there is none.
func newMatrixFoldSink(fold *matrixFold) *matrixFoldSink {
	if fold == nil {
		return nil
	}
	return &matrixFoldSink{acc: newFoldAccumulator(fold.levels[0], false)}
}

// add folds one series; a series without a point is not in the matrix.
func (s *matrixFoldSink) add(metric map[string]string, points []typedPoint) {
	if len(points) > 0 {
		s.acc.add(metric, points)
	}
}

// finish applies the remaining levels and encodes the answer, which is held to
// the response bound like the matrix it replaces; fold is marked done.
func (s *matrixFoldSink) finish(fold *matrixFold, maxBytes int) ([]byte, error) {
	body := s.acc.result()
	for _, level := range fold.levels[1:] {
		body = body.fold(level)
	}
	encoded := body.encode()
	if len(encoded) > maxBytes {
		return nil, errSlidingResponseTooLarge(maxBytes)
	}
	fold.folded = true
	return encoded, nil
}
