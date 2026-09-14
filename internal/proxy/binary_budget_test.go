package proxy

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestBinaryBudgetChecksBeforeDecodeAndAcrossChildren(t *testing.T) {
	ctx := binaryEvaluationContext(context.Background())
	state := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
	state.budget.arrays = 2
	if err := checkBinaryDecodeBudget(ctx, []byte(`{"value":"[[[", "result":[]}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := matchBinaryMetricResultsContext(ctx, []byte(`[[[`), nil, "+", "matrix", nil, false); err == nil || !strings.Contains(err.Error(), "sample/series budget") {
		t.Fatalf("expected budget rejection before malformed JSON decoding: %v", err)
	}
	state.budget.bytes = 5
	first := &binaryOperandResponse{header: make(http.Header), limit: 64, budget: state.budget}
	second := &binaryOperandResponse{header: make(http.Header), limit: 64, budget: state.budget}
	if _, err := first.Write([]byte("123")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("456")); err == nil || second.body.Len() != 0 || state.budget.bytes != 2 {
		t.Fatal("aggregate byte budget bypassed")
	}
}

func TestBinaryMatcherCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateVectorMatchCardinalityContext(ctx, nil, nil, nil, nil, false, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cardinality ignored cancellation: %v", err)
	}
	if _, err := matchBinaryMetricResultsContext(ctx, nil, nil, "+", "matrix", nil, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("matcher ignored cancellation: %v", err)
	}
	if _, err := matchBinaryScalarContext(ctx, nil, 1, false, "+", "vector", false); !errors.Is(err, context.Canceled) {
		t.Fatalf("scalar ignored cancellation: %v", err)
	}
	w := &binaryOperandResponse{header: make(http.Header), ctx: ctx, limit: 64}
	if _, err := w.Write([]byte("payload")); !errors.Is(err, context.Canceled) || w.body.Len() != 0 {
		t.Fatal("cancelled child response was buffered")
	}
}
