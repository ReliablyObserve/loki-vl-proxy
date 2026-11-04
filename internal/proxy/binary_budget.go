package proxy

import (
	"context"
	"errors"
	"fmt"
)

// Children execute sequentially and share these aggregate work limits.
type binaryExecutionBudget struct {
	bytes         int
	arrays        int
	outputLabels  int
	outputSamples int
}

// binaryBudgetError is a binary expression over one of the evaluation
// budgets an operator sets (-binary-metric-max-arrays,
// -binary-metric-max-operand-bytes): 400 bad_data naming the flag, as Loki
// answers a query over one of its limits (logqlmodel.ErrLimit,
// pkg/util/server/error.go:110-118, v3.7.9).
type binaryBudgetError struct {
	flag string
}

func (e *binaryBudgetError) Error() string {
	return "binary expression exceeds its evaluation budget" + proxyLimitMarker + e.flag
}

func isBinaryBudgetError(err error) bool {
	var budgetErr *binaryBudgetError
	return errors.As(err, &budgetErr)
}

func binaryEvaluationContext(ctx context.Context) context.Context {
	state, _ := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
	if state.remaining == nil {
		remaining := 1024
		state.remaining = &remaining
	}
	if state.budget == nil {
		limits := executionLimitsFrom(ctx)
		state.budget = &binaryExecutionBudget{bytes: limits.BinaryOperandBytes, arrays: limits.BinaryArrays, outputLabels: limits.BufferedBackendBodyBytes, outputSamples: 1000000}
	}
	return context.WithValue(ctx, binaryEvaluationKey{}, state)
}

func checkBinaryOutputSample(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
	if state.budget.outputSamples <= 0 {
		return fmt.Errorf("binary expression output sample budget exceeded")
	}
	state.budget.outputSamples--
	return nil
}

// Count array allocations before JSON unmarshalling. Every sample needs an
// array; matrix series add one more. Counting both conservatively bounds all
// sample/series allocations across nested operands without first allocating
// the decoded response. Brackets inside label/value strings do not count.
func checkBinaryDecodeBudget(ctx context.Context, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if limit := executionLimitsFrom(ctx).BufferedBackendBodyBytes; len(body) > limit {
		return fmt.Errorf("binary operand exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", limit)
	}
	state, _ := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
	budget := state.budget
	if budget == nil {
		budget = &binaryExecutionBudget{arrays: executionLimitsFrom(ctx).BinaryArrays}
	}
	quoted, escaped := false, false
	for i, c := range body {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			continue
		}
		if c == '[' {
			if budget.arrays <= 0 {
				return &binaryBudgetError{flag: "binary-metric-max-arrays"}
			}
			budget.arrays--
		}
	}
	return ctx.Err()
}
