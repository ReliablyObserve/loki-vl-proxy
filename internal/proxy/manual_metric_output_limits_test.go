package proxy

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestManualMetricOutputSeriesLimitFailsClosed(t *testing.T) {
	stamp := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	series := map[string]manualSeriesSamples{
		"a": {Metric: map[string]string{"app": "first"}, Samples: []rangeMetricSample{{ts: stamp.UnixNano(), value: 2}}},
		"b": {Metric: map[string]string{"app": "second"}, Samples: []rangeMetricSample{{ts: stamp.UnixNano(), value: 4}}},
	}
	for _, kind := range []string{"matrix", "vector"} {
		build := func(ctx context.Context, limit int) ([]byte, error) {
			if kind == "matrix" {
				return buildManualRangeMetricMatrixContext(ctx, "sum", 0, series, stamp, stamp, time.Minute, time.Minute, limit)
			}
			return buildManualRangeMetricVectorContext(ctx, "sum", 0, series, stamp, time.Minute, limit)
		}
		t.Run(kind, func(t *testing.T) {
			body, err := build(t.Context(), 2)
			var response struct {
				Data struct {
					Result []struct {
						Metric map[string]string
						Value  []any
						Values [][]any
					}
				}
			}
			if err != nil || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 2 {
				t.Fatalf("exact series cap rejected: %s %v", body, err)
			}
			values := map[string]string{}
			for _, entry := range response.Data.Result {
				point := entry.Value
				if kind == "matrix" {
					if len(entry.Values) != 1 {
						t.Fatalf("wrong matrix points %s", body)
					}
					point = entry.Values[0]
				}
				if len(point) != 2 || point[0] != float64(stamp.Unix()) {
					t.Fatalf("wrong point %s", body)
				}
				values[entry.Metric["app"]] = point[1].(string)
			}
			if values["first"] != "2" || values["second"] != "4" {
				t.Fatalf("samples changed: %s", body)
			}
			if partial, err := build(t.Context(), 1); err == nil || !strings.Contains(err.Error(), "maximum number of series (1) reached") || partial != nil {
				t.Fatalf("series were silently discarded: %s %v", partial, err)
			}
			ctx := binaryEvaluationContext(t.Context())
			ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.bytes = len(body)
			if exact, err := build(ctx, 2); err != nil || string(exact) != string(body) {
				t.Fatalf("exact byte cap rejected: %v", err)
			}
			ctx = binaryEvaluationContext(t.Context())
			ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.bytes = len(body) - 1
			if partial, err := build(ctx, 2); err == nil || partial != nil {
				t.Fatalf("byte cap returned partial success: %s %v", partial, err)
			}
			ctx = binaryEvaluationContext(t.Context())
			ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.outputSamples = 1
			if partial, err := build(ctx, 2); err == nil || !strings.Contains(err.Error(), "output sample budget") || partial != nil {
				t.Fatalf("sample cap returned partial success: %s %v", partial, err)
			}
		})
	}
}

func TestManualMetricOutputExtremeFloatsStayBoundedAndExact(t *testing.T) {
	stamp := time.Now()
	for _, value := range []float64{math.MaxFloat64, math.SmallestNonzeroFloat64} {
		series := map[string]manualSeriesSamples{"a": {Metric: map[string]string{}, Samples: []rangeMetricSample{{ts: stamp.UnixNano(), value: value}}}}
		body, err := buildManualRangeMetricVectorContext(t.Context(), "sum", 0, series, stamp, time.Minute, 1)
		var response struct {
			Data struct{ Result []struct{ Value []any } }
		}
		if err != nil || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 {
			t.Fatalf("failed to encode float: %s %v", body, err)
		}
		encoded := response.Data.Result[0].Value[1].(string)
		got, err := strconv.ParseFloat(encoded, 64)
		// Loki renders values with model.SampleValue.String (FormatFloat 'f', -1):
		// extreme magnitudes expand to fixed-point digits but stay exact. Total
		// response size is bounded by the encoder's byte cap, not per value.
		if err != nil || got != value || encoded != strconv.FormatFloat(value, 'f', -1, 64) {
			t.Fatalf("float changed or not in Loki's form: %g => %s => %g (%v)", value, encoded, got, err)
		}
	}
}

func TestManualMetricMatrixRejectsInvalidEvaluationAxis(t *testing.T) {
	stamp := time.Now()
	for _, step := range []time.Duration{0, -time.Second, time.Nanosecond} {
		body, err := buildManualRangeMetricMatrixContext(t.Context(), "count_over_time", 0, nil, stamp, stamp.Add(time.Second), step, time.Minute, 500)
		if err == nil || body != nil {
			t.Fatalf("invalid/excessive axis accepted: step=%s %s %v", step, body, err)
		}
	}
}
