package proxy

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"
)

func (p *Proxy) manualMetricRowBudget() (int, error) {
	limit := p.rangeMetricRowLimit
	if limit <= 0 {
		limit = 1_000_000
	}
	if limit == math.MaxInt {
		return 0, fmt.Errorf("manual range metric row limit is too large")
	}
	return limit, nil
}

func checkManualMetricRead(ctx context.Context, limited *io.LimitedReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if limited.N <= 0 {
		return fmt.Errorf("manual metric response exceeds %d bytes", maxBufferedBackendBodyBytes)
	}
	return nil
}

func bareParserRawSampleWeight(entry map[string]interface{}, spec bareParserMetricCompatSpec) (float64, bool) {
	if spec.unwrapField != "" {
		value, ok := stringifyEntryValue(entry[spec.unwrapField])
		if !ok {
			return 0, false
		}
		// unwrap duration(f) / bytes(f) carry unit strings such as "15ms" or
		// "1024B"; a plain float parse would drop every sample and return no
		// series where Loki returns the converted values.
		return convertUnwrapValue(value, spec.unwrapConv)
	}
	if spec.funcName == "bytes_over_time" || spec.funcName == "bytes_rate" {
		msg, _ := stringifyEntryValue(entry["_msg"])
		return float64(len(msg)), true
	}
	return 1, true
}

// Use the bounded metric encoder for raw parser results too. Enforce the
// evaluation and sample limits before allocating each point, rather than
// building an arbitrarily large document and checking its serialized size.
func buildBoundedBareParserMetric(ctx context.Context, series []bareParserMetricSeries, start, end, step int64, spec bareParserMetricCompatSpec, isRange bool) ([]byte, error) {
	distance := time.Unix(0, end).Sub(time.Unix(0, start))
	if step <= 0 || end < start || distance == time.Duration(math.MaxInt64) || distance/time.Duration(step) >= maxSubquerySamples {
		return nil, fmt.Errorf("invalid or excessive manual metric evaluation points")
	}
	ctx = binaryEvaluationContext(ctx)
	result := make(map[string]*binaryMatchedSeries, len(series))
	for index, entry := range series {
		if err := checkBinaryOutputLabels(ctx, entry.metric); err != nil {
			return nil, err
		}
		out := &binaryMatchedSeries{labels: entry.metric}
		left, right := 0, 0
		for evaluation := start; evaluation <= end; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for right < len(entry.samples) && entry.samples[right].tsNanos <= evaluation {
				right++
			}
			for left < right && entry.samples[left].tsNanos <= evaluation-int64(spec.rangeWindow) {
				left++
			}
			if right > left {
				if err := checkBinaryOutputSample(ctx); err != nil {
					return nil, err
				}
				value := bareParserMetricWindowValue(spec.funcName, entry.samples[left:right], spec)
				out.points = append(out.points, []any{float64(evaluation) / float64(time.Second), strconv.FormatFloat(value, 'f', -1, 64)})
			}
			if end-evaluation < step {
				break
			}
			evaluation += step
		}
		if len(out.points) > 0 {
			result[strconv.Itoa(index)] = out
		}
	}
	resultType := "vector"
	if isRange {
		resultType = "matrix"
	}
	return encodeBinarySeriesContext(ctx, result, resultType, maxBufferedBackendBodyBytes)
}

func (p *Proxy) writeBoundedBareParserMetric(w http.ResponseWriter, r *http.Request, requestStart time.Time, query string, series []bareParserMetricSeries, start, end, step int64, spec bareParserMetricCompatSpec, isRange bool) {
	body, err := buildBoundedBareParserMetric(r.Context(), series, start, end, step, spec, isRange)
	status := http.StatusOK
	if err != nil {
		status = statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
	endpoint := "query"
	if isRange {
		endpoint = "query_range"
	}
	elapsed := time.Since(requestStart)
	p.metrics.RecordRequest(endpoint, status, elapsed)
	p.queryTracker.Record(endpoint, query, elapsed, err != nil)
}
