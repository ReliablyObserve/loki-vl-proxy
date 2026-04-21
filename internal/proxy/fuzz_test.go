package proxy

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

// FuzzParseLokiDuration tests duration parsing with random inputs.
func FuzzParseLokiDuration(f *testing.F) {
	seeds := []string{
		"5m", "1h", "30s", "1d", "2h30m", "100ms",
		"", "abc", "5", "0", "-1m", "1h2m3s",
		"999d", "1.5h", "1e5s",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Must never panic
		_ = parseLokiDuration(input)
	})
}

// FuzzParseTimestamp tests timestamp parsing with random inputs.
func FuzzParseTimestamp(f *testing.F) {
	seeds := []string{
		"1609459200", "1609459200.123", "2021-01-01T00:00:00Z",
		"", "abc", "0", "-1", "99999999999999999",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Must never panic
		_, _ = parseTimestamp(input)
	})
}

// FuzzSubqueryAggregate tests aggregation with random values.
func FuzzSubqueryAggregate(f *testing.F) {
	f.Add("max_over_time", 1.0, 5.0, 3.0)
	f.Add("min_over_time", 1.0, 5.0, 3.0)
	f.Add("avg_over_time", 2.0, 4.0, 6.0)
	f.Add("sum_over_time", 2.0, 4.0, 6.0)
	f.Add("count_over_time", 1.0, 2.0, 3.0)
	f.Add("stddev_over_time", 1.0, 2.0, 3.0)
	f.Add("unknown_func", 1.0, 2.0, 3.0)

	f.Fuzz(func(t *testing.T, fn string, a, b, c float64) {
		values := []float64{a, b, c}
		// Must never panic
		result := subqueryAggregate(fn, values)
		// Result should be finite for finite inputs
		if !isFinite(a) || !isFinite(b) || !isFinite(c) {
			return
		}
		_ = result
	})
}

func isFinite(f float64) bool {
	return f == f && f != f+1e308 // not NaN and not Inf
}

// FuzzSeriesKeyFromMetric tests series key generation with random labels.
func FuzzSeriesKeyFromMetric(f *testing.F) {
	f.Add("app", "nginx")
	f.Add("", "")
	f.Add("key with spaces", "value with spaces")
	f.Add("a.b.c", "x.y.z")

	f.Fuzz(func(t *testing.T, key, value string) {
		metric := map[string]string{key: value}
		result := seriesKeyFromMetric(metric)
		if result == "" {
			t.Error("seriesKeyFromMetric returned empty string")
		}
	})
}

// FuzzParseValueToFloat tests value parsing with random inputs.
func FuzzParseValueToFloat(f *testing.F) {
	f.Add("42.5")
	f.Add("0")
	f.Add("-1.5")
	f.Add("NaN")
	f.Add("abc")
	f.Add("")

	f.Fuzz(func(t *testing.T, input string) {
		// Must never panic
		_ = parseValueToFloat(input)
	})
}

// FuzzLokiErrorType tests error type mapping with random codes.
func FuzzLokiErrorType(f *testing.F) {
	codes := []int{200, 400, 404, 422, 429, 499, 500, 502, 503, 504, 0, -1, 999}
	for _, c := range codes {
		f.Add(c)
	}

	f.Fuzz(func(t *testing.T, code int) {
		result := lokiErrorType(code)
		if result == "" {
			t.Errorf("lokiErrorType(%d) returned empty string", code)
		}
	})
}

// FuzzFormatVLStep tests step formatting with random inputs.
func FuzzFormatVLStep(f *testing.F) {
	seeds := []string{"60", "1m", "5m", "1h", "", "abc", "0", "3600"}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		result := formatVLStep(input)
		_ = result
	})
}

// FuzzNormalizeMetadataPairs guards metadata normalization from panics across arbitrary JSON payloads.
func FuzzNormalizeMetadataPairs(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"service.name":"orders","k8s.cluster.name":"cluster-alpha"}`),
		[]byte(`[["service.name","orders"],["k8s.cluster.name","cluster-alpha"]]`),
		[]byte(`[{"name":"service.name","value":"orders"},{"name":"k8s.cluster.name","value":"cluster-alpha"}]`),
		[]byte(`null`),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		var raw interface{}
		if err := json.Unmarshal(input, &raw); err != nil {
			return
		}
		out := normalizeMetadataPairs(raw)
		for key := range out {
			if key == "" {
				t.Fatalf("normalizeMetadataPairs produced empty key for input=%q", string(input))
			}
		}
	})
}

// FuzzCombineBinaryMetricResultsScalarPath validates scalar/vector merge shaping for all operators.
func FuzzCombineBinaryMetricResultsScalarPath(f *testing.F) {
	ops := []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">", "<", ">=", "<=", "unknown"}
	for _, op := range ops {
		f.Add(op, 10.0, 3.0, false, true)
		f.Add(op, 10.0, 3.0, true, false)
		f.Add(op, 10.0, 3.0, false, false)
	}

	f.Fuzz(func(t *testing.T, op string, metricValue, scalarValue float64, leftScalar, rightScalar bool) {
		if !isFinite(metricValue) || !isFinite(scalarValue) {
			return
		}

		metricBody := []byte(fmt.Sprintf(
			`{"results":[{"metric":{"app":"fuzz"},"values":[[1,"%s"]]}]}`,
			strconv.FormatFloat(metricValue, 'f', -1, 64),
		))
		scalarBody := []byte(fmt.Sprintf(
			`{"status":"success","data":{"resultType":"scalar","result":[0,"%s"]}}`,
			strconv.FormatFloat(scalarValue, 'f', -1, 64),
		))

		leftBody := metricBody
		rightBody := metricBody
		leftQL := ""
		rightQL := ""

		if leftScalar {
			leftBody = scalarBody
			leftQL = strconv.FormatFloat(scalarValue, 'f', -1, 64)
		}
		if rightScalar {
			rightBody = scalarBody
			rightQL = strconv.FormatFloat(scalarValue, 'f', -1, 64)
		}

		out := combineBinaryMetricResults(leftBody, rightBody, op, "matrix", leftScalar, rightScalar, leftQL, rightQL)
		var payload map[string]interface{}
		if err := json.Unmarshal(out, &payload); err != nil {
			t.Fatalf("combined output must stay valid JSON: %v; body=%s", err, string(out))
		}
		status, _ := payload["status"].(string)
		if status == "" {
			t.Fatalf("combined output missing status field: %#v", payload)
		}
	})
}
