// Package metricparity contains the shared Loki metric contract used by unit and live tests.
package metricparity

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// DecodeSamples validates matrix/vector samples and preserves exact decimal coordinates.
func DecodeSamples(resp map[string]interface{}) (map[string]map[string]string, error) {
	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("metric response data is not an object")
	}
	kind, ok := data["resultType"].(string)
	if !ok || (kind != "matrix" && kind != "vector") {
		return nil, fmt.Errorf("unsupported metric resultType %v", data["resultType"])
	}
	result, ok := data["result"].([]interface{})
	if !ok || result == nil {
		return nil, fmt.Errorf("metric result is not an array")
	}
	out := make(map[string]map[string]string, len(result))
	for i, raw := range result {
		series, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("series %d is not an object", i)
		}
		metric, ok := series["metric"].(map[string]interface{})
		if !ok || metric == nil {
			return nil, fmt.Errorf("series %d metric is not an object", i)
		}
		for name, value := range metric {
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("series %d label %q is not a string", i, name)
			}
		}
		encoded, err := json.Marshal(metric)
		if err != nil {
			return nil, fmt.Errorf("series %d labels: %w", i, err)
		}
		key := string(encoded)
		if _, duplicate := out[key]; duplicate {
			return nil, fmt.Errorf("duplicate metric series %s", key)
		}
		var values []interface{}
		if kind == "matrix" {
			values, ok = series["values"].([]interface{})
			if !ok || values == nil {
				return nil, fmt.Errorf("series %s values is not an array", key)
			}
		} else {
			values = []interface{}{series["value"]}
		}
		points := make(map[string]string, len(values))
		for j, raw := range values {
			pair, ok := raw.([]interface{})
			if !ok || len(pair) != 2 {
				return nil, fmt.Errorf("series %s sample %d is not a timestamp/value pair", key, j)
			}
			ts, err := metricSampleTimestamp(pair[0])
			if err != nil {
				return nil, fmt.Errorf("series %s sample %d: %w", key, j, err)
			}
			value, ok := pair[1].(string)
			if !ok {
				return nil, fmt.Errorf("series %s t=%s value is not a string", key, ts)
			}
			if _, err := strconv.ParseFloat(value, 64); err != nil {
				return nil, fmt.Errorf("series %s t=%s invalid value %q", key, ts, value)
			}
			if _, duplicate := points[ts]; duplicate {
				return nil, fmt.Errorf("series %s duplicate timestamp %s", key, ts)
			}
			points[ts] = value
		}
		out[key] = points
	}
	return out, nil
}

// Preserve decimal coordinates from UseNumber without rounding them through a
// float64. Default JSON decoding has already rounded its float64 coordinates;
// retain all of that available precision too. Decimal keys keep older helpers
// that parse sample times numerically compatible.
func metricSampleTimestamp(raw interface{}) (string, error) {
	var text string
	switch v := raw.(type) {
	case float64:
		text = strconv.FormatFloat(v, 'f', -1, 64)
	case json.Number:
		text = v.String()
	default:
		return "", fmt.Errorf("timestamp is not a number: %v", raw)
	}
	// Bound decimal expansion even for a malformed backend timestamp such as
	// 1e-1000000000. Real Loki coordinates have at most nanosecond precision.
	if len(text) > 1024 {
		return "", fmt.Errorf("timestamp exceeds precision bound")
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || !json.Valid([]byte(text)) {
		return "", fmt.Errorf("invalid timestamp %q", text)
	}
	mantissa, exponent, _ := strings.Cut(strings.ToLower(text), "e")
	decimalPlaces := 0
	if _, fractional, ok := strings.Cut(mantissa, "."); ok {
		decimalPlaces = len(fractional)
	}
	if exponent != "" {
		e, err := strconv.Atoi(exponent)
		if err != nil || e < -1024 || e > 1024 {
			return "", fmt.Errorf("invalid timestamp exponent %q", exponent)
		}
		decimalPlaces -= e
	}
	if decimalPlaces < 0 {
		decimalPlaces = 0
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return "", fmt.Errorf("invalid timestamp %q", text)
	}
	if r.Sign() == 0 {
		return "0", nil
	}
	text = r.FloatString(decimalPlaces)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	return text, nil
}

// Difference compares every label set, timestamp and value under the shared parity contract.
func Difference(loki, proxy map[string]map[string]string, requireNonEmpty bool) error {
	points := 0
	for _, series := range loki {
		points += len(series)
	}
	if requireNonEmpty && points == 0 {
		return fmt.Errorf("loki returned no samples; parity would be empty-vs-empty")
	}
	keys := make([]string, 0, len(loki)+len(proxy))
	for key := range loki {
		keys = append(keys, key)
	}
	for key := range proxy {
		if _, exists := loki[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		want, inLoki := loki[key]
		got, inProxy := proxy[key]
		if !inLoki {
			return fmt.Errorf("proxy series %s absent in Loki", key)
		}
		if !inProxy {
			return fmt.Errorf("loki series %s missing in proxy", key)
		}
		times := make([]string, 0, len(want)+len(got))
		for ts := range want {
			times = append(times, ts)
		}
		for ts := range got {
			if _, exists := want[ts]; !exists {
				times = append(times, ts)
			}
		}
		sort.Strings(times)
		for _, ts := range times {
			wv, inLoki := want[ts]
			gv, inProxy := got[ts]
			if !inLoki || !inProxy {
				return fmt.Errorf("series %s t=%s: Loki %q (present=%v), proxy %q (present=%v)", key, ts, wv, inLoki, gv, inProxy)
			}
			w, we := strconv.ParseFloat(wv, 64)
			g, ge := strconv.ParseFloat(gv, 64)
			if we != nil || ge != nil {
				return fmt.Errorf("series %s t=%s: invalid numeric sample: Loki %q, proxy %q", key, ts, wv, gv)
			}
			equal := w == g || (math.IsNaN(w) && math.IsNaN(g))
			if !equal && !math.IsNaN(w) && !math.IsNaN(g) && !math.IsInf(w, 0) && !math.IsInf(g, 0) {
				equal = math.Abs(w-g) <= math.Max(1e-9, 1e-6*math.Max(math.Abs(w), math.Abs(g)))
			}
			if !equal {
				return fmt.Errorf("series %s t=%s: proxy %q, Loki %q", key, ts, gv, wv)
			}
		}
	}
	return nil
}
