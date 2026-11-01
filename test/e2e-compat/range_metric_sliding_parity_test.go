//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func decodeMetricSamples(resp map[string]interface{}) (map[string]map[string]string, error) {
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

func metricSamplesDifference(loki, proxy map[string]map[string]string, requireNonEmpty bool) error {
	points := 0
	for _, series := range loki {
		points += len(series)
	}
	if requireNonEmpty && points == 0 {
		return fmt.Errorf("Loki returned no samples; parity would be empty-vs-empty")
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
			return fmt.Errorf("Loki series %s missing in proxy", key)
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

func TestRangeMetricCompatibilityDecodeSamples(t *testing.T) {
	decode := func(body string) (map[string]map[string]string, error) {
		var response map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&response); err != nil {
			t.Fatal(err)
		}
		return decodeMetricSamples(response)
	}
	t.Run("matrix_and_vector", func(t *testing.T) {
		got, err := decode(`{"data":{"resultType":"matrix","result":[{"metric":{"a":"b,c=d"},"values":[[1.125,"0"],[1.25,"-2"]]},{"metric":{"c":"d","a":"b"},"values":[[1.125,"+Inf"]]}]}}`)
		if err != nil || len(got) != 2 || !reflect.DeepEqual(got[`{"a":"b,c=d"}`], map[string]string{"1.125": "0", "1.25": "-2"}) {
			t.Fatalf("identities/axis: %v, %v", got, err)
		}
		got, err = decode(`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000.000000001,"NaN"]}]}}`)
		if err != nil || got["{}"]["1700000000.000000001"] != "NaN" {
			t.Fatalf("precise vector: %v, %v", got, err)
		}
		got, err = decode(`{"data":{"resultType":"matrix","result":[]}}`)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("empty result: %v, %v", got, err)
		}
	})
	for name, fragment := range map[string]string{
		"missing_result": `"resultType":"matrix"`, "null_result": `"resultType":"matrix","result":null`,
		"object_result": `"resultType":"matrix","result":{}`, "unsupported_type": `"resultType":"streams","result":[]`,
		"null_series":          `"resultType":"matrix","result":[null]`,
		"missing_metric":       `"resultType":"matrix","result":[{"values":[]}]`,
		"null_metric":          `"resultType":"matrix","result":[{"metric":null,"values":[]}]`,
		"number_label":         `"resultType":"matrix","result":[{"metric":{"a":1},"values":[]}]`,
		"bool_label":           `"resultType":"matrix","result":[{"metric":{"a":true},"values":[]}]`,
		"missing_values":       `"resultType":"matrix","result":[{"metric":{}}]`,
		"null_values":          `"resultType":"matrix","result":[{"metric":{},"values":null}]`,
		"object_values":        `"resultType":"matrix","result":[{"metric":{},"values":{}}]`,
		"missing_vector_value": `"resultType":"vector","result":[{"metric":{}}]`,
		"not_tuple":            `"resultType":"matrix","result":[{"metric":{},"values":[1]}]`,
		"short_tuple":          `"resultType":"matrix","result":[{"metric":{},"values":[[1]]}]`,
		"long_tuple":           `"resultType":"vector","result":[{"metric":{},"value":[1,"1",2]}]`,
		"string_time":          `"resultType":"vector","result":[{"metric":{},"value":["1","1"]}]`,
		"null_time":            `"resultType":"vector","result":[{"metric":{},"value":[null,"1"]}]`,
		"number_value":         `"resultType":"vector","result":[{"metric":{},"value":[1,1]}]`,
		"bool_value":           `"resultType":"vector","result":[{"metric":{},"value":[1,true]}]`,
		"invalid_value":        `"resultType":"vector","result":[{"metric":{},"value":[1,"no"]}]`,
		"duplicate_time":       `"resultType":"matrix","result":[{"metric":{},"values":[[1.25,"1"],[1.250,"2"]]}]`,
		"duplicate_zero":       `"resultType":"matrix","result":[{"metric":{},"values":[[-0,"1"],[0,"2"]]}]`,
		"duplicate_exponent":   `"resultType":"matrix","result":[{"metric":{},"values":[[1.25e2,"1"],[125,"2"]]}]`,
		"duplicate_series":     `"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[]},{"metric":{"c":"d","a":"b"},"values":[]}]`,
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := decode(`{"data":{` + fragment + `}}`); err == nil {
				t.Fatalf("malformed result accepted: %v", got)
			}
		})
	}
	for name, response := range map[string]map[string]interface{}{"nil_response": nil, "missing_data": {}, "null_data": {"data": nil}, "array_data": {"data": []interface{}{}}} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeMetricSamples(response); err == nil {
				t.Fatal("malformed data accepted")
			}
		})
	}
	for name, timestamp := range map[string]interface{}{"nan": math.NaN(), "inf": math.Inf(1), "negative_inf": math.Inf(-1), "invalid_number": json.Number("oops"), "bool": true, "unbounded_precision": json.Number("1e-1000000000")} {
		t.Run(name, func(t *testing.T) {
			response := map[string]interface{}{"data": map[string]interface{}{"resultType": "vector", "result": []interface{}{map[string]interface{}{"metric": map[string]interface{}{}, "value": []interface{}{timestamp, "1"}}}}}
			if _, err := decodeMetricSamples(response); err == nil {
				t.Fatal("invalid timestamp accepted")
			}
		})
	}
	t.Run("float_and_negative_zero", func(t *testing.T) {
		response := map[string]interface{}{"data": map[string]interface{}{"resultType": "vector", "result": []interface{}{map[string]interface{}{"metric": map[string]interface{}{"a": ""}, "value": []interface{}{math.Copysign(0, -1), "-Inf"}}}}}
		got, err := decodeMetricSamples(response)
		if err != nil || got[`{"a":""}`]["0"] != "-Inf" {
			t.Fatalf("negative zero/empty label: %v, %v", got, err)
		}
	})
}

func TestRangeMetricCompatibilitySampleDifference(t *testing.T) {
	type samples = map[string]map[string]string
	for _, tc := range []struct {
		name                string
		loki, proxy         samples
		nonempty, wantError bool
	}{
		{"formats", samples{"{}": {"1": "1.000", "2": "-2", "3": "0"}}, samples{"{}": {"1": "1e0", "2": "-2.0", "3": "-0"}}, true, false},
		{"relative_tolerance", samples{"{}": {"1": "1000000"}}, samples{"{}": {"1": "1000000.5"}}, true, false},
		{"absolute_tolerance", samples{"{}": {"1": "0"}}, samples{"{}": {"1": "5e-10"}}, true, false},
		{"beyond_tolerance", samples{"{}": {"1": "1"}}, samples{"{}": {"1": "1.00001"}}, true, true},
		{"swapped_equal_total", samples{"{}": {"1": "2", "2": "4"}}, samples{"{}": {"1": "4", "2": "2"}}, true, true},
		{"zero_to_nine", samples{"{}": {"1": "0"}}, samples{"{}": {"1": "9"}}, true, true},
		{"subsecond_shift", samples{"{}": {"1.125": "1"}}, samples{"{}": {"1.25": "1"}}, true, true},
		{"missing_point", samples{"{}": {"1": "1", "2": "2"}}, samples{"{}": {"1": "1"}}, true, true},
		{"extra_point", samples{"{}": {"1": "1"}}, samples{"{}": {"1": "1", "2": "2"}}, true, true},
		{"missing_series", samples{"a": {"1": "1"}, "b": {"1": "2"}}, samples{"a": {"1": "1"}}, true, true},
		{"extra_series", samples{"a": {"1": "1"}}, samples{"a": {"1": "1"}, "b": {"1": "2"}}, true, true},
		{"special_values", samples{"{}": {"1": "NaN", "2": "+Inf", "3": "-Inf"}}, samples{"{}": {"1": "NaN", "2": "Inf", "3": "-Inf"}}, true, false},
		{"infinity_sign", samples{"{}": {"1": "+Inf"}}, samples{"{}": {"1": "-Inf"}}, true, true},
		{"nan_finite", samples{"{}": {"1": "NaN"}}, samples{"{}": {"1": "1"}}, true, true},
		{"inf_finite", samples{"{}": {"1": "+Inf"}}, samples{"{}": {"1": "1e300"}}, true, true},
		{"invalid_number", samples{"{}": {"1": "bad"}}, samples{"{}": {"1": "bad"}}, true, true},
		{"empty_allowed", samples{}, samples{}, false, false},
		{"empty_forbidden", samples{}, samples{}, true, true},
		{"empty_series_forbidden", samples{"{}": {}}, samples{"{}": {}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := metricSamplesDifference(tc.loki, tc.proxy, tc.nonempty); (err != nil) != tc.wantError {
				t.Fatalf("difference=%v, want error=%v", err, tc.wantError)
			}
		})
	}
	t.Run("deterministic_diagnostic", func(t *testing.T) {
		loki, proxy := samples{"z": {"2": "4"}, "a": {"1": "2"}}, samples{"z": {"2": "9"}, "a": {"1": "8"}}
		var first string
		for i := 0; i < 20; i++ {
			err := metricSamplesDifference(loki, proxy, true)
			if err == nil {
				t.Fatal("wrong values accepted")
			}
			if i == 0 {
				first = err.Error()
			} else if first != err.Error() {
				t.Fatalf("unstable diagnostics: %s / %s", first, err)
			}
		}
	})
}

type slidingLiveLine struct {
	ts  time.Time
	msg string
}

type slidingLiveFixture struct {
	app   string
	lines []slidingLiveLine
	// service names the stream with a service_name label and stores
	// detected_level=unknown on both backends (Loki structured metadata, a
	// VictoriaLogs stream field), so bare range metrics, which keep every
	// label, have identical label sets. Otherwise the stream label is app.
	service bool
	// derivedLevel leaves detected_level out of both ingests, so each backend
	// derives it from the line (Loki at ingest, the proxy on read), which is how
	// an ordinary client's data looks. Only with service.
	derivedLevel bool
}

// streamLabels returns the fixture's stream labels as pushed to Loki.
func (fx slidingLiveFixture) streamLabels() map[string]string {
	if fx.service {
		return map[string]string{"service_name": fx.app}
	}
	return map[string]string{"app": fx.app}
}

// selector returns the LogQL stream selector for the fixture.
func (fx slidingLiveFixture) selector() string {
	for name, value := range fx.streamLabels() {
		return `{` + name + `="` + value + `"}`
	}
	return ""
}

type slidingLiveFixtures struct {
	gap, edge                        slidingLiveFixture
	bareLogfmt, bareJSON, bareUnwrap slidingLiveFixture
	hour0, s0                        time.Time
}

var (
	slidingFixturesOnce sync.Once
	slidingFixtures     *slidingLiveFixtures
)

// ensureSlidingFixtures ingests the sliding fixtures once per test binary,
// so every test shares one flush and one readiness poll.
func ensureSlidingFixtures(t *testing.T) *slidingLiveFixtures {
	t.Helper()
	slidingFixturesOnce.Do(func() {
		now := time.Now()
		fx := &slidingLiveFixtures{
			gap:        slidingLiveFixture{app: fmt.Sprintf("sliding-gap-%d", now.UnixNano())},
			edge:       slidingLiveFixture{app: fmt.Sprintf("sliding-edge-%d", now.UnixNano())},
			bareLogfmt: slidingLiveFixture{app: fmt.Sprintf("sliding-bare-logfmt-%d", now.UnixNano()), service: true},
			bareJSON:   slidingLiveFixture{app: fmt.Sprintf("sliding-bare-json-%d", now.UnixNano()), service: true},
			bareUnwrap: slidingLiveFixture{app: fmt.Sprintf("sliding-bare-unwrap-%d", now.UnixNano()), service: true},
			hour0:      now.Add(-20 * time.Hour).Truncate(time.Hour),
			s0:         now.Add(-10 * time.Hour).Truncate(time.Hour),
		}
		for _, h := range []int{0, 5} {
			for i := 0; i < 60; i++ {
				ts := fx.hour0.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Minute + 7*time.Second)
				fx.gap.lines = append(fx.gap.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("gap line %d %s", h*100+i, strings.Repeat("x", i%7))})
			}
		}
		for i := 0; i < 180; i++ { // one line every 10s for 30 minutes, on whole 10s marks
			ts := fx.s0.Add(time.Duration(i) * 10 * time.Second)
			fx.edge.lines = append(fx.edge.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("edge line %03d", i)})
			// Bare parser metrics keep parsed labels, so every line parses to the
			// same label set while line sizes vary: a logfmt key without a value
			// adds no label, and JSON whitespace adds no field.
			pad := strings.Repeat("x", i%4)
			fx.bareLogfmt.lines = append(fx.bareLogfmt.lines, slidingLiveLine{ts: ts, msg: "tick" + pad})
			fx.bareJSON.lines = append(fx.bareJSON.lines, slidingLiveLine{ts: ts, msg: `{` + strings.Repeat(" ", i%4) + `"msg":"tick"}`})
			fx.bareUnwrap.lines = append(fx.bareUnwrap.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("n=%d", i%7)})
		}
		ingestSlidingFixtures(t, fx.gap, fx.edge, fx.bareLogfmt, fx.bareJSON, fx.bareUnwrap)
		slidingFixtures = fx
	})
	if slidingFixtures == nil {
		t.Fatal("sliding fixtures were not ingested; see the first test that ran")
	}
	return slidingFixtures
}

// ingestSlidingFixtures writes byte-identical lines with one stream label per
// fixture into Loki and VictoriaLogs, flushes both, and proves both backends
// hold exactly every fixture before any comparison.
func ingestSlidingFixtures(t *testing.T, fixtures ...slidingLiveFixture) {
	t.Helper()
	var vlRows strings.Builder
	streams := make([]any, 0, len(fixtures))
	for _, fx := range fixtures {
		values := make([][]any, 0, len(fx.lines))
		for _, line := range fx.lines {
			fields := map[string]string{"_time": line.ts.UTC().Format(time.RFC3339Nano), "_msg": line.msg}
			for name, value := range fx.streamLabels() {
				fields[name] = value
			}
			value := []any{strconv.FormatInt(line.ts.UnixNano(), 10), line.msg}
			if fx.service && !fx.derivedLevel {
				fields["detected_level"] = "unknown"
				value = append(value, map[string]string{"detected_level": "unknown"})
			}
			row, _ := json.Marshal(fields)
			vlRows.Write(row)
			vlRows.WriteByte('\n')
			values = append(values, value)
		}
		streams = append(streams, map[string]any{"stream": fx.streamLabels(), "values": values})
	}
	// Rows without a listed field leave it out of their stream.
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,service_name,detected_level", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	// The fixtures are older than query_ingesters_within, so Loki only serves
	// them after the ingester flushes them to the store.
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}

	deadline := time.Now().Add(180 * time.Second)
	for {
		pending := ""
		for _, fx := range fixtures {
			lokiCount, vlCount := slidingFixtureCounts(t, fx)
			if lokiCount != len(fx.lines) || vlCount != len(fx.lines) {
				pending += fmt.Sprintf(" %s: fixture=%d loki=%d victorialogs=%d;", fx.app, len(fx.lines), lokiCount, vlCount)
			}
		}
		if pending == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends not healthy:%s", pending)
		}
		time.Sleep(time.Second)
	}
}

// slidingFixtureCounts counts the fixture natively on VictoriaLogs and through
// a warning-free Loki instant query covering every line.
func slidingFixtureCounts(t *testing.T, fx slidingLiveFixture) (lokiCount, vlCount int) {
	t.Helper()
	first, last := fx.lines[0].ts, fx.lines[len(fx.lines)-1].ts
	selector := fx.selector()
	vlParams := url.Values{"query": {selector}, "start": {first.UTC().Format(time.RFC3339Nano)}, "end": {last.Add(time.Second).UTC().Format(time.RFC3339Nano)}, "limit": {"100000"}}
	status, body := hardeningRequest(t, http.MethodGet, vlURL+"/select/logsql/query?"+vlParams.Encode(), "", nil)
	if status != http.StatusOK {
		t.Fatalf("VL count query: %d %s", status, body)
	}
	vlCount = strings.Count(string(body), "\n")

	window := last.Sub(first) + time.Minute
	lokiParams := url.Values{"query": {fmt.Sprintf("sum(count_over_time(%s[%ds]))", selector, int(window.Seconds()))}, "time": {strconv.FormatInt(last.Add(time.Second).UnixNano(), 10)}}
	_, _, resp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+lokiParams.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if resp["status"] == "success" && resp["warnings"] == nil {
		for _, item := range extractArray(extractMap(resp, "data"), "result") {
			if value, ok := item.(map[string]interface{})["value"].([]interface{}); ok && len(value) == 2 {
				n, _ := strconv.Atoi(fmt.Sprint(value[1]))
				lokiCount += n
			}
		}
	}
	return lokiCount, vlCount
}

// slidingRangeSeries runs query_range and returns series → timestamp → value,
// failing unless the response is a successful matrix without warnings.
func slidingRangeSeries(t *testing.T, baseURL, query string, start, end time.Time, step time.Duration, headers map[string]string) map[string]map[string]string {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	}
	all := map[string]string{"X-Scope-OrgID": "0"}
	for k, v := range headers {
		all[k] = v
	}
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), all)
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || resp["error"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", baseURL, query, status, body)
	}
	data := extractMap(resp, "data")
	if data["resultType"] != "matrix" {
		t.Fatalf("%s %s: expected matrix, got %s", baseURL, query, body)
	}
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		t.Fatalf("%s %s: invalid metric JSON: %v", baseURL, query, err)
	}
	out, err := decodeMetricSamples(resp)
	if err != nil {
		t.Fatalf("%s %s: invalid metric response: %v", baseURL, query, err)
	}
	return out
}

func assertSlidingParity(t *testing.T, query string, loki, proxy map[string]map[string]string) {
	t.Helper()
	if err := metricSamplesDifference(loki, proxy, true); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// Loki emits a sliding-window sample only when (t-range, t] holds lines, so a
// data gap stays absent for every client, including Drilldown-tagged requests.
func TestRangeMetricCompatibilitySlidingGaps(t *testing.T) {
	fx := ensureSlidingFixtures(t)
	selector := `{app="` + fx.gap.app + `"}`
	start, end := fx.hour0, fx.hour0.Add(8*time.Hour)
	for _, tc := range []struct {
		query string
		step  time.Duration
	}{
		{`sum by (app) (count_over_time(` + selector + `[1h]))`, 300 * time.Second},
		{`sum by (app) (count_over_time(` + selector + `[5m]))`, time.Minute},
		{`sum by (app) (rate(` + selector + `[1h]))`, 300 * time.Second},
		{`sum by (app) (bytes_rate(` + selector + `[1h]))`, 300 * time.Second},
		{`sum by (app) (count_over_time(` + selector + `[1h])) / 3600`, 300 * time.Second},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, tc.step, nil)
			steps := int(end.Sub(start)/tc.step) + 1
			for _, points := range loki {
				if len(points) >= steps {
					t.Fatalf("Loki returned %d of %d steps; the fixture gap is not exercised", len(points), steps)
				}
			}
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, proxyURL, tc.query, start, end, tc.step, nil))
			assertSlidingParity(t, tc.query+" [drilldown]", loki, slidingRangeSeries(t, proxyURL, tc.query, start, end, tc.step, map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}))
		})
	}
}

// Windows that are not a multiple of the step, and lines that sit exactly on
// window edges, must be counted as Loki counts them.
func TestRangeMetricCompatibilitySlidingBucketEdges(t *testing.T) {
	fx := ensureSlidingFixtures(t)
	s0 := fx.s0
	selector := `{app="` + fx.edge.app + `"}`
	// Starts and ends are step-aligned: this stack's Loki aligns queries with the step.
	for _, tc := range []struct {
		query      string
		start, end time.Time
		wantValue  string
	}{
		{`sum(count_over_time(` + selector + `[90s]))`, s0.Add(5 * time.Minute), s0.Add(25 * time.Minute), "9"},
		{`sum(count_over_time(` + selector + `[2m]))`, s0, s0.Add(33 * time.Minute), ""},
		{`sum by (app) (count_over_time(` + selector + `[2m]))`, s0.Add(-2 * time.Minute), s0.Add(32 * time.Minute), ""},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, tc.start, tc.end, time.Minute, nil)
			if tc.wantValue != "" {
				for _, points := range loki {
					for ts, v := range points {
						if v != tc.wantValue {
							t.Fatalf("Loki sanity: expected %s at every step, got %s at %s", tc.wantValue, v, ts)
						}
					}
				}
			}
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, proxyURL, tc.query, tc.start, tc.end, time.Minute, nil))
		})
	}
}
