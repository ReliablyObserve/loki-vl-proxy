package proxy

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

// The metric builders' response is written without reflection, byte for byte
// as encoding/json writes it, for every value the builders produce: label
// names and values with characters encoding/json escapes (quotes, <, >, &,
// control and non-ASCII characters, U+2028, invalid UTF-8), timestamps in
// whole and fractional seconds, sample values as strings, matrices and
// vectors, empty and missing parts. Values it does not write itself are left
// to encoding/json.
// conformance: quality/query-range-extent-cache, numeric-and-response-formatting
func TestManualMetricResponseMatchesEncodingJSON(t *testing.T) {
	want := func(resultType string, result []map[string]interface{}) []byte {
		if result == nil {
			result = []map[string]interface{}{}
		}
		out, _ := json.Marshal(map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": resultType, "result": result}})
		return out
	}
	strs := []string{"", "info", "api-gateway", "service.name", "_stream"}
	floats := []float64{0, math.Copysign(0, -1), 1, 1700001000, 1700001000.5, 1700001000.123456789, -3, 1e-7, 1e21, 123456789012345678, 1 << 53, 1<<53 + 2, 9007199254740993, 0.000001, 2.5e-5}
	rng := rand.New(rand.NewSource(7))
	pick := func() string { return strs[rng.Intn(len(strs))] }
	direct := 0
	for n := 0; n < 600; n++ {
		if n == 300 {
			strs = append(strs, "a b", `q"uote`, `back\slash`, "<tag>", "a&b", "tab\tx", "nl\n", "\x01", "zażółć", "line\u2028sep", "bad\xffutf8", "emoji😀", "service.name", "_stream", "{app=\"x\"}")
		}
		var result []map[string]interface{}
		resultType := "matrix"
		if n%3 == 0 {
			resultType = "vector"
		}
		for s := 0; s < rng.Intn(4); s++ {
			metric := map[string]string{}
			for l := 0; l < rng.Intn(4); l++ {
				metric[pick()] = pick()
			}
			if rng.Intn(10) == 0 {
				metric = nil
			}
			item := map[string]interface{}{"metric": metric}
			ts := floats[rng.Intn(len(floats))]
			if resultType == "vector" {
				item["value"] = []interface{}{ts, strconv.FormatFloat(floats[rng.Intn(len(floats))], 'f', -1, 64)}
			} else {
				var points [][]interface{}
				for p := 0; p < rng.Intn(5); p++ {
					points = append(points, []interface{}{ts + float64(p)*10, strconv.FormatFloat(floats[rng.Intn(len(floats))], 'f', -1, 64)})
				}
				item["values"] = points
			}
			result = append(result, item)
		}
		if n%17 == 0 {
			result = nil
		}
		if got, w := marshalManualMetricResponse(resultType, result), want(resultType, result); !bytes.Equal(got, w) {
			t.Fatalf("case %d differs:\n got: %s\nwant: %s", n, got, w)
		}
		if _, ok := appendManualMetricResponse(nil, resultType, result); ok {
			direct++
		}
	}
	if direct < 200 {
		t.Fatalf("only %d of 600 cases written directly", direct)
	}
	// Values outside the builders' types fall back to encoding/json.
	odd := []map[string]interface{}{{"metric": map[string]string{"a": "b"}, "value": []interface{}{json.Number("1.5"), math.Pi}}}
	if got, w := marshalManualMetricResponse("vector", odd), want("vector", odd); !bytes.Equal(got, w) {
		t.Fatalf("fallback differs:\n got: %s\nwant: %s", got, w)
	}
	if _, ok := appendManualMetricResponse(nil, "vector", odd); ok {
		t.Fatal("a json.Number must be left to encoding/json")
	}
}

// A relabelled stats point on a whole second is written as the integer that
// strconv.FormatFloat(x, 'f', -1, 64) writes for it, over the whole range of
// Unix timestamps the proxy meets.
// conformance: quality/query-range-extent-cache, numeric-and-response-formatting
func TestWholeSecondTimestampMatchesFormatFloat(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 200000; i++ {
		sec := rng.Int63n(4_000_000_000)
		if i < 3 {
			sec = []int64{0, 1, 9_223_372_036}[i]
		}
		ns := sec * 1_000_000_000
		want := strconv.FormatFloat(float64(ns)/1e9, 'f', -1, 64)
		if got := strconv.FormatInt(ns/1_000_000_000, 10); got != want {
			t.Fatalf("%d s: integer %s, FormatFloat %s", sec, got, want)
		}
	}
}
