package metricparity

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeSamples(t *testing.T) {
	decode := func(body string) (map[string]map[string]string, error) {
		var response map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&response); err != nil {
			t.Fatal(err)
		}
		return DecodeSamples(response)
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
			if _, err := DecodeSamples(response); err == nil {
				t.Fatal("malformed data accepted")
			}
		})
	}
	for name, timestamp := range map[string]interface{}{"nan": math.NaN(), "inf": math.Inf(1), "negative_inf": math.Inf(-1), "invalid_number": json.Number("oops"), "bool": true, "unbounded_precision": json.Number("1e-1000000000")} {
		t.Run(name, func(t *testing.T) {
			response := map[string]interface{}{"data": map[string]interface{}{"resultType": "vector", "result": []interface{}{map[string]interface{}{"metric": map[string]interface{}{}, "value": []interface{}{timestamp, "1"}}}}}
			if _, err := DecodeSamples(response); err == nil {
				t.Fatal("invalid timestamp accepted")
			}
		})
	}
	t.Run("float_and_negative_zero", func(t *testing.T) {
		response := map[string]interface{}{"data": map[string]interface{}{"resultType": "vector", "result": []interface{}{map[string]interface{}{"metric": map[string]interface{}{"a": ""}, "value": []interface{}{math.Copysign(0, -1), "-Inf"}}}}}
		got, err := DecodeSamples(response)
		if err != nil || got[`{"a":""}`]["0"] != "-Inf" {
			t.Fatalf("negative zero/empty label: %v, %v", got, err)
		}
	})
}

func TestDifference(t *testing.T) {
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
			if err := Difference(tc.loki, tc.proxy, tc.nonempty); (err != nil) != tc.wantError {
				t.Fatalf("difference=%v, want error=%v", err, tc.wantError)
			}
		})
	}
	t.Run("deterministic_diagnostic", func(t *testing.T) {
		loki, proxy := samples{"z": {"2": "4"}, "a": {"1": "2"}}, samples{"z": {"2": "9"}, "a": {"1": "8"}}
		var first string
		for i := 0; i < 20; i++ {
			err := Difference(loki, proxy, true)
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
