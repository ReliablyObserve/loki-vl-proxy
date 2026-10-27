package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// randomFoldBody writes a matrix or vector answer: series over a few label
// names (some absent on some series, odd values among them), samples on a time
// grid that each series covers sparsely, values as strings or numbers.
func randomFoldBody(rnd *rand.Rand, vector bool) []byte {
	names := []string{"pod", "zone", "svc", "a_b", "x.y", "Zed"}
	values := []string{"a", "b", "c", "", `q"r`, `s\t`, "<&>", "é", " "}
	grid := []float64{1793000000, 1793000010, 1793000020.5, 1793000030, 1.5e-7, 1e21}
	var series []string
	for i, n := 0, 1+rnd.Intn(14); i < n; i++ {
		metric := map[string]string{}
		for _, name := range names {
			if rnd.Intn(2) == 0 {
				metric[name] = values[rnd.Intn(len(values))]
			}
		}
		m, _ := json.Marshal(metric)
		if vector {
			series = append(series, fmt.Sprintf(`{"metric":%s,"value":[%s,"%s"]}`, m, "1793000000", strconv.FormatFloat(float64(rnd.Intn(50))/4, 'f', -1, 64)))
			continue
		}
		var points []string
		for _, ts := range grid {
			if rnd.Intn(3) == 0 {
				continue
			}
			v := float64(rnd.Intn(60)) / 3
			if rnd.Intn(5) == 0 {
				points = append(points, fmt.Sprintf(`[%s,%s]`, strconv.FormatFloat(ts, 'g', -1, 64), strconv.FormatFloat(v, 'g', -1, 64)))
			} else {
				points = append(points, fmt.Sprintf(`[%s,"%s"]`, strconv.FormatFloat(ts, 'g', -1, 64), strconv.FormatFloat(v, 'f', -1, 64)))
			}
		}
		series = append(series, fmt.Sprintf(`{"metric":%s,"values":[%s]}`, m, strings.Join(points, ",")))
	}
	kind := "matrix"
	if vector {
		kind = "vector"
	}
	return []byte(fmt.Sprintf(`{"status":"success","data":{"resultType":"%s","result":[%s]}}`, kind, strings.Join(series, ",")))
}

// canonicalFold reads an answer back as series sorted by their labels, so that
// the order the generic fold leaves them in (a map's) does not matter: metric
// text -> "ts=value" samples.
func canonicalFold(t *testing.T, body []byte) (kind string, series map[string][]string, values map[string][]float64) {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	series, values = map[string][]string{}, map[string][]float64{}
	for _, s := range resp.Data.Result {
		m, _ := json.Marshal(s.Metric)
		points := s.Values
		if s.Value != nil {
			points = [][]any{s.Value}
		}
		for _, point := range points {
			series[string(m)] = append(series[string(m)], fmt.Sprint(point[0]))
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			values[string(m)] = append(values[string(m)], v)
		}
	}
	return resp.Data.ResultType, series, values
}

// sameFold: the same series, the same sample times, and values equal to the
// rounding of a mean taken in another order (the generic fold's group order is a
// map's).
func sameFold(t *testing.T, got, want []byte) string {
	t.Helper()
	gk, gs, gv := canonicalFold(t, got)
	wk, ws, wv := canonicalFold(t, want)
	if gk != wk || len(gs) != len(ws) {
		return fmt.Sprintf("%s/%d series against %s/%d", gk, len(gs), wk, len(ws))
	}
	for key, times := range ws {
		if strings.Join(gs[key], ",") != strings.Join(times, ",") {
			return key + ": sample times differ"
		}
		for i, v := range wv[key] {
			if math.Abs(gv[key][i]-v) > 1e-9*math.Max(1, math.Abs(v)) && !(math.IsNaN(v) && math.IsNaN(gv[key][i])) {
				return fmt.Sprintf("%s sample %d: %g against %g", key, i, gv[key][i], v)
			}
		}
	}
	return ""
}

// conformance: semantics/nested-aggregation-outer-by-native-routes, semantics/non-sum-vector-aggregation-unreduced
func TestTypedFoldMatchesGenericFold(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261026))
	ops := []string{"sum", "avg", "min", "max", "count"}
	pool := []string{"pod", "zone", "svc", "a_b", "x.y", "Zed", "none", withoutAllLabels}
	for i := 0; i < 600; i++ {
		vector := i%3 == 0
		body := randomFoldBody(rnd, vector)
		var levels []foldLevel
		want := body
		for depth := 1 + rnd.Intn(3); depth > 0; depth-- {
			op := ops[rnd.Intn(len(ops))]
			var labels []string
			for _, name := range pool {
				if rnd.Intn(3) == 0 {
					labels = append(labels, name)
				}
			}
			if rnd.Intn(2) == 0 {
				levels = append(levels, withoutFoldLevel(labels, op))
				want = applyWithoutGroupingGeneric(want, labels, op)
				continue
			}
			levels = append(levels, byFoldLevel(labels, op))
			exclude := []string{withoutAllLabels}
			if len(labels) > 0 {
				exclude = nil
				for _, name := range seriesLabelNames(want) {
					found := false
					for _, l := range labels {
						found = found || l == name
					}
					if !found {
						exclude = append(exclude, name)
					}
				}
			}
			want = applyWithoutGroupingGeneric(want, exclude, op)
		}
		got, ok := foldLevels(body, levels...)
		if !ok {
			t.Fatalf("#%d: the body was not read: %s", i, body)
		}
		if diff := sameFold(t, got, want); diff != "" {
			t.Fatalf("#%d: typed fold differs from the generic one: %s\nbody %s\ntyped   %s\ngeneric %s", i, diff, body, got, want)
		}
	}
}

// conformance: semantics/nested-aggregation-outer-by-native-routes
func TestTypedFoldLeavesOtherAnswersToTheGenericFold(t *testing.T) {
	for _, body := range []string{
		`{"status":"error","data":{"resultType":"matrix","result":[]}}`,
		`{"status":"success","data":{"resultType":"streams","result":[]}}`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":1},"values":[]}]}}`,
		`{"status":"success","data":{"resultType":"vector","result":[{"metric":{}}]}}`,
		`not json`,
	} {
		if _, ok := foldLevels([]byte(body), withoutFoldLevel(nil, "sum")); ok {
			t.Errorf("read: %s", body)
		}
	}
	if got := applyWithoutGrouping([]byte(`not json`), nil, "sum"); string(got) != "not json" {
		t.Fatalf("%s", got)
	}
}

// conformance: semantics/nested-aggregation-outer-by-native-routes
func TestJSONNumberAndTextMatchEncodingJSON(t *testing.T) {
	for _, f := range []float64{0, 1, -1, 1793000000, 1793000010.5, 1.5e-7, 1e-6, 9.99e-7, 1e21, 1e20, 123456789.125, math.SmallestNonzeroFloat64, 1e-10, 1.5e300} {
		want, _ := json.Marshal(f)
		if got := appendJSONNumber(nil, f); string(got) != string(want) {
			t.Errorf("%v: %s, encoding/json %s", f, got, want)
		}
	}
	for _, s := range []string{"", "plain", `q"r`, `s\t`, "<&>", "é", " ", "a\x00b", "tab\t", "\xff"} {
		want, _ := json.Marshal(s)
		if got := appendJSONText(nil, s); string(got) != string(want) {
			t.Errorf("%q: %s, encoding/json %s", s, got, want)
		}
	}
}

func BenchmarkFoldWithoutMatrix(b *testing.B) {
	var series []string
	for i := 0; i < 200; i++ {
		var points []string
		for ts := 1793000000; ts < 1793000000+2160*10; ts += 10 {
			points = append(points, fmt.Sprintf(`[%d,"%d"]`, ts, i%7+1))
		}
		series = append(series, fmt.Sprintf(`{"metric":{"app":"a%d","pod":"p%d","_stream":"s%d"},"values":[%s]}`, i%10, i, i, strings.Join(points, ",")))
	}
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(series, ",") + `]}}`)
	b.Run("typed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			applyWithoutGrouping(body, []string{"pod", "_stream"}, "avg")
		}
	})
	b.Run("generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			applyWithoutGroupingGeneric(body, []string{"pod", "_stream"}, "avg")
		}
	})
}

// refDecodeTyped reads the same shapes through encoding/json's document tree.
func refDecodeTyped(body []byte) (typedBody, bool) {
	var doc map[string]any
	if json.Unmarshal(body, &doc) != nil || doc["status"] != "success" {
		return typedBody{}, false
	}
	data, _ := doc["data"].(map[string]any)
	var out typedBody
	switch data["resultType"] {
	case "vector":
		out.vector = true
	case "matrix":
	default:
		return typedBody{}, false
	}
	items, _ := data["result"].([]any)
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		series := typedSeries{metric: map[string]string{}}
		metric, _ := item["metric"].(map[string]any)
		for k, v := range metric {
			text, ok := v.(string)
			if !ok {
				return typedBody{}, false
			}
			series.metric[k] = text
		}
		var samples []any
		if out.vector {
			samples = []any{item["value"]}
		} else {
			samples, _ = item["values"].([]any)
		}
		for _, sample := range samples {
			pair, _ := sample.([]any)
			if len(pair) < 2 {
				continue
			}
			ts, ok := pair[0].(float64)
			if !ok {
				return typedBody{}, false
			}
			var v float64
			switch x := pair[1].(type) {
			case string:
				v, _ = strconv.ParseFloat(x, 64)
			case float64:
				v = x
			default:
				if !out.vector {
					continue
				}
			}
			series.points = append(series.points, typedPoint{ts, v})
		}
		out.series = append(out.series, series)
	}
	return out, true
}

// conformance: semantics/nested-aggregation-outer-by-native-routes
func TestTypedScannerReadsWhatTheDocumentTreeReads(t *testing.T) {
	rnd := rand.New(rand.NewSource(77))
	for i := 0; i < 800; i++ {
		body := randomFoldBody(rnd, i%3 == 0)
		var doc map[string]any
		_ = json.Unmarshal(body, &doc)
		// Re-encode with another layout: indented, and with extra keys the fold ignores.
		if i%2 == 0 {
			doc["warnings"] = []any{"a, {b}", `q"r`}
			doc["data"].(map[string]any)["stats"] = map[string]any{"x": []any{1, "]", map[string]any{"}": nil}}}
		}
		var layout []byte
		if i%4 < 2 {
			layout, _ = json.MarshalIndent(doc, " ", "\t")
		} else {
			layout, _ = json.Marshal(doc)
		}
		got, ok := decodeTypedBody(layout)
		want, wantOK := refDecodeTyped(layout)
		if ok != wantOK {
			t.Fatalf("#%d: scanner ok=%v, document tree ok=%v: %s", i, ok, wantOK, layout)
		}
		if !ok {
			continue
		}
		if got.vector != want.vector || len(got.series) != len(want.series) {
			t.Fatalf("#%d: %d series against %d: %s", i, len(got.series), len(want.series), layout)
		}
		for j := range want.series {
			if fmt.Sprint(got.series[j].metric) != fmt.Sprint(want.series[j].metric) || fmt.Sprint(got.series[j].points) != fmt.Sprint(want.series[j].points) {
				t.Fatalf("#%d series %d: %v against %v", i, j, got.series[j], want.series[j])
			}
		}
	}
	for _, bad := range []string{``, `{`, `{"status":"success"}`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":1},"values":[]}]}}`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[["1","2"]]}]}}`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1,"2"]`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{}}]}}`} {
		if _, ok := decodeTypedBody([]byte(bad)); ok {
			t.Errorf("read: %s", bad)
		}
	}
	// result before resultType.
	if got, ok := decodeTypedBody([]byte(`{"data":{"result":[{"metric":{"a":"b"},"values":[[1,"2"]]}],"resultType":"matrix"},"status":"success"}`)); !ok || len(got.series) != 1 || got.series[0].points[0].v != 2 {
		t.Errorf("result before resultType: %v %v", got, ok)
	}
}
