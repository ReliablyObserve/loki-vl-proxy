package main

import (
	"strings"
	"testing"
)

func ok(body string) Answer { return Answer{Status: 200, Body: body} }

const (
	matrixA = `{"status":"success","data":{"resultType":"matrix","stats":{"x":1},"result":[
		{"metric":{"app":"a"},"values":[[1800000000,"1"],[1800000060,"2"]]},
		{"metric":{"app":"b"},"values":[[1800000000,"3"]]}]}}`
	// The same series in the other order, other statistics.
	matrixB = `{"status":"success","data":{"resultType":"matrix","stats":{"x":2},"result":[
		{"metric":{"app":"b"},"values":[[1800000000,"3"]]},
		{"metric":{"app":"a"},"values":[[1800000000,"1"],[1800000060,"2"]]}]}}`
	matrixValue = `{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"app":"a"},"values":[[1800000000,"1"],[1800000060,"2.0000000001"]]},
		{"metric":{"app":"b"},"values":[[1800000000,"3"]]}]}}`
	matrixShifted = `{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"app":"a"},"values":[[1800000001,"1"],[1800000060,"2"]]},
		{"metric":{"app":"b"},"values":[[1800000000,"3"]]}]}}`
	matrixLabels = `{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"app":"a","x":"1"},"values":[[1800000000,"1"],[1800000060,"2"]]},
		{"metric":{"app":"b"},"values":[[1800000000,"3"]]}]}}`
	streamsA = `{"status":"success","data":{"resultType":"streams","encodingFlags":["categorize-labels"],"result":[
		{"stream":{"app":"a"},"values":[["1800000000000000001","line one",{"structuredMetadata":{"k":"v"}}]]}]}}`
	streamsLine = `{"status":"success","data":{"resultType":"streams","result":[
		{"stream":{"app":"a"},"values":[["1800000000000000001","line two",{"structuredMetadata":{"k":"v"}}]]}]}}`
	streamsMeta = `{"status":"success","data":{"resultType":"streams","encodingFlags":["categorize-labels"],"result":[
		{"stream":{"app":"a"},"values":[["1800000000000000001","line one",{"parsed":{"k":"v"}}]]}]}}`
)

func TestCompareFacets(t *testing.T) {
	cases := []struct {
		name        string
		facets      []string
		loki, proxy Answer
		tolerance   float64
		subset      bool
		differ      []string
	}{
		{"series order and stats ignored", []string{"data", "status"}, ok(matrixA), ok(matrixB), 0, false, nil},
		{"a value within the sample tolerance", []string{"values", "data", "body"}, ok(matrixA), ok(matrixValue), 0, false, nil},
		{"a value beyond it", []string{"values", "data"}, ok(matrixA), ok(strings.Replace(matrixValue, "2.0000000001", "2.5", 1)), 0, false, []string{"values", "data"}},
		{"a value within tolerance", []string{"values"}, ok(matrixA), ok(matrixValue), 1e-6, false, nil},
		{"a timestamp is never within tolerance", []string{"values"}, ok(matrixA), ok(matrixShifted), 1e-6, false, []string{"values"}},
		{"labels", []string{"labels", "count"}, ok(matrixA), ok(matrixLabels), 0, false, []string{"labels"}},
		{"same labels other values", []string{"labels", "resultType"}, ok(matrixA), ok(matrixValue), 0, false, nil},
		{"status", []string{"status"}, ok(matrixA), Answer{Status: 500, Body: "x"}, 0, false, []string{"status"}},
		{"stream lines", []string{"entries"}, ok(streamsA), ok(streamsLine), 0, false, []string{"entries"}},
		{"stream lines equal, metadata differs", []string{"entries", "metadata"}, ok(streamsA), ok(streamsMeta), 0, false, []string{"metadata"}},
		{"encoding flags ignored", []string{"data"}, ok(streamsA), ok(strings.Replace(streamsA, `"encodingFlags":["categorize-labels"],`, "", 1)), 0, false, nil},
		{"label names as a set", []string{"labels", "values"}, ok(`{"status":"success","data":["b","a"]}`), ok(`{"status":"success","data":["a","b"]}`), 0, false, nil},
		{"label names differ", []string{"values"}, ok(`{"data":["a","b"]}`), ok(`{"data":["a"]}`), 0, false, []string{"values"}},
		{"label names subset holds", []string{"labels", "values"}, ok(`{"data":["a","b"]}`), ok(`{"data":["a"]}`), 0, true, nil},
		{"label names subset fails", []string{"labels"}, ok(`{"data":["a"]}`), ok(`{"data":["a","c"]}`), 0, true, []string{"labels"}},
		{"subset of something that is not a list", []string{"values"}, ok(`{"data":{"x":1}}`), ok(`{"data":{"x":1}}`), 0, true, nil},
		{"detected field values", []string{"values"}, ok(`{"values":["x","y"]}`), ok(`{"values":["y","x"]}`), 0, false, nil},
		{"detected fields", []string{"fields", "labels"}, ok(`{"fields":[{"label":"a"},{"label":"b"}]}`), ok(`{"fields":[{"label":"b"},{"label":"a"}]}`), 0, false, nil},
		{"detected labels", []string{"labels"}, ok(`{"detectedLabels":[{"label":"a"}]}`), ok(`{"detectedLabels":[]}`), 0, false, []string{"labels"}},
		{"error text from JSON", []string{"error", "errorType"}, Answer{Status: 400, Body: `{"error":"parse error","errorType":"bad_data"}`},
			Answer{Status: 400, Body: `{"error":"parse error","errorType":"bad_data"}`}, 0, false, nil},
		{"error text differs", []string{"error"}, Answer{Status: 400, Body: "parse error at line 1\n"}, Answer{Status: 400, Body: `{"message":"other"}`}, 0, false, []string{"error"}},
		{"error of a success is nothing", []string{"error"}, ok(`{"error":"x"}`), ok(`{}`), 0, false, nil},
		{"warnings as a set", []string{"warnings"}, ok(`{"warnings":["b","a"]}`), ok(`{"warnings":["a","b"]}`), 0, false, nil},
		{"body without stats", []string{"body"}, ok(`{"streams":1,"stats":{"a":1}}`), ok(`{"streams":1,"stats":{"a":2}}`), 0, false, nil},
		{"body of text", []string{"body"}, ok("plain"), ok("plain!"), 0, false, []string{"body"}},
		{"vector values", []string{"values"}, ok(`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1800000000,"4"]}]}}`),
			ok(`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1800000000,"4.0000001"]}]}}`), 1e-6, false, nil},
		{"tolerance on maps of other size", []string{"values"}, ok(matrixA), ok(`{"data":{"result":[{"metric":{"app":"a"},"values":[]}]}}`), 1e-6, false, []string{"values"}},
		{"tolerance on non numbers", []string{"values"}, ok(`{"values":["x"]}`), ok(`{"values":["y"]}`), 1e-6, false, []string{"values"}},
		{"NaN equals NaN", []string{"values"}, ok(`{"data":{"result":[{"metric":{},"value":[1,"NaN"]}]}}`),
			ok(`{"data":{"result":[{"metric":{},"value":[1,"NaN"]}]}}`), 1e-6, false, nil},
		{"data that is not an object", []string{"data", "values"}, ok(`{"data":3}`), ok(`{"data":3}`), 0, false, nil},
		{"unknown facet", []string{"nothing"}, ok(`{}`), ok(`{"x":1}`), 0, false, nil},
		{"big integers stay exact", []string{"body"}, ok(`{"n":1800000000000000001}`), ok(`{"n":1800000000000000002}`), 0, false, []string{"body"}},
	}
	for _, c := range cases {
		got := Compare(c.facets, c.loki, c.proxy, c.tolerance, c.subset)
		var facets []string
		for _, d := range got {
			facets = append(facets, d.Facet)
		}
		if strings.Join(facets, ",") != strings.Join(c.differ, ",") {
			t.Errorf("%s: differ on %v, want %v (%+v)", c.name, facets, c.differ, got)
		}
	}
}

func TestCompareRecordsBothValuesClipped(t *testing.T) {
	long := strings.Repeat("x", 4000)
	got := Compare([]string{"body"}, ok(long), ok("y"), 0, false)
	if len(got) != 1 || !strings.Contains(got[0].Loki, "... (4002 bytes)") || got[0].Proxy != `"y"` {
		t.Fatalf("got %+v", got)
	}
}

func TestVacuous(t *testing.T) {
	empty := ok(`{"data":{"resultType":"matrix","result":[]}}`)
	if !Vacuous([]string{"status", "labels", "values"}, empty, empty) {
		t.Error("two empty matrices are vacuous")
	}
	if Vacuous([]string{"status", "labels"}, empty, ok(matrixA)) {
		t.Error("one side with data is not vacuous")
	}
	if Vacuous([]string{"status"}, Answer{Status: 400}, Answer{Status: 400}) {
		t.Error("two errors are compared, not vacuous")
	}
	if !Vacuous([]string{"count", "error", "body"}, ok(""), ok("")) {
		t.Error("a zero count, no error and an empty body are empty")
	}
	if Vacuous([]string{"resultType"}, empty, empty) {
		t.Error("a result type is something")
	}
}

func TestProbeFacets(t *testing.T) {
	cases := []struct {
		mode    string
		fields  []string
		want    string
		unknown string
	}{
		{"status", nil, "error,status", ""},
		{"numeric", nil, "labels,status,values", ""},
		{"structural", nil, "labels,resultType,status", ""},
		{"subset", nil, "labels,status,values", ""},
		{"exact", nil, "data,status", ""},
		{"observed", nil, "data,status", ""},
		{"observed", []string{"series labels", "streams.values"}, "entries,labels,status", ""},
		{"observed", []string{"headers"}, "data,status", "headers"},
		{"exact", []string{"status", "error_text"}, "error,status", ""},
	}
	for _, c := range cases {
		got, unknown := Probe{Mode: c.mode, Fields: c.fields}.Facets()
		if strings.Join(got, ",") != c.want || strings.Join(unknown, ",") != c.unknown {
			t.Errorf("%s %v: got %v %v", c.mode, c.fields, got, unknown)
		}
	}
}

// Comparing an answer with itself never finds a difference, whatever the answer holds.
func FuzzCompareIsReflexive(f *testing.F) {
	for _, seed := range []string{matrixA, streamsA, streamsMeta, `{"data":["a"]}`, `{"values":[1,"2"]}`, "plain", "", `{"data":{"result":[{"value":[1,"NaN"]}]}}`, `[1,2`} {
		f.Add(200, seed)
		f.Add(400, seed)
	}
	all := []string{facetStatus, facetError, facetErrorType, facetWarnings, facetResultType, facetLabels, facetCount,
		facetValues, facetEntries, facetMetadata, facetFields, facetData, facetBody}
	f.Fuzz(func(t *testing.T, status int, body string) {
		a := Answer{Status: status, Body: body}
		for _, subset := range []bool{false, true} {
			if d := Compare(all, a, a, 1e-6, subset); len(d) != 0 {
				t.Fatalf("an answer differs from itself: %+v", d)
			}
		}
		_ = Vacuous(all, a, a)
	})
}

// BenchmarkCompare is one case's comparison on the default facets of a numeric case: the runner's own cost
// per case, next to the two HTTP requests it makes.
func BenchmarkCompare(b *testing.B) {
	facets, _ := Probe{Mode: "numeric"}.Facets()
	loki, proxy := ok(matrixA), ok(matrixValue)
	b.ReportAllocs()
	for b.Loop() {
		_ = Compare(facets, loki, proxy, 1e-6, false)
	}
}

func TestSampleValuesCompareAsNumbers(t *testing.T) {
	sample := func(v string) Answer {
		return ok(`{"data":{"resultType":"matrix","result":[{"metric":{"code":"200"},"values":[[1800000000,"` + v + `"]]}]}}`)
	}
	cases := []struct {
		a, b   string
		differ bool
	}{
		{"0.30000000000000004", "0.3", false}, {"1e-13", "0", false}, {"1e-11", "0", true}, {"NaN", "NaN", false},
		{"+Inf", "+Inf", false}, {"+Inf", "-Inf", true}, {"NaN", "1", true}, {"+Inf", "1", true}, {"100", "100.0000002", true},
	}
	for _, c := range cases {
		if d := CompareWith([]string{facetValues}, sample(c.a), sample(c.b), Options{}); (len(d) > 0) != c.differ {
			t.Errorf("%s vs %s: %+v", c.a, c.b, d)
		}
	}
	d := CompareWith([]string{facetValues}, sample("100"), sample("101"), Options{})
	if len(d) != 1 || d[0].MaxRelDiff < 0.0099 || d[0].MaxRelDiff > 0.0100 {
		t.Errorf("relative difference: %+v", d)
	}
	if d := CompareWith([]string{facetValues}, sample("NaN"), sample("1"), Options{}); len(d) != 1 || d[0].MaxRelDiff != -1 {
		t.Errorf("NaN against a number: %+v", d)
	}
	// Labels and lines stay exact, even when they read as numbers.
	if d := CompareWith([]string{facetData}, sample("1"), ok(`{"data":{"resultType":"matrix","result":[{"metric":{"code":"200.0"},"values":[[1800000000,"1"]]}]}}`), Options{}); len(d) != 1 || d[0].MaxRelDiff != 0 {
		t.Errorf("a label that reads as a number: %+v", d)
	}
	lines := func(l string) Answer {
		return ok(`{"data":{"resultType":"streams","result":[{"stream":{},"values":[["1800000000000000000","` + l + `"]]}]}}`)
	}
	if d := CompareWith([]string{facetData}, lines("1.0"), lines("1"), Options{}); len(d) != 1 {
		t.Errorf("a line that reads as a number: %+v", d)
	}
	if d := CompareWith([]string{facetBody}, ok(`{"streams":5}`), ok(`{"streams":5.0000000001}`), Options{}); len(d) != 1 {
		t.Errorf("a count outside a sample is exact: %+v", d)
	}
	if d := CompareWith([]string{facetBody}, ok(`{"streams":5}`), ok(`{"streams":5.0000000001}`), Options{Tolerance: 1e-6}); len(d) != 0 {
		t.Errorf("a numeric case compares every number: %+v", d)
	}
}
