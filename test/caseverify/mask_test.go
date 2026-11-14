package main

import (
	"strings"
	"testing"
)

const pipelineHints = `.
Use a label filter to intentionally skip this error. (e.g | __error__!="SampleExtractionErr").
To skip all potential errors you can match empty errors.(e.g __error__="")
The label filter can also be specified after unwrap. (e.g | unwrap latency | __error__="" )`

func pipelineError(series string) Answer {
	return Answer{Status: 400, Body: `pipeline error: 'SampleExtractionErr' for series: '` + series + `'` + pipelineHints}
}

// Two pipeline errors that name another series compare equal with the mask on and differ without it; the rest of
// the text, the status and every other kind of error still compare exactly.
func TestMaskErrorSeries(t *testing.T) {
	s1 := `{__error__="SampleExtractionErr", __error_details__="strconv.ParseFloat: parsing \"payment processed\": invalid syntax", amount="744.5", app="payment-service"}`
	s2 := `{__error__="SampleExtractionErr", __error_details__="strconv.ParseFloat: parsing \"payment processed\": invalid syntax", amount="855.55", app="payment-service"}`
	other := Answer{Status: 400, Body: "parse error at line 1, col 7: syntax error: unexpected IDENTIFIER"}
	cases := []struct {
		name        string
		loki, proxy Answer
		masked      bool
		differs     bool
	}{
		{"another series, mask on", pipelineError(s1), pipelineError(s2), true, false},
		{"another series, mask off", pipelineError(s1), pipelineError(s2), false, true},
		{"the same series", pipelineError(s1), pipelineError(s1), false, false},
		{"another message family", pipelineError(s1), Answer{Status: 400, Body: strings.Replace(pipelineError(s1).Body, "SampleExtractionErr' for", "JSONParserErr' for", 1)}, true, true},
		{"another hint", pipelineError(s1), Answer{Status: 400, Body: strings.Replace(pipelineError(s2).Body, "intentionally skip", "skip", 1)}, true, true},
		{"a missing series clause", pipelineError(s1), Answer{Status: 400, Body: "pipeline error: 'SampleExtractionErr'" + pipelineHints}, true, true},
		{"a missing sentence stop is a difference of the text", pipelineError(s1), Answer{Status: 400, Body: strings.Replace(pipelineError(s2).Body, "'.\nUse", "'\nUse", 1)}, true, true},
		{"a quote and a brace inside the labels", pipelineError(`{a="x}'y", b="1"}`), pipelineError(`{a="z", b="2"}`), true, false},
		{"not a pipeline error", other, other, true, false},
		{"another status", pipelineError(s1), Answer{Status: 500, Body: pipelineError(s1).Body}, true, true},
		{"a pipeline error against a success", pipelineError(s1), ok(matrixA), true, true},
		{"a JSON error field", Answer{Status: 400, Body: `{"error":"pipeline error: 'E' for series: '{a=\"1\"}'.\nhint"}`},
			Answer{Status: 400, Body: `{"error":"pipeline error: 'E' for series: '{a=\"2\"}'.\nhint"}`}, true, false},
	}
	for _, c := range cases {
		d := CompareWith([]string{facetStatus, facetError}, c.loki, c.proxy, Options{MaskErrorSeries: c.masked})
		if (len(d) > 0) != c.differs {
			t.Errorf("%s: differences %v", c.name, d)
		}
	}
	// The masked text, not the original, is the recorded measure.
	d := CompareWith([]string{facetError}, pipelineError(s1), Answer{Status: 400, Body: "pipeline error: 'X' for series: '{a=\"1\"}'"}, Options{MaskErrorSeries: true})
	if len(d) != 1 || !strings.Contains(d[0].Loki, "for series: '{...}'") || strings.Contains(d[0].Loki, "amount") {
		t.Errorf("recorded difference %+v", d)
	}
}

// The case turns the mask on in its compare.values; nothing else does.
func TestOptionsOfReadsMaskErrorSeries(t *testing.T) {
	for _, c := range []struct {
		values map[string]any
		want   bool
	}{{nil, false}, {map[string]any{"mask_error_series": true}, true}, {map[string]any{"mask_error_series": false}, false}, {map[string]any{"mask_error_series": "yes"}, false}} {
		if got := OptionsOf(Probe{Path: "/loki/api/v1/query_range", Values: c.values}, nil).MaskErrorSeries; got != c.want {
			t.Errorf("%v: %v", c.values, got)
		}
	}
	p := probeOf(caseOf(t, "a/b", "request: {method: GET, path: /loki/api/v1/query_range}\ncompare: {mode: exact, fields: [error_text], values: {mask_error_series: true}}\n"))
	if !OptionsOf(p, nil).MaskErrorSeries {
		t.Error("the registry case's compare.values is read")
	}
}
