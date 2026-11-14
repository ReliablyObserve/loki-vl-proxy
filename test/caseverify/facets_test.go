package main

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// logs is a categorized log answer: one lbl, one entry.
func logs(labels, line, mt string) Answer {
	return ok(`{"status":"success","data":{"resultType":"streams","result":[{"stream":` + labels + `,"values":[["1800000000000000001",` +
		fmt.Sprintf("%q", line) + `,` + mt + `]]}]}}`)
}

func series(labels, samples string) Answer {
	return ok(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":` + labels + `,"values":[` + samples + `]}]}}`)
}

func vector(labels, sample string) Answer {
	return ok(`{"status":"success","data":{"resultType":"vector","result":[{"metric":` + labels + `,"value":` + sample + `}]}}`)
}

func fieldsAnswer(label, typ, parsers, path string, limit int) Answer {
	return ok(fmt.Sprintf(`{"fields":[{"label":%q,"type":%q,"cardinality":3,"parsers":%s,"jsonPath":%s}],"limit":%d}`, label, typ, parsers, path, limit))
}

func labelsAnswer(label string, cardinality int) Answer {
	return ok(fmt.Sprintf(`{"detectedLabels":[{"label":%q,"cardinality":%d}]}`, label, cardinality))
}

const (
	mt         = `{"structuredMetadata":{"k":"v"},"parsed":{"p":"1"}}`
	lbl        = `{"app":"a"}`
	sampleA    = `[1800000000,"1"],[1800000060,"2"]`
	sampleVal  = `[1800000000,"7"],[1800000060,"2"]`
	sampleTime = `[1800000001,"1"],[1800000060,"2"]`
)

// Every facet reads only what its name says: a change somewhere else in the answer is not a difference of the
// facet (unrelated), a change of what it names is (related). A case that names `timestamps` must not be failed
// by a value, a case that names `structured_metadata_keys` not by a value of a key, and so on.
func TestEveryFacetReadsOnlyWhatItNames(t *testing.T) {
	type change struct {
		name         string
		loki, proxy  Answer
		wantDiffer   bool
		facetOptions Options
	}
	cases := []struct {
		facet   string
		changes []change
	}{
		{facetTimestamps, []change{
			{"a value", series(lbl, sampleA), series(lbl, sampleVal), false, Options{}},
			{"a timestamp", series(lbl, sampleA), series(lbl, sampleTime), true, Options{}},
			{"an instant sample's value", vector(lbl, `[1800000000,"1"]`), vector(lbl, `[1800000000,"9"]`), false, Options{}},
			{"an instant sample's timestamp", vector(lbl, `[1800000000,"1"]`), vector(lbl, `[1800000009,"1"]`), true, Options{}},
			{"a log line", logs(lbl, "x", mt), logs(lbl, "y", mt), false, Options{}},
		}},
		{facetResult, []change{
			{"the answer's status text", series(lbl, sampleA), ok(strings.Replace(series(lbl, sampleA).Body, `"success"`, `"other"`, 1)), false, Options{}},
			{"the result type", series(lbl, sampleA), ok(strings.Replace(series(lbl, sampleA).Body, `"matrix"`, `"vector"`, 1)), false, Options{}},
			{"a value", series(lbl, sampleA), series(lbl, sampleVal), true, Options{}},
		}},
		{facetStructuredMeta, []change{
			{"the line", logs(lbl, "x", mt), logs(lbl, "y", mt), false, Options{}},
			{"the parsed labels", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"v"},"parsed":{"p":"2"}}`), false, Options{}},
			{"a structured metadata value", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"w"},"parsed":{"p":"1"}}`), true, Options{}},
		}},
		{facetParsed, []change{
			{"the structured metadata", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"w"},"parsed":{"p":"1"}}`), false, Options{}},
			{"a parsed value", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"v"},"parsed":{"p":"2"}}`), true, Options{}},
		}},
		{facetMetaKeys, []change{
			{"a structured metadata value", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"w"},"parsed":{"p":"1"}}`), false, Options{}},
			{"a parsed key", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"v"},"parsed":{"q":"1"}}`), false, Options{}},
			{"a structured metadata key", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"j":"v"},"parsed":{"p":"1"}}`), true, Options{}},
		}},
		{facetParsedKeys, []change{
			{"a parsed value", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"v"},"parsed":{"p":"2"}}`), false, Options{}},
			{"a structured metadata key", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"j":"v"},"parsed":{"p":"1"}}`), false, Options{}},
			{"a parsed key", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"v"},"parsed":{"q":"1"}}`), true, Options{}},
		}},
		{facetMetaCategories, []change{
			{"the keys inside a category", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"j":"w"},"parsed":{"q":"2"}}`), false, Options{}},
			{"a category", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"v"}}`), true, Options{}},
		}},
		{facetDetectedLevel, []change{
			{"another structured metadata key", logs(lbl, "x", `{"structuredMetadata":{"detected_level":"info"}}`), logs(lbl, "x", `{"structuredMetadata":{"detected_level":"info","k":"v"}}`), false, Options{}},
			{"the level in the metadata", logs(lbl, "x", `{"structuredMetadata":{"detected_level":"info"}}`), logs(lbl, "x", `{"structuredMetadata":{"detected_level":"warn"}}`), true, Options{}},
			{"the level as a lbl label", logs(`{"app":"a","detected_level":"info"}`, "x", `{}`), logs(`{"app":"a","detected_level":"warn"}`, "x", `{}`), true, Options{}},
			{"a metric series' level", series(`{"detected_level":"info"}`, sampleA), series(`{"detected_level":"error"}`, sampleA), true, Options{}},
			{"a value of a metric series", series(`{"detected_level":"info"}`, sampleA), series(`{"detected_level":"info"}`, sampleVal), false, Options{}},
		}},
		{facetFieldLabels, []change{
			{"the type", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "int", `["json"]`, `[["a"]]`, 1000), false, Options{}},
			{"the parsers", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["logfmt"]`, `[["a"]]`, 1000), false, Options{}},
			{"the limit", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 500), false, Options{}},
			{"the label", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("b", "string", `["json"]`, `[["a"]]`, 1000), true, Options{}},
		}},
		{facetFieldParsers, []change{
			{"the type", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "int", `["json"]`, `[["a"]]`, 1000), false, Options{}},
			{"the parsers", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["logfmt"]`, `[["a"]]`, 1000), true, Options{}},
		}},
		{facetFieldJSONPath, []change{
			{"the parsers", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["logfmt"]`, `[["a"]]`, 1000), false, Options{}},
			{"the path", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["json"]`, `[["a","b"]]`, 1000), true, Options{}},
		}},
		{facetFieldType, []change{
			{"the path", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["json"]`, `[["a","b"]]`, 1000), false, Options{}},
			{"the type", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "int", `["json"]`, `[["a"]]`, 1000), true, Options{}},
		}},
		{facetLimit, []change{
			{"the fields", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("b", "int", `["json"]`, `[["a"]]`, 1000), false, Options{}},
			{"the limit", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 500), true, Options{}},
			{"no limit against a limit", ok(`{}`), fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), true, Options{}},
		}},
		{facetLabelNames, []change{
			{"the cardinality", labelsAnswer("a", 3), labelsAnswer("a", 4), false, Options{}},
			{"the label", labelsAnswer("a", 3), labelsAnswer("b", 3), true, Options{}},
		}},
		{facetLabelCardinality, []change{
			{"the order of the labels", ok(`{"detectedLabels":[{"label":"a","cardinality":1},{"label":"b","cardinality":2}]}`),
				ok(`{"detectedLabels":[{"label":"b","cardinality":2},{"label":"a","cardinality":1}]}`), false, Options{}},
			{"the cardinality", labelsAnswer("a", 3), labelsAnswer("a", 4), true, Options{}},
		}},
		{facetStatPrefix + "streams", []change{
			{"the entries", ok(`{"streams":2,"chunks":2,"bytes":9,"entries":5}`), ok(`{"streams":2,"chunks":2,"bytes":9,"entries":6}`), false, Options{}},
			{"the streams", ok(`{"streams":2,"chunks":2,"bytes":9,"entries":5}`), ok(`{"streams":3,"chunks":2,"bytes":9,"entries":5}`), true, Options{}},
		}},
		{facetStatPrefix + "bytes", []change{
			{"the streams", ok(`{"streams":2,"bytes":9}`), ok(`{"streams":3,"bytes":9}`), false, Options{}},
			{"the bytes", ok(`{"streams":2,"bytes":9}`), ok(`{"streams":2,"bytes":10}`), true, Options{}},
		}},
		{facetStatPrefix + "chunks", []change{
			{"the bytes", ok(`{"chunks":2,"bytes":9}`), ok(`{"chunks":2,"bytes":10}`), false, Options{}},
			{"the chunks", ok(`{"chunks":2,"bytes":9}`), ok(`{"chunks":3,"bytes":9}`), true, Options{}},
		}},
		{facetLabels, []change{
			{"a value", series(lbl, sampleA), series(lbl, sampleVal), false, Options{}},
			{"a label", series(lbl, sampleA), series(`{"app":"b"}`, sampleA), true, Options{}},
		}},
		{facetCount, []change{
			{"a value", series(lbl, sampleA), series(lbl, sampleVal), false, Options{}},
			{"a series", series(lbl, sampleA), ok(`{"data":{"result":[]}}`), true, Options{}},
		}},
		{facetResultType, []change{
			{"a value", series(lbl, sampleA), series(lbl, sampleVal), false, Options{}},
			{"the type", series(lbl, sampleA), vector(lbl, `[1800000000,"1"]`), true, Options{}},
		}},
		{facetValues, []change{
			{"the status text", series(lbl, sampleA), ok(strings.Replace(series(lbl, sampleA).Body, `"success"`, `"other"`, 1)), false, Options{}},
			{"a value", series(lbl, sampleA), series(lbl, sampleVal), true, Options{}},
		}},
		{facetEntries, []change{
			{"the metadata", logs(lbl, "x", mt), logs(lbl, "x", `{"structuredMetadata":{"k":"w"}}`), false, Options{}},
			{"a line", logs(lbl, "x", mt), logs(lbl, "y", mt), true, Options{}},
		}},
		{facetFields, []change{
			{"the limit", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 500), false, Options{}},
			{"a field", fieldsAnswer("a", "string", `["json"]`, `[["a"]]`, 1000), fieldsAnswer("a", "int", `["json"]`, `[["a"]]`, 1000), true, Options{}},
		}},
		{facetError, []change{
			{"the error type", Answer{Status: 400, Body: `{"error":"e","errorType":"x"}`}, Answer{Status: 400, Body: `{"error":"e","errorType":"y"}`}, false, Options{}},
			{"the text", Answer{Status: 400, Body: `{"error":"e","errorType":"x"}`}, Answer{Status: 400, Body: `{"error":"f","errorType":"x"}`}, true, Options{}},
		}},
		{facetErrorType, []change{
			{"the text", Answer{Status: 400, Body: `{"error":"e","errorType":"x"}`}, Answer{Status: 400, Body: `{"error":"f","errorType":"x"}`}, false, Options{}},
			{"the type", Answer{Status: 400, Body: `{"error":"e","errorType":"x"}`}, Answer{Status: 400, Body: `{"error":"e","errorType":"y"}`}, true, Options{}},
		}},
		{facetWarnings, []change{
			{"the data", ok(`{"warnings":["w"],"data":1}`), ok(`{"warnings":["w"],"data":2}`), false, Options{}},
			{"a warning", ok(`{"warnings":["w"]}`), ok(`{"warnings":["v"]}`), true, Options{}},
		}},
		{facetStatus, []change{
			{"the body", ok(`{"a":1}`), ok(`{"a":2}`), false, Options{}},
			{"the status", ok(`{}`), Answer{Status: 204}, true, Options{}},
		}},
		{facetData, []change{
			{"the statistics", ok(`{"data":{"result":[],"stats":{"x":1}}}`), ok(`{"data":{"result":[],"stats":{"x":2}}}`), false, Options{}},
			{"the data", ok(`{"data":{"result":[]}}`), ok(`{"data":{"result":[1]}}`), true, Options{}},
		}},
		{facetBody, []change{
			{"the statistics", ok(`{"a":1,"stats":{"x":1}}`), ok(`{"a":1,"stats":{"x":2}}`), false, Options{}},
			{"a key", ok(`{"a":1}`), ok(`{"a":2}`), true, Options{}},
		}},
	}
	covered := map[string]bool{}
	for _, c := range cases {
		covered[c.facet] = true
		for _, ch := range c.changes {
			d := CompareWith([]string{c.facet}, ch.loki, ch.proxy, ch.facetOptions)
			if (len(d) > 0) != ch.wantDiffer {
				t.Errorf("facet %s, change of %s: differ=%v want %v (%+v)", c.facet, ch.name, len(d) > 0, ch.wantDiffer, d)
			}
		}
	}
	// Every facet a registry field name can mean has a row above.
	var missing []string
	for field, facet := range fieldFacet {
		if !covered[facet] {
			missing = append(missing, field+" -> "+facet)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("facets without a narrowness row: %v", missing)
	}
}

// Each registry field name reads the facet it names: the map is the contract, so its widest readings are
// pinned here.
func TestFieldNamesReadOnlyTheirOwnPart(t *testing.T) {
	want := map[string]string{
		"timestamps": facetTimestamps, "data.result[].value[0]": facetTimestamps, "data.result[].values[][0]": facetTimestamps,
		"data.result": facetResult, "parsed labels": facetParsed, "structuredMetadata": facetStructuredMeta,
		"structured_metadata_keys": facetMetaKeys, "metadata_categories": facetMetaCategories,
		"detected_level": facetDetectedLevel, "fields.label": facetFieldLabels, "fields.parsers": facetFieldParsers,
		"fields.jsonPath": facetFieldJSONPath, "fields.type": facetFieldType, "limit": facetLimit,
		"detectedLabels.label": facetLabelNames, "detectedLabels.cardinality": facetLabelCardinality,
		"bytes": facetStatPrefix + "bytes", "chunks": facetStatPrefix + "chunks", "parsed_keys": facetParsedKeys,
	}
	for field, facet := range want {
		if fieldFacet[field] != facet {
			t.Errorf("%s reads %q, want %q", field, fieldFacet[field], facet)
		}
	}
}

// On index/stats, streams and entries are numbers; on a query answer they stay the lbl lists.
func TestIndexStatsNamesAreNumbers(t *testing.T) {
	stats := Probe{Path: indexStatsPath, Mode: "observed", Fields: []string{"streams", "entries"}}
	got, unknown := stats.Facets()
	if strings.Join(got, ",") != "stat:entries,stat:streams,status" || len(unknown) != 0 {
		t.Errorf("index/stats facets: %v %v", got, unknown)
	}
	query := Probe{Path: "/loki/api/v1/query_range", Mode: "observed", Fields: []string{"streams", "entries"}}
	got, _ = query.Facets()
	if strings.Join(got, ",") != "entries,labels,status" {
		t.Errorf("query facets: %v", got)
	}
}
