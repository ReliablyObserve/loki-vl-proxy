package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Answer is one side's reply to a probe.
type Answer struct {
	Status    int    `json:"status"`
	Body      string `json:"body"`
	Err       string `json:"error,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
	// Stale is set when the proxy marked its answer as served stale.
	Stale bool `json:"stale,omitempty"`
	// Partial is set when the proxy marked its answer partial (X-Loki-VL-Partial-Response).
	Partial bool `json:"partial,omitempty"`
}

// OK reports whether the side answered with a 2xx.
func (a Answer) OK() bool { return a.Status >= 200 && a.Status < 300 }

// Facets the runner compares. A case's compare.fields name them in the registry's own words; fieldFacet maps
// each word to one facet.
const (
	facetStatus     = "status"
	facetError      = "error"
	facetErrorType  = "errorType"
	facetWarnings   = "warnings"
	facetResultType = "resultType"
	facetLabels     = "labels"
	facetCount      = "count"
	facetValues     = "values"
	facetEntries    = "entries"
	facetMetadata   = "metadata"
	facetFields     = "fields"
	facetData       = "data"
	facetBody       = "body"
)

var fieldFacet = map[string]string{
	"status": facetStatus, "error_text": facetError, "error": facetError, "message": facetError,
	"errorType": facetErrorType, "warnings": facetWarnings,
	"resultType": facetResultType, "data.resultType": facetResultType,
	"series": facetLabels, "series_labels": facetLabels, "series labels": facetLabels, "data.result[].metric": facetLabels,
	"parsed labels": facetLabels, "streams": facetLabels, "streams.stream": facetLabels, "streams.labels": facetLabels,
	"streams.stream labels": facetLabels,
	"series_count":          facetCount, "series count": facetCount,
	"values": facetValues, "matrix.values": facetValues, "values of the by() form": facetValues,
	"streams.values": facetEntries, "entries": facetEntries, "timestamps": facetEntries,
	"structured_metadata_keys": facetMetadata, "metadata_categories": facetMetadata, "structuredMetadata": facetMetadata,
	"structured metadata": facetMetadata, "detected_level": facetMetadata,
	"fields.label": facetFields, "fields.parsers": facetFields, "fields.jsonPath": facetFields,
	"data.result values": facetValues, "data.result[].value[0]": facetValues, "data.result[].values": facetValues,
	"detectedLabels": facetLabels, "detectedLabels.label": facetLabels, "matrix.metric": facetLabels, "matrix.series": facetLabels,
	"fields": facetFields, "fields.type": facetFields,
	"data": facetData, "data.result": facetData, "body": facetBody,
	// The index/stats answer's own numbers and the detected labels' cardinality are compared on the whole body.
	"bytes": facetBody, "chunks": facetBody, "detectedLabels.cardinality": facetBody,
}

// Facets returns the facets a probe compares and the field names the runner does not know. The status is
// always compared: an answer with another status is never the same answer.
func (p Probe) Facets() ([]string, []string) {
	set := map[string]bool{facetStatus: true}
	var unknown []string
	for _, f := range p.Fields {
		if facet, ok := fieldFacet[f]; ok {
			set[facet] = true
		} else {
			unknown = append(unknown, f)
		}
	}
	if len(set) == 1 {
		switch p.Mode {
		case "status":
			set[facetError] = true
		case "numeric":
			set[facetLabels], set[facetValues] = true, true
		case "structural":
			set[facetResultType], set[facetLabels] = true, true
		case "subset":
			set[facetLabels], set[facetValues] = true, true
		default: // exact, observed
			set[facetData] = true
		}
	}
	out := make([]string, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out, unknown
}

// Difference is one facet the two answers disagree on, with both measured values.
type Difference struct {
	Facet string `json:"facet"`
	Loki  string `json:"loki"`
	Proxy string `json:"proxy"`
	// MaxRelDiff is the largest relative difference of the facet's numbers (-1: a NaN or an infinity against
	// a number); zero when the difference is not in numbers (labels, lines, timestamps, the shape).
	MaxRelDiff float64 `json:"max_relative_difference,omitempty"`
}

// valueLimit caps a measured value in the output; the full answers are in the recorded answers file.
const valueLimit = 1500

func clip(s string) string {
	if len(s) <= valueLimit {
		return s
	}
	return s[:valueLimit] + fmt.Sprintf("... (%d bytes)", len(s))
}

// Compare compares two answers on the facets. tolerance is the relative tolerance of numeric values; subset
// accepts a proxy set that Loki's contains.
func Compare(facets []string, loki, proxy Answer, tolerance float64, subset bool) []Difference {
	return compareParsed(facets, parse(loki), parse(proxy), loki.Status, proxy.Status, tolerance, subset)
}

func compareParsed(facets []string, l, p parsed, lokiStatus, proxyStatus int, tolerance float64, subset bool) []Difference {
	var out []Difference
	for _, f := range facets {
		lv, pv := l.facet(f, lokiStatus), p.facet(f, proxyStatus)
		equal, maxRel := equalFacet(f, lv, pv, tolerance, subset)
		if equal {
			continue
		}
		d := Difference{Facet: f, Loki: clip(canonical(lv)), Proxy: clip(canonical(pv))}
		if maxRel > 0 && !math.IsInf(maxRel, 0) {
			d.MaxRelDiff = maxRel
		} else if math.IsInf(maxRel, 0) {
			d.MaxRelDiff = -1 // a NaN or an infinity against a number
		}
		out = append(out, d)
	}
	return out
}

// Vacuous reports whether both answers succeed and hold nothing on the facets other than the status: two
// empty answers prove nothing.
func Vacuous(facets []string, loki, proxy Answer) bool {
	if !loki.OK() || !proxy.OK() {
		return false
	}
	l, p := parse(loki), parse(proxy)
	for _, f := range facets {
		if f == facetStatus {
			continue
		}
		if !empty(l.facet(f, loki.Status)) || !empty(p.facet(f, proxy.Status)) {
			return false
		}
	}
	return true
}

func empty(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return x == ""
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	case float64:
		return x == 0
	}
	return false
}

type parsed struct {
	doc  any
	text string
}

func parse(a Answer) parsed {
	p := parsed{text: a.Body}
	dec := json.NewDecoder(strings.NewReader(a.Body))
	dec.UseNumber()
	var doc any
	if dec.Decode(&doc) == nil {
		p.doc = normalizeNumbers(doc)
	}
	return p
}

// normalizeNumbers turns json.Number into float64 where it is exact enough to compare, and keeps the text
// otherwise (nanosecond timestamps exceed float64's precision).
func normalizeNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil && (i > 1<<53 || i < -(1<<53)) {
			return x.String()
		}
		f, _ := x.Float64()
		return f
	case []any:
		for i := range x {
			x[i] = normalizeNumbers(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = normalizeNumbers(x[k])
		}
	}
	return v
}

func field(v any, path ...string) any {
	for _, k := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}

// result is the data.result list of a query answer.
func (p parsed) result() []any {
	list, _ := field(p.doc, "data", "result").([]any)
	return list
}

func labelsOf(item any) any {
	if m := field(item, "metric"); m != nil {
		return m
	}
	return field(item, "stream")
}

// facet extracts one facet of an answer in a form that compares by value: sets are sorted, statistics and
// the encoding flags are left out.
func (p parsed) facet(f string, status int) any {
	switch f {
	case facetStatus:
		return float64(status)
	case facetError:
		if status >= 200 && status < 300 {
			return nil
		}
		for _, k := range []string{"error", "message"} {
			if s, ok := field(p.doc, k).(string); ok {
				return s
			}
		}
		return strings.TrimSpace(p.text)
	case facetErrorType:
		return field(p.doc, "errorType")
	case facetWarnings:
		return sortedSet(field(p.doc, "warnings"))
	case facetResultType:
		return field(p.doc, "data", "resultType")
	case facetLabels:
		return p.labels()
	case facetCount:
		return float64(len(p.result()))
	case facetValues:
		return p.values()
	case facetEntries:
		return p.entries(false)
	case facetMetadata:
		return p.entries(true)
	case facetFields:
		return sortedSet(field(p.doc, "fields"))
	case facetData:
		return normalizeData(field(p.doc, "data"))
	case facetBody:
		if p.doc == nil {
			return p.text
		}
		return withoutStats(p.doc)
	}
	return nil
}

func (p parsed) labels() any {
	if res := p.result(); res != nil {
		out := make([]any, 0, len(res))
		for _, item := range res {
			out = append(out, labelsOf(item))
		}
		return sortedSet(out)
	}
	// The label and series endpoints answer a list in data; detected_labels and detected_fields answer
	// objects of their own.
	if data, ok := field(p.doc, "data").([]any); ok {
		return sortedSet(data)
	}
	for _, k := range []string{"detectedLabels", "fields"} {
		if v := field(p.doc, k); v != nil {
			return sortedSet(v)
		}
	}
	return nil
}

// values are the samples of every series, keyed by the series' labels; the list answers (label values,
// detected field values) as a set.
func (p parsed) values() any {
	if res := p.result(); res != nil {
		out := map[string]any{}
		for _, item := range res {
			key := canonical(labelsOf(item))
			if v := field(item, "values"); v != nil {
				out[key] = v
			} else {
				out[key] = field(item, "value")
			}
		}
		return out
	}
	if data, ok := field(p.doc, "data").([]any); ok {
		return sortedSet(data)
	}
	if v, ok := field(p.doc, "values").([]any); ok {
		return sortedSet(v)
	}
	return normalizeData(field(p.doc, "data"))
}

// entries are the log lines of every stream, keyed by the stream's labels; withMeta keeps the metadata and
// parsed labels that a categorized answer carries as each entry's third element.
func (p parsed) entries(withMeta bool) any {
	out := map[string]any{}
	for _, item := range p.result() {
		values, _ := field(item, "values").([]any)
		var kept []any
		for _, v := range values {
			e, _ := v.([]any)
			switch {
			case withMeta && len(e) > 2:
				kept = append(kept, e[2])
			case !withMeta && len(e) >= 2:
				kept = append(kept, []any{e[0], e[1]})
			}
		}
		out[canonical(labelsOf(item))] = kept
	}
	return out
}

// normalizeData sorts the result list (Loki's order of series and streams carries no meaning) and drops the
// statistics, which differ between any two runs.
func normalizeData(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := map[string]any{}
	for k, e := range m {
		switch k {
		case "stats", "encodingFlags":
		case "result":
			out[k] = sortedSet(e)
		default:
			out[k] = e
		}
	}
	return out
}

func withoutStats(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := map[string]any{}
	for k, e := range m {
		if k == "data" {
			out[k] = normalizeData(e)
		} else if k != "stats" {
			out[k] = e
		}
	}
	return out
}

// sortedSet sorts a list by each element's canonical text.
func sortedSet(v any) any {
	list, ok := v.([]any)
	if !ok {
		return v
	}
	out := append([]any(nil), list...)
	sort.Slice(out, func(i, j int) bool { return canonical(out[i]) < canonical(out[j]) })
	return out
}

// canonical is the JSON text with sorted keys.
func canonical(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprintf("%v", v)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// sampleTolerance is the relative tolerance of every sample value: two engines that sum in another order
// differ in the last bits of a float64. absTolerance is the floor near zero.
const (
	sampleTolerance = 1e-9
	absTolerance    = 1e-12
)

// equalFacet compares one facet. Sample values (the value of a [timestamp, value] pair) of the values, data,
// body and entries facets compare as numbers within the tolerance (at least sampleTolerance); labels, lines and
// timestamps compare exactly. In a numeric case every number does. It returns the largest relative
// difference of the numbers it compared.
func equalFacet(f string, l, p any, tolerance float64, subset bool) (bool, float64) {
	if subset && (f == facetLabels || f == facetValues) {
		return isSubset(p, l), 0
	}
	switch f {
	// entries: a metric answer's [timestamp, value] samples when a case names timestamps or values of a matrix.
	case facetValues, facetData, facetBody, facetEntries:
		c := &numCompare{rel: math.Max(tolerance, sampleTolerance), anyNumber: tolerance > 0}
		return c.eq(l, p, false), c.maxRel
	}
	return canonical(l) == canonical(p), 0
}

// isSubset reports whether every element of the proxy's set is in Loki's.
func isSubset(proxy, loki any) bool {
	pl, pok := proxy.([]any)
	ll, lok := loki.([]any)
	if !pok || !lok {
		return canonical(proxy) == canonical(loki)
	}
	have := map[string]bool{}
	for _, e := range ll {
		have[canonical(e)] = true
	}
	for _, e := range pl {
		if !have[canonical(e)] {
			return false
		}
	}
	return true
}

// numCompare compares two values of the same shape, the numbers it may read as numbers within a relative
// tolerance, and keeps the largest relative difference it met.
type numCompare struct {
	rel       float64
	anyNumber bool // every number, not only sample values (a numeric case)
	maxRel    float64
}

// isSample is a [timestamp, value] pair as Loki writes it: a number and a numeric string.
func isSample(x []any) bool {
	if len(x) != 2 {
		return false
	}
	if _, ok := x[0].(float64); !ok {
		return false
	}
	s, ok := x[1].(string)
	if !ok {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func (c *numCompare) eq(l, p any, sampleValue bool) bool {
	switch x := l.(type) {
	case map[string]any:
		y, ok := p.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		equal := true
		for k, v := range x {
			if w, found := y[k]; !found || !c.eq(v, w, false) {
				equal = false
			}
		}
		return equal
	case []any:
		y, ok := p.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		// A sample is [timestamp, value]: the timestamp is exact, only the value has a tolerance.
		if isSample(x) && isSample(y) {
			return canonical(x[0]) == canonical(y[0]) && c.eq(x[1], y[1], true)
		}
		equal := true
		for i := range x {
			if !c.eq(x[i], y[i], false) {
				equal = false
			}
		}
		return equal
	}
	if !sampleValue && !c.anyNumber {
		return canonical(l) == canonical(p)
	}
	a, aok := number(l)
	b, bok := number(p)
	if !aok || !bok {
		return canonical(l) == canonical(p)
	}
	equal, rel := numEqual(a, b, c.rel)
	c.maxRel = math.Max(c.maxRel, rel)
	return equal
}

// numEqual is |a-b| <= max(rel*max(|a|,|b|), absTolerance), NaN equal to NaN and an infinity only to itself;
// the second result is the relative difference (an infinity's against a finite number is +Inf).
func numEqual(a, b, rel float64) (bool, float64) {
	switch {
	case math.IsNaN(a) || math.IsNaN(b):
		if math.IsNaN(a) && math.IsNaN(b) {
			return true, 0
		}
		return false, math.Inf(1)
	case math.IsInf(a, 0) || math.IsInf(b, 0):
		if a == b {
			return true, 0
		}
		return false, math.Inf(1)
	}
	diff, scale := math.Abs(a-b), math.Max(math.Abs(a), math.Abs(b))
	r := 0.0
	if scale > 0 {
		r = diff / scale
	}
	return diff <= math.Max(rel*scale, absTolerance), r
}

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}
