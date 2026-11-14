package main

import (
	"regexp"
	"sort"
	"strconv"
)

// Options are the parts of a request the comparison needs beyond the facets.
type Options struct {
	Tolerance float64
	Subset    bool
	// Limit is the request's line limit (Loki's 100 when the log query names none, 0 for other requests) and
	// Forward its direction: an answer that hits the limit is compared up to the timestamp both sides hold whole.
	Limit   int
	Forward bool
	// Patterns marks a /patterns answer, whose first and last sample buckets cover part of the window.
	Patterns bool
	// Rank is the query's outermost topk or bottomk, for the ranked_values facet.
	Rank *RankSpec
	// MaskErrorSeries (compare.values.mask_error_series) masks the series labels of a pipeline error's text before
	// the error compares: Loki names a sample series of its own choosing, another one on the same request, so the
	// text compares exactly except the labels inside `for series: '{...}'`.
	MaskErrorSeries bool
}

// OptionsOf reads the comparison options of a probe's resolved request.
func OptionsOf(p Probe, query map[string][]string) Options {
	o := Options{Tolerance: tolerance(p), Subset: p.Mode == "subset"}
	o.MaskErrorSeries, _ = p.Values["mask_error_series"].(bool)
	get := func(k string) string {
		if v := query[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	switch p.Path {
	case "/loki/api/v1/query_range", "/loki/api/v1/query":
		o.Rank = RankOf(get("query"))
		o.Limit = 100
		if n, err := strconv.Atoi(get("limit")); err == nil && n > 0 {
			o.Limit = n
		}
		o.Forward = get("direction") == "forward"
	case "/loki/api/v1/patterns":
		o.Patterns = true
	}
	return o
}

// tsLess orders two timestamps written as integers (nanoseconds as strings, seconds as numbers).
func tsLess(a, b any) bool {
	x, y := tsText(a), tsText(b)
	if len(x) != len(y) {
		return len(x) < len(y)
	}
	return x < y
}

func tsText(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	}
	return canonical(v)
}

func entry(v any) (ts any, line string, ok bool) {
	e, isList := v.([]any)
	if !isList || len(e) < 2 {
		return nil, "", false
	}
	line, _ = e[1].(string)
	return e[0], line, true
}

// sortStreams orders every stream's entries as Loki defines their equality: by timestamp, ties by line. The
// order Loki returns lines of one timestamp in carries no meaning.
func (p parsed) sortStreams() {
	for _, item := range p.result() {
		m, _ := item.(map[string]any)
		values, _ := m["values"].([]any)
		if m["stream"] == nil || len(values) < 2 {
			continue
		}
		sort.SliceStable(values, func(i, j int) bool {
			ti, li, _ := entry(values[i])
			tj, lj, _ := entry(values[j])
			if tsLess(ti, tj) {
				return true
			}
			if tsLess(tj, ti) {
				return false
			}
			return li < lj
		})
	}
}

// streamEntries counts a log answer's entries and finds its edge timestamp: the oldest for a backward query,
// the newest for a forward one, where the line limit cut the answer.
func (p parsed) streamEntries(forward bool) (int, any) {
	n := 0
	var edge any
	for _, item := range p.result() {
		if field(item, "stream") == nil {
			continue
		}
		values, _ := field(item, "values").([]any)
		for _, v := range values {
			ts, _, ok := entry(v)
			if !ok {
				continue
			}
			n++
			if edge == nil || (forward && tsLess(edge, ts)) || (!forward && tsLess(ts, edge)) {
				edge = ts
			}
		}
	}
	return n, edge
}

// keepEntries drops every entry at or beyond the cut, and the streams it leaves empty.
func (p parsed) keepEntries(keep func(ts any) bool) {
	data, _ := field(p.doc, "data").(map[string]any)
	result, _ := data["result"].([]any)
	if data == nil || result == nil {
		return
	}
	kept := result[:0]
	for _, item := range result {
		m, _ := item.(map[string]any)
		values, isList := m["values"].([]any)
		if m["stream"] == nil || !isList {
			kept = append(kept, item)
			continue
		}
		var in []any
		for _, v := range values {
			if ts, _, ok := entry(v); ok && keep(ts) {
				in = append(in, v)
			}
		}
		if len(in) > 0 {
			m["values"] = in
			kept = append(kept, m)
		}
	}
	data["result"] = kept
}

// trimLimitEdge compares two log answers that reached the line limit up to the timestamp both hold whole:
// lines of the timestamp the limit cut at may be any of that timestamp's lines.
func trimLimitEdge(l, p parsed, o Options) {
	if o.Limit <= 0 {
		return
	}
	nl, el := l.streamEntries(o.Forward)
	np, ep := p.streamEntries(o.Forward)
	if (nl < o.Limit && np < o.Limit) || el == nil || ep == nil {
		return
	}
	// Backward answers are cut at their oldest timestamp, forward ones at their newest: keep what lies on the
	// near side of the more restrictive of the two cuts.
	cut := el
	if o.Forward == tsLess(ep, el) {
		cut = ep
	}
	keep := func(ts any) bool {
		if o.Forward {
			return tsLess(ts, cut)
		}
		return tsLess(cut, ts)
	}
	l.keepEntries(keep)
	p.keepEntries(keep)
}

// sampleRange is the first and last sample bucket of a patterns answer.
func (p parsed) sampleRange() (any, any) {
	var first, last any
	list, _ := field(p.doc, "data").([]any)
	for _, item := range list {
		samples, _ := field(item, "samples").([]any)
		for _, s := range samples {
			pair, _ := s.([]any)
			if len(pair) != 2 {
				continue
			}
			if first == nil || tsLess(pair[0], first) {
				first = pair[0]
			}
			if last == nil || tsLess(last, pair[0]) {
				last = pair[0]
			}
		}
	}
	return first, last
}

// trimPatternEdges drops the samples of the first and last buckets either side holds: they cover part of the
// window, and what lands in them depends on the instant each side read it.
func trimPatternEdges(l, p parsed) {
	fl, ll := l.sampleRange()
	fp, lp := p.sampleRange()
	if fl == nil || fp == nil {
		return
	}
	low, high := fl, ll
	if tsLess(low, fp) {
		low = fp
	}
	if tsLess(lp, high) {
		high = lp
	}
	for _, side := range []parsed{l, p} {
		data, _ := side.doc.(map[string]any)
		list, _ := data["data"].([]any)
		var kept []any
		for _, item := range list {
			m, _ := item.(map[string]any)
			samples, _ := m["samples"].([]any)
			var in []any
			for _, s := range samples {
				if pair, _ := s.([]any); len(pair) == 2 && tsLess(low, pair[0]) && tsLess(pair[0], high) {
					in = append(in, s)
				}
			}
			if len(in) > 0 {
				m["samples"] = in
				kept = append(kept, m)
			}
		}
		if data != nil && list != nil {
			data["data"] = kept
		}
	}
}

// CompareWith compares two answers on the facets with the request's options: entries in Loki's order of
// equality, a limit-cut answer up to the timestamp both sides hold whole, patterns without their edge buckets.
func CompareWith(facets []string, loki, proxy Answer, o Options) []Difference {
	l, p := parse(loki), parse(proxy)
	if o.MaskErrorSeries {
		l.maskErrorSeries()
		p.maskErrorSeries()
	}
	l.sortStreams()
	p.sortStreams()
	trimLimitEdge(l, p, o)
	if o.Patterns {
		trimPatternEdges(l, p)
	}
	var ranked bool
	rest := make([]string, 0, len(facets))
	for _, f := range facets {
		if f == facetRanked {
			ranked = true
			continue
		}
		rest = append(rest, f)
	}
	out := compareParsed(rest, l, p, loki.Status, proxy.Status, o.Tolerance, o.Subset)
	if !ranked {
		return out
	}
	if o.Rank == nil {
		// Not a topk or bottomk answer: nothing is tied, so the values compare as they are.
		return append(out, compareParsed([]string{facetValues}, l, p, loki.Status, proxy.Status, o.Tolerance, o.Subset)...)
	}
	if msg := RankedDifference(loki, proxy, o.Rank); msg != "" {
		out = append(out, Difference{Facet: facetRanked, Loki: "the ranked values of every step and the series above the cutoff", Proxy: clip(msg)})
	}
	return out
}

// pipelineSeries is the series labels of a pipeline error: `pipeline error: 'SampleExtractionErr' for series:
// '{...}'.` The labels may hold quotes and braces; they end at the first quote that closes the brace and is
// followed by the sentence's end.
var pipelineSeries = regexp.MustCompile(`(?s)(for series: ')\{.*?\}('\.?(?:\n|$))`)

// maskPipelineSeries replaces the series labels of every pipeline error in text.
func maskPipelineSeries(text string) string {
	return pipelineSeries.ReplaceAllString(text, "${1}{...}${2}")
}

// maskErrorSeries masks the series labels in the answer's error text, whether it is plain text or the error
// field of a JSON answer.
func (p *parsed) maskErrorSeries() {
	p.text = maskPipelineSeries(p.text)
	if m, ok := p.doc.(map[string]any); ok {
		for _, k := range []string{"error", "message"} {
			if s, ok := m[k].(string); ok {
				m[k] = maskPipelineSeries(s)
			}
		}
	}
}
