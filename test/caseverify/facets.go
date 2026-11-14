package main

import (
	"sort"
	"strings"
)

// The narrow facets. Each reads one part of an answer, so a case that names it is failed only by that part.

// entryMeta is the third element of a categorized log entry: {"structuredMetadata": {...}, "parsed": {...}}.
func entryMeta(e any) map[string]any {
	list, _ := e.([]any)
	if len(list) < 3 {
		return nil
	}
	m, _ := list[2].(map[string]any)
	return m
}

// withoutLabel is a series' or stream's labels without one label, as a key.
func withoutLabel(item any, name string) string {
	m, _ := labelsOf(item).(map[string]any)
	out := map[string]any{}
	for k, v := range m {
		if k != name {
			out[k] = v
		}
	}
	return canonical(out)
}

// timestamps are the sample or entry timestamps of every series and stream, keyed by its labels, in order.
func (p parsed) timestamps() any {
	out := map[string]any{}
	for _, item := range p.result() {
		var ts []any
		if values, ok := field(item, "values").([]any); ok {
			for _, v := range values {
				if pair, ok := v.([]any); ok && len(pair) > 0 {
					ts = append(ts, pair[0])
				}
			}
		} else if pair, ok := field(item, "value").([]any); ok && len(pair) > 0 {
			ts = append(ts, pair[0])
		}
		sort.SliceStable(ts, func(i, j int) bool { return tsLess(ts[i], ts[j]) })
		out[canonical(labelsOf(item))] = ts
	}
	return out
}

// metaOf lists, per stream, the timestamp and the named part of the metadata of every entry.
func (p parsed) metaOf(part string) any {
	out := map[string]any{}
	for _, item := range p.result() {
		values, _ := field(item, "values").([]any)
		var kept []any
		for _, v := range values {
			e, _ := v.([]any)
			if len(e) < 1 {
				continue
			}
			kept = append(kept, []any{e[0], entryMeta(e)[part]})
		}
		out[canonical(labelsOf(item))] = kept
	}
	return out
}

// metaKeys are the keys of one part of the metadata of the whole answer, or, with part "", the categories
// the metadata holds.
func (p parsed) metaKeys(part string) any {
	set := map[string]bool{}
	for _, item := range p.result() {
		values, _ := field(item, "values").([]any)
		for _, v := range values {
			meta := entryMeta(v)
			if part == "" {
				for k := range meta {
					set[k] = true
				}
				continue
			}
			sub, _ := meta[part].(map[string]any)
			for k := range sub {
				set[k] = true
			}
		}
	}
	keys := make([]any, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return sortedSet(keys)
}

// detectedLevels are the detected_level of every entry (its structured metadata, else its stream's label) or
// of every series, listed under the labels without it.
func (p parsed) detectedLevels() any {
	out := map[string]any{}
	for _, item := range p.result() {
		key := withoutLabel(item, "detected_level")
		own, _ := labelsOf(item).(map[string]any)
		if values, ok := field(item, "values").([]any); ok && field(item, "stream") != nil {
			var kept []any
			for _, v := range values {
				e, _ := v.([]any)
				if len(e) < 1 {
					continue
				}
				level := own["detected_level"]
				if sm, ok := entryMeta(e)["structuredMetadata"].(map[string]any); ok && sm["detected_level"] != nil {
					level = sm["detected_level"]
				}
				kept = append(kept, []any{e[0], level})
			}
			out[key] = kept
			continue
		}
		prev, _ := out[key].([]any)
		out[key] = append(prev, own["detected_level"])
	}
	return out
}

// listOf is a detected_fields or detected_labels list.
func (p parsed) listOf(name string) []any {
	list, _ := field(p.doc, name).([]any)
	return list
}

// namesOf are the label of every element of a list, as a set.
func namesOf(list []any) any {
	out := make([]any, 0, len(list))
	for _, e := range list {
		out = append(out, field(e, "label"))
	}
	return sortedSet(out)
}

// propertyOf maps every element's label to one of its properties.
func propertyOf(list []any, prop string) any {
	out := map[string]any{}
	for _, e := range list {
		if label, ok := field(e, "label").(string); ok {
			out[label] = field(e, prop)
		}
	}
	return out
}

// narrow extracts the narrow facets; ok is false for a facet that is not one of them.
func (p parsed) narrow(f string) (any, bool) {
	switch f {
	case facetResult:
		return sortedSet(field(p.doc, "data", "result")), true
	case facetTimestamps:
		return p.timestamps(), true
	case facetStructuredMeta:
		return p.metaOf("structuredMetadata"), true
	case facetParsed:
		return p.metaOf("parsed"), true
	case facetMetaKeys:
		return p.metaKeys("structuredMetadata"), true
	case facetParsedKeys:
		return p.metaKeys("parsed"), true
	case facetMetaCategories:
		return p.metaKeys(""), true
	case facetDetectedLevel:
		return p.detectedLevels(), true
	case facetFieldLabels:
		return namesOf(p.listOf("fields")), true
	case facetFieldParsers:
		return propertyOf(p.listOf("fields"), "parsers"), true
	case facetFieldJSONPath:
		return propertyOf(p.listOf("fields"), "jsonPath"), true
	case facetFieldType:
		return propertyOf(p.listOf("fields"), "type"), true
	case facetLimit:
		return field(p.doc, "limit"), true
	case facetLabelNames:
		return namesOf(p.listOf("detectedLabels")), true
	case facetLabelCardinality:
		return propertyOf(p.listOf("detectedLabels"), "cardinality"), true
	}
	if name, ok := strings.CutPrefix(f, facetStatPrefix); ok {
		return field(p.doc, name), true
	}
	return nil, false
}
