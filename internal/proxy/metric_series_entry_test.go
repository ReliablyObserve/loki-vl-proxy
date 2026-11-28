package proxy

import (
	"maps"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The labels of a stream-grouped stats series (buildMetricSeriesEntry) and the series key (canonicalLabelsKey) are
// held to the forms they replaced, kept here as references: the stream parsed through the shared parse cache and
// copied, the stream level read from that copy, the grouping's labels copied out of it, and the key from a sorted
// copy of the names with every quote escaped.

func refCanonicalLabelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(strings.ReplaceAll(labels[key], `"`, `\"`))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func refBuildMetricSeriesEntry(p *Proxy, streamStr, levelStr string, derived *string, groupBy []string, byExplicit bool, origGroupBy []string) *metricSeriesCacheEntry {
	rawStreamLabels := parseStreamLabels(streamStr)
	streamLabels := make(map[string]string, len(rawStreamLabels))
	for k, v := range rawStreamLabels {
		streamLabels[k] = v
	}
	if levelStr != "" {
		streamLabels["level"] = levelStr
	}
	switch {
	case derived != nil && *derived != "":
		streamLabels["detected_level"] = *derived
	case derived != nil:
		delete(streamLabels, "detected_level")
	case levelStr == "" && strings.TrimSpace(streamLabels["detected_level"]) != "":
	default:
		if v, ok := streamLevel(rawStreamLabels); ok {
			streamLabels["detected_level"] = v
		} else if levelStr != "" {
			streamLabels["detected_level"] = normalizedLevel(levelStr)
		} else {
			streamLabels["detected_level"] = levelUnknown
		}
	}
	ensureSyntheticServiceName(streamLabels)
	metricLabels := buildManualMetricLabels(streamLabels, groupBy, byExplicit)
	if len(origGroupBy) == len(groupBy) {
		for i, vlKey := range groupBy {
			if lokiKey := origGroupBy[i]; lokiKey != vlKey {
				if val, ok := metricLabels[vlKey]; ok {
					delete(metricLabels, vlKey)
					metricLabels[lokiKey] = val
				} else if val, ok := streamLabels[lokiKey]; ok {
					metricLabels[lokiKey] = val
				}
			}
		}
	}
	translated := p.labelTranslator.TranslateLabelsMap(metricLabels)
	return &metricSeriesCacheEntry{metricLabels: metricLabels, translated: translated, key: refCanonicalLabelsKey(translated)}
}

// seriesEntryCase is one input of buildMetricSeriesEntry.
type seriesEntryCase struct {
	stream, level string
	derived       *string
	groupBy, orig []string
	byExplicit    bool
}

var seriesEntryGroupings = []struct {
	groupBy, orig []string
	byExplicit    bool
}{
	{[]string{"_stream", "level", "detected_level"}, nil, false},
	{[]string{"service_name", "_stream", "level", "detected_level"}, nil, false},
	{[]string{"service_name", "_stream", "level", "detected_level"}, []string{"service_name", "_stream", "level", "detected_level"}, false},
	{[]string{"service.name", "_stream", "level"}, []string{"service_name", "_stream", "level"}, false},
	{[]string{"level", "_stream", "detected_level"}, []string{"detected_level", "_stream", "detected_level"}, false},
	{[]string{"_stream"}, nil, true},
	{[]string{"app", "pod"}, nil, false},
	{nil, nil, false},
	{nil, nil, true},
}

var seriesEntryKeys = []string{"app", "pod", "level", "LEVEL", "Level", "severity", "SeverityText", "lvl", "detected_level",
	"service_name", "service.name", "k8s.pod.name", "k8s_pod_name", "namespace", "env", "container"}

var seriesEntryValues = []string{"api", "info", "ERROR", "warn", "debug", "", " spaced ", `q"uote`, `back\slash`, "a,b", "{x}", "é", "unknown", "fatal"}

// randomSeriesEntryCase draws a stream of up to six labels as VictoriaLogs writes _stream (quoted values, sorted
// names), a level column, a detected_level VictoriaLogs derived or none, and a grouping. A dotted name and its
// underscore spelling carry one value, so the translation's choice between them is not left to map order.
func randomSeriesEntryCase(rng *rand.Rand) seriesEntryCase {
	labels := map[string]string{}
	for n := rng.Intn(7); n > 0; n-- {
		labels[seriesEntryKeys[rng.Intn(len(seriesEntryKeys))]] = seriesEntryValues[rng.Intn(len(seriesEntryValues))]
	}
	for k, v := range labels {
		if u := strings.ReplaceAll(k, ".", "_"); u != k {
			if _, ok := labels[u]; ok {
				labels[u] = v
			}
		}
	}
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = k + "=" + strconv.Quote(labels[k])
	}
	c := seriesEntryCase{stream: "{" + strings.Join(parts, ",") + "}"}
	if rng.Intn(2) == 0 {
		c.level = seriesEntryValues[rng.Intn(len(seriesEntryValues))]
	}
	switch rng.Intn(3) {
	case 1:
		d := ""
		c.derived = &d
	case 2:
		d := seriesEntryValues[rng.Intn(len(seriesEntryValues))]
		c.derived = &d
	}
	g := seriesEntryGroupings[rng.Intn(len(seriesEntryGroupings))]
	c.groupBy, c.orig, c.byExplicit = g.groupBy, g.orig, g.byExplicit
	return c
}

func checkSeriesEntry(t *testing.T, p *Proxy, c seriesEntryCase) {
	t.Helper()
	got := p.buildMetricSeriesEntry(c.stream, c.level, c.derived, c.groupBy, c.byExplicit, c.orig)
	want := refBuildMetricSeriesEntry(p, c.stream, c.level, c.derived, c.groupBy, c.byExplicit, c.orig)
	if !maps.Equal(got.metricLabels, want.metricLabels) || !maps.Equal(got.translated, want.translated) || got.key != want.key {
		t.Fatalf("%+v:\n got  %v %v %s\n want %v %v %s", c, got.metricLabels, got.translated, got.key, want.metricLabels, want.translated, want.key)
	}
	if k := canonicalLabelsKey(got.translated); k != refCanonicalLabelsKey(got.translated) {
		t.Fatalf("key %s, reference %s", k, refCanonicalLabelsKey(got.translated))
	}
}

// buildMetricSeriesEntry gives the labels, the translated labels and the key the copying form gave, under both label
// styles, on random streams, levels, derived levels and groupings (with and without the stream, renamed keys, by ()),
// and the caller may change the labels it gets (a dropped level) without changing the next series'.
//
// conformance: semantics/series-level-aggregation-bounded-memory
func TestBuildMetricSeriesEntryMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, style := range []LabelStyle{LabelStylePassthrough, LabelStyleUnderscores} {
		p := &Proxy{labelTranslator: NewLabelTranslator(style, nil)}
		for i := 0; i < 20000; i++ {
			checkSeriesEntry(t, p, randomSeriesEntryCase(rng))
		}
		// A stream the parse cache holds is copied before it changes: the cached labels stay as parsed.
		const stream = `{app="api",level="info"}`
		parsed := maps.Clone(parseStreamLabels(stream))
		e := p.buildMetricSeriesEntry(stream, "warn", nil, []string{"_stream", "level", "detected_level"}, false, nil)
		delete(e.metricLabels, "level")
		if !maps.Equal(parseStreamLabels(stream), parsed) {
			t.Fatalf("cached labels changed: %v, parsed %v", parseStreamLabels(stream), parsed)
		}
		checkSeriesEntry(t, p, seriesEntryCase{stream: stream, level: "warn", groupBy: []string{"_stream", "level", "detected_level"}})
	}
	for _, labels := range []map[string]string{nil, {}, {"a": ""}, {"a": `x"y"`, "b": "1"}, {"z": "1", "a": "2", "m": `"`}} {
		if got, want := canonicalLabelsKey(labels), refCanonicalLabelsKey(labels); got != want {
			t.Fatalf("%v: %s, reference %s", labels, got, want)
		}
	}
	many := map[string]string{}
	for i := 0; i < 40; i++ {
		many["k"+strconv.Itoa(i)] = strconv.Itoa(i * 7)
	}
	if got, want := canonicalLabelsKey(many), refCanonicalLabelsKey(many); got != want {
		t.Fatalf("40 labels: %s, reference %s", got, want)
	}
}

// FuzzBuildMetricSeriesEntryMatchesReference holds buildMetricSeriesEntry to the copying form on any _stream text,
// level, derived level and grouping.
func FuzzBuildMetricSeriesEntryMatchesReference(f *testing.F) {
	f.Add(`{app="api",level="info",pod="p1"}`, "warn", "", false, uint8(1), false)
	f.Add(`{service.name="a",service_name="a"}`, "", "error", true, uint8(3), true)
	f.Add(`{}`, "", "", true, uint8(5), false)
	f.Add(`{a="x\"y",b="1,2"}`, "ERROR", "", false, uint8(0), true)
	f.Fuzz(func(t *testing.T, stream, level, derived string, hasDerived bool, grouping uint8, underscores bool) {
		style := LabelStylePassthrough
		if underscores {
			style = LabelStyleUnderscores
		}
		p := &Proxy{labelTranslator: NewLabelTranslator(style, nil)}
		g := seriesEntryGroupings[int(grouping)%len(seriesEntryGroupings)]
		c := seriesEntryCase{stream: stream, level: level, groupBy: g.groupBy, orig: g.orig, byExplicit: g.byExplicit}
		if hasDerived {
			c.derived = &derived
		}
		// The translation coalesces two names it writes alike (a dotted name and its underscore spelling, two names
		// it sanitises to one) in map order when both carry values: that choice is not this function's, so such
		// labels are not compared.
		want := refBuildMetricSeriesEntry(p, c.stream, c.level, c.derived, c.groupBy, c.byExplicit, c.orig)
		written := map[string]string{}
		for k, v := range want.metricLabels {
			if w, ok := written[p.labelTranslator.ToLoki(k)]; ok && w != v {
				return
			}
			written[p.labelTranslator.ToLoki(k)] = v
		}
		checkSeriesEntry(t, p, c)
	})
}

// BenchmarkBuildMetricSeriesEntry builds the series of a per-stream read: a stream of eight labels per series, as
// the bench/ab generator writes them, grouped by the stream and its levels.
func BenchmarkBuildMetricSeriesEntry(b *testing.B) {
	p := &Proxy{labelTranslator: NewLabelTranslator(LabelStyleUnderscores, nil)}
	streams := make([]string, 1<<17) // more than the parse cache keeps, as a read of many short-lived streams
	for i := range streams {
		streams[i] = `{app="api-gateway",cluster="us-east-1",container="api",env="production",level="info",namespace="prod",pod="api-gateway-` +
			strconv.Itoa(i) + `-x7k2",service_name="api-gateway"}`
	}
	groupBy := []string{"service_name", "_stream", "level", "detected_level"}
	for _, c := range []struct {
		name  string
		build func(*Proxy, string, string, *string, []string, bool, []string) *metricSeriesCacheEntry
	}{
		{"copying", refBuildMetricSeriesEntry},
		{"owned", (*Proxy).buildMetricSeriesEntry},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				_ = c.build(p, streams[i%len(streams)], "info", nil, groupBy, false, nil)
				i++
			}
		})
	}
}
