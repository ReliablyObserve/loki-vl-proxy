package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// nvFold is one vector aggregation level over a vector (aggLayer.eval): by() keeps the listed labels a series has
// (nil by: no grouping, one group), without() drops the listed ones.
func nvFold(in []nvSeries, level nvLevel) []nvSeries {
	layer := aggLayer{op: level.op}
	switch {
	case level.without:
		layer.hasWith, layer.without = true, level.by
	case level.by != nil:
		layer.hasBy, layer.by = true, level.by
	}
	vec := make([]topKFoldRefSample, len(in))
	for i, s := range in {
		vec[i] = topKFoldRefSample{s.labels, s.value}
	}
	folded, _ := layer.eval(vec)
	out := make([]nvSeries, len(folded))
	for i, s := range folded {
		out[i] = nvSeries{s.labels, s.value}
	}
	return out
}

// nvRowLabels are the labels of a row's series: its stream labels and the service_name Loki gives the stream (the
// app label); with parsed, and logfmt, the pairs of the line too. Loki names a series by every label a bare parser
// extracts (parsed); the proxy keeps the stream identity by default, an owner-kept deviation
// (parsed-identity/dynamic-key-parsers, -exact-parser-series-identity opts in), so the differentials, which run
// the default, compare without them. Every generated line holds the same pairs, so the values do not change.
func nvRowLabels(parser string, parsed bool, r vlemu.Row) map[string]string {
	labels := vlemu.StreamLabels(r.Stream)
	labels["service_name"] = labels["app"]
	if parser == "logfmt" && parsed {
		for _, pair := range strings.Fields(r.Msg) {
			if k, v, ok := strings.Cut(pair, "="); ok {
				labels[k] = v
			}
		}
	}
	return labels
}

// nestedReference is the scenario's answer: count_over_time or rate (lines per second) of each series over the
// window (t-window, t], then every level folded over the level below it, then the divisor: series key (nvLabelsKey)
// -> t -> value, with each key's labels.
func nestedReference(s nvScenario) (map[string]map[int64]float64, map[string]map[string]string) {
	return nestedReferenceWith(s, nil)
}

// nestedReferenceWith is nestedReference with extra labels on every line's series (structured metadata Loki adds
// at ingest, such as detected_level).
func nestedReferenceWith(s nvScenario, extra map[string]string) (map[string]map[int64]float64, map[string]map[string]string) {
	return nestedReferenceParsed(s, extra, false)
}

// nestedReferenceParsed is nestedReferenceWith, naming the series by the parsed labels too when parsed is set
// (Loki's exact identity, see nvRowLabels).
func nestedReferenceParsed(s nvScenario, extra map[string]string, parsed bool) (map[string]map[int64]float64, map[string]map[string]string) {
	return nestedReferenceFor(s, func(r vlemu.Row) map[string]string {
		l := nvRowLabels(s.parser, parsed, r)
		for k, v := range extra {
			l[k] = v
		}
		return l
	})
}

// wpReference is the answer of a without pushdown scenario, whose streams carry label values the stream text
// escapes: each row's series labels are its stream's, with the service_name Loki gives it.
func wpReference(s nvScenario, streams []wpStream) map[string]map[int64]float64 {
	byText := map[string]map[string]string{}
	for _, st := range streams {
		byText[st.text] = st.labels
	}
	out, _ := nestedReferenceFor(s, func(r vlemu.Row) map[string]string {
		l := map[string]string{"service_name": byText[r.Stream]["app"]}
		for k, v := range byText[r.Stream] {
			l[k] = v
		}
		return l
	})
	return out
}

// nestedReferenceFor is nestedReference with each row's series labels from labelsOf.
func nestedReferenceFor(s nvScenario, labelsOf func(vlemu.Row) map[string]string) (map[string]map[int64]float64, map[string]map[string]string) {
	out := map[string]map[int64]float64{}
	labels := map[string]map[string]string{}
	for _, t := range s.times() {
		perSeries := map[string]float64{}
		seriesLabels := map[string]map[string]string{}
		for _, r := range s.rows {
			if r.TS > t-int64(s.window) && r.TS <= t {
				l := labelsOf(r)
				key := nvLabelsKey(l)
				seriesLabels[key] = l
				if s.fn == "rate" {
					perSeries[key] += 1 / s.window.Seconds()
				} else {
					perSeries[key]++
				}
			}
		}
		var vec []nvSeries
		for key, v := range perSeries {
			vec = append(vec, nvSeries{seriesLabels[key], v})
		}
		for _, level := range s.levels {
			vec = nvFold(vec, level)
		}
		for _, series := range vec {
			v := series.value
			if s.divisor != 0 {
				v /= s.divisor
			}
			key := nvLabelsKey(series.labels)
			if out[key] == nil {
				out[key] = map[int64]float64{}
			}
			out[key][t] = v
			labels[key] = series.labels
		}
	}
	return out, labels
}

// vaReference is the answer of a vector aggregation scenario (vector_agg_series_differential_test.go): the
// nested reference over its stages, with detected_level "unknown" (the level Loki gives a line with none) when the
// scenario compares it.
func vaReference(s vaScenario) (map[string]map[int64]float64, map[string]map[string]string) {
	levels := make([]nvLevel, len(s.stages))
	for i, st := range s.stages {
		levels[i] = nvLevel{op: st.op, by: st.by, without: st.without}
	}
	var extra map[string]string
	if s.withLevel {
		extra = map[string]string{"detected_level": "unknown"}
	}
	return nestedReferenceWith(nvScenario{levels: levels, fn: s.fn, parser: s.parser, window: s.window, step: s.step, start: s.start, end: s.end, rows: s.rows, divisor: s.divisor}, extra)
}

// nestedReferenceCases are scenarios of every level shape the nested differentials draw.
func nestedReferenceCases() []nvScenario {
	shapes := [][]nvLevel{
		{nvBy("avg", "pod", "zone"), nvBy("min", "zone")},
		{nvBy("count", "pod", "zone"), nvBy("sum", "zone")},
		{nvBy("max", "pod", "zone"), nvBy("count", "pod", "baz")},
		{nvBy("count", "baz"), nvBy("sum", "baz")},
		{nvBy("avg", "pod", "zone"), nvBy("min")},
		{nvBy("max", "pod", "zone"), {op: "count"}},
		{{op: "max"}, nvBy("sum", "zone")},
		{nvBy("avg", "pod", "zone"), nvWithout("min", "pod")},
		{nvBy("sum", "pod", "zone"), nvWithout("max", "baz")},
		{nvBy("min", "pod", "zone", "svc"), nvWithout("avg", "svc"), nvBy("count", "zone")},
		{nvWithout("avg", "pod"), nvBy("min", "zone")},
		{nvWithout("sum", "pod"), nvWithout("avg", "svc")},
		{nvWithout("max", "baz"), nvBy("count", "zone")},
		{nvWithout("count", "pod"), nvBy("max", "zone", "svc")},
		{nvWithout("sum")},
		{nvWithout("sum", "pod")},
	}
	rnd := rand.New(rand.NewSource(5150))
	var out []nvScenario
	for _, levels := range shapes {
		for _, fn := range []string{"count_over_time", "rate"} {
			for _, parser := range []string{"", "logfmt"} {
				for _, mode := range []string{"tumbling", "instant", "sliding"} {
					s := generateNestedScenario(rnd, fn, levels, mode == "instant", false, mode == "sliding")
					s.parser = parser
					if rnd.Intn(4) == 0 {
						s.divisor = 2
					}
					out = append(out, s)
				}
			}
		}
	}
	for _, stages := range [][]vaStage{{{op: "count", by: []string{"detected_level"}}}, {{op: "avg", by: []string{"pod"}, without: true}}, {{op: "max", by: []string{"pod"}}, {op: "sum"}}} {
		for _, parser := range []string{"", "logfmt"} {
			for _, instant := range []bool{false, true} {
				va := generateVectorAggScenario(rnd, "rate", stages, instant)
				va.parser, va.withLevel = parser, true
				levels := make([]nvLevel, len(stages))
				for i, st := range stages {
					levels[i] = nvLevel{op: st.op, by: st.by, without: st.without}
				}
				out = append(out, nvScenario{levels: levels, fn: va.fn, parser: va.parser, window: va.window, step: va.step, start: va.start, end: va.end, rows: va.rows, version: "level"})
			}
		}
	}
	return out
}

// The nested aggregation reference gives Loki's series, labels and values on every recorded scenario
// (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
func TestNestedReferenceMatchesRecordedLoki(t *testing.T) {
	cases := nestedReferenceCases()
	var sets []oraclerun.EngineDataSet
	for i, s := range cases {
		parser := s.parser
		withLevel := s.version == "level"
		streams := engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) {
			if withLevel {
				return r.Msg, map[string]string{"detected_level": "unknown"}
			}
			return r.Msg, nil
		})
		for j := range streams {
			streams[j].Labels["service_name"] = streams[j].Labels["app"]
		}
		_ = parser
		sets = append(sets, oraclerun.EngineDataSet{Streams: streams, Requests: []oraclerun.EngineRequest{lokiMetricRequest(fmt.Sprint(i), s.query(), s.start, s.end, s.step)}})
	}
	type wpCase struct {
		s       nvScenario
		streams []wpStream
	}
	var wpCases []wpCase
	wrnd := rand.New(rand.NewSource(6060))
	for i := 0; i < 80; i++ {
		fn := []string{"count_over_time", "rate"}[i%2]
		ws, streams := wpGenerateScenario(wrnd, fn)
		wpCases = append(wpCases, wpCase{ws, streams})
		labels := map[string]map[string]string{}
		for _, st := range streams {
			l := map[string]string{"service_name": st.labels["app"]}
			for k, v := range st.labels {
				l[k] = v
			}
			labels[st.text] = l
		}
		var engine []oraclerun.EngineStream
		index := map[string]int{}
		for _, r := range ws.rows {
			j, ok := index[r.Stream]
			if !ok {
				j = len(engine)
				index[r.Stream] = j
				engine = append(engine, oraclerun.EngineStream{Labels: labels[r.Stream]})
			}
			engine[j].Entries = append(engine[j].Entries, oraclerun.EngineEntry{TS: r.TS, Line: r.Msg})
		}
		sets = append(sets, oraclerun.EngineDataSet{Streams: engine, Requests: []oraclerun.EngineRequest{lokiMetricRequest(fmt.Sprintf("wp%d", i), ws.query(), ws.start, ws.end, ws.step)}})
	}
	answers := lokiEngineAnswers(t, "nested_aggregation_reference", sets)
	for i, c := range wpCases {
		id := fmt.Sprintf("wp%d", i)
		points, labels := lokiMetricPoints(t, id, answers[id])
		loki := map[string]map[int64]float64{}
		for key, series := range points {
			loki[nvLabelsKey(labels[key])] = series
		}
		if diff := wpEqual(wpReference(c.s, c.streams), loki); diff != "" {
			t.Fatalf("%s %s: reference against Loki: %s", id, c.s, diff)
		}
	}
	for i, s := range cases {
		points, labels := lokiMetricPoints(t, fmt.Sprint(i), answers[fmt.Sprint(i)])
		loki := map[string]map[int64]float64{}
		for key, series := range points {
			loki[nvLabelsKey(labels[key])] = series
		}
		var extra map[string]string
		if s.version == "level" {
			extra = map[string]string{"detected_level": "unknown"}
		}
		// The stream identity the differentials use is Loki's answer less the parsed labels.
		stream, _ := nestedReferenceParsed(s, extra, false)
		lessParsed := map[string]map[int64]float64{}
		for key, series := range points {
			l := map[string]string{}
			for k, v := range labels[key] {
				if k != "x" || s.parser == "" {
					l[k] = v
				}
			}
			if lessParsed[nvLabelsKey(l)] != nil {
				t.Fatalf("#%d %s: two Loki series without the parsed labels: %v", i, s, l)
			}
			lessParsed[nvLabelsKey(l)] = series
		}
		if diff := wpEqual(stream, lessParsed); diff != "" {
			t.Fatalf("#%d %s: stream-identity reference against Loki less parsed labels: %s", i, s, diff)
		}
		want, _ := nestedReferenceParsed(s, extra, true)
		if len(want) != len(loki) {
			t.Fatalf("#%d %s: reference %v, Loki %v", i, s, want, loki)
		}
		for key, pts := range want {
			if len(loki[key]) != len(pts) {
				t.Fatalf("#%d %s: %s reference %v, Loki %v", i, s, key, pts, loki[key])
			}
			for ts, v := range pts {
				lv, ok := loki[key][ts]
				if !ok || math.Abs(lv-v) > 1e-9*math.Max(1, math.Abs(v)) {
					t.Fatalf("#%d %s: %s at %d reference %v, Loki %v", i, s, key, ts/int64(time.Second), v, lv)
				}
			}
		}
	}
}
