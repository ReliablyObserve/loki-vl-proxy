package querycorpus_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The data each metric query of the Loki metric differential runs over (plan_loki_vl_metric_diff_test.go): the
// streams and lines of the log differential's fixture (plan_loki_vl_diff_fixture_test.go) for every log selector
// of the query, spread over time, plus lines that hold each unwrapped label.
//
//   - Streams: per log selector, two it selects and one it does not (when a value can be found); a stream two
//     selectors share is held once.
//   - Structured metadata: only the sets the log differential makes for a label filter's names. Loki's metric series
//     carry every structured metadata label, so a per-line trace_id on the catalogue lines would split every
//     stream's series, the proxy's owner-kept stream identity (metric-identity/structured-metadata-splits-series);
//     it would be the difference of nearly every query and hide the others.
//   - Levels: every line carries a detected_level structured metadata value, one per stream, so neither side derives
//     it: Loki keeps the value pushed with the line (its ingest adds one only when none is there) and the proxy reads
//     the stored field. A stream has no level label, so no level label decides it either.
//   - Lines: the log differential's catalogue and each selector's harvested lines, then per unwrapped label one json
//     and one logfmt line per value: numbers, durations or byte sizes as the conversion reads them, and a value it
//     rejects (Loki's __error__ samples).
//   - Time: the lines are spread over the request window, one range and the largest offset before it, with an empty
//     stretch in the middle (absent_over_time, zero fill). Every line is half a millisecond off a millisecond and every
//     request bound and step is whole milliseconds, so no line is on or near a window edge, where the window
//     conventions of the two stores would decide alone (the step alignment, zero fill and window edge tests hold
//     those: semantics/window-edge-float64-nanoseconds).
//
// Everything is a function of the plan, so the data and its hash are stable from run to run.

// metricFixtureVersion changes when the generator's output changes on purpose; it is part of the recorded hash.
const metricFixtureVersion = "2"

// metricMaxEntries bounds a data set.
const metricMaxEntries = 240

// metricEnd is the end of every request window, the time of the instant requests: in the past, so no freshness or
// recent-tail rule applies.
var metricEnd = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// metricWindow is the window of the range requests.
const metricWindow = time.Hour

type metricEntry struct {
	Stream   int               `json:"stream"`
	TS       int64             `json:"ts"`
	Line     string            `json:"line"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type metricFixture struct {
	Streams []map[string]string `json:"streams"`
	Entries []metricEntry       `json:"entries"`
	// Selected is how many entries a selector of the query selects.
	Selected int `json:"-"`
}

// metricStreamLevels are the detected_level values of the streams, by stream index.
var metricStreamLevels = []string{"info", "error", "warn", "debug"}

// unwrapSamples are the values of an unwrapped label per conversion: the ones it reads, then one it rejects.
var unwrapSamples = map[string][]string{
	"":                 {"3", "0.25", "-2", "1e3", "42", "abc"},
	"bytes":            {"10KB", "512B", "1.5MiB", "3", "abc"},
	"duration":         {"1.5s", "250ms", "2m", "3", "abc"},
	"duration_seconds": {"1.5s", "250ms", "2m", "3", "abc"},
}

// logSelects are the log selectors of a metric plan, in order, each once.
func logSelects(n queryplan.Node) []*queryplan.LogSelect {
	var out []*queryplan.LogSelect
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch v := n.(type) {
		case *queryplan.LogSelect:
			out = append(out, v)
		case *queryplan.RangeAgg:
			walk(v.In)
		case *queryplan.VectorAgg:
			walk(v.In)
		case *queryplan.Rank:
			walk(v.In)
		case *queryplan.LabelReplace:
			walk(v.In)
		case *queryplan.BinOp:
			walk(v.L)
			walk(v.R)
		}
	}
	walk(n)
	return out
}

// metricLookback is the longest range plus offset of a plan's range aggregations (5m when it has none).
func metricLookback(n queryplan.Node) time.Duration {
	var best time.Duration
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch v := n.(type) {
		case *queryplan.RangeAgg:
			best = max(best, v.Range+max(v.Offset, 0))
		case *queryplan.VectorAgg:
			walk(v.In)
		case *queryplan.Rank:
			walk(v.In)
		case *queryplan.LabelReplace:
			walk(v.In)
		case *queryplan.BinOp:
			walk(v.L)
			walk(v.R)
		}
	}
	walk(n)
	if best == 0 {
		best = 5 * time.Minute
	}
	return best
}

// buildMetricFixture is the data of a metric query.
func buildMetricFixture(p *queryplan.Plan) metricFixture {
	var f metricFixture
	streamIndex := func(s map[string]string) int {
		for i, x := range f.Streams {
			if reflect.DeepEqual(x, s) {
				return i
			}
		}
		f.Streams = append(f.Streams, s)
		return len(f.Streams) - 1
	}
	type line struct {
		stream   int
		text     string
		metadata map[string]string
	}
	var selected, unselected []line
	for _, sel := range logSelects(p.Root) {
		base := buildDiffFixture(sel)
		named := map[string]bool{}
		for _, m := range sel.Matchers {
			named[m.Name] = true
		}
		idx := make([]int, len(base.Streams))
		for i, s := range base.Streams {
			// A level stream label the selector does not name is left out: the stream's level is its structured
			// metadata alone.
			if !named["level"] {
				delete(s, "level")
			}
			idx[i] = streamIndex(s)
		}
		nsel := len(base.Streams)
		if hasUnselected(sel, base) {
			nsel--
		}
		for _, e := range base.Entries {
			l := line{stream: idx[e.Stream], text: e.Line}
			if strings.HasPrefix(e.Line, "metadata line ") {
				// Only the structured metadata a label filter names is kept (see the comment above).
				l.metadata = e.Metadata
			}
			if e.Stream < nsel {
				selected = append(selected, l)
			} else {
				unselected = append(unselected, l)
			}
		}
		if nsel == 0 {
			continue
		}
		for i, u := range unwrapsOf(sel) {
			for j, v := range unwrapSamples[u.Converter] {
				s := idx[(i+j)%nsel]
				selected = append(selected, line{stream: s, text: jsonLine(u.Label, v, u.Converter == "" && v != "abc")},
					line{stream: s, text: logfmtLine(u.Label, v)})
			}
		}
	}
	if len(selected) > metricMaxEntries-len(unselected) {
		selected = selected[:max(metricMaxEntries-len(unselected), 0)]
	}
	// The lines of a stream no selector selects go between the others, so they share the window.
	all := make([]line, 0, len(selected)+len(unselected))
	step := 1
	if len(unselected) > 0 {
		step = max(len(selected)/len(unselected), 1)
	}
	u := 0
	for i, l := range selected {
		all = append(all, l)
		if (i+1)%step == 0 && u < len(unselected) {
			all = append(all, unselected[u])
			u++
		}
	}
	all = append(all, unselected[u:]...)
	f.Selected = len(selected)
	span := metricWindow + metricLookback(p.Root) + 10*time.Minute
	for i, l := range all {
		md := map[string]string{"detected_level": metricStreamLevels[l.stream%len(metricStreamLevels)]}
		for k, v := range l.metadata {
			md[k] = v
		}
		f.Entries = append(f.Entries, metricEntry{Stream: l.stream, TS: metricTime(i, len(all), span), Line: l.text, Metadata: md})
	}
	return f
}

// metricTime is the time of entry i of n over span before metricEnd: evenly spread over the span without its
// middle stretch (between 45% and 60% of it), half a millisecond past a millisecond (every request bound and step is
// whole milliseconds, so every window edge is too), every time distinct.
func metricTime(i, n int, span time.Duration) int64 {
	p := 0.85 * (float64(i) + 0.5) / float64(n)
	if p >= 0.45 {
		p += 0.15
	}
	t := metricEnd.Add(-span).UnixNano() + int64(p*float64(span))
	t -= t % int64(time.Second)
	return t + int64(137*time.Millisecond) + int64(i*7919%700)*int64(time.Millisecond) + int64(500*time.Microsecond) + int64(i)
}

// unwrapsOf are the unwrap stages of a selector's pipeline.
func unwrapsOf(sel *queryplan.LogSelect) []*queryplan.Unwrap {
	var out []*queryplan.Unwrap
	for _, st := range sel.Stages {
		if u, ok := st.(*queryplan.Unwrap); ok {
			out = append(out, u)
		}
	}
	return out
}

// hash is the fixture's identity in the recorded answers.
func (f metricFixture) hash() string {
	b, _ := json.Marshal(f)
	return shortHash(metricFixtureVersion + "\x00" + string(b))
}

// lokiStreams are the fixture as the Loki engine oracle reads it.
func (f metricFixture) lokiStreams() []oraclerun.EngineStream {
	out := make([]oraclerun.EngineStream, len(f.Streams))
	for i, s := range f.Streams {
		out[i].Labels = s
	}
	for _, e := range f.Entries {
		out[e.Stream].Entries = append(out[e.Stream].Entries, oraclerun.EngineEntry{TS: e.TS, Line: e.Line, Metadata: e.Metadata})
	}
	return out
}

// vlRows are the fixture's lines as the Loki push API hands them to VictoriaLogs (stream labels, then structured
// metadata, then _msg), names in the profile's upstream spelling (the log differential's rule), detected_level as is.
func (f metricFixture) vlRows(toVL func(string) string) []oraclerun.ServeRow {
	out := make([]oraclerun.ServeRow, 0, len(f.Entries))
	for _, e := range f.Entries {
		var stream, fields [][2]string
		for _, k := range sortedKeys(f.Streams[e.Stream]) {
			stream = append(stream, [2]string{toVL(k), f.Streams[e.Stream][k]})
		}
		for _, k := range sortedKeys(e.Metadata) {
			name := toVL(k)
			if k == "detected_level" {
				// The Loki push API stores structured metadata under its own name: the level is the field
				// detected_level, whatever name the profile reads a level label from.
				name = k
			}
			fields = append(fields, [2]string{name, e.Metadata[k]})
		}
		fields = append(fields, [2]string{"_msg", e.Line})
		out = append(out, oraclerun.ServeRow{TS: e.TS, Stream: stream, Fields: fields})
	}
	return out
}
