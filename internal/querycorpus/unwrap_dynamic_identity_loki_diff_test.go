package querycorpus_test

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The Loki differential of the series a range function over an unwrapped label reads after a json or logfmt parser
// with no keep: real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle,
// test/oracle/vl, v1.53.0 and v1.49.0) over the same data, through the proxy's real query and query_range handlers,
// as the metric differential runs them (plan_loki_vl_metric_diff_test.go).
//
// A Loki series is the stream and every label the parser extracts, less the unwrapped one: the samples of the lines
// that agree on all of those are one series, whatever their values. The data holds every case of that identity
// (dynamicIdentityFixture), on json and logfmt streams of two apps:
//
//   - lines that differ only in the unwrapped value (one series of many samples);
//   - lines that are the same text, value included (one series, the same sample many times);
//   - lines that differ in a parsed label only (a series each), and lines with a key of their own;
//   - a nested json object (its flattened labels) and a line without the unwrapped label;
//   - lines of one series that write its keys in another order (dk), carry another structured metadata value (dm,
//     which Loki's series hold too) or do not parse (de, with and without a filter on __error__).
//
// Requests: an instant query, and query_range with the step equal to the range, a fifth of it and three times it.
// The answers are recorded (dynamicIdentityAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match
// Loki, or be listed in dynamicIdentityKnown with the registry case that holds the difference open; a listed run that
// matches fails too, so the list only shrinks.

const dynamicIdentityAnswersPath = "testdata/unwrap-dynamic-identity-loki-diff.jsonl.gz"

// dynamicIdentityFuncs are the functions taken per series that an outer aggregation folds.
var dynamicIdentityFuncs = []string{
	"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time", "quantile_over_time",
	"rate_counter",
}

// dynamicIdentityShapes are the query shapes, %s the function call's head.
var dynamicIdentityShapes = []string{
	`sum by (app) (%s{job="dj"} | json | unwrap duration_ms [5m]))`,
	`sum(%s{job="dj"} | json | unwrap duration_ms [5m]))`,
	`max by (app) (%s{job="dj"} | json | unwrap duration_ms [5m]))`,
	`sum by (app) (%s{job="dl"} | logfmt | unwrap duration_ms [5m]))`,
	`avg(%s{job="dl"} | logfmt | unwrap duration_ms [5m]))`,
	`sum(%s{job="dk"} | json | unwrap duration_ms [5m]))`,
	`sum(%s{job="dm"} | json | unwrap duration_ms [5m]))`,
	`sum(%s{job="de"} | json | unwrap duration_ms [5m]))`,
	`sum(%s{job="de"} | json | __error__="" | unwrap duration_ms [5m]))`,
}

// dynamicIdentityKnown are the runs that differ from Loki, by query and request kind, with the registry case that
// holds the difference open.
var dynamicIdentityKnown = func() map[string]map[string]string {
	all := []string{"instant", "step=range", "step<range", "step>range"}
	known := map[string]map[string]string{}
	add := func(id string, kinds []string, queries ...string) {
		for _, q := range queries {
			if known[q] == nil {
				known[q] = map[string]string{}
			}
			for _, k := range kinds {
				known[q][k] = id
			}
		}
	}
	const perSeries = "semantics/unwrap-per-series-identity-raw-rows"
	for _, shape := range dynamicIdentityShapes {
		q := fmt.Sprintf(shape, keepIdentityHead("rate_counter"))
		if strings.Contains(q, `"dk"`) || strings.Contains(q, `"de"`) {
			// One series per stream: the series are right, the value differs from Loki's extrapolated rate in the
			// sixth digit.
			add("semantics/unwrap-rate-counter-window-extrapolation", all, q)
			continue
		}
		// rate_counter keeps the stream's series.
		add(perSeries, all, q)
	}
	// quantile_over_time keeps the stream's series (the group's samples under sum by): Loki's answer where a stream
	// is one series.
	for _, shape := range dynamicIdentityShapes {
		q := fmt.Sprintf(shape, keepIdentityHead("quantile_over_time"))
		switch {
		case strings.Contains(q, `"dk"`) || strings.Contains(q, `"de"`):
		case strings.HasPrefix(q, "sum by (app) ("):
			add("semantics/unwrap-quantile-sum-by-group-quantile", all, q)
		default:
			add(perSeries, all, q)
		}
	}
	// A range longer than the step whose series are not all of one sample: the window evaluator merges the step
	// buckets of streams.
	sliding := []string{"step<range"}
	for _, fn := range []string{"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time"} {
		for _, shape := range dynamicIdentityShapes {
			q := fmt.Sprintf(shape, keepIdentityHead(fn))
			if strings.Contains(q, `__error__`) || fn == "max_over_time" && strings.HasPrefix(q, "max by (app) (") {
				continue
			}
			add(perSeries, sliding, q)
		}
	}
	// A label filter between the parser and an unwrapped label whose name holds an underscore (duration_ms, which the
	// line can spell duration.ms or duration-ms): the lowering does not serve the query, and the string translator's
	// series are the stream and each line.
	for _, fn := range []string{"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time"} {
		add("semantics/unwrap-per-series-identity-label-filter", all[1:], fmt.Sprintf(`sum(%s{job="de"} | json | __error__="" | unwrap duration_ms [5m]))`, keepIdentityHead(fn)))
	}
	return known
}()

// dynamicIdentityQueries are the differential's queries over dynamicIdentityFixture.
func dynamicIdentityQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := dynamicIdentityFixture()
	var out []metricQuery
	for _, fn := range dynamicIdentityFuncs {
		for _, shape := range dynamicIdentityShapes {
			q := fmt.Sprintf(shape, keepIdentityHead(fn))
			p, err := planOfText(q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
		}
	}
	return out
}

// dynamicIdentityLine is the line of pattern k with value v, in json or in logfmt.
func dynamicIdentityLine(k int, v string, logfmt bool) string {
	type kv struct{ k, v string }
	var fields []kv
	switch k {
	case 0, 1:
		// Only the value differs from line to line: one series.
		fields = []kv{{"duration_ms", v}, {"route", "/a"}}
	case 2:
		// The same, and a parsed label more: a series of its own.
		fields = []kv{{"duration_ms", v}, {"route", "/a"}, {"region", "eu"}}
	case 3:
		// The same text every time, value included.
		fields = []kv{{"duration_ms", "7"}, {"route", "/c"}}
	case 4:
		// A key of its own on every line: a series per line.
		fields = []kv{{"duration_ms", v}, {"req", "r" + v}}
	case 5:
		// No unwrapped label: no sample.
		fields = []kv{{"route", "/d"}}
	}
	var b strings.Builder
	if logfmt {
		for i, f := range fields {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(f.k + "=" + f.v)
		}
		return b.String()
	}
	b.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		if f.k == "route" || f.k == "region" || f.k == "req" {
			b.WriteString(strconv.Quote(f.k) + ":" + strconv.Quote(f.v))
		} else {
			b.WriteString(strconv.Quote(f.k) + ":" + f.v)
		}
	}
	if k == 1 {
		// A nested object of the same keys on every line of the pattern: labels of their own, one series.
		b.WriteString(`,"ctx":{"zone":"z1"}`)
	}
	b.WriteByte('}')
	return b.String()
}

// dynamicIdentityFixture is the data of every query, a line every 13 seconds per stream: json streams of job dj and
// logfmt streams of job dl (two of app=api and one of app=web each) cycling through the patterns of
// dynamicIdentityLine, and a stream each of one parsed series whose lines write the keys in either order (dk), carry
// another detected_level now and then (dm) or now and then do not parse (de).
func dynamicIdentityFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "dj", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "dj", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "dj", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "dl", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "dl", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "dl", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "dk", "app": "api", "pod": "k", "service_name": "api"},
		{"job": "dm", "app": "api", "pod": "m", "service_name": "api"},
		{"job": "de", "app": "api", "pod": "e", "service_name": "api"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 13 * time.Second
	for s, labels := range f.Streams {
		for i := 0; ; i++ {
			// Each stream on a phase of its own, half a millisecond past a millisecond (every bound and step is whole
			// milliseconds, so no line sits on a window edge).
			at := first.Add(time.Duration(i)*every + time.Duration(s)*1300*time.Millisecond + time.Duration((i*7919+s*31)%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			v := strconv.Itoa((i*7919 + s*104729) % 5000)
			level := metricStreamLevels[s%len(metricStreamLevels)]
			var line string
			switch labels["job"] {
			case "dj", "dl":
				line = dynamicIdentityLine(i%6, v, labels["job"] == "dl")
			case "dk":
				// The keys of one series in either order.
				line = `{"duration_ms":` + v + `,"route":"/a"}`
				if i%2 == 1 {
					line = `{"route":"/a","duration_ms":` + v + `}`
				}
			case "dm":
				// One parsed series, and a detected_level of another value now and then.
				line = `{"duration_ms":` + v + `,"route":"/a"}`
				if i%3 == 2 {
					level = "error"
				}
			case "de":
				// One parsed series, and now and then a line that does not parse.
				line = `{"duration_ms":` + v + `,"route":"/a"}`
				if i%5 == 4 {
					line = `{"route":"/a",`
				}
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line,
				Metadata: map[string]string{"detected_level": level}})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestUnwrapDynamicIdentityMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-quantile-sum-by-group-quantile, semantics/stdvar-grouped-pooled-variance
func TestUnwrapDynamicIdentityMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := dynamicIdentityQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, dynamicIdentityAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, dynamicIdentityAnswersPath)
	report := os.Getenv("LVP_KEEPID_REPORT") == "1"
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				d := compareMetric(loki, run)
				key := prof.name + " " + mr.kind
				if table[key] == nil {
					table[key] = &row{}
				}
				table[key].runs++
				known := dynamicIdentityKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case report:
					if len(d.classes) == 0 {
						table[key].match++
						continue
					}
					if prof.name == diffProfiles()[0].name {
						var cs []string
						for c, ex := range d.classes {
							cs = append(cs, c+": "+ex)
						}
						sort.Strings(cs)
						t.Logf("DIFF %s | %s | %s", mr.kind, mq.q, strings.Join(cs, "; "))
					}
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from dynamicIdentityKnown (%s)", prof.name, mr.kind, mq.q, known)
				case len(d.classes) == 0:
					table[key].match++
				case known == "":
					for c, ex := range d.classes {
						t.Errorf("%s %s %s: %s: %s", prof.name, mr.kind, mq.q, c, ex)
					}
				}
			}
		}
	}
	if !report {
		for q, kinds := range dynamicIdentityKnown {
			for kind := range kinds {
				if !seenKnown[q+"\x00"+kind] {
					t.Errorf("dynamicIdentityKnown lists %s %s, which the differential does not run", kind, q)
				}
			}
		}
	}
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %-32s %3d of %3d match Loki", k, table[k].match, table[k].runs)
	}
	t.Logf("the series of a parser's labels against Loki:%s", b.String())
}
