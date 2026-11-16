package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// stdRangeValue is a log range function over the lines of one series in a window: count_over_time the number
// of lines, rate that per second, bytes_over_time their bytes, bytes_rate those per second.
func stdRangeValue(fn string, lines []string, window int64) float64 {
	n, bytes := float64(len(lines)), 0.0
	for _, l := range lines {
		bytes += float64(len(l))
	}
	seconds := float64(window) / 1e9
	switch fn {
	case "rate":
		return n / seconds
	case "bytes_over_time":
		return bytes
	case "bytes_rate":
		return bytes / seconds
	}
	return n
}

// stddevReference is op (stddev or stdvar) with grouping g over fn of each series at every time: group labels
// (canonicalLabels) -> t -> the population deviation or variance of the group's series values (0 for one series).
// A series is a row's stream labels with the service_name Loki gives the stream and the labels the row's parser
// extracts.
func stddevReference(rows []vlemu.Row, window int64, times []int64, fn, op string, g stdGrouping) map[string]map[int64]float64 {
	layer := aggLayer{op: op}
	switch g.kind {
	case "by":
		layer.hasBy, layer.by = true, g.labels
	case "without":
		layer.hasWith, layer.without = true, g.labels
	}
	out := map[string]map[int64]float64{}
	for _, t := range times {
		lines := map[string][]string{}
		labels := map[string]map[string]string{}
		for _, r := range rows {
			if r.TS > t-window && r.TS <= t {
				l := vlemu.StreamLabels(r.Stream)
				l["service_name"] = l["app"]
				for k, v := range r.Labels {
					l[k] = v
				}
				key := canonicalLabels(l)
				labels[key] = l
				lines[key] = append(lines[key], r.Msg)
			}
		}
		var vec []topKFoldRefSample
		for key, ls := range lines {
			vec = append(vec, topKFoldRefSample{labels[key], stdRangeValue(fn, ls, window)})
		}
		folded, _ := layer.eval(vec)
		for _, s := range folded {
			key := canonicalLabels(s.labels)
			if out[key] == nil {
				out[key] = map[int64]float64{}
			}
			out[key][t] = s.value
		}
	}
	return out
}

// The stddev and stdvar reference gives Loki's groups and values on recorded scenarios of both differentials: every
// grouping and clause position, sliding, tumbling and instant, count, rate and bytes functions, by a parsed label
// (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again). The Loki streams carry the env label the query
// selects, which the comparison leaves out of Loki's series (the emulated VictoriaLogs answers without it).
//
// conformance: semantics/stddev-stdvar-grouped-values, metric-series-identity
func TestStddevReferenceMatchesRecordedLoki(t *testing.T) {
	groupings := []stdGrouping{{}, {kind: "by", labels: []string{"app"}}, {kind: "by", labels: []string{"app"}, trailing: true}, {kind: "by", labels: []string{"app", "pod"}},
		{kind: "by"}, {kind: "without", labels: []string{"pod"}}, {kind: "without", labels: []string{"app", "pod"}},
		{kind: "by", labels: []string{"route"}}, {kind: "by", labels: []string{"app", "route"}}}
	rnd := rand.New(rand.NewSource(31337))
	var cases []stdScenario
	var sets []oraclerun.EngineDataSet
	for _, op := range []string{"stddev", "stdvar"} {
		for _, g := range groupings {
			parser := len(g.labels) > 0 && g.labels[len(g.labels)-1] == "route"
			for _, mode := range []string{"sliding", "tumbling", "instant"} {
				for _, fn := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
					s := generateStdScenario(rnd, op, g, mode == "instant", mode == "tumbling", true, parser)
					s.fn = fn
					id := fmt.Sprint(len(cases))
					cases = append(cases, s)
					streams := engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) { return r.Msg, nil })
					for j := range streams {
						streams[j].Labels["env"] = "production"
						streams[j].Labels["service_name"] = streams[j].Labels["app"]
					}
					sets = append(sets, oraclerun.EngineDataSet{Streams: streams, Requests: []oraclerun.EngineRequest{lokiMetricRequest(id, s.query(), s.start, s.end, s.step)}})
				}
			}
		}
	}
	answers := lokiEngineAnswers(t, "stddev_reference", sets)
	for i, s := range cases {
		id := fmt.Sprint(i)
		points, labels := lokiMetricPoints(t, id, answers[id])
		loki := map[string]map[int64]float64{}
		for key, series := range points {
			l := map[string]string{}
			for k, v := range labels[key] {
				if k != "env" {
					l[k] = v
				}
			}
			loki[canonicalLabels(l)] = series
		}
		want := stddevReference(s.rows, int64(s.window), s.times(), s.function(), s.op, s.grouping)
		if len(want) != len(loki) {
			t.Fatalf("#%d %s: reference %v, Loki %v", i, s.query(), want, loki)
		}
		for key, pts := range want {
			if len(loki[key]) != len(pts) {
				t.Fatalf("#%d %s: %s reference %v, Loki %v", i, s.query(), key, pts, loki[key])
			}
			for ts, v := range pts {
				if lv, ok := loki[key][ts]; !ok || math.Abs(lv-v) > 1e-9*math.Max(1, math.Abs(v)) {
					t.Fatalf("#%d %s: %s at %d reference %v, Loki %v", i, s.query(), key, ts, v, lv)
				}
			}
		}
	}
}
