package proxy

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// rankingBinaryOperandRows are three streams in two jobs with a line every 12 s, 60 s and 20 s (5, 1 and 3 lines a
// minute, off the minute boundaries) from 15 minutes before at until at: every window of the queries below is full,
// and no two series tie in a ranking. No line holds a parenthesis, a `>` or an x, so every line filter of the queries
// keeps every line on Loki and on the emulated VictoriaLogs (which reads no line filter).
func rankingBinaryOperandRows(at int64) []vlemu.Row {
	var rows []vlemu.Row
	for _, s := range []struct {
		stream        string
		every, offset int64
	}{
		{`{app="uw",job="a",pod="p1"}`, 12, 3},
		{`{app="uw",job="a",pod="p2"}`, 60, 29},
		{`{app="uw",job="b",pod="p3"}`, 20, 7},
	} {
		for ts := at - 15*60 + s.offset; ts < at; ts += s.every {
			rows = append(rows, vlemu.Row{TS: ts * 1e9, Stream: s.stream, Msg: "line " + strconv.FormatInt(ts, 10)})
		}
	}
	return rows
}

// rankingBinaryOperandQueries are the rankings, stddev, stdvar and sort over a binary operation of op, an operand
// written with the line filter or matcher given: a parenthesis, an operator or none in a quoted value, chains, bool
// and vector(n) operands, and a label_replace inside an operand, which rewrites a label once.
func rankingBinaryOperandQueries(op string) []string {
	x := op
	return []string{
		`topk(1, ` + x + ` * 2)`,
		`bottomk(1, ` + x + ` * 2)`,
		`topk by (job) (1, ` + x + ` * 2)`,
		`stddev(` + x + ` * 2)`,
		`stdvar(` + x + ` * 2)`,
		`sort_desc(` + x + ` * 2)`,
		`topk(1, sum by (pod) (` + x + `) * 2)`,
		`topk(1, ` + x + ` * 2 + 1)`,
		`stddev(` + x + ` > bool 4)`,
		`topk(1, sum(` + x + `) > bool 4)`,
		`topk(1, ` + x + ` + vector(1))`,
		`topk(1, sum(` + x + `) + vector(1))`,
		`sum(topk(1, ` + x + ` * 2))`,
		`topk(1, label_replace(` + x + `, "pod", "x-$1", "pod", "(.*)") * 2)`,
		`sort_desc(label_replace(` + x + `, "pod", "x-$1", "pod", "(.*)") * 2)`,
		`topk(1, label_replace(` + x + ` * 2, "pod", "x-$1", "pod", "(.*)"))`,
	}
}

// A ranking, stddev, stdvar or sort over a binary operation answers Loki's series and values on query_range as on
// query, whatever the operand's quoted text holds (a parenthesis, an operator, a matcher value: the string translator's
// text for such an operand drops the operation, the binary route reads its tree), over chains, bool and vector(n)
// operands, at a step equal to and shorter than the range, and with a label_replace inside the operand applied once
// (the handlers applied it again over the answer: pod="x-x-p1"). Loki's answers are recorded from its engine over the same lines
// (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again); the Loki streams carry the service_name Loki gives them.
// Each request runs against the emulated VictoriaLogs v1.49.0 and v1.54.0.
//
// conformance: semantics/rank-binary-operand-label-replace-once, loki_api_v1_query_range, loki_api_v1_query
func TestRankingOverBinaryOperandMatchesRecordedLoki(t *testing.T) {
	at := int64(1_800_000_000)
	start, end := at-10*60, at
	rows := rankingBinaryOperandRows(at)
	streams := engineStreams(rows, func(r vlemu.Row) (string, map[string]string) { return r.Msg, nil })
	for i := range streams {
		streams[i].Labels["service_name"] = streams[i].Labels["app"]
	}
	type request struct {
		id, query string
		step      time.Duration
	}
	var requests []request
	set := oraclerun.EngineDataSet{Streams: streams}
	for _, window := range []string{"1m", "5m"} {
		for _, operand := range []string{
			`count_over_time({app="uw"} != "(" [` + window + `])`,
			`count_over_time({app="uw"} != ")" [` + window + `])`,
			`count_over_time({app="uw"} != "a > b" [` + window + `])`,
			`count_over_time({app="uw", pod!="("} [` + window + `])`,
			`rate({app="uw"} != "x" [` + window + `])`,
		} {
			for _, q := range rankingBinaryOperandQueries(operand) {
				for _, step := range []time.Duration{0, 15 * time.Second, time.Minute} {
					id := fmt.Sprintf("%d", len(requests))
					requests = append(requests, request{id: id, query: q, step: step})
					set.Requests = append(set.Requests, lokiMetricRequest(id, q, start*1e9, end*1e9, step))
				}
			}
		}
	}
	answers := lokiEngineAnswers(t, "ranking_binary_operand", []oraclerun.EngineDataSet{set})
	for _, version := range []string{"v1.49.0", "v1.54.0"} {
		emu := &vlemu.Emulator{T: t, Rows: rows, Version: version}
		backend := httptest.NewServer(emu.Handler())
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, req := range requests {
			label := fmt.Sprintf("%s step=%v %s", version, req.step, req.query)
			want := rankingLokiSeries(t, label, answers[req.id])
			params := url.Values{"query": {req.query}}
			target := "/loki/api/v1/query_range?"
			if req.step == 0 {
				target = "/loki/api/v1/query?"
				params.Set("time", strconv.FormatInt(end, 10))
			} else {
				params.Set("start", strconv.FormatInt(start, 10))
				params.Set("end", strconv.FormatInt(end, 10))
				params.Set("step", strconv.FormatInt(int64(req.step/time.Second), 10))
			}
			res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Errorf("%s: proxy %d %.300s", label, res.Code, res.Body.String())
				continue
			}
			got, err := decodeLokiSeries(res.Body.Bytes())
			if err != nil {
				t.Fatalf("%s: %v %s", label, err, res.Body.String())
			}
			compareRankingSeries(t, label, rankingProxySeries(t, label, got), want)
		}
		_ = p.Shutdown(context.Background())
		backend.Close()
	}
}

// rankingLokiSeries is a recorded Loki answer as series (label text) -> time in ms -> value.
func rankingLokiSeries(t *testing.T, label string, a lokiEngineAnswer) map[string]map[int64]float64 {
	t.Helper()
	if a.Status != http.StatusOK {
		t.Fatalf("%s: Loki answered %d %s", label, a.Status, a.Error)
	}
	s, err := decodeLokiSeries(a.Body)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	return rankingProxySeries(t, label, s)
}

// rankingProxySeries reads the values of a decoded answer: series (label text, without the detected_level Loki's
// ingest and the proxy's read path give a line with no level) -> time in ms -> value.
func rankingProxySeries(t *testing.T, label string, s lokiSeries) map[string]map[int64]float64 {
	t.Helper()
	out := map[string]map[int64]float64{}
	for key, points := range s.Points {
		labels := map[string]string{}
		for k, v := range s.Labels[key] {
			if k != "detected_level" {
				labels[k] = v
			}
		}
		series := map[int64]float64{}
		for ms, text := range points {
			v, err := strconv.ParseFloat(text, 64)
			if err != nil {
				t.Fatalf("%s: value %q", label, text)
			}
			series[ms] = v
		}
		out[canonicalLabelText(labels)] = series
	}
	return out
}

func compareRankingSeries(t *testing.T, label string, got, want map[string]map[int64]float64) {
	t.Helper()
	keys := func(m map[string]map[int64]float64) string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return strings.Join(out, " ")
	}
	if keys(got) != keys(want) {
		t.Errorf("%s:\n series %s\n Loki   %s\n got %v\nwant %v", label, keys(got), keys(want), got, want)
		return
	}
	for key, points := range want {
		if len(got[key]) != len(points) {
			t.Errorf("%s: %s has %d points, Loki %d\n got %v\nwant %v", label, key, len(got[key]), len(points), got[key], points)
			continue
		}
		for ts, w := range points {
			if g, ok := got[key][ts]; !ok || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Errorf("%s: %s at %d got %v (present %v), Loki %v", label, key, ts, g, ok, w)
			}
		}
	}
}
