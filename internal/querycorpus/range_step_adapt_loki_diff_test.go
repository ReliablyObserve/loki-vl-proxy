package querycorpus_test

import (
	"encoding/json"
	"math"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of the adaptive step (internal/proxy step_adapt.go, issue #1795): range metric shapes whose
// read at Grafana's fine step over a short range passes a bound the proxy sets itself (the rows of one evaluation, the
// bytes it buffers from one VictoriaLogs response), while Loki answers them. Real Loki (the engine oracle) answers at
// the requested step; the proxy, in front of real VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0), answers at the
// effective step its warning names.
//
// Every run must hold: status 200 with one warning naming the requested step and a whole multiple of it; every
// returned time is one of Loki's; every returned value is Loki's at that time; every point of Loki's answer at a time
// on the effective grid is in the proxy's answer (the answer is Loki's restricted to the effective grid). Both grid
// modes run: the bounds moved onto the step grid (-align-queries-with-step, the effective times are multiples of the
// effective step) and the bounds as sent (start + j * effective step). Each shape also runs with the mode off, where
// it keeps the limit error main answers.
//
// The answers are recorded (rangeStepAdaptAnswersPath; LVP_RUN_ORACLES=1 records them again).

const rangeStepAdaptAnswersPath = "testdata/range-step-adapt-loki-diff.jsonl.gz"

// rangeStepAdaptBuf is the buffered bound of the bounded runs: below the answers of the hour of raw rows.
const rangeStepAdaptBuf = 1 << 20

var rangeStepAdaptQueries = []string{
	`count_over_time({app="edge-gateway"}[1h])`,
	`sum by (peer_ip) (sum_over_time({app="edge-json"} | json | unwrap bytes [1h]))`,
	`avg_over_time({app="edge-json"} | json | unwrap bytes [1h]) by (peer_ip)`,
	`max_over_time({app="edge-json"} | json | unwrap bytes [1h]) by (peer_ip)`,
	`quantile_over_time(0.9, {app="edge-json"} | json | unwrap bytes [1h]) by (peer_ip)`,
	`topk(5, sum by (client_ip) (count_over_time({app="edge-json"} | json [1h])))`,
	`sum by (peer_ip) (count_over_time({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` [1h])) / sum by (peer_ip) (count_over_time({app="edge-json"} | json [1h]))`,
	`sum by (peer_ip) (count_over_time({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` | pattern ` + rankedParserPattern + ` [1h]))`,
}

// rangeStepAdaptRequests are the requests of a query: 80 s at 53 ms (no multiple of it divides the hour below 19 s),
// 50 ms and 200 ms; each bounded by rows and by bytes, on and off the grid, and once with the mode off.
func rangeStepAdaptRequests(q string) []metricRequest {
	var out []metricRequest
	for _, step := range []time.Duration{53 * time.Millisecond, 50 * time.Millisecond, 200 * time.Millisecond} {
		end := alignDown(metricEnd, step)
		start := alignDown(metricEnd.Add(-rankedParserSpan), step)
		form := url.Values{"query": {q}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)},
			"step": {strconv.FormatInt(step.Milliseconds(), 10) + "ms"}}
		route := lower.RouteRequest{Step: step, Span: end.Sub(start), End: end}
		name := "80s step=" + step.String()
		out = append(out,
			metricRequest{kind: name + " rows", form: form, route: route, rowLimit: rankedParserRowLimit},
			metricRequest{kind: name + " rows aligned", form: form, route: route, rowLimit: rankedParserRowLimit, align: true},
			metricRequest{kind: name + " bytes", form: form, route: route, bufLimit: rangeStepAdaptBuf},
			metricRequest{kind: name + " bytes aligned", form: form, route: route, bufLimit: rangeStepAdaptBuf, align: true},
		)
	}
	return out
}

func rangeStepAdaptMetricQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := rankedParserFixture()
	var out []metricQuery
	for _, q := range rangeStepAdaptQueries {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: rangeStepAdaptRequests(q)})
	}
	return out
}

// stepMatrix is a matrix answer: series (labels as JSON) -> time -> value.
type stepMatrix map[string]map[string]string

func decodeStepMatrix(t *testing.T, body []byte) (stepMatrix, []string) {
	t.Helper()
	var resp struct {
		Warnings []string `json:"warnings"`
		Data     struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%v: %.300s", err, body)
	}
	m := stepMatrix{}
	for _, s := range resp.Data.Result {
		key, _ := json.Marshal(s.Metric)
		points := map[string]string{}
		for _, v := range s.Values {
			ts, _ := json.Marshal(v[0])
			points[string(ts)] = v[1].(string)
		}
		m[string(key)] = points
	}
	return m, resp.Warnings
}

var stepWarningRE = regexp.MustCompile(`^step raised from (\S+) to (\S+) to fit the query limits$`)

// equalSample compares two values to the last bits (avg and quantile are floating point on both sides).
func equalSample(a, b string) bool {
	if a == b {
		return true
	}
	x, err1 := strconv.ParseFloat(a, 64)
	y, err2 := strconv.ParseFloat(b, 64)
	return err1 == nil && err2 == nil && math.Abs(x-y) <= 1e-9*math.Max(1, math.Max(math.Abs(x), math.Abs(y)))
}

// TestRangeStepAdaptMatchesLoki replays the recorded answers through the handler: every adjusted run is Loki's
// answer restricted to the effective grid; with the mode off, every run keeps its limit error.
//
// conformance: limits/range-metric-step-adapt
func TestRangeStepAdaptMatchesLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := rangeStepAdaptMetricQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, rangeStepAdaptAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, rangeStepAdaptAnswersPath)
	var b strings.Builder
	fitting, adjusted := 0, 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok || loki.Status != 200 {
					t.Fatalf("no Loki answer for %s %s (%d %s): record them again", mr.kind, mq.q, loki.Status, loki.Error)
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				offReq := mr
				offReq.stepMode = "off"
				off := runMetricHandler(t, prof, offReq, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if off.status == 200 {
					if run.status != 200 || run.body != off.body || strings.Join(run.sent, "\n") != strings.Join(off.sent, "\n") {
						t.Errorf("%s %s %s: fits with the mode off, but the adaptive run differs: %d, %d vs %d requests", prof.name, mr.kind, mq.q, run.status, len(run.sent), len(off.sent))
					}
					fitting++
					continue
				}
				if off.status != 400 && off.status != 500 {
					t.Errorf("%s %s %s: %d with the mode off: %.200s", prof.name, mr.kind, mq.q, off.status, off.body)
				}
				adjusted++
				if run.status != 200 {
					t.Errorf("%s %s %s: %d %.200s", prof.name, mr.kind, mq.q, run.status, run.body)
					continue
				}
				got, warnings := decodeStepMatrix(t, []byte(run.body))
				want, _ := decodeStepMatrix(t, loki.Body)
				if len(warnings) != 1 || !stepWarningRE.MatchString(warnings[0]) {
					t.Errorf("%s %s %s: warnings %q", prof.name, mr.kind, mq.q, warnings)
					continue
				}
				parts := stepWarningRE.FindStringSubmatch(warnings[0])
				requested, err1 := time.ParseDuration(parts[1])
				effective, err2 := time.ParseDuration(parts[2])
				if err1 != nil || err2 != nil || requested != mr.route.Step || effective <= requested || effective%requested != 0 {
					t.Errorf("%s %s %s: steps %s -> %s, requested %s", prof.name, mr.kind, mq.q, parts[1], parts[2], mr.route.Step)
					continue
				}
				start, _ := time.Parse(time.RFC3339Nano, mr.form.Get("start"))
				end, _ := time.Parse(time.RFC3339Nano, mr.form.Get("end"))
				onEffective := func(ts string) bool {
					sec, err := strconv.ParseFloat(ts, 64)
					if err != nil {
						return false
					}
					at := time.Unix(0, int64(math.Round(sec*1e3))*int64(time.Millisecond))
					if at.Before(start) || at.After(end) {
						return false
					}
					if mr.align {
						return at.UnixNano()%int64(effective) == 0
					}
					return at.Sub(start)%effective == 0
				}
				points := 0
				for series, values := range got {
					for ts, v := range values {
						points++
						lv, ok := want[series][ts]
						switch {
						case !onEffective(ts):
							t.Errorf("%s %s %s: %s at %s: not a time of the effective grid of %s", prof.name, mr.kind, mq.q, series, ts, effective)
						case !ok:
							t.Errorf("%s %s %s: %s at %s: %s, Loki has no point", prof.name, mr.kind, mq.q, series, ts, v)
						case !equalSample(v, lv):
							t.Errorf("%s %s %s: %s at %s: %s, Loki %s", prof.name, mr.kind, mq.q, series, ts, v, lv)
						}
					}
				}
				missing := 0
				for series, values := range want {
					for ts := range values {
						if _, ok := got[series][ts]; onEffective(ts) && !ok {
							missing++
						}
					}
				}
				if missing > 0 || points == 0 {
					t.Errorf("%s %s %s: %d points, %d of Loki's on the effective grid missing", prof.name, mr.kind, mq.q, points, missing)
				}
				if prof.name == diffProfiles()[0].name {
					b.WriteString("\n  " + mr.kind + ": " + parts[1] + " -> " + parts[2] + ", " + strconv.Itoa(points) + " points, " +
						strconv.Itoa(len(run.sent)) + " requests: " + mq.q)
				}
			}
		}
	}
	keys := strings.Split(b.String(), "\n")
	sort.Strings(keys)
	t.Logf("%d runs fit and are main's answer, %d adjusted; adjusted answers against Loki:%s", fitting, adjusted, strings.Join(keys, "\n"))
	if adjusted == 0 {
		t.Fatal("no run needed an adjusted step: the shapes no longer pass a read bound")
	}
}
