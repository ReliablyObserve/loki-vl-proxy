package proxy

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// absentZeroRangeQueries are absent_over_time over a zero range, alone and under the wrappers Loki keeps its one
// series through, with neighbours the early answer must leave to their routes.
var absentZeroRangeQueries = []string{
	`absent_over_time({app="a"}[0s])`, `absent_over_time({app="a"}[0])`, `absent_over_time({app="nope"}[0s])`,
	`absent_over_time({app="a"}[0s] offset 1m)`, `absent_over_time({app="a"}[0m0s])`,
	`absent_over_time({app="a", pod=~"p.*"}[0s])`, `absent_over_time({app="a", pod="p1"}[0s])`,
	`absent_over_time({app="a", app!="b"}[0s])`, `absent_over_time({app="a", env=""}[0s])`,
	`absent_over_time({app="a"} |= "x" [0s])`, `absent_over_time({app="a"} | logfmt | c > 1 [0s])`,
	`absent_over_time({app="a"} | logfmt | unwrap c [0s])`,
	`sum(absent_over_time({app="a"}[0s]))`, `sum by (app) (absent_over_time({app="a", pod="p1"}[0s]))`,
	`max without (pod) (absent_over_time({app="a", pod="p1"}[0s]))`, `count(absent_over_time({app="a"}[0s]))`,
	`stddev(absent_over_time({app="a"}[0s]))`, `topk(1, absent_over_time({app="a"}[0s]))`,
	`bottomk(2, absent_over_time({app="a"}[0]))`, `sort_desc(absent_over_time({app="a"}[0s]))`,
	`label_replace(absent_over_time({app="a"}[0s]), "x", "$1", "app", "(.*)")`,
	`sum by (x) (label_replace(absent_over_time({app="a"}[0s]), "x", "v-$1", "app", "(.*)"))`,
	`label_replace(sum by (app) (absent_over_time({app="a"}[0])), "app", "", "app", ".*")`,
	// Left to their routes: a positive range, and a binary operation (whose operand gets the early answer).
	`absent_over_time({app="a"}[1m])`, `absent_over_time({app="nope"}[1m])`,
	`absent_over_time({app="a"}[0s]) * 2`,
}

// absentZeroRangeLokiAnswers holds Loki's result (data.result) for each query on query_range and query, over a data
// set with lines in every window (test/oracle/loki engine); LVP_RUN_ORACLES=1 records it again.
var absentZeroRangeLokiAnswers = filepath.Join("testdata", "absent_zero_range_loki_answers.jsonl")

// The windows of the recorded requests, on the step grid so the proxy's step alignment leaves them as they are.
var (
	absentZeroRangeForm   = url.Values{"start": {"1700001000"}, "end": {"1700003400"}, "step": {"600"}}
	absentZeroInstantForm = url.Values{"time": {"1700003000"}}
)

type absentZeroRangeAnswer struct {
	Query   string          `json:"q"`
	Status  int             `json:"status"`
	Range   json.RawMessage `json:"range"`
	Instant json.RawMessage `json:"instant"`
}

func formWithQuery(form url.Values, q string) url.Values {
	v := url.Values{"query": {q}}
	for k, vs := range form {
		v[k] = vs
	}
	return v
}

func recordAbsentZeroRangeAnswers(t *testing.T) {
	t.Helper()
	const base = int64(1700000000) * 1e9
	var entries []oraclerun.EngineEntry
	for i := int64(0); i < 600; i++ {
		entries = append(entries, oraclerun.EngineEntry{TS: base + i*7e9, Line: `level=info c=` + string(rune('1'+i%9)) + ` msg="x"`})
	}
	set := oraclerun.EngineDataSet{Streams: []oraclerun.EngineStream{{Labels: map[string]string{"app": "a", "pod": "p1"}, Entries: entries}}}
	for i, q := range absentZeroRangeQueries {
		id := string(rune('A' + i))
		set.Requests = append(set.Requests,
			oraclerun.EngineRequest{ID: "r" + id, Form: formWithQuery(absentZeroRangeForm, q).Encode()},
			oraclerun.EngineRequest{ID: "i" + id, Instant: true, Form: formWithQuery(absentZeroInstantForm, q).Encode()})
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set})
	if err != nil {
		t.Fatal(err)
	}
	result := func(a oraclerun.EngineAnswer) json.RawMessage {
		var body struct {
			Data struct {
				Result json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if a.Status != http.StatusOK || json.Unmarshal(a.Body, &body) != nil {
			t.Fatalf("Loki's answer %d %s %s", a.Status, a.Body, a.Error)
		}
		return body.Data.Result
	}
	var b strings.Builder
	for i, q := range absentZeroRangeQueries {
		id := string(rune('A' + i))
		line, _ := json.Marshal(absentZeroRangeAnswer{Query: q, Status: answers["r"+id].Status, Range: result(answers["r"+id]), Instant: result(answers["i"+id])})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(absentZeroRangeLokiAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readAbsentZeroRangeAnswers(t *testing.T) map[string]absentZeroRangeAnswer {
	t.Helper()
	f, err := os.Open(absentZeroRangeLokiAnswers)
	if err != nil {
		t.Fatalf("%v: record the answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	loki := map[string]absentZeroRangeAnswer{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a absentZeroRangeAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		loki[a.Query] = a
	}
	return loki
}

// sameResultJSON reports whether two JSON texts hold the same value.
func sameResultJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// TestAbsentZeroRangeAnswersLikeLoki: absent_over_time over a zero range finds no sample in any window, whatever the
// data, so Loki answers its one series valued 1 at every step (query_range) and at the evaluation time (query), with
// the labels of the selector's equality matchers, through vector aggregations, rankings, sorts and label_replace.
// The proxy answers it so from the parsed query, before the cache, the plan and any VictoriaLogs call, and a binary
// operation of it gets Loki's answer through its operand's request. Loki's answers are recorded with its engine
// (test/oracle/loki engine).
//
// conformance: semantics/zero-range-metric
func TestAbsentZeroRangeAnswersLikeLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordAbsentZeroRangeAnswers(t)
	}
	loki := readAbsentZeroRangeAnswers(t)
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, q := range absentZeroRangeQueries {
		want, ok := loki[q]
		if !ok {
			t.Fatalf("%s: no recorded Loki answer: record them with LVP_RUN_ORACLES=1", q)
		}
		expr, err := logqlpkg.Parse(q)
		if err != nil {
			t.Fatal(err)
		}
		answered := absentEveryStep(expr)
		if wantAnswered := !strings.Contains(q, "[1m]") && !strings.Contains(q, " * "); answered != wantAnswered {
			t.Errorf("%s: answered at once %v, want %v", q, answered, wantAnswered)
		}
		// A binary operation keeps its route; each operand is a request of its own, which the early answer serves.
		if strings.Contains(q, "[1m]") {
			continue
		}
		before := calls.Load()
		for _, c := range []struct {
			path, resultType string
			want             json.RawMessage
		}{
			{"/loki/api/v1/query_range?" + formWithQuery(absentZeroRangeForm, q).Encode(), "matrix", want.Range},
			{"/loki/api/v1/query?" + formWithQuery(absentZeroInstantForm, q).Encode(), "vector", want.Instant},
		} {
			w := doCompatProxyRequest(p, c.path, map[string]string{"X-Scope-OrgID": "0"})
			var body struct {
				Status string `json:"status"`
				Data   struct {
					ResultType string          `json:"resultType"`
					Result     json.RawMessage `json:"result"`
				} `json:"data"`
			}
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Status != "success" || body.Data.ResultType != c.resultType {
				t.Errorf("%s: %d %s, want Loki's %s", c.path, w.Code, w.Body, c.resultType)
				continue
			}
			if !sameResultJSON(body.Data.Result, c.want) {
				t.Errorf("%s:\n got  %s\n want %s (Loki)", c.path, body.Data.Result, c.want)
			}
		}
		if calls.Load() != before {
			t.Errorf("%s: %d VictoriaLogs calls for an answer that reads no data", q, calls.Load()-before)
		}
	}
}

// Every Loki answer of an absent_over_time over a zero range holds one series with a point at every step.
func TestAbsentZeroRangeLokiAnswersHoldEveryStep(t *testing.T) {
	for q, a := range readAbsentZeroRangeAnswers(t) {
		if strings.Contains(q, "[1m]") || strings.Contains(q, " * ") || strings.Contains(q, "topk") || strings.Contains(q, "bottomk") {
			continue
		}
		var series []struct {
			Values [][]any `json:"values"`
		}
		if err := json.Unmarshal(a.Range, &series); err != nil || len(series) != 1 || len(series[0].Values) != 5 {
			t.Errorf("%s: Loki's range answer %s, want one series with 5 points", q, a.Range)
		}
	}
}
