package proxy

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// zeroRangeQueries are metric queries over a zero range, with the neighbours whose answer has series.
var zeroRangeQueries = []string{
	`rate({app="a"}[0s])`, `count_over_time({app="a"}[0s])`, `bytes_rate({app="a"}[0s])`, `rate({app="a"}[0m0s])`,
	`rate({app="a"}[0s] offset 1m)`, `sum(rate({app="a"}[0s]))`, `sum by (pod) (count_over_time({app="a"}[0s]))`,
	`topk(1, rate({app="a"}[0s]))`, `label_replace(rate({app="a"}[0s]), "x", "$1", "app", "(.*)")`,
	`sum_over_time({app="a"} | logfmt | unwrap c [0s])`, `quantile_over_time(0.5, {app="a"} | logfmt | unwrap c [0s])`,
	`rate({app="a"}[0s]) + 1`, `count_over_time({app="a"}[0s]) > 0`, `vector(1) and rate({app="a"}[0s])`,
	`rate({app="a"}[0s]) / rate({app="a"}[1m])`, `rate({app="a"}[0s]) or rate({app="a"}[0s])`,
	// Answers with series.
	`absent_over_time({app="a"}[0s])`, `absent_over_time({app="nope"}[0s])`, `absent_over_time({app="a"}[0s]) * 2`,
	`rate({app="a"}[0s]) or vector(1)`, `rate({app="a"}[0s]) or rate({app="a"}[1m])`,
	`rate({app="a"}[1m]) unless rate({app="a"}[0s])`, `rate({app="a"}[1m])`, `vector(1)`, `1 + 1`,
}

// zeroRangeLokiAnswers says, for each query, whether Loki's engine answers it with no series over a data set with
// lines in every window (test/oracle/loki engine, query_range and query); LVP_RUN_ORACLES=1 records it again.
var zeroRangeLokiAnswers = filepath.Join("testdata", "zero_range_loki_answers.jsonl")

type zeroRangeAnswer struct {
	Query        string `json:"q"`
	RangeEmpty   bool   `json:"range_empty"`
	InstantEmpty bool   `json:"instant_empty"`
}

func recordZeroRangeAnswers(t *testing.T) {
	t.Helper()
	const base = int64(1700000000) * 1e9
	var entries []oraclerun.EngineEntry
	for i := int64(0); i < 600; i++ {
		entries = append(entries, oraclerun.EngineEntry{TS: base + i*7e9, Line: `level=info c=` + string(rune('1'+i%9)) + ` msg="x"`})
	}
	set := oraclerun.EngineDataSet{Streams: []oraclerun.EngineStream{{Labels: map[string]string{"app": "a", "pod": "p1"}, Entries: entries}}}
	for i, q := range zeroRangeQueries {
		id := string(rune('A' + i))
		set.Requests = append(set.Requests,
			oraclerun.EngineRequest{ID: "r" + id, Form: url.Values{"query": {q}, "start": {"1700000600"}, "end": {"1700003600"}, "step": {"600"}}.Encode()},
			oraclerun.EngineRequest{ID: "i" + id, Instant: true, Form: url.Values{"query": {q}, "time": {"1700003000"}}.Encode()})
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set})
	if err != nil {
		t.Fatal(err)
	}
	empty := func(a oraclerun.EngineAnswer) bool {
		var body struct {
			Data struct {
				Result []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if a.Status != http.StatusOK || json.Unmarshal(a.Body, &body) != nil {
			t.Fatalf("Loki's answer %d %s %s", a.Status, a.Body, a.Error)
		}
		return len(body.Data.Result) == 0
	}
	var b strings.Builder
	for i, q := range zeroRangeQueries {
		id := string(rune('A' + i))
		line, _ := json.Marshal(zeroRangeAnswer{Query: q, RangeEmpty: empty(answers["r"+id]), InstantEmpty: empty(answers["i"+id])})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(zeroRangeLokiAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestZeroRangeAnswersLikeLoki: a metric query over a zero range Loki answers with no series is answered so at
// once, with Loki's empty matrix (query_range) or vector (query) and no VictoriaLogs call; the others (an
// absent_over_time, an `or` with an operand that has series) keep their route. Loki's answers are recorded with its
// engine.
//
// conformance: semantics/zero-range-metric
func TestZeroRangeAnswersLikeLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordZeroRangeAnswers(t)
	}
	f, err := os.Open(zeroRangeLokiAnswers)
	if err != nil {
		t.Fatalf("%v: record the answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	loki := map[string]zeroRangeAnswer{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a zeroRangeAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		loki[a.Query] = a
	}

	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, q := range zeroRangeQueries {
		want, ok := loki[q]
		if !ok {
			t.Fatalf("%s: no recorded Loki answer: record them with LVP_RUN_ORACLES=1", q)
		}
		if want.RangeEmpty != want.InstantEmpty {
			t.Fatalf("%s: Loki answers query_range empty=%v, query empty=%v", q, want.RangeEmpty, want.InstantEmpty)
		}
		expr, err := logqlpkg.Parse(q)
		if err != nil {
			t.Fatal(err)
		}
		if got := emptyWhateverTheData(expr); got != want.RangeEmpty {
			t.Errorf("%s: empty whatever the data %v, Loki's answer empty %v", q, got, want.RangeEmpty)
			continue
		}
		if !want.RangeEmpty {
			continue
		}
		before := calls.Load()
		for path, resultType := range map[string]string{
			"/loki/api/v1/query_range?" + url.Values{"query": {q}, "start": {"1700000600"}, "end": {"1700003600"}, "step": {"600"}}.Encode(): "matrix",
			"/loki/api/v1/query?" + url.Values{"query": {q}, "time": {"1700003000"}}.Encode():                                                "vector",
		} {
			w := doCompatProxyRequest(p, path, map[string]string{"X-Scope-OrgID": "0"})
			var body struct {
				Status string `json:"status"`
				Data   struct {
					ResultType string            `json:"resultType"`
					Result     []json.RawMessage `json:"result"`
				} `json:"data"`
			}
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Status != "success" || body.Data.ResultType != resultType || body.Data.Result == nil || len(body.Data.Result) != 0 {
				t.Errorf("%s: %d %s, want Loki's empty %s", path, w.Code, w.Body, resultType)
			}
		}
		if calls.Load() != before {
			t.Errorf("%s: %d VictoriaLogs calls for an answer that holds no series", q, calls.Load()-before)
		}
	}
}
