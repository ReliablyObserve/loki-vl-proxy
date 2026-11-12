package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// statsStub is a VictoriaLogs that answers every stats request with a fixed body and records what it was sent.
type statsStub struct {
	mu       sync.Mutex
	requests []url.Values
	paths    []string
	rangeAns string
	instAns  string
}

func (s *statsStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	s.mu.Lock()
	s.requests = append(s.requests, r.Form)
	s.paths = append(s.paths, r.URL.Path)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/select/logsql/stats_query_range":
		_, _ = w.Write([]byte(s.rangeAns))
	case "/select/logsql/stats_query":
		_, _ = w.Write([]byte(s.instAns))
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

// statsRequests are the requests the stub got on the stats endpoints.
func (s *statsStub) statsRequests() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []url.Values
	for i, path := range s.paths {
		if strings.HasPrefix(path, "/select/logsql/stats_query") {
			out = append(out, s.requests[i])
		}
	}
	return out
}

func newStatsStubProxy(t *testing.T, stub *statsStub) *Proxy {
	t.Helper()
	backend := httptest.NewServer(stub)
	t.Cleanup(backend.Close)
	c := cache.New(60*time.Second, 1000)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

type stubSample struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
	Value  []any             `json:"value"`
}

func decodeStubSamples(t *testing.T, body []byte) []stubSample {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []stubSample `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("answer is not JSON: %v\n%s", err, body)
	}
	if resp.Status != "success" {
		t.Fatalf("status %q: %s", resp.Status, body)
	}
	return resp.Data.Result
}

// stdvarStubAnswers are the stats answers a stub sends back: an app with a standard deviation of 3 then 4, and
// one of 0.5.
const (
	stdvarRangeAnswer = `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"api","__lvp_bad":"0"},"values":[[1799999700,"3"],[1800000000,"4"],[1800000300,"4"]]},` +
		`{"metric":{"app":"web","__lvp_bad":"0"},"values":[[1800000000,"0.5"]]}]}}`
	stdvarInstantAnswer = `{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"app":"api","__lvp_bad":"0"},"value":[1800000600,"3"]},` +
		`{"metric":{"app":"web","__lvp_bad":"0"},"value":[1800000600,"0.5"]}]}}`
)

// A stdvar_over_time with a grouping, or without a parser, is translated to the standard deviation raised to the
// power 2. Its client query is no binary expression, so the handlers used to send the binary marker to VictoriaLogs
// as a query (an error status, 400 on the stack measured; Loki answers 200). The stats query is now answered
// as translated and the power applied to its answer.
//
// conformance: semantics/stdvar-grouped-binary-marker
func TestStdvarOverTimeGroupedAnswersAsScalarBinary(t *testing.T) {
	for _, query := range []string{
		`sum by (app) (stdvar_over_time({app="x"} | json | unwrap n [5m]))`,
		`stdvar_over_time({app="x"} | json | unwrap n [5m]) by (app)`,
		`sum by (app) (stdvar_over_time({app="x"} | unwrap n [5m]))`,
		`stdvar_over_time({app="x"} | unwrap n [5m]) by (app)`,
		`stdvar_over_time({app="x"} | unwrap n [5m])`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(endpoint+"/"+query, func(t *testing.T) {
				stub := &statsStub{rangeAns: stdvarRangeAnswer, instAns: stdvarInstantAnswer}
				p := newStatsStubProxy(t, stub)
				params := url.Values{"query": {query}, "start": {"1800000000"}, "end": {"1800000600"}, "step": {"300"}, "time": {"1800000600"}}
				res := doCompatProxyRequest(p, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				if res.Code != http.StatusOK {
					t.Fatalf("status %d, Loki answers 200: %s", res.Code, res.Body)
				}
				// No request holds the binary marker, wherever it went.
				if len(stub.statsRequests()) == 0 {
					t.Fatal("no stats request reached the backend")
				}
				stub.mu.Lock()
				for i, form := range stub.requests {
					if q := form.Get("query"); strings.Contains(q, translator.BinaryMetricPrefix) || strings.Contains(q, "|||") {
						t.Fatalf("%s was sent the binary marker as a query: %.200s", stub.paths[i], q)
					}
				}
				stub.mu.Unlock()
				// The standard deviation squared, per series.
				want := map[string]float64{"api": 9, "web": 0.25}
				seen := 0
				for _, sample := range decodeStubSamples(t, res.Body.Bytes()) {
					app := sample.Metric["app"]
					if _, leaked := sample.Metric["__lvp_bad"]; leaked {
						t.Fatalf("the scratch label __lvp_bad is on the answer: %v", sample.Metric)
					}
					if app == "" {
						continue // an ungrouped form keeps its own labels
					}
					seen++
					points := sample.Values
					if endpoint == "query" {
						points = [][]any{sample.Value}
					}
					for _, point := range points {
						got, _ := strconv.ParseFloat(point[1].(string), 64)
						if app == "web" && got != want["web"] || app == "api" && got != 9 && got != 16 {
							t.Fatalf("app %s: %v, want the standard deviation squared (%v)", app, got, want)
						}
					}
				}
				if seen == 0 && strings.Contains(query, "(app)") {
					t.Fatalf("no series by app in %s", res.Body)
				}
			})
		}
	}
}

// No text that starts with the binary marker reaches a stats handler, and one that gets there is answered, not sent
// to VictoriaLogs.
//
// conformance: semantics/stdvar-grouped-binary-marker
func TestStatsHandlersNeverSendTheBinaryMarker(t *testing.T) {
	stub := &statsStub{rangeAns: stdvarRangeAnswer, instAns: stdvarInstantAnswer}
	p := newStatsStubProxy(t, stub)
	marker := translator.BinaryMetricPrefix + `^:app:="x" | stats by (app) stddev(n)|||2`
	stats, isStats := translator.DecodeStats(marker)
	if !isStats {
		t.Fatal("the marker text is read as a stats query: that is what the guard exists for")
	}
	for name, run := range map[string]func(w http.ResponseWriter, r *http.Request){
		"range":   func(w http.ResponseWriter, r *http.Request) { p.proxyStatsQueryRange(w, r, stats) },
		"instant": func(w http.ResponseWriter, r *http.Request) { p.proxyStatsQuery(w, r, stats) },
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {`stdvar_over_time({app="x"} | unwrap n [5m]) by (app)`}, "start": {"1800000000"}, "end": {"1800000600"}, "step": {"300"}, "time": {"1800000600"}}.Encode(), nil)
			rec := httptest.NewRecorder()
			run(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want the handler to refuse the marker: %s", rec.Code, rec.Body)
			}
			if n := len(stub.statsRequests()); n != 0 {
				t.Fatalf("%d stats requests reached the backend", n)
			}
		})
	}
}

// scalarStatsBinaryOf takes only a stats query and a number.
//
// conformance: semantics/stdvar-grouped-binary-marker
func TestScalarStatsBinaryOf(t *testing.T) {
	const stats = `app:="x" | stats by (app) stddev(n)`
	for _, tc := range []struct {
		name, text string
		ok         bool
		left       bool
	}{
		{"stdvar", translator.BinaryMetricPrefix + "^:" + stats + "|||2", true, false},
		{"number first", translator.BinaryMetricPrefix + "*:2|||" + stats, true, true},
		{"two queries", translator.BinaryMetricPrefix + "/:" + stats + "|||" + stats, false, false},
		{"two numbers", translator.BinaryMetricPrefix + "+:1|||2", false, false},
		{"with a modifier", translator.BinaryMetricPrefix + "^:" + stats + "|||2@@@on:app", false, false},
		{"a log query", translator.BinaryMetricPrefix + "^:app:=\"x\"|||2", false, false},
		{"no marker", stats, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, ok := scalarStatsBinaryOfText(tc.text)
			if ok != tc.ok {
				t.Fatalf("ok %v, want %v", ok, tc.ok)
			}
			if ok && (b.scalarLeft != tc.left || b.op == "") {
				t.Fatalf("got %+v", b)
			}
		})
	}
}

// statsRangeRun runs a query_range through the real handler against a stub that answers every stats range request
// with body, and returns the answer and the stats range requests.
func statsRangeRun(t *testing.T, query, body string, startSec, endSec, stepSec int64) (*httptest.ResponseRecorder, []url.Values) {
	t.Helper()
	stub := &statsStub{rangeAns: body, instAns: stdvarInstantAnswer}
	p := newStatsStubProxy(t, stub)
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(startSec, 10)}, "end": {strconv.FormatInt(endSec, 10)}, "step": {strconv.FormatInt(stepSec, 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", query, res.Code, res.Body)
	}
	return res, stub.statsRequests()
}

func matrixTimes(t *testing.T, body []byte) map[string][]float64 {
	t.Helper()
	out := map[string][]float64{}
	for _, sample := range decodeStubSamples(t, body) {
		for _, point := range sample.Values {
			out[sample.Metric["app"]] = append(out[sample.Metric["app"]], point[0].(float64))
		}
	}
	return out
}

// With a range equal to the step, VictoriaLogs' bucket labelled T holds [T, T+step) and Loki's sample at T is the
// window (T-range, T]. stddev and stdvar over time were answered from the unshifted bucket, one step late; they are
// fetched from start-step and relabelled to the evaluation time each bucket closes, as sum, avg, min and max over
// time are. A range that is not the step stays as it was: the bucket is not their window.
//
// conformance: semantics/stddev-stdvar-over-time-step-bucket, window-bounds-and-step-alignment
func TestStddevStdvarOverTimeTumblingReadsTheWindowEndingAtT(t *testing.T) {
	const start, end = int64(1800000000), int64(1800000600)
	const buckets = `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"api"},"values":[[1799999700,"3"],[1800000000,"4"],[1800000300,"5"],[1800000600,"6"]]}]}}`
	query := func(fn, window string) string {
		if fn == "sum_over_time" { // a grouping is not allowed on it
			return `sum by (app) (` + fn + `({app="x"} | json | unwrap n [` + window + `]))`
		}
		return fn + `({app="x"} | json | unwrap n [` + window + `]) by (app)`
	}

	// The reference: sum_over_time over a range equal to the step.
	refRes, refReqs := statsRangeRun(t, query("sum_over_time", "5m"), buckets, start, end, 300)
	if len(refReqs) == 0 {
		t.Fatal("no stats request for the reference")
	}
	wantStart := refReqs[0].Get("start")
	wantTimes := matrixTimes(t, refRes.Body.Bytes())["api"]
	if len(wantTimes) == 0 || wantTimes[0] != float64(start) || wantTimes[len(wantTimes)-1] != float64(end) {
		t.Fatalf("the reference does not run start..end: %v", wantTimes)
	}

	for _, fn := range []string{"stddev_over_time", "stdvar_over_time"} {
		t.Run(fn+"/range equals step", func(t *testing.T) {
			res, reqs := statsRangeRun(t, query(fn, "5m"), buckets, start, end, 300)
			if len(reqs) != 1 {
				t.Fatalf("%d stats requests, want 1", len(reqs))
			}
			if got := reqs[0].Get("start"); got != wantStart {
				t.Fatalf("fetched from %q, want %q (start-step, as sum_over_time)", got, wantStart)
			}
			got := matrixTimes(t, res.Body.Bytes())["api"]
			if len(got) != len(wantTimes) {
				t.Fatalf("times %v, want %v", got, wantTimes)
			}
			for i := range got {
				if got[i] != wantTimes[i] {
					t.Fatalf("times %v, want %v", got, wantTimes)
				}
			}
		})
		t.Run(fn+"/range is not the step", func(t *testing.T) {
			_, reqs := statsRangeRun(t, query(fn, "10m"), buckets, start, end, 300)
			if len(reqs) != 1 {
				t.Fatalf("%d stats requests, want 1", len(reqs))
			}
			// Not shifted: the request's own start, as before.
			if got := reqs[0].Get("start"); got != strconv.FormatInt(start, 10) {
				t.Fatalf("fetched from %q, want the request's start %d", got, start)
			}
		})
	}

	// sum_over_time keeps its shift for any range: the contrast the range != step case above is read against.
	_, reqs := statsRangeRun(t, query("sum_over_time", "10m"), buckets, start, end, 300)
	if len(reqs) != 1 || reqs[0].Get("start") == strconv.FormatInt(start, 10) {
		t.Fatalf("sum_over_time over a longer range is no longer shifted: %v", reqs)
	}
}

// logRangeFunctions classifies stddev and stdvar over time with unwrap as native step buckets only for the shift
// that needs range == step.
//
// conformance: semantics/stddev-stdvar-over-time-step-bucket
func TestLogRangeFunctionsStddevStdvar(t *testing.T) {
	for _, tc := range []struct {
		query                         string
		found, logRangeOnly, unwrapOn bool
	}{
		{`stddev_over_time({a="b"} | json | unwrap n [5m])`, true, true, false},
		{`stdvar_over_time({a="b"} | json | unwrap n [5m])`, true, true, false},
		{`sum by (a) (stddev_over_time({a="b"} | json | unwrap n [5m]))`, true, true, false},
		{`stddev_over_time({a="b"} | json | unwrap n [5m]) / sum_over_time({a="b"} | json | unwrap n [5m])`, true, true, false},
		{`sum_over_time({a="b"} | json | unwrap n [5m])`, true, true, true},
		{`quantile_over_time(0.5, {a="b"} | json | unwrap n [5m])`, true, false, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			expr, err := parseLogQLExpr(context.Background(), tc.query)
			if err != nil {
				t.Fatal(err)
			}
			found, _, logRangeOnly, unwrapOnly := logRangeFunctions(expr)
			if found != tc.found || logRangeOnly != tc.logRangeOnly || unwrapOnly != tc.unwrapOn {
				t.Fatalf("found %v logRangeOnly %v unwrapOnly %v, want %v %v %v", found, logRangeOnly, unwrapOnly, tc.found, tc.logRangeOnly, tc.unwrapOn)
			}
		})
	}
}

// FuzzScalarStatsBinaryOf: whatever text it is given, scalarStatsBinaryOf does not panic, and a text it takes is
// a stats query joined to a number by an operator: the operator is set, the stats query carries no marker, and the
// text it was read from is the operand and the number written back (the translator's text round-trips).
//
// conformance: semantics/stdvar-grouped-binary-marker
func FuzzScalarStatsBinaryOf(f *testing.F) {
	const stats = `app:="x" | stats by (app) stddev(n)`
	for _, seed := range []string{
		translator.BinaryMetricPrefix + "^:" + stats + "|||2",
		translator.BinaryMetricPrefix + "*:2|||" + stats,
		translator.BinaryMetricPrefix + "/:" + stats + "|||" + stats,
		translator.BinaryMetricPrefix + "^:" + translator.BinaryMetricPrefix + "+:" + stats + "|||1|||2",
		translator.BinaryMetricPrefix + "^:" + stats + "|||2@@@on:app",
		translator.BinaryMetricPrefix + "+:1|||2",
		translator.BinaryMetricPrefix + "^:|||",
		stats,
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		b, ok := scalarStatsBinaryOfText(text)
		if !ok {
			return
		}
		if inner, _ := translator.DecodePhysical(b.stats.Text); b.op == "" {
			t.Fatalf("taken, but op %q and stats %q", b.op, b.stats.Text)
		} else if _, nested := inner.(physical.Binary); nested {
			t.Fatalf("taken with a binary expression as the stats query: %q", b.stats.Text)
		}
		var written string
		if b.scalarLeft {
			written = translator.BinaryMetricPrefix + b.op + ":" + strconv.FormatFloat(b.scalar, 'g', -1, 64) + "|||" + b.stats.Text
		} else {
			written = translator.BinaryMetricPrefix + b.op + ":" + b.stats.Text + "|||" + strconv.FormatFloat(b.scalar, 'g', -1, 64)
		}
		again, ok := scalarStatsBinaryOfText(written)
		if !ok || again.op != b.op || again.scalarLeft != b.scalarLeft || again.stats.Text != b.stats.Text || again.scalar != b.scalar {
			t.Fatalf("%q read as %+v, written back as %q read as %+v (%v)", text, b, written, again, ok)
		}
	})
}

// scalarStatsBinaryOfText is scalarStatsBinaryOf of a translated text.
func scalarStatsBinaryOfText(text string) (scalarStatsBinary, bool) {
	decoded, _ := translator.DecodePhysical(text)
	bin, ok := decoded.(physical.Binary)
	if !ok {
		return scalarStatsBinary{}, false
	}
	return scalarStatsBinaryOf(bin)
}
