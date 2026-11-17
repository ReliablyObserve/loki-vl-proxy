package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The oracle checks of the reads of streams without a level label (detected_level_levelless.go) and of an unwrap
// over an extraction list (`| logfmt amount | unwrap amount`): Loki's answers come from the engine oracle (test/oracle/loki
// engine) and are recorded in testdata; the proxy runs over VictoriaLogs itself (test/oracle/vl serve, v1.49.0 and
// v1.53.0). Both legs run with LVP_RUN_ORACLES=1, which also records Loki's answers again.

// oracleLine is one line of a stream of these checks: its body, its structured metadata and the detected_level
// Loki's ingest gives it (the engine has no ingest, so it reads the value as structured metadata).
type oracleLine struct {
	line  string
	sm    map[string]string
	level string
}

type oracleStream struct {
	labels map[string]string
	lines  func(i int) oracleLine // line i, i in [0, oracleLines)
}

// oracleLines lines per stream, 10 s apart, ending just before levelSweepBase: every window and step of
// levelSweepForm holds some.
const oracleLines = 90

func oracleLineTS(i int) int64 { return oracleBase*1e9 - int64(i)*10e9 - 3e8 }

// oracleBase is the time of the instant requests and the end of the range ones (seconds): a multiple of every step,
// so Loki's step alignment moves no window.
const oracleBase = int64(1_760_000_100)

func oracleForm(q, mode string) url.Values {
	form := url.Values{"query": {q}}
	if mode == "instant" {
		form.Set("time", strconv.FormatInt(oracleBase, 10))
	} else {
		form.Set("start", strconv.FormatInt(oracleBase-900, 10))
		form.Set("end", strconv.FormatInt(oracleBase, 10))
		form.Set("step", mode)
	}
	return form
}

// oracleReducer reduces the answer to query q (its status, body and error text) to the dimension a check holds
// the proxy to.
type oracleReducer func(q string, status int, body []byte, errText string) string

type oracleSeries struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
	Values [][]any           `json:"values"`
}

func oracleSeriesOf(body []byte) []oracleSeries {
	var resp struct {
		Data struct {
			Result []oracleSeries `json:"result"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &resp)
	return resp.Data.Result
}

func oracleError(status int, errText string) string {
	return fmt.Sprintf("%d %s", status, strings.TrimSpace(errText))
}

// reduceByLevel is the answer per (pod, detected_level) and step, the series of one pair combined (summed, or the
// maximum for max_over_time, whose value is a maximum whatever the other labels split), then summed over the
// steps and rounded to six digits. The other labels of a series (the keys a parser extracts, structured metadata)
// are left out: they are not what these checks hold, and the open parser identity cases own them.
func reduceByLevel(q string, status int, body []byte, errText string) string {
	if status != http.StatusOK {
		return oracleError(status, errText)
	}
	maxOf := strings.HasPrefix(q, "max_over_time(")
	type key struct{ group, ts string }
	points := map[key]float64{}
	groups := map[string]bool{}
	for _, s := range oracleSeriesOf(body) {
		lv, ok := s.Metric[detectedLevelLabel]
		if !ok {
			lv = "-"
		}
		g := s.Metric["pod"] + "|" + lv
		groups[g] = true
		pts := s.Values
		if s.Value != nil {
			pts = [][]any{s.Value}
		}
		for _, pt := range pts {
			if len(pt) != 2 {
				continue
			}
			v, _ := strconv.ParseFloat(fmt.Sprint(pt[1]), 64)
			k := key{g, fmt.Sprint(pt[0])}
			if prev, seen := points[k]; seen && maxOf {
				v = math.Max(prev, v)
			} else if seen {
				v += prev
			}
			points[k] = v
		}
	}
	totals := map[string]float64{}
	for k, v := range points {
		totals[k.group] += v
	}
	var out []string
	for g := range groups {
		out = append(out, g+"="+strconv.FormatFloat(math.Round(totals[g]*1e6)/1e6, 'f', -1, 64))
	}
	sort.Strings(out)
	return "200 " + strings.Join(out, " ")
}

// reduceByStreams is which streams (their fmt label) answer series.
func reduceByStreams(_ string, status int, body []byte, errText string) string {
	if status != http.StatusOK {
		return oracleError(status, errText)
	}
	seen := map[string]bool{}
	for _, s := range oracleSeriesOf(body) {
		seen[s.Metric["fmt"]] = true
	}
	var out []string
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return "200 " + strings.Join(out, " ")
}

// oracleProxyAnswer sends one request to a proxy over backend and reduces its answer.
func oracleProxyAnswer(t *testing.T, backend, q, mode string, reduce func(status int, body []byte, errText string) string) string {
	t.Helper()
	p, err := New(Config{BackendURL: backend, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	target := "/loki/api/v1/query_range?"
	if mode == "instant" {
		target = "/loki/api/v1/query?"
	}
	res := doCompatProxyRequest(p, target+oracleForm(q, mode).Encode(), map[string]string{"X-Scope-OrgID": "0"})
	errText := ""
	if res.Code != http.StatusOK {
		var e struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal(res.Body.Bytes(), &e) == nil {
			errText = e.Error + e.Message
		}
	}
	return reduce(res.Code, res.Body.Bytes(), errText)
}

// runLevelOracle records Loki's answers to queries (S replaced with selector) over streams at every mode into
// answersPath, reduced by reduce to the dimension the check holds the proxy to, then runs the proxy over
// VictoriaLogs v1.49.0 and v1.53.0 and reports every request whose reduced answer differs from Loki's.
func runLevelOracle(t *testing.T, streams []oracleStream, selector string, queries []string, answersPath string, reduce oracleReducer) {
	t.Helper()
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs Loki's engine and VictoriaLogs")
	}
	t.Cleanup(querycorpus.PauseRecording())
	set := oraclerun.EngineDataSet{}
	var rows oraclerun.ServeDataSet
	for _, st := range streams {
		es := oraclerun.EngineStream{Labels: map[string]string{"service_name": st.labels["app"]}}
		var stream [][2]string
		for k, v := range st.labels {
			es.Labels[k] = v
			stream = append(stream, [2]string{k, v})
		}
		sort.Slice(stream, func(i, j int) bool { return stream[i][0] < stream[j][0] })
		for i := range oracleLines {
			l := st.lines(i)
			md := map[string]string{"detected_level": l.level}
			var fields [][2]string
			for k, v := range l.sm {
				md[k] = v
				fields = append(fields, [2]string{k, v})
			}
			sort.Slice(fields, func(i, j int) bool { return fields[i][0] < fields[j][0] })
			es.Entries = append(es.Entries, oraclerun.EngineEntry{TS: oracleLineTS(i), Line: l.line, Metadata: md})
			rows.Rows = append(rows.Rows, oraclerun.ServeRow{TS: oracleLineTS(i), Stream: stream, Fields: append(fields, [2]string{"_msg", l.line})})
		}
		set.Streams = append(set.Streams, es)
	}
	id := func(qi int, mode string) string { return strconv.Itoa(qi) + "|" + mode }
	for qi, q := range queries {
		for _, mode := range levelSweepModes {
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: id(qi, mode), Instant: mode == "instant", Form: oracleForm(strings.ReplaceAll(q, "S", selector), mode).Encode()})
		}
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(answersPath)
	if err != nil {
		t.Fatal(err)
	}
	loki := map[string]string{}
	w := bufio.NewWriter(f)
	for _, r := range set.Requests {
		a := answers[r.ID]
		qi, _ := strconv.Atoi(strings.Split(r.ID, "|")[0])
		loki[r.ID] = reduce(queries[qi], a.Status, a.Body, a.Error)
		line, _ := json.Marshal(map[string]string{"id": r.ID, "answer": loki[r.ID]})
		_, _ = w.Write(append(line, '\n'))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{rows})
		if err != nil {
			t.Fatal(err)
		}
		front := httptest.NewServer(levelSweepVLFront(v.version, srv.URL))
		same := 0
		for qi, q := range queries {
			for _, mode := range levelSweepModes {
				got := oracleProxyAnswer(t, front.URL, strings.ReplaceAll(q, "S", selector), mode, func(status int, body []byte, errText string) string {
					return reduce(q, status, body, errText)
				})
				if want := loki[id(qi, mode)]; got != want {
					t.Errorf("%s %s %s:\n got %s\nLoki %s", v.version, mode, q, got, want)
					continue
				}
				same++
			}
		}
		t.Logf("VictoriaLogs %s: %d of %d answers equal to Loki's", v.version, same, len(queries)*len(levelSweepModes))
		front.Close()
		srv.Stop()
	}
}

// TestLevelLessStreamSplitsByLineLevel: a stream without a level label whose lines carry several levels (a logfmt
// level, a plain line's level word, structured metadata, a JSON level, nothing) is one Loki series per level on
// every per-stream read, with the values of that level's lines; a stream labelled level="info" beside it stays one
// info series whatever its lines say.
//
// conformance: semantics/detected-level-stream-stats-without-level, severity-detected-level-derivation
func TestLevelLessStreamSplitsByLineLevel(t *testing.T) {
	mixed := func(i int) oracleLine {
		amount := i%7 + 1
		switch i % 6 {
		case 0:
			return oracleLine{line: fmt.Sprintf(`level=error msg="r%d" amount=%d`, i, amount), level: "error"}
		case 1:
			return oracleLine{line: fmt.Sprintf(`level=info msg="r%d" amount=%d`, i, amount), level: "info"}
		case 2:
			return oracleLine{line: fmt.Sprintf("ERROR something failed r%d amount=%d", i, amount), level: "error"}
		case 3:
			return oracleLine{line: fmt.Sprintf("plain message r%d amount=%d", i, amount), sm: map[string]string{"level": "debug"}, level: "debug"}
		case 4:
			return oracleLine{line: fmt.Sprintf("plain message r%d amount=%d", i, amount), level: "unknown"}
		}
		return oracleLine{line: fmt.Sprintf(`{"level":"warn","msg":"r%d","amount":%d}`, i, amount), level: "warn"}
	}
	labelled := func(i int) oracleLine {
		return oracleLine{line: fmt.Sprintf(`level=error msg="r%d" amount=%d`, i, i%7+1), level: "info"}
	}
	runLevelOracle(t, []oracleStream{
		{labels: map[string]string{"app": "mx", "pod": "m1"}, lines: mixed},
		{labels: map[string]string{"app": "mx", "pod": "m2", "level": "info"}, lines: labelled},
	}, `{app="mx"}`, []string{
		`count_over_time(S | logfmt [5m])`,
		`bytes_over_time(S | logfmt [5m])`,
		`count_over_time(S | logfmt [5m]) > 0`,
		`max_over_time(S | logfmt amount | unwrap amount [5m])`,
		`sum without (pod) (max_over_time(S | logfmt amount | unwrap amount [5m]))`,
		`sort(rate(S | logfmt [1m]))`,
	}, "testdata/detected_level_levelless_loki_answers.jsonl", reduceByLevel)
}

// TestLevelLessJSONStreamSplitsByLineLevel: a stream without a level label whose JSON lines carry warn, error or no
// level splits into Loki's series per level on the reads of `| json` metrics, the raw-row evaluator's (rate and
// label_replace at an instant and step 60) and the stats ones, beside a stream labelled level="info".
//
// conformance: semantics/detected-level-stream-stats-without-level, severity-detected-level-derivation
func TestLevelLessJSONStreamSplitsByLineLevel(t *testing.T) {
	mixed := func(i int) oracleLine {
		amount := i%7 + 1
		switch i % 3 {
		case 0:
			return oracleLine{line: fmt.Sprintf(`{"level":"warn","msg":"r%d","amount":%d}`, i, amount), level: "warn"}
		case 1:
			return oracleLine{line: fmt.Sprintf(`{"level":"ERROR","msg":"r%d","amount":%d}`, i, amount), level: "error"}
		}
		return oracleLine{line: fmt.Sprintf(`{"msg":"r%d","amount":%d}`, i, amount), level: "unknown"}
	}
	labelled := func(i int) oracleLine {
		return oracleLine{line: fmt.Sprintf(`{"level":"error","msg":"r%d","amount":%d}`, i, i%7+1), level: "info"}
	}
	runLevelOracle(t, []oracleStream{
		{labels: map[string]string{"app": "mj", "pod": "j1"}, lines: mixed},
		{labels: map[string]string{"app": "mj", "pod": "j2", "level": "info"}, lines: labelled},
	}, `{app="mj"}`, []string{
		`rate(S | json [1m])`,
		`label_replace(rate(S | json [1m]), "x", "$1", "pod", "(.*)")`,
		`count_over_time(S | json [5m])`,
		`sort(rate(S | json [1m]))`,
		`sum without (pod) (rate(S | json amount | unwrap amount [5m]))`,
	}, "testdata/detected_level_levelless_json_loki_answers.jsonl", reduceByLevel)
}

// TestUnwrapExtractionListAnswersTheStreamsLokiAnswers: `| logfmt amount | unwrap amount` under every range function
// answers the streams Loki answers when VictoriaLogs keeps the line (a Loki push with disable_message_parsing=1, or
// any route that stores the line in _msg): a logfmt line with amount, and structured metadata amount beside a line
// without it; a JSON line and a logfmt line without amount are dropped, with status 200 and no error. A JSON line
// that VictoriaLogs' Loki push unpacked into fields at ingest is outside this check
// (profiles/json-line-stored-without-line).
//
// conformance: semantics/detected-level-stream-stats-without-level
func TestUnwrapExtractionListAnswersTheStreamsLokiAnswers(t *testing.T) {
	body := func(format string, sm bool) func(i int) oracleLine {
		return func(i int) oracleLine {
			amount := i%7 + 1
			l := oracleLine{line: fmt.Sprintf(format, i, amount), level: "error"}
			if sm {
				l.sm = map[string]string{"amount": strconv.Itoa(amount)}
			}
			return l
		}
	}
	var queries []string
	for _, fn := range []string{"max_over_time", "min_over_time", "avg_over_time", "sum_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time", "rate_counter", "rate"} {
		queries = append(queries, fn+`(S | logfmt amount | unwrap amount [5m])`)
	}
	queries = append(queries, `quantile_over_time(0.5, S | logfmt amount | unwrap amount [5m])`)
	streams := []oracleStream{
		{labels: map[string]string{"app": "ux", "fmt": "logfmt", "level": "error"}, lines: body(`msg="r%d" amount=%d status=200`, false)},
		{labels: map[string]string{"app": "ux", "fmt": "json", "level": "error"}, lines: body(`{"msg": "r%d", "amount": %d, "status": 200}`, false)},
		{labels: map[string]string{"app": "ux", "fmt": "noamount", "level": "error"}, lines: body(`msg="r%d" other=%d`, false)},
		{labels: map[string]string{"app": "ux", "fmt": "smamount", "level": "error"}, lines: body(`plain text r%d %d`, true)},
	}
	runLevelOracle(t, streams, `{app="ux"}`, queries, "testdata/unwrap_extraction_list_loki_answers.jsonl", reduceByStreams)
	// Loki's answer, from the recording: the logfmt and the metadata streams answer, the json and no-amount lines are
	// dropped with 200.
	f, err := os.Open("testdata/unwrap_extraction_list_loki_answers.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a map[string]string
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		if a["answer"] != "200 logfmt smamount" {
			t.Errorf("Loki %s: %s", a["id"], a["answer"])
		}
	}
}

// TestLevelLessStreamFilterOnVictoriaLogs: the restriction of the second read (levelLessStreamFilter, sent as
// extra_filters) keeps, on VictoriaLogs v1.49.0 and v1.53.0, exactly the streams streamDecidesLevel says decide no
// level: a missing and an empty label alike, every level name of the list, detected_level; and the restriction of a
// first read known to hold such streams (labelledStreamFilter) keeps exactly the others.
//
// conformance: semantics/detected-level-stream-stats-without-level
func TestLevelLessStreamFilterOnVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs")
	}
	t.Cleanup(querycorpus.PauseRecording())
	streams := []map[string]string{
		{"app": "f", "pod": "none"},
		{"app": "f", "pod": "sevnum", "severity_number": "17"},
		{"app": "f", "pod": "other", "loglevel": "error"},
		{"app": "f", "pod": "dl", "detected_level": "debug"},
	}
	for i, name := range levelFieldNames {
		streams = append(streams, map[string]string{"app": "f", "pod": "n" + strconv.Itoa(i), name: "error"})
	}
	var rows oraclerun.ServeDataSet
	for _, s := range streams {
		var stream [][2]string
		for k, v := range s {
			stream = append(stream, [2]string{k, v})
		}
		sort.Slice(stream, func(i, j int) bool { return stream[i][0] < stream[j][0] })
		rows.Rows = append(rows.Rows, oraclerun.ServeRow{TS: oracleLineTS(1), Stream: stream, Fields: [][2]string{{"_msg", "line"}}})
	}
	var want []string
	for _, s := range streams {
		if !streamDecidesLevel(s) {
			want = append(want, s["pod"])
		}
	}
	sort.Strings(want)
	var labelled []string
	for _, s := range streams {
		if streamDecidesLevel(s) {
			labelled = append(labelled, s["pod"])
		}
	}
	sort.Strings(labelled)
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{rows})
		if err != nil {
			t.Fatal(err)
		}
		for _, check := range []struct {
			filter string
			want   []string
		}{{levelLessStreamFilter(), want}, {labelledStreamFilter(), labelled}} {
			form := url.Values{"query": {`app:="f" | stats by (pod) count()`}, "extra_filters": {check.filter}}
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/select/logsql/query", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("AccountID", "0")
			req.Header.Set("ProjectID", "0")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var got []string
			for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
				var row map[string]string
				if json.Unmarshal([]byte(line), &row) == nil && row["pod"] != "" {
					got = append(got, row["pod"])
				}
			}
			sort.Strings(got)
			if resp.StatusCode != http.StatusOK || strings.Join(got, ",") != strings.Join(check.want, ",") {
				t.Errorf("%s %s: %d %v, want %v (%s)", v.version, check.filter, resp.StatusCode, got, check.want, body)
			}
		}
		srv.Stop()
	}
}
