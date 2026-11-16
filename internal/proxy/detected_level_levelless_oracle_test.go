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

// oracleReduce is an answer reduced to what these checks compare: the status, an error's text, and per series (its
// labels other than service_name, sorted) the sum of its values over every step, rounded to six digits.
func oracleReduce(status int, body []byte, errText string) string {
	if status != http.StatusOK {
		return fmt.Sprintf("%d %s", status, strings.TrimSpace(errText))
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &resp)
	var series []string
	for _, s := range resp.Data.Result {
		var names []string
		for k := range s.Metric {
			if k != "service_name" {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		parts := make([]string, len(names))
		for i, k := range names {
			parts[i] = k + "=" + s.Metric[k]
		}
		points := s.Values
		if s.Value != nil {
			points = [][]any{s.Value}
		}
		sum := 0.0
		for _, pt := range points {
			if len(pt) == 2 {
				v, _ := strconv.ParseFloat(fmt.Sprint(pt[1]), 64)
				sum += v
			}
		}
		series = append(series, "{"+strings.Join(parts, ",")+"} "+strconv.FormatFloat(math.Round(sum*1e6)/1e6, 'f', -1, 64))
	}
	sort.Strings(series)
	return fmt.Sprintf("200 %d series: %s", len(series), strings.Join(series, "; "))
}

// oracleProxyAnswer sends one request to a proxy over backend and reduces its answer.
func oracleProxyAnswer(t *testing.T, backend, q, mode string) string {
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
	return oracleReduce(res.Code, res.Body.Bytes(), errText)
}

// runLevelOracle records Loki's answers to queries (S replaced with selector) over streams at every mode into
// answersPath, then runs the proxy over VictoriaLogs v1.49.0 and v1.53.0 and reports every request whose answer
// differs from Loki's and is not in known (query and mode, "query|mode" or "query|*").
func runLevelOracle(t *testing.T, streams []oracleStream, selector string, queries []string, answersPath string, known map[string]string) {
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
		loki[r.ID] = oracleReduce(a.Status, a.Body, a.Error)
		line, _ := json.Marshal(map[string]string{"id": r.ID, "answer": loki[r.ID]})
		_, _ = w.Write(append(line, '\n'))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{rows})
		if err != nil {
			t.Fatal(err)
		}
		front := httptest.NewServer(levelSweepVLFront(v.version, srv.URL))
		same := 0
		for qi, q := range queries {
			for _, mode := range levelSweepModes {
				got := oracleProxyAnswer(t, front.URL, strings.ReplaceAll(q, "S", selector), mode)
				want := loki[id(qi, mode)]
				if got == want {
					same++
					continue
				}
				if why, ok := known[q+"|"+mode]; ok {
					used[q+"|"+mode] = true
					t.Logf("%s %s %s: known (%s)", v.version, mode, q, why)
					continue
				}
				if why, ok := known[q+"|*"]; ok {
					used[q+"|*"] = true
					t.Logf("%s %s %s: known (%s)", v.version, mode, q, why)
					continue
				}
				t.Errorf("%s %s %s:\n got %s\nLoki %s", v.version, mode, q, got, want)
			}
		}
		t.Logf("VictoriaLogs %s: %d of %d answers equal to Loki's", v.version, same, len(queries)*len(levelSweepModes))
		front.Close()
		srv.Stop()
	}
	for k := range known {
		if !used[k] {
			t.Errorf("known difference no answer has any more (remove it): %s", k)
		}
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
	}, "testdata/detected_level_levelless_loki_answers.jsonl", map[string]string{
		// Loki's series over a parser also carry the labels it extracts (level, msg, ...); the proxy's
		// per-stream reads name the stream (semantics/unwrap-bare-range-exposes-parsed-labels and the parser
		// identity cases), so the series differ in those labels, not in detected_level.
		`count_over_time(S | logfmt [5m])|*`:     "parsed labels in the series (open parser identity cases)",
		`bytes_over_time(S | logfmt [5m])|*`:     "parsed labels in the series (open parser identity cases)",
		`count_over_time(S | logfmt [5m]) > 0|*`: "parsed labels in the series (open parser identity cases)",
		`sort(rate(S | logfmt [1m]))|*`:          "parsed labels in the series (open parser identity cases)",
		// The structured metadata level="debug" is a label of Loki's series, not of the proxy's
		// (metric-identity/structured-metadata-splits-series, owner-kept); the detected_level and the values are
		// Loki's. At an instant the series also carry every parsed key (semantics/unwrap-bare-range-exposes-parsed-labels).
		`max_over_time(S | logfmt amount | unwrap amount [5m])|*`:                     "structured metadata in the series (owner-kept)",
		`sum without (pod) (max_over_time(S | logfmt amount | unwrap amount [5m]))|*`: "structured metadata in the series (owner-kept)",
	})
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
	known := map[string]string{}
	for _, q := range queries {
		// The proxy labels each series with every key the line holds (msg, status), Loki with the stream
		// (semantics/unwrap-bare-range-exposes-parsed-labels, semantics/unwrap-extraction-list-series-identity).
		known[q+"|*"] = "series labels under an extraction list"
	}
	streams := []oracleStream{
		{labels: map[string]string{"app": "ux", "fmt": "logfmt", "level": "error"}, lines: body(`msg="r%d" amount=%d status=200`, false)},
		{labels: map[string]string{"app": "ux", "fmt": "json", "level": "error"}, lines: body(`{"msg": "r%d", "amount": %d, "status": 200}`, false)},
		{labels: map[string]string{"app": "ux", "fmt": "noamount", "level": "error"}, lines: body(`msg="r%d" other=%d`, false)},
		{labels: map[string]string{"app": "ux", "fmt": "smamount", "level": "error"}, lines: body(`plain text r%d %d`, true)},
	}
	runLevelOracle(t, streams, `{app="ux"}`, queries, "testdata/unwrap_extraction_list_loki_answers.jsonl", known)
	// Which streams answer: the dimension both sides already agree on, checked from the recorded answers so a
	// re-recording that changes it fails here.
	f, err := os.Open("testdata/unwrap_extraction_list_loki_answers.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<22)
	for sc.Scan() {
		var a map[string]string
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(a["answer"], "200 ") || strings.Contains(a["answer"], "fmt=json") || strings.Contains(a["answer"], "fmt=noamount") ||
			!strings.Contains(a["answer"], "fmt=logfmt") || !strings.Contains(a["answer"], "fmt=smamount") {
			t.Errorf("Loki %s: %s", a["id"], a["answer"])
		}
	}
}

// TestLevelLessStreamFilterOnVictoriaLogs: the restriction of the second read (levelLessStreamFilter, sent as
// extra_filters) keeps, on VictoriaLogs v1.49.0 and v1.53.0, exactly the streams streamDecidesLevel says decide no
// level: a missing and an empty label alike, every level name of the list, detected_level.
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
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{rows})
		if err != nil {
			t.Fatal(err)
		}
		form := url.Values{"query": {`app:="f" | stats by (pod) count()`}, "extra_filters": {levelLessStreamFilter()}}
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
		srv.Stop()
		var got []string
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			var row map[string]string
			if json.Unmarshal([]byte(line), &row) == nil && row["pod"] != "" {
				got = append(got, row["pod"])
			}
		}
		sort.Strings(got)
		if resp.StatusCode != http.StatusOK || strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: %d %v, want %v (%s)", v.version, resp.StatusCode, got, want, body)
		}
	}
}
