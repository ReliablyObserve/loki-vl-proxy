package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// levelSweepCases are the streams of the detected_level sweep: where Loki finds a line's level (a stream label in
// several spellings and casings, the line body as logfmt, JSON or text, structured metadata, a stream label beside
// a conflicting body or metadata level, nothing) and the value Loki 3.7.9 answered for every line of the stream,
// measured live on 2026-11-15 with `sum by (case, detected_level) (count_over_time({app="lvlprobe"}[20m]))` and
// with the log query `{app="lvlprobe"}` (both the same): Loki reads the stream label first, then structured
// metadata, then the body, else unknown, and normalises a known spelling (ERROR is error, warning warn) while it
// keeps an unknown word (x). Every line of a case gets the same level, so the value is the stream's.
var levelSweepCases = []struct {
	name   string
	stream map[string]string
	sm     map[string]string
	body   string // fmt pattern of the line: the row number, then the amount
	loki   string
}{
	{"label-level-ERROR", map[string]string{"level": "ERROR"}, nil, "plain message r%d amount=%d", "error"},
	{"label-LEVEL-warning", map[string]string{"LEVEL": "warning"}, nil, "plain message r%d amount=%d", "warn"},
	{"label-severity-error", map[string]string{"severity": "error"}, nil, "plain message r%d amount=%d", "error"},
	{"label-lvl-x", map[string]string{"lvl": "x"}, nil, "plain message r%d amount=%d", "x"},
	{"body-logfmt-error", nil, nil, `level=error msg="r%d" amount=%d`, "error"},
	{"body-json-warn", nil, nil, `{"level":"warn","msg":"r%d","amount":%d}`, "warn"},
	{"body-json-WARNING", nil, nil, `{"level":"WARNING","msg":"r%d","amount":%d}`, "warn"},
	{"label-info-body-error", map[string]string{"level": "info"}, nil, `level=error msg="r%d" amount=%d`, "info"},
	{"sm-level-debug", nil, map[string]string{"level": "debug"}, "plain message r%d amount=%d", "debug"},
	{"label-error-sm-debug", map[string]string{"severity": "error"}, map[string]string{"level": "debug"}, "plain message r%d amount=%d", "error"},
	{"nothing", nil, nil, "plain message r%d amount=%d", "unknown"},
	{"body-text-error-word", nil, nil, "ERROR something failed r%d amount=%d", "error"},
}

// levelSweepQueries are the query types of TestOneDetectedLevelPerStreamAcrossQueryTypes over one case's stream
// (S): plain range functions, parsers, outer sort, topk and label_replace, without folds, unwraps, a comparison
// and a by (detected_level) aggregation.
var levelSweepQueries = []string{
	`rate(S[1m])`,
	`count_over_time(S[5m])`,
	`count_over_time(S | logfmt [5m])`,
	`bytes_over_time(S | logfmt [5m])`,
	`rate(S | json [1m])`,
	`sort(rate(S | json [1m]))`,
	`sort_desc(count_over_time(S | logfmt [5m]))`,
	`label_replace(rate(S | json [1m]), "x", "$1", "pod", "(.*)")`,
	`topk(3, count_over_time(S | logfmt [5m]))`,
	`sum without (pod) (count_over_time(S[5m]))`,
	`sum without (pod) (rate(S | json amount | unwrap amount [5m]))`,
	`max_over_time(S | logfmt amount | unwrap amount [5m])`,
	`count_over_time(S | logfmt [5m]) > 0`,
	`sum by (detected_level) (count_over_time(S[5m]))`,
	// The detected_level filters, which VictoriaLogs' chain evaluates: a stream keeps its lines under the filter of
	// its own level and none under another.
	`count_over_time(S | detected_level="error" [5m])`,
	`count_over_time(S | detected_level="debug" [5m])`,
	`sum(count_over_time(S | detected_level=~"err.*" [5m]))`,
}

var levelSweepModes = []string{"instant", "60", "300"}

const levelSweepAnswersPath = "testdata/detected_level_sweep_loki_answers.jsonl"

// levelSweepBase is the time of the instant requests and the end of the range ones (seconds).
const levelSweepBase = int64(1_760_000_000)

type levelSweepRow struct {
	ts     int64
	amount int
	line   string
}

// levelSweepRowsOf are case ci's lines: 20, 7 s apart across the cases, so every window and step holds some.
func levelSweepRowsOf(ci int) []levelSweepRow {
	c := levelSweepCases[ci]
	var out []levelSweepRow
	for i := ci; i < 20*len(levelSweepCases); i += len(levelSweepCases) {
		amount := i%9 + 1
		out = append(out, levelSweepRow{ts: levelSweepBase*1e9 - int64(i)*7e9 - 3e8, amount: amount, line: fmt.Sprintf(c.body, i, amount)})
	}
	return out
}

func levelSweepStream(ci int) map[string]string {
	out := map[string]string{"app": "lv", "case": levelSweepCases[ci].name, "pod": "p" + strconv.Itoa(ci)}
	for k, v := range levelSweepCases[ci].stream {
		out[k] = v
	}
	return out
}

func levelSweepSelector(ci int) string {
	return fmt.Sprintf(`{app="lv",case=%q}`, levelSweepCases[ci].name)
}

func levelSweepQuery(ci, qi int) string {
	return strings.Replace(levelSweepQueries[qi], "S", levelSweepSelector(ci), 1)
}

func levelSweepID(ci, qi int, mode string) string {
	return levelSweepCases[ci].name + "|" + strconv.Itoa(qi) + "|" + mode
}

func levelSweepForm(q, mode string) url.Values {
	form := url.Values{"query": {q}}
	if mode == "instant" {
		form.Set("time", strconv.FormatInt(levelSweepBase, 10))
	} else {
		form.Set("start", strconv.FormatInt(levelSweepBase-900, 10))
		form.Set("end", strconv.FormatInt(levelSweepBase, 10))
		form.Set("step", mode)
	}
	return form
}

// levelSweepAnswer is one answer reduced to what the sweep compares: the status, the error text, the number of
// series and the detected_level values they carry ("-" for a series without one), sorted and unique.
type levelSweepAnswer struct {
	ID     string   `json:"id"`
	Status int      `json:"status"`
	Error  string   `json:"error,omitempty"`
	Series int      `json:"series"`
	Levels []string `json:"levels,omitempty"`
}

func levelSweepReduce(id string, status int, body []byte, errText string) levelSweepAnswer {
	a := levelSweepAnswer{ID: id, Status: status, Error: strings.TrimSpace(errText)}
	if status != http.StatusOK {
		return a
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &resp)
	seen := map[string]bool{}
	for _, s := range resp.Data.Result {
		v, ok := s.Metric["detected_level"]
		if !ok {
			v = "-"
		}
		if !seen[v] {
			seen[v] = true
			a.Levels = append(a.Levels, v)
		}
	}
	a.Series = len(resp.Data.Result)
	sort.Strings(a.Levels)
	return a
}

// recordLevelSweepAnswers runs Loki's engine (test/oracle/loki engine) over the sweep. The engine has no ingest
// path, so each line carries the detected_level Loki's ingest gave it (levelSweepCases.loki, live Loki) as
// structured metadata, as Loki stores it, and each stream the service_name label its ingest adds; the engine decides
// the rest (which series exist, the parser errors and their status and text).
func recordLevelSweepAnswers(t *testing.T) {
	t.Helper()
	set := oraclerun.EngineDataSet{}
	for ci, c := range levelSweepCases {
		// Loki's ingest also names the service of every stream (service_name from app), which the engine leaves out.
		st := oraclerun.EngineStream{Labels: levelSweepStream(ci)}
		st.Labels["service_name"] = st.Labels["app"]
		for _, r := range levelSweepRowsOf(ci) {
			md := map[string]string{"detected_level": c.loki}
			for k, v := range c.sm {
				md[k] = v
			}
			st.Entries = append(st.Entries, oraclerun.EngineEntry{TS: r.ts, Line: r.line, Metadata: md})
		}
		set.Streams = append(set.Streams, st)
		for qi := range levelSweepQueries {
			for _, mode := range levelSweepModes {
				set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: levelSweepID(ci, qi, mode), Instant: mode == "instant", Form: levelSweepForm(levelSweepQuery(ci, qi), mode).Encode()})
			}
		}
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(levelSweepAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, r := range set.Requests {
		a := answers[r.ID]
		if err := enc.Encode(levelSweepReduce(r.ID, a.Status, a.Body, a.Error)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func readLevelSweepAnswers(t *testing.T) map[string]levelSweepAnswer {
	t.Helper()
	f, err := os.Open(levelSweepAnswersPath)
	if err != nil {
		t.Fatalf("%v: LVP_RUN_ORACLES=1 records it", err)
	}
	defer func() { _ = f.Close() }()
	out := map[string]levelSweepAnswer{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a levelSweepAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out[a.ID] = a
	}
	if want := len(levelSweepCases) * len(levelSweepQueries) * len(levelSweepModes); len(out) != want {
		t.Fatalf("%d recorded answers for %d requests: record them again with LVP_RUN_ORACLES=1", len(out), want)
	}
	return out
}

// levelSweepProxyAnswer sends one sweep request to a proxy over backend and reduces its answer.
func levelSweepProxyAnswer(t *testing.T, backend, id, q, mode string) levelSweepAnswer {
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
	res := doCompatProxyRequest(p, target+levelSweepForm(q, mode).Encode(), map[string]string{"X-Scope-OrgID": "0"})
	errText := ""
	if res.Code != http.StatusOK {
		var e struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal(res.Body.Bytes(), &e) == nil && (e.Error != "" || e.Message != "") {
			errText = e.Error + e.Message
		} else {
			errText = res.Body.String()
		}
	}
	return levelSweepReduce(id, res.Code, res.Body.Bytes(), errText)
}

// levelSweepKnown are the answers of the sweep that differ from Loki's for a listed reason, each by the kind of
// difference (levelSweepDiff), the queries (indexes into levelSweepQueries), cases and backends it covers (nil or ""
// every one). A row that differs and is not listed fails, and so does an entry no differing row needs (on a backend
// that ran), so the list only shrinks.
var levelSweepKnown = []struct {
	kind    string // "status", "text" (an error's text), "series", "absent" (the series carry no detected_level) or "value"
	queries []int
	cases   []string
	backend string // "emulator", "vl" or "" (both)
	why     string
}{
	{"status", []int{5}, nil, "", "sort keeps Loki's strict JSONParserErr 400 of rate(... | json) (fixed in its own change)"},
	{"text", nil, nil, "emulator", "the emulator's raw rows hold the unwrapped field of every row, which the series of the error text name"},
	{"series", []int{14, 15, 16}, nil, "emulator", "the emulator does not run the detected_level filter: every line passes it"},
	{"series", []int{10, 11}, nil, "emulator", "the emulator runs no parser and unwraps the amount of every row; Loki answers only the lines whose parser extracts it"},
	{"value", []int{5, 10}, []string{"body-json-warn", "body-json-WARNING"}, "emulator", "the emulator does not unpack a JSON line on the raw-row read, so the row holds no level field; VictoriaLogs answers warn"},
}

// levelSweepKnownEntry returns the index of the entry of levelSweepKnown that covers a differing row, or -1.
func levelSweepKnownEntry(kind string, qi int, caseName, backend string) int {
	for i, k := range levelSweepKnown {
		if k.kind != kind || (k.backend != "" && k.backend != backend) {
			continue
		}
		if len(k.queries) > 0 && !slices.Contains(k.queries, qi) {
			continue
		}
		if len(k.cases) > 0 && !slices.Contains(k.cases, caseName) {
			continue
		}
		return i
	}
	return -1
}

// levelSweepDiff is why a proxy answer differs from Loki's, "" when it does not, and its kind: the status, the
// series Loki answers (none where Loki has some, some where Loki has none) or, where both answer series, the
// detected_level values (absent when the proxy's series carry none).
func levelSweepDiff(loki, got levelSweepAnswer) (kind, diff string) {
	switch {
	case loki.Status != got.Status:
		return "status", fmt.Sprintf("status %d, Loki %d %s", got.Status, loki.Status, loki.Error)
	case loki.Status != http.StatusOK && got.Error != loki.Error:
		return "text", fmt.Sprintf("error text %q, Loki %q", got.Error, loki.Error)
	case loki.Status != http.StatusOK:
		return "", ""
	case (loki.Series == 0) != (got.Series == 0):
		return "series", fmt.Sprintf("%d series, Loki %d", got.Series, loki.Series)
	case strings.Join(loki.Levels, ",") != strings.Join(got.Levels, ","):
		kind = "value"
		if strings.Join(got.Levels, ",") == "-" {
			kind = "absent"
		}
		return kind, fmt.Sprintf("detected_level %v, Loki %v", got.Levels, loki.Levels)
	}
	return "", ""
}

// levelSweepRun runs the sweep against one backend kind for one VictoriaLogs version and returns the rows equal to
// Loki, the rows that differ for a listed reason (counted per entry into used) and the rows that differ otherwise.
// It logs how many of the rows equal to Loki are errors (the same status and text).
func levelSweepRun(t *testing.T, loki map[string]levelSweepAnswer, backend, version string, used []int, serve func(ci int) string) (same, known int, unexplained []string) {
	t.Helper()
	errorRows := 0
	defer func() {
		t.Logf("%s %s: %d of the rows equal to Loki are errors, with Loki's status and text", backend, version, errorRows)
	}()
	for ci := range levelSweepCases {
		url := serve(ci)
		for qi := range levelSweepQueries {
			for _, mode := range levelSweepModes {
				id := levelSweepID(ci, qi, mode)
				got := levelSweepProxyAnswer(t, url, id, levelSweepQuery(ci, qi), mode)
				kind, diff := levelSweepDiff(loki[id], got)
				if diff == "" {
					same++
					if loki[id].Status != http.StatusOK {
						errorRows++
					}
					continue
				}
				if e := levelSweepKnownEntry(kind, qi, levelSweepCases[ci].name, backend); e >= 0 {
					known++
					used[e]++
					continue
				}
				unexplained = append(unexplained, fmt.Sprintf("%s %s %s %s: %s", backend, version, mode, levelSweepQuery(ci, qi), diff))
			}
		}
	}
	return same, known, unexplained
}

// TestDetectedLevelSweepMatchesLoki: the 14 query types of TestOneDetectedLevelPerStreamAcrossQueryTypes and three
// detected_level filters, each at an instant and on query_range at steps 60 and 300, over the 12 streams of levelSweepCases, against Loki's answers
// (the engine oracle over the lines with the detected_level live Loki gave them, LVP_RUN_ORACLES=1 records them
// again): the status and an error's text, which queries answer series, and the detected_level of every series. The proxy runs over
// the emulator, whose rows hold the stream fields as VictoriaLogs answers them and whose detected_level chain is
// VictoriaLogs' (TestVLChainDetectedLevelMatchesVictoriaLogs), on v1.49.0 and v1.53.0; with LVP_RUN_ORACLES=1
// it also runs over VictoriaLogs itself (test/oracle/vl serve, both versions). Every row equals Loki's but the
// listed ones (levelSweepKnown): no series carries another detected_level than Loki's, against VictoriaLogs.
//
// conformance: semantics/detected-level-one-value-per-stream, severity-detected-level-derivation, metric-series-identity
func TestDetectedLevelSweepMatchesLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordLevelSweepAnswers(t)
	}
	loki := readLevelSweepAnswers(t)
	rows := len(levelSweepCases) * len(levelSweepQueries) * len(levelSweepModes)
	var unexplained []string
	used := make([]int, len(levelSweepKnown))
	ran := map[string]bool{"emulator": true}
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		var servers []*httptest.Server
		same, known, diff := levelSweepRun(t, loki, "emulator", version, used, func(ci int) string {
			var emuRows []vlemu.Row
			stream := levelSweepStream(ci)
			for _, r := range levelSweepRowsOf(ci) {
				emuRows = append(emuRows, vlemu.Row{TS: r.ts, Stream: levelSweepStreamText(stream), Labels: levelSweepCases[ci].sm, Value: float64(r.amount), Msg: r.line})
			}
			srv := httptest.NewServer((&vlemu.Emulator{T: t, Rows: emuRows, Field: "amount", Version: version, DeriveFields: vlChainDetectedLevel, RowStreamFields: true}).Handler())
			servers = append(servers, srv)
			return srv.URL
		})
		for _, s := range servers {
			s.Close()
		}
		t.Logf("emulator %s: %d of %d rows equal to Loki, %d differ for a listed reason", version, same, rows, known)
		unexplained = append(unexplained, diff...)
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		ran["vl"] = true
		for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
			var set oraclerun.ServeDataSet
			for ci, c := range levelSweepCases {
				var stream [][2]string
				for k, val := range levelSweepStream(ci) {
					stream = append(stream, [2]string{k, val})
				}
				sort.Slice(stream, func(i, j int) bool { return stream[i][0] < stream[j][0] })
				for _, r := range levelSweepRowsOf(ci) {
					var fields [][2]string
					for k, val := range c.sm {
						fields = append(fields, [2]string{k, val})
					}
					fields = append(fields, [2]string{"_msg", r.line})
					set.Rows = append(set.Rows, oraclerun.ServeRow{TS: r.ts, Stream: stream, Fields: fields})
				}
			}
			srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{set})
			if err != nil {
				t.Fatal(err)
			}
			front := httptest.NewServer(levelSweepVLFront(v.version, srv.URL))
			same, known, diff := levelSweepRun(t, loki, "vl", v.version, used, func(int) string { return front.URL })
			front.Close()
			srv.Stop()
			t.Logf("VictoriaLogs %s: %d of %d rows equal to Loki, %d differ for a listed reason", v.version, same, rows, known)
			unexplained = append(unexplained, diff...)
		}
	}
	for _, u := range unexplained {
		t.Error(u)
	}
	for i, k := range levelSweepKnown {
		if (k.backend == "" || ran[k.backend]) && used[i] == 0 {
			t.Errorf("listed difference no row has any more (remove it): %s %v %v %s", k.kind, k.queries, k.cases, k.why)
		}
	}
}

// levelSweepStreamText is a stream as VictoriaLogs writes _stream: its labels sorted by name.
func levelSweepStreamText(stream map[string]string) string {
	keys := make([]string, 0, len(stream))
	for k := range stream {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%q", k, stream[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// levelSweepVLFront answers the version probes of the proxy and passes every other request to VictoriaLogs' query
// server under tenant 0.
func levelSweepVLFront(version, upstream string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = io.WriteString(w, "OK")
			return
		case "/metrics":
			_, _ = io.WriteString(w, `vm_app_version{version="victoria-logs-20260101-000000-tags-`+version+`-0-g0000000", short_version="`+version+`"} 1`+"\n")
			return
		}
		body, _ := io.ReadAll(r.Body)
		req, err := http.NewRequestWithContext(r.Context(), r.Method, upstream+r.URL.RequestURI(), strings.NewReader(string(body)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
		req.Header.Set("AccountID", "0")
		req.Header.Set("ProjectID", "0")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
}
