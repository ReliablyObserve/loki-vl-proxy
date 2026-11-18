package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// planSpellingQueries are metric queries spelled in the ways the string translator reads otherwise than the query
// plan, which reads the parsed tree (internal/testutil/translatorspelling names the case of each): quoted text holding
// a parenthesis under a binary operation (alone, under a ranking, as the first argument of label_replace), a
// comparison with bool under a vector aggregation, the operator, number and vector matching spellings of a binary
// operation (alone and under a ranking), a quantile written otherwise than the shortest form of its value (with and
// without a parser), and an unwrap conversion with a blank before or inside its parenthesis. The proxy serves the
// plan's text for them.
var planSpellingQueries = []string{
	`count_over_time({env="production"} != "(" [1m]) * 2`,
	`topk(2, count_over_time({env="production"} != "(" [1m]) * 2)`,
	`bottomk(1, count_over_time({env="production"} != "(" [1m]) * 2)`,
	`sort_desc(count_over_time({env="production"} != "(" [1m]) * 2)`,
	`label_replace(rate({a=~"x\\(y"}[5m]) + rate({b="z"}[5m]), "d", "$1", "app", "(.*)")`,
	"label_replace(rate({namespace=\"prod\"} |= `,)` [5m]) + rate({b=\"z\"}[5m]), \"d\", \"$1\", \"app\", \"(.*)\")",
	`sum(sum by (job) (sum_over_time({app="uw"} | json | unwrap amount [5m])) > bool 1) / sum by (job) (count_over_time({app="uw"} | logfmt [5m]))`,
	`sum by (job) (sum by (job) (sum_over_time({app="uw"} | json | unwrap amount [5m])) > bool 6)`,
	`sum by (pod) (count_over_time({env="production"}[1m]))/2`,
	`sum by (pod) (count_over_time({env="production"}[1m])) -2`,
	`sum by (pod) (count_over_time({env="production"}[1m])) * 1e3`,
	`sum by (pod) (count_over_time({env="production"}[1m])) / on(pod,app) sum by (pod, app) (count_over_time({env="production"}[1m]))`,
	`sort_desc(count_over_time({env="production"}[1m]) /2)`,
	`topk(2, count_over_time({env="production"} |= "ok" [1m]) * 1e3)`,
	`quantile_over_time(.5, {app="qt"} | logfmt | unwrap v [5m])`,
	`quantile_over_time(0.50, {app="qt"} | logfmt | unwrap v [5m])`,
	`quantile_over_time(5e-1, {app="qt"} | logfmt | unwrap v [5m])`,
	`quantile_over_time(.5, {app="qn"} | unwrap v [5m])`,
	`quantile_over_time(0.50, {app="qn"} | unwrap v [5m])`,
	`sum by (pod) (quantile_over_time(5e-1, {app="qn"} | unwrap v [5m]))`,
	`sum_over_time({app="qb"} | logfmt | unwrap bytes (b) [5m])`,
	`sum_over_time({app="qb"} | logfmt | unwrap bytes( b ) [5m])`,
	`max_over_time({app="qd"} | logfmt | unwrap duration (d) [5m])`,
	`sum by (pod) (max_over_time({app="qd"} | logfmt | unwrap duration_seconds (d) [5m]))`,
}

// planSpellingRows are the lines the queries read: three pods of env=production with a share of lines holding a
// parenthesis that differs per pod (so a ranking is not a tie), streams whose label a regexp with an escaped
// parenthesis matches or not, lines holding `,)` or not, JSON lines with an amount by job, logfmt lines with a value, a
// byte size or a duration (one key a line, so the series are the streams), and a stream with the value as structured metadata (a field to VictoriaLogs).
func planSpellingRows(s0 time.Time) []pushdownRow {
	var rows []pushdownRow
	add := func(i int, stream map[string]string, msg string) {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: stream, msg: msg})
	}
	for i := 0; i < 130; i++ {
		pod := []string{"p1", "p2", "p3"}[i%3]
		for k, pd := range []string{"p1", "p2", "p3"} {
			msg := "ok " + strconv.Itoa(i)
			if i%(k+1) != 0 {
				msg = "slow (" + strconv.Itoa(i) + ")"
			}
			add(i, map[string]string{"env": "production", "app": "api", "pod": pd}, msg)
		}
		switch i % 3 {
		case 0:
			add(i, map[string]string{"a": "x(y", "b": "z", "app": "p"}, "line "+strconv.Itoa(i))
		case 1:
			add(i, map[string]string{"a": "xy", "b": "z", "app": "q"}, "line "+strconv.Itoa(i))
		case 2:
			add(i, map[string]string{"a": "x(y", "app": "s"}, "line "+strconv.Itoa(i))
		}
		m := "plain"
		if i%2 == 0 {
			m = "a,) b"
		}
		add(i, map[string]string{"namespace": "prod", "b": "z", "app": "r"}, m)
		add(i, map[string]string{"app": "uw", "job": []string{"j1", "j2"}[i%2]}, fmt.Sprintf(`{"amount":%d}`, i%5))
		add(i, map[string]string{"app": "qt", "pod": pod}, fmt.Sprintf(`v=%d`, (i*7)%11))
		add(i, map[string]string{"app": "qb", "pod": pod}, fmt.Sprintf(`b=%dKB`, i%4+1))
		add(i, map[string]string{"app": "qd", "pod": pod}, fmt.Sprintf(`d=%dms`, (i*13)%17+1))
		v := strconv.Itoa((i * 7) % 11)
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "qn", "pod": pod},
			vl: map[string]string{"v": v}, loki: map[string]string{"v": v}, msg: "m " + strconv.Itoa(i)})
	}
	return rows
}

// planSpellingVLAnswers holds what VictoriaLogs answered the proxy for each request of TestPlanSpellingsMatchLoki,
// by version: recorded with LVP_RUN_ORACLES=1 from VictoriaLogs' own query handlers (the serve oracle,
// test/oracle/vl), replayed otherwise.
const planSpellingVLAnswers = "testdata/plan_spellings_vl_answers.json.gz"

// vlAnswer is one recorded VictoriaLogs answer.
type vlAnswer struct {
	Status int    `json:"status"`
	Type   string `json:"type,omitempty"`
	Body   string `json:"body"`
}

// vlRequestKey is a request to VictoriaLogs as the replay finds it: the path and the form, sorted.
func vlRequestKey(r *http.Request) string {
	_ = r.ParseForm()
	return r.URL.Path + "?" + r.Form.Encode()
}

// vlReplayFront answers the proxy from the recorded answers of a version, or, recording, asks VictoriaLogs at
// upstream and records what it answers. An unrecorded request fails the test.
func vlReplayFront(t *testing.T, version, upstream string, recorded map[string]vlAnswer, mu *sync.Mutex) http.Handler {
	live := levelSweepVLFront(version, upstream)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/metrics":
			live.ServeHTTP(w, r)
			return
		}
		key := vlRequestKey(r)
		if upstream != "" {
			body := r.Form.Encode()
			fwd := r.Clone(r.Context())
			fwd.Body, fwd.ContentLength = io.NopCloser(strings.NewReader(body)), int64(len(body))
			fwd.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			fwd.Method = http.MethodPost
			rec := httptest.NewRecorder()
			live.ServeHTTP(rec, fwd)
			mu.Lock()
			recorded[key] = vlAnswer{Status: rec.Code, Type: rec.Header().Get("Content-Type"), Body: rec.Body.String()}
			mu.Unlock()
			if ct := rec.Header().Get("Content-Type"); ct != "" {
				w.Header().Set("Content-Type", ct)
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		mu.Lock()
		a, ok := recorded[key]
		mu.Unlock()
		if !ok {
			t.Errorf("%s: VictoriaLogs answer not recorded for %s: record again with LVP_RUN_ORACLES=1", version, key)
			http.Error(w, "not recorded", http.StatusInternalServerError)
			return
		}
		if a.Type != "" {
			w.Header().Set("Content-Type", a.Type)
		}
		w.WriteHeader(a.Status)
		_, _ = io.WriteString(w, a.Body)
	})
}

// planSpellingServeSet is the rows as the VictoriaLogs serve oracle stores them: the stream labels, the line and the
// structured metadata as fields.
func planSpellingServeSet(rows []pushdownRow) oraclerun.ServeDataSet {
	set := oraclerun.ServeDataSet{}
	for _, r := range rows {
		keys := make([]string, 0, len(r.stream))
		for k := range r.stream {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var st [][2]string
		for _, k := range keys {
			st = append(st, [2]string{k, r.stream[k]})
		}
		fields := [][2]string{{"_msg", r.msg}}
		fk := make([]string, 0, len(r.vl))
		for k := range r.vl {
			fk = append(fk, k)
		}
		sort.Strings(fk)
		for _, k := range fk {
			fields = append(fields, [2]string{k, r.vl[k]})
		}
		set.Rows = append(set.Rows, oraclerun.ServeRow{TS: r.ts.UnixNano(), Stream: st, Fields: fields})
	}
	return set
}

// The spellings the string translator reads otherwise than the query plan answer what Loki answers for them (Loki's
// engine over the same lines, recorded with the engine oracle), through the proxy's routes against VictoriaLogs
// v1.49.0 and v1.53.0 (their own query handlers, recorded through the serve oracle and replayed; LVP_RUN_ORACLES=1
// records both again, with a module cache in which VictoriaLogs' sources match test/oracle/vl/go.sum). The proxy
// serves the plan's text for these queries (no text screen turns them away to the translator): quoted text holding a
// parenthesis under a binary operation, alone, under a ranking or as label_replace's argument, the operator, number
// and vector matching spellings of a binary operation, a quantile written otherwise than its shortest form (`.5`,
// `0.50`, `5e-1`; VictoriaLogs rejects the `.5` the translator copied), and an unwrap conversion with a blank before or
// inside its parenthesis. A query that returns no series is compared as such.
//
// conformance: semantics/translator-reads-quoted-text-as-syntax, semantics/translator-reads-binary-operation-text, semantics/translator-writes-leading-dot-quantile, semantics/blank-before-call-parenthesis-read-unlike-loki, loki_api_v1_query_range
func TestPlanSpellingsMatchLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(6*time.Minute), s0.Add(10*time.Minute)
	rows := planSpellingRows(s0)
	recording := os.Getenv("LVP_RUN_ORACLES") == "1"
	recorded := map[string]map[string]vlAnswer{}
	if !recording {
		recorded = readPlanSpellingVLAnswers(t)
	}
	var mu sync.Mutex
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
		upstream := ""
		if recording {
			srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{planSpellingServeSet(rows)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(srv.Stop)
			upstream = srv.URL
			recorded[v.version] = map[string]vlAnswer{}
		}
		front := httptest.NewServer(vlReplayFront(t, v.version, upstream, recorded[v.version], &mu))
		t.Cleanup(front.Close)
		p, err := New(Config{BackendURL: front.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		for _, q := range planSpellingQueries {
			params := url.Values{"query": {q}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "0")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			want := lokiPushdownMatrix(t, "plan_spellings", rows, q, start, end, time.Minute)
			if rec.Code != http.StatusOK {
				t.Errorf("%s: %q: status %d: %.300s", v.version, q, rec.Code, rec.Body)
				continue
			}
			// Loki's engine oracle derives no detected_level (Loki derives it at ingest).
			if got := withoutMatrixLabel(t, rec.Body.Bytes(), "detected_level"); !sameMatrixValues(got, want) {
				t.Errorf("%s: %q\n got: %v\nwant: %v", v.version, q, got, want)
			}
		}
	}
	if recording {
		writePlanSpellingVLAnswers(t, recorded)
	}
}

func readPlanSpellingVLAnswers(t *testing.T) map[string]map[string]vlAnswer {
	t.Helper()
	f, err := os.Open(planSpellingVLAnswers)
	if err != nil {
		t.Fatalf("%v: record the VictoriaLogs answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]vlAnswer{}
	if err := json.NewDecoder(bufio.NewReader(zr)).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func writePlanSpellingVLAnswers(t *testing.T, answers map[string]map[string]vlAnswer) {
	t.Helper()
	data, err := json.MarshalIndent(answers, "", " ") // map keys are written sorted: the file is deterministic
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
	if _, err := zw.Write(data); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(planSpellingVLAnswers, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
