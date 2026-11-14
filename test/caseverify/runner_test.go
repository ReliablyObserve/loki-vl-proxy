package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

var testNow = time.Date(2027, 1, 15, 10, 0, 0, 0, time.UTC)

func TestProbeOf(t *testing.T) {
	doc := func(extra string) registry.Case {
		d, err := registry.Parse([]byte("id: a/b\nconsumer: api\n" + extra))
		if err != nil {
			t.Fatal(err)
		}
		return registry.Case{ID: "a/b", Doc: d}
	}
	cases := []struct {
		name, extra, notExec, blocked string
	}{
		{"http", "request: {method: GET, path: /loki/api/v1/labels}\ncompare: {mode: exact}\n", "", ""},
		{"scenario", "request: {scope: cache, entry: x}\ncompare: {mode: observed}\n", "a scenario case", ""},
		{"precondition", "request: {method: GET, path: /x, precondition: busy}\ncompare: {mode: status}\n", "", "precondition"},
		{"setup", "request: {method: GET, path: /x, setup: seed}\ncompare: {mode: status}\n", "", "setup step"},
		{"push", "request: {method: POST, path: /loki/api/v1/push}\ncompare: {mode: status}\n", "", "a write"},
		{"tail", "request: {method: GET, path: /loki/api/v1/tail}\ncompare: {mode: status}\n", "", "websocket"},
		{"grafana", "request: {method: POST, path: /api/ds/query}\ncompare: {mode: status}\n", "", "Grafana"},
		{"double slash is sent", "request: {method: GET, path: /loki//api/v1/labels}\ncompare: {mode: status}\n", "", ""},
	}
	for _, c := range cases {
		p := probeOf(doc(c.extra))
		if !strings.Contains(p.NotExecutable, c.notExec) || (c.notExec == "") != (p.NotExecutable == "") ||
			!strings.Contains(p.Blocked, c.blocked) || (c.blocked == "") != (p.Blocked == "") {
			t.Errorf("%s: not executable %q, blocked %q", c.name, p.NotExecutable, p.Blocked)
		}
	}
	p := probeOf(doc("request: {method: GET, path: /x, params: {limit: 5}, headers: {X-Scope-OrgID: '1'}}\n" +
		"compare: {mode: numeric, fields: [values, series], values: {tolerance: 0.01}}\ngap: {status: open}\n"))
	if p.Params["limit"] != "5" || p.Headers["X-Scope-OrgID"] != "1" || p.Status != "open" || p.Mode != "numeric" ||
		!reflect.DeepEqual(p.Fields, []string{"values", "series"}) || tolerance(p) != 0.01 {
		t.Errorf("fields not read: %+v", p)
	}
	for status, want := range map[string]string{"": "match", "fixed": "match", "open": "differ", "documented": "accepted",
		"owner-kept": "accepted", "upstream": "accepted"} {
		if got := (Probe{Status: status}).Expectation(); got != want {
			t.Errorf("%q: expectation %s, want %s", status, got, want)
		}
	}
}

func TestQueryResolvesTimesOnce(t *testing.T) {
	p := Probe{Params: map[string]string{"query": `{app="a"}`, "start": "now-1h", "end": "now", "time": "now+30s", "step": "60s"}}
	q, why := p.Query(testNow)
	if why != "" {
		t.Fatal(why)
	}
	want := map[string]string{"start": fmt.Sprint(testNow.Add(-time.Hour).UnixNano()), "end": fmt.Sprint(testNow.UnixNano()),
		"time": fmt.Sprint(testNow.Add(30 * time.Second).UnixNano()), "step": "60s", "query": `{app="a"}`}
	for k, v := range want {
		if q.Get(k) != v {
			t.Errorf("%s = %q, want %q", k, q.Get(k), v)
		}
	}
	for _, v := range []string{"1800000000", "1793653187704000000", "2025-12-31T23:00:00Z", "1800000000.5", "now-2d", "now-1w", "now-15m", "now-10s"} {
		if _, why := (Probe{Params: map[string]string{"start": v}}).Query(testNow); why != "" {
			t.Errorf("%q: %s", v, why)
		}
	}
	for _, v := range []string{"<start>", "<start+30m>", "historical-aligned-hour-minus-2h", "now-", "now-1y", "now-xh", "now*1h", "nowx"} {
		if _, why := (Probe{Params: map[string]string{"end": v}}).Query(testNow); why == "" {
			t.Errorf("%q: want a placeholder refusal", v)
		}
	}
}

// fake answers from a table: case id -> attempt -> side -> answer.
type fake map[string]map[int]map[string]Answer

func (f fake) Ask(side, caseID string, attempt int, _ Request) Answer {
	if a, ok := f[caseID][attempt][side]; ok {
		return a
	}
	return f[caseID][1][side]
}

func both(l, p Answer) map[int]map[string]Answer {
	return map[int]map[string]Answer{1: {"loki": l, "proxy": p}}
}

func TestVerify(t *testing.T) {
	q := map[string]string{"query": "x", "start": "now-1h", "end": "now"}
	get := func(id, status, mode string, fields ...string) Probe {
		return Probe{ID: id, Status: status, Mode: mode, Fields: fields, Method: "GET", Path: "/loki/api/v1/query_range", Params: q}
	}
	src := fake{
		"same":      both(ok(matrixA), ok(matrixB)),
		"differ":    both(ok(matrixA), ok(matrixLabels)),
		"unstable":  {1: {"loki": ok(matrixA), "proxy": ok(matrixLabels)}, 2: {"loki": ok(matrixA), "proxy": ok(matrixA)}},
		"lokidown":  both(Answer{Err: "connection refused"}, ok(matrixA)),
		"truncated": both(ok(matrixA), Answer{Status: 200, Truncated: true}),
		"loki500":   both(Answer{Status: 500, Body: "boom"}, ok(matrixA)),
		"both500":   both(Answer{Status: 500, Body: "boom"}, Answer{Status: 503, Body: "busy"}),
		"err":       both(Answer{Status: 500, Body: "limit"}, Answer{Status: 500, Body: "limit"}),
		"empty":     both(ok(`{"data":{"result":[]}}`), ok(`{"data":{"result":[]}}`)),
		"recovered": {1: {"loki": ok(matrixA), "proxy": ok(matrixLabels)}, 2: {"loki": Answer{Err: "reset"}, "proxy": ok(matrixA)}},
	}
	cases := []struct {
		p                    Probe
		verdict, flag, why   string
		lokiStatus, differAt int
	}{
		{get("same", "fixed", "exact"), verdictMatches, "", "", 200, 0},
		{get("same", "open", "exact"), verdictMatches, flagProposedClose, "", 200, 0},
		{get("same", "documented", "exact"), verdictMatches, flagBehaviourChanged, "", 200, 0},
		{get("differ", "", "structural"), verdictDiffers, flagRegression, "", 200, 1},
		{get("differ", "open", "structural"), verdictDiffers, "", "", 200, 1},
		{get("unstable", "fixed", "structural"), verdictUnstable, "", "second matched", 200, 0},
		{get("lokidown", "fixed", "exact"), verdictBlocked, "", "Loki did not answer: connection refused", 0, 0},
		{get("truncated", "fixed", "exact"), verdictBlocked, "", "the proxy answered more than", 200, 0},
		{get("loki500", "fixed", "exact"), verdictBlocked, "", "Loki answered 500", 500, 0},
		{get("both500", "fixed", "observed", "values"), verdictBothFail, "", "Loki answered 500 and the proxy 503", 500, 0},
		{get("err", "fixed", "status"), verdictMatches, "", "", 500, 0},
		{get("err", "fixed", "observed", "error_text"), verdictMatches, "", "", 500, 0},
		{get("both500", "fixed", "observed", "status"), verdictDiffers, flagRegression, "", 500, 1},
		{get("empty", "fixed", "numeric"), verdictBlocked, "", "both answers are empty", 200, 0},
		{get("recovered", "fixed", "structural"), verdictBlocked, "", "Loki did not answer: reset", 0, 0},
		{Probe{ID: "scenario", NotExecutable: "a scenario"}, verdictNotExecutable, "", "a scenario", 0, 0},
		{Probe{ID: "pre", Blocked: "needs a precondition"}, verdictBlocked, "", "precondition", 0, 0},
		{Probe{ID: "ph", Method: "GET", Path: "/x", Params: map[string]string{"start": "<start>"}}, verdictBlocked, "", "placeholder", 0, 0},
	}
	for _, c := range cases {
		r := Verify(c.p, src, equalCounts, nil, testNow, "0")
		if r.Verdict != c.verdict || r.Flag != c.flag || !strings.Contains(r.Reason, c.why) || r.LokiStatus != c.lokiStatus ||
			len(r.Differences) != c.differAt {
			t.Errorf("%s (%s %s): got %+v", c.p.ID, c.p.Status, c.p.Mode, r)
		}
	}
}

func TestVerifySendsTheSameRequestToBothSides(t *testing.T) {
	var seen []Request
	src := recordingSource(func(side string, req Request) { seen = append(seen, req) })
	p := Probe{ID: "x", Mode: "exact", Method: "GET", Path: "/p", Params: map[string]string{"start": "now-1h", "end": "now"},
		Headers: map[string]string{"X-Query-Tags": "Source=grafana"}}
	Verify(p, src, equalCounts, nil, testNow, "7")
	if len(seen) != 2 || !reflect.DeepEqual(seen[0], seen[1]) || seen[0].Headers["X-Scope-OrgID"] != "7" ||
		seen[0].Headers["X-Query-Tags"] != "Source=grafana" || !strings.Contains(seen[0].Query, "start=") {
		t.Fatalf("requests: %+v", seen)
	}
	p.Headers = map[string]string{"X-Scope-OrgID": "2"}
	seen = nil
	Verify(p, src, equalCounts, nil, testNow, "7")
	if seen[0].Headers["X-Scope-OrgID"] != "2" {
		t.Errorf("a case's own tenant is kept: %+v", seen[0])
	}
}

type recordingSource func(side string, req Request)

func (r recordingSource) Ask(side, _ string, _ int, req Request) Answer {
	r(side, req)
	return ok(matrixA)
}

func TestSummarizeAndReport(t *testing.T) {
	f := File{Date: "2027-01-15T10:00:00Z", Mode: "live", Commit: "0123456789abcdef0123", Loki: "3.7.7", VictoriaLogs: "v1.53.0",
		Cases: map[string]Result{
			"a": {Verdict: verdictDiffers, Flag: flagRegression, Facets: []string{"status", "values"}, LokiStatus: 200, ProxyStatus: 200,
				Differences: []Difference{{Facet: "values", Loki: "[1]\n`x`", Proxy: strings.Repeat("y", 300)}}},
			"b": {Verdict: verdictMatches, Flag: flagProposedClose},
			"c": {Verdict: verdictMatches, Flag: flagBehaviourChanged},
			"d": {Verdict: verdictBlocked},
		}}
	for i := 0; i < 55; i++ {
		f.Cases[fmt.Sprintf("open/%02d", i)] = Result{Verdict: verdictMatches, Flag: flagProposedClose}
	}
	Summarize(&f)
	if f.Counts[verdictMatches] != 57 || f.Counts[verdictDiffers] != 1 || f.Counts[verdictBlocked] != 1 ||
		!reflect.DeepEqual(f.Flags[flagRegression], []string{"a"}) || f.Flags[flagProposedClose][0] != "b" {
		t.Fatalf("summary: %+v %+v", f.Counts, f.Flags)
	}
	report := Report(f)
	for _, want := range []string{"Commit `0123456789ab`, Loki 3.7.7, VictoriaLogs v1.53.0, proxy build ``.", "The proxy's build is not known", "| matches | 57 |",
		"differ from Loki on equal data: 1", "- `a` (Loki 200, proxy 200, compared: status, values)", "values: Loki `[1] 'x'`",
		"... 6 more in cases.json", "(behaviour changed): 1"} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q:\n%s", want, report)
		}
	}
	if short("abc") != "abc" {
		t.Error("a short commit stays")
	}
}

func TestVersions(t *testing.T) {
	if v := lokiVersion(`{"version":"3.7.7","revision":"x"}`); v != "3.7.7" {
		t.Errorf("loki %q", v)
	}
	if v := lokiVersion("not json"); v != "" {
		t.Errorf("loki %q", v)
	}
	metrics := "# HELP x\nvm_app_version{version=\"victoria-logs-20261101-v1.53.0\", short_version=\"v1.53.0\"} 1\n"
	if v := vlVersion(metrics); v != "v1.53.0" {
		t.Errorf("vl %q", v)
	}
	for _, text := range []string{"vm_app_version{version=\"x\"} 1", "vm_app_version{short_version=\"v1", ""} {
		if v := vlVersion(text); v != "" {
			t.Errorf("vl %q from %q", v, text)
		}
	}
}

func TestReplayFile(t *testing.T) {
	good := `{"case":"a","attempt":1,"request":{"method":"GET","path":"/x","query":""},"loki":{"status":200,"body":"{}"}}` + "\n\n"
	rp, err := LoadReplay(strings.NewReader(good))
	if err != nil {
		t.Fatal(err)
	}
	if a := rp.Ask("loki", "a", 1, Request{}); a.Status != 200 {
		t.Errorf("loki answer: %+v", a)
	}
	for _, missing := range []Answer{rp.Ask("proxy", "a", 1, Request{}), rp.Ask("loki", "a", 2, Request{}), rp.Ask("loki", "b", 1, Request{})} {
		if missing.Err != "no recorded answer" {
			t.Errorf("missing answer: %+v", missing)
		}
	}
	for _, bad := range []string{"{", `{"case":"","attempt":1}`, `{"case":"a","attempt":0}`} {
		if _, err := LoadReplay(strings.NewReader(bad)); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

// writeRegistry writes a small registry with the repository's case schema.
func writeRegistry(t *testing.T, cases map[string]string) string {
	t.Helper()
	root := t.TempDir()
	schema, err := os.ReadFile("../../conformance/registry/schema/case.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{"schema/case.schema.json": string(schema)}
	for id, body := range cases {
		files["cases/"+id+".yaml"] = "id: " + id + "\ntitle: t\nproves: [x]\nconsumer: api\nwhy: w\n" + body
	}
	for name, text := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

var testCases = map[string]string{
	"x/labels-match": "request: {method: GET, path: /loki/api/v1/labels, params: {start: now-1h, end: now}}\ncompare: {mode: exact}\n" +
		"gap: {status: open, impact: edge, effort: S, area: a, loki: l, proxy: p}\n",
	"x/range-regression": "request: {method: POST, path: /loki/api/v1/query_range, params: {query: 'sum(x)', start: now-1h, end: now, step: 60s}}\n" +
		"compare: {mode: numeric}\ngap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n",
	"x/scenario": "request: {scope: cache, entry: e}\ncompare: {mode: observed}\n",
}

// stack serves fake Loki and proxy answers: the labels agree, the proxy's metric answer has another label set.
func stack(t *testing.T) (loki, proxy, vl *httptest.Server, posts *int32) {
	t.Helper()
	posts = new(int32)
	handler := func(metric string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/loki/api/v1/status/buildinfo":
				_, _ = io.WriteString(w, `{"version":"3.7.7"}`)
			case "/loki/api/v1/query":
				// The data precheck's count: five lines for any selector.
				_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"5"]}]}}`)
			case "/loki/api/v1/labels":
				if r.Header.Get("X-Scope-OrgID") != "0" || r.URL.Query().Get("start") == "" {
					http.Error(w, "no tenant or start", http.StatusBadRequest)
					return
				}
				_, _ = io.WriteString(w, `{"status":"success","data":["app","level"]}`)
			case "/loki/api/v1/query_range":
				atomic.AddInt32(posts, 1)
				if r.Method != http.MethodPost || r.FormValue("query") != "sum(x)" {
					http.Error(w, "want a form POST", http.StatusBadRequest)
					return
				}
				_, _ = io.WriteString(w, metric)
			default:
				http.NotFound(w, r)
			}
		}
	}
	loki = httptest.NewServer(handler(matrixA))
	proxy = httptest.NewServer(handler(matrixLabels))
	vl = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			if !strings.HasSuffix(r.URL.Query().Get("query"), "| stats count() as c") || r.URL.Query().Get("start") == "" {
				http.Error(w, "not a count", http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(w, `{"c":"5"}`+"\n")
			return
		}
		_, _ = io.WriteString(w, "vm_app_version{version=\"x\", short_version=\"v1.49.0\"} 1\n")
	}))
	t.Cleanup(func() { loki.Close(); proxy.Close(); vl.Close() })
	return loki, proxy, vl, posts
}

func readFile(t *testing.T, path string) File {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRunLiveThenReplay(t *testing.T) {
	root := writeRegistry(t, testCases)
	loki, proxy, vl, posts := stack(t)
	dir := t.TempDir()
	cfg := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "cases.json"),
		record: filepath.Join(dir, "answers.jsonl"), report: filepath.Join(dir, "report.md"), commit: "abc", tenant: "0",
		proxyFlags: "-query-plan-lowering", settle: minSettle, timeout: 5 * time.Second, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("live run: %d %s", code, errs.String())
	}
	live := readFile(t, cfg.out)
	if live.Mode != "live" || live.Loki != "3.7.7" || live.VictoriaLogs != "v1.49.0" || live.Commit != "abc" ||
		live.ProxyFlags != "-query-plan-lowering" || live.WindowEnd != "2027-01-15T09:55:00Z" || live.Date != "2027-01-15T10:00:00Z" {
		t.Errorf("run facts: %+v", live)
	}
	want := map[string]string{"x/labels-match": verdictMatches, "x/range-regression": verdictDiffers, "x/scenario": verdictNotExecutable}
	for id, v := range want {
		if live.Cases[id].Verdict != v {
			t.Errorf("%s: %+v", id, live.Cases[id])
		}
	}
	if !reflect.DeepEqual(live.Flags[flagRegression], []string{"x/range-regression"}) || !reflect.DeepEqual(live.Flags[flagProposedClose], []string{"x/labels-match"}) {
		t.Errorf("flags: %+v", live.Flags)
	}
	if atomic.LoadInt32(posts) != 4 {
		t.Errorf("the differing case is asked twice on each side, got %d requests", *posts)
	}
	if report, _ := os.ReadFile(cfg.report); !strings.Contains(string(report), "`x/range-regression`") {
		t.Errorf("report:\n%s", report)
	}
	if !strings.Contains(out.String(), "3 cases (live): 1 matches, 1 differs") {
		t.Errorf("stdout: %s", out.String())
	}
	// The recorded answers give the same verdicts with no network.
	loki.Close()
	proxy.Close()
	replay := config{registry: root, replay: cfg.record, out: filepath.Join(dir, "replayed.json"), tenant: "0",
		lokiVersion: "3.7.7", vlVersion: "v1.49.0", settle: minSettle, now: func() time.Time { return testNow }}
	if code := run(replay, &out, &errs); code != 0 {
		t.Fatalf("replay: %d %s", code, errs.String())
	}
	again := readFile(t, replay.out)
	if again.Mode != "recorded" || !reflect.DeepEqual(again.Cases, live.Cases) {
		t.Errorf("replayed verdicts differ:\n%+v\n%+v", again.Cases, live.Cases)
	}
	// -check fails on the regression; -case narrows the run.
	replay.check = true
	errs.Reset()
	if code := run(replay, &out, &errs); code != 1 || !strings.Contains(errs.String(), "regression: x/range-regression") {
		t.Errorf("check: %d %s", code, errs.String())
	}
	replay.cases = "x/labels-match, x/scenario"
	if code := run(replay, &out, &errs); code != 0 || len(readFile(t, replay.out).Cases) != 2 {
		t.Errorf("one case: %d", code)
	}
}

func TestRunRefusesBadInput(t *testing.T) {
	root := writeRegistry(t, testCases)
	bad := writeRegistry(t, map[string]string{"x/broken": "request: {method: GET}\n"})
	dir := t.TempDir()
	replay := filepath.Join(dir, "r.jsonl")
	if err := os.WriteFile(replay, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty.jsonl")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return testNow }
	out := filepath.Join(dir, "c.json")
	cases := map[string]config{
		"no source":          {registry: root, out: out, now: now, settle: minSettle},
		"no registry":        {registry: filepath.Join(dir, "none"), replay: empty, out: out, now: now, settle: minSettle},
		"registry problems":  {registry: bad, replay: empty, out: out, now: now, settle: minSettle},
		"bad replay":         {registry: root, replay: replay, out: out, now: now, settle: minSettle},
		"missing replay":     {registry: root, replay: filepath.Join(dir, "none.jsonl"), out: out, now: now, settle: minSettle},
		"not http":           {registry: root, loki: "ftp://x", proxy: "http://y", out: out, now: now, settle: minSettle},
		"unwritable out":     {registry: root, replay: empty, out: filepath.Join(replay, "c.json"), now: now, settle: minSettle},
		"unwritable report":  {registry: root, replay: empty, out: out, report: filepath.Join(replay, "r.md"), now: now, settle: minSettle},
		"unwritable answers": {registry: root, loki: "http://127.0.0.1:1", proxy: "http://127.0.0.1:1", record: filepath.Join(replay, "a"), out: out, timeout: time.Second, now: now, settle: minSettle},
	}
	for name, cfg := range cases {
		var o, e bytes.Buffer
		if code := run(cfg, &o, &e); code != 2 {
			t.Errorf("%s: exit %d (%s)", name, code, e.String())
		}
	}
	// An unreachable stack blocks every probe and names no versions.
	cfg := config{registry: root, loki: "http://127.0.0.1:1", proxy: "http://127.0.0.1:1", vl: "http://127.0.0.1:1", out: out,
		timeout: time.Second, now: now, settle: minSettle}
	var o, e bytes.Buffer
	if code := run(cfg, &o, &e); code != 0 {
		t.Fatalf("unreachable: %d %s", code, e.String())
	}
	f := readFile(t, out)
	if f.Loki != "unknown" || f.VictoriaLogs != "unknown" || f.Counts[verdictBlocked] != 2 {
		t.Errorf("unreachable stack: %+v", f)
	}
	if v := readVersion(http.DefaultClient, "", lokiVersion); v != "" {
		t.Errorf("no URL, no version: %q", v)
	}
}

func TestLiveSendsAndTruncates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/big" {
			_, _ = w.Write(bytes.Repeat([]byte("x"), maxBody+10))
			return
		}
		if r.URL.Path == "/status" {
			w.WriteHeader(http.StatusTeapot)
		}
	}))
	defer srv.Close()
	l := &Live{Bases: map[string]string{"loki": srv.URL}, Client: srv.Client()}
	if a := l.Ask("loki", "c", 1, Request{Method: "GET", Path: "/big"}); !a.Truncated || len(a.Body) != maxBody {
		t.Errorf("big answer: truncated %v, %d bytes", a.Truncated, len(a.Body))
	}
	if a := l.Ask("loki", "c", 1, Request{Method: "GET", Path: "/status"}); a.Status != http.StatusTeapot {
		t.Errorf("status %d", a.Status)
	}
	if a := l.Ask("loki", "c", 1, Request{Method: "BAD METHOD", Path: "/"}); a.Err == "" {
		t.Error("a request that cannot be built is an error answer")
	}
	if err := baseURL("http://"); err == nil {
		t.Error("a URL without a host is refused")
	}
}

func TestRepositoryCasesAreClassified(t *testing.T) {
	cases, problems, err := registry.LoadAll("../../conformance/registry")
	if err != nil || len(problems) > 0 {
		t.Fatalf("registry: %v %v", err, problems)
	}
	executable := 0
	for _, p := range Probes(cases) {
		if p.NotExecutable != "" || p.Blocked != "" {
			continue
		}
		if _, why := p.Query(testNow); why != "" {
			continue
		}
		executable++
		if facets, _ := p.Facets(); len(facets) < 2 {
			t.Errorf("%s compares only the status: %v", p.ID, facets)
		}
	}
	// Most cases are executable; a drop means a schema change the runner does not read.
	if executable < len(cases)/2 {
		t.Errorf("%d of %d cases are executable", executable, len(cases))
	}
}
