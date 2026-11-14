package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const emptyStreams = `{"status":"success","data":{"resultType":"streams","result":[]}}`

// asks records every request and answers from the table keyed by case id (the control is "<id>#control").
type asks struct {
	src  fake
	reqs map[string][]Request
}

func (a *asks) Ask(side, id string, attempt int, req Request) Answer {
	if a.reqs == nil {
		a.reqs = map[string][]Request{}
	}
	a.reqs[id] = append(a.reqs[id], req)
	return a.src.Ask(side, id, attempt, req)
}

// Two empty answers prove a case only when its control returns data on both sides.
func TestControlProvesTheDataBehindTwoEmptyAnswers(t *testing.T) {
	probe := func(status string, control map[string]string) Probe {
		return Probe{ID: "c", Status: status, Mode: "exact", Fields: []string{"streams.values"}, Method: "GET", Path: "/loki/api/v1/query_range",
			Params:  map[string]string{"query": `{app="x"} | user="u1"`, "start": "now-5m", "end": "now", "limit": "100"},
			Control: control}
	}
	control := map[string]string{"query": `{app="x"} | json | user="u1"`}
	empty := both(ok(emptyStreams), ok(emptyStreams))
	cases := []struct {
		name    string
		control map[string]string
		answer  map[int]map[string]Answer
		state   string
		verdict string
		flag    string
		reason  string
	}{
		{"data on both sides", control, both(ok(streamsA), ok(streamsA)), "fixed", verdictMatches, "", "control query returns data on both sides"},
		{"an open case that now matches", control, both(ok(streamsA), ok(streamsA)), "open", verdictMatches, flagProposedClose, "the data is there"},
		{"an owner-kept case that now matches", control, both(ok(streamsA), ok(streamsA)), "owner-kept", verdictMatches, flagBehaviourChanged, ""},
		{"the control is not compared: other lines", control, both(ok(streamsA), ok(strings.Replace(streamsA, "line one", "line two", 1))), "fixed", verdictMatches, "", ""},
		{"Loki's control is empty", control, both(ok(emptyStreams), ok(streamsA)), "fixed", verdictBlocked, "", "Loki answers nothing to it"},
		{"the proxy's control is empty", control, both(ok(streamsA), ok(emptyStreams)), "fixed", verdictBlocked, "", "the proxy answers nothing to it"},
		{"the control fails on the proxy", control, both(ok(streamsA), Answer{Status: 500, Body: "boom"}), "fixed", verdictBlocked, "", "the proxy answered 500"},
		{"the control is not answered", control, both(Answer{Err: "refused"}, ok(streamsA)), "fixed", verdictBlocked, "", "Loki did not answer"},
		{"a recording without the control", control, both(Answer{Err: "no recorded answer"}, Answer{Err: "no recorded answer"}), "fixed", verdictBlocked, "", "Loki did not answer: no recorded answer"},
		{"no control: nothing is proven", nil, nil, "fixed", verdictBlocked, "", "both answers are empty: nothing is proven"},
	}
	for _, c := range cases {
		src := fake{"c": empty}
		if c.answer != nil {
			src["c#control"] = c.answer
		}
		r := Verify(probe(c.state, c.control), src, equalCounts, nil, testNow, "0")
		if r.Verdict != c.verdict || r.Flag != c.flag || !strings.Contains(r.Reason, c.reason) || (c.verdict == verdictBlocked) != (r.Class == classEmptyAnswer) {
			t.Errorf("%s: %+v", c.name, r)
		}
	}
}

// The control is the case's request with its params laid over, asked once of each side with the same headers; a case whose
// own answers hold data does not ask it, and one that differs is a difference whatever its control says.
func TestControlRequestIsTheCasesWithItsParamsLaidOver(t *testing.T) {
	p := Probe{ID: "c", Status: "fixed", Mode: "exact", Fields: []string{"streams.values"}, Method: "GET", Path: "/loki/api/v1/query_range",
		Params:  map[string]string{"query": `{app="x"} | user="u1"`, "start": "now-5m", "end": "now", "limit": "100"},
		Headers: map[string]string{"X-Scope-OrgID": "7"}, Control: map[string]string{"query": `{app="x"} | json | user="u1"`, "limit": "5"}}
	a := &asks{src: fake{"c": both(ok(emptyStreams), ok(emptyStreams)), "c#control": both(ok(streamsA), ok(streamsA))}}
	if r := Verify(p, a, counts{`{app="x"}`: {{Count: 3}, {Count: 3}}}, nil, testNow, "7"); r.Verdict != verdictMatches {
		t.Fatalf("%+v", r)
	}
	if len(a.reqs["c#control"]) != 2 {
		t.Fatalf("the control is asked once per side: %d asks", len(a.reqs["c#control"]))
	}
	main, ctl := a.reqs["c"][0], a.reqs["c#control"][0]
	mq, _ := url.ParseQuery(main.Query)
	cq, _ := url.ParseQuery(ctl.Query)
	if cq.Get("query") != `{app="x"} | json | user="u1"` || cq.Get("limit") != "5" || cq.Get("start") != mq.Get("start") || cq.Get("end") != mq.Get("end") ||
		ctl.Path != main.Path || !reflect.DeepEqual(ctl.Headers, main.Headers) {
		t.Errorf("control %+v, case %+v", ctl, main)
	}
	// Answers with data: the control is not asked.
	b := &asks{src: fake{"c": both(ok(streamsA), ok(streamsA))}}
	if r := Verify(p, b, equalCounts, nil, testNow, "7"); r.Verdict != verdictMatches || len(b.reqs["c#control"]) != 0 {
		t.Errorf("data: %+v, control asked %d times", r, len(b.reqs["c#control"]))
	}
	// A real difference stays one.
	d := &asks{src: fake{"c": both(ok(streamsA), ok(emptyStreams)), "c#control": both(ok(streamsA), ok(streamsA))}}
	if r := Verify(p, d, equalCounts, nil, testNow, "7"); r.Verdict != verdictDiffers || len(d.reqs["c#control"]) != 0 {
		t.Errorf("difference: %+v", r)
	}
}

func TestProbeReadsTheControl(t *testing.T) {
	const req = "request: {method: GET, path: /loki/api/v1/query_range, params: {query: 'q', limit: '100'}, verify: {control: %s}}\ncompare: {mode: exact}\n"
	p := probeOf(caseOf(t, "a/b", strings.Replace(req, "%s", "{params: {query: 'q2', limit: 5}}", 1)))
	if p.Blocked != "" || !reflect.DeepEqual(p.Control, map[string]string{"query": "q2", "limit": "5"}) {
		t.Errorf("control %v, blocked %q", p.Control, p.Blocked)
	}
	for _, bad := range []string{"{params: {}}", "{}"} {
		p := probeOf(caseOf(t, "a/b", strings.Replace(req, "%s", bad, 1)))
		if p.Blocked == "" || p.BlockedClass != classUnclassed || !strings.Contains(p.Blocked, "control needs params") {
			t.Errorf("%s: %q %q", bad, p.Blocked, p.BlockedClass)
		}
	}
	// The control's params leave the probe's own untouched.
	own := Probe{Params: map[string]string{"a": "1"}}
	if got := own.withParams(map[string]string{"a": "2", "b": "3"}).Params; !reflect.DeepEqual(got, map[string]string{"a": "2", "b": "3"}) || own.Params["a"] != "1" {
		t.Errorf("params %v, own %v", got, own.Params)
	}
}

// The registry's schema takes the control, and only with params.
func TestSchemaTakesTheControl(t *testing.T) {
	for body, ok := range map[string]bool{
		"{params: {query: q}}": true, "{query: q}": false, "{params: {query: 1}}": false, "{params: {query: q}, extra: 1}": false,
	} {
		root := writeRegistry(t, map[string]string{"x/c": "request: {method: GET, path: /loki/api/v1/query_range, verify: {control: " + body + "}}\ncompare: {mode: exact}\n"})
		var out, errs bytes.Buffer
		code := run(config{registry: root, replay: filepath.Join(t.TempDir(), "none.jsonl"), out: filepath.Join(t.TempDir(), "o.json"), settle: minSettle, now: time.Now}, &out, &errs)
		loaded := !strings.Contains(errs.String(), "does not load strictly")
		if loaded != ok {
			t.Errorf("%s: loads %v (exit %d: %s)", body, loaded, code, errs.String())
		}
	}
}

// A live run and its replay agree on a control: Loki and the proxy answer nothing to the case's query and data to
// the control's.
func TestReplayKeepsTheControlVerdict(t *testing.T) {
	side := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/loki/api/v1/status/buildinfo":
				_, _ = io.WriteString(w, `{"version":"3.7.9"}`)
			case "/loki/api/v1/query":
				_, _ = io.WriteString(w, `{"data":{"resultType":"vector","result":[{"metric":{},"value":[1,"4"]}]}}`)
			default:
				if strings.Contains(r.URL.Query().Get("query"), "| json") {
					_, _ = io.WriteString(w, streamsA)
					return
				}
				_, _ = io.WriteString(w, emptyStreams)
			}
		}))
	}
	loki, proxy := side(), side()
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			_, _ = io.WriteString(w, "vm_app_version{short_version=\"v1.53.0\"} 1\n")
			return
		}
		_, _ = io.WriteString(w, `{"c":"4"}`+"\n")
	}))
	defer loki.Close()
	defer proxy.Close()
	defer vl.Close()
	root := writeRegistry(t, map[string]string{"x/controlled": "request: {method: GET, path: /loki/api/v1/query_range, params: {query: '{app=\"a\"} | user=\"u1\"', start: now-1h, end: now, limit: '100'}, " +
		"verify: {control: {params: {query: '{app=\"a\"} | json | user=\"u1\"'}}}}\ncompare: {mode: exact, fields: [streams.values]}\n" +
		"gap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n"})
	dir := t.TempDir()
	live := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "live.json"), record: filepath.Join(dir, "a.jsonl"),
		tenant: "0", settle: minSettle, timeout: 5 * time.Second, noReuse: true, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(live, &out, &errs); code != 0 {
		t.Fatalf("live: %d %s", code, errs.String())
	}
	lf := readFile(t, live.out)
	if r := lf.Cases["x/controlled"]; r.Verdict != verdictMatches || !strings.Contains(r.Reason, "control") {
		t.Fatalf("live: %+v", r)
	}
	replay := config{registry: root, replay: live.record, out: filepath.Join(dir, "replay.json"), tenant: "0", settle: minSettle,
		lokiVersion: lf.Loki, vlVersion: lf.VictoriaLogs, now: func() time.Time { return testNow.Add(5 * time.Hour) }}
	if code := run(replay, &out, &errs); code != 0 {
		t.Fatalf("replay: %d %s", code, errs.String())
	}
	rf := readFile(t, replay.out)
	if r := rf.Cases["x/controlled"]; r.Verdict != verdictMatches || r.Reason != lf.Cases["x/controlled"].Reason {
		t.Errorf("replay: %+v", r)
	}
}
