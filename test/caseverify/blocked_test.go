package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

func caseOf(t *testing.T, id, body string) registry.Case {
	t.Helper()
	d, err := registry.Parse([]byte("id: " + id + "\nconsumer: api\n" + body))
	if err != nil {
		t.Fatal(err)
	}
	return registry.Case{ID: id, Doc: d}
}

// What a case says about its precondition decides whether the runner sends it, and a block names its class.
func TestVerifyBlockDecidesWhetherAndWhyACaseIsBlocked(t *testing.T) {
	const req = "request: {method: GET, path: /loki/api/v1/labels, precondition: busy%s}\ncompare: {mode: status}\n"
	cases := []struct {
		name, verify string
		blocked      bool
		class        string
		reasonHas    string
	}{
		{"a bare precondition", "", true, classUnclassed, "precondition"},
		{"met by the stack with its evidence", ", verify: {met_by: stack, evidence: 'generator.py:1'}", false, "", ""},
		{"met by the stack without evidence", ", verify: {met_by: stack}", true, classUnclassed, "evidence"},
		{"met by and blocked by", ", verify: {met_by: stack, evidence: e, blocked_by: load}", true, classUnclassed, "not valid"},
		{"blocked by an unknown class", ", verify: {blocked_by: lunch}", true, classUnclassed, "not valid"},
		{"blocked by load with its reason", ", verify: {blocked_by: load, reason: 'slots held'}", true, classLoad, "slots held"},
		{"blocked by a class without a reason", ", verify: {blocked_by: fault}", true, classFault, "precondition"},
	}
	for _, c := range cases {
		p := probeOf(caseOf(t, "a/b", strings.Replace(req, "%s", c.verify, 1)))
		if (p.Blocked != "") != c.blocked || p.BlockedClass != c.class || !strings.Contains(p.Blocked, c.reasonHas) {
			t.Errorf("%s: blocked %q class %q", c.name, p.Blocked, p.BlockedClass)
		}
	}
	// A setup step is never met by the stack: it is blocked by what it injects.
	p := probeOf(caseOf(t, "a/b", "request: {method: GET, path: /x, setup: kill, verify: {met_by: stack, evidence: e}}\ncompare: {mode: status}\n"))
	if p.Blocked == "" || !strings.Contains(p.Blocked, "setup") {
		t.Errorf("setup: %q", p.Blocked)
	}
	// Blocked by a class without any precondition (a request that only reads an answer the stack cannot give).
	p = probeOf(caseOf(t, "a/b", "request: {method: GET, path: /x, verify: {blocked_by: volume, reason: 'a 240000-pod window'}}\ncompare: {mode: status}\n"))
	if p.Blocked != "a 240000-pod window" || p.BlockedClass != classVolume {
		t.Errorf("blocked_by alone: %q %q", p.Blocked, p.BlockedClass)
	}
	// Paths the runner never sends carry their own class; a double slash is sent.
	for path, class := range map[string]string{"/loki/api/v1/push": classWrite, "/loki/api/v1/tail": classWebsocket,
		"/api/ds/query": classGrafana, "/loki/api/v1/delete": classProxyOnly, "/loki//api/v1/labels": ""} {
		p := probeOf(caseOf(t, "a/b", "request: {method: GET, path: "+path+"}\ncompare: {mode: status}\n"))
		if p.BlockedClass != class || (class == "") != (p.Blocked == "") {
			t.Errorf("%s: %q %q", path, p.Blocked, p.BlockedClass)
		}
	}
}

// A verdict of a blocked case carries its class, whether it was blocked before or after sending.
func TestBlockedVerdictsCarryTheirClass(t *testing.T) {
	q := map[string]string{"query": "x", "start": "now-1h", "end": "now"}
	get := func(id string) Probe {
		return Probe{ID: id, Status: "fixed", Mode: "numeric", Method: "GET", Path: "/loki/api/v1/query_range", Params: q}
	}
	src := fake{
		"empty":   both(ok(`{"data":{"result":[]}}`), ok(`{"data":{"result":[]}}`)),
		"down":    both(Answer{Err: "refused"}, ok(`{}`)),
		"loki500": both(Answer{Status: 500, Body: "x"}, ok(`{}`)),
	}
	ph := Probe{ID: "ph", Method: "GET", Path: "/x", Params: map[string]string{"start": "<a row time in ns>"}}
	pre := Probe{ID: "pre", Blocked: "needs", BlockedClass: classLoad}
	for _, c := range []struct {
		p    Probe
		want string
	}{{get("empty"), classEmptyAnswer}, {get("down"), classNoAnswer}, {get("loki500"), classLokiError}, {ph, classPlaceholder}, {pre, classLoad}} {
		if r := Verify(c.p, src, equalCounts, nil, testNow, "0"); r.Verdict != verdictBlocked || r.Class != c.want {
			t.Errorf("%s: %+v", c.p.ID, r)
		}
	}
	// Fields the runner does not read are said apart from an empty answer.
	p := get("empty")
	p.Fields = []string{"headers"}
	if r := Verify(p, src, equalCounts, nil, testNow, "0"); r.Class != classUnreadFields || !strings.Contains(r.Reason, "headers") {
		t.Errorf("unread fields: %+v", r)
	}
}

// An empty answer proves a case only when the case says so, and never an open one.
func TestEmptyIsAnAnswerOnlyWhereTheCaseSaysSo(t *testing.T) {
	q := map[string]string{"query": "x", "start": "now-1h", "end": "now"}
	src := fake{"e": both(ok(`{"data":{"result":[]}}`), ok(`{"data":{"result":[]}}`))}
	probe := func(status, by string) Probe {
		return Probe{ID: "e", Status: status, Mode: "numeric", Method: "GET", Path: "/loki/api/v1/query_range", Params: q, EmptyIsAnswer: by}
	}
	if r := Verify(probe("fixed", "answered like Loki"), src, equalCounts, nil, testNow, "0"); r.Verdict != verdictMatches ||
		!strings.Contains(r.Reason, "empty by design: answered like Loki") || r.Class != "" || r.Flag != "" {
		t.Errorf("fixed: %+v", r)
	}
	if r := Verify(probe("", "answered like Loki"), src, equalCounts, nil, testNow, "0"); r.Verdict != verdictMatches {
		t.Errorf("no gap: %+v", r)
	}
	if r := Verify(probe("open", "answered like Loki"), src, equalCounts, nil, testNow, "0"); r.Verdict != verdictBlocked || r.Class != classEmptyAnswer {
		t.Errorf("open: %+v", r)
	}
	if r := Verify(probe("fixed", ""), src, equalCounts, nil, testNow, "0"); r.Verdict != verdictBlocked {
		t.Errorf("not declared: %+v", r)
	}
	// An error answer is not an empty one.
	src = fake{"e": both(ok(`{"data":{"result":[]}}`), Answer{Status: 400, Body: "bad"})}
	if r := Verify(probe("fixed", "answered like Loki"), src, equalCounts, nil, testNow, "0"); r.Verdict != verdictDiffers {
		t.Errorf("an error: %+v", r)
	}
}

// Loki's index/stats counts the chunks it flushed: an answer that lags behind its own count says nothing.
func TestIndexStatsOfALokiBehindItsDataIsNotComparable(t *testing.T) {
	p := Probe{ID: "i", Status: "fixed", Mode: "observed", Fields: []string{"streams", "entries"}, Method: "GET", Path: indexStatsPath,
		Params: map[string]string{"query": `{env="production"}`, "start": "now-1h", "end": "now"}}
	lag := fake{"i": both(ok(`{"streams":2,"chunks":2,"bytes":1,"entries":4}`), ok(`{"streams":28166,"chunks":28166,"bytes":2,"entries":10}`))}
	r := Verify(p, lag, equalCounts, nil, testNow, "0")
	if r.Verdict != verdictNotComparable || !strings.Contains(r.Reason, "below its own count_over_time") || len(r.Differences) != 0 {
		t.Errorf("lag: %+v", r)
	}
	// Caught up, the two compare on the numbers named (streams, entries), and a difference is the proxy's.
	caught := fake{"i": both(ok(`{"streams":70,"chunks":70,"bytes":1,"entries":10}`), ok(`{"streams":71,"chunks":70,"bytes":9,"entries":10}`))}
	r = Verify(p, caught, equalCounts, nil, testNow, "0")
	if r.Verdict != verdictDiffers || len(r.Differences) != 1 || r.Differences[0].Facet != "stat:streams" || r.Flag != flagRegression {
		t.Errorf("caught up: %+v", r)
	}
	// A flushed Loki counts every chunk overlapping the window: entries above its own count are compared, not excused.
	above := fake{"i": both(ok(`{"streams":70,"chunks":70,"bytes":1,"entries":11}`), ok(`{"streams":70,"chunks":70,"bytes":1,"entries":10}`))}
	r = Verify(p, above, equalCounts, nil, testNow, "0")
	if r.Verdict != verdictDiffers || len(r.Differences) != 1 || r.Differences[0].Facet != "stat:entries" {
		t.Errorf("entries above the count: %+v", r)
	}
	same := fake{"i": both(ok(`{"streams":70,"entries":10}`), ok(`{"streams":70,"entries":10,"bytes":3}`))}
	if r = Verify(p, same, equalCounts, nil, testNow, "0"); r.Verdict != verdictMatches {
		t.Errorf("equal numbers: %+v", r)
	}
}

// Loki's index catches up with its data a little after the seed: the answer is asked again, and a case whose index
// caught up on a later ask is compared like any other; one that never does stays not comparable.
func TestIndexStatsIsAskedAgainWhileLokiCatchesUp(t *testing.T) {
	p := Probe{ID: "i", Status: "documented", Mode: "observed", Fields: []string{"streams", "entries"}, Method: "GET", Path: indexStatsPath,
		Params: map[string]string{"query": `{env="production"}`, "start": "now-1h", "end": "now"}}
	behind := ok(`{"streams":2,"chunks":2,"bytes":1,"entries":4}`)
	proxyAnswer := ok(`{"streams":70,"chunks":70,"bytes":9,"entries":10}`)
	caught := ok(`{"streams":70,"chunks":70,"bytes":1,"entries":10}`)
	src := fake{"i": {1: {"loki": behind, "proxy": proxyAnswer}, indexRetryAttempt + 3: {"loki": caught}}}
	if r := Verify(p, src, equalCounts, nil, testNow, "0"); r.Verdict != verdictMatches || r.Reason != "" {
		t.Errorf("caught up on the third ask: %+v", r)
	}
	never := fake{"i": {1: {"loki": behind, "proxy": proxyAnswer}}}
	if r := Verify(p, never, equalCounts, nil, testNow, "0"); r.Verdict != verdictNotComparable || !strings.Contains(r.Reason, "has not caught up") {
		t.Errorf("never caught up: %+v", r)
	}
}

// A case whose point is the answer to a range without data (request.verify.empty_range) is not `empty`: the empty counts
// are the data it needs, and the answers' status is compared. Without the declaration the same case is empty.
func TestEmptyRangeIsTheDataOfACaseThatSaysSo(t *testing.T) {
	q := map[string]string{"query": `{app="x"} | line_format "{{.a"`, "start": "now-30d", "end": "now-29d"}
	probe := func(by string) Probe {
		return Probe{ID: "e", Status: "documented", Mode: "observed", Fields: []string{"status"}, Method: "GET", Path: "/loki/api/v1/query_range", Params: q, EmptyRange: by}
	}
	none := counts{`{app="x"}`: {{Count: 0}, {Count: 0}}}
	src := fake{"e": both(ok(`{"data":{"result":[]}}`), Answer{Status: 400, Body: "bad"})}
	if r := Verify(probe("the range holds no data on purpose"), src, none, nil, testNow, "0"); r.Verdict != verdictDiffers || r.Expected != "accepted" || r.Flag != "" {
		t.Errorf("a documented difference over an empty range: %+v", r)
	}
	if r := Verify(probe(""), src, none, nil, testNow, "0"); r.Verdict != verdictEmpty {
		t.Errorf("not declared: %+v", r)
	}
	same := fake{"e": both(ok(`{"data":{"result":[]}}`), ok(`{"data":{"result":[]}}`))}
	if r := Verify(probe("the range holds no data on purpose"), same, none, nil, testNow, "0"); r.Verdict != verdictMatches || r.Flag != flagBehaviourChanged {
		t.Errorf("the proxy now answers like Loki: %+v", r)
	}
}

// A redirect is an answer: the client does not follow it, and a path with a double slash is sent as it is.
func TestRedirectsAreCompared(t *testing.T) {
	mux := http.NewServeMux() // cleans //, answers 301 with a Location
	mux.HandleFunc("/loki/api/v1/labels", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("labels")) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	dir := t.TempDir()
	root := writeRegistry(t, map[string]string{"x/slash": "request: {method: GET, path: /loki//api/v1/labels}\ncompare: {mode: exact, fields: [status, body]}\n"})
	cfg := config{registry: root, loki: srv.URL, proxy: srv.URL, out: filepath.Join(dir, "cases.json"), commit: "abc", tenant: "0",
		noReuse: true, settle: minSettle, timeout: 5 * time.Second, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("run: %d %s", code, errs.String())
	}
	r := readFile(t, cfg.out).Cases["x/slash"]
	if r.LokiStatus/100 != 3 || r.ProxyStatus/100 != 3 || r.Verdict == verdictBlocked {
		t.Errorf("a redirect is the answer on both sides: %+v", r)
	}
}

const composeFixture = `
services:
  victorialogs:
    container_name: e2e-victorialogs
    ports: ["19428:9428"]
  loki-vl-proxy:
    container_name: e2e-proxy
    ports:
      - "13100:3100"
    command:
      - "-listen=:3100"
      - "-label-style=underscores"
      - '-tenant-default-limits={"a":1}'
  loki-vl-proxy-passthrough:
    container_name: e2e-proxy-passthrough
    ports:
      - "127.0.0.1:13102:3100"
    command: ["-listen=:3100", "-label-style=passthrough"]
  log-generator:
    command: python /scripts/log-generator.py
    ports: ["9:9"]
  nameless:
    ports: ["13109:3100"]
    command: ["-listen=:3100"]
`

func TestVariantsFromCompose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(path, []byte(composeFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	variants, def, err := VariantsFromCompose(path, "127.0.0.1", "http://127.0.0.1:13100")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(def, []string{"-listen=:3100", "-label-style=underscores", `-tenant-default-limits={"a":1}`}) {
		t.Errorf("default flags: %v", def)
	}
	want := []Variant{{Name: "nameless", URL: "http://127.0.0.1:13109", Flags: []string{"-listen=:3100"}},
		{Name: "passthrough", URL: "http://127.0.0.1:13102", Flags: []string{"-listen=:3100", "-label-style=passthrough"}}}
	sort.Slice(variants, func(i, j int) bool { return variants[i].Name < variants[j].Name })
	if !reflect.DeepEqual(variants, want) {
		t.Errorf("variants: %+v", variants)
	}
	if _, _, err := VariantsFromCompose(filepath.Join(t.TempDir(), "none.yml"), "h", ""); err == nil {
		t.Error("a missing file is an error")
	}
	bad := filepath.Join(t.TempDir(), "bad.yml")
	_ = os.WriteFile(bad, []byte("services: [1"), 0o644)
	if _, _, err := VariantsFromCompose(bad, "h", ""); err == nil {
		t.Error("a bad file is an error")
	}
}

// The repository's compose file gives the proxy the cases route to: the default proxy's ports and flags, and a
// variant for every other proxy service.
func TestComposeOfTheE2EStack(t *testing.T) {
	variants, def, err := VariantsFromCompose("../e2e-compat/docker-compose.yml", "127.0.0.1", "http://127.0.0.1:13100")
	if err != nil {
		t.Fatal(err)
	}
	has := func(flags []string, f string) bool {
		for _, x := range flags {
			if x == f {
				return true
			}
		}
		return false
	}
	if !has(def, "-max-stats-query-series=1000000") || !has(def, "-label-style=underscores") {
		t.Errorf("the main proxy's flags: %v", def)
	}
	byName := map[string]Variant{}
	for _, v := range variants {
		byName[v.Name] = v
	}
	if v, ok := byName["underscore"]; !ok || v.URL != "http://127.0.0.1:13102" || !has(v.Flags, "-label-style=underscores") {
		t.Errorf("the underscore variant: %+v (of %d)", v, len(variants))
	}
	if _, ok := byName["patterns-autodetect"]; !ok {
		t.Errorf("variants: %v", byName)
	}
	for _, v := range variants {
		if v.URL == "http://127.0.0.1:13100" {
			t.Errorf("the default proxy is not a variant: %+v", v)
		}
	}
}

// A case that names proxy flags goes to the variant that has them (or the default proxy, if it does); -list says
// where each case goes and what the blocked ones need.
func TestClassifyRoutesAndCounts(t *testing.T) {
	cases := []registry.Case{
		caseOf(t, "a/plain", "request: {method: GET, path: /loki/api/v1/labels}\ncompare: {mode: status}\n"),
		caseOf(t, "a/variant", "request: {method: GET, path: /loki/api/v1/labels, proxy_flags: ['-x=1']}\ncompare: {mode: status}\n"),
		caseOf(t, "a/default", "request: {method: GET, path: /loki/api/v1/labels, proxy_flags: ['-d=1']}\ncompare: {mode: status}\n"),
		caseOf(t, "a/none", "request: {method: GET, path: /loki/api/v1/labels, proxy_flags: ['-nowhere']}\ncompare: {mode: status}\n"),
		caseOf(t, "a/load", "request: {method: GET, path: /loki/api/v1/labels, verify: {blocked_by: load}}\ncompare: {mode: status}\n"),
		caseOf(t, "a/ph", "request: {method: GET, path: /loki/api/v1/labels, params: {start: '<a row time in ns>'}}\ncompare: {mode: status}\n"),
		caseOf(t, "a/scn", "request: {scope: cache, entry: e}\ncompare: {mode: observed}\n"),
	}
	c := Classify(cases, []string{"-d=1"}, []Variant{{Name: "v", URL: "http://x", Flags: []string{"-x=1"}}}, func() time.Time { return testNow })
	if !reflect.DeepEqual(c.Sent, []string{"a/default", "a/plain"}) || !reflect.DeepEqual(c.SentToVariant["v"], []string{"a/variant"}) ||
		!reflect.DeepEqual(c.Blocked[classLoad], []string{"a/load"}) || !reflect.DeepEqual(c.Blocked[classPlaceholder], []string{"a/ph"}) ||
		!reflect.DeepEqual(c.Blocked[classNoVariant], []string{"a/none"}) || !reflect.DeepEqual(c.NotExecutable, []string{"a/scn"}) {
		t.Errorf("classification: %+v", c)
	}
	var out bytes.Buffer
	if code := list(cases, []string{"-d=1"}, nil, &out); code != 0 || !strings.Contains(out.String(), "blocked before sending: 4") ||
		!strings.Contains(out.String(), "  load: 1") || !strings.Contains(out.String(), "not executable (no request): 1") {
		t.Errorf("list:\n%s", out.String())
	}
	out.Reset()
	list(cases, nil, []Variant{{Name: "v", URL: "http://x", Flags: []string{"-x=1"}}}, &out)
	if !strings.Contains(out.String(), "sent to variant v: 1") {
		t.Errorf("list with a variant:\n%s", out.String())
	}
}

// Every case the runner blocks before sending, in the repository, names what it needs; the counts are logged
// so a change of the registry shows in the test output.
func TestRepositoryBlockedCasesAreClassified(t *testing.T) {
	cases, problems, err := registry.LoadAll("../../conformance/registry")
	if err != nil || len(problems) > 0 {
		t.Fatalf("registry: %v %v", err, problems)
	}
	variants, def, err := VariantsFromCompose("../e2e-compat/docker-compose.yml", "127.0.0.1", "http://127.0.0.1:13100")
	if err != nil {
		t.Fatal(err)
	}
	c := Classify(cases, def, variants, func() time.Time { return testNow })
	if n := len(c.Blocked[classUnclassed]); n > 0 {
		t.Errorf("%d blocked cases name no class: %v", n, c.Blocked[classUnclassed])
	}
	if n := len(c.Blocked[classNoVariant]); n > 0 {
		t.Errorf("cases need a proxy no compose service runs: %v", c.Blocked[classNoVariant])
	}
	for class, ids := range c.Blocked {
		t.Logf("blocked by %s: %d", class, len(ids))
	}
	t.Logf("sent to the default proxy: %d; to variants: %d", len(c.Sent), len(c.SentToVariant))
}
