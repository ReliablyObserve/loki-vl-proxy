package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// counts answers a count per selector: {loki, vl}; a selector it does not hold is not counted.
type counts map[string][2]CountAnswer

func (c counts) Count(side string, key CountKey) (CountAnswer, bool) {
	pair, ok := c[key.Selector]
	if !ok {
		pair, ok = c["*"]
	}
	if !ok {
		return CountAnswer{}, false
	}
	if side == "loki" {
		return pair[0], true
	}
	return pair[1], true
}

// equalCounts proves every selector's data equal and non-empty.
var equalCounts = counts{"*": {{Count: 10}, {Count: 10}}}

func TestSelectors(t *testing.T) {
	cases := map[string][]string{
		`{app="a"}`: {`{app="a"}`},
		`sum by (level) (count_over_time({app="a", env=~"p.*"} | json [5m]))`:       {`{app="a", env=~"p.*"}`},
		`sum(rate({a="1"}[1m])) / sum(rate({b="2"}[1m])) or sum(rate({a="1"}[1m]))`: {`{a="1"}`, `{b="2"}`},
		`{a="1"} | line_format "{{.x}}"`:                                            {`{a="1"}`},
		`not logql`:                                                                 nil,
		``:                                                                          nil,
	}
	for q, want := range cases {
		if got := Selectors(q); !reflect.DeepEqual(got, want) {
			t.Errorf("%q: %q, want %q", q, got, want)
		}
	}
}

func TestLookbackAndCountQueries(t *testing.T) {
	for q, want := range map[string]time.Duration{
		`count_over_time({a="1"}[10m])`: 10 * time.Minute, `max_over_time({a="1"} | unwrap x [2h])`: 2 * time.Hour,
		`{a="1"}`: 5 * time.Minute, `rate({a="1"}[1m])`: 5 * time.Minute, `sum(rate({a="1"}[1d:1m]))`: 24 * time.Hour,
	} {
		if got := Lookback(q); got != want {
			t.Errorf("%q: %s, want %s", q, got, want)
		}
	}
	if q := lokiCountQuery(`{a="1"}`, time.Hour); q != `sum(count_over_time({a="1"}[3600s]))` {
		t.Errorf("count query %q", q)
	}
	if q := lokiCountQuery(stackWide, 500*time.Millisecond); q != `sum(count_over_time({service_name=~".+"}[1s]))` {
		t.Errorf("stack-wide count query %q", q)
	}
}

func TestVLFilter(t *testing.T) {
	cases := map[string]string{
		stackWide: "*",
		`{app="a", env!="dev", pod=~"p-.*", ns!~"kube|mon"}`: `app:="a" !env:="dev" pod:~"^(?:p-.*)$" !ns:~"^(?:kube|mon)$"`,
		`{a="q\"uote"}`: `a:="q\"uote"`,
	}
	for sel, want := range cases {
		got, err := vlFilter(sel)
		if err != nil || got != want {
			t.Errorf("%q: %q %v, want %q", sel, got, err, want)
		}
	}
	for _, bad := range []string{"not a selector", `sum(count_over_time({a="1"}[1m]))`} {
		if _, err := vlFilter(bad); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestParseCounts(t *testing.T) {
	cases := []struct {
		raw  string
		want CountAnswer
		vl   bool
	}{
		{`{"data":{"result":[{"value":[1,"42"]}]}}`, CountAnswer{Count: 42}, false},
		{`{"data":{"result":[{"value":[1,"2"]},{"value":[1,"3"]}]}}`, CountAnswer{Count: 5}, false},
		{`{"data":{"result":[]}}`, CountAnswer{Empty: true}, false},
		{`nope`, CountAnswer{Err: "Loki's count is not JSON"}, false},
		{`{"data":{"result":[{"value":[1]}]}}`, CountAnswer{Err: "Loki's count has no value"}, false},
		{`{"data":{"result":[{"value":[1,"x"]}]}}`, CountAnswer{Err: "Loki's count is not a number"}, false},
		{"{\"c\":\"7\"}\n\n{\"c\":\"3\"}\n", CountAnswer{Count: 10}, true},
		{"", CountAnswer{}, true},
		{"{", CountAnswer{Err: "VictoriaLogs' count is not JSON"}, true},
		{`{"c":"x"}`, CountAnswer{Err: "VictoriaLogs' count is not a number"}, true},
	}
	for _, c := range cases {
		got := parseLokiCount([]byte(c.raw))
		if c.vl {
			got = parseVLCount([]byte(c.raw))
		}
		if got != c.want {
			t.Errorf("%q: %+v, want %+v", c.raw, got, c.want)
		}
	}
}

func TestWindow(t *testing.T) {
	now := testNow
	ns := func(t time.Time) int64 { return t.UnixNano() }
	q := url.Values{"start": {"1800000000"}, "end": {"1800000600"}}
	if s, e := window(q, now); s != 1800000000*1e9 || e != 1800000600*1e9 {
		t.Errorf("seconds: %d %d", s, e)
	}
	q = url.Values{"start": {"1800000000000000000"}}
	if s, e := window(q, now); s != 1800000000000000000 || e != ns(now) {
		t.Errorf("start only: %d %d", s, e)
	}
	q = url.Values{"time": {"2027-01-15T09:00:00Z"}, "query": {`count_over_time({a="1"}[10m])`}}
	if s, e := window(q, now); e != ns(now.Add(-time.Hour)) || s != e-int64(10*time.Minute) {
		t.Errorf("instant: %d %d", s, e)
	}
	if s, e := window(url.Values{}, now); s != ns(now.Add(-time.Hour)) || e != ns(now) {
		t.Errorf("no times: %d %d", s, e)
	}
}

func TestStepAnchorsTheWindow(t *testing.T) {
	now := time.Date(2027, 1, 15, 9, 55, 37, 0, time.UTC)
	p := Probe{Params: map[string]string{"start": "now-1h", "end": "now", "step": "60s"}}
	q, _ := p.Query(now)
	if want := time.Date(2027, 1, 15, 9, 55, 0, 0, time.UTC).UnixNano(); q.Get("end") != itoa(want) || q.Get("start") != itoa(want-int64(time.Hour)) {
		t.Errorf("aligned to the minute: %v", q)
	}
	for step, want := range map[string]time.Time{"300": time.Date(2027, 1, 15, 9, 55, 0, 0, time.UTC),
		"15m": time.Date(2027, 1, 15, 9, 45, 0, 0, time.UTC), "1d": time.Date(2027, 1, 15, 0, 0, 0, 0, time.UTC)} {
		if got := (Probe{Params: map[string]string{"step": step}}).anchor(now); !got.Equal(want) {
			t.Errorf("step %s: %s, want %s", step, got, want)
		}
	}
	for _, step := range []string{"", "0", "-5", "soon"} {
		if got := (Probe{Params: map[string]string{"step": step}}).anchor(now); !got.Equal(now) {
			t.Errorf("step %q moved the anchor to %s", step, got)
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestVerifyProvesTheDataFirst(t *testing.T) {
	q := map[string]string{"query": `sum(count_over_time({app="a"}[1m]))`, "start": "now-1h", "end": "now", "step": "60s"}
	get := func(status, mode string, fields ...string) Probe {
		return Probe{ID: "c", Status: status, Mode: mode, Fields: fields, Method: "GET", Path: "/loki/api/v1/query_range", Params: q}
	}
	differ := fake{"c": both(ok(matrixA), ok(matrixLabels))}
	same := fake{"c": both(ok(matrixA), ok(matrixB))}
	sel := `{app="a"}`
	cases := []struct {
		name    string
		src     Source
		cnt     Counter
		p       Probe
		verdict string
		flag    string
		why     string
	}{
		{"equal data, a real difference", differ, counts{sel: {{Count: 9}, {Count: 9}}}, get("fixed", "structural"), verdictDiffers, flagRegression, ""},
		{"unequal data is never a regression", differ, counts{sel: {{Count: 9}, {Count: 8}}}, get("fixed", "structural"), verdictDataDiffers, "",
			"{app=\"a\"}: Loki holds 9 lines, VictoriaLogs 8"},
		{"unequal data is never a match", same, counts{sel: {{Count: 9}, {Count: 8}}}, get("open", "structural"), verdictDataDiffers, "", ""},
		{"no data on either side", differ, counts{sel: {{Count: 0}, {Count: 0}}}, get("fixed", "structural"), verdictEmpty, "", "no line"},
		{"a fresh Loki", differ, counts{sel: {{Empty: true}, {Count: 50}}}, get("fixed", "structural"), verdictLokiNotReady, "", "VictoriaLogs holds 50 lines"},
		{"no count", differ, counts{}, get("fixed", "structural"), verdictDataUnchecked, "", "no count"},
		{"a count that failed", differ, counts{sel: {{Err: "boom"}, {Count: 1}}}, get("fixed", "structural"), verdictBlocked, "", "precheck failed: boom"},
		{"an error case needs no data", fake{"c": both(Answer{Status: 400, Body: "x"}, Answer{Status: 400, Body: "y"})}, counts{},
			get("fixed", "status"), verdictDiffers, flagRegression, ""},
		{"Loki's warnings", fake{"c": both(ok(`{"warnings":["max series"],"data":{"result":[]}}`), ok(matrixA))}, equalCounts,
			get("fixed", "exact"), verdictNotComparable, "", "1 warning"},
		{"a stale proxy answer", fake{"c": both(ok(matrixA), Answer{Status: 200, Body: matrixA, Stale: true})}, equalCounts,
			get("fixed", "exact"), verdictNotComparable, "", "stale"},
	}
	for _, c := range cases {
		r := Verify(c.p, c.src, c.cnt, nil, testNow, "0")
		if r.Verdict != c.verdict || r.Flag != c.flag || !strings.Contains(r.Reason, c.why) {
			t.Errorf("%s: %+v", c.name, r)
		}
	}
	// A numeric tenant of its own is counted on both sides under that tenant, and the key says which; a tenant that
	// is no account number is named, and not counted as the run's.
	other := get("fixed", "structural")
	other.Headers = map[string]string{"X-Scope-OrgID": "7"}
	if r := Verify(other, differ, equalCounts, nil, testNow, "0"); r.Verdict != verdictDiffers || len(r.Data) != 1 || r.Data[0].Tenant != "7" {
		t.Errorf("numeric other tenant: %+v", r)
	}
	if r := Verify(other, differ, counts{sel: {{Count: 0}, {Count: 0}}}, nil, testNow, "0"); r.Verdict != verdictEmpty {
		t.Errorf("a tenant with no data is empty, not a match: %+v", r)
	}
	named := get("fixed", "structural")
	named.Headers = map[string]string{"X-Scope-OrgID": "tenant-a"}
	if r := Verify(named, differ, equalCounts, nil, testNow, "0"); r.Verdict != verdictDataUnchecked || !strings.Contains(r.Reason, `"tenant-a"`) || len(r.Data) != 0 {
		t.Errorf("named tenant: %+v", r)
	}
	same0 := get("fixed", "structural")
	same0.Headers = map[string]string{"X-Scope-OrgID": "0"}
	if r := Verify(same0, differ, equalCounts, nil, testNow, "0"); len(r.Data) != 1 || r.Data[0].Tenant != "" {
		t.Errorf("the run's own tenant keeps the old key: %+v", r.Data)
	}
	r := Verify(get("fixed", "structural"), differ, counts{sel: {{Count: 9}, {Count: 9}}}, nil, testNow, "0")
	if len(r.Data) != 1 || r.Data[0].Selector != sel || r.Data[0].Loki.Count != 9 || r.Data[0].End-r.Data[0].Start != int64(time.Hour) {
		t.Errorf("data checks %+v", r.Data)
	}
	// The label endpoints count every stream.
	labels := Probe{ID: "c", Mode: "exact", Method: "GET", Path: "/loki/api/v1/labels"}
	r = Verify(labels, fake{"c": both(ok(`{"data":["a"]}`), ok(`{"data":["a"]}`))}, counts{stackWide: {{Count: 3}, {Count: 3}}}, nil, testNow, "0")
	if r.Verdict != verdictMatches || r.Data[0].Selector != stackWide {
		t.Errorf("stack-wide: %+v", r)
	}
}

func TestChooseVariant(t *testing.T) {
	variants := []Variant{{Name: "passthrough", URL: "http://p", Flags: []string{"-label-style=passthrough"}},
		{Name: "native", URL: "http://n", Flags: []string{"-metadata-field-mode=native", "-label-style=passthrough"}}}
	cases := []struct {
		flags            []string
		variant, blocked string
	}{
		{nil, "", ""},
		{[]string{"-label-style=underscores"}, "", ""},
		{[]string{"-label-style=passthrough"}, "passthrough", ""},
		{[]string{"-metadata-field-mode=native"}, "native", ""},
		{[]string{"-delete-api-enabled=true"}, "", "needs a proxy variant with -delete-api-enabled=true"},
	}
	for _, c := range cases {
		p := Probe{ProxyFlags: c.flags}
		ChooseVariant(&p, []string{"-listen=:3100", "-label-style=underscores"}, variants)
		if p.Variant != c.variant || p.Blocked != c.blocked {
			t.Errorf("%v: variant %q blocked %q", c.flags, p.Variant, p.Blocked)
		}
	}
	blocked := Probe{ProxyFlags: []string{"-x=1"}, Blocked: "a write"}
	ChooseVariant(&blocked, nil, variants)
	if blocked.Blocked != "a write" {
		t.Error("a blocked probe keeps its reason")
	}
}

func TestLiveSendsAVariantToItsProxy(t *testing.T) {
	var hit string
	srv := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hit = name
			w.Header().Set(staleHeader, "1")
		}))
	}
	def, variant := srv("default"), srv("variant")
	defer def.Close()
	defer variant.Close()
	l := &Live{Bases: map[string]string{"proxy": def.URL, "proxy@v": variant.URL}, Client: http.DefaultClient}
	if a := l.Ask("proxy", "c", 1, Request{Method: "GET", Path: "/", Variant: "v"}); hit != "variant" || !a.Stale {
		t.Errorf("variant: %s %+v", hit, a)
	}
	l.Ask("proxy", "c", 1, Request{Method: "GET", Path: "/"})
	if hit != "default" {
		t.Errorf("default: %s", hit)
	}
}

func TestLiveCounterRecordsAndReplays(t *testing.T) {
	loki, _, vl, _ := stack(t)
	rec := &Recorder{}
	c := &LiveCounter{Loki: loki.URL, VL: vl.URL, Tenant: "0", Client: http.DefaultClient, rec: rec}
	key := CountKey{Selector: `{app="a"}`, Start: 1, End: int64(time.Hour)}
	for i := 0; i < 2; i++ { // the second ask is served from the cache
		if l, ok := c.Count("loki", key); !ok || l.Count != 5 {
			t.Fatalf("loki %+v", l)
		}
		if v, ok := c.Count("vl", key); !ok || v.Count != 5 {
			t.Fatalf("vl %+v", v)
		}
	}
	if len(rec.lines) != 1 {
		t.Fatalf("one count line per key, got %d", len(rec.lines))
	}
	var buf bytes.Buffer
	if err := rec.Write(&buf); err != nil {
		t.Fatal(err)
	}
	rp, err := LoadReplay(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := rp.Count("vl", key); !ok || a.Count != 5 {
		t.Errorf("replayed count %+v %v", a, ok)
	}
	if _, ok := rp.Count("loki", CountKey{Selector: "x"}); ok {
		t.Error("an unrecorded key is not counted")
	}
	// Failures are answers, not panics; a side without a URL is not counted.
	bad := &LiveCounter{Loki: "http://127.0.0.1:1", VL: vl.URL, Client: &http.Client{Timeout: time.Second}}
	if a, _ := bad.Count("loki", key); a.Err == "" {
		t.Error("an unreachable Loki is an error count")
	}
	if a, _ := bad.Count("vl", CountKey{Selector: "not a selector"}); a.Err == "" {
		t.Error("a selector VictoriaLogs cannot be given is an error count")
	}
	if _, ok := (&LiveCounter{}).Count("vl", key); ok {
		t.Error("no VictoriaLogs URL, no count")
	}
	if _, ok := (&LiveCounter{}).Count("loki", key); ok {
		t.Error("no Loki URL, no count")
	}
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", 400) }))
	defer refusing.Close()
	r := &LiveCounter{Loki: refusing.URL, VL: refusing.URL, Client: http.DefaultClient}
	if a, _ := r.Count("loki", key); !strings.Contains(a.Err, "Loki answered 400") {
		t.Errorf("refused loki: %+v", a)
	}
	if a, _ := r.Count("vl", key); !strings.Contains(a.Err, "VictoriaLogs answered 400") {
		t.Errorf("refused vl: %+v", a)
	}
	if a, _ := (&LiveCounter{VL: "http://127.0.0.1:1", Client: &http.Client{Timeout: time.Second}}).Count("vl", key); a.Err == "" {
		t.Error("an unreachable VictoriaLogs is an error count")
	}
	if _, _, err := (&LiveCounter{Client: http.DefaultClient}).get("http://[::1", nil); err == nil {
		t.Error("a bad URL is an error")
	}
}

func TestRunRefusesShortSettleAndBadVariants(t *testing.T) {
	root := writeRegistry(t, testCases)
	dir := t.TempDir()
	empty := filepath.Join(dir, "e.jsonl")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return testNow }
	write := func(name, text string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, cfg := range map[string]config{
		"short settle":     {registry: root, replay: empty, out: filepath.Join(dir, "c.json"), now: now, settle: time.Minute},
		"variants missing": {registry: root, replay: empty, out: filepath.Join(dir, "c.json"), now: now, settle: minSettle, variants: filepath.Join(dir, "none")},
		"variants not json": {registry: root, replay: empty, out: filepath.Join(dir, "c.json"), now: now, settle: minSettle,
			variants: write("bad.json", "{")},
		"variant without url": {registry: root, replay: empty, out: filepath.Join(dir, "c.json"), now: now, settle: minSettle,
			variants: write("nourl.json", `[{"name":"x","url":"ftp://y"}]`)},
	} {
		var o, e bytes.Buffer
		if code := run(cfg, &o, &e); code != 2 {
			t.Errorf("%s: exit %d", name, code)
		}
	}
	// A good variants file reaches the live sources.
	loki, proxy, vl, _ := stack(t)
	cfg := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "c.json"), now: now, settle: minSettle,
		timeout: 5 * time.Second, tenant: "0", variants: write("v.json", `[{"name":"p","url":"`+proxy.URL+`","flags":["-label-style=passthrough"]}]`)}
	var o, e bytes.Buffer
	if code := run(cfg, &o, &e); code != 0 {
		t.Fatalf("with variants: %d %s", code, e.String())
	}
}

// A count is asked of the case's tenant: Loki by header, VictoriaLogs by account number.
func TestLiveCounterAsksTheCasesTenant(t *testing.T) {
	var lokiOrg, vlAccount []string
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lokiOrg = append(lokiOrg, r.Header.Get("X-Scope-OrgID"))
		_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"5"]}]}}`)
	}))
	defer loki.Close()
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vlAccount = append(vlAccount, r.Header.Get("AccountID"))
		_, _ = fmt.Fprint(w, `{"c":"5"}`+"\n")
	}))
	defer vl.Close()
	rec := &Recorder{}
	c := &LiveCounter{Loki: loki.URL, VL: vl.URL, Tenant: "0", Client: http.DefaultClient, rec: rec}
	for _, tenant := range []string{"", "7"} {
		key := CountKey{Selector: `{app="a"}`, Start: 1, End: int64(time.Hour), Tenant: tenant}
		c.Count("loki", key)
		c.Count("vl", key)
	}
	if want := []string{"0", "7"}; !reflect.DeepEqual(lokiOrg, want) {
		t.Errorf("Loki tenants %v, want %v", lokiOrg, want)
	}
	if want := []string{"0", "7"}; !reflect.DeepEqual(vlAccount, want) {
		t.Errorf("VictoriaLogs accounts %v, want %v", vlAccount, want)
	}
	if len(rec.lines) != 2 {
		t.Errorf("one recorded count per tenant, got %d", len(rec.lines))
	}
	var buf bytes.Buffer
	if err := rec.Write(&buf); err != nil {
		t.Fatal(err)
	}
	rp, err := LoadReplay(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := rp.Count("vl", CountKey{Selector: `{app="a"}`, Start: 1, End: int64(time.Hour), Tenant: "7"}); !ok || a.Count != 5 {
		t.Errorf("replayed tenant count %+v %v", a, ok)
	}
	for tenant, want := range map[string]bool{"": false, "0": true, "7": true, "tenant-a": false, "a|b": false, "-1": false} {
		if countableTenant(tenant) != want {
			t.Errorf("countableTenant(%q) = %v", tenant, !want)
		}
	}
}
