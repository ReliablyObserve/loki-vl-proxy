package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() { retryWait, indexWait = 0, 0 }

const sampleMetrics = `# HELP loki_vl_proxy_backend_duration_seconds x
loki_vl_proxy_backend_duration_seconds_count{system="victorialogs",direction="upstream",endpoint="/select/logsql/query",route="query_range"} 3
loki_vl_proxy_backend_duration_seconds_count{system="victorialogs",direction="upstream",endpoint="/select/logsql/stats_query_range",route="a,b"} 2
loki_vl_proxy_backend_read_bytes_total{kind="query"} 100
loki_vl_proxy_backend_read_bytes_total{kind="stats"} 50
loki_vl_proxy_cache_layer_requests_total{layer="loki",component="response",tier="l1_memory"} 4
loki_vl_proxy_cache_layer_hits_total{layer="loki",component="response",tier="l1_memory"} 3
loki_vl_proxy_cache_layer_requests_total{layer="vl",component="window",tier="l2_disk"} 2
process_cpu_seconds_total 1.5
loki_vl_proxy_translations_total 7
loki_vl_proxy_unrelated_total 9
broken{line 1
no_value{a="1"}
bad_value{a="1"} x
`

func TestParseMetrics(t *testing.T) {
	got := ParseMetrics(strings.NewReader(sampleMetrics))
	want := Snapshot{"vl_calls": 5, "vl_calls:/select/logsql/query": 3, "vl_calls:/select/logsql/stats_query_range": 2, "vl_bytes": 150,
		"loki:l1_memory:requests": 4, "loki:l1_memory:hits": 3, "vl:l2_disk:requests": 2, "cpu_seconds": 1.5, "translations": 7}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("snapshot:\n%v\nwant\n%v", got, want)
	}
	if got := splitLabels(`a="x\",y",b="2"`); len(got) != 2 {
		t.Errorf("escaped quote: %q", got)
	}
	d := Delta(Snapshot{"vl_calls": 5, "loki:l1_memory:requests": 4, "loki:l1_memory:hits": 3},
		Snapshot{"vl_calls": 5, "loki:l1_memory:requests": 6, "loki:l1_memory:hits": 5, "vl:l2_disk:requests": 1})
	if !reflect.DeepEqual(d, Snapshot{"loki:l1_memory:requests": 2, "loki:l1_memory:hits": 2, "vl:l2_disk:requests": 1}) {
		t.Errorf("delta %v", d)
	}
	if h := HitRatio(d); h["loki:l1_memory"] != 1 || h["vl:l2_disk"] != 0 || len(h) != 2 {
		t.Errorf("hit ratio %v", h)
	}
}

// meter answers the snapshots of a table: phase -> snapshot.
type meter map[string]Snapshot

func (m meter) Measure(_, _, phase string) (Snapshot, bool) {
	s, ok := m[phase]
	return s, ok
}

func TestMeasureReuse(t *testing.T) {
	p := Probe{ID: "c"}
	req := Request{Method: "GET", Path: "/loki/api/v1/query_range"}
	cold := Snapshot{"vl_calls": 2}
	same := fake{"c": {repeatAttempt: {"proxy": ok(matrixB)}}}
	other := fake{"c": {repeatAttempt: {"proxy": ok(matrixLabels)}}}
	failing := fake{"c": {repeatAttempt: {"proxy": Answer{Err: "reset"}}}}
	hit := meter{"repeat_before": {"vl_calls": 2, "loki:l1_memory:requests": 1}, "repeat_after": {"vl_calls": 2, "loki:l1_memory:requests": 2, "loki:l1_memory:hits": 1}}
	cold = Snapshot{"vl_calls": 2, "vl_calls:/q": 2}
	call := meter{"repeat_before": {"vl_calls": 2, "vl_calls:/q": 2}, "repeat_after": {"vl_calls": 3, "vl_calls:/q": 3}}
	background := meter{"repeat_before": {"vl_calls": 2, "vl_calls:/q": 2}, "repeat_after": {"vl_calls": 5, "vl_calls:/q": 2, "vl_calls:/health": 1, "vl_calls:/names": 2}}
	cases := []struct {
		name    string
		src     Source
		m       Meter
		cost    Snapshot
		verdict string
		why     string
	}{
		{"served from the cache", same, hit, cold, reuseReused, ""},
		{"another answer", other, hit, cold, reuseNotReused, "answers differently (labels)"},
		{"VictoriaLogs asked again", same, call, cold, reuseNotReused, "called VictoriaLogs 1 time(s) on the endpoints of the first ask"},
		{"background calls on other endpoints", same, background, cold, reuseReused, ""},
		{"no meter", same, nil, cold, reuseNotMeasured, "could not be read"},
		{"no cold cost", same, hit, nil, reuseNotMeasured, "could not be read"},
		{"a failed repeat", failing, hit, cold, reuseNotMeasured, "the repeat could not be asked: reset"},
		{"metrics gone", same, meter{}, cold, reuseNotMeasured, "metrics could not be read"},
	}
	for _, c := range cases {
		r := measureReuse(p, req, ok(matrixA), c.cost, c.src, c.m, []string{facetStatus, facetLabels}, Options{})
		if r.Verdict != c.verdict || !strings.Contains(r.Reason, c.why) {
			t.Errorf("%s: %+v", c.name, r)
		}
		if c.name == "background calls on other endpoints" && r.Background != 3 {
			t.Errorf("background calls: %+v", r)
		}
	}
	s := SummarizeReuse(map[string]Result{
		"a": {Reuse: &Reuse{Verdict: reuseReused, Cold: Snapshot{"vl_calls": 2, "vl_bytes": 10}, Repeat: Snapshot{"loki:l1_memory:requests": 1, "loki:l1_memory:hits": 1}}},
		"b": {Reuse: &Reuse{Verdict: reuseNotReused, Cold: Snapshot{"vl_calls": 1}, Repeat: Snapshot{"vl_calls": 1, "vl_bytes": 4, "loki:l1_memory:requests": 1}}},
		"c": {},
	})
	if s.Counts[reuseReused] != 1 || s.Counts[reuseNotReused] != 1 || !reflect.DeepEqual(s.NotReused, []string{"b"}) || s.ColdVLCalls != 3 ||
		s.RepeatCalls != 1 || s.ColdBytes != 10 || s.RepeatBytes != 4 || s.RepeatHits["loki:l1_memory"] != 0.5 {
		t.Errorf("summary %+v", s)
	}
}

// cachingProxy answers like the fake stack's proxy, keeps a response cache unless the query names "nocache",
// and exposes its counters on /metrics.
func cachingProxy(t *testing.T, inner http.Handler) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	seen := map[string]bool{}
	calls, hits, lookups := 0, 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/metrics" {
			fmt.Fprintf(w, "loki_vl_proxy_backend_duration_seconds_count{endpoint=%q} %d\n", "/select/logsql/query", calls)
			fmt.Fprintf(w, "loki_vl_proxy_cache_layer_requests_total{layer=\"loki\",tier=\"l1_memory\"} %d\n", lookups)
			fmt.Fprintf(w, "loki_vl_proxy_cache_layer_hits_total{layer=\"loki\",tier=\"l1_memory\"} %d\n", hits)
			return
		}
		key := r.URL.String()
		lookups++
		// The proxy's own count of this request (-debug-vl-calls-header): a hit starts no call.
		if seen[key] && !strings.Contains(r.URL.RawQuery, "nocache") {
			hits++
			w.Header().Set(backendCallsHeader, "0")
		} else {
			calls++
			seen[key] = true
			w.Header().Set(backendCallsHeader, "1")
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestReuseLiveAndReplay(t *testing.T) {
	root := writeRegistry(t, map[string]string{"x/same": rangeCase("same"), "x/nocache": strings.Replace(rangeCase("same"), `[1m]`, ` |= "nocache" [1m]`, 1)})
	loki, inner, vl := precheckStack(t)
	proxy := cachingProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := http.Get(inner.URL + r.URL.String())
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(w, resp.Body)
	}))
	dir := t.TempDir()
	live := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "live.json"), report: filepath.Join(dir, "r.md"),
		record: filepath.Join(dir, "a.jsonl"), tenant: "0", settle: minSettle, timeout: 5 * time.Second, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(live, &out, &errs); code != 0 {
		t.Fatalf("live: %d %s", code, errs.String())
	}
	lf := readFile(t, live.out)
	if r := lf.Cases["x/same"].Reuse; r == nil || r.Verdict != reuseReused || r.Source != sourceHeader || r.Cold["vl_calls"] != 1 || r.Repeat["loki:l1_memory:hits"] != 1 {
		t.Errorf("same: %+v", r)
	}
	if r := lf.Cases["x/nocache"].Reuse; r == nil || r.Verdict != reuseNotReused || !strings.Contains(r.Reason, "called VictoriaLogs 1 time(s)") {
		t.Errorf("nocache: %+v", r)
	}
	if lf.Cases["x/nocache"].Flag == flagRegression && lf.Cases["x/nocache"].Verdict != verdictDiffers {
		t.Error("a cache that is not reused is not a regression")
	}
	if !reflect.DeepEqual(lf.Reuse.NotReused, []string{"x/nocache"}) || lf.Reuse.RepeatHits["loki:l1_memory"] != 0.5 {
		t.Errorf("reuse summary %+v", lf.Reuse)
	}
	if report, _ := os.ReadFile(live.report); !strings.Contains(string(report), "Cache reuse of a repeat: 1 reused, 1 not reused") {
		t.Errorf("report:\n%s", report)
	}
	replay := config{registry: root, replay: live.record, out: filepath.Join(dir, "replay.json"), tenant: "0", settle: minSettle,
		lokiVersion: lf.Loki, vlVersion: lf.VictoriaLogs, now: func() time.Time { return testNow.Add(time.Hour) }}
	if code := run(replay, &out, &errs); code != 0 {
		t.Fatalf("replay: %d %s", code, errs.String())
	}
	if rf := readFile(t, replay.out); !reflect.DeepEqual(rf.Cases, lf.Cases) || !reflect.DeepEqual(rf.Reuse, lf.Reuse) {
		t.Errorf("replay differs:\n%+v\n%+v", rf.Reuse, lf.Reuse)
	}
	// -no-reuse measures nothing.
	live.noReuse, live.record = true, ""
	if code := run(live, &out, &errs); code != 0 {
		t.Fatal(errs.String())
	}
	if r := readFile(t, live.out).Cases["x/same"].Reuse; r != nil {
		t.Errorf("no reuse: %+v", r)
	}
}

func TestLiveMeter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v/metrics" {
			http.Error(w, "no", http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, sampleMetrics)
	}))
	defer srv.Close()
	rec := &Recorder{}
	m := &LiveMeter{Bases: map[string]string{"proxy": srv.URL, "proxy@v": srv.URL + "/v"}, Client: http.DefaultClient, rec: rec}
	if s, ok := m.Measure("c", "", "cold_before"); !ok || s["vl_calls"] != 5 {
		t.Errorf("default: %v %v", s, ok)
	}
	if _, ok := m.Measure("c", "v", "cold_before"); ok {
		t.Error("a refused scrape is not a snapshot")
	}
	if _, ok := m.Measure("c", "none", "x"); ok {
		t.Error("an unknown variant is not scraped")
	}
	if _, ok := (&LiveMeter{Bases: map[string]string{"proxy": "http://127.0.0.1:1"}, Client: &http.Client{Timeout: time.Second}}).Measure("c", "", "x"); ok {
		t.Error("an unreachable proxy is not a snapshot")
	}
	var buf bytes.Buffer
	if err := rec.Write(&buf); err != nil {
		t.Fatal(err)
	}
	rp, err := LoadReplay(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := rp.Measure("c", "", "cold_before"); !ok || s["vl_bytes"] != 150 {
		t.Errorf("replayed snapshot %v %v", s, ok)
	}
}

func TestPartialProxyAnswerIsNotCompared(t *testing.T) {
	src := fake{"c": both(ok(matrixA), Answer{Status: 200, Body: matrixA, Partial: true})}
	p := Probe{ID: "c", Status: "fixed", Mode: "exact", Method: "GET", Path: "/loki/api/v1/query_range", Params: map[string]string{"start": "now-1h", "end": "now"}}
	if r := Verify(p, src, equalCounts, nil, testNow, "0"); r.Verdict != verdictNotComparable || !strings.Contains(r.Reason, "partial") {
		t.Errorf("partial: %+v", r)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Header().Set(partialHeader, "true") }))
	defer srv.Close()
	if a := (&Live{Bases: map[string]string{"proxy": srv.URL}, Client: http.DefaultClient}).Ask("proxy", "c", 1, Request{Method: "GET", Path: "/"}); !a.Partial {
		t.Error("the partial header is read")
	}
}

// The repeat's VictoriaLogs calls are the request's own count when the answer carries the proxy's header: the proxy's
// background calls around it (a health probe, the stream field inventory's refresh) are left out even on an endpoint
// the first ask used, which the process-wide metrics cannot tell apart.
func TestMeasureReuseCountsTheRequestsOwnCalls(t *testing.T) {
	p := Probe{ID: "c"}
	req := Request{Method: "GET", Path: "/loki/api/v1/labels"}
	n := func(v int) *int { return &v }
	cold := Snapshot{"vl_calls": 2, "vl_calls:/names": 2}
	// The metrics see 3 calls on an endpoint the first ask used (/names): two background refreshes and a health probe.
	noisy := meter{"repeat_before": {"vl_calls": 2, "vl_calls:/names": 2}, "repeat_after": {"vl_calls": 5, "vl_calls:/names": 4, "vl_calls:/health": 1}}
	withHeader := func(calls *int) fake {
		a := ok(matrixB)
		a.VLCalls = calls
		return fake{"c": {repeatAttempt: {"proxy": a}}}
	}
	first := ok(matrixA)
	first.VLCalls = n(2)
	facets := []string{facetStatus, facetLabels}

	r := measureReuse(p, req, first, cold, withHeader(n(0)), noisy, facets, Options{})
	if r.Verdict != reuseReused || r.Source != sourceHeader || r.Repeat["vl_calls"] != 0 || r.Background != 3 || r.Cold["vl_calls"] != 2 {
		t.Errorf("header 0: %+v", r)
	}
	r = measureReuse(p, req, first, cold, withHeader(n(1)), noisy, facets, Options{})
	if r.Verdict != reuseNotReused || r.Repeat["vl_calls"] != 1 || r.Background != 2 || !strings.Contains(r.Reason, "called VictoriaLogs 1 time(s)") {
		t.Errorf("header 1: %+v", r)
	}
	// No header: the metrics decide, and the calls on endpoints the first ask used are the request's.
	r = measureReuse(p, req, ok(matrixA), cold, withHeader(nil), noisy, facets, Options{})
	if r.Source != sourceMetrics || r.Verdict != reuseNotReused || r.Repeat["vl_calls"] != 2 || r.Background != 1 {
		t.Errorf("no header: %+v", r)
	}
	// An absent header is not 0 calls: a repeat answered without one is not counted as clean.
	if r.Repeat["vl_calls"] == 0 {
		t.Error("an absent header was read as 0 calls")
	}

	s := SummarizeReuse(map[string]Result{
		"a": {Reuse: &Reuse{Verdict: reuseReused, Source: sourceHeader, Cold: Snapshot{"vl_calls": 2}, Repeat: Snapshot{}, Background: 3}},
		"b": {Reuse: &Reuse{Verdict: reuseNotReused, Source: sourceMetrics, Cold: Snapshot{"vl_calls": 1}, Repeat: Snapshot{"vl_calls": 2}, Background: 1}},
	})
	if s.CallsFromHeader != 1 || s.CallsFromMetrics != 1 || s.RepeatCalls != 2 || s.Background != 4 {
		t.Errorf("summary %+v", s)
	}
}

func TestVLCallsOfReadsTheProxyHeaders(t *testing.T) {
	for _, c := range []struct {
		calls, result string
		want          int
	}{{"0", "hit", 0}, {"3", "miss", 3}, {"", "", -1}, {"-1", "", -1}, {"x", "bypass", -1}} {
		h := http.Header{}
		if c.calls != "" {
			h.Set(backendCallsHeader, c.calls)
		}
		if c.result != "" {
			h.Set(cacheResultHeader, c.result)
		}
		got, result := vlCallsOf(h)
		switch {
		case c.want < 0 && got != nil, c.want >= 0 && (got == nil || *got != c.want), result != c.result:
			t.Errorf("%q %q: calls %v, result %q", c.calls, c.result, got, result)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(backendCallsHeader, "4")
		w.Header().Set(cacheResultHeader, "miss")
	}))
	defer srv.Close()
	a := (&Live{Bases: map[string]string{"proxy": srv.URL}, Client: http.DefaultClient}).Ask("proxy", "c", 1, Request{Method: "GET", Path: "/"})
	if a.VLCalls == nil || *a.VLCalls != 4 || a.CacheResult != "miss" {
		t.Errorf("live answer %+v", a)
	}
}

// flakyMeter fails its first scrapes of every phase, then answers like the table.
type flakyMeter struct {
	meter
	fail  int
	tries map[string]int
}

func (m *flakyMeter) Measure(c, v, phase string) (Snapshot, bool) {
	m.tries[phase]++
	if m.tries[phase] <= m.fail {
		return nil, false
	}
	return m.meter.Measure(c, v, phase)
}

// failingOnce answers the repeat with an error once, then like the wrapped source.
type failingOnce struct {
	Source
	failed bool
}

func (f *failingOnce) Ask(side, id string, attempt int, r Request) Answer {
	if attempt == repeatAttempt && !f.failed {
		f.failed = true
		return Answer{Err: "connection reset"}
	}
	return f.Source.Ask(side, id, attempt, r)
}

// A repeat is judged by the request's own count and answer: a scrape of /metrics that fails once or twice is tried again,
// and a scrape that stays down leaves the verdict standing without the metric delta.
func TestMeasureReuseSurvivesScrapeFailures(t *testing.T) {
	p := Probe{ID: "c"}
	req := Request{Method: "GET", Path: "/loki/api/v1/query_range"}
	zero, one := 0, 1
	withCount := func(a Answer, n *int) Answer { a.VLCalls = n; return a }
	hit := meter{"repeat_before": {"vl_calls": 2}, "repeat_after": {"vl_calls": 2, "loki:l1_memory:requests": 1, "loki:l1_memory:hits": 1}}
	src := func(n *int) fake { return fake{"c": {repeatAttempt: {"proxy": withCount(ok(matrixA), n)}}} }
	first := withCount(ok(matrixA), &one)

	// Two failed scrapes per phase, the third answers: measured, with the metric delta.
	flaky := &flakyMeter{meter: hit, fail: 2, tries: map[string]int{}}
	r := measureReuse(p, req, first, Snapshot{"vl_calls": 1}, src(&zero), flaky, []string{facetStatus}, Options{})
	if r.Verdict != reuseReused || r.Source != sourceHeader || r.Repeat["loki:l1_memory:hits"] != 1 || flaky.tries["repeat_before"] != 3 {
		t.Errorf("a scrape that answers on its third try: %+v (tries %v)", r, flaky.tries)
	}

	// Scrapes that stay down: the header's count and the answer still give the verdict.
	gone := &flakyMeter{meter: hit, fail: 99, tries: map[string]int{}}
	r = measureReuse(p, req, first, nil, src(&zero), gone, []string{facetStatus}, Options{})
	if r.Verdict != reuseReused || r.Source != sourceHeader || r.Repeat != nil && len(r.Repeat) != 0 || r.Cold["vl_calls"] != 1 {
		t.Errorf("metrics gone, header present: %+v", r)
	}
	r = measureReuse(p, req, first, nil, src(&one), gone, []string{facetStatus}, Options{})
	if r.Verdict != reuseNotReused || !strings.Contains(r.Reason, "called VictoriaLogs 1 time(s)") || r.Repeat["vl_calls"] != 1 {
		t.Errorf("a repeat that called VictoriaLogs is a finding even with the metrics gone: %+v", r)
	}

	// Without the header the metrics are the only count: unreadable, the repeat is not measured, and says why.
	r = measureReuse(p, req, ok(matrixA), Snapshot{"vl_calls": 1}, src(nil), gone, []string{facetStatus}, Options{})
	if r.Verdict != reuseNotMeasured || !strings.Contains(r.Reason, "no X-Loki-VL-Backend-Calls") {
		t.Errorf("no header, no metrics: %+v", r)
	}

	// A repeat that fails once is asked again.
	once := &failingOnce{Source: src(&zero)}
	r = measureReuse(p, req, first, Snapshot{"vl_calls": 1}, once, hit, []string{facetStatus}, Options{})
	if r.Verdict != reuseReused || !once.failed {
		t.Errorf("a repeat that failed once: %+v", r)
	}
}
