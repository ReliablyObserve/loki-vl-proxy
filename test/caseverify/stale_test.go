package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// streams builds a log answer of one stream with the given "ts:line" entries, and the cache stats of Loki's answer.
func streamsAnswer(found int, entries ...string) Answer {
	var vals []string
	for _, e := range entries {
		ts, line, _ := strings.Cut(e, ":")
		vals = append(vals, `["`+ts+`","`+line+`"]`)
	}
	stats := `{"cache":{"result":{"entriesFound":` + itoa(int64(found)) + `}}}`
	return ok(`{"status":"success","data":{"resultType":"streams","stats":` + stats + `,"result":[{"stream":{"app":"a"},"values":[` +
		strings.Join(vals, ",") + `]}]}}`)
}

func TestLokiCacheHit(t *testing.T) {
	for _, c := range []struct {
		name string
		a    Answer
		want bool
	}{
		{"an extent reused", streamsAnswer(1, "1:a"), true},
		{"no extent", streamsAnswer(0, "1:a"), false},
		{"no stats", ok(matrixA), false},
		{"not JSON", ok("text"), false},
		{"an error answer", Answer{Status: 400, Body: `{"data":{"stats":{"cache":{"result":{"entriesFound":3}}}}}`}, false},
	} {
		if got := lokiCacheHit(c.a); got != c.want {
			t.Errorf("%s: %v", c.name, got)
		}
	}
}

// The differences of an answer from a stale cache: Loki holds a strict subset of the proxy's entries. Any other
// kind of difference, or no reused extent, stays a difference.
func TestLokiCachedStaleNeedsAllTheEvidence(t *testing.T) {
	full := streamsAnswer(0, "1:a", "2:b", "3:c", "4:d")
	diff := func(l, p Answer, facets ...string) []Difference {
		if len(facets) == 0 {
			facets = []string{facetEntries}
		}
		return CompareWith(facets, l, p, Options{})
	}
	cases := []struct {
		name         string
		loki, proxy  Answer
		facets       []string
		stale        bool
		reasonHasAll []string
	}{
		{"half of the lines, an extent reused", streamsAnswer(1, "1:a", "2:b"), full, nil, true, []string{"entriesFound", "2 of the proxy's 4"}},
		{"none of the lines, an extent reused", streamsAnswer(2), full, nil, true, []string{"0 of the proxy's 4"}},
		{"the older lines missing", streamsAnswer(1, "3:c", "4:d"), full, nil, true, []string{"between 1 and 2"}},
		{"a block missing in the middle", streamsAnswer(1, "1:a", "4:d"), full, nil, true, []string{"between 2 and 3"}},
		{"the proxy's extra lines interleaved with Loki's", streamsAnswer(1, "1:a", "3:c"), full, nil, false, nil},
		{"two blocks of missing lines around Loki's", streamsAnswer(1, "2:b", "4:d"), streamsAnswer(0, "1:a", "2:b", "3:c", "4:d", "5:e"), nil, false, nil},
		{"half of the lines, no extent reused", streamsAnswer(0, "1:a", "2:b"), full, nil, false, nil},
		{"a line only Loki holds", streamsAnswer(1, "1:a", "9:z"), full, nil, false, nil},
		{"a line of another text", streamsAnswer(1, "1:a", "2:changed"), full, nil, false, nil},
		{"the proxy holds fewer", full, streamsAnswer(0, "1:a"), nil, false, nil},
		{"a series only the proxy holds", streamsAnswer(1, "1:a", "2:b"), ok(strings.Replace(full.Body, `"result":[`, `"result":[{"stream":{"app":"extra"},"values":[["1","a"]]},`, 1)), nil, false, nil},
		{"no series at all on Loki's side", ok(`{"data":{"resultType":"streams","stats":{"cache":{"result":{"entriesFound":1}}},"result":[]}}`), full, nil, false, nil},
		{"a series only Loki holds", ok(`{"data":{"stats":{"cache":{"result":{"entriesFound":1}}},"result":[{"stream":{"app":"zz"},"values":[["1","a"]]}]}}`), full, nil, false, nil},
		{"data and result type differ", streamsAnswer(1, "1:a"), ok(strings.Replace(full.Body, "streams", "matrix", 1)), []string{facetData}, false, nil},
	}
	for _, c := range cases {
		d := diff(c.loki, c.proxy, c.facets...)
		why := lokiCachedStale(d, c.loki, c.proxy)
		if (why != "") != c.stale {
			t.Errorf("%s: stale %q (differences %v)", c.name, why, d)
		}
		for _, w := range c.reasonHasAll {
			if !strings.Contains(why, w) {
				t.Errorf("%s: reason %q lacks %q", c.name, why, w)
			}
		}
	}
	// Equal answers are not stale, and a difference of the status is never explained by a cache.
	if why := lokiCachedStale(nil, streamsAnswer(1, "1:a"), streamsAnswer(1, "1:a")); why != "" {
		t.Errorf("no difference: %q", why)
	}
	if why := lokiCachedStale([]Difference{{Facet: facetStatus}, {Facet: facetEntries}}, streamsAnswer(1, "1:a"), full); why != "" {
		t.Errorf("a status difference: %q", why)
	}
}

// Samples of a metric answer are judged the same way: a missing point is stale, another value is a difference.
func TestLokiCachedStaleOnSamples(t *testing.T) {
	withStats := func(values string) Answer {
		return ok(`{"data":{"resultType":"matrix","stats":{"cache":{"result":{"entriesFound":1}}},"result":[{"metric":{"a":"1"},"values":[` + values + `]}]}}`)
	}
	proxy := ok(`{"data":{"resultType":"matrix","result":[{"metric":{"a":"1"},"values":[[10,"1"],[20,"2"],[30,"3"]]}]}}`)
	for _, c := range []struct {
		name   string
		values string
		stale  bool
	}{
		{"a missing point", `[10,"1"],[30,"3"]`, true},
		{"every point missing", ``, true},
		{"another value at a point", `[10,"1"],[20,"9"]`, false},
		{"a point only Loki holds", `[10,"1"],[25,"2"]`, false},
	} {
		l := withStats(c.values)
		d := CompareWith([]string{facetValues}, l, proxy, Options{})
		if why := lokiCachedStale(d, l, proxy); (why != "") != c.stale {
			t.Errorf("%s: %q", c.name, why)
		}
	}
	// An instant vector's single value is a sample too.
	l := ok(`{"data":{"resultType":"vector","stats":{"cache":{"result":{"entriesFound":1}}},"result":[{"metric":{"a":"1"},"value":[10,"1"]}]}}`)
	p := ok(`{"data":{"resultType":"vector","result":[{"metric":{"a":"1"},"value":[10,"1"]},{"metric":{"a":"2"},"value":[10,"1"]}]}}`)
	if why := lokiCachedStale(CompareWith([]string{facetData}, l, p, Options{}), l, p); why != "" {
		t.Errorf("a series only the proxy holds is a difference of series: %q", why)
	}
	l2 := ok(`{"data":{"resultType":"vector","stats":{"cache":{"result":{"entriesFound":1}}},"result":[{"metric":{"a":"1"},"value":[10,"1"]}]}}`)
	p2 := ok(`{"data":{"resultType":"vector","result":[{"metric":{"a":"1"},"value":[10,"1"]}]}}`)
	if why := lokiCachedStale(CompareWith([]string{facetData}, l2, p2, Options{}), l2, p2); why != "" {
		t.Errorf("equal vectors: %q", why)
	}
}

// Verify gives the verdict without asking twice, and keeps a real difference and an unreused cache as they are.
func TestVerifyClassifiesAStaleLokiCache(t *testing.T) {
	q := map[string]string{"query": `{app="a"}`, "start": "now-1h", "end": "now", "limit": "100"}
	get := func(status string) Probe {
		return Probe{ID: "c", Status: status, Mode: "exact", Fields: []string{"entries"}, Method: "GET", Path: "/loki/api/v1/query_range", Params: q}
	}
	full := streamsAnswer(0, "1:a", "2:b", "3:c", "4:d")
	sel := `{app="a"}`
	cnt := counts{sel: {{Count: 4}, {Count: 4}}}
	var asked int
	src := countingSource{fake{"c": both(streamsAnswer(1, "1:a", "2:b"), full)}, &asked}
	r := Verify(get("fixed"), src, cnt, nil, testNow, "0")
	if r.Verdict != verdictCachedStale || r.Flag != "" || len(r.Differences) != 0 || !strings.Contains(r.Reason, "entriesFound") {
		t.Errorf("stale: %+v", r)
	}
	if asked != 2 {
		t.Errorf("asked %d times (one ask per side), the verdict needs no second attempt", asked)
	}
	r = Verify(get("fixed"), fake{"c": both(streamsAnswer(0, "1:a", "2:b"), full)}, cnt, nil, testNow, "0")
	if r.Verdict != verdictDiffers || r.Flag != flagRegression {
		t.Errorf("no extent reused: %+v", r)
	}
	// An open case Loki serves stale is not a proposed close, and not a regression either.
	r = Verify(get("open"), src, cnt, nil, testNow, "0")
	if r.Verdict != verdictCachedStale || r.Flag != "" {
		t.Errorf("open: %+v", r)
	}
	// The count the precheck reads still decides first: unequal data is data_differs.
	r = Verify(get("fixed"), src, counts{sel: {{Count: 4}, {Count: 3}}}, nil, testNow, "0")
	if r.Verdict != verdictDataDiffers {
		t.Errorf("unequal data: %+v", r)
	}
	f := File{Cases: map[string]Result{"c": {Verdict: verdictCachedStale}}}
	Summarize(&f)
	if f.Counts[verdictCachedStale] != 1 || !strings.Contains(Report(f), "| loki_cached_stale | 1 |") {
		t.Errorf("counts %v", f.Counts)
	}
}

type countingSource struct {
	Source
	n *int
}

func (c countingSource) Ask(side, id string, attempt int, req Request) Answer {
	*c.n++
	return c.Source.Ask(side, id, attempt, req)
}

// A live run against a Loki that serves a stale extent and its recording give the same verdict, the recording
// replayed at another time: the reused-extent evidence is in the recorded answer.
func TestReplayKeepsTheStaleCacheVerdict(t *testing.T) {
	full := streamsAnswer(0, "1:a", "2:b", "3:c", "4:d").Body
	stale := streamsAnswer(1, "1:a", "2:b").Body
	logs := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/loki/api/v1/status/buildinfo":
				_, _ = io.WriteString(w, `{"version":"3.7.9"}`)
			case "/loki/api/v1/query":
				_, _ = io.WriteString(w, `{"data":{"resultType":"vector","result":[{"metric":{},"value":[1,"4"]}]}}`)
			default:
				_, _ = io.WriteString(w, body)
			}
		}))
	}
	loki, proxy := logs(stale), logs(full)
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
	root := writeRegistry(t, map[string]string{"x/stale": "request: {method: GET, path: /loki/api/v1/query_range, params: {query: '{app=\"a\"}', start: now-1h, end: now, limit: '100'}}\n" +
		"compare: {mode: exact, fields: [entries]}\ngap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n"})
	dir := t.TempDir()
	live := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "live.json"), record: filepath.Join(dir, "a.jsonl"),
		tenant: "0", settle: minSettle, timeout: 5 * time.Second, noReuse: true, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(live, &out, &errs); code != 0 {
		t.Fatalf("live: %d %s", code, errs.String())
	}
	lf := readFile(t, live.out)
	if r := lf.Cases["x/stale"]; r.Verdict != verdictCachedStale || r.Flag != "" || len(lf.Flags[flagRegression]) != 0 {
		t.Fatalf("live: %+v", lf.Cases)
	}
	replay := config{registry: root, replay: live.record, out: filepath.Join(dir, "replay.json"), tenant: "0", settle: minSettle,
		lokiVersion: lf.Loki, vlVersion: lf.VictoriaLogs, noReuse: true, now: func() time.Time { return testNow.Add(5 * time.Hour) }}
	if code := run(replay, &out, &errs); code != 0 {
		t.Fatalf("replay: %d %s", code, errs.String())
	}
	rf := readFile(t, replay.out)
	// The replay reads the (absent) recorded metrics of the repeat; the live run took none.
	for _, f := range []*File{&rf, &lf} {
		r := f.Cases["x/stale"]
		r.Reuse = nil
		f.Cases["x/stale"] = r
	}
	if !reflect.DeepEqual(rf.Cases, lf.Cases) || rf.Counts[verdictCachedStale] != 1 {
		t.Errorf("replay %+v, live %+v", rf.Cases, lf.Cases)
	}
}
