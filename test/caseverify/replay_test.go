package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A stack whose data differs by service: the counts the precheck reads decide each case's verdict.
//
//	svc="same"     equal counts, equal answers      -> matches
//	svc="real"     equal counts, other label sets   -> differs (a regression of a fixed case)
//	svc="drift"    Loki 9 lines, VictoriaLogs 8     -> data_differs
//	svc="none"     no line on either side           -> empty
//	svc="fresh"    Loki's metric path answers none  -> loki_not_ready
func precheckStack(t *testing.T) (loki, proxy, vl *httptest.Server) {
	t.Helper()
	service := func(q string) string {
		for _, s := range []string{"same", "real", "drift", "none", "fresh"} {
			if strings.Contains(q, `"`+s+`"`) {
				return s
			}
		}
		return ""
	}
	lokiCount := map[string]string{"same": "5", "real": "5", "drift": "9", "none": "0"}
	vlCount := map[string]string{"same": "5", "real": "5", "drift": "8", "none": "0", "fresh": "7"}
	answers := func(isProxy bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/loki/api/v1/status/buildinfo":
				_, _ = io.WriteString(w, `{"version":"3.7.9"}`)
			case "/loki/api/v1/query":
				n, ok := lokiCount[service(r.URL.Query().Get("query"))]
				if !ok {
					_, _ = io.WriteString(w, `{"data":{"resultType":"vector","result":[]}}`)
					return
				}
				_, _ = io.WriteString(w, `{"data":{"resultType":"vector","result":[{"metric":{},"value":[1,"`+n+`"]}]}}`)
			case "/loki/api/v1/query_range":
				if isProxy && service(r.URL.Query().Get("query")) == "real" {
					_, _ = io.WriteString(w, matrixLabels)
					return
				}
				_, _ = io.WriteString(w, matrixA)
			default:
				http.NotFound(w, r)
			}
		}
	}
	loki, proxy = httptest.NewServer(answers(false)), httptest.NewServer(answers(true))
	vl = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			_, _ = io.WriteString(w, "vm_app_version{short_version=\"v1.53.0\"} 1\n")
			return
		}
		_, _ = io.WriteString(w, `{"c":"`+vlCount[service(r.URL.Query().Get("query"))]+`"}`+"\n")
	}))
	t.Cleanup(func() { loki.Close(); proxy.Close(); vl.Close() })
	return loki, proxy, vl
}

func rangeCase(svc string) string {
	return "request: {method: GET, path: /loki/api/v1/query_range, params: {query: 'sum by (app) (count_over_time({svc=\"" + svc +
		"\"}[1m]))', start: now-1h, end: now, step: 60s}}\ncompare: {mode: structural}\n" +
		"gap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n"
}

// Live and replay give the same verdict for every kind, with the replay run at another time: the recording
// holds the settled end and every count.
func TestReplayEqualsLiveForEveryVerdict(t *testing.T) {
	cases := map[string]string{"x/same": rangeCase("same"), "x/real": rangeCase("real"), "x/drift": rangeCase("drift"),
		"x/none": rangeCase("none"), "x/fresh": rangeCase("fresh"), "x/scenario": "request: {scope: s, entry: e}\ncompare: {mode: observed}\n"}
	root := writeRegistry(t, cases)
	loki, proxy, vl := precheckStack(t)
	dir := t.TempDir()
	live := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "live.json"),
		record: filepath.Join(dir, "a.jsonl"), tenant: "0", settle: minSettle, timeout: 5 * time.Second, commit: "0123456789abcdef",
		proxyRevision: "0123456789abcdef", proxyImage: "sha256:img", now: func() time.Time { return testNow.Add(37 * time.Second) }}
	var out, errs bytes.Buffer
	if code := run(live, &out, &errs); code != 0 {
		t.Fatalf("live: %d %s", code, errs.String())
	}
	want := map[string]string{"x/same": verdictMatches, "x/real": verdictDiffers, "x/drift": verdictDataDiffers, "x/none": verdictEmpty,
		"x/fresh": verdictLokiNotReady, "x/scenario": verdictNotExecutable}
	lf := readFile(t, live.out)
	for id, v := range want {
		if lf.Cases[id].Verdict != v {
			t.Errorf("live %s: %+v", id, lf.Cases[id])
		}
	}
	if !reflect.DeepEqual(lf.Flags[flagRegression], []string{"x/real"}) || !lf.ProxyBuild.Verified || lf.ProxyBuild.Image != "sha256:img" {
		t.Errorf("live flags %v, build %+v", lf.Flags, lf.ProxyBuild)
	}
	replay := config{registry: root, replay: live.record, out: filepath.Join(dir, "replay.json"), tenant: "0", settle: minSettle,
		lokiVersion: lf.Loki, vlVersion: lf.VictoriaLogs, commit: live.commit, now: func() time.Time { return testNow.Add(3 * time.Hour) }}
	if code := run(replay, &out, &errs); code != 0 {
		t.Fatalf("replay: %d %s", code, errs.String())
	}
	rf := readFile(t, replay.out)
	if !reflect.DeepEqual(rf.Cases, lf.Cases) || !reflect.DeepEqual(rf.Counts, lf.Counts) || !reflect.DeepEqual(rf.Flags, lf.Flags) ||
		rf.WindowEnd != lf.WindowEnd || rf.ProxyBuild != lf.ProxyBuild {
		t.Errorf("replay differs from live:\nlive   %+v %v\nreplay %+v %v", lf.Counts, lf.WindowEnd, rf.Counts, rf.WindowEnd)
		for id := range lf.Cases {
			if !reflect.DeepEqual(rf.Cases[id], lf.Cases[id]) {
				t.Errorf("%s:\nlive   %+v\nreplay %+v", id, lf.Cases[id], rf.Cases[id])
			}
		}
	}
}

// A recording made before the header line resolves its windows at the end of its every-stream count.
func TestReplayWindowEndWithoutHeader(t *testing.T) {
	rp := &Replay{counts: map[CountKey]Recorded{{Selector: `{a="1"}`, End: 7}: {}, {Selector: stackWide, End: 5}: {},
		{Selector: stackWide, Start: 1, End: 5}: {}, {Selector: stackWide, End: 3}: {}}}
	if rp.WindowEnd() != 5 {
		t.Errorf("every-stream end: %d", rp.WindowEnd())
	}
	rp = &Replay{counts: map[CountKey]Recorded{{Selector: `{a="1"}`, End: 7}: {}, {Selector: `{b="1"}`, End: 9}: {}}}
	if rp.WindowEnd() != 9 {
		t.Errorf("latest end: %d", rp.WindowEnd())
	}
	if (&Replay{}).WindowEnd() != 0 {
		t.Error("no counts, no end")
	}
	if (&Replay{header: &RunHeader{WindowEnd: 3}}).WindowEnd() != 3 {
		t.Error("the header's end")
	}
}

func TestProxyBuild(t *testing.T) {
	commit := "0123456789abcdef0123456789abcdef01234567"
	cases := []struct {
		revision        string
		verified, stale bool
	}{
		{commit, true, false}, {"0123456789ab", true, false}, {"fedcba9876543210", false, true},
		{"", false, false}, {"unknown", false, false}, {"0123", false, false},
	}
	for _, c := range cases {
		b := proxyBuild(c.revision, "img", commit)
		if b.Verified != c.verified || b.Stale != c.stale || b.Image != "img" {
			t.Errorf("%q: %+v", c.revision, b)
		}
	}
	if b := proxyBuild(commit, "", ""); b.Verified || b.Stale {
		t.Errorf("no commit: %+v", b)
	}
}

// A run on a proxy built from another commit lists its differences apart and never fails -check on them;
// -require-current-proxy refuses it.
func TestStaleProxyRun(t *testing.T) {
	root := writeRegistry(t, map[string]string{"x/real": rangeCase("real")})
	loki, proxy, vl := precheckStack(t)
	dir := t.TempDir()
	cfg := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "c.json"), report: filepath.Join(dir, "c.md"),
		tenant: "0", settle: minSettle, timeout: 5 * time.Second, commit: "0123456789abcdef", proxyRevision: "fedcba9876543210",
		check: true, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("stale run: %d %s", code, errs.String())
	}
	f := readFile(t, cfg.out)
	if !f.ProxyBuild.Stale || len(f.Flags[flagRegression]) != 0 || !reflect.DeepEqual(f.Flags[flagStaleRegression], []string{"x/real"}) {
		t.Errorf("stale run: %+v %v", f.ProxyBuild, f.Flags)
	}
	if report, _ := os.ReadFile(cfg.report); !strings.Contains(string(report), "was built from `fedcba987654`, not the commit measured") {
		t.Errorf("report:\n%s", report)
	}
	cfg.requireCurrent = true
	if code := run(cfg, &out, &errs); code != 2 || !strings.Contains(errs.String(), "-require-current-proxy") {
		t.Errorf("require current: %d %s", code, errs.String())
	}
}

func streams(entries ...string) Answer {
	return ok(`{"data":{"resultType":"streams","result":[` + strings.Join(entries, ",") + `]}}`)
}

func stream(labels string, values ...string) string {
	return `{"stream":` + labels + `,"values":[` + strings.Join(values, ",") + `]}`
}

func TestEntriesCompareAsLokiDefinesThem(t *testing.T) {
	a, b := `{"app":"a"}`, `{"app":"b"}`
	cases := []struct {
		name        string
		loki, proxy Answer
		o           Options
		differ      bool
	}{
		{"lines of one timestamp in another order", streams(stream(a, `["30","x"]`, `["30","y"]`, `["20","z"]`)),
			streams(stream(a, `["30","y"]`, `["20","z"]`, `["30","x"]`)), Options{Limit: 100}, false},
		{"another line", streams(stream(a, `["30","x"]`)), streams(stream(a, `["30","q"]`)), Options{Limit: 100}, true},
		// At the limit (3 lines, backward), the oldest timestamp may hold any of its lines: compared above it.
		{"limit edge, backward", streams(stream(a, `["30","x"]`, `["20","p"]`), stream(b, `["20","q"]`)),
			streams(stream(a, `["30","x"]`, `["20","r"]`), stream(b, `["20","s"]`)), Options{Limit: 3}, false},
		{"limit edge keeps what is above the cut", streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`)),
			streams(stream(a, `["30","y"]`, `["20","p"]`, `["10","q"]`)), Options{Limit: 3}, true},
		// Only one side reached the limit: the other answer is complete (or short), nothing is cut, and the extra
		// older lines of the limit-reaching side are a difference.
		{"one side shorter at the limit", streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`)),
			streams(stream(a, `["30","x"]`, `["20","p"]`)), Options{Limit: 3}, true},
		{"proxy at the limit, Loki complete below it", streams(stream(a, `["30","x"]`, `["20","p"]`)),
			streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`)), Options{Limit: 3}, true},
		{"proxy at the limit, Loki complete below it, forward", streams(stream(a, `["10","x"]`, `["20","p"]`)),
			streams(stream(a, `["10","x"]`, `["20","p"]`, `["30","q"]`)), Options{Limit: 3, Forward: true}, true},
		{"both at the limit, the proxy cut at a newer edge", streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`)),
			streams(stream(a, `["30","x"]`, `["20","p"]`, `["20","r"]`)), Options{Limit: 3}, false},
		{"both at the limit, equal", streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`)),
			streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`)), Options{Limit: 3}, false},
		{"limit edge, forward", streams(stream(a, `["10","x"]`, `["20","p"]`)),
			streams(stream(a, `["10","x"]`, `["20","r"]`)), Options{Limit: 2, Forward: true}, false},
		{"under the limit nothing is cut", streams(stream(a, `["30","x"]`, `["20","p"]`)),
			streams(stream(a, `["30","x"]`, `["20","r"]`)), Options{Limit: 100}, true},
		{"no limit option, nothing is cut", streams(stream(a, `["20","p"]`)), streams(stream(a, `["20","r"]`)), Options{}, true},
	}
	for _, c := range cases {
		for _, facets := range [][]string{{facetEntries}, {facetData}} {
			got := CompareWith(facets, c.loki, c.proxy, c.o)
			if (len(got) > 0) != c.differ {
				t.Errorf("%s (%v): %+v", c.name, facets, got)
			}
		}
	}
}

func TestPatternEdgeBucketsAreLeftOut(t *testing.T) {
	pat := func(samples string) Answer {
		return ok(`{"status":"success","data":[{"pattern":"<_> x","samples":[` + samples + `]}]}`)
	}
	o := Options{Patterns: true}
	if d := CompareWith([]string{facetData}, pat(`[10,3],[20,5],[30,1]`), pat(`[10,4],[20,5],[30,2]`), o); len(d) != 0 {
		t.Errorf("edge buckets differ only: %+v", d)
	}
	if d := CompareWith([]string{facetData}, pat(`[10,3],[20,5],[30,1]`), pat(`[10,3],[20,6],[30,1]`), o); len(d) == 0 {
		t.Error("an inner bucket that differs is a difference")
	}
	// One side lacks the first bucket: both are compared from the later first bucket on.
	if d := CompareWith([]string{facetData}, pat(`[10,3],[20,5],[30,7],[40,1]`), pat(`[20,9],[30,7],[40,1]`), o); len(d) != 0 {
		t.Errorf("common inner range: %+v", d)
	}
	if d := CompareWith([]string{facetData}, ok(`{"data":[]}`), pat(`[10,1]`), o); len(d) == 0 {
		t.Error("patterns on one side only is a difference")
	}
}

func TestOptionsOf(t *testing.T) {
	q := map[string][]string{"limit": {"5"}, "direction": {"forward"}}
	if o := OptionsOf(Probe{Path: "/loki/api/v1/query_range", Mode: "subset"}, q); o.Limit != 5 || !o.Forward || !o.Subset {
		t.Errorf("query_range: %+v", o)
	}
	if o := OptionsOf(Probe{Path: "/loki/api/v1/query"}, nil); o.Limit != 100 || o.Forward {
		t.Errorf("default limit: %+v", o)
	}
	if o := OptionsOf(Probe{Path: "/loki/api/v1/patterns"}, nil); !o.Patterns || o.Limit != 0 {
		t.Errorf("patterns: %+v", o)
	}
	if o := OptionsOf(Probe{Path: "/loki/api/v1/labels", Mode: "numeric"}, nil); o.Limit != 0 || o.Tolerance != 1e-6 {
		t.Errorf("labels: %+v", o)
	}
	if !tsLess(float64(9), float64(10)) || tsLess("10", "9") || !tsLess([]any{1}, []any{2}) {
		t.Error("tsLess")
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("disk gone") }

// A record line holds both answers of a case and is not bounded: a metric answer can be megabytes.
func TestReplayReadsLongLines(t *testing.T) {
	body := strings.Repeat("x", 3<<20)
	text := `{"header":{"window_end":5}}` + "\n\n" +
		`{"case":"a","attempt":1,"request":{"method":"GET","path":"/x","query":""},"loki":{"status":200,"body":"` + body + `"}}` + "\n" +
		`{"case":"b","attempt":1,"request":{"method":"GET","path":"/x","query":""},"proxy":{"status":200,"body":"last"}}`
	rp, err := LoadReplay(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	if a := rp.Ask("loki", "a", 1, Request{}); len(a.Body) != len(body) {
		t.Errorf("long line: %d bytes", len(a.Body))
	}
	if a := rp.Ask("proxy", "b", 1, Request{}); a.Body != "last" {
		t.Errorf("a last line without a newline: %+v", a)
	}
	if rp.WindowEnd() != 5 {
		t.Errorf("header %d", rp.WindowEnd())
	}
	if _, err := LoadReplay(failingReader{}); err == nil {
		t.Error("a read error is an error")
	}
	if _, err := LoadReplay(strings.NewReader(`{"case":"a","attempt":1}` + "\n" + `{`)); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("a broken last line: %v", err)
	}
}

// A categorized answer whose structured metadata Loki keeps and the proxy answers as parsed is a difference of
// the metadata facets, not of the lines: a case that names `structuredMetadata` and `parsed labels` sees it.
func TestMetadataFacetsSeeALabelAnsweredAsParsed(t *testing.T) {
	a := `{"app":"a"}`
	loki := streams(stream(a, `["30","metadata line",{"structuredMetadata":{"msg":"hello"},"parsed":{"__error__":"JSONParserErr"}}]`))
	proxy := streams(stream(a, `["30","metadata line",{"structuredMetadata":{},"parsed":{"msg":"hello"}}]`))
	facets, unknown := Probe{Fields: []string{"status", "streams.values", "structuredMetadata", "parsed labels"}}.Facets()
	if len(unknown) != 0 {
		t.Fatalf("unknown fields %v", unknown)
	}
	got := map[string]bool{}
	for _, d := range CompareWith(facets, loki, proxy, Options{Limit: 100}) {
		got[d.Facet] = true
	}
	if got[facetEntries] || !got[facetStructuredMeta] || !got[facetParsed] {
		t.Errorf("differences %v: the lines agree, the two metadata facets do not", got)
	}
	if d := CompareWith(facets, loki, loki, Options{Limit: 100}); len(d) != 0 {
		t.Errorf("an answer differs from itself: %+v", d)
	}
}

// A difference where only one side reached the limit names both line counts and the limit.
func TestLimitDifferenceReportsTheCounts(t *testing.T) {
	a := `{"app":"a"}`
	loki := streams(stream(a, `["30","x"]`, `["20","p"]`))
	proxy := streams(stream(a, `["30","x"]`, `["20","p"]`, `["10","q"]`))
	for _, facets := range [][]string{{facetEntries}, {facetData}} {
		d := CompareWith(facets, loki, proxy, Options{Limit: 3})
		if len(d) != 1 || !strings.HasPrefix(d[0].Loki, "2 lines (under the limit 3): ") || !strings.HasPrefix(d[0].Proxy, "3 lines (at the limit 3): ") {
			t.Errorf("%v: %+v", facets, d)
		}
	}
	// Both at the limit, or both under it: no count prefix.
	for _, o := range []Options{{Limit: 2}, {Limit: 100}} {
		for _, d := range CompareWith([]string{facetEntries}, loki, streams(stream(a, `["30","x"]`, `["20","r"]`)), o) {
			if strings.Contains(d.Loki, "lines (") {
				t.Errorf("%+v: %+v", o, d)
			}
		}
	}
}
