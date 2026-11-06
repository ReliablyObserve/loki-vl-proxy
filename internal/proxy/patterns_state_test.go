package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// stateRow is a line the fake VictoriaLogs of the state tests holds.
type stateRow struct {
	stream, level, msg string
	at                 time.Time
}

// fakeStateVL answers the exact tier's count and lines queries over the rows inside each request's range, as
// VictoriaLogs does, and records the ranges the lines were read over.
type fakeStateVL struct {
	mu         sync.Mutex
	rows       []stateRow
	counts     int
	lineRanges [][2]time.Time
}

func (f *fakeStateVL) add(rows ...stateRow) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rows...)
}

func (f *fakeStateVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		q := r.Form.Get("query")
		from, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		to, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
		f.mu.Lock()
		defer f.mu.Unlock()
		var in []stateRow
		for _, row := range f.rows {
			app, pod, _ := strings.Cut(row.stream, "/")
			if ns := row.at.UnixNano(); ns >= from && ns < to && strings.Contains(q, `app:="`+app+`"`) && (!strings.Contains(q, "pod:=") || strings.Contains(q, `pod:="`+pod+`"`)) {
				in = append(in, row)
			}
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		switch {
		case strings.HasSuffix(q, "| stats count() as c"):
			f.counts++
			fmt.Fprintf(w, `{"c":"%d"}`+"\n", len(in))
		case strings.Contains(q, "| sort by (_stream_id, _time)"):
			f.lineRanges = append(f.lineRanges, [2]time.Time{time.Unix(0, from).UTC(), time.Unix(0, to).UTC()})
			sort.SliceStable(in, func(i, j int) bool {
				if in[i].stream != in[j].stream {
					return in[i].stream < in[j].stream
				}
				return in[i].at.Before(in[j].at)
			})
			for _, row := range in {
				encoded, _ := json.Marshal(map[string]string{"_time": row.at.Format(time.RFC3339Nano), "_msg": row.msg, "level": row.level, "_stream_id": row.stream})
				w.Write(append(encoded, '\n'))
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// stateLines is n lines of a stream ("app/pod"), one every 2 seconds from at, the number in each line varying.
func stateLines(stream, level, format string, at time.Time, n int) []stateRow {
	rows := make([]stateRow, n)
	for i := range rows {
		rows[i] = stateRow{stream: stream, level: level, msg: fmt.Sprintf(format, i), at: at.Add(time.Duration(2*i) * time.Second)}
	}
	return rows
}

// stateNow is the clock of the state tests: a whole hour, so the retention starts at an hour too.
var stateNow = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

func newStateProxy(t *testing.T, vl *fakeStateVL, now *time.Time, maxBytes int64) *Proxy {
	t.Helper()
	p := newPatternCountsProxy(t, vl.server(t).URL, 0)
	p.patternsExactMaxLines = 100_000
	p.patternState = newPatternStateStore(maxBytes, DefaultPatternsStateStreamIdle)
	p.patternState.now = func() time.Time { return *now }
	return p
}

// statePatterns is the answer of the proxy to a /patterns request over [start, end), as "level pattern=lines"
// and the samples, for comparisons.
func statePatterns(t *testing.T, p *Proxy, org, query string, start, end time.Time) (string, string) {
	t.Helper()
	q := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil)
	if org != "" {
		req.Header.Set("X-Scope-OrgID", org)
	}
	w := httptest.NewRecorder()
	p.handlePatterns(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var resp patternsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	var texts, samples []string
	for _, e := range resp.Data {
		texts = append(texts, fmt.Sprintf("%s %s=%d", e.Level, e.Pattern, countOf(e)))
		samples = append(samples, fmt.Sprintf("%s %s %v", e.Level, e.Pattern, e.Samples))
	}
	sort.Strings(texts)
	sort.Strings(samples)
	return strings.Join(texts, " | "), strings.Join(samples, " | ")
}

// A refresh reads only the lines after what the state read (from a little before, for lines made visible late),
// and answers as a state built from scratch on the same lines does.
//
// conformance: drilldown/patterns-full-range-counts, drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternStateRefreshReadsOnlyNewLines(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-50*time.Minute), 1500)...)
	vl.add(stateLines("api/b", "warn", "retry %d for payment", stateNow.Add(-40*time.Minute), 400)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	if vl.counts != 1 || len(vl.lineRanges) != 1 || !vl.lineRanges[0][0].Equal(stateNow.Add(-lokiPatternRetention)) {
		t.Fatalf("cold build: %d counts, lines read over %v; want one count and one read from the retention", vl.counts, vl.lineRanges)
	}

	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow, 20)...)
	now = stateNow.Add(time.Minute)
	gotTexts, gotSamples := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	if vl.counts != 1 || len(vl.lineRanges) != 2 {
		t.Fatalf("refresh: %d counts, %d reads; want no count and one more read", vl.counts, len(vl.lineRanges))
	}
	if from, to := vl.lineRanges[1][0], vl.lineRanges[1][1]; !from.Equal(stateNow.Add(-patternStateOverlap)) || !to.Equal(now) {
		t.Fatalf("refresh read [%s, %s), want the lines from %s on", from, to, stateNow.Add(-patternStateOverlap))
	}

	fresh := newStateProxy(t, vl, &now, 1<<30)
	wantTexts, wantSamples := statePatterns(t, fresh, "", `{app="api"}`, now.Add(-time.Hour), now)
	if gotTexts != wantTexts || gotSamples != wantSamples {
		t.Fatalf("refreshed state answers\n %s\n %s\nfresh state answers\n %s\n %s", gotTexts, gotSamples, wantTexts, wantSamples)
	}
	if !strings.Contains(gotTexts, "info order <_> shipped to berlin=1520") || !strings.Contains(gotTexts, "warn retry <_> for payment=400") {
		t.Fatalf("answer %s", gotTexts)
	}
}

// A pattern's text is its cluster's template trained on every line the state read, as Loki's is, the samples
// being those inside the range: the lines before the range, whose last token varies, make it <_> in the template
// of a range whose lines all end the same.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternStateTemplatesComeFromEveryLineRead(t *testing.T) {
	vl := &fakeStateVL{}
	for i := 0; i < 60; i++ {
		vl.add(stateRow{stream: "api/a", level: "info", msg: "cache warmed for the region in mode " + []string{"fast", "slow", "cold"}[i%3], at: stateNow.Add(-90*time.Minute + time.Duration(i)*time.Second)})
	}
	for i := 0; i < 40; i++ {
		vl.add(stateRow{stream: "api/a", level: "info", msg: "cache warmed for the region in mode fast", at: stateNow.Add(-10*time.Minute + time.Duration(i)*time.Second)})
	}
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-15*time.Minute), now)
	if want := "info cache warmed for the region in mode <_>=40"; got != want {
		t.Fatalf("answer %q, want %q: the template of every line read, the lines of the range counted", got, want)
	}
}

// Selectors that share streams read and train each stream once, and each answers as it would alone.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternStateSelectorsShareStreams(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-30*time.Minute), 100)...)
	vl.add(stateLines("api/b", "info", "order %d shipped to lisbon", stateNow.Add(-30*time.Minute), 100)...)
	now := stateNow
	shared := newStateProxy(t, vl, &now, 1<<30)
	_, broad := statePatterns(t, shared, "", `{app="api"}`, now.Add(-time.Hour), now)
	_, narrow := statePatterns(t, shared, "", `{app="api", pod="b"}`, now.Add(-time.Hour), now)

	alone := newStateProxy(t, vl, &now, 1<<30)
	_, wantNarrow := statePatterns(t, alone, "", `{app="api", pod="b"}`, now.Add(-time.Hour), now)
	_, wantBroad := statePatterns(t, newStateProxy(t, vl, &now, 1<<30), "", `{app="api"}`, now.Add(-time.Hour), now)
	if broad != wantBroad || narrow != wantNarrow {
		t.Fatalf("shared state answers\n %s\n %s\nalone\n %s\n %s", broad, narrow, wantBroad, wantNarrow)
	}
}

// A stream that read no line for -patterns-state-stream-idle is kept as its patterns and samples, and the answer
// is the same as before.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternStateFoldsIdleStreams(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-40*time.Minute), 100)...)
	vl.add(stateLines("api/b", "info", "order %d shipped to lisbon", stateNow.Add(-2*time.Minute), 50)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	_, before := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	tenant := p.patternState.tenants[p.patternStateTenantKey(httptest.NewRequest(http.MethodGet, "/", nil))]
	if tenant == nil || len(tenant.folded["api/a"]) != 1 || len(tenant.folded["api/b"]) != 0 {
		t.Fatalf("idle stream api/a not folded, active api/b folded: %+v", tenant)
	}
	for mk := range tenant.miner.miners {
		if mk.stream == "api/a" {
			t.Fatalf("folded stream still holds its clusterer")
		}
	}
	now = now.Add(time.Second)
	_, after := statePatterns(t, p, "", `{app="api"}`, stateNow.Add(-time.Hour), stateNow)
	if before != after {
		t.Fatalf("answer before folding\n %s\nafter\n %s", before, after)
	}
}

// Tenants never share a state: each reads its own lines.
//
// conformance: loki_api_v1_patterns
func TestPatternStateKeepsTenantsApart(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-30*time.Minute), 100)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	a := httptest.NewRequest(http.MethodGet, "/", nil)
	a.Header.Set("X-Scope-OrgID", "team-a")
	b := httptest.NewRequest(http.MethodGet, "/", nil)
	b.Header.Set("X-Scope-OrgID", "team-b")
	if p.patternStateTenantKey(a) == p.patternStateTenantKey(b) {
		t.Fatal("two tenants share a state key")
	}
}

// The state answers only what it can answer as Loki does: a range that starts before Loki's pattern retention,
// or a selector whose lines over the retention are more than -patterns-exact-max-lines, is answered without it.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternStateAnswersOnlyInsideItsBounds(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-30*time.Minute), 100)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	scope, ok := p.patternScope(`{app="api"}`, strconv.FormatInt(now.Add(-5*time.Hour).UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10), "")
	if !ok {
		t.Fatal("scope")
	}
	if _, answered := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 100_000, 100); answered {
		t.Fatal("a range older than the retention was answered from the state")
	}
	scope.startNs = now.Add(-time.Hour).UnixNano()
	if _, answered := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 99, 100); answered {
		t.Fatal("a selector with more lines than the cap was answered from the state")
	}
	if _, answered := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 100, 100); !answered {
		t.Fatal("a selector within the cap was not answered from the state")
	}
}

// Past -patterns-state-max-bytes the state is dropped, and built again it answers the same.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternStateDroppedAndBuiltAgainAnswersTheSame(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-30*time.Minute), 100)...)
	now := stateNow
	tiny := newStateProxy(t, vl, &now, 1)
	_, first := statePatterns(t, tiny, "", `{app="api"}`, now.Add(-time.Hour), now)
	reads := len(vl.lineRanges)
	_, second := statePatterns(t, tiny, "", `{app="api"}`, now.Add(-time.Hour+time.Second), now)
	if len(vl.lineRanges) != reads+1 || vl.counts != 2 {
		t.Fatalf("a dropped state was not built again: %d reads after %d, %d counts", len(vl.lineRanges), reads, vl.counts)
	}
	_, want := statePatterns(t, newStateProxy(t, vl, &now, 1<<30), "", `{app="api"}`, now.Add(-time.Hour+time.Second), now)
	if second != want || first == "" {
		t.Fatalf("rebuilt state answers\n %s\nwant\n %s", second, want)
	}
}

// A stream that pauses for longer than -patterns-state-stream-idle and writes the same pattern again has its
// samples zero-filled across the pause, as Loki's one cluster of the stream has.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternStateStreamResumingAfterIdleFillsItsPause(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-50*time.Minute), 100)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow, 40)...)
	now = stateNow.Add(2 * time.Minute)
	_, got := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	_, want := statePatterns(t, newStateProxy(t, vl, &now, 1<<30), "", `{app="api"}`, now.Add(-time.Hour), now)
	if got != want {
		t.Fatalf("resumed stream answers\n %s\nas one cluster\n %s", got, want)
	}
}

// Lines of a stream that share a time are all mined, as Loki mines them (it skips only lines older than the newest);
// a refresh that reads them again, and new lines at that time, mines only the new ones.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternStateMinesLinesThatShareATimeOnce(t *testing.T) {
	vl := &fakeStateVL{}
	at := stateNow.Add(-5 * time.Second)
	for i := 0; i < 30; i++ {
		vl.add(stateRow{stream: "api/a", level: "info", msg: fmt.Sprintf("order %d shipped to berlin", i), at: at})
	}
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	if got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now); got != "info order <_> shipped to berlin=30" {
		t.Fatalf("answer %q: every line at one time mined once", got)
	}
	for i := 30; i < 35; i++ {
		vl.add(stateRow{stream: "api/a", level: "info", msg: fmt.Sprintf("order %d shipped to berlin", i), at: at})
	}
	now = stateNow.Add(time.Second)
	if got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now); got != "info order <_> shipped to berlin=35" {
		t.Fatalf("refresh answer %q: the lines read again not mined twice, the new ones at that time mined", got)
	}
}

// The patterns queries read only the lines a pattern can be mined from: a JSON line whose text holds none of the
// message fields' keys is left out by VictoriaLogs, so a service writing only such lines answers no pattern from
// a count of 0 (Loki mines none from them either).
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternQueriesLeaveOutJSONLinesWithoutAMessage(t *testing.T) {
	want := `-(_msg:"{"* _msg:~"\\}$" NOT _msg:~"\"(log|message|msg|msg_|_msg|content)\"\\s*:")`
	if got := patternMinableLines.String(); got != want {
		t.Fatalf("filter\n got %s\nwant %s", got, want)
	}
	p := newPatternCountsProxy(t, "http://127.0.0.1:1", 0)
	scope, ok := p.patternScope(`{app="api"}`, "1793988900", "1793990100", "")
	if !ok || !strings.HasSuffix(scope.filter.String(), " "+want) {
		t.Fatalf("scope filter %s does not leave the lines out", scope.filter.String())
	}
}
