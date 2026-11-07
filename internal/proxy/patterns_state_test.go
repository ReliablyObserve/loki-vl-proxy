package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
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
	// countsCalls counts the counts flow's groups and timeline queries.
	countsCalls int
}

var stateDigits = regexp.MustCompile(`[0-9]+`)

// writeStateCounts answers the counts flow's groups query (each group of lines equal once numbers are collapsed,
// with its smallest and largest line and earliest time) or its timeline query (each group's lines per step), as
// VictoriaLogs answers them.
func writeStateCounts(w http.ResponseWriter, q string, rows []stateRow) {
	type group struct {
		level, key, lo, hi string
		first              time.Time
		n                  int
	}
	step := int64(0)
	if i := strings.Index(q, "stats by (_time:"); i >= 0 {
		rest := q[i+len("stats by (_time:"):]
		step, _ = strconv.ParseInt(rest[:strings.Index(rest, "s")], 10, 64)
	}
	groups := map[[3]string]*group{}
	var order [][3]string
	for _, row := range rows {
		key := stateDigits.ReplaceAllString(row.msg, "<N>")
		bucket := ""
		if step > 0 {
			bucket = time.Unix(row.at.Unix()/step*step, 0).UTC().Format(time.RFC3339)
		}
		k := [3]string{row.level, key, bucket}
		g := groups[k]
		if g == nil {
			g = &group{level: row.level, key: key, lo: row.msg, hi: row.msg, first: row.at}
			groups[k] = g
			order = append(order, k)
		}
		g.n++
		if row.msg < g.lo {
			g.lo = row.msg
		}
		if row.msg > g.hi {
			g.hi = row.msg
		}
		if row.at.Before(g.first) {
			g.first = row.at
		}
	}
	for _, k := range order {
		g := groups[k]
		out := map[string]string{"level": g.level, patternCountsGroupHash: g.key, "c": strconv.Itoa(g.n)}
		if step > 0 {
			out["_time"] = k[2]
		} else {
			out["_msg"], out[patternCountsMaxField], out[patternCountsFirstField] = g.lo, g.hi, g.first.Format(time.RFC3339Nano)
		}
		encoded, _ := json.Marshal(out)
		w.Write(append(encoded, '\n'))
	}
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
		case strings.Contains(q, "| sort by (_stream_id, _time, _msg)"):
			f.lineRanges = append(f.lineRanges, [2]time.Time{time.Unix(0, from).UTC(), time.Unix(0, to).UTC()})
			sort.SliceStable(in, func(i, j int) bool {
				if in[i].stream != in[j].stream {
					return in[i].stream < in[j].stream
				}
				if !in[i].at.Equal(in[j].at) {
					return in[i].at.Before(in[j].at)
				}
				return in[i].msg < in[j].msg
			})
			for _, row := range in {
				encoded, _ := json.Marshal(map[string]string{"_time": row.at.Format(time.RFC3339Nano), "_msg": row.msg, "level": row.level, "_stream_id": row.stream})
				w.Write(append(encoded, '\n'))
			}
		case strings.Contains(q, "collapse_nums at _lvp_collapsed"):
			f.countsCalls++
			writeStateCounts(w, q, in)
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

// stateNow is the clock of the state tests: a whole hour, so the retention starts at an hour too, a day before the
// real clock, so the windows before it are settled and cached as on a live proxy.
var stateNow = time.Now().UTC().Truncate(time.Hour).Add(-24 * time.Hour)

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
// conformance: drilldown/patterns-full-range-counts, drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateRefreshReadsOnlyNewLines(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-50*time.Minute), 1500)...)
	vl.add(stateLines("api/b", "warn", "retry %d for payment", stateNow.Add(-40*time.Minute), 400)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	if vl.counts != 2 || len(vl.lineRanges) != 1 || !vl.lineRanges[0][0].Equal(stateNow.Add(-lokiPatternRetention)) {
		t.Fatalf("cold build: %d counts, lines read over %v; want the retention's and the range's counts and one read from the retention", vl.counts, vl.lineRanges)
	}

	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow, 20)...)
	now = stateNow.Add(time.Minute)
	gotTexts, gotSamples := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	if vl.counts != 2 || len(vl.lineRanges) != 2 {
		t.Fatalf("refresh: %d counts, %d reads; want no more count and one more read", vl.counts, len(vl.lineRanges))
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

// The request that finds no state for a selector counts the range with the retention only when the range lies
// inside it: a longer range costs more to count than the retention, and a selector the state is built for at once
// never needs its count.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateCountsOnlyARangeInsideTheRetentionAlongside(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-50*time.Minute), 100)...)
	for _, c := range []struct {
		name   string
		start  time.Time
		counts int
	}{
		{"1h", stateNow.Add(-time.Hour), 2},
		{"24h", stateNow.Add(-24 * time.Hour), 1},
	} {
		now := stateNow
		vl.mu.Lock()
		vl.counts = 0
		vl.mu.Unlock()
		p := newStateProxy(t, vl, &now, 1<<30)
		scope, _ := p.patternScope(`{app="api"}`, strconv.FormatInt(c.start.UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10), "")
		_, _, lines, answered := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 1000, 100)
		vl.mu.Lock()
		counts := vl.counts
		vl.mu.Unlock()
		if !answered || counts != c.counts || (c.counts == 2) != (lines == 100) || (c.counts == 1) != (lines == -1) {
			t.Fatalf("%s: answered %v with %d counts and range lines %d, want %d counts", c.name, answered, counts, lines, c.counts)
		}
	}
}

// A range that ends a little after now (Drilldown's moving window one step ahead) is answered from the state, its
// lines read up to now, not by reading the whole range's lines again.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateAnswersARangeEndingAfterNow(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-50*time.Minute), 100)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	ask := func(start, end time.Time) ([]patternResultEntry, bool) {
		scope, _ := p.patternScope(`{app="api"}`, strconv.FormatInt(start.UnixNano(), 10), strconv.FormatInt(end.UnixNano(), 10), "300")
		entries, _, _, ok := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 1000, 100)
		return entries, ok
	}
	want, ok := ask(now.Add(-time.Hour), now)
	reads := len(vl.lineRanges)
	got, shiftedOK := ask(now.Add(-time.Hour+5*time.Minute), now.Add(5*time.Minute))
	if !ok || !shiftedOK || len(got) != len(want) || len(vl.lineRanges) != reads {
		t.Fatalf("a range ending a step after now: answered %v with %d patterns and %d more reads, want %d patterns from the state", shiftedOK, len(got), len(vl.lineRanges)-reads, len(want))
	}
	if _, ok := ask(now.Add(-time.Hour), now.Add(time.Hour)); ok {
		t.Fatal("a range ending an hour after now was answered from the state, want its own lines read")
	}
}

// A pattern's text is its cluster's template trained on every line the state read, as Loki's is, the samples
// being those inside the range: the lines before the range, whose last token varies, make it <_> in the template
// of a range whose lines all end the same.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
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
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
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
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateFoldsIdleStreams(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-40*time.Minute), 100)...)
	vl.add(stateLines("api/b", "info", "order %d shipped to lisbon", stateNow.Add(-2*time.Minute), 50)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	_, before := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	var tenant *patternTenantState
	for _, state := range p.patternState.tenants {
		tenant = state // one tenant and selector
	}
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
// conformance: loki_api_v1_patterns, patterns-loki-mining
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

// A range that starts before Loki's pattern retention is answered as Loki answers it, with the samples of the
// retention alone, and one that ends before it with no pattern; a selector whose lines over the retention are more
// than -patterns-exact-max-lines is answered without the state.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateAnswersOnlyInsideItsBounds(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-30*time.Minute), 100)...)
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-5*time.Hour), 100)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	if got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-6*time.Hour), now); got != "info order <_> shipped to berlin=100" {
		t.Fatalf("a 6h range answers %q, want the retention's 100 lines alone, as Loki", got)
	}
	if got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-6*time.Hour), now.Add(-4*time.Hour)); got != "" {
		t.Fatalf("a range before the retention answers %q, want none, as Loki", got)
	}
	p = newStateProxy(t, vl, &now, 1<<30)
	scope, ok := p.patternScope(`{app="api"}`, strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10), "")
	if !ok {
		t.Fatal("scope")
	}
	if _, _, _, answered := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 99, 100); answered {
		t.Fatal("a selector with more lines than the cap was answered from the state")
	}
	if _, _, _, answered := p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 100, 100); !answered {
		t.Fatal("a selector within the cap was not answered from the state")
	}
}

// Past -patterns-state-max-bytes the state is dropped; the next request continues it from the snapshot it wrote
// and answers the same, reading no line again.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateDroppedAndBuiltAgainAnswersTheSame(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-30*time.Minute), 100)...)
	now := stateNow
	tiny := newStateProxy(t, vl, &now, 1)
	_, first := statePatterns(t, tiny, "", `{app="api"}`, now.Add(-time.Hour), now)
	reads, counts := len(vl.lineRanges), vl.counts
	_, second := statePatterns(t, tiny, "", `{app="api"}`, now.Add(-time.Hour+time.Second), now)
	if len(vl.lineRanges) != reads || vl.counts != counts {
		t.Fatalf("a dropped state read its lines again (%d reads after %d, %d counts after %d), want it continued from its snapshot", len(vl.lineRanges), reads, vl.counts, counts)
	}
	_, want := statePatterns(t, newStateProxy(t, vl, &now, 1<<30), "", `{app="api"}`, now.Add(-time.Hour+time.Second), now)
	if second != want || first == "" {
		t.Fatalf("restored state answers\n %s\nwant\n %s", second, want)
	}
}

// A stream that pauses for longer than -patterns-state-stream-idle and writes the same pattern again has its
// samples zero-filled across the pause, as Loki's one cluster of the stream has.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
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
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
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
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternQueriesLeaveOutJSONLinesWithoutAMessage(t *testing.T) {
	// _msg:="{"* is the line starting with {: a line with a { inside, such as zap's console lines ending in a JSON
	// object, is mined as Loki mines it. TestVLCapability_PatternLinesFilterAndTieOrder checks the lines VictoriaLogs
	// keeps with this text (patternMinableLinesFilter there).
	want := `-(_msg:="{"* _msg:~"\\}$" -("\"log\"" OR "\"message\"" OR "\"msg\"" OR "\"msg_\"" OR "\"_msg\"" OR "\"content\""))`
	if got := patternMinableLines.String(); got != want {
		t.Fatalf("filter\n got %s\nwant %s", got, want)
	}
	p := newPatternCountsProxy(t, "http://127.0.0.1:1", 0)
	scope, ok := p.patternScope(`{app="api"}`, "1793988900", "1793990100", "")
	if !ok || !strings.HasSuffix(scope.filter.String(), " "+want) {
		t.Fatalf("scope filter %s does not leave the lines out", scope.filter.String())
	}
}

// A selector with more lines than -patterns-exact-max-lines over the retention, up to
// -patterns-state-max-build-lines, has its state built in the background an hour at a time: the request that
// starts the build and those during it are answered without the state (the first with the range's count of lines,
// counted with the retention's), the build reads once that request is answered, and once built the state answers
// as one built in a single read does.
//
// conformance: drilldown/patterns-full-range-counts, drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateBuiltInTheBackgroundForBusySelectors(t *testing.T) {
	vl := &fakeStateVL{}
	for h := 0; h < 3; h++ {
		vl.add(stateLines(fmt.Sprintf("api/p%d", h), "info", "order %d shipped to berlin", stateNow.Add(-time.Duration(170-50*h)*time.Minute), 200)...)
	}
	vl.add(stateLines("api/long", "info", "retry %d for payment", stateNow.Add(-150*time.Minute), 4000)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	p.patternState.maxBuildLines = 10_000
	scope, _ := p.patternScope(`{app="api"}`, strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10), "")
	ctx, answer := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	_, _, lines, answered := p.patternsFromState(req, scope, 1000, 100)
	if answered {
		t.Fatal("a selector over the exact cap was answered before its state was built")
	}
	if want, err := p.countPatternLines(ctx, scope, scope.startNs, scope.endNs); err != nil || lines != want || lines == 0 {
		t.Fatalf("range counted %d lines with the retention, want %d (%v)", lines, want, err)
	}
	time.Sleep(50 * time.Millisecond)
	vl.mu.Lock()
	early := len(vl.lineRanges)
	vl.mu.Unlock()
	if early != 0 {
		t.Fatalf("the build read %d windows before the request that started it was answered", early)
	}
	answer()
	p.patternState.wg.Wait()
	if len(vl.lineRanges) < 3 {
		t.Fatalf("background build read %d windows, want one per hour of the retention", len(vl.lineRanges))
	}
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	got, _, _, answered := p.patternsFromState(req, scope, 1000, 100)
	if !answered {
		t.Fatal("built state did not answer")
	}
	direct := newStateProxy(t, vl, &now, 1<<30)
	want, _, _, _ := direct.patternsFromState(req, scope, 100_000, 100)
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("background-built state answers\n %s\nbuilt in one read\n %s", gotJSON, wantJSON)
	}
}

// Closing the store stops a background build and waits for it.
//
// conformance: loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateCloseStopsBuilds(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-2*time.Hour), 3000)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	p.patternState.maxBuildLines = 10_000
	scope, _ := p.patternScope(`{app="api"}`, strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10), "")
	p.patternsFromState(httptest.NewRequest(http.MethodGet, "/", nil), scope, 100, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.patternState.close(ctx)
	if ctx.Err() != nil {
		t.Fatal("close did not wait for the build to stop")
	}
	select {
	case p.patternState.builds <- struct{}{}:
	default:
		t.Fatal("the build slot was not released")
	}
}

// With -patterns-full-range, a range longer than Loki's retention is answered over all of it: the patterns the
// state answers, with their samples before the state's counted from VictoriaLogs, each line in the pattern it
// matches; a line no pattern of the state matches is not counted, and a refresh counts only the window its moving
// start cuts again.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateFullRangeCountsTheHistoryOntoItsPatterns(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-10*time.Hour), 3000)...)
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-90*time.Minute), 100)...)
	vl.add(stateLines("api/b", "warn", "disk %d nearly full on node", stateNow.Add(-9*time.Hour), 50)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	p.patternsFullRange = true
	p.queryRangeHistoryCacheTTL = time.Hour // settled windows are cached, as with the deployed default
	got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-12*time.Hour), now)
	if want := "info order <_> shipped to berlin=3100"; got != want {
		t.Fatalf("12h answer %q, want %q: the state's pattern with every line of the range", got, want)
	}
	calls := vl.countsCalls
	now = now.Add(30 * time.Second)
	again, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-12*time.Hour), now)
	if again != got {
		t.Fatalf("refresh answers %q, want %q", again, got)
	}
	// The first window, which the moving start cuts, is counted again; the settled ones come from the cache.
	if again := vl.countsCalls - calls; again > 2 {
		t.Fatalf("refresh counted %d windows again, want the first alone", again/2)
	}
	p.patternsFullRange = false
	now = now.Add(30 * time.Second)
	if loki, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-12*time.Hour), now); loki != "info order <_> shipped to berlin=100" {
		t.Fatalf("without full range the answer is %q, want the retention's alone", loki)
	}
}

// With -patterns-full-range, a range the state holds no pattern for (one that ends before the state's samples, as
// yesterday's) is mined from its own lines, so its chart is not empty.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternStateFullRangeMinesARangeBeforeTheState(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-20*time.Hour), 100)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	p.patternsFullRange = true
	if got, _ := statePatterns(t, p, "", `{app="api"}`, now.Add(-21*time.Hour), now.Add(-19*time.Hour)); got != "info order <_> shipped to berlin=100" {
		t.Fatalf("a range before the state answers %q, want its own lines' pattern", got)
	}
}

// A cached answer is served as it was made: its samples, zero-filled per stream, are not filled again, so a
// repeated request (or a peer serving a copy) answers what the first did.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternCachedAnswerIsServedAsMade(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-50*time.Minute), 40)...)
	vl.add(stateLines("api/b", "info", "order %d shipped to berlin", stateNow.Add(-10*time.Minute), 40)...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	_, first := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	_, cached := statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	if cached != first {
		t.Fatalf("cached answer\n %s\nfirst answer\n %s", cached, first)
// A peer that takes a selector over, or its holder after a restart, continues the state from the snapshot the
// holder wrote to the cache: it reads only the lines after it, and answers as the holder would, line for line.
//
// conformance: peer-cache-patterns, patterns-loki-mining, loki_api_v1_patterns
func TestPatternStateContinuesFromItsSnapshot(t *testing.T) {
	vl := &fakeStateVL{}
	for i := 0; i < 400; i++ {
		vl.add(stateRow{stream: "api/a", level: "info", msg: fmt.Sprintf("order %d shipped to %s via %s", i, []string{"berlin", "oslo"}[i%2], []string{"dhl", "ups", "fedex"}[i%3]), at: stateNow.Add(-50*time.Minute + time.Duration(i)*5*time.Second)})
	}
	now := stateNow
	holder := newStateProxy(t, vl, &now, 1<<30)
	statePatterns(t, holder, "", `{app="api"}`, now.Add(-time.Hour), now)

	for i := 0; i < 60; i++ {
		vl.add(stateRow{stream: "api/a", level: "info", msg: fmt.Sprintf("order %d shipped to lisbon via dhl", i), at: stateNow.Add(time.Duration(i) * time.Second)})
	}
	now = stateNow.Add(2 * time.Minute)
	// The same VictoriaLogs, and the snapshot reaching it through the cache (its disk or the peers).
	taker := newPatternCountsProxy(t, holder.backend.String(), 0)
	taker.cache = holder.cache
	taker.patternsExactMaxLines = holder.patternsExactMaxLines
	taker.patternState = newPatternStateStore(1<<30, DefaultPatternsStateStreamIdle)
	taker.patternState.now = func() time.Time { return now }
	reads := len(vl.lineRanges)
	_, got := statePatterns(t, taker, "", `{app="api"}`, now.Add(-time.Hour), now)
	if len(vl.lineRanges) != reads+1 || !vl.lineRanges[reads][0].Equal(stateNow.Add(-patternStateOverlap)) {
		t.Fatalf("the taker read %v, want the lines after the snapshot alone", vl.lineRanges[reads:])
	}
	_, want := statePatterns(t, holder, "", `{app="api"}`, now.Add(-time.Hour), now)
	if got != want {
		t.Fatalf("continued from the snapshot\n %s\nthe holder\n %s", got, want)
	}
}

// A snapshot restores the state exactly: its clusters, prefix trees, samples and the lines read at each stream's
// newest time, so the state trains on as the one it was written from.
//
// conformance: peer-cache-patterns, patterns-loki-mining
func TestPatternStateSnapshotRoundTrips(t *testing.T) {
	vl := &fakeStateVL{}
	for i := 0; i < 300; i++ {
		vl.add(stateRow{stream: fmt.Sprintf("api/p%d", i%3), level: []string{"info", "warn"}[i%2], msg: fmt.Sprintf("job %d of %s finished in %dms", i, []string{"export", "import", "reindex"}[i%3], i%90), at: stateNow.Add(-40*time.Minute + time.Duration(i)*7*time.Second)})
	}
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	statePatterns(t, p, "", `{app="api"}`, now.Add(-time.Hour), now)
	var tenant *patternTenantState
	for _, state := range p.patternState.tenants {
		tenant = state
	}
	var s *patternSelectorState
	var selector string
	for sel, state := range tenant.selectors {
		selector, s = sel, state
	}
	data, err := encodePatternStateSnapshot(tenant.snapshot(s))
	if err != nil {
		t.Fatal(err)
	}
	snap, ok := decodePatternStateSnapshot(data, 1<<30)
	if !ok {
		t.Fatal("snapshot does not decode")
	}
	restored := &patternTenantState{}
	rs := restored.restore(selector, snap)
	again, err := encodePatternStateSnapshot(restored.snapshot(rs))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := decodePatternStateSnapshot(again, 1<<30)
	first, _ := json.Marshal(snap)
	second, _ := json.Marshal(a)
	if string(first) != string(second) {
		t.Fatalf("a restored state snapshots differently:\n%s\n%s", first, second)
	}
}
