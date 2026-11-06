package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// exactLines is rows of the exact tier's lines query: n lines of one stream, one every second from second `from`
// of 2026-11-06T18:20:00Z, sorted as the query sorts them.
func exactLines(stream, level, msg string, from, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		at := time.Date(2026, 11, 6, 18, 20, 0, 0, time.UTC).Add(time.Duration(from+i) * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": at.Format(time.RFC3339Nano), "_msg": msg, "level": level, "_stream_id": stream})
		b.Write(row)
		b.WriteByte('\n')
	}
	return b.String()
}

func newExactProxy(t *testing.T, fake *fakePatternCounts, maxLines int) *Proxy {
	t.Helper()
	p := newPatternCountsProxy(t, fake.server(t).URL, 0)
	p.patternsExactMaxLines = maxLines
	return p
}

func patternAnswer(resp patternsResponse) []string {
	var got []string
	for _, e := range resp.Data {
		got = append(got, fmt.Sprintf("%s %s=%d", e.Level, e.Pattern, countOf(e)))
	}
	sort.Strings(got)
	return got
}

// The queries of the exact tier are typed: a count of the selector's lines, and its lines with the fields the
// miner reads, sorted by stream then time.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternExactQueriesAreTyped(t *testing.T) {
	filter := logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "api"}
	if got, want := patternExactCountQuery(filter).String(), `app:="api" | stats count() as c`; got != want {
		t.Errorf("count query %s, want %s", got, want)
	}
	want := `app:="api" | fields _time, _msg, _stream_id, detected_level, level, LEVEL, Level, "log.level", severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT, severity_number | sort by (_stream_id, _time)`
	if got := patternExactLinesQuery(filter).String(); got != want {
		t.Errorf("lines query\n got %s\nwant %s", got, want)
	}
}

// A range with at most -patterns-exact-max-lines lines is mined from its lines: each stream apart, so two pods
// whose lines differ in the last token give a pattern each, in a count query and a lines query and nothing else.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternsFromLinesMineEachStream(t *testing.T) {
	fake := &fakePatternCounts{
		count: `{"c":"70"}` + "\n",
		lines: exactLines("pod-a", "info", "paid order for eur", 0, 35) + exactLines("pod-b", "info", "paid order for usd", 0, 35),
	}
	p := newExactProxy(t, fake, 100)
	got := patternAnswer(patternsRequest(t, p, `{app="api"}`, ""))
	if want := []string{"info paid order for eur=35", "info paid order for usd=35"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("answer %q, want %q", got, want)
	}
	if fake.countCalls.Load() != 1 || fake.lineCalls.Load() != 1 || fake.calls.Load() != 2 {
		t.Errorf("VictoriaLogs calls: %d count, %d lines, %d in all; want 1, 1, 2", fake.countCalls.Load(), fake.lineCalls.Load(), fake.calls.Load())
	}
}

// Above -patterns-exact-max-lines, with the tier off (0) and when the lines query fails, the request is answered
// from the counts, which mines the groups of a level's streams together; a failed read is never answered from.
//
// conformance: drilldown/patterns-full-range-counts, drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternsFromLinesFallBackToCounts(t *testing.T) {
	counted := func() *fakePatternCounts {
		return &fakePatternCounts{
			count: `{"c":"70"}` + "\n",
			lines: exactLines("pod-a", "info", "paid order for eur", 0, 35) + exactLines("pod-b", "info", "paid order for usd", 0, 35),
			groups: `{"level":"info","_lvp_group":"g1","c":"35","_msg":"paid order for eur","_lvp_max":"paid order for eur","_lvp_first":"2026-11-06T18:20:00Z"}
{"level":"info","_lvp_group":"g2","c":"35","_msg":"paid order for usd","_lvp_max":"paid order for usd","_lvp_first":"2026-11-06T18:20:01Z"}
`,
			timeline: `{"_time":"2026-11-06T18:20:00Z","level":"info","_lvp_group":"g1","c":"35"}
{"_time":"2026-11-06T18:20:00Z","level":"info","_lvp_group":"g2","c":"35"}
`,
		}
	}
	const fromCounts = "info paid order for <_>=70"
	for _, tc := range []struct {
		name                string
		maxLines            int
		linesStatus         int
		count               string
		wantCount, wantRead int32
	}{
		{"above the cap", 50, 0, "", 1, 0},
		{"tier off", 0, 0, "", 0, 0},
		{"lines query fails", 100, http.StatusInternalServerError, "", 1, 1},
		{"count not read", 100, 0, "not a count", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := counted()
			fake.linesStatus = tc.linesStatus
			if tc.count != "" {
				fake.count = tc.count + "\n"
			}
			p := newExactProxy(t, fake, tc.maxLines)
			got := patternAnswer(patternsRequest(t, p, `{app="api"}`, ""))
			if len(got) != 1 || got[0] != fromCounts {
				t.Errorf("answer %q, want the counts' %q", got, fromCounts)
			}
			if fake.countCalls.Load() != tc.wantCount || fake.lineCalls.Load() != tc.wantRead {
				t.Errorf("%d count and %d lines queries, want %d and %d", fake.countCalls.Load(), fake.lineCalls.Load(), tc.wantCount, tc.wantRead)
			}
		})
	}
}

// Each stream's cluster is zero-filled between its own first and last step, and equal patterns are then summed:
// two streams active in disjoint halves of the range leave the steps between with no sample at all, while one
// stream with a quiet step in its middle has a 0 there.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternSamplesAreZeroFilledPerStreamCluster(t *testing.T) {
	samples := func(lines string) map[int64]int {
		fake := &fakePatternCounts{count: fmt.Sprintf(`{"c":"%d"}`+"\n", strings.Count(lines, "\n")), lines: lines}
		resp := patternsRequest(t, newExactProxy(t, fake, 1000), `{app="api"}`, "")
		if len(resp.Data) != 1 {
			t.Fatalf("%d patterns: %+v", len(resp.Data), resp.Data)
		}
		got := map[int64]int{}
		for _, s := range resp.Data[0].Samples {
			ts, _ := numberToInt64(s[0])
			n, _ := numberToInt(s[1])
			got[ts] = n
		}
		return got
	}
	// 18:20:00 is 1793989200.
	const base = int64(1793989200)
	// Two streams, one in the first minute of the range and one 10 minutes later: no steps between them.
	disjoint := samples(exactLines("pod-a", "info", "paid order for eur", 0, 20) + exactLines("pod-b", "info", "paid order for eur", 600, 20))
	for ts := range disjoint {
		if ts > base+20 && ts < base+600 {
			t.Errorf("a sample at %d, in the gap between the streams: %v", ts, disjoint)
		}
	}
	if len(disjoint) != 4 || disjoint[base] != 10 || disjoint[base+600] != 10 {
		t.Errorf("samples %v, want two steps of each stream", disjoint)
	}
	// One stream with a quiet stretch: 20 lines at :00-:19, none at :20-:39, 20 at :40-:59.
	quiet := samples(exactLines("pod-a", "info", "paid order for eur", 0, 20) + exactLines("pod-a", "info", "paid order for eur", 40, 20))
	if want := map[int64]int{base: 10, base + 10: 10, base + 20: 0, base + 30: 0, base + 40: 10, base + 50: 10}; fmt.Sprint(quiet) != fmt.Sprint(want) {
		t.Errorf("samples %v, want %v", quiet, want)
	}
}

// Loki applies the 30-line rule after it merged equal patterns across streams: 20 and 20 lines in two pods are
// answered, as are 10 pods of 5 lines, and 15 and 14 are not.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternThirtyLineRuleCountsAcrossStreams(t *testing.T) {
	answered := func(streams ...int) int {
		var lines strings.Builder
		for i, n := range streams {
			lines.WriteString(exactLines(fmt.Sprintf("pod-%02d", i), "info", "paid order for eur", 0, n))
		}
		miner := newPatternMiner()
		miner.fillStep = 10
		if err := readPatternLines(strings.NewReader(lines.String()), miner, defaultLogRowLevels(), 1000); err != nil {
			t.Fatal(err)
		}
		entries := patternResultEntriesFromMaps(buildPatternResponse(miner, 100))
		start := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC).UnixNano()
		return len(dropPatternsBelowMinLines(entries, start, start+int64(time.Hour)))
	}
	for _, tc := range []struct {
		streams []int
		want    int
	}{
		{[]int{20, 20}, 1},
		{[]int{5, 5, 5, 5, 5, 5, 5, 5, 5, 5}, 1},
		{[]int{15, 14}, 0},
		{[]int{29}, 0},
		{[]int{30}, 1},
	} {
		if got := answered(tc.streams...); got != tc.want {
			t.Errorf("streams of %v lines: %d patterns answered, want %d", tc.streams, got, tc.want)
		}
	}
}

// A long range mined window by window is mined as the same lines are in one pass: the windows' inputs are mined
// together in the order the groups first appeared in, so the split of the range changes nothing.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternWindowInputsMineAsOnePass(t *testing.T) {
	const hour = int64(time.Hour)
	base := int64(1793966400) * int64(time.Second)
	var groups []patternGroupInput
	var timeline [][3]interface{}
	for i := 0; i < 240; i++ {
		first := base + int64(i)*hour/40
		verb := []string{"opened", "closed", "paid", "failed"}[i%4]
		line := fmt.Sprintf("order %d %s by user%d in region%d", i, verb, i%7, i%3)
		key := fmt.Sprintf("k%03d", i)
		groups = append(groups, patternGroupInput{Key: key, Level: []string{"info", "warn"}[i%2], Lo: line, Hi: line + " x", Count: 40, FirstNs: first})
		timeline = append(timeline, [3]interface{}{key, base/int64(time.Second) + int64(i)*90, 40})
	}
	inputs := func(windows int) []patternWindowInputs {
		out := make([]patternWindowInputs, windows)
		for i, g := range groups {
			w := &out[i*windows/len(groups)]
			w.Groups = append(w.Groups, g)
			w.Keys = append(w.Keys, g.Key)
			t := timeline[i]
			w.Timeline = append(w.Timeline, patternTimelineInput{Group: len(w.Keys) - 1, Bucket: t[1].(int64), Count: t[2].(int)})
		}
		return out
	}
	one, _ := json.Marshal(minePatternWindowInputs(inputs(1), 1000))
	six, _ := json.Marshal(minePatternWindowInputs(inputs(6), 1000))
	if string(one) != string(six) || len(one) < 100 {
		t.Errorf("6 windows mined differently from one pass:\n%s\n%s", six, one)
	}
	// The windows come in any order: the groups are mined by their first line, not by the window.
	reversed := inputs(6)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	if got, _ := json.Marshal(minePatternWindowInputs(reversed, 1000)); string(got) != string(one) {
		t.Error("the order of the windows changed the answer")
	}
}

// rowsReader makes JSON rows as it is read, and counts how many it made.
type rowsReader struct {
	total, made int
	pending     []byte
}

func (r *rowsReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.made == r.total {
			return 0, io.EOF
		}
		r.pending = []byte(fmt.Sprintf(`{"_time":"2026-11-06T18:20:00Z","_msg":"request %d done in %dms for user alice","_stream_id":"s","level":"info"}`+"\n", r.made, r.made%97))
		r.made++
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

// The lines are mined as they are read: a row is mined before the rows after it are read, to within the scanner's
// buffer, whatever the number of rows, so memory is that of the clusters.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternExactReadDoesNotBufferTheLines(t *testing.T) {
	src := &rowsReader{total: 300_000}
	miner := newPatternMiner()
	read := newPatternLineReader(miner, defaultLogRowLevels())
	mined, widest := 0, 0
	err := scanPatternRows(src, src.total, func(v *fj.Value) bool {
		mined++
		widest = max(widest, src.made-mined)
		return read(v)
	})
	if err != nil {
		t.Fatal(err)
	}
	if mined != src.total {
		t.Fatalf("%d rows mined of %d", mined, src.total)
	}
	// The scanner reads 64 KiB at a time, about 700 rows of this size.
	if widest > 1000 {
		t.Errorf("up to %d rows were read ahead of the one mined", widest)
	}
	if got := len(miner.AllClusters()); got > patternMaxClusters {
		t.Errorf("%d clusters", got)
	}
}

// An answer empty because no pattern holds 30 lines is an answer: the same request twice reads VictoriaLogs once.
// (An answer empty because VictoriaLogs had no lines is not cached: TestContract_Patterns_EmptyResultDoesNotPoisonCache.)
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternsQuietServiceAnswerIsCached(t *testing.T) {
	fake := &fakePatternCounts{count: `{"c":"10"}` + "\n", lines: exactLines("pod-a", "info", "paid order for eur", 0, 10)}
	p := newExactProxy(t, fake, 1000)
	ask := func() []string {
		q := url.Values{"query": {`{app="api"}`}, "start": {"1793988900"}, "end": {"1793990100"}}
		w := httptest.NewRecorder()
		p.handlePatterns(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil))
		var resp patternsResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || w.Code != http.StatusOK {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
		return patternAnswer(resp)
	}
	if got := ask(); len(got) != 0 {
		t.Fatalf("a pattern of 10 lines answered: %q", got)
	}
	calls := fake.calls.Load()
	if calls == 0 {
		t.Fatal("the first request read nothing")
	}
	if got := ask(); len(got) != 0 || fake.calls.Load() != calls {
		t.Errorf("second request answered %q after %d more VictoriaLogs calls", got, fake.calls.Load()-calls)
	}
}

// The sampling path fills the samples per stream cluster too, before equal patterns are summed: the lines come
// newest first, two streams in the two halves of the range leave no sample between them, and one stream's quiet
// step has a 0.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternSampledSamplesAreZeroFilledPerStreamCluster(t *testing.T) {
	newestFirst := func(parts ...string) []byte {
		var rows []string
		for _, part := range parts {
			rows = append(rows, strings.Split(strings.TrimSuffix(part, "\n"), "\n")...)
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i] > rows[j] })
		return []byte(strings.Join(rows, "\n") + "\n")
	}
	samples := func(body []byte, fill bool) map[int64]int {
		patterns, _, err := extractLogPatternsStreamFilled(strings.NewReader(string(body)), "10s", 100, defaultLogRowLevels(), fill)
		if err != nil || len(patterns) != 1 {
			t.Fatalf("%v: %+v", err, patterns)
		}
		got := map[int64]int{}
		for _, s := range patterns[0]["samples"].([][]interface{}) {
			ts, _ := numberToInt64(s[0])
			n, _ := numberToInt(s[1])
			got[ts] = n
		}
		return got
	}
	const base = int64(1793989200)
	disjoint := newestFirst(exactLines("pod-a", "info", "paid order for eur", 0, 20), exactLines("pod-b", "info", "paid order for eur", 600, 20))
	for _, fill := range []bool{false, true} {
		if got := samples(disjoint, fill); len(got) != 4 || got[base] != 10 || got[base+610] != 10 {
			t.Errorf("fill %v: samples %v, want the two steps of each stream and none between", fill, got)
		}
	}
	quiet := newestFirst(exactLines("pod-a", "info", "paid order for eur", 0, 20), exactLines("pod-a", "info", "paid order for eur", 40, 20))
	if got := samples(quiet, false); len(got) != 4 {
		t.Errorf("unfilled samples %v", got)
	}
	if got, want := samples(quiet, true), (map[int64]int{base: 10, base + 10: 10, base + 20: 0, base + 30: 0, base + 40: 10, base + 50: 10}); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("filled samples %v, want %v", got, want)
	}
}
