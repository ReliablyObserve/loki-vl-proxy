package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// windowStubVL is a VictoriaLogs stub whose log rows (time and line) and
// metric steps a test sets: rows are returned as asked (the proxy does not
// look at their times), a stats_query_range answer holds one value per
// element of steps at start, start+60s, ...
type windowStubVL struct {
	mu    sync.Mutex
	rows  []windowStubRow
	steps []int          // counts per step
	calls map[string]int // VictoriaLogs requests by path
}

type windowStubRow struct {
	at   time.Time
	line string
}

func (s *windowStubVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls == nil {
		s.calls = map[string]int{}
	}
	s.calls[r.URL.Path]++
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/select/logsql/query":
		var b strings.Builder
		for _, row := range s.rows {
			fmt.Fprintf(&b, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":%q,"app":"api"}`+"\n", row.at.UTC().Format(time.RFC3339Nano), row.line)
		}
		_, _ = w.Write([]byte(b.String()))
	case "/select/logsql/stats_query_range":
		_ = r.ParseForm()
		start, _ := parseLokiTimeToUnixNano(r.Form.Get("start"))
		var b strings.Builder
		for i, v := range s.steps {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, start/int64(time.Second)+int64(i)*60, v)
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[%s]}]}}`, b.String())
	default:
		_, _ = w.Write([]byte(`{"values":[]}`))
	}
}

// lines builds n rows of size bytes each, spread over [from, to).
func windowRows(n, size int, from, to time.Time, tag string) []windowStubRow {
	rows := make([]windowStubRow, 0, n)
	for i := 0; i < n; i++ {
		at := from.Add(to.Sub(from) * time.Duration(i) / time.Duration(n+1)).Add(time.Second)
		line := fmt.Sprintf("%s %04d ", tag, i)
		rows = append(rows, windowStubRow{at: at, line: line + strings.Repeat("x", size-len(line))})
	}
	return rows
}

// TestLokiStats_MetricBytesUseTheQueriedWindow: a metric query's bytes
// processed in Loki's accounting come from the log answer the proxy computed
// for the same expression over the same window (its lines' bytes exactly), not
// from the last mean line size of the selector, which another window or filter
// may have set. The part of the metric window no log answer covers (Loki's
// metric lookback of one range before the start) is priced at that answer's
// mean, so it can be off by the difference of its line sizes: a sparse stream
// whose lookback minute holds much longer lines than the log window is
// estimated 39% low here (the figure of the CI run that motivated this), a
// lookback minute of the same sizes is exact.
//
// conformance: query-stats-envelope
func TestLokiStats_MetricBytesUseTheQueriedWindow(t *testing.T) {
	end := statsTestNow().Truncate(time.Minute).Add(-time.Minute)
	start := end.Add(-5 * time.Minute)
	logWin := func(q string, from, to time.Time) url.Values {
		return url.Values{"query": {q}, "start": {fmt.Sprint(from.Unix())}, "end": {fmt.Sprint(to.Unix())}, "step": {"60"}, "limit": {"1000"}, "direction": {"backward"}}
	}
	metricWin := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {fmt.Sprint(start.Unix())}, "end": {fmt.Sprint(end.Unix())}, "step": {"60"}}
	}
	within := func(got, want int64, tol float64) bool {
		return float64(got) >= float64(want)*(1-tol) && float64(got) <= float64(want)*(1+tol)
	}
	// stepsOf spreads n lines over the six steps start..end, lookback first.
	stepsOf := func(lookback, rest int) []int {
		steps := []int{lookback}
		for i := 0; i < 5; i++ {
			steps = append(steps, rest/5)
		}
		return steps
	}
	type phase struct {
		name         string
		rows         []windowStubRow // the log answer of the expression
		logFrom      time.Time
		interleaved  []windowStubRow // another log answer on the same selector, between the two, over another window
		steps        []int
		actualBytes  int64 // what Loki's window holds
		wantBytes    int64 // the estimate, exactly
		wantTolerant float64
	}
	short, long := 62, 300
	for _, tc := range []phase{
		{
			// The log window is the metric window less its lookback minute, which holds four long lines.
			name: "sparse, lookback minute of longer lines (documented miss)",
			rows: windowRows(20, short, start, end, "in"), logFrom: start, steps: stepsOf(4, 20),
			actualBytes: int64(20*short + 4*long), wantBytes: int64(20*short + 4*short), wantTolerant: 0.40,
		},
		{
			name: "lookback minute of the same sizes",
			rows: windowRows(20, short, start, end, "in"), logFrom: start, steps: stepsOf(4, 20),
			actualBytes: int64(24 * short), wantBytes: int64(24 * short), wantTolerant: 0.01,
		},
		{
			name:    "log window covers the lookback: exact on mixed sizes",
			rows:    append(windowRows(4, long, start.Add(-time.Minute), start, "lb"), windowRows(20, short, start, end, "in")...),
			logFrom: start.Add(-time.Minute), steps: stepsOf(4, 20),
			actualBytes: int64(20*short + 4*long), wantBytes: int64(20*short + 4*long), wantTolerant: 0.01,
		},
		{
			// Another log answer of the selector, of long lines over another window, ran in between: the selector's
			// mean is now 400, the record of the expression still gives the window's own bytes.
			name: "another window's log answer in between does not set the estimate",
			rows: windowRows(24, short, start.Add(-time.Minute), end, "in"), logFrom: start.Add(-time.Minute),
			interleaved: windowRows(50, 400, end.Add(-90*time.Minute), end.Add(-80*time.Minute), "ot"),
			steps:       stepsOf(4, 20), actualBytes: int64(24 * short), wantBytes: int64(24 * short), wantTolerant: 0.01,
		},
		{
			// A dense window: a hundred lines of 1,000 bytes with a fifth of them in the lookback minute.
			name: "dense window of long lines",
			rows: windowRows(400, 1000, start.Add(-time.Minute), end, "in"), logFrom: start.Add(-time.Minute),
			steps: stepsOf(80, 400), actualBytes: 480 * 1000, wantBytes: 480 * 1000, wantTolerant: 0.30,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &windowStubVL{rows: tc.rows, steps: tc.steps}
			vl := httptest.NewServer(stub)
			defer vl.Close()
			h := newQueryStatsTestProxyMode(t, vl.URL, "", QueryStatsModeLoki)
			const filter = ` != "win-nonce"`
			// The log answer of the panel, then (optionally) another window's, then the metric query.
			_ = serveQueryStats(t, h, "/loki/api/v1/query_range", logWin(`{app="api"}`+filter, tc.logFrom, end), nil)
			if tc.interleaved != nil {
				stub.mu.Lock()
				stub.rows = tc.interleaved
				stub.mu.Unlock()
				_ = serveQueryStats(t, h, "/loki/api/v1/query_range", logWin(`{app="api"} != "other"`, end.Add(-90*time.Minute), end.Add(-80*time.Minute)), nil)
			}
			got := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", metricWin(`sum(count_over_time({app="api"}`+filter+` [1m]))`), nil))
			if int(got.Summary.Lines) != sumInts(tc.steps) {
				t.Fatalf("lines %d, want the answer's %d", got.Summary.Lines, sumInts(tc.steps))
			}
			if got.Summary.Bytes != tc.wantBytes {
				t.Errorf("bytes %d, want %d", got.Summary.Bytes, tc.wantBytes)
			}
			if !within(got.Summary.Bytes, tc.actualBytes, tc.wantTolerant) {
				t.Errorf("bytes %d for %d lines, the window holds %d bytes (tolerance %.0f%%)", got.Summary.Bytes, got.Summary.Lines, tc.actualBytes, tc.wantTolerant*100)
			}
		})
	}
}

func sumInts(v []int) (n int) {
	for _, x := range v {
		n += x
	}
	return n
}

// TestMeanLineTable_WindowRecord is the edge-case table of the record of an
// expression's lines over a window: the part of it inside the queried window,
// by the share of its time, nothing outside it or past its expiry, and the
// selector and tenant means unchanged by it.
//
// conformance: query-stats-envelope
func TestMeanLineTable_WindowRecord(t *testing.T) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
	sel := &logqlpkg.StreamSelector{Matchers: []logqlpkg.LabelMatcher{{Name: "app", Op: logqlpkg.MatchEq, Value: "api"}}}
	m := int64(time.Minute)
	const lines, size = 10, int64(100)
	keys := p.meanLineKey(r, sel, `{app="api"}`)
	p.meanLines.store(keys, lines*size, lines, 10*m, 20*m)
	for _, tc := range []struct {
		name         string
		from, to     int64
		wantOK       bool
		wantLines    float64
		wantBytes    float64
		wantMean     float64
		expireRecord bool
	}{
		{"window inside the metric window", 5 * m, 25 * m, true, 10, 1000, 100, false},
		{"equal windows", 10 * m, 20 * m, true, 10, 1000, 100, false},
		{"metric window covers the second half", 15 * m, 30 * m, true, 5, 500, 100, false},
		{"metric window covers a fifth", 18 * m, 40 * m, true, 2, 200, 100, false},
		{"metric window ends where the record starts", 0, 10 * m, false, 0, 0, 0, false},
		{"metric window starts where the record ends", 20 * m, 30 * m, false, 0, 0, 0, false},
		{"metric window far away", 100 * m, 110 * m, false, 0, 0, 0, false},
		{"expired record", 5 * m, 25 * m, false, 0, 0, 0, true},
	} {
		if tc.expireRecord {
			p.meanLines.mu.Lock()
			v := p.meanLines.m[keys.expr]
			v.expires = 0
			p.meanLines.m[keys.expr] = v
			p.meanLines.mu.Unlock()
		}
		b, l, mean, ok := p.meanLines.within(keys.expr, tc.from, tc.to)
		if ok != tc.wantOK || (ok && (b != tc.wantBytes || l != tc.wantLines || mean != tc.wantMean)) {
			t.Errorf("%s: bytes %v lines %v mean %v ok %t, want %v %v %v %t", tc.name, b, l, mean, ok, tc.wantBytes, tc.wantLines, tc.wantMean, tc.wantOK)
		}
	}
	if mean, ok := p.meanLines.mean(keys); !ok || mean != float64(size) {
		t.Fatalf("selector mean %v %t, want %d", mean, ok, size)
	}
	// A record without a window, or an empty one, keeps no expression entry.
	other := p.meanLineKey(r, sel, `{app="api"} |= "x"`)
	p.meanLines.store(other, 1000, 10, 5*m, 5*m)
	if _, _, _, ok := p.meanLines.within(other.expr, 0, 10*m); ok {
		t.Fatal("an empty window was kept")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		k := p.meanLineKey(r, sel, `{app="api"}`)
		p.meanLines.store(k, 1000, 10, 10*m, 20*m)
		_, _, _, _ = p.meanLines.within(k.expr, 5*m, 25*m)
		_, _ = p.meanLines.mean(k)
	}); allocs != 0 {
		t.Fatalf("record store and read allocate %v times", allocs)
	}
}

// TestEntriesWindow_Table is the table of the window a log answer holds every
// line of its expression for.
//
// conformance: query-stats-envelope
func TestEntriesWindow_Table(t *testing.T) {
	m := int64(time.Minute)
	for _, tc := range []struct {
		name             string
		backward         bool
		limit, n         int64
		hasTS            bool
		tsMin, tsMax     int64
		wantFrom, wantTo int64
		wantOK           bool
	}{
		{"under the limit: the whole window", true, 1000, 24, true, 3 * m, 8 * m, 0, 10 * m, true},
		{"empty answer: the whole window", true, 1000, 0, false, 0, 0, 0, 10 * m, true},
		{"backward, limit filled: from the oldest entry", true, 100, 100, true, 7 * m, 9 * m, 7 * m, 10 * m, true},
		{"forward, limit filled: up to the newest entry", false, 100, 100, true, 1 * m, 3 * m, 0, 3*m + 1, true},
		{"limit filled, times unread", true, 100, 100, false, 0, 0, 0, 0, false},
		{"limit zero reads the whole window", true, 0, 5, true, 1 * m, 2 * m, 0, 10 * m, true},
	} {
		w := &queryStatsWriter{}
		w.sreq, w.sreqRead = statsRequest{start: 0, end: 10 * m, limit: tc.limit, backward: tc.backward, ok: true}, true
		w.entries = entryCounter{n: tc.n, hasTS: tc.hasTS, tsMin: tc.tsMin, tsMax: tc.tsMax}
		from, to, ok := w.entriesWindow()
		if ok != tc.wantOK || (ok && (from != tc.wantFrom || to != tc.wantTo)) {
			t.Errorf("%s: [%d, %d] %t, want [%d, %d] %t", tc.name, from, to, ok, tc.wantFrom, tc.wantTo, tc.wantOK)
		}
	}
	w := &queryStatsWriter{}
	w.sreq, w.sreqRead = statsRequest{}, true
	if _, _, ok := w.entriesWindow(); ok {
		t.Error("an unread request gave a window")
	}
}

// FuzzMeanLineWindow: for any record and queried window the part of the record
// taken is never more than the record, is zero exactly when the windows do not
// overlap, its mean is the record's whatever the share, and the window a log
// answer covers is empty or lies inside its request's window.
//
// conformance: query-stats-envelope
func FuzzMeanLineWindow(f *testing.F) {
	f.Add(int64(100), int64(10), int64(0), int64(60), int64(30), int64(90), int64(1000), int64(50), true)
	f.Add(int64(1), int64(1), int64(5), int64(5), int64(5), int64(5), int64(0), int64(0), false)
	f.Fuzz(func(t *testing.T, bytes, lines, rFrom, rTo, qFrom, qTo, limit, n int64, backward bool) {
		const bound = 1 << 50
		for _, v := range []int64{bytes, lines, rFrom, rTo, qFrom, qTo, limit, n} {
			if v < 0 || v > bound {
				t.Skip()
			}
		}
		tbl := &meanLineTable{}
		var keys meanLineKeys
		keys.hasExpr, keys.expr[0] = true, 1
		tbl.store(keys, bytes, lines, rFrom, rTo)
		b, l, mean, ok := tbl.within(keys.expr, qFrom, qTo)
		overlap := min64(qTo, rTo) - max(qFrom, rFrom)
		stored := bytes > 0 && lines > 0 && rTo > rFrom
		if want := stored && overlap > 0; ok != want {
			t.Fatalf("ok %t, want %t (record [%d,%d] window [%d,%d])", ok, want, rFrom, rTo, qFrom, qTo)
		}
		if ok {
			if l > float64(lines) || b > float64(bytes) || l <= 0 || b <= 0 {
				t.Fatalf("part %v lines %v bytes of %d lines %d bytes", l, b, lines, bytes)
			}
			if mean != float64(bytes)/float64(lines) {
				t.Fatalf("mean %v, want %v", mean, float64(bytes)/float64(lines))
			}
		}
		w := &queryStatsWriter{}
		w.sreq, w.sreqRead = statsRequest{start: qFrom, end: qTo, limit: limit, backward: backward, ok: qTo >= qFrom}, true
		w.entries = entryCounter{n: n, hasTS: true, tsMin: rFrom, tsMax: rTo}
		if from, to, ok := w.entriesWindow(); ok && (from < qFrom || to > qTo+1 || to < from) {
			t.Fatalf("window [%d, %d] outside [%d, %d]", from, to, qFrom, qTo)
		}
	})
}
