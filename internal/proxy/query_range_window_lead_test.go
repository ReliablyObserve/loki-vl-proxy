package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// leadWindowFakeVL is a recording VictoriaLogs for windowed log queries. Every
// row matches the query; a read returns the newest (or, sorted ascending, the
// oldest) `limit` rows of [start, end], and /hits counts the rows of its range.
// An optional latency model charges each call a fixed cost plus a cost per row
// VictoriaLogs scans (its last-N search reads up to 2×limit rows, then sorts),
// multiplied by the calls in flight when it starts: concurrent queries share
// VictoriaLogs' CPUs.
type leadWindowFakeVL struct {
	rows        []int64 // ascending
	undecodable map[int64]bool

	queryBase, queryPerRow time.Duration
	hitsBase, hitsPerRow   time.Duration
	failHits               bool

	inflight atomic.Int64
	mu       sync.Mutex
	reads    []leadWindowCall
	hits     []leadWindowCall
	other    []string
	rowsRead int64
}

type leadWindowCall struct {
	start, end int64
	limit      int
	returned   int
}

func (f *leadWindowFakeVL) between(start, end int64) []int64 {
	lo := sort.Search(len(f.rows), func(i int) bool { return f.rows[i] >= start })
	hi := sort.Search(len(f.rows), func(i int) bool { return f.rows[i] > end })
	return f.rows[lo:hi]
}

func (f *leadWindowFakeVL) charge(base, perRow time.Duration, scanned int) {
	if base == 0 && perRow == 0 {
		return
	}
	n := f.inflight.Add(1)
	defer f.inflight.Add(-1)
	time.Sleep(time.Duration(n) * (base + time.Duration(scanned)*perRow))
}

func (f *leadWindowFakeVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	start, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
	end, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
	switch r.URL.Path {
	case "/select/logsql/query":
		limit, _ := strconv.Atoi(r.Form.Get("limit"))
		in := f.between(start, end)
		scanned := len(in)
		if limit > 0 && scanned > 2*limit {
			scanned = 2 * limit
		}
		f.charge(f.queryBase, f.queryPerRow, scanned)
		forward := !strings.Contains(r.Form.Get("query"), "desc")
		picked := make([]int64, 0, min(len(in), max(limit, 0)))
		if forward {
			for i := 0; i < len(in) && (limit <= 0 || len(picked) < limit); i++ {
				picked = append(picked, in[i])
			}
		} else {
			for i := len(in) - 1; i >= 0 && (limit <= 0 || len(picked) < limit); i-- {
				picked = append(picked, in[i])
			}
		}
		f.mu.Lock()
		f.reads = append(f.reads, leadWindowCall{start: start, end: end, limit: limit, returned: len(picked)})
		f.rowsRead += int64(len(picked))
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/stream+json")
		for _, ts := range picked {
			if f.undecodable[ts] {
				_, _ = w.Write([]byte("{\"_time\":\"not a time\n"))
				continue
			}
			_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"line %d\",\"_stream\":\"{app=\\\"svc\\\"}\",\"app\":\"svc\"}\n",
				time.Unix(0, ts).UTC().Format(time.RFC3339Nano), ts)
		}
	case "/select/logsql/hits":
		in := f.between(start, end)
		f.charge(f.hitsBase, f.hitsPerRow, len(in))
		f.mu.Lock()
		f.hits = append(f.hits, leadWindowCall{start: start, end: end, returned: len(in)})
		f.mu.Unlock()
		if f.failHits {
			http.Error(w, "hits failed", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `{"hits":[{"fields":{"app":"svc"},"timestamps":["%s"],"values":[%d]}]}`,
			time.Unix(0, start).UTC().Format(time.RFC3339), len(in))
	default:
		f.mu.Lock()
		f.other = append(f.other, r.URL.Path)
		f.mu.Unlock()
		http.NotFound(w, r)
	}
}

func (f *leadWindowFakeVL) counts() (reads, hits, other int, rows int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reads), len(f.hits), len(f.other), f.rowsRead
}

func (f *leadWindowFakeVL) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads, f.hits, f.other, f.rowsRead = nil, nil, nil, 0
}

// leadWindowRows spreads rows evenly over each [from, to) span at the given
// rate per hour.
func leadWindowRows(spans ...leadWindowSpan) []int64 {
	var rows []int64
	for _, s := range spans {
		if s.perHour <= 0 || !s.to.After(s.from) {
			continue
		}
		gap := time.Hour / time.Duration(s.perHour)
		for ts := s.from; ts.Before(s.to); ts = ts.Add(gap) {
			rows = append(rows, ts.UnixNano())
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i] < rows[j] })
	return rows
}

type leadWindowSpan struct {
	from, to time.Time
	perHour  int
}

// newLeadWindowProxy runs the windowed log path with the proxy's production
// defaults (cmd/proxy flags): 1h aligned windows, adaptive parallelism 2..8,
// the /hits prefilter from 8 windows, stream-aware batching, a 10m freshness
// and 24h history windows.
func newLeadWindowProxy(t testing.TB, backendURL string, parallel int) *Proxy {
	t.Helper()
	cfg := Config{
		BackendURL:                      backendURL,
		Cache:                           cache.New(60*time.Second, 50000),
		LogLevel:                        "error",
		QueryRangeWindowingEnabled:      true,
		QueryRangeSplitInterval:         time.Hour,
		QueryRangeMaxParallel:           2,
		QueryRangeAdaptiveParallel:      true,
		QueryRangeParallelMin:           2,
		QueryRangeParallelMax:           8,
		QueryRangeLatencyTarget:         1500 * time.Millisecond,
		QueryRangeLatencyBackoff:        3 * time.Second,
		QueryRangeAdaptiveCooldown:      30 * time.Second,
		QueryRangeErrorBackoffThreshold: 0.02,
		QueryRangeFreshness:             10 * time.Minute,
		QueryRangeRecentCacheTTL:        0,
		QueryRangeHistoryCacheTTL:       24 * time.Hour,
		QueryRangePrefilterIndexStats:   true,
		QueryRangePrefilterMinWindows:   8,
		QueryRangeStreamAwareBatching:   true,
		QueryRangeExpensiveHitThreshold: 2000,
		QueryRangeExpensiveMaxParallel:  1,
		QueryRangeAlignWindows:          true,
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	if t, ok := t.(*testing.T); ok {
		t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	}
	if parallel > 0 {
		// The adaptive limit a busy proxy has reached (the daily run's proxies
		// read six to eight windows at once).
		p.queryRangeAdaptiveMu.Lock()
		p.queryRangeParallelCurrent = parallel
		p.queryRangeAdaptiveMu.Unlock()
	}
	return p
}

// leadWindowQuery asks the proxy for a log query and returns the timestamps
// of the lines it answered, newest first for backward and oldest first for
// forward (the order Loki returns them in after merging streams).
func leadWindowQuery(t testing.TB, p *Proxy, start, end time.Time, limit int, direction string) (int, []int64) {
	t.Helper()
	q := url.Values{}
	q.Set("query", `{app="svc"}`)
	q.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	q.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	q.Set("limit", strconv.Itoa(limit))
	if direction != "" {
		q.Set("direction", direction)
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	var body struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v: %s", err, rec.Body.String())
	}
	var got []int64
	for _, s := range body.Data.Result {
		for _, v := range s.Values {
			ts, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				t.Fatalf("timestamp %q: %v", v[0], err)
			}
			got = append(got, ts)
		}
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if direction != "forward" {
		for i, j := 0, len(got)-1; i < j; i, j = i+1, j-1 {
			got[i], got[j] = got[j], got[i]
		}
	}
	return rec.Code, got
}

// lokiNewestOrOldest is Loki's answer for a log query over rows that all match:
// the newest `limit` lines of [start, end] for backward, the oldest for
// forward (split_by_interval.go keeps reading splits until the limit is met,
// codec.go keeps `limit` entries of the merged splits). Rows VictoriaLogs
// returns but the proxy cannot decode are not lines.
func lokiNewestOrOldest(rows []int64, skip map[int64]bool, start, end int64, limit int, direction string) []int64 {
	var in []int64
	for _, ts := range rows {
		if ts >= start && ts <= end && !skip[ts] {
			in = append(in, ts)
		}
	}
	if direction != "forward" {
		for i, j := 0, len(in)-1; i < j; i, j = i+1, j-1 {
			in[i], in[j] = in[j], in[i]
		}
	}
	if limit > 0 && len(in) > limit {
		in = in[:limit]
	}
	return in
}

// historyEnd is an end inside VictoriaLogs' settled history (older than the
// freshness window), `past` after a whole hour, as the daily A/B run's ends are.
func historyEnd(past time.Duration) time.Time {
	return time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour).Add(past)
}

type leadWindowBenchCase struct {
	name      string
	perHour   int
	from, to  time.Duration // data span before the end (0 to = up to the end)
	limit     int
	past      time.Duration // end, past a whole hour
	ranges    []time.Duration
	direction string
	// sparseBefore adds this many rows per hour from 30h before the end up to
	// where the dense span starts.
	sparseBefore int
}

// The daily run's log shapes the windowed read plan decides: dense streams a
// level filter, a drop or a pattern filter selects (the newest window holds
// the limit), and the sparse and old-data controls where every window is read.
var leadWindowBenchCases = []leadWindowBenchCase{
	{name: "dense-limit1000", perHour: 3000, from: 30 * time.Hour, limit: 1000, past: 28 * time.Minute, ranges: []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}},
	{name: "nginx-limit100", perHour: 600, from: 30 * time.Hour, limit: 100, past: 28 * time.Minute, ranges: []time.Duration{time.Hour, 24 * time.Hour}},
	{name: "dense-tiny-newest", perHour: 3000, from: 30 * time.Hour, limit: 1000, past: time.Minute, ranges: []time.Duration{6 * time.Hour, 24 * time.Hour}},
	{name: "medium-two-windows", perHour: 1500, from: 30 * time.Hour, limit: 1000, past: 20 * time.Minute, ranges: []time.Duration{6 * time.Hour, 24 * time.Hour}},
	{name: "sparse-all", perHour: 20, from: 30 * time.Hour, limit: 1000, past: 28 * time.Minute, ranges: []time.Duration{6 * time.Hour, 24 * time.Hour}},
	{name: "sparse-old", perHour: 500, from: 18 * time.Hour, to: 12 * time.Hour, limit: 1000, past: 28 * time.Minute, ranges: []time.Duration{24 * time.Hour}},
	{name: "decaying", perHour: 1500, from: 40 * time.Minute, limit: 2000, past: 20 * time.Minute, ranges: []time.Duration{6 * time.Hour, 24 * time.Hour}, sparseBefore: 10},
	{name: "forward-dense", perHour: 3000, from: 30 * time.Hour, limit: 1000, past: 28 * time.Minute, ranges: []time.Duration{6 * time.Hour, 24 * time.Hour}, direction: "forward"},
}

// BenchmarkQueryRangeWindowLeadWindow measures a log query's wall time and
// VictoriaLogs work against the latency-modelled fake: the first request
// (cold) and six repeats each moved a minute back (shifted), as the daily A/B
// run sends them.
func BenchmarkQueryRangeWindowLeadWindow(b *testing.B) {
	for _, tc := range leadWindowBenchCases {
		end := historyEnd(tc.past)
		to := end
		if tc.to > 0 {
			to = end.Add(-tc.to)
		}
		rows := leadWindowRows(leadWindowSpan{from: end.Add(-tc.from), to: to, perHour: tc.perHour},
			leadWindowSpan{from: end.Add(-30 * time.Hour), to: end.Add(-tc.from), perHour: tc.sparseBefore})
		for _, rng := range tc.ranges {
			for _, phase := range []string{"cold", "shifted"} {
				b.Run(fmt.Sprintf("%s/%s/%s", tc.name, rng, phase), func(b *testing.B) {
					fake := &leadWindowFakeVL{rows: rows, queryBase: 2 * time.Millisecond, queryPerRow: 9 * time.Microsecond, hitsBase: 3 * time.Millisecond, hitsPerRow: 2 * time.Microsecond}
					srv := httptest.NewServer(fake)
					defer srv.Close()
					var elapsed time.Duration
					requests := 0
					var reads, hits int
					var rowsRead int64
					for i := 0; i < b.N; i++ {
						p := newLeadWindowProxy(b, srv.URL, 6)
						if phase == "shifted" {
							leadWindowQuery(b, p, end.Add(-rng), end, tc.limit, tc.direction)
						}
						fake.reset()
						runs := 1
						if phase == "shifted" {
							runs = 6
						}
						for k := 0; k < runs; k++ {
							e := end
							if phase == "shifted" {
								e = end.Add(-time.Duration(k+1) * time.Minute)
							}
							t0 := time.Now()
							code, got := leadWindowQuery(b, p, e.Add(-rng), e, tc.limit, tc.direction)
							elapsed += time.Since(t0)
							requests++
							if want := lokiNewestOrOldest(rows, nil, e.Add(-rng).UnixNano(), e.UnixNano(), tc.limit, tc.direction); code != http.StatusOK || !reflect.DeepEqual(got, want) {
								b.Fatalf("%s %s: status %d, %d lines, want %d", tc.name, rng, code, len(got), len(want))
							}
						}
						r, h, _, rr := fake.counts()
						reads, hits, rowsRead = reads+r, hits+h, rowsRead+rr
						_ = p.Shutdown(b.Context())
					}
					b.ReportMetric(float64(elapsed.Microseconds())/float64(requests)/1000, "ms/req")
					b.ReportMetric(float64(reads)/float64(requests), "reads/req")
					b.ReportMetric(float64(hits)/float64(requests), "hits/req")
					b.ReportMetric(float64(rowsRead)/float64(requests), "rows/req")
				})
			}
		}
	}
}

// The first window of a windowed log query is read alone, before the /hits
// prefilter and before any batch: when it holds the limit, VictoriaLogs reads
// one window and answers no /hits call. Every answer is Loki's newest (or,
// forward, oldest) `limit` lines, whatever the windows hold.
// conformance: semantics/query-range-window-lead-window
func TestQueryRangeWindow_LeadWindowFillsBeforeFanOut(t *testing.T) {
	type tc struct {
		name        string
		spans       func(end time.Time) []leadWindowSpan
		limit       int
		past        time.Duration
		rng         time.Duration
		direction   string
		undecodable int // the newest rows the proxy cannot decode
		failHits    bool
		reads, hits int // VictoriaLogs window reads and /hits calls; -1: not asserted
	}
	dense := func(perHour int) func(time.Time) []leadWindowSpan {
		return func(end time.Time) []leadWindowSpan {
			return []leadWindowSpan{{from: end.Add(-30 * time.Hour), to: end.Add(time.Hour), perHour: perHour}}
		}
	}
	for _, c := range []tc{
		{name: "dense 24h: the newest window holds the limit", spans: dense(3000), limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, reads: 1, hits: 0},
		{name: "dense 6h", spans: dense(3000), limit: 1000, past: 28 * time.Minute, rng: 6 * time.Hour, reads: 1, hits: 0},
		{name: "dense 1h, two windows", spans: dense(3000), limit: 1000, past: 28 * time.Minute, rng: time.Hour, reads: 1, hits: 0},
		{name: "limit 100 24h", spans: dense(600), limit: 100, past: 28 * time.Minute, rng: 24 * time.Hour, reads: 1, hits: 0},
		{name: "limit 1", spans: dense(600), limit: 1, past: 28 * time.Minute, rng: 24 * time.Hour, reads: 1, hits: 0},
		{name: "forward 24h: the oldest window holds the limit", spans: dense(3000), limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, direction: "forward", reads: 1, hits: 0},
		{name: "end a minute past the hour: the tiny newest window does not fill", spans: dense(3000), limit: 1000, past: time.Minute, rng: 24 * time.Hour, reads: 2, hits: 24},
		{name: "end a minute past the hour, 6h, no prefilter", spans: dense(3000), limit: 1000, past: time.Minute, rng: 6 * time.Hour, reads: -1, hits: 0},
		{name: "empty newest window", spans: func(end time.Time) []leadWindowSpan {
			return []leadWindowSpan{{from: end.Add(-30 * time.Hour), to: end.Add(-2 * time.Hour), perHour: 3000}}
		}, limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, reads: -1, hits: 24},
		{name: "limit above every line, 6h", spans: dense(20), limit: 1000, past: 28 * time.Minute, rng: 6 * time.Hour, reads: 7, hits: 0},
		{name: "limit above every line, 24h", spans: dense(20), limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, reads: 25, hits: 24},
		{name: "old data only", spans: func(end time.Time) []leadWindowSpan {
			return []leadWindowSpan{{from: end.Add(-18 * time.Hour), to: end.Add(-12 * time.Hour), perHour: 500}}
		}, limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, reads: -1, hits: 24},
		{name: "no line at all", spans: func(time.Time) []leadWindowSpan { return nil }, limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, reads: 1, hits: 24},
		{name: "undecodable lines in a newest window below the limit", spans: dense(3000), limit: 1000, past: 10 * time.Minute, rng: 24 * time.Hour, undecodable: 7, reads: 2, hits: 24},
		{name: "prefilter failure after the first window reads every window", spans: dense(20), limit: 1000, past: 28 * time.Minute, rng: 24 * time.Hour, failHits: true, reads: 25, hits: -1},
		{name: "eight windows: the prefilter counts the first one", spans: dense(3000), limit: 1000, past: time.Minute, rng: 7 * time.Hour, reads: 2, hits: 7},
	} {
		t.Run(c.name, func(t *testing.T) {
			end := historyEnd(c.past)
			rows := leadWindowRows(c.spans(end)...)
			skip := map[int64]bool{}
			for i := 0; i < c.undecodable && i < len(rows); i++ {
				skip[rows[len(rows)-1-i]] = true
			}
			fake := &leadWindowFakeVL{rows: rows, undecodable: skip, failHits: c.failHits}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			p := newLeadWindowProxy(t, srv.URL, 6)
			code, got := leadWindowQuery(t, p, end.Add(-c.rng), end, c.limit, c.direction)
			want := lokiNewestOrOldest(rows, skip, end.Add(-c.rng).UnixNano(), end.UnixNano(), c.limit, c.direction)
			if code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("status %d, %d lines, want %d lines (Loki's)", code, len(got), len(want))
			}
			reads, hits, other, _ := fake.counts()
			if c.reads >= 0 && reads != c.reads {
				t.Errorf("%d window reads, want %d", reads, c.reads)
			}
			if c.hits >= 0 && hits != c.hits {
				t.Errorf("%d /hits calls, want %d", hits, c.hits)
			}
			if other != 0 {
				t.Errorf("unexpected VictoriaLogs calls: %v", fake.other)
			}
		})
	}
}

// The daily A/B run asks 6h and then 24h with the same end: the 24h request
// takes its first window from the window cache and calls VictoriaLogs for
// nothing. A newest window inside -query-range-freshness is never kept, so
// a request ending now reads it every time, still one window when it fills.
// conformance: semantics/query-range-window-lead-window
func TestQueryRangeWindow_LeadWindowCachedAndNearNow(t *testing.T) {
	end := historyEnd(28 * time.Minute)
	rows := leadWindowRows(leadWindowSpan{from: end.Add(-30 * time.Hour), to: end.Add(time.Hour), perHour: 3000})
	fake := &leadWindowFakeVL{rows: rows}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	p := newLeadWindowProxy(t, srv.URL, 6)
	for _, rng := range []time.Duration{6 * time.Hour, 24 * time.Hour} {
		fake.reset()
		code, got := leadWindowQuery(t, p, end.Add(-rng), end, 1000, "")
		if want := lokiNewestOrOldest(rows, nil, end.Add(-rng).UnixNano(), end.UnixNano(), 1000, ""); code != http.StatusOK || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: status %d, %d lines, want %d", rng, code, len(got), len(want))
		}
		reads, hits, _, _ := fake.counts()
		wantReads := 1
		if rng == 24*time.Hour {
			wantReads = 0
		}
		if reads != wantReads || hits != 0 {
			t.Errorf("%s: %d reads and %d /hits calls, want %d and 0", rng, reads, hits, wantReads)
		}
	}

	// 1000 lines a minute: the newest window (since the last whole hour) holds
	// 100 lines unless it is under six seconds old.
	now := time.Now()
	nowRows := leadWindowRows(
		leadWindowSpan{from: now.Add(-30 * time.Hour), to: now.Add(-2 * time.Hour), perHour: 600},
		leadWindowSpan{from: now.Add(-2 * time.Hour), to: now, perHour: 60000},
	)
	nowFake := &leadWindowFakeVL{rows: nowRows}
	nowSrv := httptest.NewServer(nowFake)
	defer nowSrv.Close()
	np := newLeadWindowProxy(t, nowSrv.URL, 6)
	for run := 0; run < 2; run++ {
		nowFake.reset()
		// Two requests a millisecond apart: the response cache cannot answer the second.
		e := now.Add(-time.Duration(run) * time.Millisecond)
		code, got := leadWindowQuery(t, np, e.Add(-24*time.Hour), e, 100, "")
		if want := lokiNewestOrOldest(nowRows, nil, e.Add(-24*time.Hour).UnixNano(), e.UnixNano(), 100, ""); code != http.StatusOK || !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: status %d, %d lines, want %d", run, code, len(got), len(want))
		}
		if e.Sub(e.Truncate(time.Hour)) < 10*time.Second {
			continue // the newest window may not hold the limit yet
		}
		if reads, hits, _, _ := nowFake.counts(); reads != 1 || hits != 0 {
			t.Errorf("run %d: %d reads and %d /hits calls, want the newest window read again (never kept) and no /hits", run, reads, hits)
		}
	}
}

// Differential: random streams, limits, ranges, ends and directions, the
// windowed answer against Loki's newest or oldest `limit` lines. A first
// window that holds the limit costs one read and no /hits call.
// conformance: semantics/query-range-window-lead-window
func TestQueryRangeWindow_LeadWindowMatchesLokiDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20261104))
	for i := 0; i < 40; i++ {
		leadWindowCheck(t, uint16(rng.Intn(4000)), uint16(1+rng.Intn(1500)), uint8(rng.Intn(60)), uint8(1+rng.Intn(30)), uint8(rng.Intn(20)), uint8(rng.Intn(20)), rng.Intn(2) == 0)
	}
}

func FuzzQueryRangeWindowLeadWindow(f *testing.F) {
	f.Add(uint16(3000), uint16(1000), uint8(28), uint8(24), uint8(0), uint8(0), false)
	f.Add(uint16(3000), uint16(1000), uint8(1), uint8(6), uint8(0), uint8(0), true)
	f.Add(uint16(20), uint16(1000), uint8(28), uint8(24), uint8(0), uint8(0), false)
	f.Add(uint16(500), uint16(100), uint8(0), uint8(9), uint8(5), uint8(3), false)
	f.Fuzz(func(t *testing.T, perHour, limit uint16, pastMin, rangeHours, gapStartHours, gapHours uint8, forward bool) {
		leadWindowCheck(t, perHour%5000, 1+limit%2000, pastMin%60, 1+rangeHours%30, gapStartHours%30, gapHours%30, forward)
	})
}

// leadWindowCheck runs one query over a stream of perHour rows with an empty
// gap [end-gapStart-gap, end-gapStart) and checks the answer against Loki's
// and the read plan's bound.
func leadWindowCheck(t *testing.T, perHour, limit uint16, pastMin, rangeHours, gapStartHours, gapHours uint8, forward bool) {
	t.Helper()
	end := historyEnd(time.Duration(pastMin) * time.Minute)
	gapTo := end.Add(-time.Duration(gapStartHours) * time.Hour)
	gapFrom := gapTo.Add(-time.Duration(gapHours) * time.Hour)
	rows := leadWindowRows(
		leadWindowSpan{from: end.Add(-32 * time.Hour), to: gapFrom, perHour: int(perHour)},
		leadWindowSpan{from: gapTo, to: end.Add(time.Hour), perHour: int(perHour)},
	)
	direction := ""
	if forward {
		direction = "forward"
	}
	fake := &leadWindowFakeVL{rows: rows}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	p := newLeadWindowProxy(t, srv.URL, 6)
	start := end.Add(-time.Duration(rangeHours) * time.Hour)
	code, got := leadWindowQuery(t, p, start, end, int(limit), direction)
	want := lokiNewestOrOldest(rows, nil, start.UnixNano(), end.UnixNano(), int(limit), direction)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("perHour=%d limit=%d past=%dm range=%dh gap=%dh+%dh forward=%v: status %d, %d lines, want %d",
			perHour, limit, pastMin, rangeHours, gapStartHours, gapHours, forward, code, len(got), len(want))
	}
	reads, hits, other, _ := fake.counts()
	windows := splitQueryRangeWindowsWithOptions(start.UnixNano(), end.UnixNano(), time.Hour, direction, true)
	if reads > len(windows) || other != 0 {
		t.Fatalf("%d reads for %d windows, other calls %v", reads, len(windows), fake.other)
	}
	if len(windows) > 1 && len(lokiNewestOrOldest(rows, nil, windows[0].startNs, windows[0].endNs, int(limit), direction)) == int(limit) && (reads != 1 || hits != 0) {
		t.Fatalf("the first window holds the limit, yet %d reads and %d /hits calls", reads, hits)
	}
}
