package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The tests in this file pin how a windowed log query reads its windows
// (semantics/query-range-window-full-limit-reads). Like Loki's own interval
// split (pkg/querier/queryrange/splitters.go: every split LokiRequest keeps
// Limit: r.Limit; split_by_interval.go: splits are taken newest first for a
// backward query until the limit is reached), every window is read with the
// request's whole limit. A window's cached rows then depend only on the
// window and the limit, so a moved, longer or shorter range, and a parallel
// or a sequential batch layout, reuse the windows an earlier request read.
// The reference for every answer is Loki's: the newest (backward) or oldest
// (forward) `limit` lines in the range.

// windowReuseRow is one stored line of the fake VictoriaLogs.
type windowReuseRow struct {
	ts        int64
	match     bool // matches the query's filters
	malformed bool // VictoriaLogs returns a line the proxy cannot decode
}

// The kinds of line a fake VictoriaLogs stores.
const (
	// windowReuseJSON lines hold the O1 keys (pipeline, k8s_namespace_name)
	// in a JSON line; the matching ones carry O1's values.
	windowReuseJSON = iota
	// windowReuseStoredUser lines all carry a stored field user=u1, which
	// VictoriaLogs matches for `| user="u1"`; the matching lines are plain
	// text (user is their structured metadata, as Loki keeps it), the others
	// JSON lines with a user key, which Loki does not see without a parser,
	// so the Loki-compatible profile drops them after the read and refills
	// the window from further pages.
	windowReuseStoredUser
)

// windowReuseVL is a recording fake VictoriaLogs: /select/logsql/query
// answers the rows of [start, end] in the requested order up to the limit,
// /select/logsql/hits counts the rows of the window times hitsPerRow (above
// the expensive-window threshold, batches turn sequential).
type windowReuseVL struct {
	rows       []windowReuseRow // ascending ts
	hitsPerRow int64
	filter     bool // apply the pushed-down filter (only matching rows)
	kind       int

	mu       sync.Mutex
	reads    []windowReuseRead
	rowsRead int
	hits     int
	srv      *httptest.Server
}

type windowReuseRead struct {
	start, end string
	limit      int
}

func parseWindowReuseTime(s string) int64 {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0
	}
	return ts.UnixNano()
}

func newWindowReuseVL(t testing.TB, rows []windowReuseRow, hitsPerRow int64, filter bool) *windowReuseVL {
	t.Helper()
	vl := &windowReuseVL{rows: rows, hitsPerRow: hitsPerRow, filter: filter}
	vl.srv = httptest.NewServer(http.HandlerFunc(vl.serve))
	t.Cleanup(vl.srv.Close)
	return vl
}

func (vl *windowReuseVL) inRange(start, end int64) []windowReuseRow {
	lo := sort.Search(len(vl.rows), func(i int) bool { return vl.rows[i].ts >= start })
	hi := sort.Search(len(vl.rows), func(i int) bool { return vl.rows[i].ts > end })
	return vl.rows[lo:hi]
}

func (vl *windowReuseVL) serve(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	start, end := parseWindowReuseTime(r.Form.Get("start")), parseWindowReuseTime(r.Form.Get("end"))
	rows := vl.inRange(start, end)
	switch r.URL.Path {
	case "/select/logsql/hits":
		vl.mu.Lock()
		vl.hits++
		vl.mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"hits":[{"fields":{},"timestamps":[%q],"values":[%d],"total":%d}]}`,
			time.Unix(0, start).UTC().Format(time.RFC3339), int64(len(rows))*vl.hitsPerRow, int64(len(rows))*vl.hitsPerRow)
	case "/select/logsql/query":
		limit, _ := strconv.Atoi(r.Form.Get("limit"))
		desc := strings.Contains(r.Form.Get("query"), "_time desc")
		n := 0
		for i := range rows {
			row := rows[i]
			if desc {
				row = rows[len(rows)-1-i]
			}
			if n >= limit {
				break
			}
			if vl.filter && !row.match {
				continue
			}
			n++
			if row.malformed {
				_, _ = w.Write([]byte(`{"_time":"not-a-time","_msg":"undecodable"}` + "\n"))
				continue
			}
			fields := map[string]string{
				"_time":   time.Unix(0, row.ts).UTC().Format(time.RFC3339Nano),
				"_stream": `{env="production"}`,
				"env":     "production",
			}
			switch vl.kind {
			case windowReuseStoredUser:
				fields["user"] = "u1"
				fields["_msg"] = fmt.Sprintf("plain line %d", row.ts)
				if !row.match {
					fields["_msg"] = fmt.Sprintf(`{"msg":"json line %d","user":"u1"}`, row.ts)
				}
			case windowReuseJSON:
				pipeline, namespace := "logs/loki", "default"
				if row.match {
					pipeline, namespace = "metrics/prometheus", "monitoring"
				}
				fields["_msg"] = fmt.Sprintf(`{"pipeline":%q,"k8s_namespace_name":%q,"ts":%d}`, pipeline, namespace, row.ts)
			}
			line, _ := json.Marshal(fields)
			_, _ = w.Write(append(line, '\n'))
		}
		vl.mu.Lock()
		vl.reads = append(vl.reads, windowReuseRead{start: r.Form.Get("start"), end: r.Form.Get("end"), limit: limit})
		vl.rowsRead += n
		vl.mu.Unlock()
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// snapshot returns the window reads and rows read so far.
func (vl *windowReuseVL) snapshot() ([]windowReuseRead, int) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return append([]windowReuseRead(nil), vl.reads...), vl.rowsRead
}

// reference is Loki's answer: the timestamps of the newest (backward) or
// oldest (forward) `limit` decodable lines in [start, end], in answer order.
func (vl *windowReuseVL) reference(start, end int64, limit int, forward, filtered bool) []int64 {
	rows := vl.inRange(start, end)
	var out []int64
	for i := range rows {
		row := rows[i]
		if !forward {
			row = rows[len(rows)-1-i]
		}
		if len(out) >= limit {
			break
		}
		if row.malformed || (filtered && !row.match) {
			continue
		}
		out = append(out, row.ts)
	}
	return out
}

// windowReuseConfig is the production default windowing configuration
// (cmd/proxy flags) on top of a shared cache.
func windowReuseConfig(backendURL string, c *cache.Cache, lokiProfile bool) Config {
	cfg := Config{
		BackendURL:                      backendURL,
		Cache:                           c,
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
		LabelFilterRefillMaxPages:       DefaultLabelFilterRefillMaxPages,
		EmitStructuredMetadata:          true,
	}
	if lokiProfile {
		cfg.LabelStyle = LabelStyleUnderscores
		cfg.MetadataFieldMode = MetadataFieldModeTranslated
	}
	return cfg
}

func newWindowReuseProxy(t testing.TB, cfg Config) *Proxy {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// newWindowReuseCache is a cache closed with the test.
func newWindowReuseCache(t testing.TB) *cache.Cache {
	c := cache.New(time.Minute, 100000)
	t.Cleanup(c.Close)
	return c
}

// windowReuseQuery asks p for a log query and returns the answer's
// timestamps in answer order (newest first for backward).
func windowReuseQuery(t testing.TB, p *Proxy, query string, start, end int64, limit int, forward bool) []int64 {
	t.Helper()
	direction := "backward"
	if forward {
		direction = "forward"
	}
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=%d&direction=%s",
		url.QueryEscape(query), start, end, limit, direction), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("query_range %s [%d, %d] limit %d: status %d: %s", query, start, end, limit, rec.Code, rec.Body.String())
	}
	var payload struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode query_range: %v", err)
	}
	var got []int64
	for _, stream := range payload.Data.Result {
		for _, value := range stream.Values {
			ts, err := strconv.ParseInt(value[0], 10, 64)
			if err != nil {
				t.Fatalf("timestamp %q: %v", value[0], err)
			}
			got = append(got, ts)
		}
	}
	sort.Slice(got, func(i, j int) bool {
		if forward {
			return got[i] < got[j]
		}
		return got[i] > got[j]
	})
	return got
}

func equalTimestamps(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// windowReuseRows stores a line every step in [from, to), half a second off
// the step grid so no line sits on a request bound; malformedEvery > 0 makes
// every n-th line one VictoriaLogs returns but the proxy cannot decode, and
// matchEvery > 1 makes only every n-th line match the filters.
func windowReuseRows(from, to time.Time, step time.Duration, malformedEvery, matchEvery int) []windowReuseRow {
	var rows []windowReuseRow
	i := 0
	for ts := from.Add(500 * time.Millisecond); ts.Before(to); ts = ts.Add(step) {
		row := windowReuseRow{ts: ts.UnixNano(), match: matchEvery <= 1 || i%matchEvery == 0}
		if malformedEvery > 0 && i%malformedEvery == malformedEvery-1 {
			row.malformed = true
		}
		rows = append(rows, row)
		i++
	}
	return rows
}

const o1Query = "{env=\"production\"} | json | pipeline=`metrics/prometheus` | k8s_namespace_name=`monitoring`"

// TestQueryRangeWindow_ShiftedRangesReuseHistoryWindows is the warm repeat of
// the O1 A/B shape (loki-profile-otel-metadata): the Loki-compatible profile,
// a json query whose filters VictoriaLogs runs, a line every 3 s, and the A/B
// matrix order: 1h, 3h, 6h and 24h, seven runs each, every run's window moved
// a minute back. The 24h windows are expensive by their /hits estimate, so
// they are read one at a time; the shorter ranges read two at a time. Every
// history window is read from VictoriaLogs once: the 24h runs find every
// window they need in the cache (the newest one partial, already read by the
// 1h run of the same end). With the remaining budget as the read limit, every
// 24h run read the second-newest hour again under a limit no earlier read had
// used (22 ms -> 170 ms warm p50 in the daily run, 2026-11-03).
func TestQueryRangeWindow_ShiftedRangesReuseHistoryWindows(t *testing.T) {
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	rows := windowReuseRows(anchor.Add(-30*time.Hour), anchor.Add(50*time.Minute), 3*time.Second, 0, 0)
	vl := newWindowReuseVL(t, rows, 1000, true)
	p := newWindowReuseProxy(t, windowReuseConfig(vl.srv.URL, newWindowReuseCache(t), true))

	seen := map[string]int{}
	for _, rng := range []time.Duration{time.Hour, 3 * time.Hour, 6 * time.Hour, 24 * time.Hour} {
		for run := 0; run < 7; run++ {
			end := anchor.Add(20*time.Minute - time.Duration(run)*time.Minute)
			start := end.Add(-rng)
			readsBefore, _ := vl.snapshot()
			got := windowReuseQuery(t, p, o1Query, start.UnixNano(), end.UnixNano(), 1000, false)
			want := vl.reference(start.UnixNano(), end.UnixNano(), 1000, false, true)
			if !equalTimestamps(got, want) {
				t.Fatalf("%s run %d: %d lines, want Loki's %d (newest %v)", rng, run, len(got), len(want), want[:1])
			}
			reads, _ := vl.snapshot()
			for _, read := range reads[len(readsBefore):] {
				if read.limit != 1000 {
					t.Errorf("%s run %d: window [%s, %s] read with limit %d, want the request's 1000", rng, run, read.start, read.end, read.limit)
				}
				key := read.start + ".." + read.end
				if seen[key]++; seen[key] > 1 {
					t.Errorf("%s run %d: window [%s, %s] read from VictoriaLogs again", rng, run, read.start, read.end)
				}
			}
			if rng == 24*time.Hour {
				if n := len(reads) - len(readsBefore); n != 0 {
					t.Errorf("24h run %d read %d windows from VictoriaLogs; every window it needs was read by the shorter ranges", run, n)
				}
			}
		}
	}
}

// TestQueryRangeWindow_BatchLayoutSharesWindows: a 24h range answered with
// parallel batches (stream-aware batching off) and then, moved by a minute,
// with sequential batches (every window expensive) on a second proxy sharing
// the cache reads only the windows the first did not: the moved newest
// window. Both answers are Loki's.
func TestQueryRangeWindow_BatchLayoutSharesWindows(t *testing.T) {
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	rows := windowReuseRows(anchor.Add(-30*time.Hour), anchor.Add(50*time.Minute), 5*time.Minute, 0, 0)
	vl := newWindowReuseVL(t, rows, 1000, false)
	shared := newWindowReuseCache(t)
	parallelCfg := windowReuseConfig(vl.srv.URL, shared, false)
	parallelCfg.QueryRangeStreamAwareBatching = false
	parallel := newWindowReuseProxy(t, parallelCfg)
	sequential := newWindowReuseProxy(t, windowReuseConfig(vl.srv.URL, shared, false))

	query := `{env="production"}`
	end := anchor.Add(20 * time.Minute)
	got := windowReuseQuery(t, parallel, query, end.Add(-24*time.Hour).UnixNano(), end.UnixNano(), 100, false)
	if want := vl.reference(end.Add(-24*time.Hour).UnixNano(), end.UnixNano(), 100, false, false); !equalTimestamps(got, want) {
		t.Fatalf("parallel layout: %d lines, want Loki's %d", len(got), len(want))
	}
	first, _ := vl.snapshot()
	if len(first) < 4 {
		t.Fatalf("fixture too dense: the limit was reached after %d windows", len(first))
	}

	moved := end.Add(-time.Minute)
	got = windowReuseQuery(t, sequential, query, moved.Add(-24*time.Hour).UnixNano(), moved.UnixNano(), 100, false)
	if want := vl.reference(moved.Add(-24*time.Hour).UnixNano(), moved.UnixNano(), 100, false, false); !equalTimestamps(got, want) {
		t.Fatalf("sequential layout: %d lines, want Loki's %d", len(got), len(want))
	}
	all, _ := vl.snapshot()
	second := all[len(first):]
	if len(second) != 1 || parseWindowReuseTime(second[0].start) != anchor.UnixNano() {
		t.Fatalf("the moved range read %v; want only its moved newest window starting at %d", second, anchor.UnixNano())
	}
	for _, read := range all {
		if read.limit != 100 {
			t.Errorf("window [%s, %s] read with limit %d, want the request's 100", read.start, read.end, read.limit)
		}
	}
}

// windowReuseCase is one row of the edge-case table: a fixture, then two
// requests on one proxy.
type windowReuseCase struct {
	name           string
	step           time.Duration // line spacing
	gap            [2]time.Duration
	malformedEvery int
	limit          int
	forward        bool
	expensive      bool // /hits above the threshold: sequential batches
	// lokiRefill asks `| user="u1"` in the Loki-compatible profile over
	// stored-user lines, only every matchEvery-th of which Loki matches
	// (windowReuseStoredUser): the others are dropped and refilled.
	lokiRefill bool
	matchEvery int
	// first and second request as (start, end) offsets from the anchor.
	first, second [2]time.Duration
}

func TestQueryRangeWindow_FullLimitReadEdgeCases(t *testing.T) {
	h := time.Hour
	m := time.Minute
	cases := []windowReuseCase{
		{name: "backward dense moved back a minute", step: 3 * time.Second, limit: 1000, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "backward dense moved forward a minute (refresh)", step: 3 * time.Second, limit: 1000, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 21*m, 21 * m}},
		{name: "backward sparse moved back, sequential", step: 5 * time.Minute, limit: 100, expensive: true, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "backward sparse moved back, parallel", step: 5 * time.Minute, limit: 100, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "forward sparse moved forward", step: 5 * time.Minute, limit: 100, forward: true, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 21*m, 21 * m}},
		{name: "forward dense moved back, sequential", step: 3 * time.Second, limit: 1000, forward: true, expensive: true, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "backward range grown at the start", step: 10 * time.Minute, limit: 50, first: [2]time.Duration{-6 * h, 20 * m}, second: [2]time.Duration{-24 * h, 20 * m}},
		{name: "backward range shrunk at the start", step: 10 * time.Minute, limit: 500, first: [2]time.Duration{-24 * h, 20 * m}, second: [2]time.Duration{-6 * h, 20 * m}},
		{name: "backward empty hours inside the range", step: 2 * time.Minute, gap: [2]time.Duration{-10 * h, -3 * h}, limit: 300, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		// An undecodable line still takes a place of its window's read limit;
		// read with only the rows still missing, a later window then
		// answered fewer lines than it holds, and the next window's older
		// lines took their place. Every window here holds fewer lines than
		// the limit, so a whole-limit read returns all of them.
		{name: "backward undecodable lines in later windows", step: 30 * time.Second, malformedEvery: 7, limit: 150, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "forward undecodable lines, sequential", step: 30 * time.Second, malformedEvery: 5, limit: 150, forward: true, expensive: true, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 21*m, 21 * m}},
		{name: "limit 1", step: 3 * time.Second, limit: 1, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "limit above every line", step: 30 * time.Minute, limit: 5000, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "newest window empty after the move", step: 20 * time.Minute, limit: 30, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 1*m, 1 * m}},
		// The label-filter refill of the Loki-compatible profile reads a
		// window's further pages until it holds the read limit's kept lines
		// or its page budget is spent. Read with only the rows still missing,
		// a page held as few rows, so with eleven of twelve lines dropped the
		// budget was spent before the window gave the lines it holds, and
		// older windows' lines took their place.
		{name: "Loki profile refill, a third of the lines kept", step: 30 * time.Second, lokiRefill: true, matchEvery: 3, limit: 200, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
		{name: "Loki profile refill, one line in twelve kept, sequential", step: 30 * time.Second, lokiRefill: true, matchEvery: 12, limit: 100, expensive: true, first: [2]time.Duration{-24*h + 20*m, 20 * m}, second: [2]time.Duration{-24*h + 19*m, 19 * m}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
			var rows []windowReuseRow
			for _, row := range windowReuseRows(anchor.Add(-30*time.Hour), anchor.Add(50*time.Minute), tc.step, tc.malformedEvery, tc.matchEvery) {
				if tc.gap != [2]time.Duration{} && row.ts >= anchor.Add(tc.gap[0]).UnixNano() && row.ts < anchor.Add(tc.gap[1]).UnixNano() {
					continue
				}
				rows = append(rows, row)
			}
			hitsPerRow := int64(1)
			if tc.expensive {
				hitsPerRow = 1000
			}
			vl := newWindowReuseVL(t, rows, hitsPerRow, false)
			query := `{env="production"}`
			if tc.lokiRefill {
				vl.kind = windowReuseStoredUser
				query = `{env="production"} | user="u1"`
			}
			p := newWindowReuseProxy(t, windowReuseConfig(vl.srv.URL, newWindowReuseCache(t), tc.lokiRefill))
			seen := map[string]bool{}
			for i, span := range [][2]time.Duration{tc.first, tc.second} {
				start, end := anchor.Add(span[0]).UnixNano(), anchor.Add(span[1]).UnixNano()
				before, beforeRows := vl.snapshot()
				got := windowReuseQuery(t, p, query, start, end, tc.limit, tc.forward)
				if want := vl.reference(start, end, tc.limit, tc.forward, tc.lokiRefill); !equalTimestamps(got, want) {
					reads, _ := vl.snapshot()
					for _, read := range reads[len(before):] {
						t.Logf("read %+v", read)
					}
					for k := range got {
						if k < len(want) && got[k] != want[k] {
							t.Logf("first difference at %d: got %s want %s", k, time.Unix(0, got[k]).UTC().Format(time.RFC3339), time.Unix(0, want[k]).UTC().Format(time.RFC3339))
							break
						}
					}
					t.Fatalf("request %d: %d lines, want Loki's %d", i+1, len(got), len(want))
				}
				reads, rowsRead := vl.snapshot()
				t.Logf("request %d: %d VictoriaLogs reads, %d rows", i+1, len(reads)-len(before), rowsRead-beforeRows)
				for _, read := range reads[len(before):] {
					if tc.lokiRefill && parseWindowReuseTime(read.end) != parseWindowReuseTime(read.start)+int64(time.Hour)-1 && read.limit != tc.limit {
						// A further refill page asks for the limit plus the
						// lines already read at its boundary.
						continue
					}
					if read.limit != tc.limit {
						t.Errorf("request %d: window [%s, %s] read with limit %d, want %d", i+1, read.start, read.end, read.limit, tc.limit)
					}
					if seen[read.start+".."+read.end] {
						t.Errorf("request %d: window [%s, %s] read again; the first request's read answers it", i+1, read.start, read.end)
					}
					seen[read.start+".."+read.end] = true
				}
			}
		})
	}
}

// TestQueryRangeWindow_NearNowWindowsReadOnlyTheMissingRows: a window no
// cache keeps (newer than -query-range-freshness, no recent TTL) gains
// nothing from a whole-limit read, so a window after the first read keeps
// the rows still missing as its limit; every kept window is read with the
// whole limit. The answer is Loki's.
func TestQueryRangeWindow_NearNowWindowsReadOnlyTheMissingRows(t *testing.T) {
	now := time.Now().UTC()
	rows := windowReuseRows(now.Add(-30*time.Hour).Truncate(time.Hour), now.Add(-time.Minute), 5*time.Minute, 0, 0)
	vl := newWindowReuseVL(t, rows, 1000, false)
	cfg := windowReuseConfig(vl.srv.URL, newWindowReuseCache(t), false)
	cfg.QueryRangeFreshness = 3 * time.Hour
	p := newWindowReuseProxy(t, cfg)
	start, end := now.Add(-24*time.Hour).Truncate(time.Second), now.Truncate(time.Second)
	const limit = 100
	got := windowReuseQuery(t, p, `{env="production"}`, start.UnixNano(), end.UnixNano(), limit, false)
	if want := vl.reference(start.UnixNano(), end.UnixNano(), limit, false, false); !equalTimestamps(got, want) {
		t.Fatalf("%d lines, want Loki's %d", len(got), len(want))
	}
	reads, _ := vl.snapshot()
	cutoff := now.Add(-cfg.QueryRangeFreshness).UnixNano()
	shorter := 0
	for i, read := range reads {
		switch kept := parseWindowReuseTime(read.end) <= cutoff; {
		case kept && read.limit != limit:
			t.Errorf("kept window [%s, %s] read with limit %d, want the request's %d", read.start, read.end, read.limit, limit)
		case !kept && i > 0 && read.limit < limit:
			shorter++
		}
	}
	if shorter == 0 {
		t.Fatalf("no near-now window after the first read with only the rows still missing: %v", reads)
	}
}

// windowReuseSequence runs one random request sequence on one proxy and
// checks every answer against Loki's. Every line is decodable: an
// undecodable line takes a place of its window's read limit, so a window
// holding more lines than the limit answers fewer than Loki (the edge table
// covers the windows that hold fewer).
func windowReuseSequence(t testing.TB, rng *rand.Rand, requests int) {
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	steps := []time.Duration{2 * time.Second, 30 * time.Second, 3 * time.Minute, 20 * time.Minute}
	step := steps[rng.Intn(len(steps))]
	malformedEvery := 0
	var rows []windowReuseRow
	gapFrom := anchor.Add(-time.Duration(rng.Intn(30)) * time.Hour)
	gapTo := gapFrom.Add(time.Duration(rng.Intn(6)) * time.Hour)
	for _, row := range windowReuseRows(anchor.Add(-30*time.Hour), anchor.Add(50*time.Minute), step, malformedEvery, 0) {
		if row.ts >= gapFrom.UnixNano() && row.ts < gapTo.UnixNano() {
			continue
		}
		rows = append(rows, row)
	}
	hitsPerRow := int64(1)
	if rng.Intn(2) == 0 {
		hitsPerRow = 1000
	}
	vl := newWindowReuseVL(t, rows, hitsPerRow, false)
	cfg := windowReuseConfig(vl.srv.URL, newWindowReuseCache(t), false)
	cfg.QueryRangeStreamAwareBatching = rng.Intn(4) != 0
	p := newWindowReuseProxy(t, cfg)
	for i := 0; i < requests; i++ {
		end := anchor.Add(time.Duration(rng.Intn(50)) * time.Minute).Add(-time.Duration(rng.Intn(20)) * time.Hour)
		start := end.Add(-time.Duration(70+rng.Intn(28*60)) * time.Minute)
		limit := 1 + rng.Intn(400)
		forward := rng.Intn(2) == 0
		got := windowReuseQuery(t, p, `{env="production"}`, start.UnixNano(), end.UnixNano(), limit, forward)
		if want := vl.reference(start.UnixNano(), end.UnixNano(), limit, forward, false); !equalTimestamps(got, want) {
			t.Fatalf("request %d (step %s, malformed every %d, forward %v, limit %d, [%s, %s]): %d lines, want Loki's %d",
				i+1, step, malformedEvery, forward, limit, start.Format(time.RFC3339), end.Format(time.RFC3339), len(got), len(want))
		}
	}
}

// TestQueryRangeWindow_ReuseMatchesLokiDifferential: random fixtures (line
// spacing, empty hours), batch layouts and request
// sequences (moved, grown and shrunk ranges, limits, both directions) on one
// cache: every answer is Loki's, whatever the earlier requests left cached.
func TestQueryRangeWindow_ReuseMatchesLokiDifferential(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 12
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(strconv.Itoa(seed), func(t *testing.T) {
			windowReuseSequence(t, rand.New(rand.NewSource(int64(seed))), 6)
		})
	}
}

// FuzzQueryRangeWindowReuse: two requests on one cache, both answered like
// Loki, every window read with the request's limit.
func FuzzQueryRangeWindowReuse(f *testing.F) {
	f.Add(int64(1), uint8(3))
	f.Add(int64(7), uint8(4))
	f.Add(int64(42), uint8(2))
	f.Fuzz(func(t *testing.T, seed int64, requests uint8) {
		windowReuseSequence(t, rand.New(rand.NewSource(seed)), 2+int(requests%3))
	})
}

// BenchmarkQueryRangeWindowO1ShiftedRepeat measures the warm 24h runs of the
// O1 A/B sequence (see TestQueryRangeWindow_ShiftedRangesReuseHistoryWindows):
// per op, the 1h, 3h and 6h runs prime a fresh cache, then the six warm 24h
// runs are timed; vl_reads/op and vl_rows/op count their window reads.
func BenchmarkQueryRangeWindowO1ShiftedRepeat(b *testing.B) {
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	rows := windowReuseRows(anchor.Add(-30*time.Hour), anchor.Add(50*time.Minute), 3*time.Second, 0, 0)
	vl := newWindowReuseVL(b, rows, 1000, true)
	var reads, rowsRead int
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p := newWindowReuseProxy(b, windowReuseConfig(vl.srv.URL, newWindowReuseCache(b), true))
		for _, rng := range []time.Duration{time.Hour, 3 * time.Hour, 6 * time.Hour, 24 * time.Hour} {
			for run := 0; run < 7; run++ {
				end := anchor.Add(20*time.Minute - time.Duration(run)*time.Minute)
				timed := rng == 24*time.Hour && run > 0
				before, beforeRows := vl.snapshot()
				if timed {
					b.StartTimer()
				}
				windowReuseQuery(b, p, o1Query, end.Add(-rng).UnixNano(), end.UnixNano(), 1000, false)
				if timed {
					b.StopTimer()
					after, afterRows := vl.snapshot()
					reads += len(after) - len(before)
					rowsRead += afterRows - beforeRows
				}
			}
		}
	}
	b.ReportMetric(float64(reads)/float64(b.N)/6, "vl_reads/op")
	b.ReportMetric(float64(rowsRead)/float64(b.N)/6, "vl_rows/op")
}

// BenchmarkQueryRangeWindowColdRead measures one cold 24h log query per op
// on a fresh cache: what reading the window that completes the limit with
// the whole limit costs. vl_reads/op counts window and refill page reads,
// vl_rows/op the rows VictoriaLogs returned.
func BenchmarkQueryRangeWindowColdRead(b *testing.B) {
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	for _, bc := range []struct {
		name       string
		step       time.Duration
		matchEvery int
		kind       int
		filter     bool
		lokiRefill bool
		query      string
		limit      int
		forward    bool
	}{
		{name: "O1 json filter, backward", step: 3 * time.Second, filter: true, lokiRefill: true, query: o1Query, limit: 1000},
		{name: "sparse, backward", step: 5 * time.Minute, query: `{env="production"}`, limit: 100},
		{name: "dense, forward", step: 3 * time.Second, query: `{env="production"}`, limit: 1000, forward: true},
		{name: "Loki profile refill, a third kept", step: 30 * time.Second, matchEvery: 3, kind: windowReuseStoredUser, lokiRefill: true, query: `{env="production"} | user="u1"`, limit: 200},
	} {
		b.Run(bc.name, func(b *testing.B) {
			rows := windowReuseRows(anchor.Add(-30*time.Hour), anchor.Add(50*time.Minute), bc.step, 0, bc.matchEvery)
			vl := newWindowReuseVL(b, rows, 1000, bc.filter)
			vl.kind = bc.kind
			end := anchor.Add(20 * time.Minute)
			var reads, rowsRead int
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				p := newWindowReuseProxy(b, windowReuseConfig(vl.srv.URL, newWindowReuseCache(b), bc.lokiRefill))
				before, beforeRows := vl.snapshot()
				b.StartTimer()
				windowReuseQuery(b, p, bc.query, end.Add(-24*time.Hour).UnixNano(), end.UnixNano(), bc.limit, bc.forward)
				b.StopTimer()
				after, afterRows := vl.snapshot()
				reads += len(after) - len(before)
				rowsRead += afterRows - beforeRows
			}
			b.ReportMetric(float64(reads)/float64(b.N), "vl_reads/op")
			b.ReportMetric(float64(rowsRead)/float64(b.N), "vl_rows/op")
		})
	}
}
