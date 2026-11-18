package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// listedChunkShapes are the extraction-list series identities whose windows are read in chunks: sum, max, min and avg
// (by a stream label or none) over the functions that merge from buckets, after a list that names a label of many
// values (user).
var listedChunkShapes = []string{
	`sum(max_over_time({app="uw"} | logfmt amount, user | unwrap amount [%s]))`,
	`sum by (ns) (stdvar_over_time({app="uw"} | logfmt amount, user | unwrap amount [%s]))`,
	`avg(avg_over_time({app="uw"} | logfmt amount, user | unwrap amount [%s]))`,
	`sum(min_over_time({app="uw"} | logfmt amount, user | unwrap amount [%s]))`,
	`avg by (ns) (stddev_over_time({app="uw"} | logfmt amount, user | unwrap amount [%s]))`,
	`max by (ns) (avg_over_time({app="uw"} | logfmt amount, user | unwrap amount [%s]))`,
}

// listedChunkRows are rows on four streams of two namespaces over span before end, a line every gap, each naming one of
// users values of user, so a window holds a series per user that has a line in it.
func listedChunkRows(rng *rand.Rand, end int64, span, gap time.Duration, users int) []vlemu.Row {
	streams := []string{`{app="uw",ns="x",pod="a"}`, `{app="uw",ns="x",pod="b"}`, `{app="uw",ns="y",pod="a"}`, `{app="uw",ns="y",pod="c"}`}
	var rows []vlemu.Row
	for at := end - int64(span); at < end; at += int64(gap) {
		// Off every whole millisecond, so no line sits on a window edge.
		ts := at + rng.Int63n(int64(gap)/2) + 300_000
		amount := float64(rng.Intn(1000)+1) / 4
		user := "u" + strconv.Itoa(rng.Intn(users))
		rows = append(rows, vlemu.Row{TS: ts, Stream: streams[rng.Intn(len(streams))], Labels: map[string]string{"user": user},
			Value: amount, Msg: fmt.Sprintf("amount=%v user=%s", amount, user)})
	}
	return rows
}

// listedChunkRun is one query_range request through a proxy of its own against rows: status, body, the chunks it read
// and its stats_query_range calls.
type listedChunkRun struct {
	code   int
	body   string
	chunks []statsReadMeter
	calls  int
}

// runListedChunks serves query over [end-span, end] at step from a fresh proxy whose reads hold budget series before the
// fold (-manual-range-metric-row-limit; 0 is the default) against rows; repeat asks again on the same proxy with the
// window moved by a step.
func runListedChunks(t testing.TB, rows []vlemu.Row, query string, end int64, span, step time.Duration, budget int, repeat bool) []listedChunkRun {
	t.Helper()
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.54.0"}
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	cfg := Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
		BackendVersionCheckTimeout: time.Second}
	if budget > 0 {
		cfg.RangeMetricRowLimit, cfg.MaxStatsQuerySeries = budget, min(budget, 20)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	var out []listedChunkRun
	for i := 0; i < 1+boolInt(repeat); i++ {
		var chunks []statsReadMeter
		listedWindowChunksObserve = func(_, _ int64, m statsReadMeter) { chunks = append(chunks, m) }
		before := len(emu.EndpointCalls("/select/logsql/stats_query_range"))
		e := end + int64(i)*int64(step)
		params := url.Values{"query": {query}, "start": {strconv.FormatInt((e-int64(span))/1e9, 10)}, "end": {strconv.FormatInt(e/1e9, 10)},
			"step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		listedWindowChunksObserve = nil
		out = append(out, listedChunkRun{code: res.Code, body: res.Body.String(), chunks: chunks,
			calls: len(emu.EndpointCalls("/select/logsql/stats_query_range")) - before})
	}
	return out
}

// listedChunkCase is one random request of the differential: a shape, a range, a step shorter than it, the request's
// span and the budget of a chunk's read.
type listedChunkCase struct {
	seed               int64
	shape              int
	window, step, span time.Duration
	users, budget      int
	gap                time.Duration
	end                int64
	query              string
	rows               []vlemu.Row
	windowSeriesAtMost int
	unchunked, chunked listedChunkRun
	chunkedRuns        []listedChunkRun
}

// newListedChunkCase draws a case from seed.
func newListedChunkCase(seed int64) *listedChunkCase {
	rng := rand.New(rand.NewSource(seed))
	c := &listedChunkCase{seed: seed, shape: rng.Intn(len(listedChunkShapes))}
	windows := []time.Duration{2 * time.Minute, 5 * time.Minute, 7 * time.Minute, 13 * time.Minute}
	steps := []time.Duration{7 * time.Second, 15 * time.Second, 30 * time.Second, 45 * time.Second, 60 * time.Second, 90 * time.Second}
	c.window = windows[rng.Intn(len(windows))]
	for c.step = steps[rng.Intn(len(steps))]; c.step >= c.window; c.step = steps[rng.Intn(len(steps))] {
	}
	c.span = time.Duration(10+rng.Intn(30)) * time.Minute
	c.gap = time.Duration(2+rng.Intn(6)) * time.Second
	c.users = 20 + rng.Intn(400)
	c.end = int64(1_800_000_000+rng.Intn(3600)) * 1e9
	c.end -= c.end % int64(c.step)
	c.query = fmt.Sprintf(listedChunkShapes[c.shape], c.window)
	c.rows = listedChunkRows(rng, c.end, c.span+c.window+2*c.step, c.gap, c.users)
	c.windowSeriesAtMost = listedChunkWindowSeries(c.rows, c.window+2*c.step)
	// A chunk's budget at least the series of a step's window (a single step over it is the one refusal), at most
	// a fraction of the request's.
	c.budget = max(c.windowSeriesAtMost, 21) + rng.Intn(max(1, c.windowSeriesAtMost))
	return c
}

// listedChunkWindowSeries is the most series (stream and user) the lines of any interval of length d hold.
func listedChunkWindowSeries(rows []vlemu.Row, d time.Duration) int {
	most := 0
	for i := range rows {
		seen := map[string]bool{}
		for j := i; j < len(rows) && rows[j].TS-rows[i].TS <= int64(d); j++ {
			seen[rows[j].Stream+rows[j].Labels["user"]] = true
		}
		most = max(most, len(seen))
	}
	return most
}

// TestListedWindowChunksMatchUnchunked is the differential of the chunked read: random windows, steps, ranges and
// budgets, every answer byte for byte the one a single read gives, over many chunks.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestListedWindowChunksMatchUnchunked(t *testing.T) {
	chunkedRequests := 0
	for seed := int64(1); seed <= 12; seed++ {
		c := newListedChunkCase(seed)
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			checkListedChunkCase(t, c)
			if len(c.chunked.chunks) > 1 {
				chunkedRequests++
			}
		})
	}
	if chunkedRequests < 9 {
		t.Fatalf("only %d of 12 requests were read in more than one chunk", chunkedRequests)
	}
}

// checkListedChunkCase holds the chunked answer of c to the unchunked one, its reads to the budget, and a repeat on
// the same proxy to the chunk size it measured (no read over the budget).
func checkListedChunkCase(t testing.TB, c *listedChunkCase) {
	t.Helper()
	c.unchunked = runListedChunks(t, c.rows, c.query, c.end, c.span, c.step, 0, false)[0]
	if c.unchunked.code != http.StatusOK || len(c.unchunked.chunks) != 1 || c.unchunked.calls != 1 {
		t.Fatalf("seed %d %s: unchunked answered %d in %d chunks, %d calls: %.300s", c.seed, c.query, c.unchunked.code, len(c.unchunked.chunks), c.unchunked.calls, c.unchunked.body)
	}
	c.chunkedRuns = runListedChunks(t, c.rows, c.query, c.end, c.span, c.step, c.budget, true)
	c.chunked = c.chunkedRuns[0]
	if c.chunked.code != http.StatusOK {
		t.Fatalf("seed %d %s (window %s step %s span %s, budget %d, a window at most %d series): %d %.300s", c.seed, c.query, c.window, c.step, c.span, c.budget, c.windowSeriesAtMost, c.chunked.code, c.chunked.body)
	}
	if c.chunked.body != c.unchunked.body {
		t.Fatalf("seed %d %s (window %s step %s span %s, budget %d): the chunked answer differs\n chunked %.600s\nunchunked %.600s", c.seed, c.query, c.window, c.step, c.span, c.budget, c.chunked.body, c.unchunked.body)
	}
	for i, m := range c.chunked.chunks {
		if !m.over && m.series > c.budget {
			t.Fatalf("seed %d: chunk %d held %d series, over the budget %d", c.seed, i, m.series, c.budget)
		}
	}
	again := c.chunkedRuns[1]
	if again.code != http.StatusOK {
		t.Fatalf("seed %d: the repeat answered %d %.300s", c.seed, again.code, again.body)
	}
	if len(again.chunks) > 0 && again.chunks[0].over && c.chunked.chunks[0].over {
		t.Fatalf("seed %d: the repeat read its first chunk over the budget again (the measured size is not used)", c.seed)
	}
}

// FuzzListedWindowChunks is the differential over any seed.
func FuzzListedWindowChunks(f *testing.F) {
	for _, seed := range []int64{1, 7, 42, 1234} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) {
		checkListedChunkCase(t, newListedChunkCase(seed))
	})
}

// TestListedWindowChunksMemoryBounded holds what a chunk's read holds to the budget while the series grow about
// fifteenfold: the request is read in more chunks, each holding at most the budget's series and a body of at most twice
// the budget's series at the smallest size's bytes per series, and answers what one read answers. What one read holds
// grows with the series.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestListedWindowChunksMemoryBounded(t *testing.T) {
	const budget = 6_000
	query := fmt.Sprintf(listedChunkShapes[0], 5*time.Minute)
	end := int64(1_800_000_000) * 1e9
	perSeries := 0
	for _, size := range []struct {
		users int
		gap   time.Duration
	}{{1_000, 2 * time.Second}, {10_000, 500 * time.Millisecond}, {40_000, 150 * time.Millisecond}} {
		rng := rand.New(rand.NewSource(int64(size.users)))
		rows := listedChunkRows(rng, end, time.Hour+10*time.Minute, size.gap, size.users)
		unchunked := runListedChunks(t, rows, query, end, time.Hour, 30*time.Second, 0, false)[0]
		chunked := runListedChunks(t, rows, query, end, time.Hour, 30*time.Second, budget, false)[0]
		if unchunked.code != http.StatusOK || chunked.code != http.StatusOK || chunked.body != unchunked.body {
			t.Fatalf("%d users: unchunked %d, chunked %d, equal %v", size.users, unchunked.code, chunked.code, chunked.body == unchunked.body)
		}
		most, mostBytes := 0, 0
		for _, m := range chunked.chunks {
			if !m.over {
				most, mostBytes = max(most, m.series), max(mostBytes, m.bytes)
			}
		}
		if most > budget {
			t.Fatalf("%d users: a chunk held %d series, over the budget %d", size.users, most, budget)
		}
		if perSeries == 0 {
			perSeries = unchunked.chunks[0].bytes / max(1, unchunked.chunks[0].series)
		} else if mostBytes > 2*perSeries*budget {
			t.Fatalf("%d users: a chunk's body of %d bytes grew past the budget's (%d bytes per series at the smallest size)", size.users, mostBytes, perSeries)
		}
		t.Logf("%7d users, %6d lines: one read holds %6d series in %8d bytes; chunked: %3d reads (%d over the budget), at most %4d series in %7d bytes each, %d stats calls",
			size.users, len(rows), unchunked.chunks[0].series, unchunked.chunks[0].bytes, len(chunked.chunks), countOver(chunked.chunks), most, mostBytes, chunked.calls)
	}
}

func countOver(chunks []statsReadMeter) int {
	n := 0
	for _, m := range chunks {
		if m.over {
			n++
		}
	}
	return n
}

// TestListedWindowChunksEdges pins the chunked read's edges: a single step whose window holds more series than the
// budget answers Loki's limit error (the flag in the log only), one whose stats pass the body bound Loki's
// message-size error, and a VictoriaLogs whose bucket read fails
// is asked once before the window evaluator reads the raw rows, as without chunks.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestListedWindowChunksEdges(t *testing.T) {
	end := int64(1_800_000_000) * 1e9
	query := fmt.Sprintf(listedChunkShapes[0], 5*time.Minute)
	rows := listedChunkRows(rand.New(rand.NewSource(3)), end, 40*time.Minute, time.Second, 5_000)
	t.Run("a step over the budget", func(t *testing.T) {
		run := runListedChunks(t, rows, query, end, 30*time.Minute, 30*time.Second, 50, false)[0]
		if run.code != http.StatusBadRequest || !strings.Contains(run.body, "limit reached while evaluating the query") || strings.Contains(run.body, "manual-range-metric-row-limit") {
			t.Fatalf("want Loki's limit answer, got %d %.300s", run.code, run.body)
		}
		if n := len(run.chunks); n < 2 || !run.chunks[n-1].over {
			t.Fatalf("want the read cut down to one step over the budget, read %d chunks", n)
		}
	})
	t.Run("a step over the body bound", func(t *testing.T) {
		old := maxStatsBucketResponseBytes
		maxStatsBucketResponseBytes = 2_000
		t.Cleanup(func() { maxStatsBucketResponseBytes = old })
		run := runListedChunks(t, rows, query, end, 30*time.Minute, 30*time.Second, 0, false)[0]
		if run.code != http.StatusInternalServerError || !strings.Contains(run.body, "ResourceExhausted") || strings.Contains(run.body, "backend-max-buffered") {
			t.Fatalf("want Loki's message-size answer, got %d %.300s", run.code, run.body)
		}
		if n := len(run.chunks); n < 2 || !run.chunks[n-1].overBytes {
			t.Fatalf("want the read cut down to one step over the body bound, read %d chunks", n)
		}
	})
	t.Run("a failed bucket read", func(t *testing.T) {
		emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.54.0"}
		inner := emu.Handler()
		statsCalls := 0
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/select/logsql/stats_query_range" {
				statsCalls++
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			inner.ServeHTTP(w, r)
		}))
		defer backend.Close()
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = p.Shutdown(context.Background()) }()
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(end/1e9-1800, 10)}, "end": {strconv.FormatInt(end/1e9, 10)}, "step": {"30"}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if statsCalls != 1 {
			t.Fatalf("the bucket read was sent %d times (answer %d %.200s), want once", statsCalls, res.Code, res.Body.String())
		}
	})
}
