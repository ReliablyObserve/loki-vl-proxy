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
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// shortLivedStreamRows are the bench/ab seed's shape: every 10s each of four services starts a new pod stream whose
// three lines sit half a second past the batch second, so a day holds hundreds of thousands of streams that live a
// few seconds each and a step's window holds a few dozen per service.
func shortLivedStreamRows(start, end int64) []vlemu.Row {
	var rows []vlemu.Row
	batch := 0
	for at := start; at < end; at += int64(10 * time.Second) {
		for svc := 0; svc < 4; svc++ {
			// Service s3 starts no pod in every fifth batch, so the counts move between steps.
			if svc == 3 && batch%5 == 0 {
				continue
			}
			stream := fmt.Sprintf(`{env="production",service_name="s%d",pod="p%d-%d"}`, svc, svc, batch)
			for line := int64(0); line < 3; line++ {
				rows = append(rows, vlemu.Row{TS: at + 500_000_000 + line*1_000, Stream: stream, Value: 1})
			}
		}
		batch++
	}
	return rows
}

// activeStreamsPerStep is Loki's answer to `count by (service_name) (count_over_time(sel[window]))` over rows: per
// step T, the streams of each service with a line in (T-window, T]; divide is applied to each value (300 for the
// rate/count ratio, 1 for the count).
func activeStreamsPerStep(rows []vlemu.Row, start, end int64, step, window time.Duration, divide float64) map[string]map[int64]float64 {
	want := map[string]map[int64]float64{}
	for ts := start; ts <= end; ts += int64(step) {
		active := map[string]map[string]bool{}
		for _, row := range rows {
			if row.TS > ts-int64(window) && row.TS <= ts {
				svc := vlemu.StreamLabels(row.Stream)["service_name"]
				if active[svc] == nil {
					active[svc] = map[string]bool{}
				}
				active[svc][row.Stream] = true
			}
		}
		for svc, streams := range active {
			key := fmt.Sprintf(`{"service_name":%q}`, svc)
			if want[key] == nil {
				want[key] = map[int64]float64{}
			}
			want[key][ts/1e9] = float64(len(streams)) / divide
		}
	}
	return want
}

// TestStreamIdentityWindowsChunkedPastBodyBound is B2 of bench/ab (aggregation-over-binary) and the count by it is
// read as, at 24h: the per-stream buckets of many short-lived streams over the whole range passed the stats body
// bound (64 MiB, maxStatsBucketResponseBytes) and the request answered 500 ResourceExhausted, where the answer is a
// few groups per step. The windows are now read in time chunks after that read, each folded as it is read
// (listed_window_chunks.go), and the answer is Loki's: every value equal to the active streams counted from the
// rows. A repeat on the same proxy reads in chunks at once (no read over the bound), and a request whose read fits the
// bound is not chunked.
//
// conformance: semantics/series-level-aggregation-bounded-memory, semantics/aggregation-over-binary-dropped, metric-series-identity, loki_api_v1_query_range
func TestStreamIdentityWindowsChunkedPastBodyBound(t *testing.T) {
	const (
		step   = 60 * time.Second
		window = 5 * time.Minute
	)
	end := binaryAggregationAt
	start := end - int64(3*time.Hour)
	rows := shortLivedStreamRows(start-int64(window)-int64(time.Minute), end)
	for _, tc := range []struct {
		query  string
		divide float64
	}{
		{`sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`, window.Seconds()},
		{`count by (service_name) (count_over_time({env="production"}[5m]))`, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			want := activeStreamsPerStep(rows, start, end, step, window, tc.divide)
			emu := &vlemu.Emulator{T: t, Rows: rows, Version: "v1.54.0"}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			// The series limit of the bench/ab and e2e stacks (-max-stats-query-series=1000000): the read holds a series
			// per stream before the fold. The buffered bound keeps the raw rows under it, as a day of the seed's lines is
			// under the 64 MiB default: the folded answer fits it, the raw rows do not.
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
				BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: 1_000_000,
				ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 128 << 10}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}

			// The whole range's read fits the default bound: one read, no chunks.
			var chunks []statsReadMeter
			listedWindowChunksObserve = func(_, _ int64, m statsReadMeter) { chunks = append(chunks, m) }
			defer func() { listedWindowChunksObserve = nil }()
			whole := streamWindowAnswer(t, p, tc.query, start, end-int64(time.Hour), step)
			if len(chunks) != 0 {
				t.Fatalf("a read within the bound was chunked: %d chunks", len(chunks))
			}
			compareBinaryAggregation(t, tc.query+" (within the bound)", whole, trimSteps(want, end-int64(time.Hour)))

			// A bound the whole range's buckets pass and a quarter of the steps' buckets do not.
			old := maxStatsBucketResponseBytes
			maxStatsBucketResponseBytes = streamWindowBound(t, emu, rows)
			defer func() { maxStatsBucketResponseBytes = old }()
			p.cache.PurgeAll() // no answer or bucket of the first request is reused
			chunks = nil
			got := streamWindowAnswer(t, p, tc.query, start, end, step)
			compareBinaryAggregation(t, tc.query, got, want)
			if len(chunks) < 2 {
				t.Fatalf("the read past the bound was not chunked: %d chunks", len(chunks))
			}
			for i, m := range chunks {
				if m.overBytes {
					t.Fatalf("chunk %d of %d passed the bound: %+v", i, len(chunks), m)
				}
			}

			// The repeat on a grid half a step later (no answer, step or bucket of the request before is reusable):
			// chunked at once, so no read is thrown away.
			chunks = nil
			before := len(emu.EndpointCalls("/select/logsql/stats_query_range"))
			shift := int64(step / 2)
			moved := streamWindowAnswer(t, p, tc.query, start+shift, end-shift, step)
			compareBinaryAggregation(t, tc.query+" (half a step later)", moved, activeStreamsPerStep(rows, start+shift, end-shift, step, window, tc.divide))
			if len(chunks) < 2 || len(emu.EndpointCalls("/select/logsql/stats_query_range"))-before != len(chunks) {
				t.Fatalf("the repeat read %d chunks in %d stats calls, want only its chunks", len(chunks), len(emu.EndpointCalls("/select/logsql/stats_query_range"))-before)
			}
		})
	}
}

// streamWindowAnswer is the answer to query over [start, end] at step: 200 or the test fails.
func streamWindowAnswer(t *testing.T, p *Proxy, query string, start, end int64, step time.Duration) map[string]map[int64]float64 {
	t.Helper()
	got, body := binaryAggregationAnswerVia(t, p, "0", query, start, end, step)
	if got == nil {
		t.Fatalf("%s over [%d, %d]: %.300s", query, start/1e9, end/1e9, body)
	}
	return got
}

// streamWindowBound is a stats body bound between what a quarter of the request's buckets and the whole request's
// take: half of the whole read, which the emulator answers for the rows' stream series.
func streamWindowBound(t *testing.T, emu *vlemu.Emulator, rows []vlemu.Row) int64 {
	t.Helper()
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	params := url.Values{"query": {`env:="production" | stats by (service_name, _stream) count() as c`},
		"start": {strconv.FormatInt(rows[0].TS/1e9, 10)}, "end": {strconv.FormatInt(rows[len(rows)-1].TS/1e9+1, 10)}, "step": {"60s"}}
	res, err := http.PostForm(backend.URL+"/select/logsql/stats_query_range", params)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := readBodyLimited(res.Body, 64<<20)
	if err != nil || res.StatusCode != http.StatusOK || len(body) < 100_000 {
		t.Fatalf("whole read: %d bytes, status %d, %v", len(body), res.StatusCode, err)
	}
	return int64(len(body)) / 2
}

// trimSteps keeps the points of want up to end.
func trimSteps(want map[string]map[int64]float64, end int64) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	for key, points := range want {
		for ts, v := range points {
			if ts <= end/1e9 {
				if out[key] == nil {
					out[key] = map[int64]float64{}
				}
				out[key][ts] = v
			}
		}
	}
	return out
}

// streamChunkShapes are the series-level shapes over the stream's series whose windows a read past the stats body bound
// reads in chunks: count, min, max and avg by over a range function, one under an outer sum, and the rate/count ratio.
var streamChunkShapes = []string{
	`count by (service_name) (count_over_time({env="production"}[%s]))`,
	`avg by (service_name) (rate({env="production"}[%s]))`,
	`max by (service_name) (count_over_time({env="production"}[%s]))`,
	`min by (service_name) (bytes_over_time({env="production"}[%s]))`,
	`sum by (service_name) (max by (service_name, pod) (count_over_time({env="production"}[%s])))`,
	`sum by (service_name) (rate({env="production"}[%s]) / count_over_time({env="production"}[%s]))`,
}

// streamChunkCase is one random request of the stream-identity differential.
type streamChunkCase struct {
	seed                 int64
	query                string
	window, step, every  time.Duration
	start, end           int64
	lines                int
	rows                 []vlemu.Row
	unchunked, chunked   string
	chunks, hintedChunks int
}

func newStreamChunkCase(seed int64) *streamChunkCase {
	rng := rand.New(rand.NewSource(seed))
	c := &streamChunkCase{seed: seed}
	windows := []time.Duration{2 * time.Minute, 5 * time.Minute, 7 * time.Minute}
	steps := []time.Duration{15 * time.Second, 20 * time.Second, 30 * time.Second, 45 * time.Second, 60 * time.Second, 90 * time.Second}
	c.window = windows[rng.Intn(len(windows))]
	for c.step = steps[rng.Intn(len(steps))]; c.step >= c.window; c.step = steps[rng.Intn(len(steps))] {
	}
	c.every = []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second}[rng.Intn(3)]
	c.lines = 1 + rng.Intn(3)
	c.end = binaryAggregationAt - int64(rng.Intn(600))*1e9
	c.end -= c.end % int64(c.step)
	c.start = c.end - int64(time.Duration(60+rng.Intn(120))*time.Minute)
	c.start -= c.start % int64(c.step)
	shape := streamChunkShapes[rng.Intn(len(streamChunkShapes))]
	c.query = strings.ReplaceAll(shape, "%s", c.window.String())
	c.query = strings.ReplaceAll(c.query, "m0s", "m")
	for at := c.start - int64(c.window) - int64(time.Minute); at < c.end; at += int64(c.every) {
		for svc := 0; svc < 3; svc++ {
			if rng.Intn(6) == 0 {
				continue // a quiet batch, so the counts move between steps
			}
			stream := fmt.Sprintf(`{env="production",service_name="s%d",pod="p%d-%d"}`, svc, svc, at/1e9)
			for line := 0; line < c.lines; line++ {
				// Off every whole millisecond, so no line sits on a window edge.
				ts := at + int64(rng.Intn(int(c.every/time.Millisecond)-2)+1)*int64(time.Millisecond) + 300_000
				c.rows = append(c.rows, vlemu.Row{TS: ts, Stream: stream, Value: 1, Msg: strings.Repeat("x", 1+rng.Intn(40))})
			}
		}
	}
	sort.Slice(c.rows, func(i, j int) bool { return c.rows[i].TS < c.rows[j].TS })
	return c
}

// newStreamChunkProxy is a proxy against rows with the e2e stacks' series limit and a buffered bound the raw rows pass.
func newStreamChunkProxy(t testing.TB, rows []vlemu.Row) (*Proxy, *vlemu.Emulator, func()) {
	t.Helper()
	emu := &vlemu.Emulator{T: t, Rows: rows, Version: "v1.54.0"}
	backend := httptest.NewServer(emu.Handler())
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
		BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: 1_000_000,
		ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 128 << 10}})
	if err != nil {
		backend.Close()
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		backend.Close()
		t.Fatal(err)
	}
	return p, emu, func() { _ = p.Shutdown(context.Background()); backend.Close() }
}

// streamChunkBody is the body of query over [start, end] at step, the stats a request's own timings fill left out.
func streamChunkBody(t testing.TB, p *Proxy, query string, start, end int64, step time.Duration) (int, string) {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start/1e9, 10)}, "end": {strconv.FormatInt(end/1e9, 10)},
		"step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	var body struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &body) != nil {
		return res.Code, res.Body.String()
	}
	return res.Code, body.Data.ResultType + " " + string(body.Data.Result)
}

// checkStreamChunkCase holds the chunked answer of c (a stats body bound under the whole read) to the one-read
// answer byte for byte, and a request of the same query and step on a shorter range after the overflow, which the
// replica's hint sends through the chunked writer at once, to a fresh proxy's answer to it.
func checkStreamChunkCase(t testing.TB, c *streamChunkCase) {
	t.Helper()
	label := fmt.Sprintf("seed %d %s (window %s step %s, a stream every %s, %d lines each)", c.seed, c.query, c.window, c.step, c.every, c.lines)
	one, _, closeOne := newStreamChunkProxy(t, c.rows)
	code, unchunked := streamChunkBody(t, one, c.query, c.start, c.end, c.step)
	shortStart := c.end - int64(20*c.step)
	_, shortWant := streamChunkBody(t, one, c.query, shortStart, c.end, c.step)
	closeOne()
	if code != http.StatusOK {
		t.Fatalf("%s: one read answered %d %.300s", label, code, unchunked)
	}
	old := maxStatsBucketResponseBytes
	maxStatsBucketResponseBytes = streamWindowBoundOf(t, c.rows, c.step) / 3
	defer func() { maxStatsBucketResponseBytes = old }()
	chunked, _, closeChunked := newStreamChunkProxy(t, c.rows)
	defer closeChunked()
	var chunks []statsReadMeter
	listedWindowChunksObserve = func(_, _ int64, m statsReadMeter) { chunks = append(chunks, m) }
	defer func() { listedWindowChunksObserve = nil }()
	code, got := streamChunkBody(t, chunked, c.query, c.start, c.end, c.step)
	if code != http.StatusOK || got != unchunked {
		t.Fatalf("%s: chunked answered %d, equal to one read %v\n chunked %.600s\none read %.600s", label, code, got == unchunked, got, unchunked)
	}
	if c.chunks = len(chunks); c.chunks < 2 {
		t.Fatalf("%s: the read past the bound was read in %d chunks", label, c.chunks)
	}
	chunks = nil
	code, short := streamChunkBody(t, chunked, c.query, shortStart, c.end, c.step)
	if code != http.StatusOK || short != shortWant {
		t.Fatalf("%s: after the overflow a shorter range answered %d, equal to a fresh proxy's %v\n got %.400s\nwant %.400s", label, code, short == shortWant, short, shortWant)
	}
	if c.hintedChunks = len(chunks); c.hintedChunks < 1 {
		t.Fatalf("%s: the shorter range after the overflow was not read through the chunks", label)
	}
}

// streamWindowBoundOf is the size of the stream-identity bucket read of rows at step (the emulator's answer to the
// read the stats of the stream's series make, without the level chain), the scale a bound under the request's read is
// taken from.
func streamWindowBoundOf(t testing.TB, rows []vlemu.Row, step time.Duration) int64 {
	t.Helper()
	emu := &vlemu.Emulator{T: t, Rows: rows, Version: "v1.54.0"}
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	params := url.Values{"query": {`env:="production" | stats by (service_name, _stream) count() as c`},
		"start": {strconv.FormatInt(rows[0].TS/1e9, 10)}, "end": {strconv.FormatInt(rows[len(rows)-1].TS/1e9+1, 10)},
		"step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64) + "s"}}
	res, err := http.PostForm(backend.URL+"/select/logsql/stats_query_range", params)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := readBodyLimited(res.Body, 256<<20)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("whole read: %d bytes, status %d, %v", len(body), res.StatusCode, err)
	}
	return int64(len(body))
}

// TestStreamIdentityWindowChunksMatchOneRead is the differential of the chunks after an overflow over the stream's
// series: random shapes (count, min, max, avg by, a sum over a max by, the rate/count ratio), windows, steps, spans and
// stream densities, a stats body bound a third of the whole read; every chunked answer is byte for byte the one-read
// answer, and a shorter request after the overflow, read through the chunks at once, is a fresh proxy's answer.
//
// conformance: semantics/series-level-aggregation-bounded-memory, metric-series-identity, loki_api_v1_query_range
func TestStreamIdentityWindowChunksMatchOneRead(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		c := newStreamChunkCase(seed)
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			checkStreamChunkCase(t, c)
			t.Logf("%s window %s step %s: %d rows, %d chunks, the shorter range after it %d", c.query, c.window, c.step, len(c.rows), c.chunks, c.hintedChunks)
		})
	}
	// Every shape is drawn at least once over the seeds above.
	seen := map[string]bool{}
	for seed := int64(1); seed <= 10; seed++ {
		q := newStreamChunkCase(seed).query
		seen[q[:strings.Index(q, "(")]+strconv.Itoa(strings.Count(q, "_over_time")+strings.Count(q, "rate("))] = true
	}
	if len(seen) < 4 {
		t.Fatalf("the seeds draw only %d shapes: %v", len(seen), seen)
	}
}

// FuzzStreamIdentityWindowChunks is the differential over any seed.
func FuzzStreamIdentityWindowChunks(f *testing.F) {
	for _, seed := range []int64{1, 5, 9, 77} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) {
		checkStreamChunkCase(t, newStreamChunkCase(seed))
	})
}
