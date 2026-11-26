package proxy

import (
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
)

// chunkRows is a busy grouping over the range and the window before it: pod i logs every 7+i seconds with jitter, so
// every pod has a line in most step buckets and the ranks move from step to step.
func chunkRows(from, to time.Time, pods int) []emulatedRow {
	var rows []emulatedRow
	seed := uint64(7)
	for i := 0; i < pods; i++ {
		every := time.Duration(7+i) * time.Second
		for ts := from.Add(time.Duration(i) * 113 * time.Millisecond); ts.Before(to); ts = ts.Add(every) {
			seed = seed*6364136223846793005 + 1442695040888963407
			jitter := time.Duration(seed>>40) % (3 * time.Second)
			rows = append(rows, emulatedRow{ts: ts.Add(jitter).UnixNano(), pod: "pod-" + strconv.Itoa(i), msg: fmt.Sprintf("%*s", int(seed>>60), "")})
		}
	}
	return rows
}

func rankChunksRun(t *testing.T, rows []emulatedRow, query string, start, end time.Time, step time.Duration, rowLimit int) (int, string, [][3]int) {
	t.Helper()
	emu := &vlStatsRangeEmulator{t: t, rows: rows}
	srv := httptest.NewServer(emu)
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error", RangeMetricRowLimit: rowLimit})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	return rankChunksOn(t, p, query, start, end, step)
}

// rankChunksOn runs one ranked request on p and returns its status, body and the chunks its reads held.
func rankChunksOn(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) (int, string, [][3]int) {
	t.Helper()
	var chunks [][3]int
	slidingChunksObserve = func(first, last, rows int, ok bool) {
		if ok {
			chunks = append(chunks, [3]int{first, last, rows})
		}
	}
	defer func() { slidingChunksObserve = nil }()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	return rec.Code, rec.Body.String(), chunks
}

// A ranked recurrence whose grids hold more rows than one read may is ranked in time chunks: the answer is the one
// read's byte for byte, every sample is the exact window of its step, and no chunk read holds more rows than the
// bound.
// conformance: heavy-metric-fetch-bounds, limits/stats-row-limit-per-grid, semantics/ranked-parser-window-fine-step
func TestSlidingWindowChunks_RankedChunksMatchOneRead(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	start := end.Add(-30 * time.Minute)
	for _, tc := range []struct {
		fn            string
		step, window  time.Duration
		k             int
		withBytes     bool
		tumbling, all bool
		by            string // the ranking's grouping: k series per group
	}{
		{fn: "count_over_time", step: 10 * time.Second, window: 5 * time.Minute, k: 3},
		{fn: "rate", step: 7 * time.Second, window: 5 * time.Minute, k: 2},
		{fn: "bytes_over_time", step: 10 * time.Second, window: 2 * time.Minute, k: 4, withBytes: true},
		{fn: "count_over_time", step: 30 * time.Second, window: 30 * time.Second, k: 3, tumbling: true},
		{fn: "count_over_time", step: 10 * time.Second, window: 5 * time.Minute, k: 2, by: "by (ns)"},
		{fn: "rate", step: 10 * time.Second, window: 2 * time.Minute, k: 2, by: "without (pod)"},
	} {
		t.Run(fmt.Sprintf("%s step=%s window=%s", tc.fn, tc.step, tc.window), func(t *testing.T) {
			rows := chunkRows(start.Add(-2*tc.window), end.Add(tc.step), 40)
			query := fmt.Sprintf(`topk %s (%d, sum by (pod) (%s({app="api"}[%s])))`, tc.by, tc.k, tc.fn, tc.window)
			if tc.by != "" {
				query = fmt.Sprintf(`topk %s (%d, sum by (ns, pod) (%s({app="api"}[%s])))`, tc.by, tc.k, tc.fn, tc.window)
			}
			code, whole, one := rankChunksRun(t, rows, query, start, end, tc.step, 1_000_000)
			if code != http.StatusOK || len(one) != 1 {
				t.Fatalf("one read: status %d, %d chunks: %.300s", code, len(one), whole)
			}
			const bound = 400
			code, chunked, chunks := rankChunksRun(t, rows, query, start, end, tc.step, bound)
			if code != http.StatusOK {
				t.Fatalf("chunked: status %d: %.300s", code, chunked)
			}
			read := 0
			for _, c := range chunks {
				if c[2] > 2*bound {
					t.Errorf("chunk %v holds more rows than two reads of the bound", c)
				}
				if c[0] != read {
					t.Fatalf("chunk %v does not start where the one before ended (%d)", c, read)
				}
				read = c[1]
			}
			if len(chunks) < 3 || read != int(end.Sub(start)/tc.step)+map[bool]int{true: 1}[tc.tumbling] {
				t.Fatalf("chunks %v do not cover the steps", chunks)
			}
			if chunked != whole {
				t.Fatalf("the chunked answer differs from the one read:\n%.400s\n%.400s", chunked, whole)
			}
			want := expectedWindowMatrix(rows, tc.fn, true, start, end, tc.step, tc.window)
			exact := map[string]string{}
			for id, points := range want {
				for _, pt := range points {
					exact[id+"@"+pt[0]] = pt[1]
				}
			}
			got := decodeMatrix(t, []byte(chunked))
			samples := 0
			for id, points := range got {
				for _, pt := range points {
					samples++
					if exact[id+"@"+pt[0]] != pt[1] {
						t.Fatalf("%s at %s: %s, the window holds %s", id, pt[0], pt[1], exact[id+"@"+pt[0]])
					}
				}
			}
			groups := 1
			if tc.by != "" {
				groups = 3 // ns-0, ns-1, ns-2
			}
			if steps := int(end.Sub(start)/tc.step) + 1; samples != steps*tc.k*groups {
				t.Fatalf("%d samples, want %d (k per step and group)", samples, steps*tc.k*groups)
			}
		})
	}
}

// The case of issue #1795: a topk over a windowed metric with a parser stage, whose bucket read over the range and
// the window passes the stats body bound, is ranked from the window grids. Before, the request went on to the raw rows
// of the window, past the buffered body bound: Loki's message-size error.
// conformance: semantics/ranked-parser-window-fine-step, heavy-metric-fetch-bounds
func TestSlidingWindowChunks_ParserRankingPastTheBodyBound(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	start := end.Add(-80 * time.Second)
	step, window := time.Second, time.Hour
	rows := chunkRows(start.Add(-window-time.Minute), end.Add(step), 30)
	emu := &vlStatsRangeEmulator{t: t, rows: rows}
	ranges := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			// A bucket answer past the bound, as a high-cardinality grouping's answer is.
			ranges++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` + strings.Repeat(`{"metric":{"pod":"x"},"values":[[1,"1"]]},`, 4096) + `{"metric":{},"values":[]}]}}`))
			return
		}
		_ = r.ParseForm()
		if r.URL.Path == "/select/logsql/query" && !strings.Contains(r.Form.Get("query"), "| stats by (_time:") {
			// Raw rows of the hour: more than the buffered body bound.
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(strings.Repeat(`{"_time":"2026-09-15T08:00:00Z","_stream":"{app=\"api\"}","_msg":"pod-1"}`+"\n", 4096)))
			return
		}
		emu.ServeHTTP(w, r)
	}))
	defer srv.Close()
	saved := maxStatsBucketResponseBytes
	maxStatsBucketResponseBytes = 64 << 10
	defer func() { maxStatsBucketResponseBytes = saved }()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error", MaxStatsQuerySeries: 100000,
		ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 64 << 10}})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	params := url.Values{
		"query": {"topk(3, sum by (pod) (count_over_time({app=\"api\"} | regexp `(?P<pod>pod-\\d+)` [1h])))"},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"1"},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %.300s", rec.Code, rec.Body.String())
	}
	got := decodeMatrix(t, rec.Body.Bytes())
	want := expectedWindowMatrix(rows, "count_over_time", true, start, end, step, window)
	samples := 0
	for id, points := range got {
		exact := map[string]string{}
		for _, pt := range want[id] {
			exact[pt[0]] = pt[1]
		}
		for _, pt := range points {
			samples++
			if exact[pt[0]] != pt[1] {
				t.Fatalf("%s at %s: %s, the window holds %s", id, pt[0], pt[1], exact[pt[0]])
			}
		}
	}
	if samples != 81*3 || ranges != 1 {
		t.Fatalf("%d samples (want 243), %d bucket reads (want the one that overflowed), %d raw-row reads", samples, ranges, emu.rawCalls)
	}
}

// The error log line names the step and bucket grid of a range metric read (issue #1795): the client text stays
// Loki's.
// conformance: heavy-metric-fetch-bounds
func TestReadGridAttrsNameStepAndBuckets(t *testing.T) {
	ctx := withReadGridNote(t.Context())
	if attrs := readGridAttrs(ctx, nil); len(attrs) != 0 {
		t.Fatalf("a request whose evaluator chose no grid logs %v", attrs)
	}
	start := time.Unix(1_700_000_000, 0)
	noteReadGrid(ctx, "buckets", 53*time.Millisecond, time.Hour, time.Millisecond, readGridBuckets(start, start.Add(80*time.Second), time.Hour, time.Millisecond))
	got := fmt.Sprint(readGridAttrs(ctx, nil))
	if want := "[read_route=buckets query_step=53ms range_window=1h0m0s read_bucket=1ms read_buckets_per_series=3680000]"; got != want {
		t.Fatalf("attrs %s, want %s", got, want)
	}
	// The route the evaluator went on to replaces the note: the step-sized window grids.
	noteReadGrid(ctx, "window-grids", 53*time.Millisecond, time.Hour, 53*time.Millisecond, 2*1509+1)
	if got, want := fmt.Sprint(readGridAttrs(ctx, nil)), "[read_route=window-grids query_step=53ms range_window=1h0m0s read_bucket=53ms read_buckets_per_series=3019]"; got != want {
		t.Fatalf("attrs %s, want %s", got, want)
	}
	if attrs := readGridAttrs(t.Context(), nil); len(attrs) != 0 {
		t.Fatalf("a request without a note logs %v", attrs)
	}
}

// FuzzSlidingWindowChunks: a ranking over random rows, step, window, function, k and row bound answers, in time
// chunks, byte for byte what one read answers, or the row limit's error when a single step's buckets pass the bound.
func FuzzSlidingWindowChunks(f *testing.F) {
	for _, seed := range []int64{1, 7, 42, 1234} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) {
		rng := rand.New(rand.NewSource(seed))
		end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
		steps := []time.Duration{time.Second, 3 * time.Second, 10 * time.Second, 53 * time.Millisecond * 100}
		step := steps[rng.Intn(len(steps))]
		windows := []time.Duration{step, 2 * step, time.Minute, 5 * time.Minute}
		window := windows[rng.Intn(len(windows))]
		start := end.Add(-time.Duration(20+rng.Intn(100)) * step)
		var rows []emulatedRow
		for i, pods := 0, 2+rng.Intn(25); i < pods; i++ {
			every := time.Duration(500+rng.Intn(20_000)) * time.Millisecond
			for ts := start.Add(-window - time.Duration(rng.Intn(5000))*time.Millisecond); ts.Before(end.Add(step)); ts = ts.Add(every) {
				rows = append(rows, emulatedRow{ts: ts.UnixNano(), pod: "pod-" + strconv.Itoa(i), msg: strings.Repeat("x", rng.Intn(4))})
			}
		}
		fn := []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"}[rng.Intn(4)]
		// A ranking of one group, and one per group (topk by keeps k series of each group's heaps).
		rank := []string{"topk", "bottomk", "topk by (ns)", "bottomk without (pod)", "topk by (pod)"}[rng.Intn(5)]
		query := fmt.Sprintf(`%s(%d, sum by (ns, pod) (%s({app="api"}[%dms])))`, rank, 1+rng.Intn(4), fn, window.Milliseconds())
		bound := 20 + rng.Intn(300)
		wholeCode, whole, _ := rankChunksRun(t, rows, query, start, end, step, 1_000_000)
		code, chunked, _ := rankChunksRun(t, rows, query, start, end, step, bound)
		switch {
		case wholeCode != http.StatusOK:
			t.Fatalf("one read of %s answered %d: %.200s", query, wholeCode, whole)
		case code == http.StatusBadRequest && chunked == "limit reached while evaluating the query":
			// A single step's buckets, or the window's series, pass the bound.
		case code != http.StatusOK || chunked != whole:
			t.Fatalf("%s at a bound of %d rows: %d %.300s\none read: %.300s", query, bound, code, chunked, whole)
		}
	})
}

// A request whose grids passed the row bound starts its repeat at a chunk size a read held: the repeat reads no
// chunk that is cut again, and answers the same.
// conformance: heavy-metric-fetch-bounds, limits/stats-row-limit-per-grid
func TestSlidingWindowChunks_RepeatStartsAtTheRememberedSize(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	start := end.Add(-30 * time.Minute)
	rows := chunkRows(start.Add(-10*time.Minute), end.Add(10*time.Second), 40)
	emu := &vlStatsRangeEmulator{t: t, rows: rows}
	srv := httptest.NewServer(emu)
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error", RangeMetricRowLimit: 400})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	query := `topk(3, sum by (pod) (count_over_time({app="api"}[5m])))`
	reads := func() (int, string, int) {
		emu.mu.Lock()
		before := len(emu.calls)
		emu.mu.Unlock()
		code, body, _ := rankChunksOn(t, p, query, start, end, 10*time.Second)
		emu.mu.Lock()
		defer emu.mu.Unlock()
		return code, body, len(emu.calls) - before
	}
	code1, first, calls1 := reads()
	code2, second, calls2 := reads()
	if code1 != http.StatusOK || code2 != http.StatusOK || first != second {
		t.Fatalf("statuses %d, %d; same answer %v", code1, code2, first == second)
	}
	if calls2 >= calls1 {
		t.Fatalf("the repeat made %d VictoriaLogs calls, the first %d: it should skip the reads cut on overflow", calls2, calls1)
	}
	t.Logf("first request %d VictoriaLogs calls, repeat %d", calls1, calls2)
}

// The series a ranking names may pass the row bound when every read holds fewer rows than it: the seed's series and
// the series new in the range together, as one read before the chunks answered them.
// conformance: heavy-metric-fetch-bounds, limits/stats-row-limit-per-grid
func TestSlidingWindowChunks_SeriesOfSeveralReadsPassTheRowBound(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	start := end.Add(-10 * time.Minute)
	var rows []emulatedRow
	for i := 0; i < 40; i++ {
		rows = append(rows, emulatedRow{ts: start.Add(-time.Duration(i+1) * 5 * time.Second).UnixNano(), pod: "seed-" + strconv.Itoa(i)})
		rows = append(rows, emulatedRow{ts: start.Add(time.Duration(i+1) * 13 * time.Second).UnixNano(), pod: "new-" + strconv.Itoa(i)})
	}
	query := `topk(2, sum by (pod) (count_over_time({app="api"}[5m])))`
	code, body, _ := rankChunksRun(t, rows, query, start, end, time.Minute, 50)
	_, whole, _ := rankChunksRun(t, rows, query, start, end, time.Minute, 1_000_000)
	if code != http.StatusOK || body != whole {
		t.Fatalf("80 series over reads of at most 40 rows, a bound of 50: status %d, the one-read answer %v: %.200s", code, body == whole, body)
	}
}

// The remembered chunk size is a hint read from the local cache: a value past the request's buckets (up to the
// largest int64), zero, negative or not a number starts at the whole range; a smaller one starts there.
// conformance: heavy-metric-fetch-bounds
func TestSlidingWindowChunks_StartSizeHintIsBounded(t *testing.T) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for _, tc := range []struct {
		hint string
		want int
	}{
		{"9223372036854775807", 100}, {"18446744073709551615", 100}, {"101", 100}, {"100", 100},
		{"40", 40}, {"1", 1}, {"0", 100}, {"-5", 100}, {"x", 100},
	} {
		key := "sliding-window-chunk:test:" + tc.hint
		p.setLocalReadCacheWithTTL(key, []byte(tc.hint), time.Minute)
		if got := p.slidingChunkStartSize(ctx, key, 100); got != tc.want {
			t.Errorf("hint %q: start size %d, want %d", tc.hint, got, tc.want)
		}
	}
	if got := p.slidingChunkStartSize(ctx, "sliding-window-chunk:test:none", 0); got != 1 {
		t.Errorf("no buckets: start size %d, want 1", got)
	}
}
