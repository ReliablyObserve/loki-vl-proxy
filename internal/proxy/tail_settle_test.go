package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// settleRow is a row of the VictoriaLogs live tail emulator: stored at once,
// searchable from visible on.
type settleRow struct {
	ts, visible time.Time
	msg         string
}

// settleVL emulates VictoriaLogs' read side as the tail sees it
// (app/vlselect/logsql ProcessLiveTailRequest and tailProcessor, v1.49.0 and
// v1.53.0):
//   - an ingested row becomes searchable only when its rows-buffer shard
//     flushes (lib/logstorage/datadb.go rowsBuffer: shards filled round-robin,
//     each flushed a second after its first row), so rows of one stream can
//     become visible out of order;
//   - the live tail reads [now-offset-start_offset, now-offset] first, then
//     every refresh interval [previous end - 5s, now-offset], and skips each
//     row of a stream at or below the newest one of that stream it already
//     sent;
//   - /select/logsql/query returns the visible rows of [start, end], oldest
//     first, up to limit.
//
// The refresh interval is shortened (VictoriaLogs' default is one second) so
// the tests run in seconds; the loss does not depend on it. overlap is the 5s
// each later read reaches back (tailOffsetNsecs); a test shortens it to see a
// resume that would rely on it.
type settleVL struct {
	srv     *httptest.Server
	refresh time.Duration
	overlap time.Duration

	mu      sync.Mutex
	rows    []settleRow
	offsets []string // the offset of each tail call

	gen  atomic.Int64 // bumped to end the open tail streams
	down atomic.Bool  // tail calls answer 503 (a restarting backend)
}

func newSettleVL(t *testing.T) *settleVL {
	t.Helper()
	v := &settleVL{refresh: 50 * time.Millisecond, overlap: 5 * time.Second}
	v.srv = httptest.NewServer(v)
	t.Cleanup(v.srv.Close)
	return v
}

// push stores a row stamped now that becomes searchable after delay.
func (v *settleVL) push(msg string, delay time.Duration) {
	now := time.Now()
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rows = append(v.rows, settleRow{ts: now, visible: now.Add(delay), msg: msg})
}

func (v *settleVL) tailOffsets() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.offsets...)
}

// visibleIn returns the rows of [start, end] searchable now, oldest first.
func (v *settleVL) visibleIn(start, end int64) []settleRow {
	now := time.Now()
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []settleRow
	for _, r := range v.rows {
		if ts := r.ts.UnixNano(); ts >= start && ts <= end && !now.Before(r.visible) {
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ts.Before(out[j].ts) })
	return out
}

func settleRowJSON(r settleRow) string {
	return fmt.Sprintf(`{"_time":"%s","_stream":"{app=\"api\"}","app":"api","_msg":%q}`+"\n", r.ts.UTC().Format(time.RFC3339Nano), r.msg)
}

// settleDuration parses the durations the proxy sends ("5000ms", "0s").
func settleDuration(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	return time.ParseDuration(s)
}

func (v *settleVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	q := r.URL.Query()
	switch r.URL.Path {
	case "/select/logsql/tail":
		v.serveTail(w, r)
	case "/select/logsql/query":
		if q.Get("limit") == "1" { // the probe before the upgrade
			w.WriteHeader(http.StatusNoContent)
			return
		}
		start, _ := parseEntryTime(q.Get("start"))
		end, _ := parseEntryTime(q.Get("end"))
		limit, _ := strconv.Atoi(q.Get("limit"))
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i, row := range v.visibleIn(start.UnixNano(), end.UnixNano()) {
			if limit > 0 && i >= limit {
				break
			}
			_, _ = io.WriteString(w, settleRowJSON(row))
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (v *settleVL) serveTail(w http.ResponseWriter, r *http.Request) {
	if v.down.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	offset, err := settleDuration(q.Get("offset"), 5*time.Second)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	startOffset, err := settleDuration(q.Get("start_offset"), 5*time.Second)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	v.mu.Lock()
	v.offsets = append(v.offsets, q.Get("offset"))
	v.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher := w.(http.Flusher)
	flusher.Flush()

	gen := v.gen.Load()
	lastTimestamp := map[string]int64{} // one stream here
	end := time.Now().Add(-offset).UnixNano()
	start := end - startOffset.Nanoseconds()
	tick := time.NewTicker(v.refresh)
	defer tick.Stop()
	for {
		rows := v.visibleIn(start, end)
		last, ok := lastTimestamp["api"]
		for _, row := range rows {
			if ok && row.ts.UnixNano() <= last {
				continue // VictoriaLogs: "Skip already written rows"
			}
			_, _ = io.WriteString(w, settleRowJSON(row))
			lastTimestamp["api"] = row.ts.UnixNano()
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
		if v.gen.Load() != gen {
			return
		}
		start = end - v.overlap.Nanoseconds()
		end = time.Now().Add(-offset).UnixNano()
	}
}

// settleProxy is a proxy in front of vl reading settle behind now.
func settleProxy(t *testing.T, vl *settleVL, mode TailMode, settle time.Duration) string {
	t.Helper()
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: vl.srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, TailMode: mode, TailBackendRetryWindow: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	p.tailSettle = settle
	p.tailRetryInitialBackoff = 10 * time.Millisecond
	p.tailRetryMaxBackoff = 40 * time.Millisecond
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(mux)
	t.Cleanup(front.Close)
	return "ws" + strings.TrimPrefix(front.URL, "http") + `/loki/api/v1/tail?query=` + url.QueryEscape(`{app="api"}`)
}

// settleWaitLive pushes a row stamped now until one reaches the client: the
// tail is live from then on.
func settleWaitLive(t *testing.T, vl *settleVL, c *retryClient) {
	t.Helper()
	for i := 0; ; i++ {
		if i == 50 {
			t.Fatalf("the tail never went live (seen %v)", c.seen)
		}
		vl.push(fmt.Sprintf("ready%d", i), 0)
		c.readFor(200*time.Millisecond, func() bool {
			for m := range c.seen {
				if strings.HasPrefix(m, "ready") {
					return true
				}
			}
			return false
		})
		for m := range c.seen {
			if strings.HasPrefix(m, "ready") {
				return
			}
		}
	}
}

// pushSettleFixture pushes the e2e fixture's shape: seven rows of one stream,
// 150 ms apart, the second to fourth landing in a buffer shard that flushes
// late (searchable after late), the rest at once.
func pushSettleFixture(vl *settleVL, late time.Duration) []string {
	var msgs []string
	for i := 1; i <= 7; i++ {
		msg := fmt.Sprintf("row%d", i)
		delay := 20 * time.Millisecond
		if i >= 2 && i <= 4 {
			delay = late
		}
		vl.push(msg, delay)
		msgs = append(msgs, msg)
		time.Sleep(150 * time.Millisecond)
	}
	return msgs
}

// Loki's tail delivers every line pushed after it started, in order. Rows that
// VictoriaLogs makes searchable out of order (the second to fourth row of a
// stream visible after the fifth) reach the client too: the native tail reads
// only rows older than the settle delay, the polled tail reads that span again
// and drops the rows it already sent. With the settle delay at 0 the native
// tail delivers rows 1, 5, 6 and 7 only, as in the CI failure of
// TestFeature_Tail_PipelineParity.
// conformance: semantics/tail-delivers-rows-visible-out-of-order, tail-reads-settled-rows, loki_api_v1_tail
func TestTailSettle_RowsVisibleOutOfOrderAllArrive(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   TailMode
		late   time.Duration // how long rows 2-4 stay unsearchable
		settle time.Duration
		wait   time.Duration
	}{
		// The native stream reads every 50 ms: rows 2-4 hidden for 600 ms are
		// behind row 5 at any read reaching up to now.
		{"native", TailModeNative, 600 * time.Millisecond, time.Second, 6 * time.Second},
		{"auto", TailModeAuto, 600 * time.Millisecond, time.Second, 6 * time.Second},
		// The polled tail reads every 2 s: rows 2-4 hidden for 2.6 s are
		// behind row 5 at one poll at least.
		{"polling", TailModeSynthetic, 2600 * time.Millisecond, 3 * time.Second, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vl := newSettleVL(t)
			c := dialRetryClient(t, settleProxy(t, vl, tc.mode, tc.settle))
			settleWaitLive(t, vl, c)
			want := pushSettleFixture(vl, tc.late)
			c.readFor(tc.wait, func() bool {
				for _, m := range want {
					if c.seen[m] == 0 {
						return false
					}
				}
				return true
			})
			var missing []string
			for _, m := range want {
				if c.seen[m] != 1 {
					missing = append(missing, fmt.Sprintf("%s×%d", m, c.seen[m]))
				}
			}
			if len(missing) > 0 {
				t.Fatalf("lines not delivered once: %v (seen %v)", missing, c.seen)
			}
		})
	}
}

// The native tail asks VictoriaLogs for its settle delay as the offset, and
// the default is VictoriaLogs' own: at least a rows-buffer flush (1 s) plus a
// tail refresh (1 s).
// conformance: semantics/tail-delivers-rows-visible-out-of-order, tail-reads-settled-rows
func TestTailSettle_NativeOffsetIsTheSettleDelay(t *testing.T) {
	if tailSettleDelay < 2*time.Second {
		t.Fatalf("tailSettleDelay %v is below a rows-buffer flush plus a tail refresh", tailSettleDelay)
	}
	vl := newSettleVL(t)
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: vl.srv.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if p.tailSettle != tailSettleDelay {
		t.Fatalf("default settle %v, want %v", p.tailSettle, tailSettleDelay)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resp, native, err := p.openNativeTailStream(ctx, `{app="api"}`)
	if err != nil || !native {
		t.Fatalf("native tail: %v %v", native, err)
	}
	cancel()
	_ = resp.Body.Close()
	if got := vl.tailOffsets(); len(got) != 1 || got[0] != "5000ms" {
		t.Fatalf("tail offsets %v, want [5000ms]", got)
	}
}

// A native stream reopened after an outage, with the settle delay on, loses
// none of the rows written before, during and after the outage, and repeats
// none. The rows of the last settle delay before the failure were not sent:
// the resumed read (start_offset from the last delivered row, ending a settle
// delay before now) must cover them, since the outage outlasts the span
// VictoriaLogs' later reads reach back.
// conformance: semantics/tail-delivers-rows-visible-out-of-order, tail-reads-settled-rows, semantics/tail-survives-backend-outage
func TestTailSettle_ReopenedStreamLosesNothing(t *testing.T) {
	vl := newSettleVL(t)
	vl.overlap = 250 * time.Millisecond
	c := dialRetryClient(t, settleProxy(t, vl, TailModeNative, time.Second))
	settleWaitLive(t, vl, c)
	vl.push("before1", 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	vl.push("before2", 20*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	// VictoriaLogs goes away for longer than the settle delay plus the
	// overlap of its later reads.
	vl.down.Store(true)
	vl.gen.Add(1)
	time.Sleep(200 * time.Millisecond)
	vl.push("during", 20*time.Millisecond)
	time.Sleep(1300 * time.Millisecond)
	vl.down.Store(false)
	vl.push("after", 20*time.Millisecond)
	want := []string{"before1", "before2", "during", "after"}
	c.waitFor(8*time.Second, want...)
	c.readFor(1500*time.Millisecond, nil) // a repeat would show here
	for _, m := range want {
		if c.seen[m] != 1 {
			t.Fatalf("line %q delivered %d times, want once (seen %v)", m, c.seen[m], c.seen)
		}
	}
	if n := len(vl.tailOffsets()); n < 2 {
		t.Fatalf("the stream was not reopened (%d tail calls)", n)
	}
}
