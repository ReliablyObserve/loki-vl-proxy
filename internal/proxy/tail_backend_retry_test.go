package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// retryRow is a log row of the fake VictoriaLogs.
type retryRow struct {
	ts     time.Time
	msg    string
	hidden bool // stored, but a read does not return it yet (a late row)
}

// retryVL is a fake VictoriaLogs for the tail's backend outage tests: it
// stores rows, serves the native tail stream (honouring start_offset) and the
// polled query, and fails on demand the way a restarting backend does.
type retryVL struct {
	srv *httptest.Server

	mu       sync.Mutex
	rows     []retryRow
	offsets  []time.Duration // start_offset of each native tail call that carried one
	mode     string          // "": healthy; down-close, down-503: unreachable; 400, malformed, oversize: the tail and the poll answer so
	firstBad time.Time       // the first failed call of the current outage

	tailCalls atomic.Int64 // calls to the native tail endpoint
	pollCalls atomic.Int64 // polled queries (the probe before the upgrade excluded)
	badCalls  atomic.Int64 // calls that failed
	gen       atomic.Int64 // bumped to end the open native streams
	abrupt    atomic.Bool  // end them mid-stream instead of cleanly
	// resumeDelay is how long a reopened stream (one that carries start_offset)
	// takes to send its first rows after its headers, as a backend that has just
	// restarted does while it reads the resumed window.
	resumeDelay atomic.Int64 // nanoseconds
}

func newRetryVL(t *testing.T) *retryVL {
	t.Helper()
	v := &retryVL{}
	v.srv = httptest.NewServer(v)
	t.Cleanup(v.srv.Close)
	return v
}

func (v *retryVL) add(msg string, ts time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rows = append(v.rows, retryRow{ts: ts, msg: msg})
}

func (v *retryVL) addHidden(msg string, ts time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rows = append(v.rows, retryRow{ts: ts, msg: msg, hidden: true})
}

func (v *retryVL) reveal(msg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for i := range v.rows {
		if v.rows[i].msg == msg {
			v.rows[i].hidden = false
		}
	}
}

func (v *retryVL) setMode(mode string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.mode = mode
	v.firstBad = time.Time{}
}

func (v *retryVL) getMode() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.mode
}

// breakStreams ends the open native streams: cleanly (the handler returns),
// or abruptly (the connection is cut mid-body).
func (v *retryVL) breakStreams(abrupt bool) {
	v.abrupt.Store(abrupt)
	v.gen.Add(1)
}

func (v *retryVL) lastOffset() (time.Duration, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.offsets) == 0 {
		return 0, false
	}
	return v.offsets[len(v.offsets)-1], true
}

func (v *retryVL) failedAt() time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.firstBad
}

func (v *retryVL) rowJSON(r retryRow) string {
	return fmt.Sprintf(`{"_time":"%s","_stream":"{app=\"api\"}","app":"api","_msg":%q}`+"\n", r.ts.UTC().Format(time.RFC3339Nano), r.msg)
}

func retryHijackClose(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			_ = c.Close()
		}
	}
}

// fail answers a call the way the current mode says, and reports whether it did.
func (v *retryVL) fail(w http.ResponseWriter, probe bool) bool {
	mode := v.getMode()
	switch mode {
	case "down-close", "down-503":
	case "400", "malformed", "oversize":
		if probe {
			return false
		}
	default:
		return false
	}
	v.badCalls.Add(1)
	v.mu.Lock()
	if v.firstBad.IsZero() {
		v.firstBad = time.Now()
	}
	v.mu.Unlock()
	switch mode {
	case "down-close":
		retryHijackClose(w)
	case "down-503":
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "<html>503 Service Unavailable</html>")
	case "400":
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"cannot parse query"}`)
	case "malformed":
		_, _ = io.WriteString(w, `{"status":"success","data":{"result":[`+"\n")
	case "oversize":
		_, _ = io.WriteString(w, `{"_time":"`+time.Now().UTC().Format(time.RFC3339Nano)+`","_msg":"`+strings.Repeat("x", 1100*1024)+`"}`+"\n")
	}
	return true
}

func (v *retryVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	switch r.URL.Path {
	case "/select/logsql/tail":
		v.serveTail(w, r)
	case "/select/logsql/query":
		v.serveQuery(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (v *retryVL) serveQuery(w http.ResponseWriter, r *http.Request) {
	probe := r.URL.Query().Get("limit") == "1"
	if !probe {
		v.pollCalls.Add(1)
	}
	if !probe && v.getMode() == "cut" {
		// The body breaks after the first row and half of the second.
		v.badCalls.Add(1)
		v.mu.Lock()
		rows := append([]retryRow(nil), v.rows...)
		v.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i, row := range rows {
			line := v.rowJSON(row)
			if i == 1 {
				line = line[:len(line)/2]
			}
			_, _ = io.WriteString(w, line)
			if i == 1 {
				break
			}
		}
		w.(http.Flusher).Flush()
		retryHijackClose(w)
		return
	}
	if v.fail(w, probe) {
		return
	}
	if probe {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	start, _ := parseEntryTime(r.URL.Query().Get("start"))
	v.mu.Lock()
	rows := append([]retryRow(nil), v.rows...)
	v.mu.Unlock()
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].ts.Before(rows[j].ts) })
	w.Header().Set("Content-Type", "application/x-ndjson")
	n := 0
	for _, row := range rows {
		if row.hidden || row.ts.Before(start) || n >= 200 {
			continue
		}
		_, _ = io.WriteString(w, v.rowJSON(row))
		n++
	}
}

func (v *retryVL) serveTail(w http.ResponseWriter, r *http.Request) {
	v.tailCalls.Add(1)
	if v.fail(w, false) {
		return
	}
	offset := 5 * time.Second
	if s := r.URL.Query().Get("start_offset"); s != "" {
		ms, err := strconv.ParseInt(strings.TrimSuffix(s, "ms"), 10, 64)
		if err != nil || !strings.HasSuffix(s, "ms") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		offset = time.Duration(ms) * time.Millisecond
		v.mu.Lock()
		v.offsets = append(v.offsets, offset)
		v.mu.Unlock()
	}
	from := time.Now().Add(-offset)
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher := w.(http.Flusher)
	flusher.Flush()
	if d := time.Duration(v.resumeDelay.Load()); d > 0 && r.URL.Query().Get("start_offset") != "" {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}
	gen := v.gen.Load()
	sent := map[int]bool{}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if v.gen.Load() != gen {
			if v.abrupt.Load() {
				retryHijackClose(w)
			}
			return
		}
		v.mu.Lock()
		rows := append([]retryRow(nil), v.rows...)
		v.mu.Unlock()
		for i, row := range rows {
			if sent[i] || row.hidden || row.ts.Before(from) || row.ts.After(time.Now()) {
				continue
			}
			sent[i] = true
			_, _ = io.WriteString(w, v.rowJSON(row))
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}

// retryProxy starts a proxy in front of vl with the given tail mode and retry
// window, and short backoffs so a test runs in milliseconds. It returns the
// websocket URL of the tail and the number of tail handlers running.
func retryProxy(t *testing.T, vl *retryVL, mode TailMode, window time.Duration) (string, *atomic.Int64) {
	t.Helper()
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: vl.srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, TailMode: mode, TailBackendRetryWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	p.tailRetryInitialBackoff = 10 * time.Millisecond
	p.tailRetryMaxBackoff = 40 * time.Millisecond
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	inflight := &atomic.Int64{}
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inflight.Add(1)
		defer inflight.Add(-1)
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	return "ws" + strings.TrimPrefix(front.URL, "http") + `/loki/api/v1/tail?query=` + url.QueryEscape(`{app="api"}`), inflight
}

// retryClient is a tail client: it counts every line it receives. A reader
// goroutine owns the connection, because a websocket that hit a read deadline
// cannot be read again.
type retryClient struct {
	t        *testing.T
	conn     *websocket.Conn
	msgs     chan []byte
	errc     chan error
	seen     map[string]int
	closeErr *websocket.CloseError
	ended    bool // the connection ended (a close frame or a read error)
}

func dialRetryClient(t *testing.T, wsURL string) *retryClient {
	t.Helper()
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 8 * time.Second}).Dial(wsURL, http.Header{"X-Scope-OrgID": {"0"}})
	if err != nil {
		t.Fatalf("tail dial: %v (resp %v)", err, resp)
	}
	c := &retryClient{t: t, conn: conn, msgs: make(chan []byte, 1024), errc: make(chan error, 1), seen: map[string]int{}}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				c.errc <- err
				return
			}
			c.msgs <- msg
		}
	}()
	return c
}

func (c *retryClient) take(msg []byte) {
	var frame struct {
		Streams []struct {
			Values [][]string `json:"values"`
		} `json:"streams"`
	}
	if json.Unmarshal(msg, &frame) != nil {
		return
	}
	for _, s := range frame.Streams {
		for _, v := range s.Values {
			if len(v) > 1 {
				c.seen[v[1]]++
			}
		}
	}
}

// readFor reads until the time is up, the end of the connection, or done
// reports true.
func (c *retryClient) readFor(d time.Duration, done func() bool) {
	c.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for !c.ended && (done == nil || !done()) {
		select {
		case msg := <-c.msgs:
			c.take(msg)
		case err := <-c.errc:
			c.ended = true
			var ce *websocket.CloseError
			if errors.As(err, &ce) && ce.Code != websocket.CloseAbnormalClosure {
				c.closeErr = ce
			}
			for len(c.msgs) > 0 { // lines that arrived before the close
				c.take(<-c.msgs)
			}
		case <-timer.C:
			return
		}
	}
}

// waitFor reads until every message has arrived.
func (c *retryClient) waitFor(d time.Duration, msgs ...string) {
	c.t.Helper()
	c.readFor(d, func() bool {
		for _, m := range msgs {
			if c.seen[m] == 0 {
				return false
			}
		}
		return true
	})
	for _, m := range msgs {
		if c.seen[m] == 0 {
			c.t.Fatalf("line %q never arrived (seen %v, closed %v)", m, c.seen, c.closeErr)
		}
	}
}

func (c *retryClient) requireEachOnce(want ...string) {
	c.t.Helper()
	for _, m := range want {
		if c.seen[m] != 1 {
			c.t.Fatalf("line %q delivered %d times, want once (seen %v)", m, c.seen[m], c.seen)
		}
	}
	if len(c.seen) != len(want) {
		c.t.Fatalf("lines delivered %v, want exactly %v", c.seen, want)
	}
}

func retryWaitUntil(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A backend outage shorter than the retry window keeps the client connected:
// the native tail reopens from the last delivered line and every line arrives
// once, the ones written during the outage (and one at the last delivered
// timestamp that was not visible before) included; the polling tail resumes
// from its cursor the same way.
// conformance: semantics/tail-survives-backend-outage, loki_api_v1_tail
func TestTailRetry_ShortOutageKeepsTailAndLosesNothing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mode  TailMode
		down  string
		abort bool
	}{
		{"native/stream-ends", TailModeNative, "down-503", false},
		{"native/connection-cut", TailModeNative, "down-close", true},
		{"auto/stream-ends", TailModeAuto, "down-503", false},
		{"polling/refused", TailModeSynthetic, "down-close", false},
		{"polling/503", TailModeSynthetic, "down-503", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vl := newRetryVL(t)
			wsURL, _ := retryProxy(t, vl, tc.mode, 20*time.Second)
			c := dialRetryClient(t, wsURL)
			polling := tc.mode == TailModeSynthetic

			ts1 := time.Now()
			vl.add("m1", ts1)
			ts2 := time.Now()
			vl.add("m2", ts2)
			c.waitFor(8*time.Second, "m1", "m2")

			// VictoriaLogs goes away: the streams end and every call fails.
			vl.setMode(tc.down)
			vl.breakStreams(tc.abort)
			if !polling {
				// A row at the last delivered timestamp that the stream had not
				// shown: the resume delivers it, and drops m2 sent again.
				vl.addHidden("tie", ts2)
			}
			vl.add("gap1", time.Now())
			retryWaitUntil(t, "retries during the outage", 8*time.Second, func() bool { return vl.badCalls.Load() >= 3 })
			c.readFor(100*time.Millisecond, nil)
			if c.ended {
				t.Fatalf("the tail ended during an outage shorter than the window: close %v", c.closeErr)
			}
			vl.add("gap2", time.Now())
			vl.reveal("tie")
			vl.setMode("")

			want := []string{"m1", "m2", "gap1", "gap2"}
			if !polling {
				want = append(want, "tie")
			}
			c.waitFor(10*time.Second, want...)
			ts3 := time.Now()
			vl.add("m3", ts3)
			c.waitFor(8*time.Second, "m3")
			c.readFor(600*time.Millisecond, nil) // a duplicate would show here
			if c.ended {
				t.Fatalf("the tail ended after the backend came back: close %v", c.closeErr)
			}
			c.requireEachOnce(append(want, "m3")...)
			if !polling {
				// The reopened stream asked VictoriaLogs for the span since the
				// last delivered line (about the time since m2 plus the margin).
				off, ok := vl.lastOffset()
				if !ok || off < tailResumeMargin || off > 8*time.Second {
					t.Fatalf("reopen start_offset = %v (%v), want between %v and 8s", off, ok, tailResumeMargin)
				}
			}
		})
	}
}

// A poll whose body breaks part-way is read again from the same cursor: the
// rows it had already read are delivered once, with the rest, after the retry.
// conformance: semantics/tail-survives-backend-outage, loki_api_v1_tail
func TestTailRetry_PollingBodyCutIsReadAgain(t *testing.T) {
	vl := newRetryVL(t)
	wsURL, _ := retryProxy(t, vl, TailModeSynthetic, 20*time.Second)
	c := dialRetryClient(t, wsURL)
	vl.setMode("cut")
	vl.add("a", time.Now())
	vl.add("b", time.Now())
	retryWaitUntil(t, "a cut poll", 8*time.Second, func() bool { return vl.badCalls.Load() >= 2 })
	vl.setMode("")
	c.waitFor(8*time.Second, "a", "b")
	c.readFor(600*time.Millisecond, nil)
	if c.ended {
		t.Fatalf("the tail ended: close %v", c.closeErr)
	}
	c.requireEachOnce("a", "b")
}

// The dedupe of a reopened stream starts with its first rows, not with the
// call: a backend that has just restarted answers the headers and spends
// seconds reading the resumed window, and the lines it sends again then are
// still dropped.
// conformance: semantics/tail-survives-backend-outage, loki_api_v1_tail
func TestTailRetry_SlowResumedReadStillDropsRepeatedLines(t *testing.T) {
	vl := newRetryVL(t)
	wsURL, _ := retryProxy(t, vl, TailModeNative, 20*time.Second)
	c := dialRetryClient(t, wsURL)
	vl.add("m1", time.Now())
	vl.add("m2", time.Now())
	c.waitFor(8*time.Second, "m1", "m2")
	vl.resumeDelay.Store(int64(tailResumeDedupeFor + 500*time.Millisecond))
	vl.setMode("down-503")
	vl.breakStreams(false)
	retryWaitUntil(t, "retries", 8*time.Second, func() bool { return vl.badCalls.Load() >= 2 })
	vl.add("gap", time.Now())
	vl.setMode("")
	c.waitFor(15*time.Second, "gap")
	c.readFor(500*time.Millisecond, nil)
	if c.ended {
		t.Fatalf("the tail ended: close %v", c.closeErr)
	}
	c.requireEachOnce("m1", "m2", "gap")
}

// An outage longer than the window ends the tail with Loki's 1011 and text,
// once the window is spent and not before.
// conformance: semantics/tail-survives-backend-outage, semantics/error-layer-internal-go-text, loki_api_v1_tail
func TestTailRetry_OutageLongerThanWindowClosesWithLokiText(t *testing.T) {
	const window = 700 * time.Millisecond
	for _, tc := range []struct {
		name string
		mode TailMode
		down string
	}{
		{"native/refused", TailModeNative, "down-close"},
		{"native/503", TailModeNative, "down-503"},
		{"polling/refused", TailModeSynthetic, "down-close"},
		{"polling/503", TailModeSynthetic, "down-503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vl := newRetryVL(t)
			wsURL, inflight := retryProxy(t, vl, tc.mode, window)
			c := dialRetryClient(t, wsURL)
			vl.add("m1", time.Now())
			c.waitFor(8*time.Second, "m1")
			vl.setMode(tc.down)
			vl.breakStreams(true)
			c.readFor(10*time.Second, nil)
			closedAt := time.Now()
			if c.closeErr == nil || c.closeErr.Code != websocket.CloseInternalServerErr || c.closeErr.Text != lokiErrInternal {
				t.Fatalf("want close 1011 %q, got %v", lokiErrInternal, c.closeErr)
			}
			// The first failed call opens the window; the last attempt is made
			// when it closes (an attempt in flight may overrun it by the tail
			// transport's 5s header timeout, which a fake that answers at once
			// never reaches).
			outage := closedAt.Sub(vl.failedAt())
			if outage < window-100*time.Millisecond || outage > window+2*time.Second {
				t.Fatalf("closed %v after the first failed call, want about the %v window", outage, window)
			}
			if vl.badCalls.Load() < 3 {
				t.Fatalf("only %d failed calls: the tail did not retry", vl.badCalls.Load())
			}
			retryWaitUntil(t, "the handler to return", 3*time.Second, func() bool { return inflight.Load() == 0 })
			calls := vl.badCalls.Load()
			time.Sleep(300 * time.Millisecond)
			if after := vl.badCalls.Load(); after != calls {
				t.Fatalf("calls continued after the close: %d, then %d", calls, after)
			}
		})
	}
}

// A failure a retry cannot fix ends the tail at once, with a 30s window: a
// row that is not JSON, a line above the scanner's cap, and a 4xx answer
// (VictoriaLogs rejects the query), on the first call or on a reopen.
// conformance: semantics/tail-survives-backend-outage, semantics/error-layer-internal-go-text, loki_api_v1_tail
func TestTailRetry_PermanentFailuresCloseAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     TailMode
		failure  string
		afterRun bool // the failure starts after a healthy stream
		calls    func(*retryVL) int64
		wantCall int64
	}{
		{"native/malformed-row", TailModeNative, "malformed", false, func(v *retryVL) int64 { return v.tailCalls.Load() }, 1},
		{"native/oversize-line", TailModeNative, "oversize", false, func(v *retryVL) int64 { return v.tailCalls.Load() }, 1},
		{"native/4xx-first-call", TailModeNative, "400", false, func(v *retryVL) int64 { return v.tailCalls.Load() }, 1},
		{"native/4xx-on-reopen", TailModeNative, "400", true, func(v *retryVL) int64 { return v.tailCalls.Load() }, 2},
		{"polling/malformed-body", TailModeSynthetic, "malformed", false, func(v *retryVL) int64 { return v.pollCalls.Load() }, 1},
		{"polling/4xx", TailModeSynthetic, "400", false, func(v *retryVL) int64 { return v.pollCalls.Load() }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vl := newRetryVL(t)
			wsURL, inflight := retryProxy(t, vl, tc.mode, 30*time.Second)
			if !tc.afterRun {
				vl.setMode(tc.failure)
			}
			start := time.Now()
			c := dialRetryClient(t, wsURL)
			if tc.afterRun {
				vl.add("m1", time.Now())
				c.waitFor(8*time.Second, "m1")
				start = time.Now()
				vl.setMode(tc.failure)
				vl.breakStreams(false)
			}
			c.readFor(6*time.Second, nil)
			if c.closeErr == nil || c.closeErr.Code != websocket.CloseInternalServerErr || c.closeErr.Text != lokiErrInternal {
				t.Fatalf("want close 1011 %q, got %v (ended %v)", lokiErrInternal, c.closeErr, c.ended)
			}
			if took := time.Since(start); took > 4*time.Second {
				t.Fatalf("closed after %v, want at once (the window is 30s)", took)
			}
			if got := tc.calls(vl); got != tc.wantCall {
				t.Fatalf("%d calls to VictoriaLogs, want %d: a failure no retry fixes is not retried", got, tc.wantCall)
			}
			retryWaitUntil(t, "the handler to return", 3*time.Second, func() bool { return inflight.Load() == 0 })
		})
	}
}

// A client that leaves while the tail waits to retry releases everything: the
// handler returns, the retry goroutines are gone and VictoriaLogs is not called
// again.
// conformance: semantics/tail-survives-backend-outage, semantics/error-layer-cancel-deadline, loki_api_v1_tail
func TestTailRetry_ClientDisconnectDuringRetryReleasesEverything(t *testing.T) {
	for _, mode := range []TailMode{TailModeNative, TailModeSynthetic} {
		t.Run(string(mode), func(t *testing.T) {
			vl := newRetryVL(t)
			wsURL, inflight := retryProxy(t, vl, mode, 30*time.Second)
			c := dialRetryClient(t, wsURL)
			vl.add("m1", time.Now())
			c.waitFor(8*time.Second, "m1")
			vl.setMode("down-close")
			vl.breakStreams(true)
			retryWaitUntil(t, "retries", 8*time.Second, func() bool { return vl.badCalls.Load() >= 3 })
			_ = c.conn.Close()
			retryWaitUntil(t, "the tail handler to return", 3*time.Second, func() bool { return inflight.Load() == 0 })
			retryWaitUntil(t, "the retry goroutines to stop", 3*time.Second, func() bool {
				buf := make([]byte, 1<<20)
				stacks := string(buf[:runtime.Stack(buf, true)])
				return !strings.Contains(stacks, "streamNativeTail") && !strings.Contains(stacks, "streamSyntheticTail") && !strings.Contains(stacks, "forwardNativeTail") && !strings.Contains(stacks, "tailSleep")
			})
			time.Sleep(100 * time.Millisecond)
			calls := vl.badCalls.Load()
			time.Sleep(300 * time.Millisecond)
			if after := vl.badCalls.Load(); after != calls {
				t.Fatalf("VictoriaLogs was called after the client left: %d, then %d", calls, after)
			}
		})
	}
}

// With the window at 0 the tail closes on the first failed call, as before
// retries: a stream that ends, a poll that fails.
// conformance: semantics/tail-survives-backend-outage, semantics/error-layer-internal-go-text, loki_api_v1_tail
func TestTailRetry_WindowZeroClosesOnFirstFailure(t *testing.T) {
	for _, mode := range []TailMode{TailModeNative, TailModeSynthetic} {
		t.Run(string(mode), func(t *testing.T) {
			vl := newRetryVL(t)
			wsURL, _ := retryProxy(t, vl, mode, 0)
			c := dialRetryClient(t, wsURL)
			vl.add("m1", time.Now())
			c.waitFor(8*time.Second, "m1")
			vl.setMode("down-503")
			vl.breakStreams(false)
			c.readFor(8*time.Second, nil)
			if c.closeErr == nil || c.closeErr.Code != websocket.CloseInternalServerErr || c.closeErr.Text != lokiErrInternal {
				t.Fatalf("want close 1011 %q, got %v", lokiErrInternal, c.closeErr)
			}
			if n := vl.badCalls.Load(); n > 1 {
				t.Fatalf("%d failed calls with the window at 0, want at most the one that closed the tail", n)
			}
		})
	}
}

// pingCountConn records the control and ping frames a tail writes.
type pingCountConn struct {
	mu    sync.Mutex
	pings int
}

func (c *pingCountConn) SetWriteDeadline(time.Time) error { return nil }
func (c *pingCountConn) WriteMessage(mt int, _ []byte) error {
	if mt == websocket.PingMessage {
		c.mu.Lock()
		c.pings++
		c.mu.Unlock()
	}
	return nil
}
func (c *pingCountConn) WriteControl(int, []byte, time.Time) error { return nil }

// The wait before a retry keeps the client's connection alive with the ping
// ticks, stops at the client's cancel and reports a client that is gone.
// conformance: semantics/tail-survives-backend-outage, loki_api_v1_tail
func TestTailRetry_SleepPingsAndStopsWithTheClient(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	conn := &pingCountConn{}
	ping := make(chan time.Time)
	go func() {
		for i := 0; i < 3; i++ {
			ping <- time.Now()
		}
	}()
	if !p.tailSleep(context.Background(), conn, ping, 150*time.Millisecond) {
		t.Fatal("the sleep reported a gone client")
	}
	conn.mu.Lock()
	pings := conn.pings
	conn.mu.Unlock()
	if pings != 3 {
		t.Fatalf("%d pings during the wait, want 3", pings)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.tailSleep(ctx, conn, nil, time.Minute) {
		t.Fatal("the sleep outlived the client's cancel")
	}
	failing := &fakeTailConn{writeMessageErr: errors.New("gone")}
	tick := make(chan time.Time, 1)
	tick <- time.Now()
	if p.tailSleep(context.Background(), failing, tick, time.Minute) {
		t.Fatal("the sleep outlived a failed ping")
	}
}

// The backoff doubles from the initial wait to the cap with jitter, never
// passes the window, and ends it with the verdict the failure calls for.
// conformance: semantics/tail-survives-backend-outage
func TestTailRetry_BackoffAndVerdicts(t *testing.T) {
	now := time.Unix(1000, 0)
	r := &tailRetry{window: time.Hour, initial: 250 * time.Millisecond, max: 5 * time.Second}
	transient := io.ErrUnexpectedEOF
	prevCeiling := 250 * time.Millisecond
	for i := 0; i < 12; i++ {
		wait, verdict := r.next(transient, now)
		if verdict != tailRetryAgain {
			t.Fatalf("attempt %d: verdict %v, want retry", i, verdict)
		}
		ceiling := prevCeiling
		if wait < ceiling/2 || wait > ceiling {
			t.Fatalf("attempt %d: wait %v outside the jittered [%v, %v]", i, wait, ceiling/2, ceiling)
		}
		if prevCeiling *= 2; prevCeiling > 5*time.Second {
			prevCeiling = 5 * time.Second
		}
		now = now.Add(wait)
		if i >= 6 && ceiling != 5*time.Second {
			t.Fatalf("attempt %d: ceiling %v, want the 5s cap", i, ceiling)
		}
	}
	// A wait is cut to the end of the window, then the window is spent.
	r = &tailRetry{window: time.Second, initial: 5 * time.Second, max: 5 * time.Second}
	start := time.Unix(2000, 0)
	wait, verdict := r.next(transient, start)
	if verdict != tailRetryAgain || wait != time.Second {
		t.Fatalf("first wait %v (%v), want the window's 1s", wait, verdict)
	}
	if _, verdict := r.next(transient, start.Add(time.Second)); verdict != tailRetryExhausted {
		t.Fatalf("verdict %v at the window's end, want exhausted", verdict)
	}
	// Healthy again: the next outage has a full window.
	r.reset()
	if _, verdict := r.next(transient, start.Add(time.Hour)); verdict != tailRetryAgain {
		t.Fatalf("verdict %v after a reset, want retry", verdict)
	}
	// A failure no retry fixes, and a disabled window.
	if _, verdict := r.next(errTailMalformedRow, start); verdict != tailRetryPermanent {
		t.Fatalf("verdict %v for a malformed row, want permanent", verdict)
	}
	if _, verdict := (&tailRetry{}).next(transient, start); verdict != tailRetryOff {
		t.Fatalf("verdict %v with the window at 0, want off", verdict)
	}
}

// Which failed VictoriaLogs calls a tail retries.
// conformance: semantics/tail-survives-backend-outage
func TestTailRetry_Classifier(t *testing.T) {
	status := func(code int) error { return &upstreamStatusError{status: code, msg: "x"} }
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"nil", nil, false},
		{"connection refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"stream ended", errTailStreamEnded, true},
		{"timeout", context.DeadlineExceeded, true},
		{"circuit breaker open", errors.New("circuit breaker open — backend unavailable"), true},
		{"429", status(429), true},
		{"500", status(500), true},
		{"502", status(502), true},
		{"503", status(503), true},
		{"wrapped 503", fmt.Errorf("synthetic tail query failed: %w", status(503)), true},
		{"400", status(400), false},
		{"401", status(401), false},
		{"404", status(404), false},
		{"422", status(422), false},
		{"malformed row", errTailRowNotJSON(), false},
		{"line above the cap", bufio.ErrTooLong, false},
		{"client cancel", context.Canceled, false},
	} {
		if got := tailRetryable(tc.err); got != tc.retry {
			t.Errorf("%s: retryable = %v, want %v", tc.name, got, tc.retry)
		}
	}
}

func resumeEntry(ts int64, msg string) tailEntry {
	return tailEntry{ts: ts, stream: map[string]string{"app": "api"}, value: []string{strconv.FormatInt(ts, 10), msg}}
}

// The resume state asks VictoriaLogs for the span since the cursor plus the
// margin, advances the cursor only after a healthy stream, and drops after a
// reopen the entries at or before the last delivered one.
// conformance: semantics/tail-survives-backend-outage
func TestTailRetry_ResumeCursorAndDedupe(t *testing.T) {
	now := time.Unix(5000, 0)
	var r tailResume
	r.start(true, now)
	// Never delivered anything: the first read covers the lookback.
	if got, want := r.reopen(now.Add(2*time.Second)), 2*time.Second+tailResumeLookback+tailResumeMargin+time.Millisecond; got != want {
		t.Fatalf("offset before any row = %v, want %v", got, want)
	}
	// Delivered entries at one timestamp, then a newer one.
	last := now.Add(time.Second).UnixNano()
	r.observe([]tailEntry{resumeEntry(last-5, "a"), resumeEntry(last, "b"), resumeEntry(last, "c")})
	r.observe([]tailEntry{resumeEntry(last+7, "d")})
	if r.last != last+7 || len(r.keys) != 1 {
		t.Fatalf("last %d keys %d, want the newest timestamp and its one entry", r.last, len(r.keys))
	}
	// A stream that failed before it was healthy leaves the cursor; a healthy
	// one moves it to the newer of the last row and the lookback.
	if r.cursor != now.Add(-tailResumeLookback).UnixNano() {
		t.Fatalf("cursor moved before a healthy stream: %d", r.cursor)
	}
	r.advance(now.Add(time.Second))
	if r.cursor != last+7 {
		t.Fatalf("cursor = %d, want the last delivered %d", r.cursor, last+7)
	}
	r.advance(now.Add(time.Hour))
	if want := now.Add(time.Hour - tailResumeLookback).UnixNano(); r.cursor != want {
		t.Fatalf("cursor = %d, want a lookback before the failure %d", r.cursor, want)
	}
	// Dedupe: below the last and the last itself are dropped, new ones stay,
	// and only for a moment after the reopen.
	reopenAt := now.Add(2 * time.Hour)
	r.reopen(reopenAt)
	in := []tailEntry{resumeEntry(last, "old"), resumeEntry(last+7, "d"), resumeEntry(last+7, "e"), resumeEntry(last+9, "f")}
	// The first rows come ten seconds after the call: the dedupe runs from them.
	firstRows := reopenAt.Add(10 * time.Second)
	got := r.filter(append([]tailEntry(nil), in...), firstRows)
	var msgs []string
	for _, e := range got {
		msgs = append(msgs, e.value.([]string)[1])
	}
	if strings.Join(msgs, ",") != "e,f" {
		t.Fatalf("kept %v, want e,f", msgs)
	}
	if got := r.filter(append([]tailEntry(nil), in...), firstRows.Add(tailResumeDedupeFor+time.Second)); len(got) != len(in) {
		t.Fatalf("filtered %d of %d entries after the dedupe window", len(in)-len(got), len(in))
	}
	// Retries off: nothing is tracked or dropped.
	var off tailResume
	off.observe(in)
	if off.last != 0 || len(off.filter(in, now)) != len(in) {
		t.Fatal("a disabled resume tracked or dropped entries")
	}
}

// A row the stream was cut in the middle of is not delivered as a row, and
// the cheap row check passes JSON objects only.
// conformance: semantics/tail-survives-backend-outage
func TestTailRetry_RowSplitAndShape(t *testing.T) {
	scan := func(in string) (rows []string, partial bool) {
		sc := bufio.NewScanner(strings.NewReader(in))
		sc.Split(splitTailRows(&partial))
		for sc.Scan() {
			rows = append(rows, sc.Text())
		}
		return rows, partial
	}
	for _, tc := range []struct {
		in      string
		rows    []string
		partial bool
	}{
		{"", nil, false},
		{"{\"a\":1}\n", []string{`{"a":1}`}, false},
		{"{\"a\":1}\n{\"b\":2}\n", []string{`{"a":1}`, `{"b":2}`}, false},
		{"{\"a\":1}\n{\"b\":", []string{`{"a":1}`}, true},
		{"{\"b\":", nil, true},
		{"\n\n{\"a\":1}\r\n", []string{"", "", "{\"a\":1}"}, false},
	} {
		rows, partial := scan(tc.in)
		if strings.Join(rows, "|") != strings.Join(tc.rows, "|") || partial != tc.partial {
			t.Errorf("%q: rows %q partial %v, want %q %v", tc.in, rows, partial, tc.rows, tc.partial)
		}
	}
	for row, want := range map[string]bool{`{}`: true, `{"a":1}`: true, ``: false, `{`: false, `}`: false, `[1]`: false, `{"a":`: false, `garbage`: false} {
		if got := tailRowShapeOK([]byte(row)); got != want {
			t.Errorf("tailRowShapeOK(%q) = %v, want %v", row, got, want)
		}
	}
}

// A native stream the backend cut mid-row ends with an error the tail
// retries, and a body that is not NDJSON of objects ends with the final one.
// conformance: semantics/tail-survives-backend-outage
func TestTailRetry_ForwardNativeTailEndings(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	row := `{"_time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","_stream":"{app=\"api\"}","app":"api","_msg":"x"}` + "\n"
	for _, tc := range []struct {
		name, body string
		retryable  bool
		rows       int
	}{
		{"clean end", row, true, 1},
		{"cut mid-row", row + row[:20], true, 1},
		{"malformed", row + "not json\n", false, 1},
		{"empty", "", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := p.forwardNativeTail(context.Background(), &countingTailConn{}, p.newTailPipeline(`{app="api"}`, false), strings.NewReader(tc.body), nil)
			if res.err == nil || res.clientGone || tailRetryable(res.err) != tc.retryable || res.rows != tc.rows {
				t.Fatalf("result %+v: want a retryable=%v error and %d rows", res, tc.retryable, tc.rows)
			}
		})
	}
	// The frames of a stream whose rows arrive twice are not repeated once a
	// resume has seen them, and the rows it only repeats do not count as
	// delivered (a stream that serves its resumed window and dies is not healthy).
	tp := p.newTailPipeline(`{app="api"}`, false)
	tp.resume.start(true, time.Now())
	conn := &countingTailConn{}
	if res := p.forwardNativeTail(context.Background(), conn, tp, strings.NewReader(row), nil); res.rows != 1 {
		t.Fatalf("first stream: %+v", res)
	}
	tp.resume.reopen(time.Now())
	before := conn.messages
	if res := p.forwardNativeTail(context.Background(), conn, tp, bytes.NewReader([]byte(row)), nil); res.rows != 0 || conn.messages != before {
		t.Fatalf("the reopened stream sent the delivered row again: %+v, %d frames after %d", res, conn.messages, before)
	}
}

// start_offset reaches VictoriaLogs only on a reopen, in milliseconds.
// conformance: semantics/tail-survives-backend-outage
func TestTailRetry_OpenNativeTailStreamFromStartOffset(t *testing.T) {
	calls := make(chan url.Values, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls <- r.URL.Query()
		<-r.Context().Done()
	}))
	defer srv.Close()
	p := newTestProxy(t, srv.URL)
	for _, tc := range []struct {
		off  time.Duration
		want string
	}{{0, ""}, {1500 * time.Millisecond, "1500ms"}} {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			if resp, _, _ := p.openNativeTailStreamFrom(ctx, `{app="api"}`, tc.off); resp != nil {
				_ = resp.Body.Close()
			}
		}()
		select {
		case q := <-calls:
			if q.Get("start_offset") != tc.want || q.Get("offset") != "0s" {
				t.Fatalf("start_offset %q offset %q, want %q and 0s", q.Get("start_offset"), q.Get("offset"), tc.want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("the proxy never called the tail endpoint")
		}
		cancel()
		<-done
	}
}

// FuzzTailResume: the resume offset is always a positive span a backend
// accepts, and the dedupe never keeps an entry at or before the last delivered
// one nor drops one after it, for any cursor, timestamps and clock.
func FuzzTailResume(f *testing.F) {
	f.Add(int64(1_700_000_000_000_000_000), int64(1_700_000_000_000_000_000), int64(5_000_000_000), int64(0))
	f.Add(int64(0), int64(0), int64(0), int64(1))
	f.Add(int64(-5), int64(1<<62), int64(1<<62), int64(-1<<62))
	f.Fuzz(func(t *testing.T, cursor, last, nowNs, delta int64) {
		now := time.Unix(0, nowNs)
		r := tailResume{track: true, cursor: cursor, last: last, keys: map[uint64]struct{}{}}
		if off := r.reopen(now); off < time.Millisecond {
			t.Fatalf("offset %v below 1ms", off)
		}
		entries := []tailEntry{resumeEntry(last-1, "a"), resumeEntry(last, "b"), resumeEntry(last+1, "c"), resumeEntry(last+delta, "d")}
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].ts < entries[j].ts })
		for _, e := range r.filter(entries, now) {
			if last != 0 && e.ts < last {
				t.Fatalf("kept entry %d below the last delivered %d", e.ts, last)
			}
		}
		r.advance(now)
		if r.cursor < cursor {
			t.Fatalf("cursor moved back: %d to %d", cursor, r.cursor)
		}
	})
}

// FuzzTailRows: any stream body splits into rows in bounded work, the cheap row
// check accepts only objects, and a cut last row is flagged, never returned.
func FuzzTailRows(f *testing.F) {
	f.Add([]byte("{\"a\":1}\n{\"b\":"))
	f.Add([]byte("\n\n\r\n{}\n"))
	f.Add([]byte{0xff, '\n', '{'})
	f.Fuzz(func(t *testing.T, in []byte) {
		var partial bool
		sc := bufio.NewScanner(bytes.NewReader(in))
		sc.Buffer(make([]byte, 0, 64), 1<<16)
		sc.Split(splitTailRows(&partial))
		rows := 0
		for sc.Scan() {
			rows++
			row := bytes.TrimSpace(sc.Bytes())
			if tailRowShapeOK(row) && (row[0] != '{' || row[len(row)-1] != '}') {
				t.Fatalf("row %q passed the shape check", row)
			}
		}
		if rows > bytes.Count(in, []byte{'\n'})+1 {
			t.Fatalf("%d rows from %d newlines", rows, bytes.Count(in, []byte{'\n'}))
		}
		if partial && len(in) > 0 && in[len(in)-1] == '\n' && sc.Err() == nil {
			t.Fatal("a body ending in a newline has no cut row")
		}
	})
}

// BenchmarkTail_NativeForwardRetryEnabled is BenchmarkTail_NativeForward with
// the retry state noting every delivered batch (the default -tail.backend-retry-window):
// the cost of being able to resume.
func BenchmarkTail_NativeForwardRetryEnabled(b *testing.B) {
	var rows bytes.Buffer
	ts := time.Now().Add(-time.Hour)
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&rows, `{"_time":"%s","_stream":"{app=\"api\",env=\"prod\"}","app":"api","env":"prod","level":"info","_msg":"request %d done in 12ms"}`+"\n",
			ts.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
	}
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error", TailBackendRetryWindow: 30 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tp := p.newTailPipeline(`{app="api"}`, false)
		tp.resume.start(true, time.Now())
		conn := &countingTailConn{}
		p.forwardNativeTail(context.Background(), conn, tp, bytes.NewReader(rows.Bytes()), nil)
		if conn.messages == 0 {
			b.Fatal("no frames")
		}
	}
}
