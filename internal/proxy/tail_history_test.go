package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Loki's /loki/api/v1/tail takes start (default one hour ago) and limit
// (default 100) and sends the entries since start, up to limit, before the
// live ones (HTTP API reference). These tests pin that history on every tail
// mode against a fake VictoriaLogs.

// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_LimitDefaultsLikeLoki(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		max  int
		want int
	}{
		{"no limit is Loki's 100", "", 0, 100},
		{"no limit under a lower max", "", 10, 10},
		{"a limit", "5", 0, 5},
		{"a limit under the max", "50", 5000, 50},
		{"a limit above the max is held to it", "20000", 5000, 5000},
		{"unlimited max keeps the limit", "20000", 0, 20000},
		// The route answered these with Loki's 400 before the handler; a
		// direct call reads them as no limit.
		{"zero", "0", 0, 100},
		{"negative", "-1", 0, 100},
		{"not a number", "abc", 0, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tailHistoryLimit(tc.raw, tc.max); got != tc.want {
				t.Fatalf("tailHistoryLimit(%q, %d) = %d, want %d", tc.raw, tc.max, got, tc.want)
			}
		})
	}
}

// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_StartDefaultsLikeLoki(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	past := now.Add(-20 * time.Minute)
	for _, tc := range []struct {
		name string
		raw  string
		want time.Time
	}{
		{"no start is an hour ago", "", now.Add(-time.Hour)},
		{"unix seconds", strconv.FormatInt(past.Unix(), 10), past},
		{"unix nanoseconds", strconv.FormatInt(past.UnixNano(), 10), past},
		{"RFC3339", past.Format(time.RFC3339Nano), past},
		{"older than any retention", "1", time.Unix(1, 0)},
		{"in the future is now", strconv.FormatInt(now.Add(time.Hour).UnixNano(), 10), now},
		{"unreadable is an hour ago", "abc", now.Add(-time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tailHistoryStart(tc.raw, now); !got.Equal(tc.want) {
				t.Fatalf("tailHistoryStart(%q) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

// historyProxy is retryProxy with a hook to set limits on the proxy.
func historyProxy(t *testing.T, vl *retryVL, mode TailMode, window time.Duration, configure func(*Proxy)) string {
	t.Helper()
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: vl.srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, TailMode: mode, TailBackendRetryWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	if configure != nil {
		configure(p)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(mux)
	t.Cleanup(front.Close)
	return "ws" + strings.TrimPrefix(front.URL, "http") + `/loki/api/v1/tail?query=` + url.QueryEscape(`{app="api"}`)
}

// orderedTailClient reads every line a tail sends, in the order it sends them.
type orderedTailClient struct {
	lines  chan string
	frames atomic.Int64
	labels sync.Map // line -> its stream labels, printed, without detected_level
	level  sync.Map // line -> its stream's detected_level label, when it has one
}

func dialOrderedTail(t *testing.T, wsURL string) *orderedTailClient {
	t.Helper()
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 8 * time.Second}).Dial(wsURL, http.Header{"X-Scope-OrgID": {"0"}})
	if err != nil {
		t.Fatalf("tail dial: %v (resp %v)", err, resp)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &orderedTailClient{lines: make(chan string, 4096)}
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame struct {
				Streams []struct {
					Stream map[string]string `json:"stream"`
					Values [][]string        `json:"values"`
				} `json:"streams"`
			}
			if json.Unmarshal(msg, &frame) != nil {
				continue
			}
			c.frames.Add(1)
			for _, s := range frame.Streams {
				for _, v := range s.Values {
					if len(v) > 1 {
						if lv, ok := s.Stream[detectedLevelLabel]; ok {
							c.level.Store(v[1], lv)
						}
						stream := map[string]string{}
						for k, val := range s.Stream {
							if k != detectedLevelLabel { // the history carries it, as Loki's query path does; a live entry does not
								stream[k] = val
							}
						}
						c.labels.Store(v[1], fmt.Sprint(stream))
						c.lines <- v[1]
					}
				}
			}
		}
	}()
	return c
}

// until reads lines until last arrives, then for quiet more, and returns them.
func (c *orderedTailClient) until(t *testing.T, last string, quiet time.Duration) []string {
	t.Helper()
	var got []string
	deadline := time.After(10 * time.Second)
	for {
		select {
		case line := <-c.lines:
			got = append(got, line)
			if line != last {
				continue
			}
			settle := time.After(quiet)
			for {
				select {
				case more := <-c.lines:
					got = append(got, more)
				case <-settle:
					return got
				}
			}
		case <-deadline:
			t.Fatalf("line %q never arrived; got %d lines: %v", last, len(got), got)
		}
	}
}

func fixed(params string) func(time.Time) string { return func(time.Time) string { return params } }

func historyLines(from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("history %03d", i))
	}
	return out
}

// A tail sends the newest limit entries since start, oldest first, before any
// live entry, then the live entries once each: the history read is the one
// VictoriaLogs call before the upgrade, and the native stream's first read,
// which covers the seconds before it opens, repeats nothing the history sent.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_NewestEntriesSinceStartBeforeLiveLikeLoki(t *testing.T) {
	type tailCase struct {
		name     string
		params   func(now time.Time) string // added to the tail URL
		maxLimit int                        // max_entries_limit_per_query; 0 keeps the default
		want     []string                   // the history, before the live line
	}
	// 130 rows ten seconds apart ending 8.5 minutes ago, and one a second
	// ago that the native stream's first read also returns.
	const rows = 130
	recent := "recent line"
	cases := []tailCase{
		{"no limit is the newest 100 of the last hour", fixed(""), 0, append(historyLines(rows-99, rows-1), recent)},
		{"limit 5", fixed("&limit=5"), 0, append(historyLines(rows-4, rows-1), recent)},
		{"limit above max_entries_limit_per_query is held to it", fixed("&limit=50"), 10, append(historyLines(rows-9, rows-1), recent)},
		{"start older than the retention reads what is stored", fixed("&start=1&limit=1000"), 0, append(historyLines(0, rows-1), recent)},
		{"start in unix seconds", func(now time.Time) string {
			return "&start=" + strconv.FormatInt(now.Add(-20*time.Minute).Unix(), 10)
		}, 0, append(historyLines(60, rows-1), recent)}, // row 60 is 20 minutes old
		{"start in RFC3339", func(now time.Time) string {
			return "&start=" + url.QueryEscape(now.Add(-20*time.Minute).Format(time.RFC3339Nano))
		}, 0, append(historyLines(60, rows-1), recent)},
		{"start in the future sends no history", func(now time.Time) string {
			return "&start=" + strconv.FormatInt(now.Add(time.Hour).UnixNano(), 10)
		}, 0, nil},
	}
	for _, mode := range []TailMode{TailModeSynthetic, TailModeNative, TailModeAuto} {
		for _, window := range []time.Duration{0, time.Minute} {
			if mode == TailModeSynthetic && window == 0 {
				continue // the polling tail has no stream to reopen: the window changes nothing there
			}
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%s/window=%s/%s", mode, window, tc.name), func(t *testing.T) {
					vl := newRetryVL(t)
					vl.history.Store(true)
					now := time.Now()
					base := now.Add(-30 * time.Minute)
					for i := 0; i < rows; i++ {
						vl.add(fmt.Sprintf("history %03d", i), base.Add(time.Duration(i)*10*time.Second))
					}
					vl.add(recent, now.Add(-time.Second))
					wsURL := historyProxy(t, vl, mode, window, func(p *Proxy) {
						if tc.maxLimit > 0 {
							p.tenantDefaultLimits = map[string]any{limitMaxEntriesLimitPerQuery: tc.maxLimit}
						}
					})
					client := dialOrderedTail(t, wsURL+tc.params(now))
					want := append([]string(nil), tc.want...)
					if len(want) == 0 && mode != TailModeSynthetic {
						// No history: the native stream's first read still
						// covers the seconds before it opens. The polling
						// tail reads from start, which is now.
						want = []string{recent}
					}
					live := "live line"
					vl.add(live, time.Now())
					want = append(want, live)
					got := client.until(t, live, 300*time.Millisecond)
					if strings.Join(got, "|") != strings.Join(want, "|") {
						t.Fatalf("lines\n got %d %v\nwant %d %v", len(got), got, len(want), want)
					}
					// History entries carry the labels a live entry of the
					// same stream carries, and detected_level as Loki's
					// history does (TestTailHistory_HistoryCarriesDetectedLevel).
					liveLabels, _ := client.labels.Load(live)
					for _, line := range got {
						if l, _ := client.labels.Load(line); l != liveLabels {
							t.Fatalf("line %q labels %v, live line's %v", line, l, liveLabels)
						}
					}
					if n := vl.historyCalls.Load(); n != 1 {
						t.Fatalf("%d history reads, want 1 per connection", n)
					}
				})
			}
		}
	}
}

// The history read keeps preflight's failure answers: an error status fails
// the request before the upgrade with Loki's status, and a slow backend
// skips the history instead of holding up the handshake.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_BackendStatusFailsBeforeUpgrade(t *testing.T) {
	vl := newRetryVL(t)
	vl.history.Store(true)
	vl.setMode("down-503")
	wsURL := historyProxy(t, vl, TailModeSynthetic, 0, nil)
	_, resp, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial(wsURL, http.Header{"X-Scope-OrgID": {"0"}})
	if err == nil {
		t.Fatal("dial succeeded with a failing backend")
	}
	if resp == nil || resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %v, want Loki's 500 before the upgrade", resp)
	}
}

// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_LimitValidationLikeLoki(t *testing.T) {
	vl := newRetryVL(t)
	wsURL := historyProxy(t, vl, TailModeSynthetic, 0, nil)
	httpURL := "http" + strings.TrimPrefix(wsURL, "ws")
	for _, tc := range []struct {
		limit, body string
	}{
		// Recorded Loki answer (error-layer row r1142) for -1; 0 is not
		// positive either.
		{"-1", "limit must be a positive value"},
		{"0", "limit must be a positive value"},
		{"abc", `strconv.Atoi: parsing "abc": invalid syntax`},
	} {
		resp, err := http.Get(httpURL + "&limit=" + tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 512)
		n, _ := resp.Body.Read(body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body[:n]), tc.body) {
			t.Fatalf("limit=%s: %d %q, want 400 %q", tc.limit, resp.StatusCode, body[:n], tc.body)
		}
	}
	if n := vl.historyCalls.Load() + vl.pollCalls.Load() + vl.tailCalls.Load(); n != 0 {
		t.Fatalf("%d VictoriaLogs calls for refused requests, want 0", n)
	}
}

// A label filter VictoriaLogs cannot evaluate like Loki drops rows on the
// proxy: the limit counts the entries it keeps, as in a query_range response,
// so further pages fill the history.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_LabelFilterFillsTheLimit(t *testing.T) {
	now := time.Now()
	var calls atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" || !strings.HasSuffix(r.FormValue("query"), "sort by (_time desc)") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		calls.Add(1)
		end, _ := parseEntryTime(r.FormValue("end"))
		limit, _ := strconv.Atoi(r.FormValue("limit"))
		w.Header().Set("Content-Type", "application/x-ndjson")
		n := 0
		// Rows newest first, one second apart: odd ones carry the stored
		// field user=u1 that no stage of the query exposes, so Loki's
		// label filter matches no label there and the proxy drops them.
		for i := 0; i < 40 && n < limit; i++ {
			ts := now.Add(-time.Duration(i+1) * time.Second)
			if ts.After(end) {
				continue
			}
			row := fmt.Sprintf(`{"_time":%q,"_stream":"{app=\"api\"}","app":"api","_msg":"row %02d"}`, ts.UTC().Format(time.RFC3339Nano), i)
			if i%2 == 1 {
				row = fmt.Sprintf(`{"_time":%q,"_stream":"{app=\"api\"}","app":"api","user":"u1","_msg":"row %02d"}`, ts.UTC().Format(time.RFC3339Nano), i)
			}
			_, _ = fmt.Fprintln(w, row)
			n++
		}
	}))
	t.Cleanup(vl.Close)
	p := newLokiProfileTailProxy(t, vl.URL)
	tp := p.newTailPipeline(`{app="api"} | user="u1"`, false)
	if !tp.exposure.filtersLineFields() {
		t.Skip("the query no longer filters line fields on the proxy")
	}
	h, err := p.fetchTailHistory(context.Background(), `app:="api"`, "", "4", tp)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := p.tailEntries(context.Background(), tp, h.rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("%d history entries, want the limit of 4 kept ones: %+v", len(entries), entries)
	}
}

// The polling tail reads from just past the newest entry the history sent,
// with the history's rows noted as seen; without history, from the lookback
// before the read, never before start.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_SyntheticWindowStart(t *testing.T) {
	end := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	row := func(ts time.Time, msg string) string {
		return fmt.Sprintf(`{"_time":%q,"_stream":"{app=\"api\"}","_msg":%q}`, ts.Format(time.RFC3339Nano), msg)
	}
	for _, tc := range []struct {
		name string
		h    tailHistory
		want time.Time
		seen int
	}{
		{"history: just past its newest entry", tailHistory{
			rows:  []byte(row(end.Add(-time.Second), "b") + "\n" + row(end.Add(-time.Minute), "a") + "\n"),
			start: end.Add(-time.Hour), end: end,
		}, end.Add(-time.Second + time.Nanosecond), 2},
		{"a row that is not JSON is passed over", tailHistory{
			rows:  []byte("not json\n\n" + row(end.Add(-time.Minute), "a")),
			start: end.Add(-time.Hour), end: end,
		}, end.Add(-time.Minute + time.Nanosecond), 1},
		{"no history: the lookback before the read", tailHistory{start: end.Add(-time.Hour), end: end}, end.Add(-tailResumeLookback), 0},
		{"no history, start inside the lookback: start", tailHistory{start: end.Add(-time.Second), end: end}, end.Add(-time.Second), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := newSyntheticTailSeen(10)
			if got := syntheticTailWindowStart(tc.h, seen); !got.Equal(tc.want) {
				t.Fatalf("window start %s, want %s", got, tc.want)
			}
			if len(seen.order) != tc.seen {
				t.Fatalf("%d rows noted as seen, want %d", len(seen.order), tc.seen)
			}
		})
	}
	// A history that was never read (a direct call) reads the lookback before now.
	if got := syntheticTailWindowStart(tailHistory{}, newSyntheticTailSeen(1)); time.Since(got) < tailResumeLookback || time.Since(got) > tailResumeLookback+time.Minute {
		t.Fatalf("window start %s, want about %s ago", got, tailResumeLookback)
	}
}

// A history body above -backend-max-buffered-response-bytes is skipped, not
// sent in part; a client that is gone stops the tail before it streams.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_BoundedAndBestEffort(t *testing.T) {
	vl := newRetryVL(t)
	vl.history.Store(true)
	now := time.Now()
	for i := 0; i < 50; i++ {
		vl.add(fmt.Sprintf("history %03d", i), now.Add(-time.Duration(50-i)*time.Second))
	}
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: vl.srv.URL, Cache: c, LogLevel: "error", ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 512}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	tp := p.newTailPipeline(`{app="api"}`, false)
	h, err := p.fetchTailHistory(context.Background(), `app:="api"`, "", "", tp)
	if err != nil || h.rows != nil {
		t.Fatalf("oversized history: rows %d bytes, err %v; want skipped", len(h.rows), err)
	}
	if h.end.IsZero() || !h.start.Equal(h.end.Add(-time.Hour)) {
		t.Fatalf("window %s..%s, want the hour before the read", h.start, h.end)
	}

	// The client went away: the history write fails and nothing else is sent.
	conn := &fakeTailConn{writeMessageErr: fmt.Errorf("gone")}
	rows := []byte(vl.rowJSON(retryRow{ts: now, msg: "x"}))
	if err := p.sendTailHistory(context.Background(), conn, tp, tailHistory{rows: rows}); err == nil {
		t.Fatal("history write to a gone client succeeded")
	}
	if err := p.sendTailHistory(context.Background(), conn, tp, tailHistory{}); err != nil {
		t.Fatalf("empty history: %v", err)
	}
}

// FuzzTailHistoryParams: for any start and limit text the history window ends
// at now and starts at or before it, and the limit is positive and within a
// non-zero max_entries_limit_per_query.
func FuzzTailHistoryParams(f *testing.F) {
	for _, s := range []string{"", "1", "1794567075", "1794567075000000000", "2026-11-14T12:00:00Z", "abc", "-1", "99999999999999999999", "1e9"} {
		f.Add(s, s, 0)
		f.Add(s, "100", 5000)
	}
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, start, limit string, maxEntries int) {
		if got := tailHistoryStart(start, now); got.After(now) {
			t.Fatalf("start %q: window starts %s, after now", start, got)
		}
		n := tailHistoryLimit(limit, maxEntries)
		if n <= 0 || (maxEntries > 0 && n > maxEntries) {
			t.Fatalf("limit %q, max %d: %d", limit, maxEntries, n)
		}
	})
}

// Loki reads the history through its query path, whose entries carry
// detected_level in the stream labels; its live tailer sends the pushed stream
// as is. The history here does the same, on every tail mode, and the
// newest history entry is not sent again when the native stream's first read
// returns it without that label.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_HistoryCarriesDetectedLevelLikeLoki(t *testing.T) {
	for _, mode := range []TailMode{TailModeSynthetic, TailModeNative, TailModeAuto} {
		t.Run(string(mode), func(t *testing.T) {
			vl := newRetryVL(t)
			vl.history.Store(true)
			now := time.Now()
			// The newest history entry is also inside the native stream's
			// first read (the seconds before it opens).
			for i := 0; i < 3; i++ {
				vl.add(fmt.Sprintf("history %03d", i), now.Add(-time.Duration(3-i)*time.Second))
			}
			wsURL := historyProxy(t, vl, mode, time.Minute, nil)
			client := dialOrderedTail(t, wsURL)
			vl.add("live line", time.Now())
			got := client.until(t, "live line", 300*time.Millisecond)
			if want := "history 000|history 001|history 002|live line"; strings.Join(got, "|") != want {
				t.Fatalf("lines %v, want %s", got, want)
			}
			for _, line := range got[:3] {
				if lv, _ := client.level.Load(line); lv != "unknown" {
					t.Errorf("history line %q: detected_level %v, want the query path's unknown", line, lv)
				}
			}
			if lv, ok := client.level.Load("live line"); ok {
				t.Errorf("live line carries detected_level %v: Loki's live tailer sends the pushed stream as is", lv)
			}
		})
	}
}

// A start further back than max_query_length gets no history: Loki upgrades the
// tail and sends live entries only (recorded with start=1), and the proxy
// sends the one-row access call it always sends before the upgrade instead of a history read.
// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_StartPastMaxQueryLengthSendsNoHistory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start string
		want  string // the lines before the live one
	}{
		{"start past the limit", "1", ""},
		{"start inside the limit", "", "history 000|history 001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := newRetryVL(t)
			vl.history.Store(true)
			now := time.Now()
			vl.add("history 000", now.Add(-30*time.Second))
			vl.add("history 001", now.Add(-29*time.Second))
			wsURL := historyProxy(t, vl, TailModeSynthetic, 0, func(p *Proxy) {
				p.tenantDefaultLimits = map[string]any{limitMaxQueryLength: "721h"}
			})
			if tc.start != "" {
				wsURL += "&start=" + tc.start
			}
			client := dialOrderedTail(t, wsURL)
			vl.add("live line", time.Now())
			got := client.until(t, "live line", 300*time.Millisecond)
			want := "live line"
			if tc.want != "" {
				want = tc.want + "|live line"
			}
			if strings.Join(got, "|") != want {
				t.Fatalf("lines %v, want %s", got, want)
			}
			// The call before the upgrade stays either way.
			if n := vl.historyCalls.Load(); n != 1 {
				t.Fatalf("%d calls before the upgrade, want 1", n)
			}
		})
	}
	// A start past the limit still fails with Loki's 500 on a failing backend.
	vl := newRetryVL(t)
	vl.history.Store(true)
	vl.setMode("down-503")
	wsURL := historyProxy(t, vl, TailModeSynthetic, 0, func(p *Proxy) {
		p.tenantDefaultLimits = map[string]any{limitMaxQueryLength: "721h"}
	}) + "&start=1"
	_, resp, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial(wsURL, http.Header{"X-Scope-OrgID": {"0"}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("a failing backend: err %v, resp %v, want Loki's 500 before the upgrade", err, resp)
	}
}
