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
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// firstWindowLookupSuffix ends the conversion check's lookup queries.
const firstWindowLookupSuffix = ` | filter __lvp_bad:="1" | limit 3`

// firstWindowRun answers one query_range through the real handler over the
// emulator, with the stats and lookup endpoints delayed as asked: slowStats
// holds every stats call until the request is cancelled (or 10 s), lookupAfter
// holds the lookups until a stats call has answered. heavy is the proxy's
// -backend-heavy-query-min-range.
type firstWindowRun struct {
	slowStats, lookupAfterStats bool
	heavy                       time.Duration
}

func (fw firstWindowRun) do(t *testing.T, rows []emuRow, query string, start, end int64, step time.Duration) (code int, body, contentType string, statsCancelled bool, elapsed time.Duration) {
	t.Helper()
	emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
	inner := emu.handler()
	statsDone := make(chan struct{})
	var cancelled atomic.Bool
	var closeOnce atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch {
		case strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query"):
			if fw.slowStats {
				select {
				case <-r.Context().Done():
					cancelled.Store(true)
					return
				case <-time.After(10 * time.Second):
				}
			}
			inner.ServeHTTP(w, r)
			if closeOnce.CompareAndSwap(false, true) {
				close(statsDone)
			}
			return
		case r.URL.Path == "/select/logsql/query" && strings.HasSuffix(r.FormValue("query"), firstWindowLookupSuffix) && fw.lookupAfterStats:
			select {
			case <-statsDone:
			case <-time.After(10 * time.Second):
			}
		}
		inner.ServeHTTP(w, r)
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, BackendHeavyQueryMinRange: fw.heavy})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start/1e9, 10)}, "end": {strconv.FormatInt(end/1e9, 10)}, "step": {strconv.FormatInt(int64(step/time.Second), 10)}}
	began := time.Now()
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	return res.Code, res.Body.String(), res.Header().Get("Content-Type"), cancelled.Load(), time.Since(began)
}

// firstWindowRows: lines every 50 s over [from, to), each pod's value a number
// except rejected ones (a unit string) at the given offsets from from.
func firstWindowRows(rnd *rand.Rand, from, to int64, rejectedAt []int64) []emuRow {
	var rows []emuRow
	pods := []string{"a", "b", "c"}
	for ts := from; ts < to; ts += int64(50 * time.Second) {
		rows = append(rows, emuRow{ts: ts + rnd.Int63n(int64(time.Second)), stream: `{app="uw",pod="` + pods[rnd.Intn(len(pods))] + `"}`, value: float64(rnd.Intn(100))})
	}
	for i, off := range rejectedAt {
		rows = append(rows, emuRow{ts: from + off, stream: `{app="uw",pod="r` + strconv.Itoa(i) + `"}`, raw: "5m"})
	}
	return rows
}

// TestUnwrapFirstWindowAnswersBeforeTheHeavyRead: a range request that reads
// -backend-heavy-query-min-range or more and meets a value the conversion
// rejects in its first evaluation window answers Loki's 400 without waiting
// for its stats read, which is cancelled (Loki stops at the first rejected
// value, pkg/logql/evaluator.go:726-733). The answer is byte for byte the one
// the request gives when the stats read answers first, over random windows,
// steps and ranges (shorter, equal and longer than the step); a rejected value
// only outside the first window, or none, keeps the full read's answer, the
// same as with the first-window lookup off.
//
// conformance: semantics/unwrap-conversion-error
func TestUnwrapFirstWindowAnswersBeforeTheHeavyRead(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	base := int64(1_800_000_000) * 1e9
	queries := []string{
		`max by (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [%s]))`,
		`sum by (pod) (sum_over_time({app="uw"} | logfmt | unwrap amount [%s]))`,
		`sum(rate({app="uw"} | logfmt | unwrap amount [%s]))`,
	}
	for i := 0; i < 24; i++ {
		step := []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute}[rnd.Intn(3)]
		window := []time.Duration{time.Minute, 5 * time.Minute, 150 * time.Second, 10 * time.Minute}[rnd.Intn(4)]
		start := base + int64(rnd.Intn(30))*int64(time.Minute)
		end := start + int64(7*time.Hour)
		query := fmt.Sprintf(queries[i%len(queries)], uwWindowText(window))
		label := fmt.Sprintf("#%d %s step=%v", i, query, step)
		// Rejected values inside the first window (start-window, start], and later.
		first := []int64{int64(window) - int64(time.Second), int64(window) + int64(2*time.Hour)}
		later := []int64{int64(window) + int64(3*time.Hour)}
		fromRows := start - int64(window) - int64(time.Hour)

		rows := firstWindowRows(rnd, fromRows, end+int64(time.Hour), shiftAll(first, start-int64(window)-fromRows))
		code, body, ct, cancelled, elapsed := firstWindowRun{slowStats: true, heavy: 6 * time.Hour}.do(t, rows, query, start, end, step)
		if code != http.StatusBadRequest || !cancelled || elapsed > 5*time.Second {
			t.Fatalf("%s: early route %d (stats cancelled %v, %v) %s", label, code, cancelled, elapsed, body)
		}
		if !strings.Contains(body, `pod="r0"`) {
			t.Fatalf("%s: the error names a row outside the first window: %s", label, body)
		}
		code2, body2, ct2, _, _ := firstWindowRun{lookupAfterStats: true, heavy: 6 * time.Hour}.do(t, rows, query, start, end, step)
		if code2 != code || body2 != body || ct2 != ct {
			t.Fatalf("%s: the early answer differs from the one after the stats read:\n%d %q %s\n%d %q %s", label, code, ct, body, code2, ct2, body2)
		}

		// Only a later rejected value: the full read decides, as with the lookup off.
		rows = firstWindowRows(rnd, fromRows, end+int64(time.Hour), shiftAll(later, start-int64(window)-fromRows))
		code, body, ct, _, _ = firstWindowRun{heavy: 6 * time.Hour}.do(t, rows, query, start, end, step)
		code2, body2, ct2, _, _ = firstWindowRun{heavy: 1000 * time.Hour}.do(t, rows, query, start, end, step)
		if code != http.StatusBadRequest || code2 != code || body2 != body || ct2 != ct {
			t.Fatalf("%s: a later rejected value answers differently with the lookup on:\n%d %s\n%d %s", label, code, body, code2, body2)
		}

		// No rejected value: the metric's 200, the same with the lookup off.
		rows = firstWindowRows(rnd, fromRows, end+int64(time.Hour), nil)
		code, body, _, _, _ = firstWindowRun{heavy: 6 * time.Hour}.do(t, rows, query, start, end, step)
		code2, body2, _, _, _ = firstWindowRun{heavy: 1000 * time.Hour}.do(t, rows, query, start, end, step)
		if code != http.StatusOK || code2 != code || body2 != body {
			t.Fatalf("%s: a clean window answers differently with the lookup on:\n%d %.300s\n%d %.300s", label, code, body, code2, body2)
		}
	}
}

// TestUnwrapFirstWindowScope: a read shorter than -backend-heavy-query-min-range
// or a query whose post filter intercepts the error takes no lookup.
//
// conformance: semantics/unwrap-conversion-error
func TestUnwrapFirstWindowScope(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	rows := firstWindowRows(rand.New(rand.NewSource(3)), base-int64(2*time.Hour), base+int64(8*time.Hour), nil)
	for _, tc := range []struct {
		query  string
		span   time.Duration
		lookup bool
	}{
		{`max by (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`, 7 * time.Hour, true},
		{`max by (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`, 5 * time.Hour, false},
		{`max by (pod) (max_over_time({app="uw"} | logfmt | unwrap amount | __error__="" [5m]))`, 7 * time.Hour, false},
	} {
		emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
		backend := httptest.NewServer(emu.handler())
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, BackendHeavyQueryMinRange: 6 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		_ = p.ValidateBackendVersionCompatibility(context.Background())
		params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(base/1e9, 10)}, "end": {strconv.FormatInt((base+int64(tc.span))/1e9, 10)}, "step": {"60"}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		_ = p.Shutdown(context.Background())
		backend.Close()
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", tc.query, res.Code, res.Body)
		}
		looked := false
		for _, q := range emu.endpointCalls("/select/logsql/query") {
			looked = looked || strings.HasSuffix(q, firstWindowLookupSuffix)
		}
		if looked != tc.lookup {
			t.Fatalf("%s over %v: lookup %v, want %v", tc.query, tc.span, looked, tc.lookup)
		}
	}
}

func shiftAll(offsets []int64, by int64) []int64 {
	out := make([]int64, len(offsets))
	for i, o := range offsets {
		out[i] = o + by
	}
	return out
}
