package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// indexStatsRow is one stored log line: its tenant, stream and timestamp.
type indexStatsRow struct {
	tenant, stream string
	ns             int64
}

// indexStatsFakeVL answers the proxy's `| stats count() entries,
// count_uniq_hash(_stream_id) streams` over the rows of the requesting tenant
// whose time lies in VictoriaLogs' inclusive [start, end] (a missing bound is
// open), the way VictoriaLogs evaluates that stats row.
type indexStatsFakeVL struct {
	mu      sync.Mutex
	rows    []indexStatsRow
	windows []string // "start end" of every call
}

func (f *indexStatsFakeVL) set(rows []indexStatsRow) {
	f.mu.Lock()
	f.rows = rows
	f.mu.Unlock()
}

func (f *indexStatsFakeVL) server(t testing.TB) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/query" || !strings.HasSuffix(r.Form.Get("query"), "| stats count() entries, count_uniq_hash(_stream_id) streams") {
			http.Error(w, "unexpected request "+r.URL.Path+" "+r.Form.Get("query"), http.StatusTeapot)
			return
		}
		lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
		if s := r.Form.Get("start"); s != "" {
			lo, _ = strconv.ParseInt(s, 10, 64)
		}
		if e := r.Form.Get("end"); e != "" {
			hi, _ = strconv.ParseInt(e, 10, 64)
		}
		tenant := r.Header.Get("AccountID")
		if tenant == "" {
			tenant = "0" // the default tenant
		}
		f.mu.Lock()
		f.windows = append(f.windows, r.Form.Get("start")+" "+r.Form.Get("end"))
		entries, streams := 0, map[string]bool{}
		for _, row := range f.rows {
			if row.tenant == tenant && row.ns >= lo && row.ns <= hi {
				entries++
				streams[row.stream] = true
			}
		}
		f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"entries":"%d","streams":"%d"}`+"\n", entries, len(streams))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newIndexStatsTestProxy(t *testing.T, backend string) *Proxy {
	t.Helper()
	p := newTestProxyWithOptions(t, func(c *Config) { c.BackendURL = backend; c.TenantMapFile = testTenantMapFile })
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// lokiIndexStatsReference is Loki's index/stats for rows held in chunks that lie
// wholly inside the window and are counted once (the case where Loki's chunk
// estimate is exact): the window is Loki's range window
// (pkg/loghttp/params.go:91-120 determineBounds, :163-186 parseTimestamp,
// default since 1h at :23), streams are the streams with an entry in it and
// entries its lines; tenants are summed (pkg/querier/multi_tenant_querier.go:208-230).
// Written from Loki's rules, independently of the proxy's helpers.
func lokiIndexStatsReference(rows []indexStatsRow, tenants []string, startRaw, endRaw, sinceRaw string, now time.Time) (streams, entries int, ok bool) {
	parse := func(value string, def time.Time) (time.Time, bool) {
		if value == "" {
			return def, true
		}
		if strings.Contains(value, ".") {
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				s, frac := math.Modf(f)
				frac = math.Round(frac*1000) / 1000
				return time.Unix(int64(s), int64(frac*float64(time.Second))), true
			}
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			ts, err := time.Parse(time.RFC3339Nano, value)
			return ts, err == nil
		}
		if len(value) <= 10 {
			return time.Unix(n, 0), true
		}
		return time.Unix(0, n), true
	}
	since := time.Hour
	if sinceRaw != "" {
		d, err := time.ParseDuration(sinceRaw)
		if err != nil {
			return 0, 0, false
		}
		since = d
	}
	end, ok1 := parse(endRaw, now)
	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}
	start, ok2 := parse(startRaw, endOrNow.Add(-since))
	if !ok1 || !ok2 {
		return 0, 0, false
	}
	for _, tenant := range tenants {
		seen := map[string]bool{}
		for _, row := range rows {
			if row.tenant == tenant && row.ns >= start.UnixNano() && row.ns <= end.UnixNano() {
				entries++
				seen[row.stream] = true
			}
		}
		streams += len(seen)
	}
	return streams, entries, true
}

type indexStatsAnswer struct {
	Streams int `json:"streams"`
	Chunks  int `json:"chunks"`
	Bytes   int `json:"bytes"`
	Entries int `json:"entries"`
}

func getIndexStats(t *testing.T, p *Proxy, params url.Values, orgID string) indexStatsAnswer {
	t.Helper()
	headers := map[string]string{}
	if orgID != "" {
		headers["X-Scope-OrgID"] = orgID
	}
	w := doCompatProxyRequest(p, "/loki/api/v1/index/stats?"+params.Encode(), headers)
	var got indexStatsAnswer
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil {
		t.Fatalf("index/stats %s: status=%d body=%s", params.Encode(), w.Code, w.Body)
	}
	return got
}

// TestIndexStats_WindowLikeLoki: index/stats counts the streams and lines of
// Loki's range window. A request without start (or end) read all of
// VictoriaLogs' retention; Loki reads the last hour before end (or now), the
// `since` before it when given, and never the future part of an end after now
// (pkg/loghttp/params.go:91-120). Explicit windows in every encoding Loki and
// the proxy read alike (seconds, nanoseconds, float seconds, RFC3339) pass
// through, and tenants of a multi-tenant request are summed like Loki's
// MultiTenantQuerier.IndexStats.
//
// conformance: loki_api_v1_index_stats, quality/index-stats-default-window
func TestIndexStats_WindowLikeLoki(t *testing.T) {
	const t0 = int64(1_700_000_000) // seconds
	at := func(d time.Duration) int64 { return t0*int64(time.Second) + int64(d) + 137 }
	now := time.Now()
	rel := func(d time.Duration) int64 { return now.Add(d).UnixNano() + 137 }
	rows := []indexStatsRow{
		{"0", "a", at(10 * time.Second)}, {"0", "a", at(20 * time.Minute)}, {"0", "a", at(50 * time.Minute)},
		{"0", "b", at(30 * time.Minute)}, {"0", "b", at(59 * time.Minute)},
		{"0", "c", at(-2 * time.Hour)}, // before every explicit window but the 3h `since`
		{"0", "n", rel(-10 * time.Minute)}, {"0", "o", rel(-2 * time.Hour)}, {"0", "f", rel(10 * time.Minute)},
		{"7", "a", at(5 * time.Minute)}, {"7", "b", at(6 * time.Minute)},
		{"8", "a", at(7 * time.Minute)},
	}
	fake := &indexStatsFakeVL{}
	fake.set(rows)
	p := newIndexStatsTestProxy(t, fake.server(t).URL)

	sec := func(d time.Duration) string { return strconv.FormatInt(t0+int64(d/time.Second), 10) }
	ns := func(d time.Duration) string { return strconv.FormatInt(t0*int64(time.Second)+int64(d), 10) }
	rfc := func(d time.Duration) string { return time.Unix(t0, 0).Add(d).UTC().Format(time.RFC3339Nano) }
	relNs := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).UnixNano(), 10) }

	cases := []struct {
		name, start, end, since, org string
		want                         indexStatsAnswer
	}{
		{"nanoseconds", ns(0), ns(time.Hour), "", "", indexStatsAnswer{2, 2, 500, 5}},
		{"seconds", sec(0), sec(time.Hour), "", "", indexStatsAnswer{2, 2, 500, 5}},
		{"rfc3339", rfc(0), rfc(time.Hour), "", "", indexStatsAnswer{2, 2, 500, 5}},
		{"float seconds", sec(0) + ".5", sec(time.Hour) + ".5", "", "", indexStatsAnswer{2, 2, 500, 5}},
		{"sub-window", ns(15 * time.Minute), ns(25 * time.Minute), "", "", indexStatsAnswer{1, 1, 100, 1}},
		{"window boundary on a line", ns(20*time.Minute + 137), ns(30*time.Minute + 137), "", "", indexStatsAnswer{2, 2, 200, 2}},
		{"empty window", ns(2 * time.Hour), ns(3 * time.Hour), "", "", indexStatsAnswer{0, 0, 0, 0}},
		{"no start: the hour before end", "", ns(time.Hour), "", "", indexStatsAnswer{2, 2, 500, 5}},
		{"no start, since 30m", "", ns(time.Hour), "30m", "", indexStatsAnswer{2, 2, 300, 3}},
		{"no start, since 3h", "", ns(time.Hour), "3h", "", indexStatsAnswer{3, 3, 600, 6}},
		{"no start, no end: the last hour", "", "", "", "", indexStatsAnswer{1, 1, 100, 1}},
		{"no start, end after now: the hour before now", "", relNs(time.Hour), "", "", indexStatsAnswer{2, 2, 200, 2}},
		{"no end: up to now", relNs(-30 * time.Minute), "", "", "", indexStatsAnswer{1, 1, 100, 1}},
		{"multi-tenant: summed per tenant", ns(0), ns(time.Hour), "", "7|8", indexStatsAnswer{3, 3, 300, 3}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := url.Values{"query": {fmt.Sprintf(`{app="x", case="%d"}`, i)}}
			for k, v := range map[string]string{"start": tc.start, "end": tc.end, "since": tc.since} {
				if v != "" {
					params.Set(k, v)
				}
			}
			if got := getIndexStats(t, p, params, tc.org); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}

	// A defaulted window is never open: every call carried both bounds.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, w := range fake.windows {
		if parts := strings.Fields(w); len(parts) != 2 {
			t.Fatalf("VictoriaLogs was asked an open window %q", w)
		}
	}
}

// TestIndexStats_DifferentialVsLokiReference: over random rows, tenants and
// windows (every encoding, missing bounds, `since`), the proxy's streams and
// entries equal lokiIndexStatsReference. A window whose default bound moved
// across a row while the request ran (now changes) is skipped, not compared.
//
// conformance: loki_api_v1_index_stats, quality/index-stats-default-window
func TestIndexStats_DifferentialVsLokiReference(t *testing.T) {
	fake := &indexStatsFakeVL{}
	p := newIndexStatsTestProxy(t, fake.server(t).URL)
	rng := rand.New(rand.NewSource(20261028))
	const base = int64(1_700_000_000) * int64(time.Second)

	encode := func(ts time.Time) string {
		switch rng.Intn(5) {
		case 0:
			return strconv.FormatInt(ts.Unix(), 10)
		case 1:
			return strconv.FormatInt(ts.UnixNano(), 10)
		case 2:
			return strconv.FormatInt(ts.Unix(), 10) + ".25"
		case 3:
			return ts.UTC().Format(time.RFC3339Nano)
		default:
			return ""
		}
	}

	compared := 0
	for it := 0; it < 300; it++ {
		now := time.Now()
		anchor := []int64{base, now.UnixNano()}[rng.Intn(2)]
		var rows []indexStatsRow
		for i := rng.Intn(60); i > 0; i-- {
			rows = append(rows, indexStatsRow{
				tenant: []string{"0", "7", "8"}[rng.Intn(3)],
				stream: fmt.Sprintf("s%d", rng.Intn(8)),
				// whole seconds plus an odd offset, so no row sits on a window bound
				ns: anchor + int64(rng.Intn(4*3600)-3*3600)*int64(time.Second) + 137,
			})
		}
		fake.set(rows)
		start := time.Unix(0, anchor).Add(time.Duration(rng.Intn(3*3600)-3*3600) * time.Second)
		end := start.Add(time.Duration(rng.Intn(2*3600)+1) * time.Second)
		params := url.Values{"query": {fmt.Sprintf(`{app="x", it="%d"}`, it)}}
		startRaw, endRaw, sinceRaw := encode(start), encode(end), []string{"", "", "30m", "2h"}[rng.Intn(4)]
		for k, v := range map[string]string{"start": startRaw, "end": endRaw, "since": sinceRaw} {
			if v != "" {
				params.Set(k, v)
			}
		}
		org, tenants := []string{"", "7|8", "0|7|8"}[rng.Intn(3)], []string(nil)
		if org == "" {
			tenants = []string{"0"}
		} else {
			tenants = strings.Split(org, "|")
		}

		before := time.Now()
		got := getIndexStats(t, p, params, org)
		after := time.Now()
		s1, e1, ok1 := lokiIndexStatsReference(rows, tenants, startRaw, endRaw, sinceRaw, before)
		s2, e2, ok2 := lokiIndexStatsReference(rows, tenants, startRaw, endRaw, sinceRaw, after)
		if !ok1 || !ok2 || s1 != s2 || e1 != e2 {
			continue
		}
		compared++
		if got.Streams != s1 || got.Entries != e1 || got.Chunks != s1 || got.Bytes != 100*e1 {
			t.Fatalf("iteration %d (%s, org %q): proxy %+v, Loki reference streams=%d entries=%d", it, params.Encode(), org, got, s1, e1)
		}
	}
	if compared < 250 {
		t.Fatalf("only %d of 300 random windows were compared", compared)
	}
}

// FuzzIndexStatsWindowLikeLoki: for any window around now in any encoding Loki
// reads (seconds, nanoseconds, float seconds, RFC3339, or missing), with or
// without `since`, index/stats counts what Loki's range window holds
// (lokiIndexStatsReference). A missing start or end must never widen the count
// to all of VictoriaLogs' retention.
//
// conformance: quality/index-stats-default-window
func FuzzIndexStatsWindowLikeLoki(f *testing.F) {
	f.Add(int32(-1800), uint32(900), uint8(0), uint8(1), uint8(0), int64(1))
	f.Add(int32(-7200), uint32(3600), uint8(4), uint8(4), uint8(2), int64(2))
	f.Add(int32(-600), uint32(7200), uint8(3), uint8(4), uint8(3), int64(3))
	f.Add(int32(60), uint32(60), uint8(2), uint8(2), uint8(1), int64(4))
	fake := &indexStatsFakeVL{}
	p, err := New(Config{BackendURL: fake.server(f).URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
	if err != nil {
		f.Fatalf("failed to create proxy: %v", err)
	}
	f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	it := 0
	f.Fuzz(func(t *testing.T, startOffset int32, length uint32, startEnc, endEnc, sinceIdx uint8, seed int64) {
		// a window from 4h before now to 1h after it, at least a second long
		startOffset %= 4 * 3600
		if startOffset > 3600 {
			startOffset -= 5 * 3600
		}
		length = length%(5*3600) + 1
		rng := rand.New(rand.NewSource(seed))
		now := time.Now()
		var rows []indexStatsRow
		for i := rng.Intn(40); i > 0; i-- {
			rows = append(rows, indexStatsRow{
				tenant: "0",
				stream: fmt.Sprintf("s%d", rng.Intn(6)),
				ns:     now.UnixNano() + int64(rng.Intn(6*3600)-5*3600)*int64(time.Second) + 137,
			})
		}
		fake.set(rows)
		start := now.Add(time.Duration(startOffset) * time.Second)
		end := start.Add(time.Duration(length) * time.Second)
		encode := func(ts time.Time, how uint8) string {
			switch how % 5 {
			case 0:
				return strconv.FormatInt(ts.Unix(), 10)
			case 1:
				return strconv.FormatInt(ts.UnixNano(), 10)
			case 2:
				return strconv.FormatInt(ts.Unix(), 10) + ".25"
			case 3:
				return ts.UTC().Format(time.RFC3339Nano)
			default:
				return ""
			}
		}
		it++
		startRaw, endRaw := encode(start, startEnc), encode(end, endEnc)
		sinceRaw := []string{"", "30m", "2h", "4h"}[sinceIdx%4]
		params := url.Values{"query": {fmt.Sprintf(`{app="x", fuzz="%d"}`, it)}}
		for k, v := range map[string]string{"start": startRaw, "end": endRaw, "since": sinceRaw} {
			if v != "" {
				params.Set(k, v)
			}
		}
		before := time.Now()
		w := doCompatProxyRequest(p, "/loki/api/v1/index/stats?"+params.Encode(), nil)
		after := time.Now()
		s1, e1, ok1 := lokiIndexStatsReference(rows, []string{"0"}, startRaw, endRaw, sinceRaw, before)
		s2, e2, ok2 := lokiIndexStatsReference(rows, []string{"0"}, startRaw, endRaw, sinceRaw, after)
		if !ok1 || !ok2 || s1 != s2 || e1 != e2 {
			return // a row on a moving bound: the answer depends on the instant
		}
		var got indexStatsAnswer
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil {
			t.Fatalf("index/stats %s: status=%d body=%s", params.Encode(), w.Code, w.Body)
		}
		if got.Streams != s1 || got.Entries != e1 {
			t.Fatalf("index/stats %s: proxy %+v, Loki reference streams=%d entries=%d", params.Encode(), got, s1, e1)
		}
	})
}
