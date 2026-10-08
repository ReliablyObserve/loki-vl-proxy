package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
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

// conformance: semantics/label-inventory-exact-and-incremental
func TestPlanInventorySegments_CoversTheWindowWithAlignedBuckets(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	base := time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC).UnixNano()
	for i := 0; i < 2000; i++ {
		end := base - rng.Int64N(int64(48*time.Hour))
		window := time.Duration(rng.Int64N(int64(9 * 24 * time.Hour)))
		start := end - int64(window)
		seal := end - rng.Int64N(int64(2*time.Hour))
		plan := planInventorySegments(start, end, seal)
		cur := start
		for _, seg := range plan {
			if seg.start != cur || seg.end <= seg.start {
				t.Fatalf("window [%d,%d) seal %d: gap or empty segment %+v in %+v", start, end, seal, seg, plan)
			}
			if seg.level >= 0 {
				size := int64(metadataInventoryLevels[seg.level])
				if seg.start%size != 0 || seg.end-seg.start != size {
					t.Fatalf("bucket %+v is not aligned to %s", seg, metadataInventoryLevels[seg.level])
				}
				if seg.end > seal {
					t.Fatalf("bucket %+v ends after the seal %d", seg, seal)
				}
			}
			cur = seg.end
		}
		if cur != end && end > start {
			t.Fatalf("plan of [%d,%d) ends at %d: %+v", start, end, cur, plan)
		}
	}
}

// A 7-day window needs about 80 segments, and moving it by seconds changes
// only its two edges: every bucket in between is the same bucket.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestPlanInventorySegments_ShiftedWindowReusesItsBuckets(t *testing.T) {
	now := time.Date(2026, 9, 24, 7, 17, 23, 0, time.UTC)
	for _, window := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour} {
		first := planInventorySegments(now.Add(-window).UnixNano(), now.UnixNano(), now.Add(-time.Minute).UnixNano())
		later := now.Add(17 * time.Second)
		second := planInventorySegments(later.Add(-window).UnixNano(), later.UnixNano(), later.Add(-time.Minute).UnixNano())
		buckets := map[inventorySegment]bool{}
		for _, seg := range first {
			if seg.level >= 0 {
				buckets[seg] = true
			}
		}
		newBuckets, edges := 0, 0
		for _, seg := range second {
			if seg.level < 0 {
				edges++
				continue
			}
			if !buckets[seg] {
				newBuckets++
			}
		}
		if len(first) > 90 || newBuckets > 1 || edges > 2 {
			t.Fatalf("%s: %d segments, shifted window needs %d new buckets and %d edges", window, len(first), newBuckets, edges)
		}
	}
}

// conformance: semantics/label-inventory-exact-and-incremental
func TestInventoryQueryShape(t *testing.T) {
	for _, tc := range []struct {
		query                 string
		bucketable, countable bool
	}{
		{`*`, true, true},
		{`_stream:{app="a|b"}`, true, true},
		{`{app="x"}`, true, true},
		{`app:="x"`, true, true},
		{`"error" app:~"a|b"`, true, false},
		{`error`, true, false},
		{`_msg:error`, true, false},
		{`app:="x" and (env:="y" or -level:="debug")`, true, true},
		{`{app="x", env=~"a|b"} !pod:""`, true, true},
		{`* | format "<a>" as service_name keep_original_fields`, true, false},
		{`* | coalesce(service.name, app) default "unknown_service" as service_name`, true, false},
		{`"err|warn" | unpack_json | filter level:=error`, true, false},
		{`* | stats count()`, false, false},
		{`* | uniq by (app)`, false, false},
		{`* | limit 10`, false, false},
		{`_time:5m`, false, false},
		{`options(ignore_global_time_filter=true) *`, false, false},
		{`user:in(* | fields user)`, false, false},
		{``, false, false},
		{`"unterminated`, false, false},
	} {
		b, c := inventoryQueryShape(tc.query)
		if b != tc.bucketable || c != tc.countable {
			t.Fatalf("inventoryQueryShape(%q) = %v, %v; want %v, %v", tc.query, b, c, tc.bucketable, tc.countable)
		}
	}
}

// conformance: semantics/label-inventory-exact-and-incremental
func TestLessNatural_MatchesVictoriaLogsOrder(t *testing.T) {
	sorted := []string{"", "0", "00", "1", "01", "2", "10", "a", "a1", "a2", "a10", "a10b", "a010c", "ab", "b", "pod-2", "pod-10", "z"}
	for i := range sorted {
		for j := range sorted {
			if got, want := lessNatural(sorted[i], sorted[j]), i < j; got != want {
				t.Fatalf("lessNatural(%q, %q) = %v, want %v", sorted[i], sorted[j], got, want)
			}
		}
	}
	huge := "x" + strings.Repeat("9", 25)
	if !lessNatural(huge+"0", huge+"1") || lessNatural(huge+"1", huge+"0") {
		t.Fatal("digit runs past uint64 must fall back to byte order")
	}
}

// fakeVLRow is one row of the fake VictoriaLogs below.
type fakeVLRow struct {
	ts     int64
	stream map[string]string
	fields map[string]string
}

// fakeVictoriaLogs answers the four listings, the row count and /metrics over
// a fixed set of rows, with VictoriaLogs' exclusive end and ordering. It
// understands only the queries the tests send: * and {k="v"} stream filters.
type fakeVictoriaLogs struct {
	mu    sync.Mutex
	rows  []fakeVLRow
	calls map[string]int
	spans []time.Duration
	long  map[string]int // calls spanning an hour or more, by path
}

func (f *fakeVictoriaLogs) add(r fakeVLRow) {
	f.mu.Lock()
	f.rows = append(f.rows, r)
	f.mu.Unlock()
}

func (f *fakeVictoriaLogs) matching(q url.Values) []fakeVLRow {
	start, _ := strconv.ParseInt(q.Get("start"), 10, 64)
	end, _ := strconv.ParseInt(q.Get("end"), 10, 64)
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(q.Get("query")), "| count() as rows"))
	var sel map[string]string
	if strings.HasPrefix(query, "{") {
		sel = map[string]string{}
		for _, kv := range strings.Split(strings.Trim(query, "{}"), ",") {
			k, v, _ := strings.Cut(kv, "=")
			sel[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"`)
		}
	}
	var out []fakeVLRow
	for _, r := range f.rows {
		if r.ts < start || r.ts >= end {
			continue
		}
		ok := true
		for k, v := range sel {
			if r.stream[k] != v {
				ok = false
			}
		}
		if ok {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeVictoriaLogs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[r.URL.Path]++
	if s, e := r.Form.Get("start"), r.Form.Get("end"); s != "" && e != "" {
		a, _ := strconv.ParseInt(s, 10, 64)
		b, _ := strconv.ParseInt(e, 10, 64)
		if r.URL.Path != "/select/logsql/query" { // row counts are not listing reads
			f.spans = append(f.spans, time.Duration(b-a))
		}
		if time.Duration(b-a) >= time.Hour {
			if f.long == nil {
				f.long = map[string]int{}
			}
			f.long[r.URL.Path]++
		}
	}
	rows := f.matching(r.Form)
	hits := map[string]int64{}
	switch r.URL.Path {
	case "/select/logsql/query":
		_, _ = fmt.Fprintf(w, `{"rows":"%d"}`+"\n", len(rows))
		return
	case "/select/logsql/stream_field_names":
		for _, row := range rows {
			for k := range row.stream {
				hits[k]++
			}
		}
	case "/select/logsql/field_names":
		for _, row := range rows {
			for k := range row.stream {
				hits[k]++
			}
			for k := range row.fields {
				hits[k]++
			}
		}
	case "/select/logsql/stream_field_values":
		for _, row := range rows {
			if v, ok := row.stream[r.Form.Get("field")]; ok {
				hits[v]++
			}
		}
	case "/select/logsql/field_values":
		for _, row := range rows {
			if v, ok := row.stream[r.Form.Get("field")]; ok {
				hits[v]++
			} else if v, ok := row.fields[r.Form.Get("field")]; ok {
				hits[v]++
			}
		}
	default:
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	items := make([]vlValueHits, 0, len(hits))
	for v, h := range hits {
		items = append(items, vlValueHits{Value: v, Hits: h})
	}
	sortVLValueHits(items)
	_ = json.NewEncoder(w).Encode(map[string]any{"values": items})
}

func (f *fakeVictoriaLogs) reset() (calls map[string]int, spans []time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls, spans = f.calls, f.spans
	f.calls, f.spans, f.long = map[string]int{}, nil, map[string]int{}
	return calls, spans
}

func newInventoryTestProxy(t *testing.T, backend http.Handler, parallelism int) *Proxy {
	t.Helper()
	srv := httptest.NewServer(backend)
	t.Cleanup(srv.Close)
	p, err := New(Config{
		BackendURL:                   srv.URL,
		Cache:                        cache.New(time.Hour, 100000),
		LogLevel:                     "error",
		MetadataInventoryParallelism: parallelism,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// seedRows writes a week of rows whose streams rotate: pods live for a few
// hours, a label appears on one day only, some rows sit exactly on bucket
// boundaries.
func seedRows(f *fakeVictoriaLogs, now time.Time, rng *rand.Rand) {
	start := now.Add(-8 * 24 * time.Hour).Truncate(time.Hour)
	for ts := start; ts.Before(now); ts = ts.Add(time.Duration(1+rng.IntN(7)) * time.Minute) {
		pod := "pod-" + strconv.Itoa(int(ts.Sub(start)/(3*time.Hour)))
		stream := map[string]string{"app": []string{"api", "web", "db"}[rng.IntN(3)], "pod": pod}
		if ts.Day()%3 == 0 {
			stream["canary"] = "true"
		}
		f.add(fakeVLRow{ts: ts.UnixNano(), stream: stream, fields: map[string]string{"level": []string{"info", "warn"}[rng.IntN(2)]}})
	}
	f.add(fakeVLRow{ts: now.Add(-50 * time.Hour).Truncate(24 * time.Hour).UnixNano(), stream: map[string]string{"boundary": "day"}})
	f.add(fakeVLRow{ts: now.Add(-3 * time.Hour).Truncate(time.Hour).UnixNano(), stream: map[string]string{"boundary": "hour"}})
	f.add(fakeVLRow{ts: now.Add(-40 * time.Second).UnixNano(), stream: map[string]string{"fresh": "yes"}})
}

// The inventory answer is exactly the answer of one full-range call, values,
// hits and order, for every listing, query and window; and a window moved by
// seconds costs VictoriaLogs only its edges.
//
// conformance: semantics/label-inventory-exact-and-incremental, semantics/label-browser-latency-cold-and-refresh, loki_api_v1_labels, loki_api_v1_label_name_values
func TestMetadataInventory_EqualsFullRangeScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedRows(vl, now, rng)
	bucketed := newInventoryTestProxy(t, vl, 4)
	full := newInventoryTestProxy(t, vl, -1)
	ctx := context.Background()

	listings := []struct {
		path  string
		extra url.Values
	}{
		{"/select/logsql/stream_field_names", nil},
		{"/select/logsql/field_names", nil},
		{"/select/logsql/stream_field_values", url.Values{"field": {"pod"}}},
		{"/select/logsql/field_values", url.Values{"field": {"level"}}},
	}
	for i := 0; i < 60; i++ {
		end := now.Add(-time.Duration(rng.Int64N(int64(26 * time.Hour))))
		if i%3 == 0 {
			end = now
		}
		window := []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 37 * time.Minute, 50 * time.Hour}[i%6]
		query := []string{"*", `{app="api"}`}[i%2]
		for _, l := range listings {
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(end.Add(-window).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
			for k, v := range l.extra {
				params[k] = v
			}
			got, err := bucketed.fetchVLListing(ctx, l.path, params)
			if err != nil {
				t.Fatal(err)
			}
			want, err := full.fetchVLListing(ctx, l.path, params)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 && len(want) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s %s over %s ending %s:\n bucketed %v\n full     %v", l.path, query, window, end.Format(time.RFC3339), got, want)
			}
		}
	}

	// A Grafana refresh: the same 7-day window 20 s later reads only its two
	// edges from VictoriaLogs, each shorter than the finest bucket.
	window := 7 * 24 * time.Hour
	params := func(end time.Time) url.Values {
		return url.Values{"query": {"*"}, "start": {strconv.FormatInt(end.Add(-window).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	}
	// Anchored two minutes into a 5-minute block that ended before now, so
	// neither window edge crosses a 5-minute, hour or day boundary over the
	// 20 s shift (a crossing reads the minutes and 5-minute blocks that
	// newly fit, a legitimate extra cost this test does not measure), and
	// every minute of both windows is sealed.
	first := time.Now().Truncate(5 * time.Minute).Add(-3 * time.Minute)
	if _, err := bucketed.fetchVLListing(ctx, "/select/logsql/stream_field_names", params(first)); err != nil {
		t.Fatal(err)
	}
	vl.reset()
	shifted := first.Add(20 * time.Second)
	got, err := bucketed.fetchVLListing(ctx, "/select/logsql/stream_field_names", params(shifted))
	if err != nil {
		t.Fatal(err)
	}
	calls, spans := vl.reset()
	scanned := time.Duration(0)
	for _, s := range spans {
		scanned += s
	}
	if calls["/select/logsql/stream_field_names"] > 3 || scanned > 5*time.Minute {
		t.Fatalf("shifted 7-day window scanned %s in %d calls (%v); want only its edges", scanned, calls["/select/logsql/stream_field_names"], spans)
	}
	// The row counts over the runs of empty buckets are separate calls: one
	// per run, never one per bucket.
	if calls["/select/logsql/query"] > 4 {
		t.Fatalf("shifted 7-day window made %d row counts, want one per run of empty buckets", calls["/select/logsql/query"])
	}
	want, _ := full.fetchVLListing(ctx, "/select/logsql/stream_field_names", params(shifted))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shifted window: bucketed %v, full %v", got, want)
	}
}

// A row written late into an old hour changes that hour's row count, so its
// revalidation rescans it and the new label appears; an unchanged hour is
// confirmed by its count alone.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestMetadataInventory_RevalidatesByRowCount(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedRows(vl, now, rng)
	p := newInventoryTestProxy(t, vl, 2)
	p.cacheTTLLabels = time.Millisecond // every entry is due for revalidation at once
	ctx := context.Background()
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(now.Add(-24*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}}
	if _, err := p.fetchVLListing(ctx, "/select/logsql/stream_field_names", params); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	vl.reset()
	if _, err := p.fetchVLListing(ctx, "/select/logsql/stream_field_names", params); err != nil {
		t.Fatal(err)
	}
	vl.mu.Lock()
	longScans, counts := vl.long["/select/logsql/stream_field_names"], vl.long["/select/logsql/query"]
	vl.mu.Unlock()
	if longScans != 0 || counts == 0 {
		t.Fatalf("unchanged hour and day buckets: %d rescans and %d counts, want counts only", longScans, counts)
	}

	late := now.Add(-5 * time.Hour).Truncate(time.Hour).Add(17 * time.Minute)
	vl.add(fakeVLRow{ts: late.UnixNano(), stream: map[string]string{"backfilled": "yes"}})
	time.Sleep(5 * time.Millisecond)
	got, err := p.fetchVLListing(ctx, "/select/logsql/stream_field_names", params)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range got {
		found = found || item.Value == "backfilled"
	}
	if !found {
		t.Fatalf("late row in a revalidated hour is missing: %v", got)
	}
}

// Queries that are not row-local, and truncated listings, are one call over
// the whole range.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestMetadataInventory_NonMergeableListingsAreOneCall(t *testing.T) {
	vl := &fakeVictoriaLogs{}
	p := newInventoryTestProxy(t, vl, 4)
	now := time.Now()
	for _, params := range []url.Values{
		{"query": {"* | stats count()"}},
		{"query": {"*"}, "limit": {"10"}},
		{"query": {"_time:1h"}},
	} {
		params.Set("start", strconv.FormatInt(now.Add(-7*24*time.Hour).UnixNano(), 10))
		params.Set("end", strconv.FormatInt(now.UnixNano(), 10))
		vl.reset()
		if _, err := p.fetchVLListing(context.Background(), "/select/logsql/field_values", params); err != nil {
			t.Fatal(err)
		}
		if calls, _ := vl.reset(); calls["/select/logsql/field_values"] != 1 {
			t.Fatalf("%v: %d calls, want one full-range call", params, calls["/select/logsql/field_values"])
		}
	}
}

// Concurrent requests for the same bucket share one scan, and a failed
// bucket fails the request instead of answering with part of the window.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestMetadataInventory_SharedFillAndErrors(t *testing.T) {
	var scans atomic.Int32
	var fail atomic.Bool
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			scans.Add(1)
			time.Sleep(20 * time.Millisecond)
			if fail.Load() {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":3}]}`))
		case "/select/logsql/query":
			_, _ = w.Write([]byte(`{"rows":"3"}` + "\n"))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
	p := newInventoryTestProxy(t, backend, 4)
	// The hour before the current one: sealed whatever the minute is now.
	end := time.Now().Truncate(time.Hour).Add(-time.Hour)
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(end.Add(-6*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := scans.Load(); n != 6 {
		t.Fatalf("8 concurrent requests for six hour buckets made %d scans, want 6", n)
	}
	fail.Store(true)
	params.Set("start", strconv.FormatInt(end.Add(-30*time.Hour).UnixNano(), 10))
	if _, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params); err == nil {
		t.Fatal("a failed bucket must fail the listing")
	}
}

// End to end through /loki/api/v1/labels and /label/{name}/values: the
// inventory changes the cost, not the answer.
//
// conformance: semantics/label-inventory-exact-and-incremental, semantics/label-browser-latency-cold-and-refresh, loki_api_v1_labels, loki_api_v1_label_name_values
func TestMetadataInventory_LabelEndpointsUnchanged(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 9))
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedRows(vl, now, rng)
	bucketed := newInventoryTestProxy(t, vl, 4)
	full := newInventoryTestProxy(t, vl, -1)
	serve := func(p *Proxy, target string) string {
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", target, w.Code, w.Body.String())
		}
		return w.Body.String()
	}
	for _, window := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour} {
		q := url.Values{"start": {strconv.FormatInt(now.Add(-window).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}}
		for _, target := range []string{"/loki/api/v1/labels?" + q.Encode(), "/loki/api/v1/label/pod/values?" + q.Encode(), "/loki/api/v1/label/app/values?" + q.Encode()} {
			got, want := serve(bucketed, target), serve(full, target)
			if got != want {
				t.Fatalf("%s over %s:\n bucketed %s\n full     %s", target, window, got, want)
			}
		}
	}
}

// The warm-up fills the buckets user requests read: it runs as the /labels
// request of a client without a tenant header, so the same listing asked by
// such a client finds every sealed bucket the warm-up scanned (before, the
// warm-up's buckets carried no auth scope and no user request ever read
// them). The listing is asked directly, below the response cache, which
// would otherwise answer it.
//
// conformance: semantics/label-inventory-exact-and-incremental, limits/concurrent-full-retention-scans-exhaust-backend
func TestMetadataInventory_WarmUpFillsTheBucketsUsersRead(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 4))
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedRows(vl, now, rng)
	p := newInventoryTestProxy(t, vl, 4)
	p.warmLabelWindows(context.Background(), 0, time.Minute, false, 2*time.Hour) // the 1h preset
	calls, _ := vl.reset()
	if calls["/select/logsql/stream_field_names"] == 0 {
		t.Fatal("the warm-up listed nothing")
	}

	bs, be := bucketMetadataTime(now.Add(-time.Hour).UnixNano(), now.UnixNano())
	user := context.WithValue(context.Background(), origRequestKey, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(bs, 10)}, "end": {strconv.FormatInt(be, 10)}}
	if _, err := p.fetchVLListing(user, "/select/logsql/stream_field_names", params); err != nil {
		t.Fatal(err)
	}
	// Only the unsealed tail (the last minute and the rounded-up end) is
	// read again.
	calls, spans := vl.reset()
	if calls["/select/logsql/stream_field_names"] > 1 {
		t.Fatalf("a user's listing of the warmed window made %d scans (%v): the warm-up filled buckets under another key", calls["/select/logsql/stream_field_names"], spans)
	}
}

// spansTile reports whether the [start, end) spans, in any order, cover
// [start, end) exactly once: the inventory may split a listing's range into
// buckets, but never caps, overlaps or leaves a gap in it.
func spansTile(spans [][2]int64, start, end int64) bool {
	sorted := append([][2]int64(nil), spans...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
	cur := start
	for _, s := range sorted {
		if s[0] != cur || s[1] <= s[0] {
			return false
		}
		cur = s[1]
	}
	return cur == end
}

func parseSpan(startRaw, endRaw string) [2]int64 {
	s, _ := parseLokiTimeToUnixNano(startRaw)
	e, _ := parseLokiTimeToUnixNano(endRaw)
	return [2]int64{s, e}
}

// spansCover reports whether the union of the spans covers [start, end).
func spansCover(spans [][2]int64, start, end int64) bool {
	sorted := append([][2]int64(nil), spans...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
	cur := start
	for _, s := range sorted {
		if s[0] > cur {
			break
		}
		if s[1] > cur {
			cur = s[1]
		}
	}
	return cur >= end
}

// The day bucket scans of long-range listings go through the metadata-scan
// limiter one scan at a time, and hold a slot only while their own call
// runs (hour buckets are short scans, never admitted). With a ceiling of
// one, three cold 7-day listings, two of them sharing most of their buckets,
// never run two day scans at once, never
// wait on each other while holding the slot, and are all answered; the
// shared buckets are scanned once.
//
// conformance: semantics/label-inventory-exact-and-incremental, limits/metadata-scan-adaptive-limit, backend-admission-and-heavy-query-queueing
func TestMetadataInventory_LongScansAreAdmittedOneScanAtATime(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 8))
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedRows(vl, now, rng)
	var mu sync.Mutex
	running, maxRunning := 0, 0
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		span := parseSpan(r.Form.Get("start"), r.Form.Get("end"))
		if r.URL.Path == "/select/logsql/stream_field_names" && time.Duration(span[1]-span[0]) >= 24*time.Hour {
			mu.Lock()
			running++
			maxRunning = max(maxRunning, running)
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			defer func() { mu.Lock(); running--; mu.Unlock() }()
		}
		vl.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(slow)
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Hour, 100000), LogLevel: "error",
		BackendMaxConcurrentMetadataScans: 1, BackendHeavyQueryQueueWait: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	type listing struct {
		query string
		end   time.Time
	}
	var wg sync.WaitGroup
	for _, l := range []listing{{`{app="api"}`, now}, {`{app="api"}`, now.Add(-time.Minute)}, {`{app="web"}`, now}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			params := url.Values{"query": {l.query}, "start": {strconv.FormatInt(l.end.Add(-7*24*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(l.end.UnixNano(), 10)}}
			if _, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params); err != nil {
				t.Error(l.query, err)
			}
		}()
	}
	wg.Wait()
	if maxRunning != 1 {
		t.Fatalf("%d day bucket scans ran at once under a ceiling of one", maxRunning)
	}
	if snap := p.metadataScanLimiter.snapshot(); snap.InFlight != 0 || snap.Rejected != 0 {
		t.Fatalf("slots left in flight or requests refused: %+v", snap)
	}
}

// A request that shares a bucket fill with another request is not failed by
// the other request going away: Grafana cancels the previous refresh when it
// sends the next one, and both need the same buckets.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestMetadataInventory_FollowerSurvivesLeaderCancel(t *testing.T) {
	rng := rand.New(rand.NewPCG(2, 4))
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedRows(vl, now, rng)
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stream_field_names" {
			select {
			case <-time.After(30 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		vl.ServeHTTP(w, r)
	})
	p := newInventoryTestProxy(t, slow, 4)
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(now.Add(-24*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}}
	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := p.fetchVLListing(leaderCtx, "/select/logsql/stream_field_names", params)
		leaderDone <- err
	}()
	time.Sleep(10 * time.Millisecond)
	followerDone := make(chan error, 1)
	var got []vlValueHits
	go func() {
		var err error
		got, err = p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params)
		followerDone <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v, want canceled", err)
	}
	if err := <-followerDone; err != nil {
		t.Fatalf("follower failed with its leader: %v", err)
	}
	full := newInventoryTestProxy(t, vl, -1)
	want, _ := full.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("follower answer %v, full %v", got, want)
	}
}

// A bucket larger than the read cache keeps turns the listing into one
// full-range call instead of scanning every bucket on every request.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestMetadataInventory_UncacheableBucketFallsBackToOneCall(t *testing.T) {
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	for i := 0; i < 3000; i++ {
		vl.add(fakeVLRow{ts: now.Add(-3 * time.Hour).UnixNano(), stream: map[string]string{"pod": fmt.Sprintf("pod-%04d-%s", i, strings.Repeat("x", 40))}})
	}
	srv := httptest.NewServer(vl)
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewWithMaxBytes(time.Hour, 1000, 200_000), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	params := url.Values{"query": {"*"}, "field": {"pod"}, "start": {strconv.FormatInt(now.Add(-6*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}}
	got, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_values", params)
	if err != nil || len(got) != 3000 {
		t.Fatalf("got %d values, err %v", len(got), err)
	}
	vl.reset()
	if _, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_values", params); err != nil {
		t.Fatal(err)
	}
	if calls, _ := vl.reset(); calls["/select/logsql/stream_field_values"] != 1 {
		t.Fatalf("an uncacheable listing made %d calls", calls["/select/logsql/stream_field_values"])
	}
}

// The request that led a bucket fill and was refused a slot fails after one
// queue wait; a request that only followed it retries once as leader. Neither
// waits the queue wait three times.
//
// conformance: semantics/label-inventory-exact-and-incremental, limits/metadata-scan-queue-429
func TestMetadataInventory_RefusedFillIsNotRetriedByItsLeader(t *testing.T) {
	vl := &fakeVictoriaLogs{}
	srv := httptest.NewServer(vl)
	t.Cleanup(srv.Close)
	const wait = 150 * time.Millisecond
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Hour, 1000), LogLevel: "error",
		BackendMaxConcurrentMetadataScans: 1, BackendHeavyQueryQueueWait: wait})
	if err != nil {
		t.Fatal(err)
	}
	p.metadataScanLimiter.jitter = nil // the timing bound below has no room for it
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	// A sealed day: a window ending within the inventory's seal lag of now (the
	// first minute after midnight UTC) is split into live listings of another
	// cost class, which the limiter may admit next to the held scan.
	end := time.Now().Add(-2 * metadataInventorySealLag).Truncate(24 * time.Hour)
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(end.Add(-24*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	holder, err := p.metadataScanLimiter.acquire(context.Background(), "/select/logsql/stream_field_names", params, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer p.metadataScanLimiter.release(holder)
	started := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params); !isHeavyQueryQueueFull(err) {
				t.Errorf("want the documented 429, got %v", err)
			}
		}()
	}
	wg.Wait()
	if took := time.Since(started); took > 3*wait+wait/2 {
		t.Fatalf("two requests refused in %s: the leader retried its own refusal", took)
	}
}

// Hits are decoded as the listing decoder always did (signed), so a backend
// that reports a negative count is not turned into a 502, bucketed or not.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestMetadataInventory_SignedHitsDecode(t *testing.T) {
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":3},{"value":"odd","hits":-1}]}`))
	})
	end := time.Now().Truncate(time.Hour)
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(end.Add(-6*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	for _, parallelism := range []int{4, -1} {
		p := newInventoryTestProxy(t, backend, parallelism)
		got, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params)
		if err != nil || len(got) != 2 {
			t.Fatalf("parallelism %d: %v %v", parallelism, got, err)
		}
	}
}

// A bucket that fails does not cancel the bucket scans already running: they
// finish and are cached, so the retry that follows continues where the
// failed request stopped.
//
// conformance: semantics/label-inventory-exact-and-incremental, limits/metadata-scan-queue-429
func TestMetadataInventory_FailedBucketKeepsRunningScans(t *testing.T) {
	end := time.Now().Truncate(24 * time.Hour)
	failDay := end.Add(-24 * time.Hour).UnixNano()
	var completed, cancelled atomic.Int32
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		start, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		switch {
		case r.URL.Path != "/select/logsql/stream_field_names":
			_, _ = w.Write([]byte(`{"rows":"1"}`))
		case start == failDay:
			w.WriteHeader(http.StatusBadGateway)
		default:
			select {
			case <-time.After(150 * time.Millisecond):
				completed.Add(1)
				_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
			case <-r.Context().Done():
				cancelled.Add(1)
			}
		}
	})
	p := newInventoryTestProxy(t, backend, 4)
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(end.Add(-4*24*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	if _, err := p.fetchVLListing(context.Background(), "/select/logsql/stream_field_names", params); err == nil {
		t.Fatal("a failed bucket must fail the listing")
	}
	if cancelled.Load() != 0 || completed.Load() == 0 {
		t.Fatalf("%d running bucket scans were cancelled by a sibling's failure (%d completed)", cancelled.Load(), completed.Load())
	}
}
