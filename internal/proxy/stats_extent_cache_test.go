package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// extentShim makes a fake VictoriaLogs answer stats_query_range the way
// VictoriaLogs does in the two respects the extent cache relies on, written
// apart from the cache: start and end widen to the bucket edges k*step-offset
// (alignStartEndToStep, app/vlselect/logsql/logsql.go, v1.49.0-v1.53.0), and
// series come in VictoriaLogs' order, by metric column (the order of the
// results of the last stats pipe), then by the non-empty labels as JSON
// (ProcessStatsQueryRangeRequest). It records the bucket range of every call.
type extentShim struct {
	t     testing.TB
	inner http.Handler
	mu    sync.Mutex
	calls []extentShimCall
	// fail answers the stats_query_range calls it matches with a 503.
	fail func(start, end int64) bool
}

type extentShimCall struct {
	query      string
	start, end int64 // the widened range, [start, end)
	step       int64
}

func newExtentShim(t testing.TB, inner http.Handler) (*httptest.Server, *extentShim) {
	shim := &extentShim{t: t, inner: inner}
	srv := httptest.NewServer(shim)
	t.Cleanup(srv.Close)
	return srv, shim
}

func (s *extentShim) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.URL.Path != "/select/logsql/stats_query_range" {
		s.inner.ServeHTTP(w, r)
		return
	}
	step, err := time.ParseDuration(r.Form.Get("step"))
	if err != nil || step <= 0 {
		s.t.Errorf("shim: step %q", r.Form.Get("step"))
		return
	}
	var offset time.Duration
	if raw := r.Form.Get("offset"); raw != "" {
		if offset, err = time.ParseDuration(raw); err != nil {
			s.t.Errorf("shim: offset %q", raw)
			return
		}
	}
	start := vlTruncate(parseFakeVLTime(s.t, r.Form.Get("start")), int64(step), int64(offset))
	end := vlTruncate(parseFakeVLTime(s.t, r.Form.Get("end"))-1, int64(step), int64(offset)) + int64(step)
	r.Form.Set("start", strconv.FormatInt(start, 10))
	r.Form.Set("end", strconv.FormatInt(end, 10))
	s.mu.Lock()
	s.calls = append(s.calls, extentShimCall{query: r.Form.Get("query"), start: start, end: end, step: int64(step)})
	fail := s.fail
	s.mu.Unlock()
	if fail != nil && fail(start, end) {
		http.Error(w, `{"error":"backend overloaded"}`, http.StatusServiceUnavailable)
		return
	}
	rec := httptest.NewRecorder()
	s.inner.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(vlOrderedStatsBody(s.t, r.Form.Get("query"), rec.Body.Bytes()))
}

// bucketsRead is the number of buckets the calls asked VictoriaLogs for.
func (s *extentShim) bucketsRead() (calls int, buckets int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		buckets += (c.end - c.start) / c.step
	}
	return len(s.calls), buckets
}

func (s *extentShim) reset() {
	s.mu.Lock()
	s.calls = nil
	s.mu.Unlock()
}

// vlOrderedStatsBody rewrites a fake's stats_query_range answer in
// VictoriaLogs' series order.
func vlOrderedStatsBody(t testing.TB, query string, body []byte) []byte {
	var resp struct {
		Data struct {
			Result []struct {
				Metric json.RawMessage   `json:"metric"`
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("shim: fake answer %s: %v", body, err)
	}
	columns := map[string]int{}
	for i, alias := range lastStatsResultNames(query) {
		columns[alias] = i
	}
	type series struct {
		column int
		name   string
		pairs  [][2]string
		key    string
		values []json.RawMessage
	}
	quote := func(s string) string {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(s)
		return strings.TrimSpace(b.String())
	}
	var all []series
	for _, res := range resp.Data.Result {
		dec := json.NewDecoder(bytes.NewReader(res.Metric))
		_, _ = dec.Token() // {
		var s series
		var labels []string
		for dec.More() {
			k, _ := dec.Token()
			v, _ := dec.Token()
			ks, vs := k.(string), v.(string)
			if ks == "__name__" {
				s.name = vs
				continue
			}
			s.pairs = append(s.pairs, [2]string{ks, vs})
			if vs != "" {
				labels = append(labels, quote(ks)+":"+quote(vs))
			}
		}
		column, ok := columns[s.name]
		if !ok {
			column = len(columns)
		}
		s.column, s.key, s.values = column, "{"+strings.Join(labels, ",")+"}", res.Values
		all = append(all, s)
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if a.column != b.column {
			return a.column < b.column
		}
		if a.name != b.name {
			return a.name < b.name
		}
		return a.key < b.key
	})
	var out bytes.Buffer
	out.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, s := range all {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteString(`{"metric":{"__name__":` + quote(s.name))
		for _, pair := range s.pairs {
			out.WriteString("," + quote(pair[0]) + ":" + quote(pair[1]))
		}
		out.WriteString(`},"values":[`)
		for j, v := range s.values {
			if j > 0 {
				out.WriteByte(',')
			}
			var compact bytes.Buffer
			_ = json.Compact(&compact, v)
			out.Write(compact.Bytes())
		}
		out.WriteString("]}")
	}
	out.WriteString("]}}")
	return out.Bytes()
}

// lastStatsResultNames returns the result names of the last stats pipe of a
// LogsQL query, in order: VictoriaLogs' metric columns.
func lastStatsResultNames(query string) []string {
	at := strings.LastIndex(query, "| stats ")
	if at < 0 {
		return nil
	}
	pipe := query[at+len("| stats "):]
	if strings.HasPrefix(pipe, "by (") {
		depth := 0
		for i, c := range pipe {
			if c == '(' {
				depth++
			} else if c == ')' {
				if depth--; depth == 0 {
					pipe = pipe[i+1:]
					break
				}
			}
		}
	}
	if end := strings.Index(pipe, " | "); end >= 0 {
		pipe = pipe[:end]
	}
	var names []string
	depth, from := 0, 0
	for i := 0; i <= len(pipe); i++ {
		if i < len(pipe) {
			switch pipe[i] {
			case '(':
				depth++
				continue
			case ')':
				depth--
				continue
			case ',':
				if depth > 0 {
					continue
				}
			default:
				continue
			}
		}
		part := strings.TrimSpace(pipe[from:i])
		if as := strings.LastIndex(part, " as "); as >= 0 {
			names = append(names, strings.TrimSpace(part[as+len(" as "):]))
		} else {
			names = append(names, part)
		}
		from = i + 1
	}
	return names
}

func extentQueryRange(t testing.TB, p *Proxy, query string, start, end time.Time, step time.Duration, header http.Header) (int, []byte) {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	// The buckets of a miss are stored off the request path: the next
	// request of a run meets them stored, as it would a moment later.
	p.statsExtentStores.Wait()
	return rec.Code, rec.Body.Bytes()
}

// extentWindow is one query_range request of a differential run.
type extentWindow struct {
	start, end time.Time
	step       time.Duration
}

// extentWindows returns the request windows of a run over [from, to]: a
// dashboard refresh (the window moves forward a minute at a time), a user
// re-running a query (it moves back), the same window again, and random
// ranges, steps and start offsets, some off the step grid.
func extentWindows(seed int64, from, to time.Time) []extentWindow {
	rng := rand.New(rand.NewSource(seed))
	var out []extentWindow
	end := from.Add(3 * time.Hour)
	for i := 0; i < 4; i++ { // refresh
		e := end.Add(time.Duration(i) * time.Minute)
		out = append(out, extentWindow{e.Add(-time.Hour), e, 10 * time.Second})
	}
	for i := 1; i < 4; i++ { // shifted back
		e := end.Add(-time.Duration(i) * time.Minute)
		out = append(out, extentWindow{e.Add(-time.Hour), e, 10 * time.Second})
	}
	out = append(out, out[0]) // the same window again
	steps := []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute, 7 * time.Second}
	ranges := []time.Duration{5 * time.Minute, 15 * time.Minute, time.Hour, 3 * time.Hour, 5 * time.Hour}
	span := to.Sub(from)
	for i := 0; i < 10; i++ {
		step := steps[rng.Intn(len(steps))]
		r := ranges[rng.Intn(len(ranges))]
		latest := span - r
		if latest <= 0 {
			continue
		}
		start := from.Add(time.Duration(rng.Int63n(int64(latest))) / time.Second * time.Second)
		if rng.Intn(2) == 0 {
			start = start.Truncate(step)
		}
		out = append(out, extentWindow{start, start.Add(r), step})
		// and a refresh of it
		out = append(out, extentWindow{start.Add(time.Minute), start.Add(r + time.Minute), step})
	}
	return out
}

// extentJSONFixture is a JSON stream over [s0, s0+span): the body's level
// collides with the stream label on most lines (Loki's level_extracted), some
// lines carry no stream level, the pipeline key varies, and a series appears
// only in the last hour.
func extentJSONFixture(s0 time.Time, span time.Duration) []pushdownRow {
	var rows []pushdownRow
	levels := []string{"info", "warn", "error", "debug"}
	for i := 0; time.Duration(i)*17*time.Second < span; i++ {
		ts := s0.Add(time.Duration(i) * 17 * time.Second)
		row := pushdownRow{ts: ts, stream: map[string]string{"app": "api"}}
		level := levels[(i*7)%len(levels)]
		pipeline := []string{"logs/loki", "traces/otlp"}[i%2]
		if ts.After(s0.Add(span - time.Hour)) {
			pipeline = "late/new"
		}
		switch i % 5 {
		case 0: // no stream label: the body's level stays level
			row.msg = `{"level":"` + level + `","pipeline":"` + pipeline + `"}`
		default:
			row.stream["level"] = levels[i%len(levels)]
			row.msg = `{"level":"` + level + `","pipeline":"` + pipeline + `","n":` + strconv.Itoa(i) + `}`
		}
		rows = append(rows, row)
	}
	return rows
}

// newExtentTestProxy builds a proxy over backendURL with its own cache; on
// makes the extent cache answer.
func newExtentTestProxy(t testing.TB, backendURL string, on bool, maxBytes int) *Proxy {
	t.Helper()
	c := cache.NewWithMaxBytes(time.Minute, 100000, maxBytes)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: backendURL, Cache: c, LogLevel: "error", QueryRangeExtentCache: on, QueryRangeExtentCacheTTL: time.Hour})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	return p
}

// extentOutcomes counts the extent cache's answers by outcome, from the
// proxy's internal operation metric.
func extentOutcomes(t testing.TB, p *Proxy) map[string]int {
	t.Helper()
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	out := map[string]int{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		const prefix = `loki_vl_proxy_internal_operation_total{operation="stats_extent_cache",outcome="`
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			outcome, value, _ := strings.Cut(rest, `"} `)
			n, _ := strconv.Atoi(value)
			out[outcome] += n
		}
	}
	return out
}

// runExtentDifferential sends every window of every query to a proxy with the
// extent cache and one without, each over its own copy of the backend, and
// requires the same status and the same bytes. It returns the buckets each
// proxy asked VictoriaLogs for and the number of answers with data.
func runExtentDifferential(t *testing.T, newBackend func() http.Handler, queries []string, windows []extentWindow, setup func(on, off *Proxy)) (onBuckets, offBuckets int64, withData int) {
	t.Helper()
	onSrv, onShim := newExtentShim(t, newBackend())
	offSrv, offShim := newExtentShim(t, newBackend())
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	if setup != nil {
		setup(on, off)
	}
	for _, query := range queries {
		for _, w := range windows {
			q := strings.ReplaceAll(query, "$__auto", w.step.String())
			wantCode, want := extentQueryRange(t, off, q, w.start, w.end, w.step, nil)
			gotCode, got := extentQueryRange(t, on, q, w.start, w.end, w.step, nil)
			if gotCode != wantCode || !bytes.Equal(got, want) {
				t.Fatalf("%s [%s, %s] step %s: the extent cache changed the answer\n with: %d %s\nwithout: %d %s", q, w.start.Format(time.RFC3339), w.end.Format(time.RFC3339), w.step, gotCode, got, wantCode, want)
			}
			if wantCode == http.StatusOK && bytes.Contains(want, []byte(`"values":[[`)) {
				withData++
			}
		}
	}
	_, onBuckets = onShim.bucketsRead()
	_, offBuckets = offShim.bucketsRead()
	outcomes := extentOutcomes(t, on)
	t.Logf("extent cache outcomes: %v", outcomes)
	if outcomes["hit"]+outcomes["partial"] == 0 || outcomes["unreadable"] != 0 || outcomes["too_large"] != 0 {
		t.Fatalf("the run must be answered from stored buckets and every answer read back exactly: %v", outcomes)
	}
	return onBuckets, offBuckets, withData
}

// The extent cache never changes an answer: the parsed-label metric shapes of
// Explore and Logs Drilldown (the stats pushdown with its risk probes, label
// filters, name_extracted labels, bytes, logfmt, detected_level), each over
// refreshes, re-runs, repeated and random windows with steps that do or do not
// divide a split, ranges longer than the step, and a series that exists only
// in the newest hour, give byte for byte the answer of a proxy without it.
// The cached proxy reads a fraction of the buckets.
// conformance: quality/query-range-extent-cache, loki_api_v1_query_range, semantics/json-extracted-metric-pushdown
func TestStatsExtentCacheDifferentialParsedMetrics(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	span := 6 * time.Hour
	rows := extentJSONFixture(s0, span)
	queries := []string{
		`sum by (level) (count_over_time({app="api"} | json | level_extracted!="" [1m]))`,
		`sum by (level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`,
		`sum(rate({app="api"} | json | level_extracted="error" [1m]))`,
		`sum by (level) (bytes_over_time({app="api"} | json | level_extracted=~"err.*|warn" [5m]))`,
		`sum by (pipeline) (count_over_time({app="api"} | json | drop __error__ [2m]))`,
		`sum by (level) (count_over_time({app="api"} | json | drop __error__ [$__auto]))`,
		`sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__ [1m]))`,
	}
	newBackend := func() http.Handler {
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.stored = NewLabelTranslator(LabelStyleUnderscores, nil).ToVL
		fake.presence = true
		return srv.Config.Handler
	}
	on, off, withData := runExtentDifferential(t, newBackend, queries, extentWindows(1, s0, s0.Add(span)), nil)
	if withData < len(queries)*10 {
		t.Fatalf("only %d answers held data: the run proves little", withData)
	}
	t.Logf("buckets read: %d with the extent cache, %d without; %d answers with data", on, off, withData)
	if on*2 > off {
		t.Fatalf("the extent cache read %d buckets, the uncached proxy %d: it should read far fewer", on, off)
	}
}

// The same for the stream-label metric shapes (counts, rates, bytes, by
// label or by stream, topk, range equal to the step, a binary expression)
// over the sliding-window fake, and an unwrap the conversion check reads,
// which goes to VictoriaLogs whole.
// conformance: quality/query-range-extent-cache, loki_api_v1_query_range
func TestStatsExtentCacheDifferentialStreamMetrics(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	span := 6 * time.Hour
	var lines []slidingFixtureLine
	for i := 0; time.Duration(i)*13*time.Second < span; i++ {
		app := []string{"a", "b", "c"}[i%3]
		if time.Duration(i)*13*time.Second > span-time.Hour {
			app = "late"
		}
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 13 * time.Second).UnixNano(), app: app, msg: "n=" + strconv.Itoa(i%17) + " msg=tick" + strings.Repeat("x", i%5)})
	}
	queries := []string{
		`sum(count_over_time({app="a"}[1m]))`,
		`sum by (app) (count_over_time({app=~".+"}[5m]))`,
		`sum by (app) (rate({app=~".+"}[1m]))`,
		`sum(bytes_over_time({app=~".+"}[2m]))`,
		`sum by (app) (bytes_rate({app=~".+"}[5m]))`,
		`topk(2, sum by (app) (rate({app=~".+"}[5m])))`,
		`count_over_time({app="a"}[1m])`,
		`sum by (app) (count_over_time({app=~".+"}[$__auto]))`,
		`sum(rate({app="a"}[1m])) / sum(rate({app=~".+"}[1m]))`,
		`sum by (app) (sum_over_time({app=~".+"} | logfmt | unwrap n [5m]))`,
	}
	newBackend := func() http.Handler {
		srv, _ := newSlidingFakeVL(t, lines)
		return srv.Config.Handler
	}
	on, off, withData := runExtentDifferential(t, newBackend, queries, extentWindows(2, s0, s0.Add(span)), nil)
	if withData < len(queries)*10 {
		t.Fatalf("only %d answers held data: the run proves little", withData)
	}
	t.Logf("buckets read: %d with the extent cache, %d without; %d answers with data", on, off, withData)
	if on*2 > off {
		t.Fatalf("the extent cache read %d buckets, the uncached proxy %d: it should read far fewer", on, off)
	}
}

// Buckets newer than now - -query-range-freshness are never stored: a window
// that crosses the horizon reads them from VictoriaLogs on every request,
// and the answer stays the uncached one; the next refresh reads only the
// buckets past the horizon.
// conformance: quality/query-range-extent-cache
func TestStatsExtentCacheFreshnessHorizon(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	span := 6 * time.Hour
	rows := extentJSONFixture(s0, span)
	newBackend := func() http.Handler {
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.stored = NewLabelTranslator(LabelStyleUnderscores, nil).ToVL
		return srv.Config.Handler
	}
	horizon := s0.Add(3 * time.Hour)
	onSrv, onShim := newExtentShim(t, newBackend())
	offSrv, _ := newExtentShim(t, newBackend())
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	on.queryRangeFreshness = time.Since(horizon)
	query := `sum by (level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`
	step := 10 * time.Second
	end := horizon.Add(30 * time.Minute)
	for i := 0; i < 3; i++ {
		start := end.Add(-2 * time.Hour)
		onShim.reset()
		wantCode, want := extentQueryRange(t, off, query, start, end, step, nil)
		gotCode, got := extentQueryRange(t, on, query, start, end, step, nil)
		if gotCode != http.StatusOK || gotCode != wantCode || !bytes.Equal(got, want) {
			t.Fatalf("run %d: answer differs\n with: %d %s\nwithout: %d %s", i, gotCode, got, wantCode, want)
		}
		onShim.mu.Lock()
		calls := append([]extentShimCall(nil), onShim.calls...)
		onShim.mu.Unlock()
		var stats []extentShimCall
		for _, c := range calls {
			if !strings.Contains(c.query, "| limit 1") {
				stats = append(stats, c)
			}
		}
		if i == 0 {
			if len(stats) != 1 {
				t.Fatalf("a cold request is one call as it came, got %d", len(stats))
			}
		} else {
			// Only the buckets from the horizon on: the stored ones end before it.
			// (The horizon moves on with the clock between requests.)
			if len(stats) != 1 || stats[0].start > horizon.Add(step).UnixNano() || stats[0].start < horizon.Add(-step).UnixNano() {
				t.Fatalf("run %d: expected one call from the horizon, got %+v (horizon %d)", i, stats, horizon.UnixNano())
			}
		}
		end = end.Add(time.Minute)
	}
}

// A VictoriaLogs error is the call's answer and nothing of it is stored: the
// next request reads the range again, and gets the uncached answer.
// conformance: quality/query-range-extent-cache
func TestStatsExtentCacheDoesNotStoreErrors(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	rows := extentJSONFixture(s0, 6*time.Hour)
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.stored = NewLabelTranslator(LabelStyleUnderscores, nil).ToVL
	onSrv, shim := newExtentShim(t, srv.Config.Handler)
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	query := `sum by (level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`
	start, end, step := s0.Add(time.Hour), s0.Add(2*time.Hour), 10*time.Second
	shim.fail = func(int64, int64) bool { return true }
	if code, body := extentQueryRange(t, on, query, start, end, step, nil); code == http.StatusOK {
		t.Fatalf("a backend failure must not answer 200: %s", body)
	}
	shim.mu.Lock()
	shim.fail = nil
	shim.mu.Unlock()
	shim.reset()
	if code, body := extentQueryRange(t, on, query, start, end.Add(time.Minute), step, nil); code != http.StatusOK {
		t.Fatalf("after recovery: %d %s", code, body)
	}
	if calls, _ := shim.bucketsRead(); calls != 1 {
		t.Fatalf("the range must be read whole after a failure, got %d calls", calls)
	}
}

// Entries are per tenant: the same query and window over two tenants'
// different data answer each tenant its own data, with the cache warm for
// both.
// conformance: quality/query-range-extent-cache, tenant-numeric-routing
func TestStatsExtentCacheTenantIsolation(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	byTenant := map[string][]slidingFixtureLine{}
	for i := 0; i < 1000; i++ {
		ts := s0.Add(time.Duration(i) * 15 * time.Second).UnixNano()
		byTenant["1"] = append(byTenant["1"], slidingFixtureLine{ts: ts, app: "a", msg: "x"})
		if i%3 == 0 {
			byTenant["2"] = append(byTenant["2"], slidingFixtureLine{ts: ts, app: "a", msg: "x"})
		}
	}
	tenantFakes := map[string]http.Handler{}
	for tenant, lines := range byTenant {
		srv, _ := newSlidingFakeVL(t, lines)
		tenantFakes[tenant] = srv.Config.Handler
	}
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h, ok := tenantFakes[r.Header.Get("AccountID")]; ok {
			h.ServeHTTP(w, r)
			return
		}
		tenantFakes["1"].ServeHTTP(w, r) // version and health probes
	})
	onSrv, _ := newExtentShim(t, backend)
	offSrv, _ := newExtentShim(t, backend)
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	query := `sum(count_over_time({app="a"}[1m]))`
	step := time.Minute
	for round := 0; round < 3; round++ {
		end := s0.Add(3*time.Hour + time.Duration(round)*time.Minute)
		answers := map[string][]byte{}
		for _, tenant := range []string{"1", "2"} {
			header := http.Header{"X-Scope-Orgid": {tenant}}
			_, want := extentQueryRange(t, off, query, end.Add(-2*time.Hour), end, step, header)
			code, got := extentQueryRange(t, on, query, end.Add(-2*time.Hour), end, step, header)
			if code != http.StatusOK || !bytes.Equal(got, want) {
				t.Fatalf("tenant %s round %d: %d\n with: %s\nwithout: %s", tenant, round, code, got, want)
			}
			answers[tenant] = got
		}
		if bytes.Equal(answers["1"], answers["2"]) {
			t.Fatalf("round %d: the two tenants got the same answer: %s", round, answers["1"])
		}
	}
}

// Under a small byte budget the cache evicts entries (the response cache's
// own LRU) and the answers stay the uncached ones; a request with
// Cache-Control: no-cache is answered alike (Loki 3.7.7 serves query_range
// from its results cache whatever that header says).
// conformance: quality/query-range-extent-cache
func TestStatsExtentCacheEvictionAndNoCache(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 2000; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 10 * time.Second).UnixNano(), app: []string{"a", "b"}[i%2], msg: "n=1"})
	}
	newBackend := func() http.Handler {
		srv, _ := newSlidingFakeVL(t, lines)
		return srv.Config.Handler
	}
	onSrv, _ := newExtentShim(t, newBackend())
	offSrv, _ := newExtentShim(t, newBackend())
	// 64 KiB: an entry may hold 6.4 KiB, so most splits are not kept and the
	// rest evict each other.
	on := newExtentTestProxy(t, onSrv.URL, true, 64<<10)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	query := `sum by (app) (count_over_time({app=~".+"}[1m]))`
	for i, w := range extentWindows(3, s0, s0.Add(5*time.Hour)) {
		header := http.Header{}
		if i%2 == 1 {
			header.Set("Cache-Control", "no-cache")
		}
		wantCode, want := extentQueryRange(t, off, query, w.start, w.end, w.step, header)
		gotCode, got := extentQueryRange(t, on, query, w.start, w.end, w.step, header)
		if gotCode != wantCode || !bytes.Equal(got, want) {
			t.Fatalf("window %d: answer differs\n with: %d %s\nwithout: %d %s", i, gotCode, got, wantCode, want)
		}
	}
	if stats := on.cache.Stats(); stats.Bytes > 64<<10 {
		t.Fatalf("the cache holds %d bytes over its 64 KiB budget", stats.Bytes)
	}
}

// Requests through the extent cache leave no goroutine behind and the cache
// within its byte budget.
// conformance: quality/query-range-extent-cache
func TestStatsExtentCacheBoundedResources(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 1500; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 12 * time.Second).UnixNano(), app: "a", msg: "n=1"})
	}
	srv, _ := newSlidingFakeVL(t, lines)
	onSrv, _ := newExtentShim(t, srv.Config.Handler)
	const budget = 1 << 20
	on := newExtentTestProxy(t, onSrv.URL, true, budget)
	query := `sum(rate({app="a"}[1m]))`
	run := func() {
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				end := s0.Add(4*time.Hour + time.Duration(i%4)*time.Minute)
				if code, body := extentQueryRange(t, on, query, end.Add(-3*time.Hour), end, 10*time.Second, nil); code != http.StatusOK {
					t.Errorf("status %d: %s", code, body)
				}
			}()
		}
		wg.Wait()
	}
	run() // warm the cache and the client's connections
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		run()
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
	if stats := on.cache.Stats(); stats.Bytes > budget {
		t.Fatalf("the cache holds %d bytes over its %d budget", stats.Bytes, budget)
	}
}

// The cache leaves alone the queries whose buckets depend on more than their
// own rows, and reads exactly the forms whose meaning is fixed.
// conformance: quality/query-range-extent-cache
func TestStatsExtentCacheQueryAndArgForms(t *testing.T) {
	for query, want := range map[string]bool{
		`app:="api" | stats by (level) count() as c`:                                                                              true,
		`app:="api" | unpack_json fields (level) | filter level:="error" | stats count() as c`:                                    true,
		`app:="api" | filter _msg:"a|b" | stats count() as c`:                                                                     true,
		`app:="api" | math ((_time - 5) % 10) as p | stats count() as c`:                                                          true,
		`app:="api" _time:5m | stats count() as c`:                                                                                false,
		`app:="api" | time_add 1h | stats count() as c`:                                                                           false,
		`app:="api" | stats by (level) count() as c | running_stats sum(c) as s`:                                                  false,
		`app:="api" | stats count() as c | total_stats sum(c) as s`:                                                               false,
		`app:="api" | filter level:in(app:="api" | stats by (level) count() as r | fields level) | stats by (level) count() as c`: false,
		`app:="api" | join by (level) (app:="x" | stats by (level) count() as r) | stats count() as c`:                            false,
		`app:="api" | filter _msg:"unclosed | stats count() as c`:                                                                 false,
	} {
		if got := statsExtentQueryCacheable(query); got != want {
			t.Errorf("statsExtentQueryCacheable(%q) = %v, want %v", query, got, want)
		}
	}
	for raw, want := range map[string]int64{"10s": 10e9, "60000000000ns": 60e9, "-1ns": -1, "5m": 300e9, "1h": 3600e9, "2d": 2 * 86400e9, "250ms": 250e6} {
		if got, ok := parseVLWholeDuration(raw); !ok || got != want {
			t.Errorf("parseVLWholeDuration(%q) = %d, %v; want %d", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"", "1.5s", "1h30m", "s", "10", "10x"} {
		if _, ok := parseVLWholeDuration(raw); ok {
			t.Errorf("parseVLWholeDuration(%q) must be refused", raw)
		}
	}
	for raw, want := range map[string]int64{
		"1700001000":                     1700001000e9,
		"1700001000123":                  1700001000123e6,
		"1700001000123456":               1700001000123456e3,
		"1700001000123456789":            1700001000123456789,
		"2023-11-14T22:30:00Z":           1700001000e9,
		"2023-11-14T22:30:00.000000001Z": 1700001000e9 + 1,
	} {
		if got, ok := parseVLStatsTime(raw); !ok || got != want {
			t.Errorf("parseVLStatsTime(%q) = %d, %v; want %d", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"", "now", "now-1h", "2023-11-14T22:30:00+01:00", "2023-11-14T22:30:00", "1700001000.5", "-5"} {
		if _, ok := parseVLStatsTime(raw); ok {
			t.Errorf("parseVLStatsTime(%q) must be refused", raw)
		}
	}
}

// An answer is taken apart and put back together byte for byte; one that is
// not in VictoriaLogs' form or order, or whose points are off the grid, is
// left alone; an entry survives its encoding.
// conformance: quality/query-range-extent-cache
func TestStatsExtentFragmentRoundTrip(t *testing.T) {
	req := &statsExtentRequest{step: int64(10 * time.Second), first: 170000100, last: 170000105}
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"__name__":"c","app":"a","lvl":""},"values":[[1700001000,"1"],[1700001010,"2.5"]]},` +
		`{"metric":{"__name__":"c","app":"b","lvl":"x\"y"},"values":[[1700001050,"3"]]},` +
		`{"metric":{"__name__":"n","app":"a","lvl":""},"values":[[1700001000,"1"]]}]}}`)
	f, ok := parseStatsExtentFragment(body, req, req.first, req.last)
	if !ok {
		t.Fatal("a VictoriaLogs answer must be read")
	}
	if got, ok := renderStatsExtentPieces([]*statsExtentFragment{f}); !ok || !bytes.Equal(got, body) {
		t.Fatalf("rebuilt answer differs:\n%s\n%s", got, body)
	}
	raw, err := encodeStatsExtentChunk(f)
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeStatsExtentChunk(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := renderStatsExtentPieces([]*statsExtentFragment{back}); !bytes.Equal(got, body) {
		t.Fatalf("decoded entry differs:\n%s\n%s", got, body)
	}
	// Split in two and joined again.
	if got, _ := renderStatsExtentPieces([]*statsExtentFragment{back.slice(req.first, req.first+2), back.slice(req.first+3, req.last)}); !bytes.Equal(got, body) {
		t.Fatalf("joined slices differ:\n%s\n%s", got, body)
	}
	for name, bad := range map[string]string{
		"labels out of order": strings.Replace(string(body), `"app":"a","lvl":""},"values":[[1700001000,"1"],[1700001010`, `"app":"z","lvl":""},"values":[[1700001000,"1"],[1700001010`, 1),
		"names apart":         strings.Replace(string(body), `{"metric":{"__name__":"c","app":"b"`, `{"metric":{"__name__":"n","app":"b"`, 1),
		"off the grid":        strings.Replace(string(body), "1700001050", "1700001053", 1),
		"outside the range":   strings.Replace(string(body), "1700001050", "1700001090", 1),
		"spaces":              strings.Replace(string(body), `"values":[`, `"values": [`, 1),
		"truncated":           string(body[:len(body)-3]),
	} {
		if _, ok := parseStatsExtentFragment([]byte(bad), req, req.first, req.last); ok {
			t.Errorf("%s: must be left alone: %s", name, bad)
		}
	}
	if _, err := decodeStatsExtentChunk(raw[:len(raw)/2]); err == nil {
		t.Error("a truncated entry must not decode")
	}
}

// Kept buckets are reused for -query-range-extent-cache-ttl from their oldest
// read, then read again: a row written late into a kept range shows once its
// entry expired.
// conformance: quality/query-range-extent-cache
func TestStatsExtentCacheEntriesExpire(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 1500; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 12 * time.Second).UnixNano(), app: "a", msg: "n=1"})
	}
	srv, fake := newSlidingFakeVL(t, lines)
	onSrv, shim := newExtentShim(t, srv.Config.Handler)
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	query := `sum(count_over_time({app="a"}[1m]))`
	end := s0.Add(4 * time.Hour)
	if code, body := extentQueryRange(t, on, query, end.Add(-3*time.Hour), end, time.Minute, nil); code != http.StatusOK {
		t.Fatalf("%d %s", code, body)
	}
	// A late row inside the kept range.
	late := s0.Add(2 * time.Hour).Add(30 * time.Second).UnixNano()
	fake.mu.Lock()
	fake.lines = append(fake.lines, slidingFixtureLine{ts: late, app: "a", msg: "n=1"})
	fake.mu.Unlock()
	count := func(body []byte) int {
		return strings.Count(string(body), `"6"`) // five lines a minute, six with the late row
	}
	shim.reset()
	_, kept := extentQueryRange(t, on, query, end.Add(-3*time.Hour+time.Minute), end.Add(time.Minute), time.Minute, nil)
	if _, buckets := shim.bucketsRead(); buckets > 2 || count(kept) != 0 {
		t.Fatalf("within the TTL the kept buckets answer: %d buckets read, late row seen %d times", buckets, count(kept))
	}
	restore := cache.AdvanceClockForTesting(time.Hour + time.Minute)
	defer restore()
	shim.reset()
	_, fresh := extentQueryRange(t, on, query, end.Add(-3*time.Hour+2*time.Minute), end.Add(2*time.Minute), time.Minute, nil)
	if _, buckets := shim.bucketsRead(); buckets < 170 || count(fresh) == 0 {
		t.Fatalf("after the TTL the range is read again: %d buckets read, late row seen %d times", buckets, count(fresh))
	}
}

// A miss hands VictoriaLogs' answer to the caller as it streams; the copy is
// stored only when the caller read it to the end and it fits the limit.
// conformance: quality/query-range-extent-cache
func TestStatsExtentTeeHandsOnOnlyWholeAnswers(t *testing.T) {
	body := strings.Repeat("x", 100)
	run := func(limit int64, read func(io.Reader)) (got []byte, called bool) {
		tee := &statsExtentTee{ReadCloser: io.NopCloser(strings.NewReader(body)), limit: limit, done: func(b []byte) { got, called = b, true }}
		read(tee)
		_ = tee.Close()
		_ = tee.Close() // a second close hands nothing on again
		return got, called
	}
	whole := func(r io.Reader) { _, _ = io.ReadAll(r) }
	if got, called := run(1000, whole); !called || string(got) != body {
		t.Fatalf("a whole answer must be handed on: called=%v len=%d", called, len(got))
	}
	if _, called := run(1000, func(r io.Reader) { _, _ = io.ReadFull(r, make([]byte, 10)) }); called {
		t.Fatal("an answer read in part must not be handed on")
	}
	if _, called := run(50, whole); called {
		t.Fatal("an answer over the limit must not be handed on")
	}
}
