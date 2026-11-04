package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// segRow is one stored line of the fake VictoriaLogs: its time, its fields
// and the length of its message.
type segRow struct {
	ts     int64
	labels map[string]string
	size   int
}

type segCall struct {
	path             string
	start, end, step int64
	query            string
}

// segVL is a fake VictoriaLogs that evaluates the volume stats queries the
// proxy sends over stored rows: [start, end) (end exclusive, as VictoriaLogs
// since v1.36.0), an optional `_time:[a, b)` clip in the query, `stats by
// (...) sum_len(_msg)`, epoch-aligned stats_query_range buckets labelled by
// their start, and `sort by (_b desc, ...) limit N`. Rows are kept per
// AccountID, so tenants see their own rows. Every call is recorded.
type segVL struct {
	*httptest.Server
	mu    sync.Mutex
	rows  map[string][]segRow
	calls []segCall
	// fail, when set, answers a stats call with an error status and body.
	fail func(call segCall) (int, string)
}

func newSegVL(t testing.TB) *segVL {
	t.Helper()
	vl := &segVL{rows: map[string][]segRow{}}
	vl.Server = httptest.NewServer(http.HandlerFunc(vl.serve))
	t.Cleanup(vl.Close)
	return vl
}

func (vl *segVL) add(account string, rows ...segRow) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vl.rows[account] = append(vl.rows[account], rows...)
}

// take returns and clears the recorded stats calls.
func (vl *segVL) take() []segCall {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	out := vl.calls
	vl.calls = nil
	return out
}

func segParseInt(raw string) int64 {
	n, _ := strconv.ParseInt(raw, 10, 64)
	return n
}

// segGroupFields reads `stats by (a, "b.c")`.
func segGroupFields(query string) []string {
	at := strings.LastIndex(query, "| stats by (")
	if at < 0 {
		return nil
	}
	rest := query[at+len("| stats by ("):]
	rest = rest[:strings.Index(rest, ")")]
	var fields []string
	for _, f := range strings.Split(rest, ",") {
		fields = append(fields, strings.Trim(strings.TrimSpace(f), `"`))
	}
	return fields
}

// segClip reads the `_time:[a, b)` clip the proxy adds to stats_query_range.
func segClip(query string) (int64, int64, bool) {
	at := strings.Index(query, "_time:[")
	if at < 0 {
		return 0, 0, false
	}
	rest := query[at+len("_time:["):]
	rest = rest[:strings.Index(rest, ")")]
	parts := strings.SplitN(rest, ", ", 2)
	a, _ := time.Parse(time.RFC3339Nano, parts[0])
	b, _ := time.Parse(time.RFC3339Nano, parts[1])
	return a.UnixNano(), b.UnixNano(), true
}

func segValue(row segRow, field string) string {
	if field == volumeStreamField {
		names := make([]string, 0, len(row.labels))
		for k := range row.labels {
			names = append(names, k)
		}
		sort.Strings(names)
		parts := make([]string, 0, len(names))
		for _, k := range names {
			parts = append(parts, k+"="+strconv.Quote(row.labels[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	if v, ok := row.labels[field]; ok {
		return v
	}
	if field == "service_name" {
		return unknownServiceName // the coalesce default of the derivation pipes
	}
	return ""
}

func (vl *segVL) serve(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	account := r.Header.Get("AccountID")
	switch r.URL.Path {
	case "/select/logsql/stats_query", "/select/logsql/stats_query_range":
	case "/select/logsql/field_names", "/select/logsql/stream_field_names":
		vl.mu.Lock()
		names := map[string]bool{}
		for _, row := range vl.rows[account] {
			for k := range row.labels {
				names[k] = true
			}
		}
		vl.mu.Unlock()
		values := []map[string]interface{}{}
		for k := range names {
			values = append(values, map[string]interface{}{"value": k, "hits": 1})
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"values": values})
		return
	default:
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	query := r.Form.Get("query")
	call := segCall{path: r.URL.Path, start: segParseInt(r.Form.Get("start")), end: segParseInt(r.Form.Get("end")), query: query}
	if raw := r.Form.Get("step"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			http.Error(w, "bad step", http.StatusBadRequest)
			return
		}
		call.step = int64(d)
	}
	fields := segGroupFields(query)
	clipA, clipB, clipped := segClip(query)
	limit := -1
	if at := strings.LastIndex(query, ") limit "); at >= 0 && strings.Contains(query, "| sort by (") {
		limit, _ = strconv.Atoi(strings.TrimSpace(query[at+len(") limit "):]))
	}
	type group struct {
		values []string
		bytes  map[int64]uint64
	}
	groups := map[string]*group{}
	vl.mu.Lock()
	vl.calls = append(vl.calls, call)
	if vl.fail != nil {
		if status, body := vl.fail(call); status != 0 {
			vl.mu.Unlock()
			http.Error(w, body, status)
			return
		}
	}
	for _, row := range vl.rows[account] {
		if row.ts < call.start || row.ts >= call.end || (clipped && (row.ts < clipA || row.ts >= clipB)) {
			continue
		}
		values := make([]string, len(fields))
		for i, f := range fields {
			values[i] = segValue(row, f)
		}
		key := strings.Join(values, "\x00")
		g := groups[key]
		if g == nil {
			g = &group{values: values, bytes: map[int64]uint64{}}
			groups[key] = g
		}
		bucket := call.end
		if call.step > 0 {
			bucket = row.ts - row.ts%call.step
		}
		g.bytes[bucket] += uint64(row.size)
	}
	vl.mu.Unlock()
	list := make([]*group, 0, len(groups))
	for _, g := range groups {
		list = append(list, g)
	}
	sort.Slice(list, func(i, j int) bool {
		bi, bj := list[i].bytes[call.end], list[j].bytes[call.end]
		if bi != bj {
			return bi > bj
		}
		return strings.Join(list[i].values, "\x00") < strings.Join(list[j].values, "\x00")
	})
	if limit >= 0 && len(list) > limit {
		list = list[:limit]
	}
	result := make([]map[string]interface{}, 0, len(list))
	for _, g := range list {
		metric := map[string]string{"__name__": volumeBytesField}
		for i, f := range fields {
			metric[f] = g.values[i]
		}
		item := map[string]interface{}{"metric": metric}
		if call.step > 0 {
			buckets := make([]int64, 0, len(g.bytes))
			for b := range g.bytes {
				buckets = append(buckets, b)
			}
			sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
			values := make([][]interface{}, 0, len(buckets))
			for _, b := range buckets {
				values = append(values, []interface{}{float64(b) / 1e9, strconv.FormatUint(g.bytes[b], 10)})
			}
			item["values"] = values
		} else {
			item["value"] = []interface{}{float64(call.end) / 1e9, strconv.FormatUint(g.bytes[call.end], 10)}
		}
		result = append(result, item)
	}
	resultType := "vector"
	if call.step > 0 {
		resultType = "matrix"
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": resultType, "result": result}})
}

// newSegProxy builds a proxy over the fake. inventory < 0 turns the segments
// off: the whole-window path the proxy took before them.
func newSegProxy(t testing.TB, backend string, inventory int, freshness time.Duration) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                   backend,
		Cache:                        cache.New(60*time.Second, 100000),
		LogLevel:                     "error",
		MetadataInventoryParallelism: inventory,
		MaxStatsCacheFreshness:       freshness,
	})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	return p
}

// drilldownHeaders are the headers Logs Drilldown's landing volume carries.
var drilldownHeaders = map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app", "User-Agent": "Grafana/13.2.1"}

// segVolume asks the proxy for an instant volume and returns the raw answer.
func segVolume(t testing.TB, p *Proxy, org string, params url.Values, headers ...map[string]string) string {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume?"+params.Encode(), nil)
	if org != "" {
		r.Header.Set("X-Scope-OrgID", org)
	}
	for _, h := range headers {
		for k, v := range h {
			r.Header.Set(k, v)
		}
	}
	w := httptest.NewRecorder()
	p.handleVolume(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("volume %s: %d %s", params.Encode(), w.Code, w.Body.String())
	}
	return w.Body.String()
}

func segWindow(params url.Values, start, end time.Time) url.Values {
	out := url.Values{}
	for k, v := range params {
		out[k] = v
	}
	out.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	out.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	return out
}

// landingParams is Logs Drilldown's landing volume.
func landingParams() url.Values {
	return url.Values{"query": {`{service_name=~".+"}`}, "limit": {"5000"}, "targetLabels": {"service_name"}, "aggregateBy": {"series"}}
}

// lokiServiceVolumes is the Loki-semantics reference for a service_name
// volume over [start, end): the line bytes of every service summed, the
// largest `limit` kept (ties by name), ordered by volume then name, stamped at
// the request end (seriesvolume.MapToVolumeResponse, queryrange.toPrometheusData).
func lokiServiceVolumes(rows []segRow, start, end time.Time, limit int) string {
	sums := map[string]uint64{}
	for _, row := range rows {
		if row.ts >= start.UnixNano() && row.ts < end.UnixNano() {
			sums[segValue(row, "service_name")] += uint64(row.size)
		}
	}
	type vol struct {
		name  string
		bytes uint64
	}
	vols := make([]vol, 0, len(sums))
	for name, b := range sums {
		vols = append(vols, vol{name, b})
	}
	sort.Slice(vols, func(i, j int) bool {
		if vols[i].bytes != vols[j].bytes {
			return vols[i].bytes > vols[j].bytes
		}
		return lokiVolumeSeriesName(map[string]string{"service_name": vols[i].name}) < lokiVolumeSeriesName(map[string]string{"service_name": vols[j].name})
	})
	if len(vols) > limit {
		vols = vols[:limit]
	}
	// Loki stamps the sample at the end rounded up to the millisecond
	// (pkg/util/conv.go RoundToMilliseconds).
	endMs := end.UnixNano() / int64(time.Millisecond)
	if end.UnixNano()%int64(time.Millisecond) != 0 {
		endMs++
	}
	result := make([]map[string]interface{}, 0, len(vols))
	for _, v := range vols {
		result = append(result, map[string]interface{}{
			"metric": map[string]string{"service_name": v.name},
			"value":  []interface{}{float64(endMs) / 1e3, strconv.FormatUint(v.bytes, 10)},
		})
	}
	out, _ := json.Marshal(map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "vector", "result": result}})
	return string(out)
}

func segJSONEqual(t testing.TB, label, got, want string) {
	t.Helper()
	var g, w interface{}
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("%s: got is not JSON: %v %s", label, err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: want is not JSON: %v %s", label, err, want)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("%s:\n got: %s\nwant: %s", label, gb, wb)
	}
}

// segFixture writes `n` rows over [from, to) for a few services, a stream
// without a service label (unknown_service), and rows exactly on minute, 5
// minute and hour boundaries.
func segFixture(rng *rand.Rand, from, to time.Time, n int) []segRow {
	services := []string{"api", "web", "db", "cache", "auth"}
	levels := []string{"info", "warn", "error", ""}
	span := to.Sub(from)
	rows := make([]segRow, 0, n)
	for i := 0; i < n; i++ {
		ts := from.Add(time.Duration(rng.Int63n(int64(span))))
		switch i % 7 {
		case 0:
			ts = ts.Truncate(time.Minute)
		case 1:
			ts = ts.Truncate(time.Hour)
		case 2:
			ts = ts.Truncate(5 * time.Minute)
		}
		if ts.Before(from) {
			ts = from
		}
		labels := map[string]string{"app": "app-" + strconv.Itoa(rng.Intn(3)), "pod": "pod-" + strconv.Itoa(rng.Intn(4))}
		if s := rng.Intn(len(services) + 1); s < len(services) {
			labels["service_name"] = services[s]
		}
		if l := levels[rng.Intn(len(levels))]; l != "" {
			labels["level"] = l
		}
		rows = append(rows, segRow{ts: ts.UnixNano(), labels: labels, size: 1 + rng.Intn(400)})
	}
	return rows
}

func segCallsSummary(calls []segCall) (n int, span time.Duration) {
	for _, c := range calls {
		n++
		span += time.Duration(c.end - c.start)
	}
	return n, span
}

// segRepeatEnd is the end of the first window of the repeat test: 5 s into a
// minute, at least 3 s before now. The test moves the window by 7 s three times,
// so its start stays inside one minute whatever the clock says. A start that
// crosses a minute, 5-minute or hour boundary stops using the cached segment
// that began before it and reads the pieces of that segment instead (up to 4
// minutes, or 55 for an hour boundary): a wall-clock phase the test must not
// depend on, as about 7% of the phases of an hour did.
func segRepeatEnd(now time.Time) time.Time {
	end := now.Add(-3 * time.Second).Truncate(time.Minute).Add(5 * time.Second)
	if end.After(now.Add(-3 * time.Second)) {
		end = end.Add(-time.Minute)
	}
	return end
}

// TestVolumeSegmentsRepeatWindowIsClockPhaseIndependent plans the repeat test's
// four windows against a cache filled by the earlier ones, for every second of
// an hour as "now" (the tail and its sealing move with it, as the proxy's clock
// does): each repeat reads at most 3 calls and the last 10 minutes plus the
// moved start, so the repeat test cannot fail on the time it runs at.
// conformance: quality/index-volume-settled-segments
func TestVolumeSegmentsRepeatWindowIsClockPhaseIndependent(t *testing.T) {
	base := time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC)
	for off := 0; off < 3600; off++ {
		now0 := base.Add(time.Duration(off) * time.Second)
		cached := map[[2]int64]bool{}
		end0 := segRepeatEnd(now0)
		for i := 0; i < 4; i++ {
			now := now0.Add(time.Duration(i) * 200 * time.Millisecond)
			end := end0.Add(time.Duration(i) * 7 * time.Second)
			segs := planInventorySegments(end.Add(-24*time.Hour).UnixNano(), end.UnixNano(), now.Add(-DefaultMaxStatsCacheFreshness).UnixNano())
			var missing []int
			for k, s := range segs {
				if s.level < 0 || !cached[[2]int64{s.start, int64(s.level)}] {
					missing = append(missing, k)
				}
			}
			jobs := volumeSegmentJobs(segs, missing)
			var span time.Duration
			for _, job := range jobs {
				span += time.Duration(segs[job[len(job)-1]].end - segs[job[0]].start)
			}
			for _, k := range missing {
				if segs[k].level >= 0 {
					cached[[2]int64{segs[k].start, int64(segs[k].level)}] = true
				}
			}
			if i > 0 && (len(jobs) > 3 || span > DefaultMaxStatsCacheFreshness+2*time.Minute+time.Duration(i)*7*time.Second) {
				t.Fatalf("now=%s repeat %d reads %d calls over %s", now0.Format("15:04:05"), i, len(jobs), span)
			}
		}
	}
}

// TestVolumeSegmentsRepeatReadsOnlyTheEdges: Logs Drilldown's landing volume
// over 24h, asked again with the window moved by seconds (a refresh), reads
// only its two edges from VictoriaLogs, and every answer equals the
// whole-window path's and the Loki-semantics reference.
// conformance: quality/index-volume-settled-segments, loki_api_v1_index_volume
func TestVolumeSegmentsRepeatReadsOnlyTheEdges(t *testing.T) {
	vl := newSegVL(t)
	now := time.Now()
	rows := segFixture(rand.New(rand.NewSource(7)), now.Add(-26*time.Hour), now, 4000)
	vl.add("", rows...)
	seg := newSegProxy(t, vl.URL, 0, DefaultMaxStatsCacheFreshness)
	whole := newSegProxy(t, vl.URL, -1, DefaultMaxStatsCacheFreshness)

	end := segRepeatEnd(now)
	start := end.Add(-24 * time.Hour)
	for i := 0; i < 4; i++ {
		params := segWindow(landingParams(), start, end)
		got := segVolume(t, seg, "", params, drilldownHeaders)
		segCalls := vl.take()
		want := segVolume(t, whole, "", params, drilldownHeaders)
		wholeCalls := vl.take()
		if got != want {
			t.Fatalf("request %d: segmented answer differs from the whole-window answer\n got: %s\nwant: %s", i, got, want)
		}
		segJSONEqual(t, fmt.Sprintf("request %d vs Loki reference", i), got, lokiServiceVolumes(rows, start, end, 5000))
		n, span := segCallsSummary(segCalls)
		wn, wspan := segCallsSummary(wholeCalls)
		t.Logf("request %d: segmented %d VictoriaLogs calls over %s; whole-window %d call over %s", i, n, span, wn, wspan)
		if span > 24*time.Hour {
			t.Errorf("request %d: segmented calls read %s, more than the 24h window", i, span)
		}
		if i == 0 && n > 8 {
			t.Errorf("cold window took %d VictoriaLogs calls; runs of one level must share a stats_query_range call", n)
		}
		if i > 0 {
			if n > 3 || span > DefaultMaxStatsCacheFreshness+2*time.Minute+time.Duration(i)*7*time.Second {
				t.Errorf("repeat %d read %d calls over %s; only the edges (the last %s and the moved start) should be read", i, n, span, DefaultMaxStatsCacheFreshness)
			}
		}
		start, end = start.Add(7*time.Second), end.Add(7*time.Second)
	}
}

// TestVolumeSegmentsMatchWholeWindowOverRandomWindows is the differential
// test: random windows, shapes and limits answered from segments (whose cache
// fills and is reused across the windows) equal the whole-window path and,
// for service_name volumes, the Loki-semantics reference.
// conformance: quality/index-volume-settled-segments, loki_api_v1_index_volume
func TestVolumeSegmentsMatchWholeWindowOverRandomWindows(t *testing.T) {
	rng := rand.New(rand.NewSource(20261103))
	vl := newSegVL(t)
	now := time.Now()
	rows := segFixture(rng, now.Add(-30*time.Hour), now, 3000)
	vl.add("", rows...)
	seg := newSegProxy(t, vl.URL, 0, DefaultMaxStatsCacheFreshness)
	whole := newSegProxy(t, vl.URL, -1, DefaultMaxStatsCacheFreshness)
	shapes := []url.Values{
		landingParams(),
		{"query": {`{service_name=~".+"}`}, "targetLabels": {"service_name"}, "limit": {"2"}},
		{"query": {`{service_name=~".+"}`}},
		{"query": {`{app=~".+"}`}, "aggregateBy": {"labels"}},
		{"query": {`{app=~".+"}`}, "targetLabels": {"detected_level"}, "limit": {"3"}},
		{"query": {`{app=~".+"}`}, "aggregateBy": {"labels"}, "targetLabels": {"pod,app"}, "limit": {"1"}},
	}
	segmented := 0
	for i := 0; i < 120; i++ {
		shape := shapes[rng.Intn(len(shapes))]
		end := now.Add(-time.Duration(rng.Int63n(int64(6 * time.Hour))))
		if rng.Intn(3) == 0 {
			end = now.Add(-time.Duration(rng.Int63n(int64(30 * time.Second))))
		}
		length := 10*time.Minute + time.Duration(rng.Int63n(int64(24*time.Hour)))
		if rng.Intn(4) == 0 { // aligned windows: every bound on a segment boundary
			end = end.Truncate(time.Hour)
			length = length.Truncate(time.Hour) + time.Hour
		}
		start := end.Add(-length)
		params := segWindow(shape, start, end)
		got := segVolume(t, seg, "", params)
		calls := vl.take()
		want := segVolume(t, whole, "", params)
		vl.take()
		if got != want {
			t.Fatalf("window %d %s: segmented answer differs from the whole-window answer\n got: %s\nwant: %s", i, params.Encode(), got, want)
		}
		if shape.Get("targetLabels") == "service_name" {
			limit, _ := strconv.Atoi(shape.Get("limit"))
			segJSONEqual(t, fmt.Sprintf("window %d vs Loki reference", i), got, lokiServiceVolumes(rows, start, end, limit))
		}
		for _, c := range calls {
			if c.path == "/select/logsql/stats_query_range" || c.end-c.start < int64(length) {
				segmented++
				break
			}
		}
	}
	if segmented < 60 {
		t.Fatalf("only %d of 120 windows were answered from segments; the test does not exercise them", segmented)
	}
}

// TestVolumeSegmentsEdgeTable pins the answers and VictoriaLogs reads at the
// boundaries of the segmented path.
// conformance: quality/index-volume-settled-segments, loki_api_v1_index_volume
func TestVolumeSegmentsEdgeTable(t *testing.T) {
	hour := time.Now().Add(-5 * time.Hour).Truncate(time.Hour)
	row := func(at time.Time, service string, size int) segRow {
		return segRow{ts: at.UnixNano(), labels: map[string]string{"service_name": service}, size: size}
	}
	type step struct {
		start, end time.Time
		params     url.Values
		org        string
		// want is the answer as service -> bytes, in answer order.
		want [][2]string
		// maxCalls bounds the VictoriaLogs calls of this request (-1: any).
		maxCalls int
		// whole: the request must be one whole-window stats_query.
		whole bool
		// addAfter rows are written after this step.
		addAfter []segRow
	}
	cases := []struct {
		name  string
		rows  map[string][]segRow
		steps []step
	}{
		{
			name: "rows exactly on segment boundaries count once",
			rows: map[string][]segRow{"": {
				row(hour, "a", 10), row(hour.Add(time.Hour), "a", 20), row(hour.Add(65*time.Minute), "a", 40),
				row(hour.Add(2*time.Hour), "a", 80), // == end: outside [start, end)
			}},
			steps: []step{
				{start: hour, end: hour.Add(2 * time.Hour), params: landingParams(), want: [][2]string{{"a", "70"}}, maxCalls: -1},
				{start: hour, end: hour.Add(2 * time.Hour), params: segWith(landingParams(), "limit", "4999"), want: [][2]string{{"a", "70"}}, maxCalls: 0},
			},
		},
		{
			name: "limit applies to the sum, not per segment",
			rows: map[string][]segRow{"": {
				row(hour.Add(10*time.Minute), "solo", 60),
				row(hour.Add(70*time.Minute), "steady", 40), row(hour.Add(130*time.Minute), "steady", 40),
			}},
			steps: []step{{start: hour, end: hour.Add(3 * time.Hour), params: url.Values{"query": {`{service_name=~".+"}`}, "targetLabels": {"service_name"}, "limit": {"1"}}, want: [][2]string{{"steady", "80"}}, maxCalls: -1}},
		},
		{
			name: "equal volumes keep Loki's name order at the limit",
			rows: map[string][]segRow{"": {
				row(hour.Add(10*time.Minute), "b", 50), row(hour.Add(70*time.Minute), "a", 50), row(hour.Add(80*time.Minute), "c", 50),
			}},
			steps: []step{{start: hour, end: hour.Add(2 * time.Hour), params: url.Values{"query": {`{service_name=~".+"}`}, "targetLabels": {"service_name"}, "limit": {"2"}}, want: [][2]string{{"a", "50"}, {"b", "50"}}, maxCalls: -1}},
		},
		{
			name: "empty segments are kept and not read again",
			rows: map[string][]segRow{"": {row(hour.Add(-3*time.Hour), "early", 5), row(hour.Add(150*time.Minute), "late", 7)}},
			steps: []step{
				{start: hour.Add(-4 * time.Hour).Add(13 * time.Second), end: hour.Add(3 * time.Hour).Add(13 * time.Second), params: landingParams(), want: [][2]string{{"late", "7"}, {"early", "5"}}, maxCalls: 8},
				{start: hour.Add(-4 * time.Hour).Add(21 * time.Second), end: hour.Add(3 * time.Hour).Add(21 * time.Second), params: landingParams(), want: [][2]string{{"late", "7"}, {"early", "5"}}, maxCalls: 2},
			},
		},
		{
			name: "the near-now edge is read live; sealed segments are kept",
			rows: map[string][]segRow{"": {row(time.Now().Add(-90*time.Minute), "svc", 100)}},
			steps: []step{
				{start: time.Now().Add(-2 * time.Hour), end: time.Now(), params: landingParams(), want: [][2]string{{"svc", "100"}}, maxCalls: -1,
					addAfter: []segRow{row(time.Now().Add(-2*time.Minute), "svc", 1), row(time.Now().Add(-9*time.Minute), "svc", 2), row(time.Now().Add(-80*time.Minute), "svc", 1000)}},
				// The rows of the last 10 minutes show at once; the row written
				// late into a sealed segment shows when that segment is re-read.
				// A refresh: the window moved, so no exact reply answers it.
				{start: time.Now().Add(-2*time.Hour + 2*time.Second), end: time.Now().Add(2 * time.Second), params: landingParams(), want: [][2]string{{"svc", "103"}}, maxCalls: 3},
			},
		},
		{
			name: "tenants never share segments",
			rows: map[string][]segRow{"1": {row(hour.Add(10*time.Minute), "one", 11)}, "2": {row(hour.Add(10*time.Minute), "two", 22)}},
			steps: []step{
				{org: "1", start: hour, end: hour.Add(time.Hour), params: landingParams(), want: [][2]string{{"one", "11"}}, maxCalls: -1},
				{org: "2", start: hour, end: hour.Add(time.Hour), params: landingParams(), want: [][2]string{{"two", "22"}}, maxCalls: -1},
				{org: "1", start: hour, end: hour.Add(time.Hour), params: segWith(landingParams(), "limit", "4999"), want: [][2]string{{"one", "11"}}, maxCalls: 0},
			},
		},
		{
			name:  "a window under 10 minutes is one whole-window call",
			rows:  map[string][]segRow{"": {row(hour.Add(time.Minute), "x", 3)}},
			steps: []step{{start: hour, end: hour.Add(9 * time.Minute), params: landingParams(), want: [][2]string{{"x", "3"}}, whole: true}},
		},
		{
			name:  "a limit VictoriaLogs applies keeps the whole-window call",
			rows:  map[string][]segRow{"": {{ts: hour.Add(time.Minute).UnixNano(), labels: map[string]string{"app": "x"}, size: 3}}},
			steps: []step{{start: hour, end: hour.Add(2 * time.Hour), params: url.Values{"query": {`{app=~".+"}`}}, whole: true}},
		},
		{
			// 10m20s ending now: the 20 seconds before the freshness window
			// hold no whole minute, so no segment can be kept.
			name:  "a window with nothing older than the freshness window is one whole-window call",
			rows:  map[string][]segRow{"": {row(time.Now().Add(-5*time.Minute), "x", 3)}},
			steps: []step{{start: time.Now().Add(-10*time.Minute - 20*time.Second), end: time.Now(), params: landingParams(), want: [][2]string{{"x", "3"}}, whole: true}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vl := newSegVL(t)
			for account, rows := range tc.rows {
				vl.add(account, rows...)
			}
			p := newSegProxy(t, vl.URL, 0, DefaultMaxStatsCacheFreshness)
			for i, st := range tc.steps {
				params := segWindow(st.params, st.start, st.end)
				body := segVolume(t, p, st.org, params)
				calls := vl.take()
				if st.want != nil {
					var resp volumeTestResponse
					if err := json.Unmarshal([]byte(body), &resp); err != nil {
						t.Fatal(err)
					}
					var got [][2]string
					for _, r := range resp.Data.Result {
						got = append(got, [2]string{r.Metric["service_name"], fmt.Sprint(r.Value[1])})
					}
					if fmt.Sprint(got) != fmt.Sprint(st.want) {
						t.Fatalf("step %d: got %v, want %v (%s)", i, got, st.want, body)
					}
				}
				if st.whole {
					if len(calls) != 1 || calls[0].path != "/select/logsql/stats_query" || calls[0].start != st.start.UnixNano() || calls[0].end != st.end.UnixNano() {
						t.Fatalf("step %d: want one whole-window stats_query, got %+v", i, calls)
					}
				}
				if st.maxCalls >= 0 && !st.whole && len(calls) > st.maxCalls {
					t.Fatalf("step %d: %d VictoriaLogs calls, want at most %d: %+v", i, len(calls), st.maxCalls, calls)
				}
				for account, rows := range map[string][]segRow{"": st.addAfter} {
					vl.add(account, rows...)
				}
			}
		})
	}
}

// segWith returns a copy of params with key set to value.
func segWith(params url.Values, key, value string) url.Values {
	out := url.Values{}
	for k, v := range params {
		out[k] = v
	}
	out.Set(key, value)
	return out
}

func segVolumeStatus(t testing.TB, p *Proxy, params url.Values, headers ...map[string]string) (int, string) {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume?"+params.Encode(), nil)
	for _, h := range headers {
		for k, v := range h {
			r.Header.Set(k, v)
		}
	}
	p.handleVolume(w, r)
	return w.Code, w.Body.String()
}

// TestVolumeSegmentsFallBackToTheWholeWindow: a request whose segments cannot
// be used is answered exactly as the whole-window path answers it, never
// from a mix of segment rows and rows of another query form.
// conformance: quality/index-volume-settled-segments, loki_api_v1_index_volume
func TestVolumeSegmentsFallBackToTheWholeWindow(t *testing.T) {
	end := time.Now().Add(-2 * time.Hour).Truncate(time.Minute).Add(17 * time.Second)
	start := end.Add(-6 * time.Hour)
	rows := segFixture(rand.New(rand.NewSource(3)), start.Add(-time.Hour), end.Add(time.Hour), 600)
	params := segWindow(landingParams(), start, end)

	t.Run("rejected optimised query retries the plain stream form", func(t *testing.T) {
		vl := newSegVL(t)
		vl.add("", rows...)
		vl.fail = func(call segCall) (int, string) {
			// The derivation pipes are the proxy's rewrite; the plain form
			// keeps only the user's selector.
			if strings.Contains(call.query, "coalesce(") || strings.Contains(call.query, "| format ") {
				return http.StatusBadRequest, "cannot parse query arg: unexpected token"
			}
			return 0, ""
		}
		seg := newSegProxy(t, vl.URL, 0, DefaultMaxStatsCacheFreshness)
		whole := newSegProxy(t, vl.URL, -1, DefaultMaxStatsCacheFreshness)
		gotCode, got := segVolumeStatus(t, seg, params)
		calls := vl.take()
		wantCode, want := segVolumeStatus(t, whole, params)
		if gotCode != wantCode || got != want {
			t.Fatalf("segmented %d %s, whole-window %d %s", gotCode, got, wantCode, want)
		}
		last := calls[len(calls)-1]
		if last.path != "/select/logsql/stats_query" || last.start != start.UnixNano() || last.end != end.UnixNano() || !strings.Contains(last.query, "stats by (_stream)") {
			t.Fatalf("the answer must come from one whole-window plain stats_query, last call %+v", last)
		}
	})

	t.Run("a segment over the cache entry size answers whole and is remembered", func(t *testing.T) {
		vl := newSegVL(t)
		// 300 services: a segment holds about 10 KB of rows, over the 2 KB
		// entry size of a 20 KB cache.
		for i := 0; i < 3000; i++ {
			at := start.Add(time.Duration(i) * (end.Sub(start) / 3000))
			vl.add("", segRow{ts: at.UnixNano(), labels: map[string]string{"service_name": fmt.Sprintf("service-%03d", i%300)}, size: 10 + i%7})
		}
		p, err := New(Config{BackendURL: vl.URL, Cache: cache.NewWithMaxBytes(time.Minute, 1000, 20000), LogLevel: "error", MaxStatsCacheFreshness: DefaultMaxStatsCacheFreshness})
		if err != nil {
			t.Fatal(err)
		}
		whole := newSegProxy(t, vl.URL, -1, DefaultMaxStatsCacheFreshness)
		for i := 0; i < 2; i++ {
			moved := segWindow(landingParams(), start.Add(time.Duration(i)*time.Second), end.Add(time.Duration(i)*time.Second))
			got := segVolume(t, p, "", moved)
			calls := vl.take()
			if want := segVolume(t, whole, "", moved); got != want {
				t.Fatalf("request %d: %s, whole-window %s", i, got, want)
			}
			vl.take()
			if i == 1 && len(calls) != 1 {
				t.Fatalf("after an oversize segment the next request must be one whole-window call, got %+v", calls)
			}
		}
	})

	t.Run("segment rows beyond the request's byte budget answer whole", func(t *testing.T) {
		vl := newSegVL(t)
		// 50 services over 8 aligned hours: about 2.3 KB of rows per hour
		// segment, 18 KB for the window, against a 4 KB whole-window answer.
		from := time.Now().Add(-30 * time.Hour).Truncate(time.Hour)
		for i := 0; i < 2000; i++ {
			vl.add("", segRow{ts: from.Add(time.Duration(i) * (8 * time.Hour / 2000)).UnixNano(), labels: map[string]string{"service_name": fmt.Sprintf("service-%02d", i%50)}, size: 7 + i%5})
		}
		shared := cache.New(time.Minute, 100000)
		build := func(budget int) *Proxy {
			p, err := New(Config{BackendURL: vl.URL, Cache: shared, LogLevel: "error", MaxStatsCacheFreshness: DefaultMaxStatsCacheFreshness,
				ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: budget}})
			if err != nil {
				t.Fatal(err)
			}
			return p
		}
		filler, small := build(0), build(10000)
		segVolume(t, filler, "", segWindow(landingParams(), from, from.Add(8*time.Hour)))
		vl.take()
		whole := newSegProxy(t, vl.URL, -1, DefaultMaxStatsCacheFreshness)
		for i := 0; i < 2; i++ {
			moved := segWindow(segWith(landingParams(), "limit", strconv.Itoa(4000+i)), from, from.Add(8*time.Hour))
			got := segVolume(t, small, "", moved)
			calls := vl.take()
			if want := segVolume(t, whole, "", moved); got != want {
				t.Fatalf("request %d: %s, whole-window %s", i, got, want)
			}
			vl.take()
			if len(calls) != 1 || calls[0].path != "/select/logsql/stats_query" || calls[0].end-calls[0].start != int64(8*time.Hour) {
				t.Fatalf("request %d: want one whole-window stats_query, got %+v", i, calls)
			}
		}
	})

	t.Run("a failing VictoriaLogs answers like the whole-window path", func(t *testing.T) {
		vl := newSegVL(t)
		vl.add("", rows...)
		vl.fail = func(segCall) (int, string) { return http.StatusServiceUnavailable, "storage unavailable" }
		seg := newSegProxy(t, vl.URL, 0, DefaultMaxStatsCacheFreshness)
		whole := newSegProxy(t, vl.URL, -1, DefaultMaxStatsCacheFreshness)
		// As Logs Drilldown sends it, and as any other client does.
		for _, headers := range []map[string]string{drilldownHeaders, nil} {
			gotCode, got := segVolumeStatus(t, seg, params, headers)
			wantCode, want := segVolumeStatus(t, whole, params, headers)
			if gotCode != wantCode || got != want || gotCode == http.StatusOK {
				t.Fatalf("headers %v: segmented %d %s, whole-window %d %s", headers, gotCode, got, wantCode, want)
			}
		}
		// A failed read keeps nothing: once VictoriaLogs answers, the
		// segments hold its rows.
		vl.fail = nil
		vl.add("", rows...)
		if got, want := segVolume(t, seg, "", params, drilldownHeaders), segVolume(t, whole, "", params, drilldownHeaders); got != want {
			t.Fatalf("after recovery: segmented %s, whole-window %s", got, want)
		}
	})
}

// FuzzVolumeSegmentsMergeLikeOneWindow: rows split into segments in any way,
// read back from one stats_query_range answer (a run) and through the cache
// entry, and summed per row, give exactly the answer of one whole-window
// stats_query over the same rows: for series and label aggregation, every
// limit, empty values and rows that repeat across segments.
// conformance: quality/index-volume-settled-segments, loki_api_v1_index_volume
func FuzzVolumeSegmentsMergeLikeOneWindow(f *testing.F) {
	f.Add([]byte{0, 1, 10, 0, 1, 2, 20, 1, 0, 1, 30, 2, 5, 3, 7, 2}, uint8(2), false)
	f.Add([]byte{1, 1, 255, 0, 1, 1, 255, 3, 2, 0, 0, 1}, uint8(1), true)
	f.Add([]byte{}, uint8(0), false)
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100000), LogLevel: "error"})
	if err != nil {
		f.Fatal(err)
	}
	services := []string{"api", "web", "", "db", "unknown_service", "a\"b"}
	levels := []string{"info", "", "error", "warn"}
	const segSize = int64(time.Hour)
	runStart := int64(1789448400) * int64(time.Second) // an hour boundary
	f.Fuzz(func(t *testing.T, data []byte, limit uint8, byLabels bool) {
		// Arbitrary bytes are never a panic.
		_, _ = parseVolumeSegmentRows(data, nil)
		_, _ = parseVolumeSegmentRows(data, map[int64]int{runStart: 0})

		targets := []string{"service_name"}
		if len(data) > 0 && data[0]%2 == 1 {
			targets = append(targets, "detected_level")
		}
		end := time.Unix(0, runStart+4*segSize)
		req := volumeRequest{query: "*", startNs: runStart, endNs: end.UnixNano(), targets: targets, byLabels: byLabels, limit: int(limit%5) + 1}
		plan := p.planVolume(t.Context(), req, "*")
		if plan.pushLimit {
			t.Fatalf("plan for %v pushes the limit; segments never serve such a plan", targets)
		}
		type fuzzRow struct {
			kv    []string
			bytes uint64
			seg   int
		}
		var rows []fuzzRow
		for i := 0; i+3 < len(data) && len(rows) < 64; i += 4 {
			var kv []string
			for _, field := range plan.groupFields {
				switch field {
				case "service_name":
					kv = append(kv, field, services[int(data[i])%len(services)])
				default:
					kv = append(kv, field, levels[int(data[i+1])%len(levels)])
				}
			}
			sortVolumeLabelPairs(kv)
			rows = append(rows, fuzzRow{kv: kv, bytes: uint64(data[i+2]) * 997, seg: int(data[i+3]) % 4})
		}
		body := func(points map[string][2]interface{}, step bool) []byte {
			result := []map[string]interface{}{}
			for key, pt := range points {
				metric := map[string]string{"__name__": volumeBytesField}
				kv := strings.Split(key, "\x00")
				for j := 0; j+1 < len(kv); j += 2 {
					metric[kv[j]] = kv[j+1]
				}
				item := map[string]interface{}{"metric": metric}
				if step {
					item["values"] = pt[1]
				} else {
					item["value"] = pt[1]
				}
				result = append(result, item)
			}
			out, _ := json.Marshal(map[string]interface{}{"status": "success", "data": map[string]interface{}{"result": result}})
			return out
		}
		// The whole window: one row per distinct row key, as VictoriaLogs groups.
		wholeSum := map[string]uint64{}
		perSeg := map[int]map[string]uint64{}
		for _, r := range rows {
			key := strings.Join(r.kv, "\x00")
			wholeSum[key] += r.bytes
			if perSeg[r.seg] == nil {
				perSeg[r.seg] = map[string]uint64{}
			}
			perSeg[r.seg][key] += r.bytes
		}
		wholePoints := map[string][2]interface{}{}
		for key, b := range wholeSum {
			wholePoints[key] = [2]interface{}{nil, []interface{}{float64(end.Unix()), strconv.FormatUint(b, 10)}}
		}
		want, _ := json.Marshal(p.volumeResponse(req, plan, body(wholePoints, false)))

		// The run: one stats_query_range answer whose buckets are the segments.
		runPoints := map[string][2]interface{}{}
		for seg := 0; seg < 4; seg++ {
			for key, b := range perSeg[seg] {
				pt := runPoints[key]
				values, _ := pt[1].([]interface{})
				values = append(values, []interface{}{float64((runStart + int64(seg)*segSize) / int64(time.Second)), strconv.FormatUint(b, 10)})
				runPoints[key] = [2]interface{}{nil, values}
			}
		}
		index := map[int64]int{}
		for seg := 0; seg < 4; seg++ {
			index[runStart+int64(seg)*segSize] = seg
		}
		parts, err := parseVolumeSegmentRows(body(runPoints, true), index)
		if err != nil {
			t.Fatalf("parse run: %v", err)
		}
		for seg := range parts {
			key := fmt.Sprintf("fuzz-volume-segment-%d", seg)
			if err := p.storeVolumeSegment(key, parts[seg], time.Minute); err != nil {
				t.Fatalf("store: %v", err)
			}
			loaded, ok := p.loadVolumeSegment(key)
			if !ok {
				t.Fatalf("segment %d did not load back", seg)
			}
			parts[seg] = loaded
		}
		got, _ := json.Marshal(p.mergeVolumeSegments(req, plan, parts))
		if string(got) != string(want) {
			t.Fatalf("segments differ from one window\n got: %s\nwant: %s", got, want)
		}
	})
}

// BenchmarkVolumeLandingRepeat measures Logs Drilldown's 24h landing volume
// repeated with the window moved by a second each time (a refresh), on the
// whole-window path and from segments: proxy time per request (including the
// fake VictoriaLogs), VictoriaLogs calls and the time range they read.
func BenchmarkVolumeLandingRepeat(b *testing.B) {
	for _, mode := range []struct {
		name      string
		inventory int
	}{{"whole_window", -1}, {"segments", 0}} {
		b.Run(mode.name, func(b *testing.B) {
			vl := newSegVL(b)
			now := time.Now()
			vl.add("", segFixture(rand.New(rand.NewSource(11)), now.Add(-25*time.Hour), now, 2000)...)
			p := newSegProxy(b, vl.URL, mode.inventory, DefaultMaxStatsCacheFreshness)
			end := now.Add(-time.Second)
			segVolume(b, p, "", segWindow(landingParams(), end.Add(-24*time.Hour), end)) // cold, not measured
			vl.take()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				end = end.Add(time.Second)
				segVolume(b, p, "", segWindow(landingParams(), end.Add(-24*time.Hour), end))
			}
			b.StopTimer()
			n, span := segCallsSummary(vl.take())
			b.ReportMetric(float64(n)/float64(b.N), "vl_calls/op")
			b.ReportMetric(span.Seconds()/float64(b.N), "vl_range_s/op")
		})
	}
}
