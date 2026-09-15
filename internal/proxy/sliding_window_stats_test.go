package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

type emulatedRow struct {
	ts  int64
	pod string
	msg string
}

var vlStatsPipeRE = regexp.MustCompile(`\| stats by \(_time:(\d+)ns(?: offset (\d+)ns)?((?:, [^)]*)?)\) (.*)$`)

// emulateVLStatsPipe answers a /select/logsql/query stats pipe grouped by
// `_time:Nns offset Mns` like VictoriaLogs v1.50: rows are filtered by the
// half-open [start, end) range, land in bucket truncate(ts+offset, N)-offset
// and one JSON line is written per non-empty (bucket, group) with the bucket
// start as RFC3339Nano. Label filters in the query are ignored. It reports the
// number of buckets the range spans and false when query has no such pipe.
func emulateVLStatsPipe(query string, start, end int64, rowsFor func(yield func(ts int64, labels map[string]string, msg string))) ([]byte, int, bool) {
	m := vlStatsPipeRE.FindStringSubmatch(query)
	if m == nil {
		return nil, 0, false
	}
	size, _ := strconv.ParseInt(m[1], 10, 64)
	offset, _ := strconv.ParseInt(m[2], 10, 64)
	var groups []string
	for _, g := range strings.Split(strings.TrimPrefix(m[3], ", "), ", ") {
		if g != "" {
			groups = append(groups, g)
		}
	}
	type agg struct{ fn, name string }
	var aggs []agg
	for _, part := range strings.Split(m[4], ", ") {
		fn, name, _ := strings.Cut(part, " as ")
		aggs = append(aggs, agg{fn, name})
	}
	type key struct {
		bucket int64
		group  string
	}
	totals := map[key][]float64{}
	groupLabels := map[string]map[string]string{}
	rowsFor(func(ts int64, labels map[string]string, msg string) {
		if ts < start || ts >= end {
			return
		}
		shifted := ts + offset
		rem := shifted % size
		if rem < 0 {
			rem += size
		}
		values := map[string]string{}
		var id strings.Builder
		for _, g := range groups {
			values[g] = labels[g]
			id.WriteString(g + "=" + labels[g] + ";")
		}
		k := key{bucket: shifted - rem - offset, group: id.String()}
		groupLabels[k.group] = values
		if totals[k] == nil {
			totals[k] = make([]float64, len(aggs))
		}
		for i, a := range aggs {
			switch a.fn {
			case "count()":
				totals[k][i]++
			case "sum_len(_msg)":
				totals[k][i] += float64(len(msg))
			}
		}
	})
	keys := make([]key, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket != keys[j].bucket {
			return keys[i].bucket < keys[j].bucket
		}
		return keys[i].group < keys[j].group
	})
	var body []byte
	for _, k := range keys {
		row := map[string]string{"_time": time.Unix(0, k.bucket).UTC().Format(time.RFC3339Nano)}
		for g, v := range groupLabels[k.group] {
			row[g] = v
		}
		for i, a := range aggs {
			row[a.name] = strconv.FormatFloat(totals[k][i], 'f', -1, 64)
		}
		line, _ := json.Marshal(row)
		body = append(append(body, line...), '\n')
	}
	return body, int((end - start + size - 1) / size), true
}

// vlStatsRangeEmulator serves stats pipes over synthetic rows and counts raw
// row fetches and the widest bucket grid requested.
type vlStatsRangeEmulator struct {
	t    *testing.T
	rows []emulatedRow

	mu        sync.Mutex
	calls     []url.Values
	rawCalls  int
	maxPoints int
}

func (e *vlStatsRangeEmulator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	if r.URL.Path != "/select/logsql/query" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	start, err1 := strconv.ParseInt(formatVLTimestamp(r.Form.Get("start")), 10, 64)
	end, err2 := strconv.ParseInt(formatVLTimestamp(r.Form.Get("end")), 10, 64)
	if err1 != nil || err2 != nil {
		e.t.Errorf("emulator: bad range %q..%q", r.Form.Get("start"), r.Form.Get("end"))
		return
	}
	body, buckets, ok := emulateVLStatsPipe(r.Form.Get("query"), start, end, func(yield func(int64, map[string]string, string)) {
		for _, row := range e.rows {
			yield(row.ts, map[string]string{"pod": row.pod}, row.msg)
		}
	})
	e.mu.Lock()
	if ok {
		e.calls = append(e.calls, r.Form)
		if buckets > e.maxPoints {
			e.maxPoints = buckets
		}
	} else {
		e.rawCalls++
	}
	e.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = w.Write(body)
}

// syntheticRows spreads rows over [from, to] with deterministic jitter and puts
// extra rows exactly on evaluation times and window edges. pod-b and pod-c
// repeat every pod-a row two and four times, so per-window counts never tie.
func syntheticRows(from, to time.Time, start time.Time, step, window time.Duration) []emulatedRow {
	var rows []emulatedRow
	add := func(ts int64, n int) {
		for pod, copies := range map[string]int{"pod-a": 1, "pod-b": 2, "pod-c": 4} {
			for i := 0; i < copies*n; i++ {
				rows = append(rows, emulatedRow{ts: ts, pod: pod, msg: strings.Repeat("x", (int(ts%7)+i)%5)})
			}
		}
	}
	seed := uint64(42)
	for ts := from.UnixNano(); ts <= to.UnixNano(); {
		seed = seed*6364136223846793005 + 1442695040888963407
		add(ts, 1)
		ts += int64(seed>>33)%int64(97*time.Second) + int64(time.Millisecond)
	}
	for k := 0; start.Add(time.Duration(k) * step).Before(to); k += 7 {
		t := start.Add(time.Duration(k) * step)
		add(t.UnixNano(), 1)
		add(t.Add(-window).UnixNano(), 1)
	}
	return rows
}

// expectedWindowMatrix evaluates Loki's (T-range, T] windows directly.
func expectedWindowMatrix(rows []emulatedRow, fn string, byPod bool, start, end time.Time, step, window time.Duration) map[string][][2]string {
	out := map[string][][2]string{}
	sorted := append([]emulatedRow(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ts < sorted[j].ts })
	for t := start; !t.After(end); t = t.Add(step) {
		counts, bytes := map[string]float64{}, map[string]float64{}
		lo := sort.Search(len(sorted), func(i int) bool { return sorted[i].ts > t.Add(-window).UnixNano() })
		hi := sort.Search(len(sorted), func(i int) bool { return sorted[i].ts > t.UnixNano() })
		for _, row := range sorted[lo:hi] {
			id := "{}"
			if byPod {
				id = `{pod="` + row.pod + `"}`
			}
			counts[id]++
			bytes[id] += float64(len(row.msg))
		}
		for id, c := range counts {
			value := c
			switch fn {
			case "rate":
				value = c / window.Seconds()
			case "bytes_over_time":
				value = bytes[id]
			case "bytes_rate":
				value = bytes[id] / window.Seconds()
			}
			out[id] = append(out[id], [2]string{strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', -1, 64), strconv.FormatFloat(value, 'f', -1, 64)})
		}
	}
	return out
}

func decodeMatrix(t *testing.T, body []byte) map[string][][2]string {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" {
		t.Fatalf("bad matrix response: %v %s", err, body)
	}
	out := map[string][][2]string{}
	for _, series := range resp.Data.Result {
		id := "{}"
		if pod, ok := series.Metric["pod"]; ok {
			id = `{pod="` + pod + `"}`
		}
		for _, v := range series.Values {
			out[id] = append(out[id], [2]string{strconv.FormatFloat(v[0].(float64), 'f', -1, 64), v[1].(string)})
		}
	}
	return out
}

func TestSlidingWindowStats_LongRangeIsExactWithStepSizedBuckets(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		query  string
		fn     string
		byPod  bool
		rng    time.Duration
		step   time.Duration
		window time.Duration
	}{
		{"24h overlapping count", `sum by (pod) (count_over_time({app="api"}[5m]))`, "count_over_time", true, 24 * time.Hour, 86400 * time.Millisecond, 5 * time.Minute},
		{"7d overlapping rate", `sum by (pod) (rate({app="api"}[15m]))`, "rate", true, 7 * 24 * time.Hour, 604800 * time.Millisecond, 15 * time.Minute},
		{"7d bytes rate all", `sum(bytes_rate({app="api"}[20m]))`, "bytes_rate", false, 7 * 24 * time.Hour, 604800 * time.Millisecond, 20 * time.Minute},
		{"24h bytes over time by pod", `sum by (pod) (bytes_over_time({app="api"}[10m]))`, "bytes_over_time", true, 24 * time.Hour, 86400 * time.Millisecond, 10 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := end.Add(-tc.rng)
			emu := &vlStatsRangeEmulator{t: t, rows: syntheticRows(start.Add(-2*tc.window), end.Add(tc.step), start, tc.step, tc.window)}
			srv := httptest.NewServer(emu)
			defer srv.Close()
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			params := url.Values{
				"query": {tc.query},
				"start": {strconv.FormatInt(start.UnixNano(), 10)},
				"end":   {strconv.FormatInt(end.UnixNano(), 10)},
				"step":  {strconv.FormatFloat(tc.step.Seconds(), 'f', -1, 64)},
			}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			got := decodeMatrix(t, rec.Body.Bytes())
			want := expectedWindowMatrix(emu.rows, tc.fn, tc.byPod, start, end, tc.step, tc.window)
			if len(got) != len(want) {
				t.Fatalf("series = %d, want %d", len(got), len(want))
			}
			for id, points := range want {
				if fmt.Sprint(got[id]) != fmt.Sprint(points) {
					t.Fatalf("series %s differs:\n got %v\nwant %v", id, head(got[id]), head(points))
				}
			}
			steps := int(tc.rng / tc.step)
			if emu.rawCalls != 0 {
				t.Fatalf("raw row fetches = %d, want 0", emu.rawCalls)
			}
			if emu.maxPoints > steps+2 {
				t.Fatalf("largest stats bucket grid = %d buckets, want at most %d (one per step)", emu.maxPoints, steps+2)
			}
			if len(emu.calls) != 3 {
				t.Fatalf("stats calls = %d, want 3", len(emu.calls))
			}
		})
	}
}

func TestSlidingWindowStats_TopKRanksExactWindowsWithoutRawFetch(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	for _, rng := range []time.Duration{24 * time.Hour, 7 * 24 * time.Hour} {
		t.Run(rng.String(), func(t *testing.T) {
			step := rng / 1000
			window := 5 * time.Minute
			start := end.Add(-rng)
			emu := &vlStatsRangeEmulator{t: t, rows: syntheticRows(start.Add(-2*window), end.Add(step), start, step, window)}
			srv := httptest.NewServer(emu)
			defer srv.Close()
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			params := url.Values{
				"query": {`topk(2, sum by (pod) (count_over_time({app="api"}[5m])))`},
				"start": {strconv.FormatInt(start.UnixNano(), 10)},
				"end":   {strconv.FormatInt(end.UnixNano(), 10)},
				"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
			}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			got := decodeMatrix(t, rec.Body.Bytes())
			all := expectedWindowMatrix(emu.rows, "count_over_time", true, start, end, step, window)
			// pod-c always holds 4x and pod-b 2x the pod-a count: the top 2 at
			// every step are pod-c and pod-b.
			want := map[string][][2]string{`{pod="pod-b"}`: all[`{pod="pod-b"}`], `{pod="pod-c"}`: all[`{pod="pod-c"}`]}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("topk differs:\n got %d series\nwant %d series", len(got), len(want))
			}
			if emu.rawCalls != 0 {
				t.Fatalf("raw row fetches = %d, want 0", emu.rawCalls)
			}
			if steps := int(rng / step); emu.maxPoints > steps+2 {
				t.Fatalf("largest stats bucket grid = %d buckets, want at most %d", emu.maxPoints, steps+2)
			}
		})
	}
}

func head(points [][2]string) [][2]string {
	if len(points) > 6 {
		return points[:6]
	}
	return points
}

// topk ranks every series per step: a series whose total is too small to
// survive -max-stats-query-series still wins the step where it spikes.
func TestSlidingWindowStats_TopKRanksBeyondSeriesCap(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	rng := 24 * time.Hour
	step, window := rng/1000, 5*time.Minute
	start := end.Add(-rng)
	spike := start.Add(500 * step)
	var rows []emulatedRow
	for k := 0; k <= 1000; k++ {
		ts := start.Add(time.Duration(k)*step - time.Second).UnixNano()
		for i := 0; i < 10; i++ {
			rows = append(rows, emulatedRow{ts: ts, pod: "pod-c"})
		}
		for i := 0; i < 9; i++ {
			rows = append(rows, emulatedRow{ts: ts, pod: "pod-b"})
		}
	}
	for i := 0; i < 100; i++ {
		rows = append(rows, emulatedRow{ts: spike.UnixNano(), pod: "pod-a"})
	}
	emu := &vlStatsRangeEmulator{t: t, rows: rows}
	srv := httptest.NewServer(emu)
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error", MaxStatsQuerySeries: 2})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	params := url.Values{
		"query": {`topk(1, sum by (pod) (count_over_time({app="api"}[5m])))`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := decodeMatrix(t, rec.Body.Bytes())
	all := expectedWindowMatrix(rows, "count_over_time", true, start, end, step, window)
	best := map[string][2]string{} // timestamp -> {series, value}
	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		for _, point := range all[id] {
			v, _ := strconv.ParseFloat(point[1], 64)
			cur, ok := best[point[0]]
			curV, _ := strconv.ParseFloat(cur[1], 64)
			if !ok || v > curV {
				best[point[0]] = [2]string{id, point[1]}
			}
		}
	}
	want := map[string][][2]string{}
	for _, id := range ids {
		for _, point := range all[id] {
			if best[point[0]][0] == id {
				want[id] = append(want[id], point)
			}
		}
	}
	if len(want[`{pod="pod-a"}`]) == 0 {
		t.Fatal("fixture error: pod-a never wins a step")
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("topk(1) differs:\n got pod-a=%v pod-c points=%d\nwant pod-a=%v pod-c points=%d", got[`{pod="pod-a"}`], len(got[`{pod="pod-c"}`]), want[`{pod="pod-a"}`], len(want[`{pod="pod-c"}`]))
	}
	_ = spike
}

// A high-cardinality grouping stops at -manual-range-metric-row-limit stats
// rows with an error naming the flag, before the proxy keeps more rows.
func TestSlidingWindowStats_RowLimitNamesFlag(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	rng := 24 * time.Hour
	step := rng / 1000
	start := end.Add(-rng)
	var rows []emulatedRow
	for i := 0; i < 50; i++ {
		rows = append(rows, emulatedRow{ts: start.Add(time.Duration(i) * time.Hour / 3).UnixNano(), pod: fmt.Sprintf("pod-%d", i)})
	}
	emu := &vlStatsRangeEmulator{t: t, rows: rows}
	srv := httptest.NewServer(emu)
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error", RangeMetricRowLimit: 20})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	params := url.Values{
		"query": {`topk(3, sum by (pod) (count_over_time({app="api"}[5m])))`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code < 400 || !strings.Contains(rec.Body.String(), "manual range metric row limit exceeded (20)") || !strings.Contains(rec.Body.String(), "-manual-range-metric-row-limit") {
		t.Fatalf("status %d body %s, want the row limit error naming its flag", rec.Code, rec.Body.String())
	}
}
