package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// jsonShape is a JSON value's shape: an object's keys in their written order
// with their shapes, or the type of any other value.
func jsonShape(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	raw = bytes.TrimSpace(raw)
	switch {
	case len(raw) == 0:
		t.Fatal("empty JSON value")
	case raw[0] == '{':
		dec := json.NewDecoder(bytes.NewReader(raw))
		_, _ = dec.Token()
		var out []any
		for dec.More() {
			tok, err := dec.Token()
			if err != nil {
				t.Fatal(err)
			}
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				t.Fatal(err)
			}
			out = append(out, tok.(string), jsonShape(t, v))
		}
		return out
	case raw[0] == 't' || raw[0] == 'f':
		return "bool"
	case raw[0] == '"':
		return "string"
	}
	return "number"
}

// lokiRecordedStats are Loki's data.stats objects as recorded from Loki
// 3.7.9 (testdata/loki_query_stats_object.json): a computed answer's and a
// results-cache hit's, whose component sections are in their zero form.
func lokiRecordedStats(t *testing.T) (computed, zero json.RawMessage) {
	t.Helper()
	raw, err := os.ReadFile("testdata/loki_query_stats_object.json")
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Computed json.RawMessage `json:"computed"`
		Hit      json.RawMessage `json:"results_cache_hit"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	return rec.Computed, rec.Hit
}

// statsOf returns an answer's data.stats.
func statsOf(t *testing.T, body []byte) json.RawMessage {
	t.Helper()
	var ans queryStatsAnswer
	if err := json.Unmarshal(body, &ans); err != nil {
		t.Fatalf("answer is not JSON: %v\n%s", err, body)
	}
	if len(ans.Data.Stats) == 0 {
		t.Fatalf("answer without data.stats: %s", body)
	}
	return ans.Data.Stats
}

// TestQueryStats_ObjectHasLokisRecordedShape: every route Loki writes a stats
// object into answers one with exactly the shape Loki 3.7.9 wrote (every
// section, key and key order, and each value's JSON type), on a miss and on
// a response-cache hit. The proxy has none of Loki's components, so its
// querier, ingester and index sections are always in Loki's zero form (the
// recorded results-cache hit); the recorded computed answer has the same
// keys plus the ingester's recvWaitTime, which Loki omits when zero.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_ObjectHasLokisRecordedShape(t *testing.T) {
	computed, zero := lokiRecordedStats(t)
	want := jsonShape(t, zero)
	if got := jsonShape(t, computed); !reflect.DeepEqual(dropRecvWaitTime(got), want) {
		t.Fatalf("the recorded answers differ in more than recvWaitTime:\n%v\n%v", got, want)
	}
	stub := &statsStubVL{rows: 5}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := newQueryStatsTestProxy(t, vl.URL, "")
	now := time.Now().Truncate(time.Minute)
	window := url.Values{"start": {fmt.Sprint(now.Add(-10 * time.Minute).Unix())}, "end": {fmt.Sprint(now.Unix())}, "step": {"60"}}
	with := func(kv ...string) url.Values {
		out := url.Values{}
		for k, v := range window {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			out.Set(kv[i], kv[i+1])
		}
		return out
	}
	for _, tc := range []struct {
		name, path string
		params     url.Values
	}{
		{"log", "/loki/api/v1/query_range", with("query", `{app="api"}`, "limit", "100")},
		{"metric", "/loki/api/v1/query_range", with("query", `sum(count_over_time({app="api"}[1m]))`)},
		{"instant", "/loki/api/v1/query", url.Values{"query": {`sum(count_over_time({app="api"}[5m]))`}, "time": {fmt.Sprint(now.Unix())}}},
		{"volume_range", "/loki/api/v1/index/volume_range", with("query", `{app="api"}`)},
		{"volume", "/loki/api/v1/index/volume", with("query", `{app="api"}`)},
	} {
		for _, attempt := range []string{"miss", "hit"} {
			t.Run(tc.name+"/"+attempt, func(t *testing.T) {
				got := jsonShape(t, statsOf(t, serveQueryStats(t, h, tc.path, tc.params, nil)))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("stats shape differs from Loki's recorded object\ngot  %v\nwant %v", got, want)
				}
			})
		}
	}
}

// dropRecvWaitTime removes the ingester's recvWaitTime from a stats shape.
func dropRecvWaitTime(shape any) any {
	sections := shape.([]any)
	out := make([]any, 0, len(sections))
	for i := 0; i < len(sections); i += 2 {
		v := sections[i+1]
		if sections[i] == "ingester" {
			var keep []any
			fields := v.([]any)
			for j := 0; j < len(fields); j += 2 {
				if fields[j] != "recvWaitTime" {
					keep = append(keep, fields[j], fields[j+1])
				}
			}
			v = keep
		}
		out = append(out, sections[i], v)
	}
	return out
}

type lokiSummary struct {
	BytesPerSecond int64   `json:"bytesProcessedPerSecond"`
	LinesPerSecond int64   `json:"linesProcessedPerSecond"`
	Bytes          int64   `json:"totalBytesProcessed"`
	Lines          int64   `json:"totalLinesProcessed"`
	ExecTime       float64 `json:"execTime"`
	Entries        int64   `json:"totalEntriesReturned"`
	Splits         int64   `json:"splits"`
	PostFilter     int64   `json:"totalPostFilterLines"`
}

type lokiStatsView struct {
	Summary lokiSummary `json:"summary"`
	Cache   map[string]struct {
		EntriesFound      int64 `json:"entriesFound"`
		QueryLengthServed int64 `json:"queryLengthServed"`
	} `json:"cache"`
}

func statsView(t *testing.T, body []byte) lokiStatsView {
	t.Helper()
	var v lokiStatsView
	if err := json.Unmarshal(statsOf(t, body), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// TestQueryStats_LokiAccountingPerRouteAndCacheTier is the table of what each
// route reports on a miss and on a response-cache hit, as Loki 3.7.9 does
// with its default results cache (observed on the test stack):
//
//   - a non-empty log query is computed on every request: the hit reports the
//     stored computation's lines and bytes with its own execTime;
//   - an empty log answer is served from Loki's results cache: the hit
//     reports no processing;
//   - an instant query is computed on every request (no instant results
//     cache by default): the hit replays the computation;
//   - a metric query_range hit is a results-cache hit: no processing, the
//     window served as cache.result.queryLengthServed;
//   - volume answers never report processed lines or bytes.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_LokiAccountingPerRouteAndCacheTier(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	start, end := now.Add(-10*time.Minute), now
	window := func(q string, extra ...string) url.Values {
		v := url.Values{"query": {q}, "step": {"60"}, "start": {fmt.Sprint(start.Unix())}, "end": {fmt.Sprint(end.Unix())}}
		for i := 0; i < len(extra); i += 2 {
			v.Set(extra[i], extra[i+1])
		}
		return v
	}
	type check func(t *testing.T, miss, hit lokiStatsView)
	for _, tc := range []struct {
		name, path string
		rows       int
		params     url.Values
		check      check
	}{
		{"log non-empty", "/loki/api/v1/query_range", 6, window(`{app="api"}`, "limit", "100"), func(t *testing.T, miss, hit lokiStatsView) {
			if miss.Summary.Lines != 6 || miss.Summary.Bytes <= 0 || miss.Summary.Entries != 6 {
				t.Fatalf("miss %+v: want the 6 rows read and their line bytes", miss.Summary)
			}
			if hit.Summary.Lines != miss.Summary.Lines || hit.Summary.Bytes != miss.Summary.Bytes || hit.Summary.PostFilter != miss.Summary.PostFilter {
				t.Fatalf("hit %+v: want the stored computation's totals %+v", hit.Summary, miss.Summary)
			}
			if hit.Summary.Splits != 0 || hit.Cache["result"].EntriesFound != 1 || hit.Summary.ExecTime == miss.Summary.ExecTime {
				t.Fatalf("hit %+v cache %+v: want no VictoriaLogs call, the entry found and the hit's own execTime", hit.Summary, hit.Cache["result"])
			}
		}},
		{"log empty", "/loki/api/v1/query_range", 0, window(`{app="api"}`, "limit", "100"), func(t *testing.T, miss, hit lokiStatsView) {
			if hit.Summary.Lines != 0 || hit.Summary.Bytes != 0 || hit.Summary.Entries != 0 || hit.Cache["result"].EntriesFound != 1 || hit.Cache["result"].QueryLengthServed != 0 {
				t.Fatalf("empty log hit %+v: want Loki's results-cache hit (no processing)", hit.Summary)
			}
		}},
		{"metric range", "/loki/api/v1/query_range", 6, window(`sum(count_over_time({app="api"}[1m]))`), func(t *testing.T, miss, hit lokiStatsView) {
			if miss.Summary.Lines <= 0 || miss.Summary.Lines != miss.Summary.PostFilter {
				t.Fatalf("miss %+v: want the window's lines from the answer", miss.Summary)
			}
			if miss.Summary.Bytes != 0 {
				t.Fatalf("miss %+v: a count answer gives no bytes; VictoriaLogs' aggregated rows are not Loki's bytes processed", miss.Summary)
			}
			if hit.Summary.Lines != 0 || hit.Summary.Bytes != 0 || hit.Summary.PostFilter != 0 {
				t.Fatalf("hit %+v: want Loki's results-cache hit (no processing)", hit.Summary)
			}
			if got, want := hit.Cache["result"].QueryLengthServed, end.Sub(start).Nanoseconds(); got != want || hit.Cache["result"].EntriesFound != 1 {
				t.Fatalf("hit cache.result %+v: want the window served, %d ns", hit.Cache["result"], want)
			}
		}},
		{"metric sliding", "/loki/api/v1/query_range", 6, window(`sum(count_over_time({app="api"}[5m]))`), func(t *testing.T, miss, hit lokiStatsView) {
			if miss.Summary.Lines != 0 || miss.Summary.Bytes != 0 {
				t.Fatalf("miss %+v: a sliding window counts a line in several steps; nothing derived, nothing read means 0", miss.Summary)
			}
		}},
		{"instant", "/loki/api/v1/query", 6, url.Values{"query": {`sum(count_over_time({app="api"}[5m]))`}, "time": {fmt.Sprint(end.Unix())}}, func(t *testing.T, miss, hit lokiStatsView) {
			if miss.Summary.Lines <= 0 {
				t.Fatalf("miss %+v: want the instant answer's lines", miss.Summary)
			}
			if hit.Summary.Lines != miss.Summary.Lines || hit.Summary.Bytes != miss.Summary.Bytes || hit.Cache["instantMetricResult"].EntriesFound != 1 {
				t.Fatalf("hit %+v: want the stored computation's totals %+v", hit.Summary, miss.Summary)
			}
		}},
		{"volume_range", "/loki/api/v1/index/volume_range", 6, window(`{app="api"}`), func(t *testing.T, miss, hit lokiStatsView) {
			for _, s := range []lokiSummary{miss.Summary, hit.Summary} {
				if s.Lines != 0 || s.Bytes != 0 || s.PostFilter != 0 || s.ExecTime <= 0 {
					t.Fatalf("volume %+v: Loki reports no processed lines or bytes for volume", s)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &statsStubVL{rows: tc.rows}
			vl := httptest.NewServer(stub)
			defer vl.Close()
			h := newQueryStatsTestProxy(t, vl.URL, "")
			miss := statsView(t, serveQueryStats(t, h, tc.path, tc.params, nil))
			before, _, _ := stub.snapshot()
			hit := statsView(t, serveQueryStats(t, h, tc.path, tc.params, nil))
			if after, _, _ := stub.snapshot(); after != before && tc.path != "/loki/api/v1/index/volume_range" {
				t.Fatalf("the repeat reached VictoriaLogs (%d -> %d)", before, after)
			}
			tc.check(t, miss, hit)
		})
	}
}

// TestQueryStatsTotals_PartlyCachedMetricAnswerGivesNoTotals: an answer whose
// steps partly came from a cache was not all processed by this request, so
// it reports no derived lines (Loki counts only the part it computed); the
// same answer computed whole does.
//
// conformance: query-stats-envelope
func TestQueryStatsTotals_PartlyCachedMetricAnswerGivesNoTotals(t *testing.T) {
	stub := &statsStubVL{rows: 1}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	p, err := New(Config{BackendURL: vl.URL, LogLevel: "error", Cache: cache.New(time.Minute, 100)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1800,"4"],[1860,"5"]]}]}}`)
	for _, reused := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{
			"query": {`sum(count_over_time({app="api"}[1m]))`}, "start": {"1740"}, "end": {"1860"}, "step": {"60"}}.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rt := newRequestTelemetry()
		rt.statsWriter = new(queryStatsWriter)
		rt.flow.vlCalls = 1
		if reused {
			rt.flow.lokiCache[lokiCacheSlots[lokiCacheStatsResult]].found = 1
		}
		req = req.WithContext(context.WithValue(req.Context(), requestTelemetryKey, rt))
		p.deriveQueryStatsTotals(req, "query_range", body)
		if got := rt.statsTotals.hasLines; got == reused {
			t.Fatalf("reused=%t: derived lines %t (%+v)", reused, got, rt.statsTotals)
		}
		if !reused && rt.statsTotals.lines != 9 {
			t.Fatalf("whole answer: %d lines, want 9", rt.statsTotals.lines)
		}
	}
}

// TestEntryCounter_LineBytes: the line bytes of a streams answer are the
// unescaped bytes of each entry's line, however the answer is cut into
// writes; timestamps, labels and structured metadata are not counted.
//
// conformance: query-stats-envelope
func TestEntryCounter_LineBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"plain", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"a":"b"},"values":[["1","hello"],["2","world!"]]}]}}`},
		{"escapes", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a \"q\" \\ b\n\té [\"x\"] \"metric\":{"]]}]}}`},
		{"categorized", `{"status":"success","data":{"encodingFlags":["categorize-labels"],"resultType":"streams","result":[{"stream":{},"values":[["1","abc",{"structuredMetadata":{"k":"vvvvvv"}}],["2","",{}]]}]}}`},
		{"sorted keys", `{"data":{"result":[{"stream":{"x":"y"},"values":[["1","line one"],["2","\\\\"]]}],"resultType":"streams"},"status":"success"}`},
		{"matrix", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"1"},"values":[[1,"2"]]}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := expectedLineBytes(t, []byte(tc.body))
			for _, size := range []int{1, 2, 3, 5, 7, 64, len(tc.body)} {
				var c entryCounter
				c.mode = entryModeOf([]byte(tc.body[:min(len(tc.body), queryStatsTailHold)]))
				for i := 0; i < len(tc.body); i += size {
					c.add([]byte(tc.body[i:min(i+size, len(tc.body))]))
				}
				if c.lineBytes != want {
					t.Fatalf("writes of %d: %d line bytes, want %d", size, c.lineBytes, want)
				}
			}
		})
	}
}

// expectedLineBytes decodes a streams answer and sums its lines' bytes
// (\uXXXX escapes count as one byte, as the counter counts them).
func expectedLineBytes(t testing.TB, body []byte) int64 {
	t.Helper()
	var ans struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &ans); err != nil {
		t.Fatal(err)
	}
	if ans.Data.ResultType != "streams" {
		return 0
	}
	var n int64
	for _, s := range ans.Data.Result {
		for _, v := range s.Values {
			var line string
			if err := json.Unmarshal(v[1], &line); err != nil {
				t.Fatal(err)
			}
			n += int64(len(line)) - unicodeEscapeExtra(string(v[1]))
		}
	}
	return n
}

// unicodeEscapeExtra is how many more bytes than one the \uXXXX escapes of an
// encoded string decode to.
func unicodeEscapeExtra(encoded string) int64 {
	var extra int64
	for i := 0; i+5 < len(encoded); i++ {
		if encoded[i] != '\\' {
			continue
		}
		if encoded[i+1] != 'u' {
			i++
			continue
		}
		var r rune
		_, _ = fmt.Sscanf(encoded[i+2:i+6], "%04x", &r)
		extra += int64(len(string(r))) - 1
		i += 5
	}
	return extra
}

// FuzzEntryCounterLineBytes: the line bytes do not depend on how the answer
// is cut into writes, and on a valid streams answer they are its lines'.
func FuzzEntryCounterLineBytes(f *testing.F) {
	f.Add([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a\"b\\c"],["2","x"]]}]}}`), uint8(3))
	f.Add([]byte(`{"data":{"result":[{"stream":{},"values":[["1","é"]]}],"resultType":"streams"},"status":"success"}`), uint8(1))
	f.Fuzz(func(t *testing.T, body []byte, chunk uint8) {
		if len(body) > 1<<16 {
			return
		}
		mode := entryModeOf(body[:min(len(body), queryStatsTailHold)])
		whole := entryCounter{mode: mode}
		whole.add(body)
		size := int(chunk)%31 + 1
		split := entryCounter{mode: mode}
		for i := 0; i < len(body); i += size {
			split.add(body[i:min(i+size, len(body))])
		}
		if whole.lineBytes != split.lineBytes || whole.n != split.n {
			t.Fatalf("whole %d/%d, split by %d: %d/%d (%q)", whole.n, whole.lineBytes, size, split.n, split.lineBytes, body)
		}
		if json.Valid(body) && strings.HasPrefix(string(body), `{"status":"success","data":{"resultType":"streams","result":[`) {
			var ans struct {
				Data struct {
					Result []struct {
						Values [][]json.RawMessage `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if json.Unmarshal(body, &ans) != nil {
				return
			}
			var entries int64
			for _, s := range ans.Data.Result {
				for _, v := range s.Values {
					if len(v) < 2 || len(v[0]) == 0 || v[0][0] != '"' || len(v[1]) == 0 || v[1][0] != '"' {
						return // not a Loki entry; nothing to compare
					}
					entries++
				}
			}
			if entries != whole.n {
				return // entry-shaped arrays outside "values": the decoder's view differs
			}
			if want := expectedLineBytes(t, body); whole.lineBytes != want {
				t.Fatalf("line bytes %d, want %d (%q)", whole.lineBytes, want, body)
			}
		}
	})
}
