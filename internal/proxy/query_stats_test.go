package proxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// lokiStatsSummaryKeys is the key order of Loki's data.stats.summary
// (observed on Loki 3.7.7 answers).
var lokiStatsSummaryKeys = []string{
	"bytesProcessedPerSecond", "linesProcessedPerSecond", "totalBytesProcessed", "totalLinesProcessed",
	"execTime", "queueTime", "subqueries", "totalEntriesReturned", "splits", "shards",
	"totalPostFilterLines", "totalStructuredMetadataBytesProcessed",
}

var lokiStatsSections = []string{"summary", "querier", "ingester", "cache", "index"}

var lokiStatsCacheKeys = []string{
	"chunk", "index", "result", "statsResult", "volumeResult", "seriesResult", "labelResult", "instantMetricResult",
}

var lokiStatsCacheEntryKeys = []string{
	"entriesFound", "entriesRequested", "entriesStored", "bytesReceived", "bytesSent", "requests",
	"downloadTime", "queryLengthServed",
}

// objectKeys returns the keys of the JSON object raw in their written order.
func objectKeys(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("not an object: %s", raw)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

type queryStatsAnswer struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
		Stats      json.RawMessage `json:"stats"`
	} `json:"data"`
}

type parsedQueryStats struct {
	Summary struct {
		BytesProcessedPerSecond int64   `json:"bytesProcessedPerSecond"`
		LinesProcessedPerSecond int64   `json:"linesProcessedPerSecond"`
		TotalBytesProcessed     int64   `json:"totalBytesProcessed"`
		TotalLinesProcessed     int64   `json:"totalLinesProcessed"`
		ExecTime                float64 `json:"execTime"`
		QueueTime               float64 `json:"queueTime"`
		TotalEntriesReturned    int64   `json:"totalEntriesReturned"`
		Splits                  int64   `json:"splits"`
	} `json:"summary"`
	Cache map[string]struct {
		EntriesFound     int64 `json:"entriesFound"`
		EntriesRequested int64 `json:"entriesRequested"`
		EntriesStored    int64 `json:"entriesStored"`
		BytesReceived    int64 `json:"bytesReceived"`
		BytesSent        int64 `json:"bytesSent"`
		Requests         int64 `json:"requests"`
		DownloadTime     int64 `json:"downloadTime"`
	} `json:"cache"`
}

// assertLokiStatsShape checks data.stats has Loki's summary and cache
// sections with Loki's keys in Loki's order, and returns it parsed.
func assertLokiStatsShape(t *testing.T, body []byte) (queryStatsAnswer, parsedQueryStats) {
	t.Helper()
	var ans queryStatsAnswer
	if err := json.Unmarshal(body, &ans); err != nil {
		t.Fatalf("answer is not JSON: %v\n%s", err, body)
	}
	if ans.Status != "success" {
		t.Fatalf("status %q: %s", ans.Status, body)
	}
	if got := objectKeys(t, ans.Data.Stats); !reflect.DeepEqual(got, lokiStatsSections) {
		t.Fatalf("stats sections %v, want %v: %s", got, lokiStatsSections, ans.Data.Stats)
	}
	var sections map[string]json.RawMessage
	_ = json.Unmarshal(ans.Data.Stats, &sections)
	if got := objectKeys(t, sections["summary"]); !reflect.DeepEqual(got, lokiStatsSummaryKeys) {
		t.Fatalf("summary keys %v\nwant %v", got, lokiStatsSummaryKeys)
	}
	if got := objectKeys(t, sections["cache"]); !reflect.DeepEqual(got, lokiStatsCacheKeys) {
		t.Fatalf("cache keys %v\nwant %v", got, lokiStatsCacheKeys)
	}
	var cacheSections map[string]json.RawMessage
	_ = json.Unmarshal(sections["cache"], &cacheSections)
	for _, name := range lokiStatsCacheKeys {
		if got := objectKeys(t, cacheSections[name]); !reflect.DeepEqual(got, lokiStatsCacheEntryKeys) {
			t.Fatalf("cache.%s keys %v", name, got)
		}
	}
	var st parsedQueryStats
	if err := json.Unmarshal(ans.Data.Stats, &st); err != nil {
		t.Fatal(err)
	}
	if st.Summary.ExecTime <= 0 {
		t.Fatalf("execTime %v, want the request's measured time", st.Summary.ExecTime)
	}
	return ans, st
}

// statsStubVL is a VictoriaLogs stub that serves `rows` log rows to raw
// queries and records what it sent: requests, bytes and rows.
type statsStubVL struct {
	mu                 sync.Mutex
	rows               int
	requests, sentRows int
	sentBytes          int64
}

func (s *statsStubVL) snapshot() (requests int, bytes int64, rows int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.sentBytes, s.sentRows
}

func (s *statsStubVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	rows := 0
	switch r.URL.Path {
	case "/select/logsql/query":
		var b strings.Builder
		base := time.Now().Add(-time.Minute)
		for i := 0; i < s.rows; i++ {
			fmt.Fprintf(&b, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":"line %d [\"q\"] \"metric\":{x}","app":"api","level":"info"}`+"\n",
				base.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
		}
		body, rows = []byte(b.String()), s.rows
	case "/select/logsql/stats_query_range":
		_ = r.ParseForm()
		start, _ := parseLokiTimeToUnixNano(r.Form.Get("start"))
		body = []byte(fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[%d,"7"]]}]}}`, start/int64(time.Second)))
	case "/select/logsql/stats_query":
		body = []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api"},"value":[1,"7"]}]}}`)
	case "/select/logsql/hits":
		body = []byte(`{"hits":[]}`)
	default:
		body = []byte(`{"values":[]}`)
	}
	s.mu.Lock()
	s.requests++
	s.sentBytes += int64(len(body))
	s.sentRows += rows
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func newQueryStatsTestProxy(t *testing.T, backendURL, compression string) http.Handler {
	t.Helper()
	p, err := New(Config{
		BackendURL:                        backendURL,
		Cache:                             cache.New(60*time.Second, 1000),
		CompatCache:                       cache.New(60*time.Second, 100),
		LogLevel:                          "error",
		ClientResponseCompression:         compression,
		ClientResponseCompressionMinBytes: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	if compression == "" {
		return mux
	}
	return mw.CompressionHandlerWithOptions(mux, mw.CompressionOptions{Mode: compression, MinBytes: 64})
}

func serveQueryStats(t *testing.T, h http.Handler, path string, params url.Values, headers map[string]string) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", path, rec.Code, rec.Body)
	}
	body := rec.Body.Bytes()
	if rec.Header().Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body, err = io.ReadAll(zr); err != nil {
			t.Fatal(err)
		}
	}
	return body
}

// answerLineBytes sums the bytes of the lines of a streams result.
func answerLineBytes(t *testing.T, result json.RawMessage) int64 {
	t.Helper()
	var streams []struct {
		Values [][]json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(result, &streams); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, s := range streams {
		for _, v := range s.Values {
			var line string
			if err := json.Unmarshal(v[1], &line); err != nil {
				t.Fatal(err)
			}
			n += int64(len(line))
		}
	}
	return n
}

func countStreamEntries(t *testing.T, result json.RawMessage) int64 {
	t.Helper()
	var streams []struct {
		Values [][]json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(result, &streams); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, s := range streams {
		n += int64(len(s.Values))
	}
	return n
}

// TestQueryStats_LogQueryNumbersAreMeasured is the differential test: for a
// random number of rows the stub serves, the answer's stats equal what the
// stub measured on its side (requests, bytes, rows) and the entries the
// answer holds. A constant would fail it.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_LogQueryNumbersAreMeasured(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, rows := range []int{1, 3 + rng.Intn(40), 50 + rng.Intn(200)} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			stub := &statsStubVL{rows: rows}
			vl := httptest.NewServer(stub)
			defer vl.Close()
			h := newQueryStatsTestProxy(t, vl.URL, "")
			now := time.Now()
			params := url.Values{
				"query": {`{app="api"}`}, "limit": {"1000"}, "direction": {"backward"},
				"start": {fmt.Sprint(now.Add(-5 * time.Minute).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())},
			}
			body := serveQueryStats(t, h, "/loki/api/v1/query_range", params, nil)
			ans, st := assertLokiStatsShape(t, body)
			requests, sentBytes, sentRows := stub.snapshot()
			if st.Summary.Splits != int64(requests) {
				t.Errorf("splits %d, VictoriaLogs received %d requests", st.Summary.Splits, requests)
			}
			// Loki's totalBytesProcessed counts line bytes: the lines the
			// answer returns (unescaped), not VictoriaLogs' NDJSON bytes.
			if lineBytes := answerLineBytes(t, ans.Data.Result); st.Summary.TotalBytesProcessed != lineBytes || lineBytes >= sentBytes {
				t.Errorf("totalBytesProcessed %d, the answer's lines hold %d bytes (VictoriaLogs sent %d)", st.Summary.TotalBytesProcessed, lineBytes, sentBytes)
			}
			if st.Summary.TotalLinesProcessed != int64(sentRows) {
				t.Errorf("totalLinesProcessed %d, VictoriaLogs sent %d rows", st.Summary.TotalLinesProcessed, sentRows)
			}
			if got := countStreamEntries(t, ans.Data.Result); st.Summary.TotalEntriesReturned != got || got != int64(rows) {
				t.Errorf("totalEntriesReturned %d, answer holds %d entries (stub rows %d)", st.Summary.TotalEntriesReturned, got, rows)
			}
			if st.Summary.BytesProcessedPerSecond <= 0 || st.Summary.LinesProcessedPerSecond <= 0 {
				t.Errorf("per-second rates not derived: %+v", st.Summary)
			}
			miss := st.Cache["result"]
			if miss.EntriesRequested == 0 || miss.EntriesFound != 0 || miss.EntriesStored == 0 || miss.BytesSent == 0 {
				t.Errorf("first request: cache.result %+v, want a lookup that missed and a store", miss)
			}

			// The same request again is answered from the response cache, with
			// no VictoriaLogs work. Loki computes a non-empty log query on every
			// request, so the hit reports the stored computation's totals, its
			// own execTime and the entry found.
			again := serveQueryStats(t, h, "/loki/api/v1/query_range", params, nil)
			_, hit := assertLokiStatsShape(t, again)
			if requests2, _, _ := stub.snapshot(); requests2 != requests {
				t.Fatalf("repeat reached VictoriaLogs (%d -> %d requests)", requests, requests2)
			}
			if hit.Summary.TotalBytesProcessed != st.Summary.TotalBytesProcessed || hit.Summary.TotalLinesProcessed != st.Summary.TotalLinesProcessed || hit.Summary.Splits != 0 {
				t.Errorf("cache hit %+v, want the stored computation's totals of %+v and no splits", hit.Summary, st.Summary)
			}
			if hit.Summary.BytesProcessedPerSecond <= 0 || hit.Summary.BytesProcessedPerSecond > 2*st.Summary.BytesProcessedPerSecond+1 {
				t.Errorf("cache hit rate %d, want the stored computation's (%d), not totals over the hit's time", hit.Summary.BytesProcessedPerSecond, st.Summary.BytesProcessedPerSecond)
			}
			if hit.Summary.TotalEntriesReturned != int64(rows) {
				t.Errorf("cache hit totalEntriesReturned %d, want %d", hit.Summary.TotalEntriesReturned, rows)
			}
			found := hit.Cache["result"]
			if found.EntriesFound != 1 || found.BytesReceived <= 0 || found.EntriesStored != 0 {
				t.Errorf("cache hit: cache.result %+v, want one entry found", found)
			}
			if hit.Summary.ExecTime == st.Summary.ExecTime {
				t.Errorf("cache hit replays the first request's execTime %v", hit.Summary.ExecTime)
			}
		})
	}
}

// TestQueryStats_EveryAnswerShape covers the metric, instant and empty paths:
// each answer carries Loki's stats object whatever writer produced it.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_EveryAnswerShape(t *testing.T) {
	stub := &statsStubVL{rows: 5}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := newQueryStatsTestProxy(t, vl.URL, "")
	now := time.Now().Truncate(time.Minute)
	rangeParams := func(q string) url.Values {
		return url.Values{"query": {q}, "step": {"60"},
			"start": {fmt.Sprint(now.Add(-10 * time.Minute).Unix())}, "end": {fmt.Sprint(now.Unix())}}
	}
	for _, tc := range []struct {
		name, path string
		params     url.Values
		resultType string
	}{
		{"log", "/loki/api/v1/query_range", rangeParams(`{app="api"} |= "line"`), "streams"},
		{"log-json", "/loki/api/v1/query_range", rangeParams(`{app="api"} | json`), "streams"},
		{"metric", "/loki/api/v1/query_range", rangeParams(`sum(count_over_time({app="api"}[1m]))`), "matrix"},
		{"metric-by", "/loki/api/v1/query_range", rangeParams(`sum by (app) (rate({app="api"}[1m]))`), "matrix"},
		{"binary", "/loki/api/v1/query_range", rangeParams(`sum(rate({app="api"}[1m])) / 2`), "matrix"},
		{"instant", "/loki/api/v1/query", url.Values{"query": {`sum(count_over_time({app="api"}[5m]))`}, "time": {fmt.Sprint(now.Unix())}}, "vector"},
		{"literal", "/loki/api/v1/query", url.Values{"query": {`vector(1)`}, "time": {fmt.Sprint(now.Unix())}}, "vector"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := serveQueryStats(t, h, tc.path, tc.params, nil)
			ans, _ := assertLokiStatsShape(t, body)
			if ans.Data.ResultType != tc.resultType {
				t.Fatalf("resultType %q, want %q", ans.Data.ResultType, tc.resultType)
			}
		})
	}
}

// TestQueryStats_CategorizedAndMultiTenantAnswers: the Logs Drilldown request
// (categorize-labels, encodingFlags before result) and the multi-tenant merge
// (sorted keys) carry exactly one stats object whose entry count is the
// answer's.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_CategorizedAndMultiTenantAnswers(t *testing.T) {
	stub := &statsStubVL{rows: 7}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	_, _, mux := newTwoTenantProxy(t, vl.URL)
	now := time.Now()
	logParams := url.Values{"query": {`{app="api"}`}, "limit": {"1000"}, "direction": {"backward"},
		"start": {fmt.Sprint(now.Add(-5 * time.Minute).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}}
	metricParams := url.Values{"query": {`sum(count_over_time({app="api"}[1m]))`}, "step": {"60"},
		"start": {fmt.Sprint(now.Truncate(time.Minute).Add(-5 * time.Minute).Unix())}, "end": {fmt.Sprint(now.Truncate(time.Minute).Unix())}}
	for _, tc := range []struct {
		name    string
		params  url.Values
		headers map[string]string
		entries int64 // -1: the series count of the answer
	}{
		{"categorized", logParams, map[string]string{"X-Scope-OrgID": "tenant-a", "X-Query-Tags": "Source=grafana-lokiexplore-app",
			"X-Loki-Response-Encoding-Flags": "categorize-labels"}, 7},
		{"multi-tenant log", logParams, map[string]string{"X-Scope-OrgID": "tenant-a|tenant-b"}, 14},
		{"multi-tenant metric", metricParams, map[string]string{"X-Scope-OrgID": "tenant-a|tenant-b"}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+tc.params.Encode(), nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			body := rec.Body.Bytes()
			ans, st := assertLokiStatsShape(t, body)
			var env map[string]json.RawMessage
			_ = json.Unmarshal(body, &env)
			keys, _ := jsonKeysOf(env["data"])
			if n := strings.Count(strings.Join(keys, ","), "stats"); n != 1 {
				t.Fatalf("data keys %v", keys)
			}
			want := tc.entries
			if want < 0 {
				var series []json.RawMessage
				_ = json.Unmarshal(ans.Data.Result, &series)
				want = int64(len(series))
			}
			if st.Summary.TotalEntriesReturned != want {
				t.Fatalf("totalEntriesReturned %d, want %d", st.Summary.TotalEntriesReturned, want)
			}
		})
	}
}

// TestQueryStats_CompressedCacheHitCarriesStats: a gzip client (Grafana's
// backend) hitting the response cache gets this request's stats, not a stored
// encoded body without them.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_CompressedCacheHitCarriesStats(t *testing.T) {
	stub := &statsStubVL{rows: 40}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := newQueryStatsTestProxy(t, vl.URL, "gzip")
	now := time.Now()
	params := url.Values{"query": {`{app="api"}`}, "limit": {"100"},
		"start": {fmt.Sprint(now.Add(-5 * time.Minute).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}}
	gz := map[string]string{"Accept-Encoding": "gzip"}
	_, first := assertLokiStatsShape(t, serveQueryStats(t, h, "/loki/api/v1/query_range", params, gz))
	_, hit := assertLokiStatsShape(t, serveQueryStats(t, h, "/loki/api/v1/query_range", params, gz))
	if first.Summary.TotalLinesProcessed != 40 || hit.Summary.TotalLinesProcessed != 40 || hit.Cache["result"].EntriesFound != 1 {
		t.Fatalf("miss lines %d, hit lines %d found %d", first.Summary.TotalLinesProcessed,
			hit.Summary.TotalLinesProcessed, hit.Cache["result"].EntriesFound)
	}
}

// TestQueryStats_GzipCacheHitIsOneMember: a gzip response-cache hit is one
// valid gzip member (CRC and size checked, no second member) whose answer is
// the identity answer with this request's stats; a sidecar built from an
// older body, or none (a body from a peer), never mixes bytes.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStats_GzipCacheHitIsOneMember(t *testing.T) {
	stub := &statsStubVL{rows: 300}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: cache.New(60*time.Second, 100),
		LogLevel: "error", ClientResponseCompression: "gzip", ClientResponseCompressionMinBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	h := mw.CompressionHandlerWithOptions(mux, mw.CompressionOptions{Mode: "gzip", MinBytes: 64})
	now := time.Now()
	params := url.Values{"query": {`{app="api"}`}, "limit": {"1000"},
		"start": {fmt.Sprint(now.Add(-5 * time.Minute).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}}
	target := "/loki/api/v1/query_range?" + params.Encode()
	get := func(gz bool) (*httptest.ResponseRecorder, []byte) {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		if gz {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if !gz {
			return rec, rec.Body.Bytes()
		}
		if rec.Header().Get("Content-Encoding") != "gzip" {
			t.Fatalf("gzip client got Content-Encoding %q", rec.Header().Get("Content-Encoding"))
		}
		zr, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		zr.Multistream(false)
		plain, err := io.ReadAll(zr) // checks the CRC and the size
		if err != nil {
			t.Fatalf("gzip member: %v", err)
		}
		if _, err := zr.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("bytes after the gzip member: %v", err)
		}
		return rec, plain
	}
	keyReq := httptest.NewRequest(http.MethodGet, target, nil)
	keyReq.Header.Set("X-Scope-OrgID", "0")
	cacheKey, ok := p.compatCacheKey("query_range", keyReq)
	if !ok {
		t.Fatal("no compat cache key")
	}
	// A gzip miss: its own compressed answer becomes the hits' sidecar.
	_, missed := get(true)
	_, st := assertLokiStatsShape(t, missed)
	if st.Summary.TotalEntriesReturned != 300 || st.Summary.TotalLinesProcessed != 300 {
		t.Fatalf("miss stats %+v", st.Summary)
	}
	raw, ok := p.compatCache.Get(statsSidecarKey(cacheKey))
	if !ok {
		t.Fatal("the gzip miss stored no sidecar")
	}
	captured, ok := decodeStatsSidecar(raw)
	value, _ := p.compatCache.Get(cacheKey)
	stored, trailer, hasTrailer := splitStatsTrailer(value)
	entries := trailer.entries
	built, _ := buildStatsSidecar(stored)
	if !ok || !hasTrailer || entries != 300 || captured.crc != built.crc || !bytes.Equal(captured.tail, built.tail) || captured.bodyLen != built.bodyLen {
		t.Fatalf("captured sidecar %d/%x, built %d/%x; trailer %t entries %d", captured.bodyLen, captured.crc, built.bodyLen, built.crc, hasTrailer, entries)
	}
	_, identity := get(false)
	if !bytes.Equal(stripQueryStats(identity), stripQueryStats(missed)) {
		t.Fatal("the identity hit differs from the gzip miss")
	}
	for _, step := range []string{"captured sidecar", "stale sidecar", "no sidecar", "rebuilt sidecar"} {
		switch step {
		case "stale sidecar":
			old, _ := buildStatsSidecar([]byte(`{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`))
			p.compatCache.SetWithTTL(statsSidecarKey(cacheKey), old.encode(), time.Minute)
		case "no sidecar":
			p.compatCache.Invalidate(statsSidecarKey(cacheKey))
		}
		_, plain := get(true)
		_, hit := assertLokiStatsShape(t, plain)
		if !bytes.Equal(stripQueryStats(plain), stripQueryStats(identity)) {
			t.Fatalf("%s: the gzip hit's answer differs from the identity answer", step)
		}
		// A log answer's hit replays the computation's totals (Loki computes
		// log queries on every request).
		if hit.Summary.TotalEntriesReturned != 300 || hit.Summary.TotalLinesProcessed != 300 || hit.Cache["result"].EntriesFound != 1 {
			t.Fatalf("%s: hit stats %+v cache %+v", step, hit.Summary, hit.Cache["result"])
		}
		if _, ok := p.compatCache.Get(statsSidecarKey(cacheKey)); !ok {
			t.Fatalf("%s: no gzip sidecar stored", step)
		}
	}
}

// TestQueryStats_SmallAnswersStayUncompressed: an answer under the client
// compression threshold is sent uncompressed with its stats, on a miss and
// on a hit, as it was before stats.
//
// conformance: query-stats-envelope
func TestQueryStats_SmallAnswersStayUncompressed(t *testing.T) {
	stub := &statsStubVL{rows: 1}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: cache.New(60*time.Second, 100),
		LogLevel: "error", ClientResponseCompression: "gzip", ClientResponseCompressionMinBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	gz := mw.CompressionHandlerWithOptions(mux, mw.CompressionOptions{Mode: "gzip", MinBytes: 1024})
	now := time.Now().Truncate(time.Minute)
	target := "/loki/api/v1/query?" + url.Values{"query": {`sum(count_over_time({app="api"}[5m]))`}, "time": {fmt.Sprint(now.Unix())}}.Encode()
	for _, step := range []string{"miss", "hit"} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		gz.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Encoding") != "" {
			t.Fatalf("%s: status %d, Content-Encoding %q", step, rec.Code, rec.Header().Get("Content-Encoding"))
		}
		if rec.Body.Len() < 1024 {
			t.Fatalf("%s: the answer with stats is %d bytes; the case needs one over the threshold", step, rec.Body.Len())
		}
		assertLokiStatsShape(t, rec.Body.Bytes())
	}
}

// TestStatsTrailerRoundTrip: the entry-count trailer splits back exactly and
// a JSON body (which cannot end with a NUL byte) is never read as having one.
//
// conformance: query-stats-envelope
func TestStatsTrailerRoundTrip(t *testing.T) {
	for _, body := range []string{"", `{}`, `{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`} {
		for _, n := range []int64{0, 1, 127, 128, 1 << 40} {
			for _, want := range []statsTrailer{
				{entries: n},
				{entries: n, replay: true, replayed: statsReplay{bytes: n * 3, lines: n, postFilter: n / 2, exec: time.Duration(n) + 1}},
			} {
				got, tr, ok := splitStatsTrailer(appendStatsTrailer([]byte(body), want))
				if !ok || tr != want || string(got) != body {
					t.Fatalf("%q %+v: %q %+v %t", body, want, got, tr, ok)
				}
			}
		}
		if got, _, ok := splitStatsTrailer([]byte(body)); ok || string(got) != body {
			t.Fatalf("%q read as having a trailer", body)
		}
	}
}

// TestCRC32Unappend: undoing a suffix's CRC steps gives the prefix's CRC.
//
// conformance: query-stats-envelope
func TestCRC32Unappend(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 200; i++ {
		a, b := make([]byte, rng.Intn(3000)), make([]byte, rng.Intn(2000))
		rng.Read(a)
		rng.Read(b)
		full := crc32.ChecksumIEEE(append(append([]byte(nil), a...), b...))
		if got, want := crc32Unappend(full, b), crc32.ChecksumIEEE(a); got != want {
			t.Fatalf("len(a)=%d len(b)=%d: %x want %x", len(a), len(b), got, want)
		}
	}
}

// TestQueryStats_ErrorAnswerUntouched: a Loki error answer keeps its bytes.
// conformance: query-stats-envelope
func TestQueryStats_ErrorAnswerUntouched(t *testing.T) {
	stub := &statsStubVL{rows: 1}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := newQueryStatsTestProxy(t, vl.URL, "")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D", nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), "summary") {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
}

// stripQueryStats removes data.stats from an answer, for tests that compare
// two answers (cached against uncached, two routes): the stats describe each
// request's own work, so only the rest of the answer must be equal.
func stripQueryStats(body []byte) []byte {
	body, _, _ = splitStatsTrailer(body) // a stored compat value
	i := bytes.LastIndex(body, []byte(`"stats":{`))
	if i < 0 {
		return body
	}
	depth, end := 0, -1
	for j := i + len(`"stats":`); j < len(body) && end < 0; j++ {
		switch body[j] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = j + 1
			}
		}
	}
	if end < 0 {
		return body
	}
	if i > 0 && body[i-1] == ',' {
		i--
	} else if end < len(body) && body[end] == ',' {
		end++
	}
	return append(append([]byte(nil), body[:i]...), body[end:]...)
}

func testStats() []byte { return []byte(`{"summary":{"x":1}}`) }

// TestPatchStatsTail is the edge-case table of the envelope endings the
// proxy's writers produce.
// conformance: query-stats-envelope
func TestPatchStatsTail(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"empty stats", `{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`,
			`{"status":"success","data":{"resultType":"streams","result":[],"stats":{"summary":{"x":1}}}}`},
		{"no stats", `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			`{"status":"success","data":{"resultType":"matrix","result":[],"stats":{"summary":{"x":1}}}}`},
		{"no stats newline", "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":[[1,\"2\"]]}]}}\n",
			"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":[[1,\"2\"]]}],\"stats\":{\"summary\":{\"x\":1}}}}\n"},
		{"sorted empty stats", `{"data":{"result":[],"resultType":"vector","stats":{}},"status":"success"}`,
			`{"data":{"result":[],"resultType":"vector","stats":{"summary":{"x":1}}},"status":"success"}`},
		{"sorted no stats", `{"data":{"result":[],"resultType":"matrix"},"status":"success"}`,
			`{"data":{"result":[],"resultType":"matrix","stats":{"summary":{"x":1}}},"status":"success"}`},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}`,
			`{"status":"success","data":{"resultType":"scalar","result":[1,"2"],"stats":{"summary":{"x":1}}}}`},
		{"filled stats untouched", `{"status":"success","data":{"resultType":"streams","result":[],"stats":{"a":{}}}}`, ""},
		{"error untouched", `{"status":"error","errorType":"bad_data","error":"x"}`, ""},
		{"warnings untouched", `{"status":"success","data":{"resultType":"matrix","result":[]},"warnings":["w"]}`, ""},
		{"sorted other resultType untouched", `{"data":{"result":[],"resultType":"x\"y"},"status":"success"}`, ""},
		{"stats before an array untouched", `{"status":"success","data":{"resultType":"streams","result":[],"stats":{},"encodingFlags":["categorize-labels"]}}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := writeThroughStatsWriter(t, []byte(tc.body), len(tc.body))
			want := tc.want
			if want == "" {
				want = tc.body
			}
			if string(got) != want {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// writeThroughStatsWriter writes body in chunks of size chunk through the
// stats writer and returns what reached the client.
func writeThroughStatsWriter(t testing.TB, body []byte, chunk int) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "application/json")
	w := new(queryStatsWriter)
	w.reset(rec, newRequestTelemetry())
	for i := 0; i < len(body); i += chunk {
		if _, err := w.Write(body[i:min(i+chunk, len(body))]); err != nil {
			t.Fatal(err)
		}
	}
	if s, ok := statsSplice(w.head[:w.headN], w.tail[:w.tailN]); ok {
		for _, part := range [][]byte{s.pre, s.insert, testStats(), s.post, s.ws} {
			_, _ = rec.Write(part)
		}
	} else {
		_, _ = rec.Write(w.tail[:w.tailN])
	}
	return rec.Body.Bytes()
}

// TestEntryCounter_SplitWrites: the count does not depend on how the answer
// is cut into writes, and text that looks like structure inside a string is
// not counted.
// conformance: query-stats-envelope
func TestEntryCounter_SplitWrites(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       int64
	}{
		{"streams", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"metric":"x"},"values":[["1","a [\"b\"] \"metric\":{"],["2","c"]]},{"stream":{},"values":[["3","d",{"structuredMetadata":{"k":"v"}}]]}],"stats":{}}}`, 3},
		{"encoding flags", `{"status":"success","data":{"encodingFlags":["categorize-labels"],"resultType":"streams","result":[{"stream":{},"values":[["1","a",{}]]}],"stats":{}}}`, 1},
		{"matrix", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"1"},"values":[[1,"2"],[2,"3"]]},{"metric":{},"values":[[1,"2"]]}]}}`, 2},
		{"vector", `{"data":{"result":[{"metric":{"metric":"m"},"value":[1,"2"]}],"resultType":"vector"},"status":"success"}`, 1},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}`, 0},
	} {
		for chunk := 1; chunk <= len(tc.body); chunk++ {
			var c entryCounter
			for i := 0; i < len(tc.body); i += chunk {
				c.add([]byte(tc.body[i:min(i+chunk, len(tc.body))]))
			}
			if c.n != tc.want {
				t.Fatalf("%s chunk %d: %d entries, want %d", tc.name, chunk, c.n, tc.want)
			}
		}
	}
}

// FuzzPatchStatsTail: the proxy answers in one of two envelopes,
// {"status":"success","data":D} or {"data":D,"status":"success"}. For any
// JSON object D and any cut into writes, the stats writer either leaves the
// answer byte-identical or changes nothing but data.stats, which then holds
// the stats object.
// conformance: query-stats-envelope
func FuzzPatchStatsTail(f *testing.F) {
	for _, seed := range []string{
		`{"resultType":"streams","result":[],"stats":{}}`,
		`{"resultType":"matrix","result":[]}`,
		`{"result":[],"resultType":"vector","stats":{}}`,
		`{"result":[],"resultType":"matrix"}`,
		`{"a":{"stats":{}}}`,
		`{"a":{"b":[]}}`,
		`{"0000000000":"0000000","\\"stats":{}}`, // a key holding `"stats":{}` after an escaped quote
	} {
		f.Add([]byte(seed), uint8(7))
	}
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		var d map[string]any
		if json.Unmarshal(data, &d) != nil || d == nil {
			return
		}
		data = bytes.TrimSpace(data)
		// The domain is the proxy's writers': a stats key, when there is one,
		// is data's last key, once (statsSplice's known limit).
		keys, ok := jsonKeysOf(data)
		if !ok {
			return
		}
		for i, k := range keys {
			if k == "stats" && i != len(keys)-1 {
				return
			}
		}
		size := int(chunk)%97 + 1
		for _, body := range [][]byte{
			append(append([]byte(`{"status":"success","data":`), data...), '}'),
			append(append([]byte(`{"data":`), data...), `,"status":"success"}`...),
		} {
			got := writeThroughStatsWriter(t, body, size)
			if bytes.Equal(got, body) {
				continue
			}
			var in, out map[string]any
			_ = json.Unmarshal(body, &in)
			if err := json.Unmarshal(got, &out); err != nil {
				t.Fatalf("valid JSON became invalid: %q -> %q: %v", body, got, err)
			}
			inData, _ := in["data"].(map[string]any)
			outData, _ := out["data"].(map[string]any)
			if inData == nil || outData == nil || !reflect.DeepEqual(outData["stats"], map[string]any{"summary": map[string]any{"x": float64(1)}}) {
				t.Fatalf("data.stats not filled: %q -> %q", body, got)
			}
			delete(inData, "stats")
			delete(outData, "stats")
			if !reflect.DeepEqual(in, out) {
				t.Fatalf("changed more than data.stats: %q -> %q", body, got)
			}
			var env map[string]json.RawMessage
			_ = json.Unmarshal(got, &env)
			outKeys, _ := jsonKeysOf(env["data"])
			n := 0
			for _, k := range outKeys {
				if k == "stats" {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("data holds %d stats keys: %q -> %q", n, body, got)
			}
		}
	})
}

// jsonKeysOf returns the keys of the JSON object raw in their written order,
// duplicates included.
func jsonKeysOf(raw []byte) ([]string, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, false
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		k, _ := tok.(string)
		keys = append(keys, k)
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil, false
		}
	}
	return keys, true
}

// FuzzPatchStatsTailAnyBytes: on any bytes the writer neither panics nor
// changes anything but the end it fills.
// conformance: query-stats-envelope
func FuzzPatchStatsTailAnyBytes(f *testing.F) {
	f.Add([]byte(`{"status":"success","data":{"result":[]}}`), uint8(5))
	f.Fuzz(func(t *testing.T, body []byte, chunk uint8) {
		got := writeThroughStatsWriter(t, body, int(chunk)%97+1)
		if !bytes.Equal(got, body) && !bytes.Contains(got, testStats()) {
			t.Fatalf("changed without adding stats: %q -> %q", body, got)
		}
		if n := len(body) - queryStatsTailHold; n > 0 && !bytes.Equal(got[:n], body[:n]) {
			t.Fatalf("changed bytes before the held-back end: %q -> %q", body, got)
		}
	})
}

// FuzzEntryCounter: cutting a body into writes never changes the count.
// conformance: query-stats-envelope
func FuzzEntryCounter(f *testing.F) {
	f.Add([]byte(`{"result":[{"metric":{},"values":[["1","x"],["2","y"]]}]}`), uint8(3))
	f.Fuzz(func(t *testing.T, body []byte, chunk uint8) {
		var whole entryCounter
		whole.add(body)
		size := int(chunk)%31 + 1
		var split entryCounter
		for i := 0; i < len(body); i += size {
			split.add(body[i:min(i+size, len(body))])
		}
		if whole.n != split.n {
			t.Fatalf("whole %d, split by %d: %d (%q)", whole.n, size, split.n, body)
		}
	})
}

// BenchmarkQueryStatsWriter measures the layer on a log answer of 1000
// entries written in 32 KiB chunks, as the streams writer does.
func BenchmarkQueryStatsWriter(b *testing.B) {
	var body bytes.Buffer
	body.WriteString(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api"},"values":[`)
	for i := 0; i < 1000; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `["%d","GET /api/v1/items/%d 200 12ms user=alice region=us-east-1"]`, 1794232419000000000+i, i)
	}
	body.WriteString(`]}],"stats":{}}}`)
	raw := body.Bytes()
	rt := newRequestTelemetry()
	rt.start = time.Now()
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := discardResponseWriter{h: http.Header{"Content-Type": {"application/json"}}}
		w := queryStatsWriterPool.Get().(*queryStatsWriter)
		w.reset(&rec, rt)
		for off := 0; off < len(raw); off += 32 << 10 {
			_, _ = w.Write(raw[off:min(off+32<<10, len(raw))])
		}
		w.finish()
		queryStatsWriterPool.Put(w)
	}
}

type discardResponseWriter struct{ h http.Header }

func (d *discardResponseWriter) Header() http.Header         { return d.h }
func (d *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardResponseWriter) WriteHeader(int)             {}
