package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// malformedBodyRoutes are the routes that read a VictoriaLogs 200 body and used
// to answer 200 with no data when the body was not VictoriaLogs JSON.
var malformedBodyRoutes = []string{
	"query_instant",
	"query_range_log",
	"query_range_rate",
	"query_range_stats",
	"query_range_stats_drilldown",
	"query_range_unwrap_rate_counter",
	"series",
	"index_volume",
	"index_volume_range",
	"index_volume_range_drilldown",
}

// malformedBodyCase is one body VictoriaLogs answers with status 200.
type malformedBodyCase struct {
	name string
	// body builds the answer for a VictoriaLogs path from the valid answer
	// the error-layer gate uses for it.
	body func(path, valid string) string
	// fails reports whether the route must answer Loki's 500.
	fails bool
}

var malformedBodyCases = []malformedBodyCase{
	{name: "valid", body: func(_, valid string) string { return valid }},
	{name: "empty", body: func(_, _ string) string { return "" }},
	{name: "malformed", fails: true, body: func(_, _ string) string {
		return `{"status":"success","data":{"result":[` + errLayerSentinel + "\n"
	}},
	{name: "malformed_after_valid", fails: true, body: func(_, valid string) string {
		// A line-per-row answer holds a row that is not JSON after valid rows;
		// a one-document answer is followed by text that is not JSON.
		if strings.Contains(strings.TrimSpace(valid), "\n") {
			return valid + `{"_time":"2026-01-01T00:00:00Z","_msg":` + errLayerSentinel + "\n"
		}
		return valid + "\n" + errLayerSentinel
	}},
}

// malformedBodyValid is a valid answer of the VictoriaLogs endpoint at path
// that gives the client at least one result.
func malformedBodyValid(path string) string {
	if strings.HasSuffix(path, "/select/logsql/streams") {
		return `{"values":[{"value":"{app=\"api\"}","hits":1}]}`
	}
	return errLayerShapedBody(path, 3)
}

// runMalformedBodyCell drives one route against a VictoriaLogs that answers
// every query call with 200 and the case's body.
func runMalformedBodyCell(t *testing.T, rt errLayerRoute, tc malformedBodyCase, cfg Config) errLayerOutcome {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if !errLayerQueryPath(r.URL.Path) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(tc.body(r.URL.Path, malformedBodyValid(r.URL.Path))))
	}))
	t.Cleanup(srv.Close)
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	cfg.BackendURL = srv.URL
	cfg.Cache = c
	cfg.LogLevel = "error"
	cfg.BackendTimeout = 5 * time.Second
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, rt.path+"?"+rt.params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	for k, v := range rt.headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	shippedChain(mux).ServeHTTP(w, req)
	if hits.Load() == 0 {
		t.Fatalf("route %s never called the backend; the case proves nothing", rt.name)
	}
	return errLayerOutcome{status: w.Code, body: w.Body.String(), headers: w.Header()}
}

// TestMalformedBackendBody_FailsLikeLoki: a 200 body that is not VictoriaLogs
// JSON, or that holds a bad row after valid ones, is a failed backend call:
// Loki's 500 with its fixed text and none of the body. A valid body answers
// 200, and a body with nothing in it is still an empty result.
// conformance: semantics/error-layer-silent-success
func TestMalformedBackendBody_FailsLikeLoki(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	routes := map[string]errLayerRoute{}
	for _, rt := range errLayerRoutes(now) {
		routes[rt.name] = rt
	}
	for _, name := range malformedBodyRoutes {
		rt, ok := routes[name]
		if !ok {
			t.Fatalf("route %s is not in the error-layer gate", name)
		}
		for _, tc := range malformedBodyCases {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				o := runMalformedBodyCell(t, rt, tc, Config{})
				assertMalformedBodyOutcome(t, rt, tc, o)
			})
		}
	}
}

// assertMalformedBodyOutcome holds an answer to the case's contract.
func assertMalformedBodyOutcome(t *testing.T, rt errLayerRoute, tc malformedBodyCase, o errLayerOutcome) {
	t.Helper()
	if !tc.fails {
		if o.status != http.StatusOK {
			t.Fatalf("status %d, want 200; body %.200q", o.status, o.body)
		}
		if got := o.headers.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Errorf("Content-Type %q, want application/json", got)
		}
		if tc.name == "valid" && (strings.Contains(o.body, `"result":[]`) || strings.Contains(o.body, `"data":[]`)) {
			t.Errorf("a valid body answered no results: %.300q", o.body)
		}
		return
	}
	if o.status != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500; body %.200q", o.status, o.body)
	}
	if o.body != lokiErrInternal {
		t.Errorf("body %q, want Loki's %q", o.body, lokiErrInternal)
	}
	if got := o.headers.Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type %q, want text/plain; charset=utf-8", got)
	}
	if got := o.headers.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options %q, want nosniff", got)
	}
	if classes := errLayerClasses(rt, "malformed", o, "127.0.0.1"); len(classes) > 0 {
		t.Errorf("error-layer classes %v for %d %q", classes, o.status, o.body)
	}
	if strings.Contains(o.body+fmt.Sprint(o.headers), errLayerSentinel) {
		t.Errorf("backend text reached the client: %q %v", o.body, o.headers)
	}
}

// TestMalformedBackendBody_WindowedLogQueryFails runs the same cases through
// the windowed log query_range, whose windows were converted to an empty
// window (and cached as one) when the body was not VictoriaLogs JSON.
// conformance: semantics/error-layer-silent-success
func TestMalformedBackendBody_WindowedLogQueryFails(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	rt := errLayerRoutes(now)[0]
	if rt.name != "query_range_log" {
		t.Fatalf("first gate route is %s, want query_range_log", rt.name)
	}
	cfg := Config{
		QueryRangeWindowingEnabled: true,
		QueryRangeSplitInterval:    time.Hour,
		QueryRangeMaxParallel:      2,
		QueryRangeParallelMin:      1,
		QueryRangeParallelMax:      2,
		QueryRangeFreshness:        10 * time.Minute,
	}
	for _, tc := range malformedBodyCases {
		t.Run(tc.name, func(t *testing.T) {
			o := runMalformedBodyCell(t, rt, tc, cfg)
			assertMalformedBodyOutcome(t, rt, tc, o)
		})
	}
}

// largeStatsBody is a valid stats_query_range answer above the bound of the
// pooled stats parser, so the proxy reads it series by series.
func largeStatsBody(series, points int, ts int64) string {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < series; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"metric":{"level":"l%d"},"values":[`, i)
		for j := 0; j < points; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, ts+int64(j), j)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}}`)
	return b.String()
}

// TestMalformedBackendBody_LargeStatsBody: a stats answer above the pooled
// parser's bound is read series by series, and its junk tail or broken-off
// middle is a failed call there too.
// conformance: semantics/error-layer-silent-success
func TestMalformedBackendBody_LargeStatsBody(t *testing.T) {
	ts := time.Now().Add(-time.Hour).Unix()
	large := largeStatsBody(50, 1200, ts)
	if len(large) <= maxPooledStatsParserInputSize {
		t.Fatalf("fixture is %d bytes, want above %d", len(large), maxPooledStatsParserInputSize)
	}
	now := time.Now().Truncate(time.Minute)
	var rt errLayerRoute
	for _, r := range errLayerRoutes(now) {
		if r.name == "query_range_stats" {
			rt = r
		}
	}
	for _, tc := range []malformedBodyCase{
		{name: "valid", body: func(_, _ string) string { return large }},
		{name: "junk_tail", fails: true, body: func(_, _ string) string { return large + "\n" + errLayerSentinel }},
		{name: "junk_inside", fails: true, body: func(_, _ string) string {
			return strings.Replace(large, `[`+fmt.Sprint(ts+500)+`,"500"]`, `[1,`+errLayerSentinel+`]`, 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := runMalformedBodyCell(t, rt, tc, Config{})
			assertMalformedBodyOutcome(t, rt, tc, o)
		})
	}
}

// brokenBodyReader yields data, then fails: a body cut off after the headers.
type brokenBodyReader struct {
	data []byte
	err  error
}

func (r *brokenBodyReader) Read(b []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(b, r.data)
	r.data = r.data[n:]
	return n, nil
}

const malformedBodyRow = `{"_time":"2026-01-01T00:00:00Z","_stream":"{app=\"api\"}","app":"api","_msg":"x"}` + "\n"

// TestMalformedBackendBody_RowDecoders holds the row decoders to the contract
// directly: a row that is not JSON fails the whole read, a body that breaks
// off fails it too, and neither returns the rows read before.
// conformance: semantics/error-layer-silent-success
func TestMalformedBackendBody_RowDecoders(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	shape := newLogQueryShape(`{app="api"}`)
	cutOff := errors.New("connection reset")
	bodies := []struct {
		name    string
		reader  func() io.Reader
		wantErr error
	}{
		{"valid", func() io.Reader { return strings.NewReader(malformedBodyRow + malformedBodyRow) }, nil},
		{"empty", func() io.Reader { return strings.NewReader("") }, nil},
		{"only_blank_lines", func() io.Reader { return strings.NewReader("\n  \n") }, nil},
		{"row_without_time_is_skipped", func() io.Reader { return strings.NewReader(`{"app":"api"}` + "\n" + malformedBodyRow) }, nil},
		{"bad_row_first", func() io.Reader { return strings.NewReader("not json\n" + malformedBodyRow) }, errVLBodyNotJSON},
		{"bad_row_after_valid", func() io.Reader { return strings.NewReader(malformedBodyRow + "{\"_time\":\n") }, errVLBodyNotJSON},
		{"json_value_that_is_not_a_row", func() io.Reader { return strings.NewReader(malformedBodyRow + "[1,2]\n") }, errVLBodyNotJSON},
		{"cut_off", func() io.Reader { return &brokenBodyReader{data: []byte(malformedBodyRow), err: cutOff} }, cutOff},
	}
	for _, tc := range bodies {
		t.Run("streams/"+tc.name, func(t *testing.T) {
			streams, _, err := p.vlReaderToLokiStreams(tc.reader(), `{app="api"}`, "", false, false, false)
			checkDecoderError(t, err, tc.wantErr, len(streams))
		})
		t.Run("window/"+tc.name, func(t *testing.T) {
			entries, err := p.vlLogsToLokiWindowEntriesStream(tc.reader(), shape, false, false)
			checkDecoderError(t, err, tc.wantErr, len(entries))
		})
	}

	// The line-field filter's page reader feeds the decoders above.
	// It copies the rows it keeps to a buffer the log decoder then reads, which rejects a JSON value that is
	// not a row; a failed page returns no rows, whatever the buffer holds.
	for _, tc := range bodies {
		if tc.name == "json_value_that_is_not_a_row" {
			continue
		}
		t.Run("refill/"+tc.name, func(t *testing.T) {
			var out bytes.Buffer
			read, _, _, _, _, err := p.filterLineFieldPage(tc.reader(), nil, time.Time{}, 10, &lineFieldExposure{}, false, &out)
			checkDecoderError(t, err, tc.wantErr, read)
		})
	}

	// A segment is never stored as an empty answer when its body is not JSON.
	for _, tc := range []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"valid", `{"data":{"result":[]}}`, false},
		{"empty", ``, false},
		{"malformed", `{"data":{"result":[` + errLayerSentinel, true},
	} {
		t.Run("volume_segments/"+tc.name, func(t *testing.T) {
			_, err := parseVolumeSegmentRows([]byte(tc.body), nil)
			if (err != nil) != tc.wantErr || (err != nil && !errors.Is(err, errVLBodyNotJSON)) {
				t.Fatalf("err = %v, want error=%v", err, tc.wantErr)
			}
		})
	}
}

// checkDecoderError holds a decoder's error to want (nil: no error; a cut-off
// body keeps its read error). A failed read returns nothing of what it read.
func checkDecoderError(t *testing.T, err, want error, got int) {
	t.Helper()
	switch {
	case want == nil && err != nil:
		t.Fatalf("unexpected error %v", err)
	case want != nil && !errors.Is(err, want):
		t.Fatalf("err = %v, want %v", err, want)
	case want != nil && got != 0:
		t.Fatalf("a failed read returned %d results", got)
	}
}

// FuzzVLRowDecodersFailTheSameBodies: whatever the body, the log row decoder
// and the window decoder fail it with errVLBodyNotJSON exactly when one of its
// lines is not a JSON object, and read every other body. The -stream-response
// decoder (Loki's 500 before its envelope, an aborted response after it) fails
// a body only when the buffered decoder does.
// conformance: semantics/error-layer-silent-success
func FuzzVLRowDecodersFailTheSameBodies(f *testing.F) {
	for _, seed := range []string{
		"",
		malformedBodyRow,
		malformedBodyRow + malformedBodyRow,
		"not json\n" + malformedBodyRow,
		malformedBodyRow + "{\"_time\":\n",
		malformedBodyRow + "[1,2]\n",
		"\n  \n",
		`{"app":"api"}` + "\n",
		"123\n",
		`"text"` + "\n",
		"null\n",
		"\f\n",             // whitespace the decoders do not trim
		"{\"\":\"\\0\"}\n", // an escape fastjson reads and encoding/json refuses
	} {
		f.Add([]byte(seed))
	}
	c := cache.New(time.Minute, 100)
	f.Cleanup(c.Close)
	p, err := New(Config{BackendURL: "http://unused", Cache: c, LogLevel: "error"})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	shape := newLogQueryShape(`{app="api"}`)
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		want := false
		for _, line := range bytes.Split(body, []byte("\n")) {
			line = bytes.Trim(line, " \t\r")
			if len(line) == 0 {
				continue
			}
			var parser fj.Parser
			v, err := parser.ParseBytes(line)
			if err != nil || v.Type() != fj.TypeObject {
				want = true
			}
		}
		streamedFails := streamedLogBodyFails(p, body)
		_, _, streamsErr := p.vlReaderToLokiStreams(bytes.NewReader(body), `{app="api"}`, "", false, false, false)
		_, windowErr := p.vlLogsToLokiWindowEntriesStream(bytes.NewReader(body), shape, false, false)
		if got := errors.Is(streamsErr, errVLBodyNotJSON); got != want {
			t.Fatalf("%q: log decoder failed=%v (%v), want %v", body, got, streamsErr, want)
		}
		// The streamed decoder reads rows with encoding/json and falls back to
		// the buffered decoder's fastjson: it fails a body exactly when one of
		// its lines is no JSON object to either, and so never when the
		// buffered decoder reads the body.
		wantStreamed := false
		for _, line := range bytes.Split(body, []byte("\n")) {
			line = bytes.Trim(line, " \t\r")
			var row map[string]any
			if len(line) > 0 && !bufferedDecoderReadsRow(line) && (json.Unmarshal(line, &row) != nil || row == nil) {
				wantStreamed = true
			}
		}
		if streamedFails != wantStreamed {
			t.Fatalf("%q: -stream-response decoder failed=%v, want %v", body, streamedFails, wantStreamed)
		}
		if streamedFails && !want {
			t.Fatalf("%q: -stream-response decoder failed a body the buffered decoder reads", body)
		}
		if got := errors.Is(windowErr, errVLBodyNotJSON); got != want {
			t.Fatalf("%q: window decoder failed=%v (%v), want %v", body, got, windowErr, want)
		}
	})
}
