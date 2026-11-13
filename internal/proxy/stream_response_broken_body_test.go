package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// With -stream-response the proxy sends the 200 and the opening envelope
// before it has read all of VictoriaLogs' body. Loki answers a failed backend
// with an error, never with a partial result, so a body that is broken before
// the first row is answered with Loki's 500, and one that breaks after the
// envelope is out ends the response abnormally: the client reads a broken
// answer, never a complete-looking JSON document.

// streamRows is n VictoriaLogs rows of about 220 bytes each.
func streamRows(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"_time":"2024-01-15T10:30:%02d.%09dZ","_stream":"{app=\"api\"}","app":"api","level":"info","_msg":"line %d %s"}`+"\n",
			i%60, i, i, strings.Repeat("x", 100))
	}
	return b.String()
}

// streamCutBody sends the headers of a body of length declared and then
// body, and closes the connection: the body breaks off.
func streamCutBody(w http.ResponseWriter, declared int, body string) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	c, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", declared)
	_, _ = buf.WriteString(body)
	_ = buf.Flush()
	_ = c.Close()
}

// streamClientView is what a client read of the proxy.
type streamClientView struct {
	status  int
	header  http.Header
	body    string
	readErr error
}

// newStreamProxyServer serves the proxy with -stream-response on the shipped
// chain from a real server, so the client sees what a connection shows. tune
// edits the config first.
func newStreamProxyServer(t testing.TB, backendURL string, tune ...func(*Config)) *httptest.Server {
	t.Helper()
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	cfg := Config{BackendURL: backendURL, Cache: c, LogLevel: "error", StreamResponse: true}
	for _, f := range tune {
		f(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(shippedChain(mux))
	t.Cleanup(front.Close)
	return front
}

func streamLogRequest(base string, limit int) string {
	now := time.Now()
	v := url.Values{
		"query": {`{app="api"}`},
		"start": {fmt.Sprint(now.Add(-time.Hour).Unix())},
		"end":   {fmt.Sprint(now.Unix())},
		"limit": {fmt.Sprint(limit)},
	}
	return base + "/loki/api/v1/query_range?" + v.Encode()
}

// readStreamClient asks the proxy for a log query; gzip lets the compression
// layer in front of the handler carry the answer.
func readStreamClient(t testing.TB, base string, limit int, acceptGzip bool) streamClientView {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, streamLogRequest(base, limit), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", "0")
	if acceptGzip {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	// The transport must not decode: the test reads the bytes itself.
	client := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			// A response that broke off before the gzip header is a read error.
			return streamClientView{status: resp.StatusCode, header: resp.Header, readErr: err}
		}
		reader = zr
	}
	body, rerr := io.ReadAll(reader)
	return streamClientView{status: resp.StatusCode, header: resp.Header, body: string(body), readErr: rerr}
}

// TestStreamResponse_BrokenBackendBody drives a real server and client through
// every shape of a broken VictoriaLogs body, with and without gzip.
// conformance: semantics/error-layer-silent-success, semantics/error-layer-internal-go-text, status-500
func TestStreamResponse_BrokenBackendBody(t *testing.T) {
	good := streamRows(12)
	// The line cap is the buffered decoder's 8 MiB: a 2 MiB row is read, a 9 MiB row is not.
	rowOf := func(n int) string {
		return `{"_time":"2024-01-15T10:30:00Z","_stream":"{app=\"api\"}","app":"api","_msg":"` + strings.Repeat("y", n) + `"}` + "\n"
	}
	bigRow := rowOf(9 << 20)
	cases := []struct {
		name    string
		serve   func(w http.ResponseWriter)
		status  int  // the status the client sees
		aborted bool // the 200 was sent and the response then ends abnormally
		streams int  // the streams of a complete answer
	}{
		// Broken before the first row: Loki's error, no envelope.
		{"first_row_not_json", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "this is not json\n"+good) }, 500, false, 0},
		{"first_row_scalar", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "42\n"+good) }, 500, false, 0},
		{"first_row_array", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "[1,2]\n"+good) }, 500, false, 0},
		{"first_row_json_null", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "null\n"+good) }, 500, false, 0},
		{"cut_before_first_row", func(w http.ResponseWriter) { streamCutBody(w, 4096, "") }, 500, false, 0},
		{"cut_inside_first_row", func(w http.ResponseWriter) { streamCutBody(w, 4096, good[:60]) }, 500, false, 0},
		{"first_row_over_the_line_limit", func(w http.ResponseWriter) { _, _ = io.WriteString(w, bigRow+good) }, 500, false, 0},
		// Rows that are not part of the answer come before the first one that is.
		{"skipped_rows_then_not_json", func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, `{"app":"no time"}`+"\n\n"+"garbage\n"+good)
		}, 500, false, 0},
		// Broken after the envelope is out: the response ends abnormally.
		{"rows_then_not_json", func(w http.ResponseWriter) { _, _ = io.WriteString(w, good+"garbage\n"+good) }, 200, true, 0},
		{"rows_then_scalar", func(w http.ResponseWriter) { _, _ = io.WriteString(w, good+"7\n"+good) }, 200, true, 0},
		{"rows_then_array", func(w http.ResponseWriter) { _, _ = io.WriteString(w, good+"[]\n"+good) }, 200, true, 0},
		{"rows_then_cut_on_a_row_boundary", func(w http.ResponseWriter) { streamCutBody(w, len(good)*2, good) }, 200, true, 0},
		{"rows_then_cut_inside_a_row", func(w http.ResponseWriter) { streamCutBody(w, len(good)*2, good+good[:90]) }, 200, true, 0},
		{"rows_then_row_over_the_line_limit", func(w http.ResponseWriter) { _, _ = io.WriteString(w, good+bigRow+good) }, 200, true, 0},
		// Not failures: nothing to answer, or rows Loki would not return.
		{"empty_body", func(w http.ResponseWriter) {}, 200, false, 0},
		{"blank_lines_only", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "\n\n  \n") }, 200, false, 0},
		{"rows_without_time_only", func(w http.ResponseWriter) { _, _ = io.WriteString(w, `{"app":"no time"}`+"\n"+`{"_msg":"x"}`+"\n") }, 200, false, 0},
		{"complete_body", func(w http.ResponseWriter) { _, _ = io.WriteString(w, good) }, 200, false, 12},
		{"row_of_2_MiB_is_read", func(w http.ResponseWriter) { _, _ = io.WriteString(w, good+rowOf(2<<20)) }, 200, false, 13},
	}
	for _, tc := range cases {
		for _, acceptGzip := range []bool{false, true} {
			name := tc.name + "/identity"
			if acceptGzip {
				name = tc.name + "/gzip"
			}
			t.Run(name, func(t *testing.T) {
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.ReadAll(r.Body)
					if !errLayerQueryPath(r.URL.Path) {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					tc.serve(w)
				}))
				defer backend.Close()
				front := newStreamProxyServer(t, backend.URL)
				got := readStreamClient(t, front.URL, 1000, acceptGzip)
				if got.status != tc.status {
					t.Fatalf("status %d, want %d (body %.200q, read error %v)", got.status, tc.status, got.body, got.readErr)
				}
				switch {
				case tc.aborted:
					if got.readErr == nil {
						t.Fatalf("the response ended cleanly with %d bytes; a broken backend body must break the response: %.300q", len(got.body), got.body)
					}
					if json.Valid([]byte(got.body)) {
						t.Fatalf("the client was sent a complete JSON document after the backend body broke: %.300q", got.body)
					}
					if !strings.HasPrefix(got.body, `{"status":"success","data":{"resultType":"streams","result":[`) {
						t.Fatalf("the rows before the failure were not streamed: %.200q", got.body)
					}
				case tc.status == 500:
					if got.readErr != nil {
						t.Fatalf("error answer unreadable: %v", got.readErr)
					}
					view := readErrorBody(t, got.header, []byte(got.body))
					if view.Error != lokiErrInternal {
						t.Fatalf("error text %q, want Loki's %q", view.Error, lokiErrInternal)
					}
					if containsAny(got.body, errLayerBackendMarkers) || containsAny(got.body, errLayerInternalMarkers) || containsAny(got.body, errLayerTransportMarkers) {
						t.Fatalf("error body leaks backend text: %q", got.body)
					}
				default:
					if got.readErr != nil {
						t.Fatalf("read error %v", got.readErr)
					}
					var doc struct {
						Status string `json:"status"`
						Data   struct {
							ResultType string            `json:"resultType"`
							Result     []json.RawMessage `json:"result"`
						} `json:"data"`
					}
					if err := json.Unmarshal([]byte(got.body), &doc); err != nil {
						t.Fatalf("answer is not JSON: %v: %.300q", err, got.body)
					}
					wantRows := tc.streams // one stream object per row
					if doc.Status != "success" || doc.Data.ResultType != "streams" || len(doc.Data.Result) != wantRows {
						t.Fatalf("answer %q, want success/streams with %d stream(s)", got.body, wantRows)
					}
					if got.header.Get("Content-Type") != "application/json" {
						t.Fatalf("Content-Type %q", got.header.Get("Content-Type"))
					}
				}
			})
		}
	}
}

// TestStreamResponse_HappyPathBytes locks the streamed answer of a healthy
// backend byte for byte.
func TestStreamResponse_HappyPathBytes(t *testing.T) {
	body := `{"_time":"2024-01-15T10:30:00Z","_stream":"{app=\"api\"}","app":"api","level":"info","_msg":"first"}` + "\n" +
		`{"_time":"2024-01-15T10:30:01Z","_stream":"{app=\"web\"}","app":"web","_msg":"second"}` + "\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if !errLayerQueryPath(r.URL.Path) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	defer backend.Close()
	front := newStreamProxyServer(t, backend.URL)
	got := readStreamClient(t, front.URL, 1000, false)
	if got.readErr != nil || got.status != http.StatusOK {
		t.Fatalf("status %d, read error %v", got.status, got.readErr)
	}
	// The query-stats writer in front of the handler fills the stats object
	// with timings; everything before it is the handler's own bytes.
	want := `{"status":"success","data":{"resultType":"streams","result":[` +
		`{"stream":{"app":"api","detected_level":"info","level":"info","service_name":"api"},"values":[["1705314600000000000","first"]]}` + `,` +
		`{"stream":{"app":"web","detected_level":"unknown","service_name":"web"},"values":[["1705314601000000000","second"]]}` +
		`],"stats":{"summary":{`
	if !strings.HasPrefix(got.body, want) || !json.Valid([]byte(got.body)) {
		t.Fatalf("streamed body changed:\n got %.600s\nwant prefix %s", got.body, want)
	}
}

// TestStreamResponse_LargeBodyMemoryFlat streams a body far larger than the
// memory the proxy may hold: the heap must not grow with the body. The bound
// leaves room for what other tests of a full run leave on the heap.
func TestStreamResponse_LargeBodyMemoryFlat(t *testing.T) {
	const rows = 240000 // about 56 MiB of VictoriaLogs rows
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if !errLayerQueryPath(r.URL.Path) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		chunk := streamRows(500)
		for i := 0; i < rows/500; i++ {
			_, _ = io.WriteString(w, chunk)
		}
	}))
	defer backend.Close()
	front := newStreamProxyServer(t, backend.URL)

	req, _ := http.NewRequest(http.MethodGet, streamLogRequest(front.URL, 1000), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := (&http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{DisableCompression: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var peak uint64
	var total int64
	buf := make([]byte, 64<<10)
	var last int64
	for {
		n, rerr := resp.Body.Read(buf)
		total += int64(n)
		if total-last > 8<<20 {
			last = total
			runtime.GC()
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > peak {
				peak = ms.HeapAlloc
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				t.Fatalf("read after %d bytes: %v", total, rerr)
			}
			break
		}
	}
	if total < 40<<20 {
		t.Fatalf("answer of %d bytes (status %d) is too small to prove anything", total, resp.StatusCode)
	}
	if growth := int64(peak) - int64(base); growth > 24<<20 {
		t.Fatalf("heap grew by %d bytes while streaming %d bytes; the body is being held", growth, total)
	}
}

// TestErrorLayer_StreamResponseNoPartialSuccess runs the error-layer gate's
// log-query routes with -stream-response on, through every backend failure
// mode, and requires Loki's answer: an error status where the status is still
// unsent, and a response that ends abnormally (never a complete document)
// where the backend body breaks off after the first row. The known-leaks list
// is not consulted, so this dimension has no leaks.
// conformance: semantics/error-layer-silent-success, semantics/error-layer-backend-failure-status, semantics/error-layer-backend-text, semantics/error-layer-internal-go-text, semantics/error-layer-cancel-deadline, backend-deadlines-and-cancellation, status-500
func TestErrorLayer_StreamResponseNoPartialSuccess(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	var routes []errLayerRoute
	for _, rt := range errLayerRoutes(now) {
		if rt.name == "query_range_log" {
			routes = append(routes, rt)
		}
	}
	if len(routes) != 1 {
		t.Fatalf("routes %d, want the range log query (an instant log query is Loki's 400, not a backend call)", len(routes))
	}
	for _, rt := range routes {
		for _, mode := range errLayerModes {
			t.Run(rt.name+"/"+mode, func(t *testing.T) {
				outcome, host := runErrLayerBackendCell(t, rt, &errLayerBackend{mode: mode, stream: true})
				if got := errLayerClasses(rt, mode, outcome, host); len(got) > 0 {
					t.Fatalf("streamed %s answers with a leak %v: status %d, aborted %v, body %.300q", mode, got, outcome.status, outcome.aborted, outcome.body)
				}
				if wantAbort := mode == "truncated"; outcome.aborted != wantAbort {
					t.Fatalf("aborted %v, want %v (status %d, body %.200q)", outcome.aborted, wantAbort, outcome.status, outcome.body)
				}
			})
		}
	}
}

// BenchmarkStreamLogQuery measures the streamed log query on a healthy body.
func BenchmarkStreamLogQuery(b *testing.B) {
	for _, kind := range []string{"plain", "json"} {
		body := detectedLevelBenchBody(kind)
		for _, categorized := range []bool{false, true} {
			name := kind
			if categorized {
				name += "/categorized"
			}
			b.Run(name, func(b *testing.B) {
				p := benchmarkDetectedLevelProxy(b)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rec := httptest.NewRecorder()
					resp := &http.Response{Body: io.NopCloser(bytes.NewReader(body))}
					p.streamLogQuery(context.Background(), rec, resp, `{app="checkout"}`, categorized, categorized)
				}
			})
		}
	}
}

// streamedLogBodyFails runs a body through the streamed log decoder and
// reports whether the client would see a failure: Loki's 500 before the
// envelope, or an aborted response after it.
func streamedLogBodyFails(p *Proxy, body []byte) bool {
	rec := httptest.NewRecorder()
	resp := &http.Response{Body: io.NopCloser(bytes.NewReader(body))}
	aborted := func() (aborted bool) {
		defer func() {
			if v := recover(); v != nil {
				if v != http.ErrAbortHandler {
					panic(v)
				}
				aborted = true
			}
		}()
		p.streamLogQuery(context.Background(), rec, resp, `{app="api"}`, false, false)
		return false
	}()
	return aborted || rec.Code != http.StatusOK
}

// TestStreamResponse_LineCapMatchesBufferedDecoder: a row the buffered log
// decoder reads is read with -stream-response, and one it refuses is refused.
// conformance: semantics/error-layer-silent-success
func TestStreamResponse_LineCapMatchesBufferedDecoder(t *testing.T) {
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: "http://unused", Cache: c, LogLevel: "error", StreamResponse: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	for _, n := range []int{100, 900 << 10, 1100 << 10, 2 << 20, 7 << 20, 9 << 20} {
		row := `{"_time":"2024-01-15T10:30:00Z","_stream":"{app=\"api\"}","app":"api","_msg":"` + strings.Repeat("y", n) + `"}` + "\n"
		_, _, bufferedErr := p.vlReaderToLokiStreams(strings.NewReader(row), `{app="api"}`, "", false, false, false)
		if streamed := streamedLogBodyFails(p, []byte(row)); streamed != (bufferedErr != nil) {
			t.Errorf("row of %d bytes: streamed fails=%v, buffered error=%v", n, streamed, bufferedErr)
		}
	}
}

// TestStreamResponse_AbortReleasesEverything: an aborted streamed request
// gives back what it held. With room for one request, and one VictoriaLogs
// call, at a time, the request after an aborted one is answered.
// conformance: semantics/error-layer-silent-success
func TestStreamResponse_AbortReleasesEverything(t *testing.T) {
	good := streamRows(12)
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if !errLayerQueryPath(r.URL.Path) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if calls.Add(1) == 1 {
			streamCutBody(w, len(good)*2, good) // breaks off after the envelope is out
			return
		}
		_, _ = io.WriteString(w, good)
	}))
	defer backend.Close()
	front := newStreamProxyServer(t, backend.URL, func(cfg *Config) {
		cfg.MaxConcurrent = 1
	})
	first := readStreamClient(t, front.URL, 1000, false)
	if first.status != http.StatusOK || first.readErr == nil {
		t.Fatalf("first request: status %d, read error %v; want an aborted 200", first.status, first.readErr)
	}
	second := readStreamClient(t, front.URL, 1000, false)
	if second.status != http.StatusOK || second.readErr != nil {
		t.Fatalf("request after the abort: status %d, read error %v, body %.200q; a slot was not released", second.status, second.readErr, second.body)
	}
}
