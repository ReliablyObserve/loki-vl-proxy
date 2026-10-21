package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	boundsTestCap  = 64 << 10 // -backend-max-buffered-response-bytes in these tests
	boundsTestBody = 16 << 20 // what the fake backend sends
	// A bounded read holds about the cap; the unbounded ones held the body (or
	// twice it, growing). Generous against runtime noise, far below the body.
	boundsTestAllocBudget = 4 << 20
)

func boundsAllocated(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func requireBoundedAlloc(t *testing.T, allocated uint64) {
	t.Helper()
	if raceDetectorEnabled {
		return // the race detector's shadow allocations are not the proxy's
	}
	if allocated > boundsTestAllocBudget {
		t.Fatalf("allocated %d bytes reading a %d byte body under a %d byte cap, want at most %d", allocated, boundsTestBody, boundsTestCap, boundsTestAllocBudget)
	}
}

func requireTooLargeError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("an over-limit backend response must fail, not be returned")
	}
	if !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("error %q does not match errBodyTooLarge", err)
	}
	if !strings.Contains(err.Error(), "-backend-max-buffered-response-bytes") {
		t.Fatalf("error %q does not name the flag that bounds it", err)
	}
	if got := statusFromUpstreamErr(err); got != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", got)
	}
}

func newBoundsProxy(t *testing.T, cfg Config) *Proxy {
	t.Helper()
	cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = boundsTestCap
	return newGuardTestProxy(t, cfg)
}

func filledBody(prefix, fill string) []byte {
	b := make([]byte, 0, boundsTestBody)
	b = append(b, prefix...)
	for len(b) < boundsTestBody {
		b = append(b, fill...)
	}
	return b[:boundsTestBody]
}

// conformance: operator-configurable-limits
func TestBufferedBackendReads_DrilldownSitesStopAtTheBufferedLimit(t *testing.T) {
	body := filledBody(`{"values":[`, `{"value":"aaaaaaaaaaaaaaaaaaaaaaaa","hits":1},`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/field_values" && r.FormValue("field") == "service_name" {
			_, _ = w.Write([]byte(`{"values":[]}`)) // no native service names: the streams listing is read next
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	p := newBoundsProxy(t, Config{BackendURL: srv.URL})
	ctx := context.Background()

	t.Run("native field values", func(t *testing.T) {
		var err error
		allocated := boundsAllocated(func() {
			_, err = p.fetchNativeFieldValues(ctx, `{app="x"}`, "", "", "field", 10)
		})
		requireTooLargeError(t, err)
		requireBoundedAlloc(t, allocated)
	})
	t.Run("unpacked field values", func(t *testing.T) {
		var err error
		allocated := boundsAllocated(func() {
			_, err = p.fetchUnpackedFieldValues(ctx, `{app="x"}`, "", "", "field", 10)
		})
		requireTooLargeError(t, err)
		requireBoundedAlloc(t, allocated)
	})
	t.Run("streams", func(t *testing.T) {
		var err error
		allocated := boundsAllocated(func() {
			_, err = p.serviceNameValues(ctx, `{app="x"}`, "", "")
		})
		requireTooLargeError(t, err)
		requireBoundedAlloc(t, allocated)
	})
}

// A short body keeps working: the bound is a cap, not a smaller default.
// conformance: operator-configurable-limits
func TestBufferedBackendReads_UnderTheLimitStillAnswers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":[{"value":"checkout","hits":3}]}`))
	}))
	t.Cleanup(srv.Close)
	p := newBoundsProxy(t, Config{BackendURL: srv.URL})
	values, err := p.fetchNativeFieldValues(context.Background(), `{app="x"}`, "", "", "field", 10)
	if err != nil || len(values) != 1 || values[0] != "checkout" {
		t.Fatalf("values = %v, err = %v, want [checkout]", values, err)
	}
}

// An error status keeps its bounded diagnostic, not a read of the whole body.
func TestBufferedBackendReads_ErrorBodiesAreBounded(t *testing.T) {
	big := bytes.Repeat([]byte("x"), boundsTestBody)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(big)
	}))
	t.Cleanup(srv.Close)
	p := newBoundsProxy(t, Config{BackendURL: srv.URL})
	var err error
	allocated := boundsAllocated(func() {
		_, err = p.fetchNativeFieldValues(context.Background(), `{app="x"}`, "", "", "field", 10)
	})
	if err == nil {
		t.Fatal("a 500 from the backend must be an error")
	}
	requireBoundedAlloc(t, allocated)
}

// The scan paths read an error status's body up to 4 KiB, and the retry with
// unpack pipes reads its answer through the buffered limit.
// conformance: operator-configurable-limits
func TestBufferedBackendReads_ScanErrorBodiesAndUnpackRetry(t *testing.T) {
	big := bytes.Repeat([]byte("x"), boundsTestBody)
	status := func(code int) *Proxy {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Only the line scan gets the large body: the other listings
			// detectFields asks for run through the request coalescer.
			w.WriteHeader(code)
			if r.URL.Path == "/select/logsql/query" {
				_, _ = w.Write(big)
			}
		}))
		t.Cleanup(srv.Close)
		return newBoundsProxy(t, Config{BackendURL: srv.URL})
	}
	ctx := context.Background()
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadRequest} {
		t.Run(fmt.Sprintf("detectFields %d", code), func(t *testing.T) {
			p := status(code)
			allocated := boundsAllocated(func() { _, _, _ = p.detectFields(ctx, `{app="x"}`, "", "", 100) })
			requireBoundedAlloc(t, allocated)
		})
	}
	t.Run("detectScannedLabels 400", func(t *testing.T) {
		p := status(http.StatusBadRequest)
		allocated := boundsAllocated(func() { _, _ = p.detectScannedLabels(ctx, `{app="x"}`, "", "", 100) })
		requireBoundedAlloc(t, allocated)
	})
	t.Run("unpack retry of native field values", func(t *testing.T) {
		body := filledBody(`{"values":[`, `{"value":"aaaaaaaaaaaaaaaaaaaaaaaa","hits":1},`)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.FormValue("query"), "unpack_json") {
				_, _ = w.Write(body)
				return
			}
			_, _ = w.Write([]byte(`{"values":[]}`)) // no native values: the retry with unpack pipes follows
		}))
		t.Cleanup(srv.Close)
		p := newBoundsProxy(t, Config{BackendURL: srv.URL})
		var err error
		allocated := boundsAllocated(func() {
			_, err = p.fetchNativeFieldValues(ctx, `{app="x"}`, "", "", "field", 10)
		})
		requireTooLargeError(t, err)
		requireBoundedAlloc(t, allocated)
	})
}

// conformance: operator-configurable-limits
func TestLineFieldRefill_NextPageStopsAtTheBufferedLimit(t *testing.T) {
	body := filledBody(`{"_time":"2026-01-01T00:00:00Z","_msg":"{}"}`+"\n", `{"_time":"2026-01-01T00:00:00Z","_msg":"{}"}`+"\n")
	p := newBoundsProxy(t, Config{BackendURL: "http://127.0.0.1:1", LabelFilterRefillMaxPages: 2})
	// A label filter on level, with nothing before it exposing level from the
	// line: every JSON row that holds a level key is dropped.
	exposure := lineFieldExposure{filters: []lineFieldFilter{{name: "level"}}}
	// Two rows the filter drops, so the page was full and short of the limit.
	first := strings.NewReader(
		`{"_time":"2026-01-02T00:00:00Z","_stream":"{app=\"x\"}","_msg":"{\"level\":\"info\"}"}` + "\n" +
			`{"_time":"2026-01-01T00:00:00Z","_stream":"{app=\"x\"}","_msg":"{\"level\":\"info\"}"}` + "\n")
	fetch := func(ctx context.Context, params url.Values) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}, nil
	}
	var err error
	allocated := boundsAllocated(func() {
		_, err = p.refillLineFieldRows(context.Background(), first, url.Values{}, 2, false, &exposure, false, fetch)
	})
	requireTooLargeError(t, err)
	requireBoundedAlloc(t, allocated)
}

func coldBoundsProxy(t *testing.T, cold http.Handler, hot http.Handler) *Proxy {
	t.Helper()
	coldSrv := httptest.NewServer(cold)
	t.Cleanup(coldSrv.Close)
	hotSrv := httptest.NewServer(hot)
	t.Cleanup(hotSrv.Close)
	p, err := New(Config{
		BackendURL:      hotSrv.URL,
		ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: boundsTestCap},
		LogLevel:        "error",
		ColdBackend:     ColdBackendConfig{Enabled: true, URL: coldSrv.URL, Boundary: 7 * 24 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// A cold chunk above the buffered limit fails the query; so do chunks that
// each fit but together pass it, which would otherwise grow with the range.
// conformance: operator-configurable-limits
func TestColdBackwardChunkedFetch_StopsAtTheBufferedLimit(t *testing.T) {
	row := []byte(`{"_time":"2026-01-01T00:00:00Z","_msg":"cold row"}` + "\n")
	hot := http.NotFoundHandler()
	now := time.Now()
	start, end := now.Add(-10*time.Hour).UnixNano(), now.UnixNano()

	t.Run("one chunk over the limit", func(t *testing.T) {
		big := bytes.Repeat(row, boundsTestBody/len(row))
		p := coldBoundsProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(big) }), hot)
		var err error
		allocated := boundsAllocated(func() {
			_, err = p.coldBackwardChunkedFetch(context.Background(), url.Values{"query": {"*"}}, start, end, 1_000_000)
		})
		requireTooLargeError(t, err)
		requireBoundedAlloc(t, allocated)
	})

	t.Run("chunks together over the limit", func(t *testing.T) {
		chunk := bytes.Repeat(row, (boundsTestCap*2/5)/len(row)) // 40% of the limit per chunk
		var calls int
		p := coldBoundsProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			_, _ = w.Write(chunk)
		}), hot)
		_, err := p.coldBackwardChunkedFetch(context.Background(), url.Values{"query": {"*"}}, start, end, 1_000_000)
		requireTooLargeError(t, err)
		if calls != 3 {
			t.Fatalf("cold backend calls = %d, want 3 (two chunks fit, the third passes the shared budget)", calls)
		}
	})

	t.Run("under the limit", func(t *testing.T) {
		p := coldBoundsProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(row) }), hot)
		got, err := p.coldBackwardChunkedFetch(context.Background(), url.Values{"query": {"*"}}, start, end, 1_000_000)
		if err != nil || len(got) == 0 {
			t.Fatalf("len = %d, err = %v, want rows", len(got), err)
		}
	})
}

func TestColdManifestRefresh_BodyIsBounded(t *testing.T) {
	// One JSON value the size of the body: a decoder has to hold all of it.
	manifest := filledBody(`{"minDate":"`, "aaaaaaaaaaaaaaaa")
	manifest = append(manifest[:len(manifest)-2], '"', '}')
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(manifest) }))
	t.Cleanup(srv.Close)
	cr := &ColdRouter{coldBackend: mustParseURL(srv.URL), client: srv.Client(), logger: testSlogger()}

	allocated := boundsAllocated(func() { cr.refreshManifest(context.Background()) })
	if cr.HasManifest() {
		t.Fatal("a manifest above its size limit must not be applied")
	}
	if !raceDetectorEnabled && allocated > boundsTestAllocBudget {
		t.Fatalf("allocated %d bytes for a %d byte manifest, want about the %d byte limit", allocated, len(manifest), maxColdManifestBytes)
	}

	small := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"minDate":"2026-01-01","maxDate":"2026-04-30","totalFiles":1}`))
	}))
	t.Cleanup(small.Close)
	cr = &ColdRouter{coldBackend: mustParseURL(small.URL), client: small.Client(), logger: testSlogger()}
	cr.refreshManifest(context.Background())
	if m := cr.ManifestRange(); m == nil || m.TotalFiles != 1 {
		t.Fatalf("a manifest under the limit must be applied, got %+v", m)
	}
}

func gzipBomb(t *testing.T, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if _, err := zw.Write(bytes.Repeat([]byte("0"), n)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zstdBomb(t *testing.T, n int, opts ...zstd.EOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(bytes.Repeat([]byte("0"), n)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func compressedResponse(encoding string, body []byte) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Encoding": []string{encoding}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

// A few KiB that expand to far more than the limit stop at the limit: the
// consumer is handed at most the limit and then the error.
func TestDecodeCompressedHTTPResponse_StopsADecompressionBomb(t *testing.T) {
	const limit = 1 << 20
	for name, frame := range map[string][]byte{
		"gzip": gzipBomb(t, boundsTestBody),
		"zstd": zstdBomb(t, boundsTestBody),
	} {
		t.Run(name, func(t *testing.T) {
			if len(frame) > 256<<10 {
				t.Fatalf("a bomb should be small, got %d bytes", len(frame))
			}
			resp := compressedResponse(name, frame)
			if err := decodeCompressedHTTPResponse(resp, limit); err != nil {
				t.Fatal(err)
			}
			var got []byte
			var err error
			allocated := boundsAllocated(func() { got, err = io.ReadAll(resp.Body) })
			_ = resp.Body.Close()
			requireTooLargeError(t, err)
			if len(got) > limit {
				t.Fatalf("the consumer received %d bytes, want at most the %d byte limit", len(got), limit)
			}
			// The zstd decoder holds the history its frame declares (8 MiB here).
			if !raceDetectorEnabled && allocated > limit+backendZstdMaxWindow {
				t.Fatalf("allocated %d bytes for a %d byte limit", allocated, limit)
			}
		})
	}
}

func TestDecodeCompressedHTTPResponse_LimitIsInclusiveAndStreamsAreUnbounded(t *testing.T) {
	exact := bytes.Repeat([]byte("0"), 4096)
	resp := compressedResponse("gzip", gzipBomb(t, len(exact)))
	if err := decodeCompressedHTTPResponse(resp, int64(len(exact))); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(resp.Body); err != nil || !bytes.Equal(got, exact) {
		t.Fatalf("a body of exactly the limit must be read whole, got %d bytes, err %v", len(got), err)
	}

	resp = compressedResponse("zstd", zstdBomb(t, 2<<20))
	if err := decodeCompressedHTTPResponse(resp, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(resp.Body); err != nil || len(got) != 2<<20 {
		t.Fatalf("limit 0 (tail) must not cap the stream, got %d bytes, err %v", len(got), err)
	}
}

// A streamed frame far longer than the window limit decodes whole: the limit is
// on the history a frame declares, not on the bytes it carries. VictoriaLogs
// sends answers like this one (128 KiB window, no declared length).
func TestDecodeCompressedHTTPResponse_StreamLongerThanTheWindowLimitDecodes(t *testing.T) {
	const n = 2*backendZstdMaxWindow + 1
	resp := compressedResponse("zstd", zstdBomb(t, n, zstd.WithWindowSize(128<<10)))
	if err := decodeCompressedHTTPResponse(resp, 256<<20); err != nil {
		t.Fatal(err)
	}
	if got, err := io.Copy(io.Discard, resp.Body); err != nil || got != n {
		t.Fatalf("decoded %d bytes, err %v, want %d", got, err, n)
	}
}

// A frame that declares a window above the limit is refused before the decoder
// allocates it: the declared window, not the bytes decoded, is what a hostile
// frame spends.
func TestDecodeCompressedHTTPResponse_RejectsAnOversizedZstdWindow(t *testing.T) {
	frame := zstdBomb(t, 1<<20, zstd.WithWindowSize(128<<20))
	resp := compressedResponse("zstd", frame)
	var err error
	allocated := boundsAllocated(func() {
		if err = decodeCompressedHTTPResponse(resp, 256<<20); err == nil {
			_, err = io.ReadAll(resp.Body)
		}
	})
	_ = resp.Body.Close()
	if err == nil {
		t.Fatal("a frame declaring a 128 MiB window must be rejected")
	}
	if !raceDetectorEnabled && allocated > backendZstdMaxWindow {
		t.Fatalf("rejecting the frame allocated %d bytes, want at most the %d byte window limit", allocated, backendZstdMaxWindow)
	}
}

// A bomb through the real request path: the guard that turns a failed body into
// "VictoriaLogs aborted the response" must hand the size error through, and a
// POST answers with the same error.
// conformance: semantics/backend-aborted-response
func TestVLRequests_DecompressionBombFailsWithTheSizeError(t *testing.T) {
	old := decodedBackendBodyFloor
	decodedBackendBodyFloor = 1 << 20
	t.Cleanup(func() { decodedBackendBodyFloor = old })

	for _, encoding := range []string{"gzip", "zstd"} {
		frame := gzipBomb(t, boundsTestBody)
		if encoding == "zstd" {
			frame = zstdBomb(t, boundsTestBody)
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", encoding)
			_, _ = w.Write(frame)
		}))
		t.Cleanup(srv.Close)
		p := newBoundsProxy(t, Config{BackendURL: srv.URL, BackendCompression: encoding})

		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(encoding+" "+method, func(t *testing.T) {
				var resp *http.Response
				var err error
				if method == http.MethodGet {
					resp, err = p.vlGet(context.Background(), "/select/logsql/field_values", url.Values{"query": {"*"}, "field": {"x"}})
				} else {
					resp, err = p.vlPost(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}})
				}
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				_, err = io.ReadAll(resp.Body)
				requireTooLargeError(t, err)
				var aborted *vlResponseAbortedError
				if errors.As(err, &aborted) {
					t.Fatalf("a size error must not be reported as an aborted VictoriaLogs response: %v", err)
				}
				if strings.Contains(err.Error(), "aborted") || strings.Contains(err.Error(), "timed out") {
					t.Fatalf("error %q misreports the cause", err)
				}
			})
		}
	}
}

func TestBackendBodyTooLargeError_Message(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &backendBodyTooLargeError{limit: 123})
	if !errors.Is(err, errBodyTooLarge) {
		t.Fatal("must match errBodyTooLarge so the existing degrade paths still see it")
	}
	if !strings.Contains(err.Error(), "123 bytes") || !strings.Contains(err.Error(), "-backend-max-buffered-response-bytes") {
		t.Fatalf("message %q must name the limit and the flag", err)
	}
}
