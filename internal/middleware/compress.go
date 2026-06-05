package middleware

import (
	"bufio"
	"bytes"
	"fmt"
	gzip "github.com/klauspost/compress/gzip"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
)

type responseCompressor interface {
	io.WriteCloser
	Flush() error
}

type responseCompressionMode string

const (
	ResponseCompressionNone responseCompressionMode = "none"
	ResponseCompressionAuto responseCompressionMode = "auto"
	ResponseCompressionGzip responseCompressionMode = "gzip"
)

type CompressionOptions struct {
	Mode     string
	MinBytes int
}

type encodedResponseCapture struct {
	encoding string
	buf      bytes.Buffer
	callback func(string, []byte)
}

// compressedResponseWriter delays compression until the handler proves the
// response is worth compressing. This keeps small control-plane responses cheap
// and lets inner handlers serve pre-compressed cache variants directly.
//
// `buf` is a POOLED *bytes.Buffer (see acquireHoldBuf/releaseHoldBuf). It must
// be released exactly once — either in startCompression (after the buffered
// bytes have been forwarded to the compressor) or in startBypass (after the
// buffered bytes have been forwarded to the underlying writer). The fallthrough
// case in finish() will release it as a safety net.
type compressedResponseWriter struct {
	http.ResponseWriter
	encoding   string
	minBytes   int
	writer     responseCompressor
	release    func()
	statusCode int
	started    bool
	bypass     bool
	buf        *bytes.Buffer
	capture    *encodedResponseCapture
}

func (w *compressedResponseWriter) Write(b []byte) (int, error) {
	ensureSafeResponseHeaders(w.ResponseWriter, "text/plain; charset=utf-8")
	if w.started {
		if w.bypass {
			header := w.Header()
			if strings.TrimSpace(header.Get("Content-Type")) == "" {
				header.Set("Content-Type", "text/plain; charset=utf-8")
			}
			if strings.TrimSpace(header.Get("X-Content-Type-Options")) == "" {
				header.Set("X-Content-Type-Options", "nosniff")
			}
			return w.ResponseWriter.Write(b)
		}
		return w.writer.Write(b)
	}
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	if w.shouldBypassCompression() {
		if err := w.startBypass(); err != nil {
			return 0, err
		}
		header := w.Header()
		if strings.TrimSpace(header.Get("Content-Type")) == "" {
			header.Set("Content-Type", "text/plain; charset=utf-8")
		}
		if strings.TrimSpace(header.Get("X-Content-Type-Options")) == "" {
			header.Set("X-Content-Type-Options", "nosniff")
		}
		return w.ResponseWriter.Write(b)
	}
	if w.minBytes <= 0 {
		if err := w.startCompression(); err != nil {
			return 0, err
		}
		return w.writer.Write(b)
	}
	// Defensive: buf may have been pre-released if finish() ran already (e.g.
	// handler panic recovered upstream). Re-acquire to handle the stray write
	// rather than panicking on a nil deref.
	if w.buf == nil {
		w.buf = acquireHoldBuf()
	}
	_, _ = w.buf.Write(b)
	if w.buf.Len() >= w.minBytes {
		if err := w.startCompression(); err != nil {
			return 0, err
		}
	}
	return len(b), nil
}

func (w *compressedResponseWriter) WriteHeader(code int) {
	ensureSafeResponseHeaders(w.ResponseWriter, "text/plain; charset=utf-8")
	if w.started {
		return
	}
	if w.writer != nil {
		w.Header().Del("Content-Length")
		w.statusCode = code
		w.ResponseWriter.WriteHeader(code)
		return
	}
	w.statusCode = code
}

func (w *compressedResponseWriter) Flush() {
	if !w.started {
		if w.shouldBypassCompression() {
			_ = w.startBypass()
		} else {
			_ = w.startCompression()
		}
	}
	if !w.bypass && w.writer != nil {
		_ = w.writer.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *compressedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijack not supported")
}

func (w *compressedResponseWriter) finish() error {
	defer w.releaseCompressor()
	// Safety net: if startCompression / startBypass didn't already release
	// the pooled hold buffer (e.g. handler returned without writing anything),
	// release it here. releaseHoldBuf tolerates a nil buf.
	defer w.releaseHoldBuf()

	if !w.started {
		if w.shouldBypassCompression() || w.buf.Len() < w.minBytes {
			if err := w.startBypass(); err != nil {
				return err
			}
		} else {
			if err := w.startCompression(); err != nil {
				return err
			}
		}
	}
	var closeErr error
	if !w.bypass && w.writer != nil {
		closeErr = w.writer.Close()
	}
	w.finalizeEncodedCapture()
	return closeErr
}

// releaseHoldBuf returns w.buf to holdBufPool and clears the field so a stray
// later Write or Flush doesn't operate on a recycled buffer. Idempotent.
func (w *compressedResponseWriter) releaseHoldBuf() {
	if w.buf == nil {
		return
	}
	releaseHoldBuf(w.buf)
	w.buf = nil
}

func (w *compressedResponseWriter) shouldBypassCompression() bool {
	if w.encoding == "" || !responseStatusAllowsBody(w.statusCode) {
		return true
	}
	return strings.TrimSpace(w.Header().Get("Content-Encoding")) != ""
}

func (w *compressedResponseWriter) startCompression() error {
	if w.started {
		return nil
	}
	w.started = true
	addVaryHeader(w.Header(), "Accept-Encoding")
	w.Header().Set("Content-Encoding", w.encoding)
	w.Header().Del("Content-Length")
	dst := io.Writer(w.ResponseWriter)
	if w.capture != nil {
		dst = io.MultiWriter(dst, &w.capture.buf)
	}
	compressor, release := acquireResponseCompressor(w.encoding, dst)
	w.writer = compressor
	w.release = release
	if w.statusCode != 0 {
		w.ResponseWriter.WriteHeader(w.statusCode)
	}
	// Drain the pooled hold buffer into the compressor exactly once, then
	// release it back to the pool — no further reads from w.buf after this
	// because we set it to nil in releaseHoldBuf.
	defer w.releaseHoldBuf()
	if w.buf == nil || w.buf.Len() == 0 {
		return nil
	}
	_, err := w.writer.Write(w.buf.Bytes())
	return err
}

func (w *compressedResponseWriter) startBypass() error {
	if w.started {
		return nil
	}
	w.started = true
	w.bypass = true
	if w.statusCode != 0 {
		w.ResponseWriter.WriteHeader(w.statusCode)
	}
	// Drain and release the hold buffer in one pass — same lifecycle rule as
	// startCompression: w.buf becomes nil after this and any subsequent Write
	// goes straight to the underlying ResponseWriter.
	defer w.releaseHoldBuf()
	if w.buf == nil || w.buf.Len() == 0 {
		return nil
	}
	_, err := w.ResponseWriter.Write(w.buf.Bytes())
	return err
}

func (w *compressedResponseWriter) releaseCompressor() {
	if w.release == nil {
		return
	}
	w.release()
	w.release = nil
}

func (w *compressedResponseWriter) registerEncodedResponseCapture(encoding string, callback func(string, []byte)) bool {
	if callback == nil || w.started {
		return false
	}
	encoding = strings.ToLower(strings.TrimSpace(encoding))
	if encoding == "" || encoding != w.encoding {
		return false
	}
	w.capture = &encodedResponseCapture{
		encoding: encoding,
		callback: callback,
	}
	return true
}

func (w *compressedResponseWriter) finalizeEncodedCapture() {
	if w.capture == nil || w.capture.callback == nil || w.capture.buf.Len() == 0 {
		return
	}
	encoded := append([]byte(nil), w.capture.buf.Bytes()...)
	w.capture.callback(w.capture.encoding, encoded)
	w.capture = nil
}

// gzipWriterChan is a GC-resistant pool of gzip.Writers. sync.Pool entries are
// cleared on every GC cycle; under high allocation pressure (10+ GC/s at c50)
// this forces flate.NewWriter on every request. A buffered channel survives GC.
const gzipWriterChanCap = 128

var gzipWriterChan = func() chan *gzip.Writer {
	ch := make(chan *gzip.Writer, gzipWriterChanCap)
	// Pre-warm with compressor already initialized so Reset(dst) reuses it.
	for range 32 {
		w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		_ = w.Flush() // forces flate.NewWriter → compressor non-nil after Reset
		w.Reset(io.Discard)
		ch <- w
	}
	return ch
}()

// holdBufPool reuses the byte-staging buffer used by compressedResponseWriter
// during the "wait for minBytes" phase before we know whether to compress or
// bypass. Previously this was a value-type bytes.Buffer inside the wrapper
// struct, allocated fresh per request — under bench load (~10–20 concurrent
// drilldown handlers) every request grew its own buffer through the standard
// doubling sequence and the buffers stayed live until the request completed.
// Pooling these caps the per-handler heap cost at one already-grown buffer.
var holdBufPool = sync.Pool{
	New: func() interface{} {
		// 8 KiB matches the default minBytes for the primary read path
		// (-response-compression-min-bytes default 4096 doubled by the
		// dashboard tier). Smaller responses fit without grow.
		return bytes.NewBuffer(make([]byte, 0, 8*1024))
	},
}

func acquireHoldBuf() *bytes.Buffer {
	b := holdBufPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func releaseHoldBuf(b *bytes.Buffer) {
	if b == nil {
		return
	}
	// Same cap guard as encodeBodyBufPool — don't pin large buffers.
	if b.Cap() > compressBufPoolMaxCap {
		*b = bytes.Buffer{}
		b.Grow(8 * 1024)
	}
	holdBufPool.Put(b)
}

// GzipHandler is kept for backward compatibility with existing tests/callers.
func GzipHandler(next http.Handler) http.Handler {
	return CompressionHandler(next, string(ResponseCompressionGzip))
}

type encodedResponseCaptureRegistrar interface {
	registerEncodedResponseCapture(string, func(string, []byte)) bool
}

type responseWriterUnwrapper interface {
	Unwrap() http.ResponseWriter
}

// RegisterEncodedResponseCapture asks the compression middleware to tee the
// compressed bytes it emits. This is used by hot caches to persist the exact
// frontend-compatible gzip payload from the initial miss instead of recomputing
// it on the first encoded hit.
func RegisterEncodedResponseCapture(w http.ResponseWriter, encoding string, callback func(string, []byte)) bool {
	for w != nil {
		if registrar, ok := w.(encodedResponseCaptureRegistrar); ok {
			return registrar.registerEncodedResponseCapture(encoding, callback)
		}
		unwrapper, ok := w.(responseWriterUnwrapper)
		if !ok {
			return false
		}
		next := unwrapper.Unwrap()
		if next == w {
			return false
		}
		w = next
	}
	return false
}

func ensureNoSniffHeader(w http.ResponseWriter) {
	if w == nil {
		return
	}
	if strings.TrimSpace(w.Header().Get("X-Content-Type-Options")) == "" {
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}
}

func ensureSafeResponseHeaders(w http.ResponseWriter, defaultContentType string) {
	ensureNoSniffHeader(w)
	if w == nil || strings.TrimSpace(defaultContentType) == "" {
		return
	}
	if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
		w.Header().Set("Content-Type", defaultContentType)
	}
}

// CompressionHandlerWithOptions negotiates response compression with clients.
// "auto" keeps the gzip path enabled for clients that advertise support.
func CompressionHandlerWithOptions(next http.Handler, opts CompressionOptions) http.Handler {
	selectedMode := normalizeCompressionMode(opts.Mode)
	if selectedMode == ResponseCompressionNone {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ensureSafeResponseHeaders(w, "text/plain; charset=utf-8")
		encoding, minBytes := PlanResponseCompression(r, opts)
		if encoding == "" {
			next.ServeHTTP(w, r)
			return
		}

		cw := &compressedResponseWriter{
			ResponseWriter: w,
			encoding:       encoding,
			minBytes:       minBytes,
			// Acquire a pooled hold buffer for the "wait for minBytes" phase.
			// finish() always releases via releaseHoldBuf (defense in depth)
			// even if startCompression / startBypass have already returned it.
			buf: acquireHoldBuf(),
		}

		next.ServeHTTP(cw, r)
		_ = cw.finish()
	})
}

// CompressionHandler negotiates response compression with clients.
// "auto" keeps the gzip path enabled for clients that advertise support.
func CompressionHandler(next http.Handler, mode string) http.Handler {
	return CompressionHandlerWithOptions(next, CompressionOptions{Mode: mode})
}

func normalizeCompressionMode(mode string) responseCompressionMode {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", string(ResponseCompressionAuto):
		return ResponseCompressionAuto
	case string(ResponseCompressionNone):
		return ResponseCompressionNone
	case string(ResponseCompressionGzip):
		return ResponseCompressionGzip
	case "zstd":
		return ResponseCompressionGzip
	default:
		return ResponseCompressionAuto
	}
}

// PlanResponseCompression applies route-aware compression policy and returns
// the negotiated encoding plus the minimum response size required before
// compression starts.
func PlanResponseCompression(r *http.Request, opts CompressionOptions) (string, int) {
	selectedMode := normalizeCompressionMode(opts.Mode)
	_, minBytes := compressionPolicyForPath("", selectedMode, opts.MinBytes)
	if r == nil {
		return "", minBytes
	}
	mode, minBytes := compressionPolicyForPath(r.URL.Path, selectedMode, opts.MinBytes)
	if mode == ResponseCompressionNone || isWebSocketUpgrade(r) || r.Method == http.MethodHead {
		return "", minBytes
	}
	return negotiateResponseEncoding(r.Header.Get("Accept-Encoding"), mode), minBytes
}

func negotiateResponseEncoding(acceptEncoding string, mode responseCompressionMode) string {
	switch mode {
	case ResponseCompressionGzip:
		if acceptsEncoding(acceptEncoding, "gzip") {
			return "gzip"
		}
	case ResponseCompressionAuto:
		if acceptsEncoding(acceptEncoding, "gzip") {
			return "gzip"
		}
	}
	return ""
}

// encodeBodyBufPool pools bytes.Buffers used to compress one response body for
// the compat cache encoded-variant flow (compatCacheEncodedVariant →
// EncodeResponseBody). Pre-pprof showed this path was the largest live-heap
// allocator (96 MB / 61 % of inuse_space — `bytes.growSlice` from
// `bytes.Buffer.ReadFrom` inside the gzip writer). Without pooling each call
// allocated a fresh bytes.Buffer that doubled through 8→16→…→1 MB+, leaving
// a peak working set proportional to N concurrent encodings. Pooling
// amortizes allocation and keeps the steady-state buffer at ~response-size
// rather than O(N) per request.
//
// Buffers are returned to the pool after the encoded bytes are copied out;
// we cap pooled buffer capacity at compressBufPoolMaxCap so a single huge
// response can't permanently inflate the pooled buffer footprint.
var encodeBodyBufPool = sync.Pool{
	New: func() interface{} {
		// Start at 8 KiB — gzip output for small JSON envelopes typically fits.
		// Larger responses grow on first use; we cap-trim on Put.
		return bytes.NewBuffer(make([]byte, 0, 8*1024))
	},
}

// compressBufPoolMaxCap is the hard limit on per-buffer capacity returned to
// the pool. A single 50 MB response would otherwise pin 50 MB in the pool
// forever. 4 MiB covers the p95 Drilldown response size with headroom.
const compressBufPoolMaxCap = 4 * 1024 * 1024

func EncodeResponseBody(encoding string, body []byte) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return append([]byte(nil), body...), nil
	case "gzip":
		buf := encodeBodyBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer func() {
			// Trim oversize buffers before returning to the pool — see
			// compressBufPoolMaxCap. bytes.Buffer doesn't expose a cap-shrink
			// API, so we replace the underlying slice when it has grown past
			// the cap, then put the trimmed buffer back.
			if buf.Cap() > compressBufPoolMaxCap {
				*buf = bytes.Buffer{}
				buf.Grow(8 * 1024)
			}
			encodeBodyBufPool.Put(buf)
		}()
		// Hint the writer about the input size — gzip output for JSON is
		// typically 4–10 % of input. Pre-growing here avoids the
		// 8 KiB→16 KiB→…→N MB doubling cascade that dominated pprof.
		if est := len(body) / 10; est > buf.Cap() {
			buf.Grow(est)
		}
		compressor, release := acquireResponseCompressor(strings.ToLower(strings.TrimSpace(encoding)), buf)
		defer release()
		if _, err := compressor.Write(body); err != nil {
			_ = compressor.Close()
			return nil, err
		}
		if err := compressor.Close(); err != nil {
			return nil, err
		}
		// Caller mutates the returned slice (cache store); must be a copy
		// because the buffer's backing array goes back to the pool.
		return append([]byte(nil), buf.Bytes()...), nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", encoding)
	}
}

func acceptsEncoding(header, encoding string) bool {
	for _, part := range strings.Split(strings.ToLower(header), ",") {
		token := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if token == encoding || token == "*" {
			return true
		}
	}
	return false
}

func acquireResponseCompressor(encoding string, dst io.Writer) (responseCompressor, func()) {
	var gz *gzip.Writer
	select {
	case gz = <-gzipWriterChan:
		gz.Reset(dst)
	default:
		gz, _ = gzip.NewWriterLevel(dst, gzip.BestSpeed)
	}
	return gz, func() {
		gz.Reset(io.Discard)
		select {
		case gzipWriterChan <- gz:
		default: // channel full — let GC collect
		}
	}
}

func compressionPolicyForPath(path string, mode responseCompressionMode, minBytes int) (responseCompressionMode, int) {
	baseMinBytes := minBytes
	if baseMinBytes < 0 {
		baseMinBytes = 0
	}
	switch {
	case mode == ResponseCompressionNone:
		return ResponseCompressionNone, 0
	case isControlPlanePath(path):
		return ResponseCompressionNone, 0
	case isPrimaryReadPath(path):
		return mode, baseMinBytes
	case isMetadataReadPath(path):
		return mode, scaleMinBytes(baseMinBytes, 4)
	default:
		return mode, scaleMinBytes(baseMinBytes, 2)
	}
}

func isControlPlanePath(path string) bool {
	switch path {
	case "/alive", "/livez", "/health", "/healthz", "/ready", "/config", "/loki/api/v1/status/buildinfo":
		return true
	default:
		return false
	}
}

func isPrimaryReadPath(path string) bool {
	switch path {
	case "/loki/api/v1/query", "/loki/api/v1/query_range":
		return true
	default:
		return false
	}
}

func isMetadataReadPath(path string) bool {
	return strings.HasPrefix(path, "/loki/api/v1/label/") ||
		path == "/loki/api/v1/labels" ||
		path == "/loki/api/v1/series" ||
		strings.HasPrefix(path, "/loki/api/v1/index/") ||
		strings.HasPrefix(path, "/loki/api/v1/detected_") ||
		path == "/loki/api/v1/patterns"
}

func scaleMinBytes(baseMinBytes, factor int) int {
	if baseMinBytes <= 0 || factor <= 1 {
		return baseMinBytes
	}
	return baseMinBytes * factor
}

func responseStatusAllowsBody(statusCode int) bool {
	switch {
	case statusCode == 0:
		return true
	case statusCode >= 100 && statusCode < 200:
		return false
	case statusCode == http.StatusNoContent:
		return false
	case statusCode == http.StatusNotModified:
		return false
	default:
		return true
	}
}

func addVaryHeader(header http.Header, value string) {
	for _, existing := range header.Values("Vary") {
		for _, token := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(token), value) {
				return
			}
		}
	}
	header.Add("Vary", value)
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") &&
		strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}
