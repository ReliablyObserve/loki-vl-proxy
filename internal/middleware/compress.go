package middleware

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
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
type compressedResponseWriter struct {
	http.ResponseWriter
	encoding   string
	minBytes   int
	writer     responseCompressor
	release    func()
	statusCode int
	started    bool
	bypass     bool
	buf        bytes.Buffer
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
	if w.buf.Len() == 0 {
		return nil
	}
	_, err := w.writer.Write(w.buf.Bytes())
	w.buf.Reset()
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
	if w.buf.Len() == 0 {
		return nil
	}
	_, err := w.ResponseWriter.Write(w.buf.Bytes())
	w.buf.Reset()
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

func EncodeResponseBody(encoding string, body []byte) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return append([]byte(nil), body...), nil
	case "gzip":
		var buf bytes.Buffer
		compressor, release := acquireResponseCompressor(strings.ToLower(strings.TrimSpace(encoding)), &buf)
		defer release()
		if _, err := compressor.Write(body); err != nil {
			_ = compressor.Close()
			return nil, err
		}
		if err := compressor.Close(); err != nil {
			return nil, err
		}
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
