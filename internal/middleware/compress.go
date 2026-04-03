package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"
)

// gzipResponseWriter wraps http.ResponseWriter to gzip the response body.
type gzipResponseWriter struct {
	http.ResponseWriter
	writer     *gzip.Writer
	statusCode int
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

func (w *gzipResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	// Don't set Content-Length since gzip changes it
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(code)
}

func (w *gzipResponseWriter) Flush() {
	w.writer.Flush()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// gzip.Writer pool to reduce allocations
var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return w
	},
}

// GzipHandler compresses HTTP responses for clients that accept gzip.
// Only compresses application/json responses (the main proxy content type).
func GzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only compress if client accepts gzip
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		gz := gzipWriterPool.Get().(*gzip.Writer)
		defer gzipWriterPool.Put(gz)
		gz.Reset(w)

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")

		gzw := &gzipResponseWriter{
			ResponseWriter: w,
			writer:         gz,
		}

		next.ServeHTTP(gzw, r)
		gz.Close()
	})
}
