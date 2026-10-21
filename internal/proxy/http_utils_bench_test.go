package proxy

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func BenchmarkDecodeCompressedHTTPResponseGzip(b *testing.B) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	w.Close()
	compressed := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{"gzip"}},
			Body:   io.NopCloser(bytes.NewReader(compressed)),
		}
		if err := decodeCompressedHTTPResponse(resp, 1<<20); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

func benchDecodeAndRead(b *testing.B, encoding string, compressed []byte, limit int64) {
	b.ReportAllocs()
	b.SetBytes(int64(len(compressed)))
	for i := 0; i < b.N; i++ {
		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{encoding}},
			Body:   io.NopCloser(bytes.NewReader(compressed)),
		}
		if err := decodeCompressedHTTPResponse(resp, limit); err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

func BenchmarkDecodeCompressedHTTPResponseGzipRead(b *testing.B) {
	benchDecodeAndRead(b, "gzip", gzipBodyForBench(b, benchPayload(2000)), 1<<20)
}

func BenchmarkDecodeCompressedHTTPResponseZstdRead(b *testing.B) {
	benchDecodeAndRead(b, "zstd", zstdBodyForBench(b, benchPayload(2000)), 1<<20)
}

// BenchmarkDecodeCompressedHTTPResponseZstdReadLarge decodes 16 MiB, where a
// decoder that works on blocks ahead of the reader shows.
func BenchmarkDecodeCompressedHTTPResponseZstdReadLarge(b *testing.B) {
	benchDecodeAndRead(b, "zstd", zstdBodyForBench(b, benchPayload(180_000)), 64<<20)
}

func BenchmarkDecodeCompressedHTTPResponseGzipReadLarge(b *testing.B) {
	benchDecodeAndRead(b, "gzip", gzipBodyForBench(b, benchPayload(180_000)), 64<<20)
}

// benchPayload is rows of log lines with a counter, so the data is not one
// repeated block.
func benchPayload(rows int) []byte {
	var buf bytes.Buffer
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&buf, `{"_time":"2026-01-01T00:00:%02dZ","_msg":"GET /api/v1/query/%d 200 %dms","_stream":"{app=\"x\"}"}`+"\n", i%60, i, i%97)
	}
	return buf.Bytes()
}

func gzipBodyForBench(b *testing.B, payload []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(payload)
	_ = w.Close()
	return buf.Bytes()
}

func zstdBodyForBench(b *testing.B, payload []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		b.Fatal(err)
	}
	_, _ = w.Write(payload)
	_ = w.Close()
	return buf.Bytes()
}
