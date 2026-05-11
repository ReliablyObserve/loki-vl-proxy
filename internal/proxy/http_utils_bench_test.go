package proxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"testing"
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
		if err := decodeCompressedHTTPResponse(resp); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
