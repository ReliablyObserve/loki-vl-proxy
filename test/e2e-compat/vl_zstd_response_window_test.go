//go:build e2e

package e2e_compat

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// zstdWindowLimit mirrors proxy.backendZstdMaxWindow: the history a zstd
// response may declare, and the memory the decoder may hold, before the proxy
// refuses the frame.
const zstdWindowLimit = 16 << 20

// The proxy decodes a VictoriaLogs zstd response with a 16 MiB window limit, so
// a VictoriaLogs release that streamed a large answer with a bigger window, or
// as one single-segment frame whose window is its whole length, would be
// refused as a decompression bomb. This pins what VictoriaLogs sends for an
// answer well above that size: a streamed frame whose window stays inside the
// limit and which the proxy's decoder settings read to the end. The name puts it
// in the vl-pinned check (every pull request) and the VictoriaLogs version matrix.
//
// conformance: vl-zstd-response-window
func TestVLCapability_ZstdResponseWindow(t *testing.T) {
	const rows, rowBytes = 110000, 230
	unique := fmt.Sprintf("zstdwin%d", time.Now().UnixNano())
	// A tenant of its own keeps the rows out of every parity query.
	tenant := map[string]string{"AccountID": "9910022", "ProjectID": "0"}
	stamp := time.Now().Add(-2 * time.Minute).UTC()

	var batch strings.Builder
	flushBatch := func() {
		headers := map[string]string{"Content-Type": "application/stream+json"}
		for k, v := range tenant {
			headers[k] = v
		}
		target := vlURL + "/insert/jsonline?_stream_fields=app&_msg_field=_msg&_time_field=_time"
		if status, body, _ := zstdWindowDo(t, "POST", target, batch.String(), headers, "identity"); status < 200 || status >= 300 {
			t.Fatalf("VL ingest %d: %s", status, body)
		}
		batch.Reset()
	}
	for i := 0; i < rows; i++ {
		// Varied text, so the frame is a real stream and not one repeated block.
		msg := fmt.Sprintf("%08x %s", uint32(i)*2654435761, strings.Repeat(fmt.Sprintf("w%d ", (i*7919)%9973), rowBytes/8))
		fmt.Fprintf(&batch, `{"_time":%q,"_msg":%q,"app":%q}`+"\n", stamp.Add(time.Duration(i)*time.Microsecond).Format(time.RFC3339Nano), msg, unique)
		if (i+1)%20000 == 0 {
			flushBatch()
		}
	}
	if batch.Len() > 0 {
		flushBatch()
	}
	if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}

	form := url.Values{"query": {`app:="` + unique + `"`}, "limit": {fmt.Sprint(rows + 10)}}.Encode()
	headers := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	for k, v := range tenant {
		headers[k] = v
	}
	var raw []byte
	var encoding string
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, body, enc := zstdWindowDo(t, "POST", vlURL+"/select/logsql/query", form, headers, "zstd")
		if status != 200 {
			t.Fatalf("query %d: %.200s", status, body)
		}
		raw, encoding = body, enc
		if strings.Count(string(zstdWindowDecodeAll(t, body, enc)), "\n") >= rows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ingested rows did not become searchable")
		}
		time.Sleep(500 * time.Millisecond)
	}

	if encoding != "zstd" {
		t.Skipf("this VictoriaLogs answers Accept-Encoding: zstd with Content-Encoding %q, so the proxy's zstd window limit does not apply", encoding)
	}
	var header zstd.Header
	if err := header.Decode(raw); err != nil {
		t.Fatalf("zstd frame header: %v", err)
	}
	if header.SingleSegment {
		t.Fatalf("a large answer came as one single-segment frame (window = its %d byte length); the proxy refuses a window above %d", header.FrameContentSize, zstdWindowLimit)
	}
	if header.WindowSize > zstdWindowLimit {
		t.Fatalf("zstd window %d is above the proxy's %d byte limit", header.WindowSize, zstdWindowLimit)
	}

	dec, err := zstd.NewReader(bytes.NewReader(raw), zstd.WithDecoderMaxWindow(zstdWindowLimit), zstd.WithDecoderMaxMemory(zstdWindowLimit))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	n, err := io.Copy(io.Discard, dec)
	if err != nil {
		t.Fatalf("decoding %d decoded bytes with the proxy's decoder limits: %v", n, err)
	}
	if n <= zstdWindowLimit {
		t.Fatalf("the answer decodes to %d bytes, want more than the %d byte window limit so the check covers a frame that long", n, zstdWindowLimit)
	}
	t.Logf("VictoriaLogs zstd answer: %d bytes decoded from %d, window %d", n, len(raw), header.WindowSize)
}

// zstdWindowDo sends the request with the given Accept-Encoding and returns
// the body exactly as it came, with its Content-Encoding. The Go client would
// decode gzip itself when it set the header, so the header is set by hand and
// the transport left with compression disabled.
func zstdWindowDo(t *testing.T, method, target, body string, headers map[string]string, acceptEncoding string) (int, []byte, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept-Encoding", acceptEncoding)
	client := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data, resp.Header.Get("Content-Encoding")
}

func zstdWindowDecodeAll(t *testing.T, raw []byte, encoding string) []byte {
	t.Helper()
	if encoding != "zstd" {
		return raw
	}
	dec, err := zstd.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	out, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
