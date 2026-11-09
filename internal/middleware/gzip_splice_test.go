package middleware

import (
	"bytes"
	stdgzip "compress/gzip"
	"hash/crc32"
	"io"
	"strings"
	"testing"
)

// TestGzipPrefixFinishedWithStoredBlock: a prefix encoded by EncodeGzipPrefix,
// one final stored block and the trailer decode as one gzip member holding
// the prefix and the block's bytes, for empty, short and long inputs.
func TestGzipPrefixFinishedWithStoredBlock(t *testing.T) {
	for _, tc := range []struct{ prefix, tail string }{
		{"", ""},
		{"", `{"a":1}`},
		{`{"status":"success","data":{`, `"stats":{"summary":{}}}}`},
		{strings.Repeat(`["1","line"],`, 20000), strings.Repeat("x", 65535)},
	} {
		encoded, err := EncodeGzipPrefix([]byte(tc.prefix))
		if err != nil {
			t.Fatal(err)
		}
		member := AppendGzipStoredBlock(append([]byte(nil), encoded...), len(tc.tail))
		member = append(member, tc.tail...)
		crc := crc32.Update(crc32.ChecksumIEEE([]byte(tc.prefix)), crc32.IEEETable, []byte(tc.tail))
		member = AppendGzipTrailer(member, crc, uint32(len(tc.prefix)+len(tc.tail)))
		zr, err := stdgzip.NewReader(bytes.NewReader(member))
		if err != nil {
			t.Fatal(err)
		}
		zr.Multistream(false)
		got, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("prefix %d tail %d: %v", len(tc.prefix), len(tc.tail), err)
		}
		if string(got) != tc.prefix+tc.tail {
			t.Fatalf("prefix %d tail %d: decoded %d bytes", len(tc.prefix), len(tc.tail), len(got))
		}
	}
}
