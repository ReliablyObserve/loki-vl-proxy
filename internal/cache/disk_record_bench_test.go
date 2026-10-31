package cache

import (
	"bytes"
	"encoding/binary"
	"testing"
)

var diskRecordBenchmarkEntry diskEntry
var diskRecordBenchmarkRaw []byte

func BenchmarkDiskRecordDecode(b *testing.B) {
	entry := diskEntry{Value: bytes.Repeat([]byte(`{"level":"info","message":"cache hit","service":"api"}`), 80), ExpiresAt: 1893456000123456789}
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		b.Run(codec.String(), func(b *testing.B) {
			raw, err := encodeDiskRecord(entry, codec)
			if err != nil {
				b.Fatal(err)
			}
			if _, err = decodeDiskRecord(raw, false); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(entry.Value)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				got, err := decodeDiskRecord(raw, false)
				if err != nil {
					b.Fatal(err)
				}
				diskRecordBenchmarkEntry = got
			}
		})
	}
	b.Run("legacy_gzip", func(b *testing.B) {
		old := make([]byte, 8, len(entry.Value)+8)
		binary.BigEndian.PutUint64(old, uint64(entry.ExpiresAt))
		old = append(old, entry.Value...)
		raw, err := compress(old)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(entry.Value)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			decoded, err := decompress(raw)
			if err != nil {
				b.Fatal(err)
			}
			diskRecordBenchmarkRaw = decoded
		}
	})
}

func BenchmarkDiskRecordEncode(b *testing.B) {
	entry := diskEntry{Value: bytes.Repeat([]byte(`{"level":"info","message":"cache hit","service":"api"}`), 80), ExpiresAt: 1893456000123456789}
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		b.Run(codec.String(), func(b *testing.B) {
			if _, err := encodeDiskRecord(entry, codec); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(entry.Value)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				raw, err := encodeDiskRecord(entry, codec)
				if err != nil {
					b.Fatal(err)
				}
				diskRecordBenchmarkRaw = raw
			}
		})
	}
}

func BenchmarkDiskRecordExpiry(b *testing.B) {
	entry := diskEntry{Value: bytes.Repeat([]byte("expiry scan doesn't decode versioned records"), 100), ExpiresAt: 1893456000123456789}
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		b.Run(codec.String(), func(b *testing.B) {
			raw, err := encodeDiskRecord(entry, codec)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if expiry, err := diskRecordExpiry(raw, false); err != nil || expiry != 1893456000123456789 {
					b.Fatal(err)
				}
			}
		})
	}
}
