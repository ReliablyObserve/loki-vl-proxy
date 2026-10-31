package cache

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"
)

func TestDiskRecordRoundTrip(t *testing.T) {
	random := make([]byte, 4096)
	_, _ = rand.New(rand.NewSource(17)).Read(random)
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		for name, value := range map[string][]byte{"empty": {}, "json": bytes.Repeat([]byte(`{"message":"read cache","level":"info"}`), 200), "random": random} {
			t.Run(codec.String()+"/"+name, func(t *testing.T) {
				entry := diskEntry{Value: value, ExpiresAt: 1893456000123456789}
				raw, err := encodeDiskRecord(entry, codec)
				if err != nil {
					t.Fatal(err)
				}
				for _, legacyCompression := range []bool{false, true} {
					got, err := decodeDiskRecord(raw, legacyCompression)
					if err != nil || got.ExpiresAt != entry.ExpiresAt || !bytes.Equal(got.Value, value) {
						t.Fatalf("roundtrip got=%+v err=%v", got, err)
					}
					expiry, err := diskRecordExpiry(raw, legacyCompression)
					if err != nil || expiry != entry.ExpiresAt {
						t.Fatalf("expiry=%d err=%v", expiry, err)
					}
				}
				if name != "json" && raw[5] != byte(diskCodecRaw) {
					t.Fatalf("incompressible payload was not stored raw: %d", raw[5])
				}
				if name == "json" && codec != diskCodecRaw && len(raw) >= len(value) {
					t.Fatal("compressible payload was not compressed")
				}
			})
		}
	}
}

func TestParseDiskCodec(t *testing.T) {
	for name, want := range map[string]diskCodec{"": diskCodecS2, "s2": diskCodecS2, "zstd": diskCodecZstd, "gzip": diskCodecGzip, "raw": diskCodecRaw} {
		got, err := parseDiskCodec(name)
		if err != nil || got != want {
			t.Fatalf("parse %q=%v,%v", name, got, err)
		}
	}
	if _, err := parseDiskCodec("brotli"); err == nil {
		t.Fatal("unsupported codec accepted")
	}
	if _, err := encodeDiskRecord(diskEntry{}, diskCodec(99)); err == nil {
		t.Fatal("unsupported encode codec accepted")
	}
}

func TestDiskRecordLegacy(t *testing.T) {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, 1893456000123456789)
	raw = append(raw, []byte("legacy body")...)
	gz, err := compress(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body       []byte
		compressed bool
	}{{raw, false}, {gz, true}} {
		got, err := decodeDiskRecord(tc.body, tc.compressed)
		if err != nil || got.ExpiresAt != 1893456000123456789 || string(got.Value) != "legacy body" {
			t.Fatalf("legacy decode=%+v err=%v", got, err)
		}
		expiry, err := diskRecordExpiry(tc.body, tc.compressed)
		if err != nil || expiry != 1893456000123456789 {
			t.Fatalf("legacy expiry=%d err=%v", expiry, err)
		}
	}
	for _, body := range [][]byte{nil, {1, 2, 3}, bytes.Repeat([]byte{0xff}, 32)} {
		if _, err := decodeDiskRecord(body, true); err == nil {
			t.Fatal("invalid legacy gzip accepted")
		}
		if _, err := diskRecordExpiry(body, true); err == nil {
			t.Fatal("invalid legacy gzip expiry accepted")
		}
	}
	shortGzip, _ := compress([]byte{1, 2, 3})
	if _, err := decodeDiskRecord(shortGzip, true); err == nil {
		t.Fatal("short legacy body accepted")
	}
	if _, err := diskRecordExpiry(shortGzip, true); err == nil {
		t.Fatal("short legacy expiry accepted")
	}
	if _, err := decodeDiskRecord([]byte{1, 2, 3}, false); err == nil {
		t.Fatal("short legacy raw accepted")
	}
}

func TestDiskRecordRejectsCorruption(t *testing.T) {
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecS2, diskCodecZstd, diskCodecGzip} {
		raw, err := encodeDiskRecord(diskEntry{Value: bytes.Repeat([]byte("compress me"), 1000), ExpiresAt: 1893456000123456789}, codec)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			mutate func([]byte) []byte
		}{
			{"truncated header", func(b []byte) []byte { return b[:23] }},
			{"unsupported version", func(b []byte) []byte { b[4] = 99; return b }},
			{"unsupported codec", func(b []byte) []byte { b[5] = 99; return b }},
			{"reserved bits", func(b []byte) []byte { b[6] = 1; return b }},
			{"size cap", func(b []byte) []byte { binary.BigEndian.PutUint32(b[16:20], 256<<20+1); return b }},
			{"wrong size", func(b []byte) []byte { binary.BigEndian.PutUint32(b[16:20], 7); return b }},
			{"expiry bitflip", func(b []byte) []byte { b[8] ^= 1; return b }},
			{"checksum", func(b []byte) []byte { b[20] ^= 1; return b }},
			{"body bitflip", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }},
			{"truncated body", func(b []byte) []byte { return b[:len(b)-1] }},
			{"trailing body", func(b []byte) []byte { return append(b, 0) }},
		} {
			t.Run(codec.String()+"/"+tc.name, func(t *testing.T) {
				bad := tc.mutate(bytes.Clone(raw))
				if _, err := decodeDiskRecord(bad, false); err == nil {
					t.Fatal("corrupt frame accepted")
				}
			})
		}
		// Expiry scans must not decode the body, even if it is damaged.
		raw[len(raw)-1] ^= 1
		if expiry, err := diskRecordExpiry(raw, false); err != nil || expiry != 1893456000123456789 {
			t.Fatalf("expiry scans decoded payload: %d %v", expiry, err)
		}
	}
}

func TestDiskRecordOwnsDecodedMemory(t *testing.T) {
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		value := bytes.Repeat([]byte("owned memory"), 100)
		raw, _ := encodeDiskRecord(diskEntry{Value: value, ExpiresAt: 12345}, codec)
		got, err := decodeDiskRecord(raw, false)
		if err != nil {
			t.Fatal(err)
		}
		for i := range raw {
			raw[i] = 0
		}
		for i := 0; i < 50; i++ {
			other, _ := encodeDiskRecord(diskEntry{Value: bytes.Repeat([]byte("another body"), 100)}, codec)
			decoded, err := decodeDiskRecord(other, false)
			if err != nil {
				t.Fatal(err)
			}
			for j := range decoded.Value {
				decoded.Value[j] = 0
			}
		}
		if !bytes.Equal(got.Value, value) {
			t.Fatal("decoded memory aliases input or pooled state")
		}
	}
}

func TestDiskRecordConcurrentDecode(t *testing.T) {
	var wg sync.WaitGroup
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		raw, _ := encodeDiskRecord(diskEntry{Value: bytes.Repeat([]byte("parallel body"), 100), ExpiresAt: 98765}, codec)
		for i := 0; i < 8; i++ {
			wg.Go(func() {
				for j := 0; j < 100; j++ {
					got, err := decodeDiskRecord(raw, true)
					if err != nil || got.ExpiresAt != 98765 || len(got.Value) != 1300 {
						t.Errorf("concurrent decode len=%d err=%v", len(got.Value), err)
						return
					}
				}
			})
		}
	}
	wg.Wait()
}

func TestDiskRecordCallerLimit(t *testing.T) {
	value := bytes.Repeat([]byte("bounded value"), 100)
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		raw, err := encodeDiskRecord(diskEntry{Value: value}, codec)
		if err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{-1, 0, len(value) - 1} {
			if _, err := decodeDiskRecordLimit(raw, false, limit); err == nil {
				t.Fatalf("%s accepted value beyond limit %d", codec, limit)
			}
		}
		if got, err := decodeDiskRecordLimit(raw, false, len(value)); err != nil || !bytes.Equal(got.Value, value) {
			t.Fatalf("%s rejected exact limit: %v", codec, err)
		}
	}
	legacy := make([]byte, 8)
	legacy = append(legacy, value...)
	compressed, _ := compress(legacy)
	for _, tc := range []struct {
		raw        []byte
		compressed bool
	}{{legacy, false}, {compressed, true}} {
		if _, err := decodeDiskRecordLimit(tc.raw, tc.compressed, len(value)-1); err == nil {
			t.Fatal("legacy decode bypassed caller limit")
		}
		if got, err := decodeDiskRecordLimit(tc.raw, tc.compressed, len(value)); err != nil || !bytes.Equal(got.Value, value) {
			t.Fatalf("legacy exact limit: %v", err)
		}
	}
}

func TestDiskRecordExpiryRejectsInvalidHeaders(t *testing.T) {
	raw, _ := encodeDiskRecord(diskEntry{Value: []byte("body"), ExpiresAt: 1234}, diskCodecRaw)
	for _, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { return b[:23] },
		func(b []byte) []byte { b[4] = 255; return b },
		func(b []byte) []byte { b[5] = 255; return b },
		func(b []byte) []byte { b[7] = 1; return b },
		func(b []byte) []byte { binary.BigEndian.PutUint32(b[16:20], 256<<20+1); return b },
	} {
		if _, err := diskRecordExpiry(mutate(bytes.Clone(raw)), false); err == nil {
			t.Fatal("invalid expiry header accepted")
		}
	}
}

func TestNativeDiskRecordRejectsDamagedMagic(t *testing.T) {
	raw, _ := encodeDiskRecord(diskEntry{Value: []byte("native body"), ExpiresAt: 1893456000123456789}, diskCodecS2)
	for offset := 0; offset < 4; offset++ {
		bad := bytes.Clone(raw)
		bad[offset] ^= 1
		if _, err := decodeNativeDiskRecordLimit(bad, 64<<10); err == nil {
			t.Fatal("damaged native magic fell back to legacy")
		}
		if _, err := nativeDiskRecordExpiry(bad); err == nil {
			t.Fatal("damaged native magic interpreted as legacy expiry")
		}
	}
	if got, err := decodeNativeDiskRecordLimit(raw, 64<<10); err != nil || string(got.Value) != "native body" {
		t.Fatalf("valid native decode: %v", err)
	}
	if expiry, err := nativeDiskRecordExpiry(raw); err != nil || expiry != 1893456000123456789 {
		t.Fatalf("valid native expiry: %v", err)
	}
	for _, bad := range [][]byte{nil, {}, []byte("LVDC")} {
		if _, err := decodeNativeDiskRecordLimit(bad, 64<<10); err == nil {
			t.Fatal("short native record accepted")
		}
		if _, err := nativeDiskRecordExpiry(bad); err == nil {
			t.Fatal("short native expiry accepted")
		}
	}
}

func FuzzDiskRecordDecode(f *testing.F) {
	f.Add([]byte{}, false)
	f.Add([]byte("bad gzip"), true)
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecGzip, diskCodecS2, diskCodecZstd} {
		frame, err := encodeDiskRecord(diskEntry{Value: bytes.Repeat([]byte("fuzz payload"), 100), ExpiresAt: 1893456000123456789}, codec)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(frame, false)
	}
	f.Fuzz(func(t *testing.T, raw []byte, legacyCompression bool) {
		got, err := decodeDiskRecordLimit(raw, legacyCompression, 64<<10)
		if err == nil && len(got.Value) > 64<<10 {
			t.Fatal("decoder exceeded limit")
		}
		_, _ = diskRecordExpiry(raw, legacyCompression)
		got, err = decodeNativeDiskRecordLimit(raw, 64<<10)
		if err == nil && len(got.Value) > 64<<10 {
			t.Fatal("native decoder exceeded limit")
		}
		_, _ = nativeDiskRecordExpiry(raw)
	})
}
