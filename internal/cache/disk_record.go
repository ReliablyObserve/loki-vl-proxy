package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

type diskCodec byte

const (
	diskCodecRaw diskCodec = iota
	diskCodecGzip
	diskCodecS2
	diskCodecZstd
	diskRecordHeaderSize = 24
	diskRecordMaxDecoded = 256 << 20
)

// Version 1 records contain magic[4], version[1], codec[1], reserved[2],
// expiry[8], decoded value length[4], CRC32C[4], then the encoded value.
// All integers are big endian. The checksum covers the first 20 header bytes
// and the decoded value, protecting both the value and its expiry metadata.
const diskRecordMagic = "LVDC"

var diskRecordCRC = crc32.MakeTable(crc32.Castagnoli)

func (codec diskCodec) String() string {
	switch codec {
	case diskCodecRaw:
		return "raw"
	case diskCodecGzip:
		return "gzip"
	case diskCodecS2:
		return "s2"
	case diskCodecZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

func parseDiskCodec(name string) (diskCodec, error) {
	switch name {
	case "", "s2":
		return diskCodecS2, nil
	case "raw":
		return diskCodecRaw, nil
	case "gzip":
		return diskCodecGzip, nil
	case "zstd":
		return diskCodecZstd, nil
	default:
		return 0, fmt.Errorf("unsupported disk cache codec %q", name)
	}
}

var diskRecordEncodeBuffer = sync.Pool{New: func() any { b := make([]byte, 0, 4096); return &b }}

// zstd's EncodeAll and DecodeAll are safe for concurrent calls. One encoder and
// decoder share their state rather than maintaining an unbounded pool per caller.
// DecodeAll is constrained by both this hard cap and each caller's destination.
var diskRecordZstd struct {
	once    sync.Once
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	err     error
}

func initDiskRecordZstd() error {
	diskRecordZstd.once.Do(func() {
		diskRecordZstd.encoder, diskRecordZstd.err = zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithWindowSize(128<<10), zstd.WithSingleSegment(true))
		if diskRecordZstd.err != nil {
			return
		}
		diskRecordZstd.decoder, diskRecordZstd.err = zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(diskRecordMaxDecoded),
			zstd.WithDecoderMaxWindow(diskRecordMaxDecoded), zstd.WithDecodeAllCapLimit(true))
	})
	return diskRecordZstd.err
}

func encodeDiskRecord(entry diskEntry, codec diskCodec) ([]byte, error) {
	if len(entry.Value) > diskRecordMaxDecoded {
		return nil, fmt.Errorf("disk cache value exceeds decoded size limit")
	}
	if codec > diskCodecZstd {
		return nil, fmt.Errorf("unsupported disk cache codec %d", codec)
	}
	value := entry.Value
	if len(value) > 0 && codec != diskCodecRaw {
		var encoded []byte
		var err error
		scratch := diskRecordEncodeBuffer.Get().(*[]byte)
		defer func() {
			if cap(*scratch) <= 1<<20 {
				diskRecordEncodeBuffer.Put(scratch)
			}
		}()
		switch codec {
		case diskCodecS2:
			encoded = s2.Encode((*scratch)[:0], value)
		case diskCodecZstd:
			if err = initDiskRecordZstd(); err == nil {
				encoded = diskRecordZstd.encoder.EncodeAll(value, (*scratch)[:0])
			}
		case diskCodecGzip:
			encoded, err = compress(value)
		}
		if err != nil {
			return nil, err
		}
		*scratch = encoded
		if len(encoded) < len(value) {
			value = encoded
		} else {
			codec = diskCodecRaw
		}
	} else {
		codec = diskCodecRaw
	}
	raw := make([]byte, diskRecordHeaderSize+len(value))
	copy(raw, diskRecordMagic)
	raw[4], raw[5] = 1, byte(codec)
	binary.BigEndian.PutUint64(raw[8:16], uint64(entry.ExpiresAt))
	binary.BigEndian.PutUint32(raw[16:20], uint32(len(entry.Value)))
	checksum := crc32.Update(0, diskRecordCRC, raw[:20])
	checksum = crc32.Update(checksum, diskRecordCRC, entry.Value)
	binary.BigEndian.PutUint32(raw[20:24], checksum)
	copy(raw[diskRecordHeaderSize:], value)
	return raw, nil
}

func diskRecordHeader(raw []byte, maxDecoded int) (diskCodec, int, int64, error) {
	if len(raw) < diskRecordHeaderSize {
		return 0, 0, 0, fmt.Errorf("truncated disk cache header")
	}
	if raw[4] != 1 || raw[6] != 0 || raw[7] != 0 {
		return 0, 0, 0, fmt.Errorf("unsupported disk cache record version or flags")
	}
	codec := diskCodec(raw[5])
	if codec > diskCodecZstd {
		return 0, 0, 0, fmt.Errorf("unsupported disk cache codec %d", codec)
	}
	size := uint64(binary.BigEndian.Uint32(raw[16:20]))
	if size > uint64(maxDecoded) || len(raw)-diskRecordHeaderSize > diskRecordMaxDecoded {
		return 0, 0, 0, fmt.Errorf("disk cache record exceeds decoded size limit")
	}
	return codec, int(size), int64(binary.BigEndian.Uint64(raw[8:16])), nil
}

func decodeDiskRecord(raw []byte, legacyCompression bool) (diskEntry, error) {
	return decodeDiskRecordLimit(raw, legacyCompression, diskRecordMaxDecoded)
}

// Native records live in a separate bucket from legacy values. Requiring the
// native magic prevents a damaged frame from being mistaken for a legacy raw
// value whose first eight bytes would otherwise be interpreted as an expiry.
func decodeNativeDiskRecordLimit(raw []byte, maxDecoded int) (diskEntry, error) {
	if len(raw) < 4 || string(raw[:4]) != diskRecordMagic {
		return diskEntry{}, fmt.Errorf("invalid native disk cache record magic")
	}
	return decodeDiskRecordLimit(raw, false, maxDecoded)
}

// decodeDiskRecordLimit always returns owned memory, so callers can decode
// directly from a bbolt read transaction without retaining its mmap pages.
// The caller's limit additionally bounds allocations for untrusted length headers.
func decodeDiskRecordLimit(raw []byte, legacyCompression bool, maxDecoded int) (diskEntry, error) {
	if maxDecoded < 0 {
		return diskEntry{}, fmt.Errorf("invalid disk cache decoded size limit")
	}
	if maxDecoded > diskRecordMaxDecoded {
		maxDecoded = diskRecordMaxDecoded
	}
	if len(raw) < 4 || string(raw[:4]) != diskRecordMagic {
		return decodeLegacyDiskRecord(raw, legacyCompression, maxDecoded)
	}
	codec, size, expiry, err := diskRecordHeader(raw, maxDecoded)
	if err != nil {
		return diskEntry{}, err
	}
	payload := raw[diskRecordHeaderSize:]
	var value []byte
	switch codec {
	case diskCodecRaw:
		if len(payload) != size {
			return diskEntry{}, fmt.Errorf("disk cache raw length mismatch")
		}
		value = bytes.Clone(payload)
	case diskCodecS2:
		decoded, decodeErr := s2.DecodedLen(payload)
		if decodeErr != nil || decoded != size {
			return diskEntry{}, fmt.Errorf("disk cache S2 length mismatch")
		}
		value, err = s2.Decode(make([]byte, 0, size), payload)
	case diskCodecZstd:
		var header zstd.Header
		if err = header.Decode(payload); err != nil {
			return diskEntry{}, err
		}
		if header.Skippable || !header.HasFCS || header.FrameContentSize != uint64(size) {
			return diskEntry{}, fmt.Errorf("disk cache zstd length mismatch")
		}
		if err = initDiskRecordZstd(); err == nil {
			value, err = diskRecordZstd.decoder.DecodeAll(payload, make([]byte, 0, size))
		}
	case diskCodecGzip:
		// A matching gzip footer rejects accidentally corrupt length headers
		// before allocating. The exact-length read still validates the stream.
		if len(payload) < 18 || binary.LittleEndian.Uint32(payload[len(payload)-4:]) != uint32(size) {
			return diskEntry{}, fmt.Errorf("disk cache gzip length mismatch")
		}
		value, err = decodeDiskGzip(payload, size, true)
	}
	if err != nil {
		return diskEntry{}, err
	}
	if len(value) != size {
		return diskEntry{}, fmt.Errorf("disk cache decoded length mismatch")
	}
	checksum := crc32.Update(0, diskRecordCRC, raw[:20])
	checksum = crc32.Update(checksum, diskRecordCRC, value)
	if checksum != binary.BigEndian.Uint32(raw[20:24]) {
		return diskEntry{}, fmt.Errorf("disk cache checksum mismatch")
	}
	return diskEntry{Value: value, ExpiresAt: expiry}, nil
}

var diskRecordInputPool = sync.Pool{New: func() any { return new(bytes.Reader) }}

// Reuse the same gzip reader pool as legacy full reads. Pooled input readers
// are cleared on release so they cannot pin a bbolt mmap or a large input value.
func acquireDiskGzip(raw []byte) (*gzip.Reader, *bytes.Reader, error) {
	input := diskRecordInputPool.Get().(*bytes.Reader)
	input.Reset(raw)
	var reader *gzip.Reader
	var err error
	if pooled := gzipReaderPool.Get(); pooled != nil {
		reader = pooled.(*gzip.Reader)
		err = reader.Reset(input)
	} else {
		reader, err = gzip.NewReader(input)
	}
	if err != nil {
		input.Reset(nil)
		diskRecordInputPool.Put(input)
		return nil, nil, err
	}
	reader.Multistream(false)
	return reader, input, nil
}

func releaseDiskGzip(reader *gzip.Reader, input *bytes.Reader) {
	if reader.Close() == nil {
		gzipReaderPool.Put(reader)
	}
	input.Reset(nil)
	diskRecordInputPool.Put(input)
}

func decodeDiskGzip(raw []byte, size int, exact bool) ([]byte, error) {
	reader, input, err := acquireDiskGzip(raw)
	if err != nil {
		return nil, err
	}
	defer releaseDiskGzip(reader, input)
	var value []byte
	if exact {
		value = make([]byte, size)
		if _, err = io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		var extra [1]byte
		if n, readErr := reader.Read(extra[:]); n != 0 || readErr != io.EOF {
			return nil, fmt.Errorf("disk cache gzip decoded length mismatch: %v", readErr)
		}
	} else {
		value, err = io.ReadAll(io.LimitReader(reader, int64(size)+1))
		if err != nil {
			return nil, err
		}
		if len(value) > size {
			return nil, fmt.Errorf("disk cache legacy gzip exceeds decoded size limit")
		}
	}
	if input.Len() != 0 {
		return nil, fmt.Errorf("disk cache gzip has trailing data")
	}
	return value, nil
}

func decodeLegacyDiskRecord(raw []byte, compressed bool, maxDecoded int) (diskEntry, error) {
	var err error
	if compressed {
		raw, err = decodeDiskGzip(raw, maxDecoded+8, false)
	} else {
		if len(raw) > maxDecoded+8 {
			return diskEntry{}, fmt.Errorf("disk cache legacy value exceeds decoded size limit")
		}
		raw = bytes.Clone(raw)
	}
	if err != nil {
		return diskEntry{}, err
	}
	if len(raw) < 8 {
		return diskEntry{}, fmt.Errorf("truncated legacy disk cache record")
	}
	return diskEntry{Value: raw[8:], ExpiresAt: int64(binary.BigEndian.Uint64(raw[:8]))}, nil
}

// Expiry scanning never decompresses versioned records. The complete checksum
// is verified by decodeDiskRecordLimit before a value can be returned as a hit.
func diskRecordExpiry(raw []byte, legacyCompression bool) (int64, error) {
	if len(raw) >= 4 && string(raw[:4]) == diskRecordMagic {
		_, _, expiry, err := diskRecordHeader(raw, diskRecordMaxDecoded)
		return expiry, err
	}
	if !legacyCompression {
		if len(raw) < 8 {
			return 0, fmt.Errorf("truncated legacy disk cache expiry")
		}
		return int64(binary.BigEndian.Uint64(raw[:8])), nil
	}
	reader, input, err := acquireDiskGzip(raw)
	if err != nil {
		return 0, err
	}
	defer releaseDiskGzip(reader, input)
	var expiry [8]byte
	if _, err = io.ReadFull(reader, expiry[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(expiry[:])), nil
}

func nativeDiskRecordExpiry(raw []byte) (int64, error) {
	if len(raw) < 4 || string(raw[:4]) != diskRecordMagic {
		return 0, fmt.Errorf("invalid native disk cache record magic")
	}
	_, _, expiry, err := diskRecordHeader(raw, diskRecordMaxDecoded)
	return expiry, err
}
