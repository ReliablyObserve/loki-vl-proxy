package cache

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	bolt "go.etcd.io/bbolt"
)

// Wire format of the peer bulk restore stream, version 1 (POST /_cache/bulk).
//
// The body starts with the 8-byte magic, then frames. Every frame starts with
// a header:
//
//	type[1] flags[1] a[uvarint] b[uvarint] c[uvarint] crc32c[4]
//
// whose checksum covers the bytes before it. A plan frame (a = entries,
// b = bytes) comes first; record frames (a = key length, b = record length,
// c = remaining TTL in milliseconds, flags = memory-only) are followed by the
// key, the record and a CRC32C of key and record; the record is a native disk
// record (disk_record.go), whose own checksum covers the decoded value and its
// header. An end frame (flags = outcome, a = entries sent, b = their bytes,
// c = planned entries the sender skipped) closes a complete stream.
//
// A reader stops at a header whose checksum or bounds fail (the frame
// boundaries can no longer be trusted) and skips a record whose payload
// checksum or decoding fails. A stream that ends without an end frame is
// truncated: what arrived before stays valid record by record.

const (
	peerBulkPath          = "/_cache/bulk"
	peerBulkVersion       = "1"
	peerBulkVersionHeader = "X-Peer-Bulk-Version"
	// peerBulkStateHeader says why a bulk request was not streamed: busy
	// (retry after Retry-After), restoring or disabled (do not retry), ready
	// (a probe).
	peerBulkStateHeader = "X-Peer-Bulk-State"
	peerBulkContentType = "application/x-loki-vl-proxy-bulk"
	peerBulkMagic       = "LVPBULK1"

	bulkFramePlan   byte = 1
	bulkFrameRecord byte = 2
	bulkFrameEnd    byte = 3

	bulkFlagMemoryOnly byte = 1

	// bulkMaxKeyBytes and bulkMaxRecordBytes bound a record frame before any
	// of its payload is read.
	bulkMaxKeyBytes    = bolt.MaxKeySize
	bulkMaxRecordBytes = diskRecordHeaderSize + diskRecordMaxDecoded
	bulkMaxHeaderBytes = 2 + 3*binary.MaxVarintLen64 + 4
)

// End frame outcomes.
const (
	bulkEndDone byte = iota
	bulkEndBudget
	bulkEndPurged
	bulkEndStopped
	bulkEndCount
)

var bulkEndNames = [bulkEndCount]string{"done", "budget", "purged", "stopped"}

var bulkCRC = crc32.MakeTable(crc32.Castagnoli)

var (
	errBulkMagic         = errors.New("peer bulk stream: bad magic")
	errBulkHeader        = errors.New("peer bulk stream: corrupt frame header")
	errBulkRecordCorrupt = errors.New("peer bulk stream: corrupt record payload")
)

// bulkFrame is one decoded frame header.
type bulkFrame struct {
	typ, flags byte
	a, b, c    uint64
}

func appendBulkHeader(dst []byte, f bulkFrame) []byte {
	start := len(dst)
	dst = append(dst, f.typ, f.flags)
	dst = binary.AppendUvarint(dst, f.a)
	dst = binary.AppendUvarint(dst, f.b)
	dst = binary.AppendUvarint(dst, f.c)
	return binary.BigEndian.AppendUint32(dst, crc32.Checksum(dst[start:], bulkCRC))
}

// appendBulkRecord appends a whole record frame.
func appendBulkRecord(dst []byte, key string, record []byte, ttlMs uint64, memOnly bool) []byte {
	var flags byte
	if memOnly {
		flags = bulkFlagMemoryOnly
	}
	dst = appendBulkHeader(dst, bulkFrame{typ: bulkFrameRecord, flags: flags, a: uint64(len(key)), b: uint64(len(record)), c: ttlMs})
	start := len(dst)
	dst = append(dst, key...)
	dst = append(dst, record...)
	return binary.BigEndian.AppendUint32(dst, crc32.Checksum(dst[start:], bulkCRC))
}

// bulkReader reads frames from a bulk stream.
type bulkReader struct {
	r      *bufio.Reader
	header [bulkMaxHeaderBytes]byte
	buf    []byte
}

func newBulkReader(r io.Reader) *bulkReader {
	return &bulkReader{r: bufio.NewReaderSize(r, 64<<10)}
}

// readMagic checks the stream's first bytes.
func (br *bulkReader) readMagic() error {
	var magic [len(peerBulkMagic)]byte
	if _, err := io.ReadFull(br.r, magic[:]); err != nil {
		return err
	}
	if string(magic[:]) != peerBulkMagic {
		return errBulkMagic
	}
	return nil
}

// next reads the next frame header. io.EOF at a frame boundary means the
// stream ended there; any other error ends the stream.
func (br *bulkReader) next() (bulkFrame, error) {
	var f bulkFrame
	n := 0
	readByte := func() (byte, error) {
		b, err := br.r.ReadByte()
		if err != nil {
			return 0, err
		}
		if n >= len(br.header) {
			return 0, errBulkHeader
		}
		br.header[n] = b
		n++
		return b, nil
	}
	typ, err := readByte()
	if err != nil {
		return f, err // io.EOF here is a clean frame boundary
	}
	flags, err := readByte()
	if err != nil {
		return f, unexpected(err)
	}
	var vals [3]uint64
	for i := range vals {
		v, err := binary.ReadUvarint(byteReaderFunc(readByte))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return f, io.ErrUnexpectedEOF
			}
			return f, errBulkHeader
		}
		vals[i] = v
	}
	var sum [4]byte
	if _, err := io.ReadFull(br.r, sum[:]); err != nil {
		return f, unexpected(err)
	}
	if binary.BigEndian.Uint32(sum[:]) != crc32.Checksum(br.header[:n], bulkCRC) {
		return f, errBulkHeader
	}
	f = bulkFrame{typ: typ, flags: flags, a: vals[0], b: vals[1], c: vals[2]}
	switch typ {
	case bulkFramePlan:
	case bulkFrameEnd:
		if flags >= bulkEndCount {
			return f, errBulkHeader
		}
	case bulkFrameRecord:
		if f.a == 0 || f.a > bulkMaxKeyBytes || f.b > bulkMaxRecordBytes || f.flags&^bulkFlagMemoryOnly != 0 {
			return f, errBulkHeader
		}
	default:
		return f, errBulkHeader
	}
	return f, nil
}

// skipPayload discards a record frame's payload.
func (br *bulkReader) skipPayload(f bulkFrame) error {
	_, err := io.CopyN(io.Discard, br.r, int64(f.a+f.b+4))
	return unexpected(err)
}

// payload reads a record frame's key and record. The record aliases the
// reader's buffer until the next call; errBulkRecordCorrupt means the payload
// checksum failed and the stream can go on.
func (br *bulkReader) payload(f bulkFrame) (string, []byte, error) {
	need := int(f.a + f.b + 4)
	if cap(br.buf) < need {
		br.buf = make([]byte, need)
	}
	buf := br.buf[:need]
	if _, err := io.ReadFull(br.r, buf); err != nil {
		return "", nil, unexpected(err)
	}
	body := buf[:need-4]
	if binary.BigEndian.Uint32(buf[need-4:]) != crc32.Checksum(body, bulkCRC) {
		return "", nil, errBulkRecordCorrupt
	}
	// A large buffer is not kept for the rest of the stream.
	if cap(br.buf) > 8<<20 {
		defer func() { br.buf = nil }()
	}
	return string(body[:f.a]), body[f.a:], nil
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

type byteReaderFunc func() (byte, error)

func (f byteReaderFunc) ReadByte() (byte, error) { return f() }

func bulkEndName(outcome byte) string {
	if outcome < bulkEndCount {
		return bulkEndNames[outcome]
	}
	return fmt.Sprintf("unknown(%d)", outcome)
}
