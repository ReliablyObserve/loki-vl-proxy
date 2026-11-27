package cache

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

type wireRecord struct {
	key     string
	value   []byte
	ttl     time.Duration
	memOnly bool
}

func testRecords(n int) []wireRecord {
	out := make([]wireRecord, n)
	for i := range out {
		out[i] = wireRecord{key: fmt.Sprintf("wire-%d", i), value: bytes.Repeat([]byte{byte('a' + i)}, 100+i), ttl: 10 * time.Minute}
	}
	return out
}

// buildStream returns a stream of the records and, for each record, the
// offset its frame starts at.
func buildStream(t testing.TB, recs []wireRecord, end bool) ([]byte, []int) {
	t.Helper()
	buf := []byte(peerBulkMagic)
	var planned uint64
	encoded := make([][]byte, len(recs))
	for i, r := range recs {
		var err error
		encoded[i], err = encodeDiskRecord(diskEntry{Value: r.value, ExpiresAt: time.Now().Add(r.ttl).UnixNano()}, diskCodecS2)
		if err != nil {
			t.Fatal(err)
		}
		planned += uint64(len(r.key) + len(encoded[i]))
	}
	buf = appendBulkHeader(buf, bulkFrame{typ: bulkFramePlan, a: uint64(len(recs)), b: planned})
	offsets := make([]int, len(recs))
	for i, r := range recs {
		offsets[i] = len(buf)
		buf = appendBulkRecord(buf, r.key, encoded[i], uint64(r.ttl.Milliseconds()), r.memOnly)
	}
	if end {
		buf = appendBulkHeader(buf, bulkFrame{typ: bulkFrameEnd, flags: bulkEndDone, a: uint64(len(recs)), b: planned})
	}
	return buf, offsets
}

func newTestRestore(c *Cache, scope string) *peerRestore {
	r := &peerRestore{c: c, maxEntry: int64(c.maxBytes / 10), budget: int64(c.maxBytes / 2), scope: scope}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.generation, _ = c.diskPromotionGeneration()
	r.progress = RestoreProgress{Source: RestoreSourcePeer, Outcome: RestoreRunning, Started: time.Now(), BudgetBytes: r.budget}
	return r
}

func receiveStream(t testing.TB, stream []byte, scope string) (*Cache, string, RestoreProgress) {
	t.Helper()
	c := NewWithMaxBytes(time.Hour, 1000, 16<<20)
	t.Cleanup(c.Close)
	r := newTestRestore(c, scope)
	defer r.cancel()
	var res PeerRestorePeer
	outcome := r.receive(bytes.NewReader(stream), &res)
	return c, outcome, r.progress
}

// conformance: cache-restart-handover
func TestPeerBulkStream_RoundTrip(t *testing.T) {
	recs := testRecords(5)
	recs[1].memOnly = true
	stream, _ := buildStream(t, recs, true)
	c, outcome, p := receiveStream(t, stream, "scope")
	if outcome != PeerRestoreDone || p.RestoredEntries != 5 {
		t.Fatalf("outcome %q restored %d", outcome, p.RestoredEntries)
	}
	for _, r := range recs {
		v, e, ok := inMemory(c, r.key)
		if !ok || !bytes.Equal(v, r.value) || e.memOnly != r.memOnly {
			t.Fatalf("%s: %v %q memOnly %v", r.key, ok, v, e.memOnly)
		}
	}
	checkProgressSums(t, p)
}

// A record whose payload checksum fails is skipped and the stream goes on; a
// record whose own checksum fails is skipped too.
//
// conformance: cache-restart-handover
func TestPeerBulkStream_CorruptRecordIsSkipped(t *testing.T) {
	recs := testRecords(4)
	stream, offsets := buildStream(t, recs, true)
	// Flip a value byte of record 1 (frame checksum fails).
	stream[offsets[2]-6] ^= 0xff
	c, outcome, p := receiveStream(t, stream, "")
	if outcome != PeerRestoreDone || p.RestoredEntries != 3 || p.Skipped[RestoreSkipUnreadable] != 1 {
		t.Fatalf("outcome %q restored %d skipped %v", outcome, p.RestoredEntries, p.Skipped)
	}
	if _, _, ok := inMemory(c, recs[1].key); ok {
		t.Fatal("the corrupt record was restored")
	}
	checkProgressSums(t, p)

	// A record whose frame checksum is right but whose disk record is not.
	bad, err := encodeDiskRecord(diskEntry{Value: []byte("value"), ExpiresAt: time.Now().Add(time.Hour).UnixNano()}, diskCodecRaw)
	if err != nil {
		t.Fatal(err)
	}
	bad[len(bad)-1] ^= 0xff
	stream2 := appendBulkHeader([]byte(peerBulkMagic), bulkFrame{typ: bulkFramePlan, a: 2, b: 100})
	stream2 = appendBulkRecord(stream2, "bad", bad, 60_000, false)
	good, _ := encodeDiskRecord(diskEntry{Value: []byte("good"), ExpiresAt: time.Now().Add(time.Hour).UnixNano()}, diskCodecRaw)
	stream2 = appendBulkRecord(stream2, "good", good, 60_000, false)
	stream2 = appendBulkHeader(stream2, bulkFrame{typ: bulkFrameEnd, a: 2, b: 100})
	c, outcome, p = receiveStream(t, stream2, "")
	if _, _, ok := inMemory(c, "bad"); ok || outcome != PeerRestoreDone || p.RestoredEntries != 1 {
		t.Fatalf("outcome %q restored %d", outcome, p.RestoredEntries)
	}
}

// A corrupt frame header ends the stream: the records before it stay, the
// rest of the plan is counted unreadable.
//
// conformance: cache-restart-handover
func TestPeerBulkStream_CorruptHeaderEndsTheStream(t *testing.T) {
	recs := testRecords(4)
	stream, offsets := buildStream(t, recs, true)
	stream[offsets[2]+2] ^= 0x01 // the key length of record 2
	c, outcome, p := receiveStream(t, stream, "")
	if outcome != PeerRestoreCorrupt || p.RestoredEntries != 2 || p.Skipped[RestoreSkipUnreadable] != 2 {
		t.Fatalf("outcome %q restored %d skipped %v", outcome, p.RestoredEntries, p.Skipped)
	}
	for _, r := range recs[:2] {
		if _, _, ok := inMemory(c, r.key); !ok {
			t.Fatalf("%s before the corrupt header was dropped", r.key)
		}
	}
	checkProgressSums(t, p)
}

// A stream cut anywhere keeps the whole records before the cut and reports
// truncated, never done.
//
// conformance: cache-restart-handover
func TestPeerBulkStream_TruncatedKeepsWhatArrived(t *testing.T) {
	recs := testRecords(4)
	stream, offsets := buildStream(t, recs, true)
	for _, cut := range []int{offsets[2], offsets[2] + 3, offsets[3] - 1, len(stream) - 1} {
		c, outcome, p := receiveStream(t, stream[:cut], "")
		ends := append(append([]int(nil), offsets[1:]...), endFrameStart(t, stream))
		whole := 0
		for _, end := range ends {
			if end <= cut {
				whole++
			}
		}
		if outcome != PeerRestoreTruncated || p.RestoredEntries != int64(whole) {
			t.Fatalf("cut %d: outcome %q restored %d, want truncated and %d", cut, outcome, p.RestoredEntries, whole)
		}
		if _, _, ok := inMemory(c, recs[0].key); !ok {
			t.Fatalf("cut %d dropped the first record", cut)
		}
		checkProgressSums(t, p)
	}
	if _, outcome, _ := receiveStream(t, stream[:4], ""); outcome != PeerRestoreTruncated {
		t.Fatalf("a cut magic: %q", outcome)
	}
	bad := append([]byte("NOTBULK!"), stream[8:]...)
	if _, outcome, _ := receiveStream(t, bad, ""); outcome != PeerRestoreCorrupt {
		t.Fatalf("a wrong magic: %q", outcome)
	}
}

// Records too large for one memory entry are skipped unread, near-expiry
// ones and memory-only ones without a scope are not admitted.
//
// conformance: cache-restart-handover
func TestPeerBulkStream_SkipsTooLargeExpiringAndOtherScope(t *testing.T) {
	recs := testRecords(4)
	recs[0].value = bytes.Repeat([]byte("x"), 2<<20) // above 16 MiB / 10
	recs[1].ttl = 2 * time.Second
	recs[2].memOnly = true
	stream, _ := buildStream(t, recs, true)
	_, outcome, p := receiveStream(t, stream, "")
	if outcome != PeerRestoreDone || p.RestoredEntries != 1 || p.Skipped[RestoreSkipTooLarge] != 1 || p.Skipped[RestoreSkipExpired] != 1 || p.Skipped[RestoreSkipOtherScope] != 1 {
		t.Fatalf("outcome %q restored %d skipped %v", outcome, p.RestoredEntries, p.Skipped)
	}
	checkProgressSums(t, p)
}

// A header that claims an oversized key or record, or an unknown frame type,
// is refused before any payload is read.
//
// conformance: cache-restart-handover
func TestPeerBulkStream_HeaderBounds(t *testing.T) {
	for _, f := range []bulkFrame{
		{typ: bulkFrameRecord, a: bulkMaxKeyBytes + 1, b: 1},
		{typ: bulkFrameRecord, a: 1, b: bulkMaxRecordBytes + 1},
		{typ: bulkFrameRecord, a: 0, b: 1},
		{typ: bulkFrameRecord, flags: 0x80, a: 1, b: 1},
		{typ: 9},
		{typ: bulkFrameEnd, flags: bulkEndCount},
	} {
		stream := appendBulkHeader([]byte(peerBulkMagic), bulkFrame{typ: bulkFramePlan, a: 1, b: 1})
		stream = appendBulkHeader(stream, f)
		stream = append(stream, make([]byte, 64)...)
		if _, outcome, _ := receiveStream(t, stream, ""); outcome != PeerRestoreCorrupt {
			t.Fatalf("%+v: outcome %q", f, outcome)
		}
	}
}

// Any input ends with an outcome, bounded work and progress that adds up.
func FuzzPeerBulkStream(f *testing.F) {
	recs := testRecords(3)
	recs[1].memOnly = true
	stream, offsets := buildStream(f, recs, true)
	f.Add(stream)
	f.Add(stream[:offsets[1]+5])
	f.Add([]byte(peerBulkMagic))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		c := NewWithMaxBytes(time.Hour, 100, 1<<20)
		defer c.Close()
		r := newTestRestore(c, "scope")
		defer r.cancel()
		var res PeerRestorePeer
		outcome := r.receive(bytes.NewReader(data), &res)
		if outcome == "" {
			t.Fatal("no outcome")
		}
		p := r.progress
		if p.ProcessedEntries > 0 && p.ProcessedEntries != p.RestoredEntries+p.SkippedEntries() {
			t.Fatalf("processed %d, restored %d + skipped %d", p.ProcessedEntries, p.RestoredEntries, p.SkippedEntries())
		}
		if c.curBytes > c.maxBytes {
			t.Fatalf("memory tier over its bytes: %d", c.curBytes)
		}
	})
}

// endFrameStart is where a stream's end frame begins.
func endFrameStart(t testing.TB, stream []byte) int {
	t.Helper()
	end := appendBulkHeader(nil, bulkFrame{typ: bulkFrameEnd, flags: bulkEndDone, a: 4})
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i] == bulkFrameEnd && bytes.HasPrefix(stream[i:], end[:2]) {
			return i
		}
	}
	t.Fatal("no end frame")
	return 0
}
