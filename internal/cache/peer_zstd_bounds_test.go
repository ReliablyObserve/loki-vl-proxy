package cache

import (
	"bytes"
	"math/rand"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func peerAllocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestDecodePeerResponseBodyRejectsOversizedZstdWindow(t *testing.T) {
	payload := bytes.Repeat([]byte("peer cache entry "), 64<<10) // ~1 MiB
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf, zstd.WithWindowSize(128<<20))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	frame := buf.Bytes()
	if len(frame) > 64<<10 {
		t.Fatalf("test frame should be small, got %d bytes", len(frame))
	}

	var decodeErr error
	allocated := peerAllocatedBytes(func() {
		_, decodeErr = decodePeerResponseBody("zstd", frame, maxPeerResponseBytes)
	})
	if decodeErr == nil {
		t.Fatal("a frame declaring a 128 MiB window must be rejected")
	}
	// The default decoder allocates the declared window (tens of MiB) before
	// the response cap is consulted; the bounded decoder never gets there.
	if limit := uint64(peerZstdMaxWindow); !raceDetectorEnabled && allocated > limit {
		t.Fatalf("rejecting the frame allocated %d bytes, want at most %d", allocated, limit)
	}
}

func TestDecodePeerResponseBodyZstdRoundTripsAtEveryEndpointCap(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for name, limit := range map[string]int64{
		"hot index": maxPeerHotIndexBytes,
		"set body":  maxPeerSetBodyBytes,
		"response":  maxPeerDecodedBodyBytes,
	} {
		t.Run(name, func(t *testing.T) {
			// Compressible but not trivially: random words from a small alphabet.
			body := make([]byte, limit)
			for i := range body {
				body[i] = "abcdefgh\n"[rng.Intn(9)]
			}
			frame, err := encodePeerRequestBody(body, "zstd")
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodePeerResponseBody("zstd", frame, limit)
			if err != nil {
				t.Fatalf("a frame from the peer encoder at the cap must decode: %v", err)
			}
			if !bytes.Equal(got, body) {
				t.Fatal("round trip changed the body")
			}
			if _, err := decodePeerResponseBody("zstd", frame, limit-1); err == nil {
				t.Fatal("a body one byte over the cap must be rejected")
			}
		})
	}
}
