package cache

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPeerTTLMillisPreservesScannerInputs(t *testing.T) {
	for _, raw := range []string{"", "0", "-1", "+42", "123tail", " 123", "123 ", "1_000", "0x10", "9223372036854775807", "9223372036854775808", "-9223372036854775809", "invalid"} {
		var want int64
		_, err := fmt.Sscanf(raw, "%d", &want)
		got, ok := parsePeerTTLMillis(raw)
		if ok != (err == nil) || (ok && got != want) {
			t.Errorf("input %q: got (%d,%v), scanner (%d,%v)", raw, got, ok, want, err)
		}
	}
}

func TestPeerFetchEscapedKeyLegacyTTLHeader(t *testing.T) {
	key := "query_range:tenant +/?&=雪"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("key"); got != key {
			t.Errorf("key %q, want %q", got, key)
		}
		w.Header().Set("X-Cache-TTL-Ms", "60000tail")
		_, _ = w.Write([]byte("legacy peer"))
	}))
	defer srv.Close()
	pc := NewPeerCache(PeerConfig{Timeout: time.Second})
	defer pc.Close()
	body, ttl, _, status := pc.fetchFrom(strings.TrimPrefix(srv.URL, "http://"), key)
	if status != http.StatusOK || string(body) != "legacy peer" || ttl != time.Minute {
		t.Fatalf("fetch = (%q,%s,%d)", body, ttl, status)
	}
}

// These tests run sequentially: the scratch channel is shared by every peer cache.
func emptyPeerResponseScratch() {
	for {
		select {
		case <-peerEncodedScratch:
		default:
			return
		}
	}
}

type peerScratchBlockingWriter struct {
	header  http.Header
	entered chan []byte
	release chan struct{}
}

func (w *peerScratchBlockingWriter) Header() http.Header { return w.header }
func (w *peerScratchBlockingWriter) WriteHeader(int)     {}
func (w *peerScratchBlockingWriter) Write(b []byte) (int, error) {
	w.entered <- b
	<-w.release
	return len(b), nil
}

func TestPeerResponseScratchOwnedUntilWriteCompletes(t *testing.T) {
	emptyPeerResponseScratch()
	defer emptyPeerResponseScratch()
	for i := 0; i < cap(peerEncodedScratch); i++ {
		peerEncodedScratch <- make([]byte, 0, 4096)
	}
	w := &peerScratchBlockingWriter{header: make(http.Header), entered: make(chan []byte, 1), release: make(chan struct{})}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "zstd")
	done := make(chan struct{})
	defer func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
		<-done
	}()
	go func() { writePeerEncodedResponse(w, req, bytes.Repeat([]byte("first response"), 1024)); close(done) }()
	var encoded []byte
	select {
	case encoded = <-w.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not start")
	}
	snapshot := bytes.Clone(encoded)
	if got := len(peerEncodedScratch); got != cap(peerEncodedScratch)-1 {
		t.Errorf("scratch returned during Write: retained %d", got)
	}
	second := httptest.NewRecorder()
	writePeerEncodedResponse(second, req, bytes.Repeat([]byte("second response"), 1024))
	if !bytes.Equal(encoded, snapshot) {
		t.Error("in-flight encoded response was overwritten")
	}
	close(w.release)
	<-done
	decoded, err := decodePeerResponseBody("zstd", snapshot, maxPeerDecodedBodyBytes)
	if err != nil || !bytes.Equal(decoded, bytes.Repeat([]byte("first response"), 1024)) {
		t.Fatalf("first response corrupted: %v", err)
	}
	if got := len(peerEncodedScratch); got != cap(peerEncodedScratch) {
		t.Fatalf("scratch not returned after Write: %d", got)
	}
}

func TestPeerResponseScratchRetainedBudget(t *testing.T) {
	emptyPeerResponseScratch()
	defer emptyPeerResponseScratch()
	if cap(peerEncodedScratch)*peerEncodedScratchMax > 2<<20 {
		t.Fatal("retained scratch exceeds 2MiB")
	}
	body := make([]byte, 512<<10)
	_, _ = rand.New(rand.NewSource(17)).Read(body)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "zstd")
	rec := httptest.NewRecorder()
	writePeerEncodedResponse(rec, req, body)
	decoded, err := decodePeerResponseBody(rec.Header().Get("Content-Encoding"), rec.Body.Bytes(), maxPeerDecodedBodyBytes)
	if err != nil || !bytes.Equal(body, decoded) {
		t.Fatalf("large response mismatch: %v", err)
	}
	if len(peerEncodedScratch) != 0 {
		t.Fatal("oversized encoded buffer retained")
	}
}

func FuzzPeerTTLMillisCompatibility(f *testing.F) {
	for _, raw := range []string{"0", "60000", "-1", "60000tail", " 12", "9223372036854775808", "\x00", "雪"} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		var want int64
		_, err := fmt.Sscanf(raw, "%d", &want)
		got, ok := parsePeerTTLMillis(raw)
		if ok != (err == nil) || (ok && got != want) {
			t.Fatalf("%q: got (%d,%v), scanner (%d,%v)", raw, got, ok, want, err)
		}
	})
}

func TestPeerSetPreservesLegacyTTLParsing(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"60000tail", time.Minute},
		{"invalid", 30 * time.Second},
		{"-1000", 30 * time.Second},
		{"9223372036854775808", 30 * time.Second},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			local := New(time.Minute, 10)
			defer local.Close()
			pc := NewPeerCache(PeerConfig{})
			defer pc.Close()
			req := httptest.NewRequest(http.MethodPost, "/_cache/set?key=legacy&ttl_ms="+url.QueryEscape(tc.raw), strings.NewReader("value"))
			rec := httptest.NewRecorder()
			pc.ServeHTTP(rec, req, local)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("status %d", rec.Code)
			}
			value, ttl, ok := local.GetWithTTL("legacy")
			if !ok || string(value) != "value" || ttl > tc.want || ttl < tc.want-time.Second {
				t.Fatalf("stored (%q,%s,%v), want TTL %s", value, ttl, ok, tc.want)
			}
		})
	}
}
