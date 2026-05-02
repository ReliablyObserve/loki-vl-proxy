package cache

import (
	"bytes"
	gzip "github.com/klauspost/compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

func decodeGzipPeerBody(t *testing.T, body []byte) []byte {
	t.Helper()

	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer gr.Close()
	decoded, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	return decoded
}

func TestPeerHelperFunctions(t *testing.T) {
	t.Run("request timeout", func(t *testing.T) {
		var pc *PeerCache
		if got := pc.RequestTimeout(); got != 0 {
			t.Fatalf("expected nil peer cache timeout 0, got %s", got)
		}

		pc = &PeerCache{}
		if got := pc.RequestTimeout(); got != 0 {
			t.Fatalf("expected nil client timeout 0, got %s", got)
		}

		pc = NewPeerCache(PeerConfig{SelfAddr: "self", Timeout: 3 * time.Second})
		defer pc.Close()
		if got := pc.RequestTimeout(); got != 3*time.Second {
			t.Fatalf("expected request timeout 3s, got %s", got)
		}
	})

	t.Run("classify peer fetch error", func(t *testing.T) {
		if got := classifyPeerFetchError(nil); got != "" {
			t.Fatalf("expected empty reason for nil error, got %q", got)
		}
		if got := classifyPeerFetchError(context.DeadlineExceeded); got != "timeout" {
			t.Fatalf("expected deadline to map to timeout, got %q", got)
		}
		if got := classifyPeerFetchError(timeoutNetError{}); got != "timeout" {
			t.Fatalf("expected timeout net error to map to timeout, got %q", got)
		}
		if got := classifyPeerFetchError(errors.New("boom")); got != "transport" {
			t.Fatalf("expected generic error to map to transport, got %q", got)
		}
	})

	t.Run("read peer body limited", func(t *testing.T) {
		body, err := readPeerBodyLimited(strings.NewReader("hello"), 0)
		if err != nil {
			t.Fatalf("unexpected unlimited read error: %v", err)
		}
		if string(body) != "hello" {
			t.Fatalf("unexpected unlimited body %q", string(body))
		}

		if _, err := readPeerBodyLimited(strings.NewReader("toolong"), 3); err == nil {
			t.Fatal("expected limit error")
		}
	})

	t.Run("decode peer response body", func(t *testing.T) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write([]byte("hello")); err != nil {
			t.Fatalf("write gzip payload: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close gzip payload: %v", err)
		}

		decoded, err := decodePeerResponseBody("gzip", buf.Bytes(), 1024)
		if err != nil {
			t.Fatalf("decode gzip payload: %v", err)
		}
		if string(decoded) != "hello" {
			t.Fatalf("unexpected decoded gzip payload %q", string(decoded))
		}

		identity, err := decodePeerResponseBody("br", []byte("raw"), 1024)
		if err != nil {
			t.Fatalf("unexpected unknown-encoding error: %v", err)
		}
		if string(identity) != "raw" {
			t.Fatalf("unexpected unknown-encoding fallback %q", string(identity))
		}

		if _, err := decodePeerResponseBody("gzip", []byte("bad-gzip"), 1024); err == nil {
			t.Fatal("expected invalid gzip to fail")
		}
	})
}

func TestWritePeerEncodedResponse_AdditionalCoverage(t *testing.T) {
	t.Run("gzip branch", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/_cache/get?key=test", nil)
		req.Header.Set("Accept-Encoding", "gzip")

		payload := []byte(strings.Repeat("x", 2048))
		writePeerEncodedResponse(recorder, req, payload)

		if got := recorder.Header().Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("expected gzip content encoding, got %q", got)
		}
		if got := decodeGzipPeerBody(t, recorder.Body.Bytes()); !bytes.Equal(got, payload) {
			t.Fatalf("unexpected decoded payload len=%d want=%d", len(got), len(payload))
		}
	})

	t.Run("identity fallback for small body", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/_cache/get?key=test", nil)
		req.Header.Set("Accept-Encoding", "gzip")

		writePeerEncodedResponse(recorder, req, []byte("tiny"))

		if got := recorder.Header().Get("Content-Encoding"); got != "" {
			t.Fatalf("expected identity response, got %q", got)
		}
		if got := recorder.Body.String(); got != "tiny" {
			t.Fatalf("unexpected identity body %q", got)
		}
	})
}

func TestPeerReadAheadHelpers(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:              "self",
		ReadAheadEnabled:      true,
		ReadAheadInterval:     20 * time.Millisecond,
		ReadAheadJitter:       10 * time.Millisecond,
		ReadAheadErrorBackoff: 15 * time.Millisecond,
	})
	defer pc.Close()

	noJitter := NewPeerCache(PeerConfig{
		SelfAddr:          "self",
		ReadAheadEnabled:  true,
		ReadAheadInterval: 25 * time.Millisecond,
	})
	defer noJitter.Close()

	if got := noJitter.nextReadAheadDelay(); got != 25*time.Millisecond {
		t.Fatalf("expected exact interval without jitter, got %s", got)
	}

	if got := pc.nextReadAheadDelay(); got < 20*time.Millisecond || got > 30*time.Millisecond {
		t.Fatalf("expected jittered delay in [20ms,30ms], got %s", got)
	}

	pc.readAheadErrorStreak.Store(7)
	pc.readAheadBackoffUntilUnix.Store(time.Now().Add(time.Second).UnixNano())
	pc.applyReadAheadBackoff(0)
	if got := pc.readAheadErrorStreak.Load(); got != 0 {
		t.Fatalf("expected streak reset, got %d", got)
	}
	if got := pc.readAheadBackoffUntilUnix.Load(); got != 0 {
		t.Fatalf("expected backoff reset, got %d", got)
	}

	pc.applyReadAheadBackoff(1)
	firstUntil := time.Unix(0, pc.readAheadBackoffUntilUnix.Load())
	if time.Until(firstUntil) <= 0 {
		t.Fatal("expected positive backoff window")
	}
	for range 5 {
		pc.applyReadAheadBackoff(1)
	}
	if got := pc.readAheadErrorStreak.Load(); got < 2 {
		t.Fatalf("expected error streak to increase, got %d", got)
	}
}

func TestPeerCache_WriteThroughAndReadAheadBranches(t *testing.T) {
	t.Run("set with ttl pushes to remote owner", func(t *testing.T) {
		localCache := New(60*time.Second, 1000)
		defer localCache.Close()

		type pushResult struct {
			key  string
			ttl  string
			body []byte
		}
		var (
			mu     sync.Mutex
			pushed pushResult
		)
		peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			pushed = pushResult{
				key:  r.URL.Query().Get("key"),
				ttl:  r.URL.Query().Get("ttl_ms"),
				body: append([]byte(nil), body...),
			}
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer peerServer.Close()

		pc := NewPeerCache(PeerConfig{
			SelfAddr:           "self:3100",
			DiscoveryType:      "static",
			StaticPeers:        peerServer.Listener.Addr().String(),
			Timeout:            time.Second,
			WriteThrough:       true,
			WriteThroughMinTTL: 10 * time.Second,
		})
		defer pc.Close()
		localCache.SetL3(pc)

		var targetKey string
		for i := 0; i < 100; i++ {
			key := "write-through-key-" + strconv.Itoa(i)
			pc.mu.RLock()
			owner := pc.ring.get(key)
			pc.mu.RUnlock()
			if owner == peerServer.Listener.Addr().String() {
				targetKey = key
				break
			}
		}
		if targetKey == "" {
			t.Fatal("no key mapped to peer")
		}

		localCache.SetWithTTL(targetKey, []byte("payload"), 30*time.Second)
		requireEventually(t, time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return pushed.key == targetKey
		})

		mu.Lock()
		got := pushed
		mu.Unlock()
		if string(got.body) != "payload" {
			t.Fatalf("unexpected pushed body %q", string(got.body))
		}
		if got.ttl != "30000" {
			t.Fatalf("unexpected pushed ttl %q", got.ttl)
		}
		if got := pc.WTPushes.Load(); got != 1 {
			t.Fatalf("expected write-through push count 1, got %d", got)
		}
	})

	t.Run("set with ttl uses compressed push for capable peers", func(t *testing.T) {
		localCache := New(60*time.Second, 1000)
		defer localCache.Close()

		type pushResult struct {
			key      string
			ttl      string
			body     []byte
			encoding string
		}
		var (
			mu     sync.Mutex
			pushed pushResult
		)
		peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			pushed = pushResult{
				key:      r.URL.Query().Get("key"),
				ttl:      r.URL.Query().Get("ttl_ms"),
				body:     append([]byte(nil), body...),
				encoding: r.Header.Get("Content-Encoding"),
			}
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer peerServer.Close()

		pc := NewPeerCache(PeerConfig{
			SelfAddr:           "self:3100",
			DiscoveryType:      "static",
			StaticPeers:        peerServer.Listener.Addr().String(),
			Timeout:            time.Second,
			WriteThrough:       true,
			WriteThroughMinTTL: 10 * time.Second,
		})
		defer pc.Close()
		localCache.SetL3(pc)

		var targetKey string
		for i := 0; i < 100; i++ {
			key := "write-through-compressed-key-" + strconv.Itoa(i)
			pc.mu.RLock()
			owner := pc.ring.get(key)
			pc.mu.RUnlock()
			if owner == peerServer.Listener.Addr().String() {
				targetKey = key
				break
			}
		}
		if targetKey == "" {
			t.Fatal("no key mapped to peer")
		}

		pc.recordPeerSetEncoding(peerServer.Listener.Addr().String(), "zstd")
		payload := []byte(strings.Repeat("payload-", 256))
		localCache.SetWithTTL(targetKey, payload, 30*time.Second)
		requireEventually(t, time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return pushed.key == targetKey
		})

		mu.Lock()
		got := pushed
		mu.Unlock()
		if got.encoding != "zstd" {
			t.Fatalf("expected zstd write-through encoding, got %q", got.encoding)
		}
		decoded, err := decodePeerResponseBody(got.encoding, got.body, maxPeerSetBodyBytes)
		if err != nil {
			t.Fatalf("decode pushed body: %v", err)
		}
		if !bytes.Equal(decoded, payload) {
			t.Fatalf("unexpected pushed payload len=%d want=%d", len(decoded), len(payload))
		}
	})

	t.Run("prefetch hot key additional branches", func(t *testing.T) {
		pc := NewPeerCache(PeerConfig{
			SelfAddr:                "self",
			ReadAheadEnabled:        true,
			ReadAheadMinTTL:         10 * time.Second,
			ReadAheadMaxObjectBytes: 4,
		})
		defer pc.Close()

		if pc.prefetchHotKey(nil, hotIndexEntry{Key: "k"}) {
			t.Fatal("expected nil local cache to fail")
		}
		local := New(60*time.Second, 1000)
		defer local.Close()
		local.SetWithTTL("warm", []byte("ok"), 30*time.Second)
		if !pc.prefetchHotKey(local, hotIndexEntry{Key: "warm"}) {
			t.Fatal("expected already-warm local key to succeed")
		}
	})

	t.Run("select read ahead candidates enforces tenant fairness and size budget", func(t *testing.T) {
		pc := NewPeerCache(PeerConfig{
			SelfAddr:                 "self",
			ReadAheadEnabled:         true,
			ReadAheadMaxKeys:         2,
			ReadAheadMaxBytes:        10,
			ReadAheadTenantFairShare: 50,
			ReadAheadMinTTL:          time.Second,
		})
		defer pc.Close()

		selected := pc.selectReadAheadCandidates([]hotIndexEntry{
			{Key: "a", Tenant: "tenant-a", Score: 9, SizeBytes: 3, RemainingTTLMS: 20_000},
			{Key: "b", Tenant: "tenant-a", Score: 8, SizeBytes: 3, RemainingTTLMS: 20_000},
			{Key: "c", Tenant: "tenant-b", Score: 7, SizeBytes: 20, RemainingTTLMS: 20_000},
			{Key: "d", Tenant: "tenant-b", Score: 6, SizeBytes: 3, RemainingTTLMS: 20_000},
		})

		if len(selected) != 2 {
			t.Fatalf("expected 2 selected keys, got %d", len(selected))
		}
		if selected[0].Key != "a" || selected[1].Key != "d" {
			t.Fatalf("unexpected selected keys %+v", selected)
		}
		if got := pc.RATenant.Load(); got == 0 {
			t.Fatal("expected tenant fairness counter to increase")
		}
		if got := pc.RABudget.Load(); got == 0 {
			t.Fatal("expected budget counter to increase")
		}
	})
}

func TestFetchPeerHotIndex_FailurePaths(t *testing.T) {
	t.Run("status error", func(t *testing.T) {
		peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusBadGateway)
		}))
		defer peerServer.Close()

		pc := NewPeerCache(PeerConfig{SelfAddr: "self", Timeout: time.Second})
		defer pc.Close()

		if _, err := pc.fetchPeerHotIndex(peerServer.Listener.Addr().String(), 10); err == nil {
			t.Fatal("expected fetchPeerHotIndex to fail on 502")
		}
		breaker, ok := pc.breakers.Load(peerServer.Listener.Addr().String())
		if !ok {
			t.Fatal("expected peer breaker to be created")
		}
		if got := breaker.(*peerBreaker).failures.Load(); got == 0 {
			t.Fatal("expected breaker failures to increase")
		}
	})

	t.Run("decode error", func(t *testing.T) {
		peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write([]byte("not-gzip"))
		}))
		defer peerServer.Close()

		pc := NewPeerCache(PeerConfig{SelfAddr: "self", Timeout: time.Second})
		defer pc.Close()

		if _, err := pc.fetchPeerHotIndex(peerServer.Listener.Addr().String(), 10); err == nil {
			t.Fatal("expected fetchPeerHotIndex to fail on decode error")
		}
	})
}

func TestPeerServeHTTP_AdditionalBranches(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	pc := NewPeerCache(PeerConfig{SelfAddr: "self"})
	defer pc.Close()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/_cache/get?key=test", nil)
	pc.ServeHTTP(recorder, req, localCache)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for unsupported method, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Allow"); got != "GET, POST" {
		t.Fatalf("unexpected Allow header %q", got)
	}
}
