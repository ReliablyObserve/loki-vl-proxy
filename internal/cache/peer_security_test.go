package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// A parseable positive int64 count of milliseconds can still overflow a
// time.Duration. Reject it rather than converting the negative result to the
// requester's default 30-second shadow TTL or accepting a wrapped positive TTL.
func TestPeerFetchRejectsTTLDurationOverflow(t *testing.T) {
	for _, raw := range []string{"9223372036855", "18446744133710", "9223372036854775807", "-9223372036855", "-18446744133710"} {
		t.Run(raw, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Cache-Ttl-Ms", raw)
				_, _ = w.Write([]byte("must not become a cache hit"))
			}))
			defer server.Close()
			peer := NewPeerCache(PeerConfig{Timeout: time.Second})
			defer peer.Close()
			body, ttl, _, status := peer.fetchFrom(strings.TrimPrefix(server.URL, "http://"), "tenant-isolated-key")
			if status != 0 || len(body) != 0 || ttl != 0 {
				t.Fatalf("overflow TTL accepted: status=%d ttl=%s value=%q", status, ttl, body)
			}
		})
	}
}

func BenchmarkPeerTTLMalformedInput(b *testing.B) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{"canonical", "60000"},
		{"overflow4KiB", strings.Repeat("9", 4<<10)},
		{"overflow1MiB", strings.Repeat("9", 1<<20)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = parsePeerTTLMillis(tc.input)
			}
		})
	}
}

func TestPeerSetRejectsTTLDurationOverflow(t *testing.T) {
	for _, raw := range []string{"9223372036855", "18446744133710", "9223372036854775807"} {
		t.Run(raw, func(t *testing.T) {
			local := New(time.Minute, 10)
			defer local.Close()
			peer := NewPeerCache(PeerConfig{})
			defer peer.Close()
			request := httptest.NewRequest(http.MethodPost, "/_cache/set?key=overflow&ttl_ms="+url.QueryEscape(raw), strings.NewReader("must not be cached"))
			response := httptest.NewRecorder()
			peer.ServeHTTP(response, request, local)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("overflow TTL status=%d, want400", response.Code)
			}
			if remaining, err := io.ReadAll(request.Body); err != nil || string(remaining) != "must not be cached" {
				t.Fatal("invalid TTL caused body buffering before rejection")
			}
			if _, _, ok := local.GetWithTTL("overflow"); ok {
				t.Fatal("overflow TTL value was cached")
			}
		})
	}
}

func TestPeerFetchAcceptsMaximumRepresentableTTL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache-Ttl-Ms", "9223372036854")
		_, _ = w.Write([]byte("maximum safe duration"))
	}))
	defer server.Close()
	peer := NewPeerCache(PeerConfig{Timeout: time.Second})
	defer peer.Close()
	body, ttl, _, status := peer.fetchFrom(strings.TrimPrefix(server.URL, "http://"), "boundary")
	if status != http.StatusOK || string(body) != "maximum safe duration" || ttl != 9223372036854000000 {
		t.Fatalf("valid maximum TTL rejected: status=%d ttl=%s value=%q", status, ttl, body)
	}
}

func TestPeerFetchRejectsOverlongTTLHeader(t *testing.T) {
	for name, raw := range map[string]string{
		"zeros":      strings.Repeat("0", 129),
		"whitespace": "0" + strings.Repeat(" ", 129) + "tail",
		"nonnumeric": strings.Repeat("x", 129),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Cache-Ttl-Ms", raw)
				_, _ = w.Write([]byte("must not become a cache hit"))
			}))
			defer server.Close()
			peer := NewPeerCache(PeerConfig{Timeout: time.Second})
			defer peer.Close()
			body, ttl, _, status := peer.fetchFrom(strings.TrimPrefix(server.URL, "http://"), "overlong")
			if status != 0 || len(body) != 0 || ttl != 0 {
				t.Fatalf("overlong header accepted: status=%d ttl=%s", status, ttl)
			}
		})
	}
}

func TestPeerSetRejectsOverlongTTLParameter(t *testing.T) {
	for name, raw := range map[string]string{
		"zeros":      strings.Repeat("0", 129),
		"whitespace": strings.Repeat(" ", 129),
		"nonnumeric": strings.Repeat("x", 129),
	} {
		t.Run(name, func(t *testing.T) {
			local := New(time.Minute, 10)
			defer local.Close()
			peer := NewPeerCache(PeerConfig{})
			defer peer.Close()
			request := httptest.NewRequest(http.MethodPost, "/_cache/set?key=overlong&ttl_ms="+url.QueryEscape(raw), strings.NewReader("must not be cached"))
			response := httptest.NewRecorder()
			peer.ServeHTTP(response, request, local)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("overlong TTL parameter status=%d", response.Code)
			}
			if remaining, err := io.ReadAll(request.Body); err != nil || string(remaining) != "must not be cached" {
				t.Fatal("overlong TTL caused body buffering before rejection")
			}
			if _, _, ok := local.GetWithTTL("overlong"); ok {
				t.Fatal("overlong TTL value was cached")
			}
		})
	}
}

func BenchmarkPeerSetRejectOversizedTTL(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"4KiB", 4 << 10}, {"1MiB", 1 << 20}} {
		b.Run(tc.name, func(b *testing.B) {
			local := New(time.Minute, 10)
			defer local.Close()
			peer := NewPeerCache(PeerConfig{})
			defer peer.Close()
			request := httptest.NewRequest(http.MethodPost, "/_cache/set?key=overlong&ttl_ms="+strings.Repeat("9", tc.size), strings.NewReader("must not be cached"))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				response := httptest.NewRecorder()
				peer.ServeHTTP(response, request, local)
				if response.Code != http.StatusBadRequest {
					b.Fatalf("status %d", response.Code)
				}
			}
		})
	}
}
