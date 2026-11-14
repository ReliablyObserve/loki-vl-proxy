package cache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// clientPeer is a PeerCache whose only peer is the test server: its calls go to that server.
func clientPeer(t *testing.T, handler http.HandlerFunc) (*PeerCache, string) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   srv.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	t.Cleanup(pc.Close)
	return pc, srv.Listener.Addr().String()
}

func serverPeer(t *testing.T) *PeerCache {
	t.Helper()
	pc := NewPeerCache(PeerConfig{SelfAddr: "owner:3100"})
	t.Cleanup(pc.Close)
	return pc
}

func serve(pc *PeerCache, local *Cache, method, target string, body []byte, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	pc.ServeHTTP(rec, req, local)
	return rec
}

// What a replica asks a peer about its copies is answered without sending any value: the keys it holds with a usable
// TTL, and the others as absent.
// conformance: peer-cache-replication
func TestServeHasPresence(t *testing.T) {
	local := New(time.Hour, 100)
	t.Cleanup(local.Close)
	local.SetWithTTL("fresh", []byte("v"), time.Minute)
	local.SetWithTTL("expiring", []byte("v"), MinUsableTTL-time.Second)
	pc := serverPeer(t)

	rec := serve(pc, local, http.MethodGet, "/_cache/has?keys="+url.QueryEscape("fresh, expiring ,missing,,"), nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, content type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	var got map[string]PeerKeyPresence
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || !got["fresh"].OK || got["fresh"].TTLMs <= 50_000 || got["fresh"].TTLMs > 60_000 {
		t.Errorf("fresh key: %+v of %v", got["fresh"], got)
	}
	if got["expiring"].OK || got["missing"].OK {
		t.Errorf("a key about to expire or not held is absent: %v", got)
	}
}

// conformance: peer-cache-replication
func TestServeHasRefusals(t *testing.T) {
	local := New(time.Hour, 100)
	t.Cleanup(local.Close)
	pc := serverPeer(t)
	tooMany := make([]string, 201)
	for i := range tooMany {
		tooMany[i] = "k"
	}
	atLimit := tooMany[:200]
	for _, c := range []struct {
		name, method, target string
		code                 int
		allow, body          string
	}{
		{"post", http.MethodPost, "/_cache/has?keys=a", http.StatusMethodNotAllowed, "GET", "method not allowed"},
		{"no keys", http.MethodGet, "/_cache/has", http.StatusBadRequest, "", "missing keys"},
		{"blank keys", http.MethodGet, "/_cache/has?keys=%20", http.StatusBadRequest, "", "missing keys"},
		{"201 keys", http.MethodGet, "/_cache/has?keys=" + strings.Join(tooMany, ","), http.StatusBadRequest, "", "too many keys (max 200)"},
		{"200 keys", http.MethodGet, "/_cache/has?keys=" + strings.Join(atLimit, ","), http.StatusOK, "", ""},
		{"hot index post", http.MethodPost, "/_cache/hot", http.StatusMethodNotAllowed, "GET", "method not allowed"},
		{"get with no key", http.MethodGet, "/_cache/get", http.StatusBadRequest, "", "missing key"},
		{"delete", http.MethodDelete, "/_cache/get?key=a", http.StatusMethodNotAllowed, "GET, POST", "method not allowed"},
	} {
		rec := serve(pc, local, c.method, c.target, nil)
		if rec.Code != c.code || rec.Header().Get("Allow") != c.allow || !strings.Contains(rec.Body.String(), c.body) {
			t.Errorf("%s: %d allow %q body %q; want %d %q %q", c.name, rec.Code, rec.Header().Get("Allow"), rec.Body, c.code, c.allow, c.body)
		}
	}
}

// A peer's write is stored as a replica copy with the TTL it names (30 s when it names none), and a malformed write
// is refused with a 400.
// conformance: peer-cache-replication
func TestServeSetValidation(t *testing.T) {
	local := New(time.Hour, 100)
	t.Cleanup(local.Close)
	pc := serverPeer(t)
	long := strings.Repeat("9", maxPeerTTLTextBytes+1)
	for _, c := range []struct {
		name, target string
		body         []byte
		headers      []string
		code         int
		message      string
	}{
		{"no key", "/_cache/set", []byte("v"), nil, http.StatusBadRequest, "missing key"},
		{"ttl text too long", "/_cache/set?key=k&ttl_ms=" + long, []byte("v"), nil, http.StatusBadRequest, "cache TTL exceeds text limit"},
		{"ttl beyond a duration", "/_cache/set?key=k&ttl_ms=9223372036854775807", []byte("v"), nil, http.StatusBadRequest, "cache TTL exceeds duration limit"},
		{"empty value", "/_cache/set?key=k&ttl_ms=1000", nil, nil, http.StatusBadRequest, "empty value"},
		{"undecodable value", "/_cache/set?key=k&ttl_ms=1000", []byte("not gzip"), []string{"Content-Encoding", "gzip"}, http.StatusBadRequest, "decode body failed"},
	} {
		rec := serve(pc, local, http.MethodPost, c.target, c.body, c.headers...)
		if rec.Code != c.code || !strings.Contains(rec.Body.String(), c.message) {
			t.Errorf("%s: %d %q, want %d %q", c.name, rec.Code, rec.Body, c.code, c.message)
		}
	}
	if _, _, ok := local.GetWithTTL("k"); ok {
		t.Error("a refused write must not be stored")
	}
	// Accepted: the named TTL, and 30 s for a missing, zero, negative or unparsable one.
	for name, c := range map[string]struct {
		query string
		min   time.Duration
		max   time.Duration
	}{
		"named":       {"ttl_ms=90000", 80 * time.Second, 90 * time.Second},
		"missing":     {"", 25 * time.Second, 30 * time.Second},
		"zero":        {"ttl_ms=0", 25 * time.Second, 30 * time.Second},
		"negative":    {"ttl_ms=-5", 25 * time.Second, 30 * time.Second},
		"unparsable":  {"ttl_ms=soon", 25 * time.Second, 30 * time.Second},
		"with spaces": {"ttl_ms=%2060000%20", 55 * time.Second, 60 * time.Second},
	} {
		key := "set-" + strings.ReplaceAll(name, " ", "-")
		rec := serve(pc, local, http.MethodPost, "/_cache/set?key="+key+"&"+c.query, []byte("value"))
		if rec.Code != http.StatusNoContent || rec.Header().Get("X-Peer-Set-Encoding") != "zstd" {
			t.Errorf("%s: %d %q", name, rec.Code, rec.Body)
			continue
		}
		got, ttl, ok := local.GetWithTTL(key)
		if !ok || string(got) != "value" || ttl < c.min || ttl > c.max {
			t.Errorf("%s: stored %q ttl %v (found %v), want ttl in [%v, %v]", name, got, ttl, ok, c.min, c.max)
		}
	}
}

// A peer that answers wrongly costs the replica one error with its reason, never a bad value: a malformed TTL header,
// an undecodable body, a cut body and an error status are each refused and counted.
// conformance: peer-cache-fleet-recovery
func TestFetchFromRefusesWhatAPeerGetsWrong(t *testing.T) {
	for _, c := range []struct {
		name    string
		handler http.HandlerFunc
		status  int
		reason  string // the error reason counted; "" for none
		body    string
		ttl     time.Duration
		memOnly bool
	}{
		{"hit", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Cache-Ttl-Ms", "1500")
			w.Header().Set(peerMemoryOnlyHeader, "1")
			_, _ = w.Write([]byte("value"))
		}, http.StatusOK, "", "value", 1500 * time.Millisecond, true},
		{"hit without a ttl header", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("value")) }, http.StatusOK, "", "value", 0, false},
		{"hit with a ttl that is no number", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Cache-Ttl-Ms", "soon")
			_, _ = w.Write([]byte("value"))
		}, http.StatusOK, "", "value", 0, false},
		{"miss", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }, http.StatusNotFound, "", "", 0, false},
		{"error status", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) }, 0, "status_500", "", 0, false},
		{"unavailable", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }, 0, "status_503", "", 0, false},
		{"ttl header too long", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Cache-Ttl-Ms", strings.Repeat("1", maxPeerTTLTextBytes+1))
			_, _ = w.Write([]byte("value"))
		}, 0, "invalid_ttl", "", 0, false},
		{"ttl beyond a duration", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Cache-Ttl-Ms", "9223372036854775807")
			_, _ = w.Write([]byte("value"))
		}, 0, "invalid_ttl", "", 0, false},
		{"gzip header over plain bytes", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write([]byte("plain bytes"))
		}, 0, "decode", "", 0, false},
		{"zstd header over plain bytes", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "zstd")
			_, _ = w.Write([]byte("plain bytes"))
		}, 0, "decode", "", 0, false},
		{"body cut short", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write([]byte("short"))
		}, 0, "body_read", "", 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			pc, addr := clientPeer(t, c.handler)
			before := pc.PeerErrors.Load()
			body, ttl, memOnly, status := pc.fetchFrom(addr, "key")
			if status != c.status || string(body) != c.body || ttl != c.ttl || memOnly != c.memOnly {
				t.Errorf("fetchFrom = %q, %v, %v, %d; want %q, %v, %v, %d", body, ttl, memOnly, status, c.body, c.ttl, c.memOnly, c.status)
			}
			counted := pc.PeerErrors.Load() - before
			reasons := pc.PeerErrorReasons()
			switch {
			case c.reason == "" && (counted != 0 || len(reasons) != 0):
				t.Errorf("an answer the peer got right counted %d errors %v", counted, reasons)
			case c.reason != "" && (counted != 1 || reasons[c.reason] != 1):
				t.Errorf("counted %d errors %v, want one for %s", counted, reasons, c.reason)
			}
		})
	}
	// A peer that cannot be reached is an error of its own.
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) {})
	pc.client.Transport = failingTransport{}
	if body, _, _, status := pc.fetchFrom(addr, "key"); body != nil || status != 0 || pc.PeerErrors.Load() != 1 {
		t.Errorf("unreachable: %q %d, %d errors", body, status, pc.PeerErrors.Load())
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &url.Error{Op: "Get", URL: "peer", Err: http.ErrServerClosed}
}

// A caller that may wait long on a peer asks first whether it answers at all.
// conformance: peer-cache-fleet-recovery
func TestPeerReachable(t *testing.T) {
	var gotToken, gotKey string
	status := http.StatusOK
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) {
		gotToken, gotKey = r.Header.Get("X-Peer-Token"), r.URL.Query().Get("keys")
		w.WriteHeader(status)
		_, _ = w.Write([]byte("{}"))
	})
	pc.authToken = "fleet-secret"
	if !pc.Reachable(addr, "a key,with comma") || gotToken != "fleet-secret" || gotKey != "a key,with comma" {
		t.Errorf("a peer answering 200 is reachable: token %q key %q", gotToken, gotKey)
	}
	status = http.StatusInternalServerError
	if pc.Reachable(addr, "k") {
		t.Error("a peer answering 500 is not reachable")
	}
	if got := pc.PeerErrorReasons()["status_500"]; got != 1 {
		t.Errorf("reasons = %v", pc.PeerErrorReasons())
	}
	pc.client.Transport = failingTransport{}
	if pc.Reachable(addr, "k") {
		t.Error("a peer that cannot be called is not reachable")
	}
}

// A write to the key's owner is delivered with its TTL; a refusal or an unreachable owner is counted and reported.
// conformance: peer-cache-replication
func TestPushToOwner(t *testing.T) {
	var gotKey, gotTTL, gotBody, gotEncoding string
	status := http.StatusNoContent
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotTTL, gotEncoding = r.URL.Query().Get("key"), r.URL.Query().Get("ttl_ms"), r.Header.Get("Content-Encoding")
		b := new(bytes.Buffer)
		_, _ = b.ReadFrom(r.Body)
		gotBody = b.String()
		w.WriteHeader(status)
	})
	if !pc.push(addr, "k&1", []byte("value"), 90*time.Second, false) {
		t.Fatal("a 204 is a delivered write")
	}
	if gotKey != "k&1" || gotTTL != "90000" || gotBody != "value" || gotEncoding != "" || pc.WTPushes.Load() != 1 {
		t.Errorf("delivered key %q ttl %q body %q encoding %q, %d pushes", gotKey, gotTTL, gotBody, gotEncoding, pc.WTPushes.Load())
	}
	if !pc.push(addr, "k2", []byte("value"), 0, false) || gotTTL != "0" {
		t.Errorf("a write without a TTL names 0: %q", gotTTL)
	}
	status = http.StatusInsufficientStorage
	if pc.push(addr, "k3", []byte("value"), time.Minute, false) || pc.WTErrors.Load() != 1 {
		t.Errorf("a refused write is counted: %d errors", pc.WTErrors.Load())
	}
	pc.client.Transport = failingTransport{}
	if pc.push(addr, "k4", []byte("value"), time.Minute, false) || pc.WTErrors.Load() != 2 {
		t.Errorf("an unreachable owner is counted: %d errors", pc.WTErrors.Load())
	}
}
