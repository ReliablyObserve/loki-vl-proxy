package proxy

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestProxyHelpers_ReloadFieldMappings(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	if got := p.labelTranslator.ToLoki("service.name"); got != "service.name" {
		t.Fatalf("expected passthrough translator before reload, got %q", got)
	}

	p.ReloadFieldMappings([]FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}})

	if got := p.labelTranslator.ToLoki("service.name"); got != "service_name" {
		t.Fatalf("expected reloaded field mapping to apply, got %q", got)
	}
}

func TestProxyHelpers_RequestPolicyError(t *testing.T) {
	err := &requestPolicyError{status: 403, msg: "denied"}
	if got := err.Error(); got != "denied" {
		t.Fatalf("expected error message to round-trip, got %q", got)
	}
}

func TestProxyHelpers_HostOnlyAndKnownPeerHost(t *testing.T) {
	if got := hostOnly("10.0.0.1:3100"); got != "10.0.0.1" {
		t.Fatalf("expected hostOnly to strip port, got %q", got)
	}
	if got := hostOnly("10.0.0.2"); got != "10.0.0.2" {
		t.Fatalf("expected hostOnly to keep bare host, got %q", got)
	}

	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "10.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "10.0.0.2:3100,10.0.0.3:3100",
		Timeout:       50 * time.Millisecond,
	})
	defer pc.Close()

	p := &Proxy{peerCache: pc}
	if !p.isKnownPeerHost("10.0.0.2") {
		t.Fatal("expected configured peer host to be recognized")
	}
	if p.isKnownPeerHost("10.0.0.9") {
		t.Fatal("expected unknown host to be rejected")
	}
}

func TestProxyHelpers_CanonicalReadCacheKey_NormalizesEquivalentHelperRequests(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	reqA := httptest.NewRequest("GET", "/loki/api/v1/labels?from=10&to=20&q=svc&query=%7Bapp%3D%22demo%22%7D", nil)
	reqB := httptest.NewRequest("GET", "/loki/api/v1/labels?search=svc&end=20&start=10&query=%7Bapp%3D%22demo%22%7D", nil)

	keyA := p.canonicalReadCacheKey("labels", "tenant-a", reqA)
	keyB := p.canonicalReadCacheKey("labels", "tenant-a", reqB)
	if keyA != keyB {
		t.Fatalf("expected equivalent helper requests to share cache key, got %q != %q", keyA, keyB)
	}
}

func TestProxyHelpers_CanonicalReadCacheKey_NormalizesDetectedLimitsAndDefaults(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	reqA := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?line_limit=1000", nil)
	reqB := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?limit=1000&query=*", nil)

	keyA := p.canonicalReadCacheKey("detected_fields", "tenant-a", reqA)
	keyB := p.canonicalReadCacheKey("detected_fields", "tenant-a", reqB)
	if keyA != keyB {
		t.Fatalf("expected equivalent detected_fields requests to share cache key, got %q != %q", keyA, keyB)
	}
}
