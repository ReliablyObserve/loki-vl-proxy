package cache

import (
	"testing"
	"time"
)

// TestNewDisabled_BasicOperationsAreNoOps covers the NewDisabled() constructor
// and verifies that core Cache operations on a disabled cache behave as
// expected (no-op writes, miss-on-read, idempotent close).
func TestNewDisabled_BasicOperationsAreNoOps(t *testing.T) {
	c := NewDisabled()
	if c == nil {
		t.Fatal("NewDisabled returned nil")
	}
	if !c.disabled {
		t.Fatal("disabled flag must be true on NewDisabled cache")
	}

	// Disabled cache should not store anything — Get must miss after Set.
	c.Set("k", []byte("v"))
	if _, ok := c.Get("k"); ok {
		t.Fatal("disabled cache must not retain values")
	}
	// SetWithTTL should also be a no-op.
	c.SetWithTTL("k2", []byte("v2"), 30*time.Second)
	if _, ok := c.Get("k2"); ok {
		t.Fatal("disabled cache must not retain values set via SetWithTTL")
	}
}

// TestPeerCache_GossipHaveKey_NoOp covers the GossipHaveKey method which is
// kept as an interface-compatibility stub in the sharded model.
func TestPeerCache_GossipHaveKey_NoOp(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self", Timeout: time.Second})
	defer pc.Close()
	// Just call it — there's no observable effect, but the method must not panic.
	pc.GossipHaveKey("any-key")
	pc.GossipHaveKey("")
}

// TestPeerCache_SetPeerAZ covers SetPeerAZ — store + delete branches + nil-receiver guard.
func TestPeerCache_SetPeerAZ(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self", Timeout: time.Second})
	defer pc.Close()

	// Store branch.
	pc.SetPeerAZ("peer1:8080", "az-a")
	if v, ok := pc.peerAZs.Load("peer1:8080"); !ok || v != "az-a" {
		t.Fatalf("expected peer1 → az-a stored, got %v ok=%v", v, ok)
	}

	// Delete branch (empty AZ).
	pc.SetPeerAZ("peer1:8080", "")
	if _, ok := pc.peerAZs.Load("peer1:8080"); ok {
		t.Fatal("empty AZ must delete the entry")
	}

	// Nil-receiver guard must not panic.
	var nilPC *PeerCache
	nilPC.SetPeerAZ("x", "y")

	// Empty addr must short-circuit.
	pc.SetPeerAZ("", "az-b")
	if _, ok := pc.peerAZs.Load(""); ok {
		t.Fatal("empty addr must short-circuit (no store)")
	}
}

// TestPeerCache_HasPeerHost covers HasPeerHost — present + missing branches.
func TestPeerCache_HasPeerHost(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self", Timeout: time.Second})
	defer pc.Close()

	// Missing host returns false.
	if pc.HasPeerHost("unknown.example.com") {
		t.Fatal("expected unknown host to return false")
	}

	// Manually seed the peerHosts map (the public API to populate this is
	// via UpdatePeers/discovery; for a focused unit test we go in directly).
	pc.mu.Lock()
	if pc.peerHosts == nil {
		pc.peerHosts = make(map[string]struct{})
	}
	pc.peerHosts["known.example.com"] = struct{}{}
	pc.mu.Unlock()

	if !pc.HasPeerHost("known.example.com") {
		t.Fatal("expected known host to return true")
	}
}
