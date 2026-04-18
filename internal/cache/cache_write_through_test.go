package cache

import (
	"fmt"
	"testing"
	"time"
)

func findKeyForOwner(t *testing.T, pc *PeerCache, owner string) string {
	t.Helper()
	for i := 0; i < 4096; i++ {
		key := fmt.Sprintf("wt-cache-key-%d", i)
		pc.mu.RLock()
		got := pc.ring.get(key)
		pc.mu.RUnlock()
		if got == owner {
			return key
		}
	}
	t.Fatalf("failed to find key for owner %q", owner)
	return ""
}

func TestCache_NonOwnerWriteThrough_ClampsShadowTTLAndSkipsL2(t *testing.T) {
	c := NewWithMaxBytes(10*time.Minute, 1000, 1024*1024)
	defer c.Close()

	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
	if err != nil {
		t.Fatalf("new disk cache: %v", err)
	}
	defer func() { _ = dc.Close() }()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:           "self:3100",
		DiscoveryType:      "static",
		StaticPeers:        "self:3100,owner:3100",
		Timeout:            10 * time.Millisecond,
		WriteThrough:       true,
		WriteThroughMinTTL: 5 * time.Second,
	})
	defer pc.Close()

	c.SetL2(dc)
	c.SetL3(pc)

	key := findKeyForOwner(t, pc, "owner:3100")
	c.SetWithTTL(key, []byte("value"), 2*time.Minute)

	_, ttl, ok := c.GetWithTTL(key)
	if !ok {
		t.Fatalf("expected local L1 hit for key %q", key)
	}
	if ttl <= 0 || ttl > nonOwnerShadowMaxTTL+2*time.Second {
		t.Fatalf("expected non-owner TTL to be clamped to ~%v, got %v", nonOwnerShadowMaxTTL, ttl)
	}

	if _, ok := dc.Get(key); ok {
		t.Fatalf("expected non-owner write-through key %q to skip local L2 write", key)
	}
}

func TestCache_OwnerWriteThrough_KeepsTTLAndWritesL2(t *testing.T) {
	c := NewWithMaxBytes(10*time.Minute, 1000, 1024*1024)
	defer c.Close()

	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
	if err != nil {
		t.Fatalf("new disk cache: %v", err)
	}
	defer func() { _ = dc.Close() }()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:           "self:3100",
		DiscoveryType:      "static",
		StaticPeers:        "self:3100,owner:3100",
		Timeout:            10 * time.Millisecond,
		WriteThrough:       true,
		WriteThroughMinTTL: 5 * time.Second,
	})
	defer pc.Close()

	c.SetL2(dc)
	c.SetL3(pc)

	key := findKeyForOwner(t, pc, "self:3100")
	c.SetWithTTL(key, []byte("value"), 2*time.Minute)

	_, ttl, ok := c.GetWithTTL(key)
	if !ok {
		t.Fatalf("expected local L1 hit for owner key %q", key)
	}
	if ttl <= nonOwnerShadowMaxTTL {
		t.Fatalf("expected owner key TTL to stay above non-owner shadow cap (%v), got %v", nonOwnerShadowMaxTTL, ttl)
	}

	v, ok := dc.Get(key)
	if !ok {
		t.Fatalf("expected owner key %q to be written to local L2", key)
	}
	if string(v) != "value" {
		t.Fatalf("unexpected L2 value for key %q: %q", key, string(v))
	}
}

func TestCache_LocalOnlyWrite_KeepsTTLAndSkipsL2(t *testing.T) {
	c := NewWithMaxBytes(10*time.Minute, 1000, 1024*1024)
	defer c.Close()

	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
	if err != nil {
		t.Fatalf("new disk cache: %v", err)
	}
	defer func() { _ = dc.Close() }()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:           "self:3100",
		DiscoveryType:      "static",
		StaticPeers:        "self:3100,owner:3100",
		Timeout:            10 * time.Millisecond,
		WriteThrough:       true,
		WriteThroughMinTTL: 5 * time.Second,
	})
	defer pc.Close()

	c.SetL2(dc)
	c.SetL3(pc)

	key := findKeyForOwner(t, pc, "owner:3100")
	c.SetLocalOnlyWithTTL(key, []byte("value"), 2*time.Minute)

	_, ttl, ok := c.GetWithTTL(key)
	if !ok {
		t.Fatalf("expected local L1 hit for key %q", key)
	}
	if ttl <= nonOwnerShadowMaxTTL {
		t.Fatalf("expected local-only TTL to stay above non-owner shadow cap (%v), got %v", nonOwnerShadowMaxTTL, ttl)
	}

	if _, ok := dc.Get(key); ok {
		t.Fatalf("expected local-only key %q to skip local L2 write", key)
	}
	if got := pc.WTPushes.Load(); got != 0 {
		t.Fatalf("expected local-only write to skip write-through pushes, got %d", got)
	}
}
