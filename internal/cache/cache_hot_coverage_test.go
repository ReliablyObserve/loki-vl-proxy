package cache

import (
	"testing"
	"time"
)

func TestCacheHotHelpers_PruneAndTopHotKeys(t *testing.T) {
	c := &Cache{
		entries: map[string]entry{
			"query_range:tenant-a:key-hot": {
				value:     []byte("abcd"),
				expiresAt: time.Now().Add(3 * time.Minute),
				sizeBytes: 4,
			},
			"query_range:tenant-b:key-other": {
				value:     []byte("abcdefgh"),
				expiresAt: time.Now().Add(2 * time.Minute),
				sizeBytes: 8,
			},
			"query_range:tenant-c:key-short": {
				value:     []byte("abc"),
				expiresAt: time.Now().Add(15 * time.Second),
				sizeBytes: 3,
			},
		},
		hot: map[string]hotStat{
			"query_range:tenant-a:key-hot":   {score: 5, lastAccess: 30, tenant: "tenant-a"},
			"query_range:tenant-b:key-other": {score: 2, lastAccess: 10, tenant: "tenant-b"},
			"query_range:tenant-c:key-short": {score: 2, lastAccess: 5, tenant: "tenant-c"},
			"query_range:tenant-missing:key": {score: 9, lastAccess: 100, tenant: "tenant-missing"},
		},
		hotOn: true,
	}

	hot := c.TopHotKeys(10, 30*time.Second, 6)
	if len(hot) != 1 {
		t.Fatalf("expected only one hot key after TTL and size filters, got %#v", hot)
	}
	if hot[0].Key != "query_range:tenant-a:key-hot" || hot[0].Tenant != "tenant-a" || hot[0].Score != 5 {
		t.Fatalf("unexpected hot key snapshot: %#v", hot[0])
	}

	c.pruneHotLocked(2)
	if _, ok := c.hot["query_range:tenant-c:key-short"]; ok {
		t.Fatalf("expected oldest lowest-score key to be pruned")
	}
	if _, ok := c.hot["query_range:tenant-b:key-other"]; ok {
		t.Fatalf("expected second-lowest-score key to be pruned")
	}
	if _, ok := c.hot["query_range:tenant-a:key-hot"]; !ok {
		t.Fatalf("expected hottest key to remain after prune")
	}
}

func TestCacheHotHelpers_RecordHotLockedAssignsTenant(t *testing.T) {
	c := &Cache{
		hot:    map[string]hotStat{},
		hotMax: 2,
	}

	c.recordHotLocked("query_range:tenant-a:key-1")
	c.recordHotLocked("query_range:tenant-b:key-2")
	c.recordHotLocked("query_range:tenant-a:key-1")

	stat := c.hot["query_range:tenant-a:key-1"]
	if stat.score != 2 {
		t.Fatalf("expected score to increment, got %#v", stat)
	}
	if stat.tenant != "tenant-a" {
		t.Fatalf("expected tenant derived from cache key, got %#v", stat)
	}
	if stat.lastAccess == 0 {
		t.Fatalf("expected lastAccess to be recorded")
	}
}

func TestTenantFromCacheKey_Variants(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "internal key", key: "__meta:ignored", want: ""},
		{name: "malformed key", key: "query_range:tenant-only", want: ""},
		{name: "supported prefix", key: "patterns:tenant-a:selector", want: "tenant-a"},
		{name: "unsupported prefix", key: "custom:tenant-a:value", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tenantFromCacheKey(tt.key); got != tt.want {
				t.Fatalf("tenantFromCacheKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestCache_MaxEntrySizeBytesNilAndZero(t *testing.T) {
	var nilCache *Cache
	if got := nilCache.MaxEntrySizeBytes(); got != 0 {
		t.Fatalf("expected nil cache max entry size to be 0, got %d", got)
	}

	zero := &Cache{}
	if got := zero.MaxEntrySizeBytes(); got != 0 {
		t.Fatalf("expected zero-byte cache max entry size to be 0, got %d", got)
	}
}
