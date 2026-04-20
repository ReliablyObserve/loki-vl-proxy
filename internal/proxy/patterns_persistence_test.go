package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func requireProxyMetricsContain(t *testing.T, p *Proxy, needle string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	p.metrics.Handler(rec, req)
	if !strings.Contains(rec.Body.String(), needle) {
		t.Fatalf("expected metrics output to contain %q, got:\n%s", needle, rec.Body.String())
	}
}

func TestPatternsRestoreFromDisk_AndWarmStartup(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p := newPatternPersistenceProxy(t, backend.URL, persistPath, true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	payload := mustMarshalJSON(t, map[string]interface{}{
		"status": "success",
		"data": []map[string]interface{}{
			{
				"pattern": "user <_> logged in from <_>",
				"samples": [][]interface{}{{"1710000000000000000", "user 123 logged in from 10.0.0.1"}},
			},
		},
	})
	cacheKey := "patterns:org:query=%7Bapp%3D%22web%22%7D"
	savedAt := time.Now().UTC().UnixNano()
	snapshot := patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: savedAt,
		EntriesByKey: map[string]patternSnapshotEntry{
			cacheKey: {
				Value:             payload,
				UpdatedAtUnixNano: savedAt,
				PatternCount:      1,
			},
		},
	}
	snapshotBody := mustMarshalJSON(t, snapshot)
	if err := os.WriteFile(persistPath, snapshotBody, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	ok, restoredAt, err := p.restorePatternsFromDisk()
	if err != nil {
		t.Fatalf("restorePatternsFromDisk returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected restorePatternsFromDisk to apply snapshot")
	}
	if restoredAt != savedAt {
		t.Fatalf("expected restoredAt=%d, got %d", savedAt, restoredAt)
	}
	requireProxyMetricsContain(t, p, "loki_vl_proxy_patterns_persisted_disk_entries 1")
	requireProxyMetricsContain(t, p, "loki_vl_proxy_patterns_persisted_disk_patterns 1")
	requireProxyMetricsContain(t, p, "loki_vl_proxy_patterns_restored_disk_bytes_total "+strconv.Itoa(len(snapshotBody)))
	if got, _, hit := p.cache.GetWithTTL(cacheKey); !hit || len(got) == 0 {
		t.Fatalf("expected cache key %q to be restored from disk snapshot", cacheKey)
	}

	// Warm startup should complete and mark readiness even when peer warm is a no-op.
	p.patternsWarmReady.Store(false)
	p.warmPatternsOnStartup()
	if !p.patternsWarmReady.Load() {
		t.Fatal("expected warmPatternsOnStartup to mark patterns warm as ready")
	}

	// Invalid snapshot version should fail fast.
	badSnapshot := snapshot
	badSnapshot.Version = 2
	if err := os.WriteFile(persistPath, mustMarshalJSON(t, badSnapshot), 0o644); err != nil {
		t.Fatalf("write invalid snapshot: %v", err)
	}
	if _, _, err := p.restorePatternsFromDisk(); err == nil {
		t.Fatal("expected restorePatternsFromDisk to fail for unsupported snapshot version")
	}
}

func TestPatternsRestoreFromPeers_MergesSnapshot(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p := newPatternPersistenceProxy(t, backend.URL, persistPath, true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	peerSavedAt := time.Now().UTC().Add(2 * time.Second).UnixNano()
	peerCacheKey := "patterns:peer:query=%7Bservice_name%3D%22api%22%7D"
	peerPayload := mustMarshalJSON(t, map[string]interface{}{
		"status": "success",
		"data": []map[string]interface{}{
			{
				"pattern": "request <_> completed in <_> ms",
				"samples": [][]interface{}{{"1710000000000001000", "request abc completed in 12 ms"}},
			},
		},
	})
	peerSnapshot := patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: peerSavedAt,
		EntriesByKey: map[string]patternSnapshotEntry{
			peerCacheKey: {
				Value:             peerPayload,
				UpdatedAtUnixNano: peerSavedAt,
				PatternCount:      1,
			},
		},
	}

	peerBody := append(mustMarshalJSON(t, peerSnapshot), '\n')
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cache/get" {
			t.Fatalf("unexpected peer path: %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Peer-Token"); got != "shared-secret" {
			t.Fatalf("unexpected peer token header: got %q", got)
		}
		if !strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip") {
			t.Fatalf("expected peer warm fetch to advertise compression, got %q", r.Header.Get("Accept-Encoding"))
		}
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		if err != nil {
			t.Fatalf("create gzip writer: %v", err)
		}
		if _, err := zw.Write(peerBody); err != nil {
			t.Fatalf("compress peer snapshot: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close gzip writer: %v", err)
		}
		w.Header().Set("Content-Encoding", "gzip")
		if _, err := w.Write(buf.Bytes()); err != nil {
			t.Fatalf("write peer snapshot: %v", err)
		}
	}))
	defer peerSrv.Close()

	peerURL, err := url.Parse(peerSrv.URL)
	if err != nil {
		t.Fatalf("parse peer URL: %v", err)
	}
	p.peerCache = cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "127.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   peerURL.Host,
	})
	t.Cleanup(func() {
		p.peerCache.Close()
	})
	p.peerAuthToken = "shared-secret"

	// Empty peer address branch.
	if snap, bodyBytes, err := p.fetchPatternsSnapshotFromPeer("", 50*time.Millisecond); err != nil || snap != nil || bodyBytes != 0 {
		t.Fatalf("expected empty peer address to return nil,0,nil; got snapshot=%v bodyBytes=%d err=%v", snap, bodyBytes, err)
	}

	ok, restoredAt, err := p.restorePatternsFromPeers(0)
	if err != nil {
		t.Fatalf("restorePatternsFromPeers returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected restorePatternsFromPeers to apply peer snapshot")
	}
	if restoredAt != peerSavedAt {
		t.Fatalf("expected restoredAt=%d, got %d", peerSavedAt, restoredAt)
	}
	requireProxyMetricsContain(t, p, "loki_vl_proxy_patterns_restored_peer_bytes_total "+strconv.Itoa(len(peerBody)))
	if got, _, hit := p.cache.GetWithTTL(peerCacheKey); !hit || len(got) == 0 {
		t.Fatalf("expected cache key %q to be restored from peer snapshot", peerCacheKey)
	}

	// Once minSavedAt reaches the peer snapshot timestamp, no new apply should occur.
	ok, _, err = p.restorePatternsFromPeers(peerSavedAt)
	if err != nil {
		t.Fatalf("restorePatternsFromPeers second call returned error: %v", err)
	}
	if ok {
		t.Fatal("expected restorePatternsFromPeers to no-op when minSavedAt is current")
	}
}

func TestPatternsPersistence_SkipsUnchangedPeriodicRewrite(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p := newPatternPersistenceProxy(t, backend.URL, persistPath, true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	payload := mustMarshalJSON(t, patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "request <_> completed", Samples: [][]interface{}{{"1710000000000000000", 1}}},
		},
	})
	p.recordPatternSnapshotEntry("patterns:test", payload, time.Unix(1710000000, 0).UTC())
	if err := p.persistPatternsNow("startup_peer_warm"); err != nil {
		t.Fatalf("persistPatternsNow first write: %v", err)
	}
	infoBefore, err := os.Stat(persistPath)
	if err != nil {
		t.Fatalf("stat persisted patterns snapshot: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	p.patternsPersistDirty.Store(true)
	if err := p.persistPatternsNow("periodic"); err != nil {
		t.Fatalf("persistPatternsNow second write: %v", err)
	}
	infoAfter, err := os.Stat(persistPath)
	if err != nil {
		t.Fatalf("stat persisted patterns snapshot after skip: %v", err)
	}
	if !infoAfter.ModTime().Equal(infoBefore.ModTime()) {
		t.Fatalf("expected unchanged periodic persist to skip disk rewrite: before=%s after=%s", infoBefore.ModTime(), infoAfter.ModTime())
	}
	requireProxyMetricsContain(t, p, "loki_vl_proxy_patterns_persist_writes_total 1")
}

func TestPatternsPersistSnapshotCache_StaysLocalWithoutWriteThrough(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	var remoteSetCalls atomic.Int32
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cache/set" {
			remoteSetCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer ownerSrv.Close()

	ownerURL, err := url.Parse(ownerSrv.URL)
	if err != nil {
		t.Fatalf("parse owner URL: %v", err)
	}

	var pc *cache.PeerCache
	for i := 0; i < 256; i++ {
		candidate := cache.NewPeerCache(cache.PeerConfig{
			SelfAddr:           fmt.Sprintf("self-%d:3100", i),
			DiscoveryType:      "static",
			StaticPeers:        fmt.Sprintf("%s,%s", fmt.Sprintf("self-%d:3100", i), ownerURL.Host),
			Timeout:            50 * time.Millisecond,
			WriteThrough:       true,
			WriteThroughMinTTL: 5 * time.Second,
		})
		if !candidate.IsOwner(patternsSnapshotCacheKey) {
			pc = candidate
			break
		}
		candidate.Close()
	}
	if pc == nil {
		t.Fatal("expected non-owner peer-cache test setup for snapshot key")
	}
	defer pc.Close()

	c := cache.New(60*time.Second, 1000)
	c.SetL3(pc)
	defer c.Close()

	enabled := true
	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p, err := New(Config{
		BackendURL:              backend.URL,
		Cache:                   c,
		PeerCache:               pc,
		LogLevel:                "error",
		PatternsEnabled:         &enabled,
		PatternsPersistPath:     persistPath,
		PatternsPersistInterval: 30 * time.Second,
		PatternsStartupStale:    5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	payload := mustMarshalJSON(t, map[string]any{
		"status": "success",
		"data": []map[string]any{
			{"pattern": "request <_> completed"},
		},
	})
	now := time.Now().UTC().UnixNano()
	p.patternsSnapshotEntries["patterns:test"] = patternSnapshotEntry{
		Value:             payload,
		UpdatedAtUnixNano: now,
		PatternCount:      1,
	}
	p.patternsSnapshotPatternCount = 1
	p.patternsSnapshotPayloadBytes = int64(len(payload))
	p.patternsPersistDirty.Store(true)

	if err := p.persistPatternsNow("periodic"); err != nil {
		t.Fatalf("persistPatternsNow returned error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if got := remoteSetCalls.Load(); got != 0 {
		t.Fatalf("expected snapshot cache blob to stay local, got %d remote /_cache/set calls", got)
	}
	if _, _, ok := c.GetWithTTL(patternsSnapshotCacheKey); !ok {
		t.Fatalf("expected snapshot cache blob %q to be stored locally", patternsSnapshotCacheKey)
	}
}

func TestPatternsAutodetectFromWindowEntries_PopulatesCacheAndSnapshot(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p := newPatternPersistenceProxy(t, backend.URL, persistPath, true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	query := `{app="web"}`
	cacheKey := p.patternsAutodetectCacheKey("org-a", query, "1", "2", "1m")
	if cacheKey == "" {
		t.Fatal("expected non-empty autodetect cache key")
	}

	entries := []queryRangeWindowEntry{
		{
			Stream: map[string]string{"detected_level": "info"},
			Value:  []interface{}{"1710000000000000000", "user 123 action login from 10.0.0.1"},
		},
		{
			Stream: map[string]string{"detected_level": "info"},
			Value:  []interface{}{"1710000001000000000", "user 456 action login from 10.0.0.2"},
		},
	}
	p.maybeAutodetectPatternsFromWindowEntries("org-a", query, "1", "2", "1m", entries)

	body, _, hit := p.cache.GetWithTTL(cacheKey)
	if !hit {
		t.Fatalf("expected autodetected patterns cached under key %q", cacheKey)
	}
	if got := patternCountFromPayload(body); got == 0 {
		t.Fatalf("expected non-empty autodetected pattern payload, got %d patterns", got)
	}
	if !p.patternsPersistDirty.Load() {
		t.Fatal("expected autodetect to mark pattern snapshot as dirty")
	}
	if len(p.patternsSnapshotEntries) == 0 {
		t.Fatal("expected autodetect to record snapshot entry for persistence")
	}
}

func TestPatternsAutodetectCacheKey_NormalizesTimeAndStepFormats(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p := newPatternPersistenceProxy(t, backend.URL, persistPath, true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	query := `{app="web"} | json | filter source_message_bytes:=89`
	startTime := time.Unix(1710000000, 0).UTC()
	endTime := startTime.Add(5 * time.Minute)

	numericKey := p.patternsAutodetectCacheKey(
		"org-a",
		query,
		strconv.FormatInt(startTime.UnixNano(), 10),
		strconv.FormatInt(endTime.UnixNano(), 10),
		"60",
	)
	rfcKey := p.patternsAutodetectCacheKey(
		"org-a",
		query,
		startTime.Format(time.RFC3339Nano),
		endTime.Format(time.RFC3339Nano),
		"1m",
	)
	if numericKey != rfcKey {
		t.Fatalf("expected normalized pattern cache key parity, got numeric=%q rfc=%q", numericKey, rfcKey)
	}
}

func TestPatternsRestoreFromDisk_EmptyFileIsNoop(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	if err := os.WriteFile(persistPath, []byte(""), 0o644); err != nil {
		t.Fatalf("write empty patterns snapshot: %v", err)
	}

	p := newPatternPersistenceProxy(t, backend.URL, persistPath, true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	ok, restoredAt, err := p.restorePatternsFromDisk()
	if err != nil {
		t.Fatalf("restorePatternsFromDisk returned error for empty file: %v", err)
	}
	if ok || restoredAt != 0 {
		t.Fatalf("expected empty snapshot restore noop, got ok=%v restoredAt=%d", ok, restoredAt)
	}
}

func TestPatternSnapshotDedup_Update_RemovesExactDuplicatePayload(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	p := newPatternPersistenceProxy(t, backend.URL, filepath.Join(t.TempDir(), "patterns.snapshot.json"), true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	query := `{service_name="api"}`
	keyOld := p.patternsAutodetectCacheKey("org-a", query, "1", "2", "1m")
	keyNew := p.patternsAutodetectCacheKey("org-a", query, "3", "4", "1m")
	payload := mustMarshalJSON(t, patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "request <_> completed", Samples: [][]interface{}{{"1710000000000000000", 2}}},
		},
	})

	p.recordPatternSnapshotEntry(keyOld, payload, time.Unix(1710000000, 0).UTC())
	p.cache.SetWithTTL(keyOld, payload, patternsCacheRetention)

	p.recordPatternSnapshotEntry(keyNew, payload, time.Unix(1710000001, 0).UTC())

	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()
	if len(p.patternsSnapshotEntries) != 1 {
		t.Fatalf("expected one deduplicated snapshot entry, got %d", len(p.patternsSnapshotEntries))
	}
	canonicalKey := canonicalPatternSnapshotCacheKey(keyNew)
	if _, ok := p.patternsSnapshotEntries[canonicalKey]; !ok {
		t.Fatalf("expected canonical snapshot key %q to be retained after deduplication", canonicalKey)
	}
	if _, _, hit := p.cache.GetWithTTL(keyOld); !hit {
		t.Fatalf("expected request-scoped cache key %q to remain untouched", keyOld)
	}
}

func TestPatternSnapshotEntry_CanonicalizesRangeKeysAndCompactsSamples(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	p := newPatternPersistenceProxy(t, backend.URL, filepath.Join(t.TempDir(), "patterns.snapshot.json"), true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	query := `{service_name="api"}`
	keyA := p.patternsAutodetectCacheKey("org-a", query, "1", "181", "60s")
	keyB := p.patternsAutodetectCacheKey("org-a", query, "61", "241", "60s")
	payload := mustMarshalJSON(t, patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{
				Pattern: "request <_> completed",
				Samples: [][]interface{}{
					{int64(60), 0},
					{int64(120), 4},
					{int64(180), 0},
				},
			},
		},
	})

	p.recordPatternSnapshotEntry(keyA, payload, time.Unix(1710000000, 0).UTC())

	canonicalKey := canonicalPatternSnapshotCacheKey(keyA)
	p.patternsSnapshotMu.RLock()
	entry, ok := p.patternsSnapshotEntries[canonicalKey]
	p.patternsSnapshotMu.RUnlock()
	if !ok {
		t.Fatalf("expected canonical snapshot key %q to be stored", canonicalKey)
	}

	var stored patternsResponse
	if err := json.Unmarshal(entry.Value, &stored); err != nil {
		t.Fatalf("decode stored snapshot payload: %v", err)
	}
	if len(stored.Data) != 1 {
		t.Fatalf("expected one persisted pattern entry, got %#v", stored.Data)
	}
	if got := stored.Data[0].Samples; len(got) != 1 || got[0][0] != float64(120) || got[0][1] != float64(4) {
		t.Fatalf("expected compacted non-zero samples only, got %#v", got)
	}

	p.patternsPersistDirty.Store(false)
	p.recordPatternSnapshotEntry(keyB, payload, time.Unix(1710000001, 0).UTC())
	if p.patternsPersistDirty.Load() {
		t.Fatal("expected canonical identical snapshot update to avoid marking persistence dirty")
	}

	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()
	if len(p.patternsSnapshotEntries) != 1 {
		t.Fatalf("expected one canonical snapshot entry, got %d", len(p.patternsSnapshotEntries))
	}
}

func TestPatternSnapshotDedup_Update_PreservesDistinctPayloads(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	p := newPatternPersistenceProxy(t, backend.URL, filepath.Join(t.TempDir(), "patterns.snapshot.json"), true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	query := `{service_name="api"}`
	keyA := p.patternsAutodetectCacheKey("org-a", query, "1", "2", "1m")
	keyB := p.patternsAutodetectCacheKey("org-a", query, "3", "4", "1m")
	payloadA := mustMarshalJSON(t, patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "request <_> completed", Samples: [][]interface{}{{"1710000000000000000", 2}}},
		},
	})
	payloadB := mustMarshalJSON(t, patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "request <_> failed", Samples: [][]interface{}{{"1710000005000000000", 1}}},
		},
	})

	p.recordPatternSnapshotEntry(keyA, payloadA, time.Unix(1710000000, 0).UTC())
	p.recordPatternSnapshotEntry(keyB, payloadB, time.Unix(1710000001, 0).UTC())

	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()
	if len(p.patternsSnapshotEntries) != 1 {
		t.Fatalf("expected latest query-level snapshot to replace older range variants, got %d entries", len(p.patternsSnapshotEntries))
	}
	canonicalKey := canonicalPatternSnapshotCacheKey(keyB)
	entry, ok := p.patternsSnapshotEntries[canonicalKey]
	if !ok {
		t.Fatalf("expected canonical key %q to be retained", canonicalKey)
	}
	var stored patternsResponse
	if err := json.Unmarshal(entry.Value, &stored); err != nil {
		t.Fatalf("decode stored payload: %v", err)
	}
	if len(stored.Data) != 1 || stored.Data[0].Pattern != "request <_> failed" {
		t.Fatalf("expected latest payload to win for canonical snapshot, got %#v", stored.Data)
	}
}

func TestPatternSnapshotDedup_PeerMerge_RemovesExactDuplicates(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	p := newPatternPersistenceProxy(t, backend.URL, filepath.Join(t.TempDir(), "patterns.snapshot.json"), true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	query := `{service_name="api"}`
	keyA := p.patternsAutodetectCacheKey("org-a", query, "1", "2", "1m")
	keyB := p.patternsAutodetectCacheKey("org-a", query, "3", "4", "1m")
	payload := mustMarshalJSON(t, patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "request <_> completed", Samples: [][]interface{}{{"1710000000000000000", 2}}},
		},
	})

	snapshot := patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: time.Now().UTC().UnixNano(),
		EntriesByKey: map[string]patternSnapshotEntry{
			keyA: {Value: payload, UpdatedAtUnixNano: time.Unix(1710000000, 0).UTC().UnixNano(), PatternCount: 1},
			keyB: {Value: payload, UpdatedAtUnixNano: time.Unix(1710000001, 0).UTC().UnixNano(), PatternCount: 1},
		},
	}

	p.applyPatternsSnapshot(snapshot, patternDedupSourcePeer)

	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()
	if len(p.patternsSnapshotEntries) != 1 {
		t.Fatalf("expected peer merge dedup to keep one entry, got %d", len(p.patternsSnapshotEntries))
	}
	canonicalKey := canonicalPatternSnapshotCacheKey(keyB)
	if _, ok := p.patternsSnapshotEntries[canonicalKey]; !ok {
		t.Fatalf("expected canonical peer key %q to be retained", canonicalKey)
	}
	if _, _, hit := p.cache.GetWithTTL(keyA); hit {
		t.Fatalf("expected duplicate peer key %q to be invalidated from cache", keyA)
	}
}

func TestHandlePatterns_FallsBackToLatestSnapshotOnEmptyRefresh(t *testing.T) {
	var calls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		calls++
		if calls == 1 {
			_, _ = w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"GET /api/users 200 15ms","_stream":"{app=\"api\"}","level":"info"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte{})
	}))
	defer backend.Close()

	p := newPatternPersistenceProxy(t, backend.URL, filepath.Join(t.TempDir(), "patterns.snapshot.json"), true)
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
	})

	firstReq := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&start=1710000000&end=1710000120&step=60s`, nil)
	firstRec := httptest.NewRecorder()
	p.handlePatterns(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first patterns call to succeed, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}

	var firstResp patternsResponse
	if err := json.Unmarshal(firstRec.Body.Bytes(), &firstResp); err != nil {
		t.Fatalf("decode first patterns response: %v", err)
	}
	if len(firstResp.Data) == 0 {
		t.Fatalf("expected first patterns response to contain data, got %#v", firstResp)
	}

	secondReq := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&start=1710000060&end=1710000180&step=60s`, nil)
	secondRec := httptest.NewRecorder()
	p.handlePatterns(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("expected snapshot fallback response, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}

	var secondResp patternsResponse
	if err := json.Unmarshal(secondRec.Body.Bytes(), &secondResp); err != nil {
		t.Fatalf("decode second patterns response: %v", err)
	}
	if len(secondResp.Data) == 0 {
		t.Fatalf("expected snapshot fallback to keep non-empty patterns, got %#v", secondResp)
	}
	if len(secondResp.Data[0].Samples) != 1 {
		t.Fatalf("expected snapshot fallback to preserve observed samples without synthetic tail fill, got %#v", secondResp.Data[0].Samples)
	}
}

func newPatternPersistenceProxy(t *testing.T, backendURL, persistPath string, autodetect bool) *Proxy {
	t.Helper()
	enabled := true
	p, err := New(Config{
		BackendURL:                    backendURL,
		Cache:                         cache.New(60*time.Second, 1000),
		LogLevel:                      "error",
		PatternsEnabled:               &enabled,
		PatternsAutodetectFromQueries: autodetect,
		PatternsPersistPath:           persistPath,
		PatternsPersistInterval:       30 * time.Second,
		PatternsStartupStale:          5 * time.Minute,
		PatternsPeerWarmTimeout:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create pattern persistence proxy: %v", err)
	}
	return p
}

func mustMarshalJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return out
}
