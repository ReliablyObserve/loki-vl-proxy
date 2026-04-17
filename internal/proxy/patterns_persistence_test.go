package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

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
	if err := os.WriteFile(persistPath, mustMarshalJSON(t, snapshot), 0o644); err != nil {
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

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cache/get" {
			t.Fatalf("unexpected peer path: %s", r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(peerSnapshot); err != nil {
			t.Fatalf("encode peer snapshot: %v", err)
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

	// Empty peer address branch.
	if snap, err := p.fetchPatternsSnapshotFromPeer("", 50*time.Millisecond); err != nil || snap != nil {
		t.Fatalf("expected empty peer address to return nil,nil; got snapshot=%v err=%v", snap, err)
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
	if _, ok := p.patternsSnapshotEntries[keyNew]; !ok {
		t.Fatalf("expected newer cache key %q to be retained after deduplication", keyNew)
	}
	if _, ok := p.patternsSnapshotEntries[keyOld]; ok {
		t.Fatalf("expected older duplicate cache key %q to be removed", keyOld)
	}
	if _, _, hit := p.cache.GetWithTTL(keyOld); hit {
		t.Fatalf("expected dropped cache key %q to be invalidated", keyOld)
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
	if len(p.patternsSnapshotEntries) != 2 {
		t.Fatalf("expected distinct payloads to remain separate, got %d entries", len(p.patternsSnapshotEntries))
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
	if _, ok := p.patternsSnapshotEntries[keyB]; !ok {
		t.Fatalf("expected newer peer key %q to be retained", keyB)
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
