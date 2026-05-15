package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

func (p *Proxy) recordPatternSnapshotEntry(cacheKey string, payload []byte, now time.Time) {
	if !p.patternsEnabled {
		return
	}
	canonicalKey := canonicalPatternSnapshotCacheKey(cacheKey)
	if canonicalKey != "" {
		cacheKey = canonicalKey
	}
	payload = normalizePatternSnapshotPayload(payload)
	patternCount := patternCountFromPayload(payload)
	if patternCount > 0 {
		p.metrics.RecordPatternsStored(patternCount)
	}
	if strings.TrimSpace(p.patternsPersistPath) == "" {
		return
	}
	copied := append([]byte(nil), payload...)
	p.patternsSnapshotMu.Lock()
	if existing, ok := p.patternsSnapshotEntries[cacheKey]; ok {
		if bytes.Equal(existing.Value, copied) {
			if existing.UpdatedAtUnixNano < now.UnixNano() {
				existing.UpdatedAtUnixNano = now.UnixNano()
				p.patternsSnapshotEntries[cacheKey] = existing
			}
			p.updatePatternSnapshotMetricsLocked()
			p.patternsSnapshotMu.Unlock()
			return
		}
		p.patternsSnapshotPatternCount -= int64(existing.PatternCount)
		p.patternsSnapshotPayloadBytes -= int64(len(existing.Value))
	}
	p.patternsSnapshotEntries[cacheKey] = patternSnapshotEntry{
		Value:             copied,
		UpdatedAtUnixNano: now.UnixNano(),
		PatternCount:      patternCount,
	}
	p.patternsSnapshotPatternCount += int64(patternCount)
	p.patternsSnapshotPayloadBytes += int64(len(copied))
	droppedEntries, _ := p.compactPatternSnapshotEntriesLocked()
	p.updatePatternSnapshotMetricsLocked()
	p.patternsSnapshotMu.Unlock()
	p.patternsPersistDirty.Store(true)
	if droppedEntries > 0 {
		p.recordPatternSnapshotDedup(patternDedupSourceMemory, "update", droppedEntries)
	}
}

func patternCountFromPayload(payload []byte) int {
	if len(payload) == 0 {
		return 0
	}
	var resp patternsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return 0
	}
	return len(resp.Data)
}

func recordPatternResponseMetrics(m *metrics.Metrics, payload []byte) {
	if m == nil {
		return
	}
	m.SetPatternsLastResponse(patternCountFromPayload(payload), int64(len(payload)))
}

func patternCountFromSnapshot(snapshot patternsSnapshot) int {
	total := 0
	for key, entry := range snapshot.EntriesByKey {
		if strings.TrimSpace(key) == "" || len(entry.Value) == 0 {
			continue
		}
		if entry.PatternCount > 0 {
			total += entry.PatternCount
			continue
		}
		total += patternCountFromPayload(entry.Value)
	}
	return total
}

func (p *Proxy) cacheSnapshotBlobLocally(key string, data []byte, ttl time.Duration) {
	if p.cache == nil || strings.TrimSpace(key) == "" || len(data) == 0 || ttl <= 0 {
		return
	}
	p.cache.SetLocalOnlyWithTTL(key, data, ttl)
}

func (p *Proxy) updatePatternSnapshotMetricsLocked() {
	if p == nil || p.metrics == nil {
		return
	}
	p.metrics.SetPatternsInMemory(
		int(p.patternsSnapshotPatternCount),
		len(p.patternsSnapshotEntries),
		p.patternsSnapshotPayloadBytes,
	)
}

func (p *Proxy) buildPatternsSnapshot(now time.Time) patternsSnapshot {
	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()

	entries := make(map[string]patternSnapshotEntry, len(p.patternsSnapshotEntries))
	for key, entry := range p.patternsSnapshotEntries {
		if strings.TrimSpace(key) == "" || len(entry.Value) == 0 {
			continue
		}
		patternCount := entry.PatternCount
		if patternCount <= 0 {
			patternCount = patternCountFromPayload(entry.Value)
		}
		entries[key] = patternSnapshotEntry{
			Value:             append([]byte(nil), entry.Value...),
			UpdatedAtUnixNano: entry.UpdatedAtUnixNano,
			PatternCount:      patternCount,
		}
	}

	return patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: now.UnixNano(),
		EntriesByKey:    entries,
	}
}

func (p *Proxy) applyPatternsSnapshot(snapshot patternsSnapshot, source string) (int, int) {
	if p.cache == nil {
		return 0, 0
	}

	nowUnix := time.Now().UTC().UnixNano()
	appliedEntries := 0
	appliedPatterns := 0

	p.patternsSnapshotMu.Lock()

	for key, incoming := range snapshot.EntriesByKey {
		if strings.TrimSpace(key) == "" || len(incoming.Value) == 0 {
			continue
		}
		canonicalKey := canonicalPatternSnapshotCacheKey(key)
		if strings.TrimSpace(canonicalKey) == "" {
			canonicalKey = key
		}
		incoming.Value = normalizePatternSnapshotPayload(incoming.Value)
		if incoming.UpdatedAtUnixNano <= 0 {
			incoming.UpdatedAtUnixNano = nowUnix
		}
		if existing, ok := p.patternsSnapshotEntries[canonicalKey]; ok && existing.UpdatedAtUnixNano >= incoming.UpdatedAtUnixNano {
			continue
		}
		incomingPatternCount := incoming.PatternCount
		if incomingPatternCount <= 0 {
			incomingPatternCount = patternCountFromPayload(incoming.Value)
		}
		copied := append([]byte(nil), incoming.Value...)
		if existing, ok := p.patternsSnapshotEntries[canonicalKey]; ok {
			p.patternsSnapshotPatternCount -= int64(existing.PatternCount)
			p.patternsSnapshotPayloadBytes -= int64(len(existing.Value))
		}
		p.patternsSnapshotEntries[canonicalKey] = patternSnapshotEntry{
			Value:             copied,
			UpdatedAtUnixNano: incoming.UpdatedAtUnixNano,
			PatternCount:      incomingPatternCount,
		}
		p.patternsSnapshotPatternCount += int64(incomingPatternCount)
		p.patternsSnapshotPayloadBytes += int64(len(copied))
		p.cache.SetWithTTL(canonicalKey, copied, patternsCacheRetention)
		appliedEntries++
		appliedPatterns += incomingPatternCount
	}
	droppedEntries, _ := p.compactPatternSnapshotEntriesLocked()
	p.updatePatternSnapshotMetricsLocked()
	p.patternsSnapshotMu.Unlock()

	if appliedEntries > 0 || droppedEntries > 0 {
		p.patternsPersistDirty.Store(true)
	}
	if droppedEntries > 0 {
		p.recordPatternSnapshotDedup(source, "merge", droppedEntries)
	}
	return appliedEntries, appliedPatterns
}

func patternSnapshotIdentityFromCacheKey(cacheKey string) string {
	cacheKey = strings.TrimSpace(cacheKey)
	if cacheKey == "" {
		return ""
	}
	parts := strings.SplitN(cacheKey, ":", 3)
	if len(parts) != 3 || parts[0] != "patterns" {
		return ""
	}
	orgID := strings.TrimSpace(parts[1])
	params, err := url.ParseQuery(parts[2])
	if err != nil {
		return ""
	}
	query := patternScopeQuery(params.Get("query"))
	if strings.TrimSpace(query) == "" {
		return ""
	}
	return orgID + "\x00" + query
}

func canonicalPatternSnapshotCacheKey(cacheKey string) string {
	cacheKey = strings.TrimSpace(cacheKey)
	if cacheKey == "" {
		return ""
	}
	parts := strings.SplitN(cacheKey, ":", 3)
	if len(parts) != 3 || parts[0] != "patterns" {
		return cacheKey
	}
	orgID := strings.TrimSpace(parts[1])
	params, err := url.ParseQuery(parts[2])
	if err != nil {
		return cacheKey
	}
	query := patternScopeQuery(params.Get("query"))
	if strings.TrimSpace(query) == "" {
		return cacheKey
	}
	canonical := url.Values{}
	canonical.Set("query", query)
	return "patterns:" + orgID + ":" + canonical.Encode()
}

func normalizePatternSnapshotPayload(payload []byte) []byte {
	if len(payload) == 0 {
		return payload
	}
	var resp patternsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return payload
	}
	changed := false
	for i := range resp.Data {
		entry := resp.Data[i]
		if len(entry.Samples) == 0 {
			continue
		}
		compacted := compactPatternSnapshotSamples(entry.Samples)
		if len(compacted) != len(entry.Samples) {
			changed = true
		}
		entry.Samples = compacted
		resp.Data[i] = entry
	}
	if !changed {
		return payload
	}
	encoded, err := json.Marshal(resp)
	if err != nil {
		return payload
	}
	return encoded
}

func compactPatternSnapshotSamples(samples [][]interface{}) [][]interface{} {
	if len(samples) == 0 {
		return samples
	}
	byTimestamp := make(map[int64]int, len(samples))
	order := make([]int64, 0, len(samples))
	for _, pair := range samples {
		if len(pair) < 2 {
			continue
		}
		ts, okTS := numberToInt64(pair[0])
		count, okCount := numberToInt(pair[1])
		if !okTS || !okCount || count <= 0 {
			continue
		}
		if _, seen := byTimestamp[ts]; !seen {
			order = append(order, ts)
		}
		if count > byTimestamp[ts] {
			byTimestamp[ts] = count
		}
	}
	if len(byTimestamp) == 0 {
		return [][]interface{}{}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	compacted := make([][]interface{}, 0, len(order))
	for _, ts := range order {
		compacted = append(compacted, []interface{}{ts, byTimestamp[ts]})
	}
	return compacted
}

func betterPatternSnapshotEntry(current, incoming patternSnapshotEntry, currentKey, incomingKey string) bool {
	if incoming.UpdatedAtUnixNano != current.UpdatedAtUnixNano {
		return incoming.UpdatedAtUnixNano > current.UpdatedAtUnixNano
	}
	if incoming.PatternCount != current.PatternCount {
		return incoming.PatternCount > current.PatternCount
	}
	if len(incoming.Value) != len(current.Value) {
		return len(incoming.Value) > len(current.Value)
	}
	// Deterministic tie-breaker.
	return incomingKey > currentKey
}

func (p *Proxy) latestPatternSnapshotPayload(cacheKey string) ([]byte, bool) {
	if p == nil || !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return nil, false
	}
	identity := patternSnapshotIdentityFromCacheKey(cacheKey)
	if identity == "" {
		return nil, false
	}

	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()

	var (
		bestKey   string
		bestEntry patternSnapshotEntry
	)
	for key, entry := range p.patternsSnapshotEntries {
		if patternSnapshotIdentityFromCacheKey(key) != identity {
			continue
		}
		if entry.PatternCount <= 0 || len(entry.Value) == 0 {
			continue
		}
		if bestKey == "" || betterPatternSnapshotEntry(bestEntry, entry, bestKey, key) {
			bestKey = key
			bestEntry = entry
		}
	}
	if bestKey == "" {
		return nil, false
	}
	return append([]byte(nil), bestEntry.Value...), true
}

func (p *Proxy) compactPatternSnapshotEntriesLocked() (int, int) {
	if p == nil || len(p.patternsSnapshotEntries) <= 1 {
		return 0, 0
	}

	keysByIdentity := make(map[string][]string, len(p.patternsSnapshotEntries))
	for key := range p.patternsSnapshotEntries {
		identity := patternSnapshotIdentityFromCacheKey(key)
		if identity == "" {
			continue
		}
		keysByIdentity[identity] = append(keysByIdentity[identity], key)
	}

	dropped := make(map[string]struct{})
	for _, keys := range keysByIdentity {
		if len(keys) <= 1 {
			continue
		}
		keepKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			entry := p.patternsSnapshotEntries[key]
			matched := -1
			for i, keepKey := range keepKeys {
				keepEntry := p.patternsSnapshotEntries[keepKey]
				if bytes.Equal(keepEntry.Value, entry.Value) {
					matched = i
					break
				}
			}
			if matched == -1 {
				keepKeys = append(keepKeys, key)
				continue
			}
			existingKey := keepKeys[matched]
			existingEntry := p.patternsSnapshotEntries[existingKey]
			if betterPatternSnapshotEntry(existingEntry, entry, existingKey, key) {
				keepKeys[matched] = key
				dropped[existingKey] = struct{}{}
			} else {
				dropped[key] = struct{}{}
			}
		}
	}

	if len(dropped) == 0 {
		return 0, 0
	}

	beforePatterns := p.patternsSnapshotPatternCount
	droppedEntries := 0
	for key := range dropped {
		if _, ok := p.patternsSnapshotEntries[key]; !ok {
			continue
		}
		delete(p.patternsSnapshotEntries, key)
		droppedEntries++
		if p.cache != nil {
			p.cache.Invalidate(key)
		}
	}
	p.recomputePatternSnapshotStatsLocked()
	droppedPatterns := int(beforePatterns - p.patternsSnapshotPatternCount)
	if droppedPatterns < 0 {
		droppedPatterns = 0
	}
	return droppedEntries, droppedPatterns
}

func (p *Proxy) compactPatternsSnapshot(source, reason string) (int, int) {
	p.patternsSnapshotMu.Lock()
	droppedEntries, droppedPatterns := p.compactPatternSnapshotEntriesLocked()
	p.updatePatternSnapshotMetricsLocked()
	p.patternsSnapshotMu.Unlock()
	if droppedEntries > 0 {
		p.patternsPersistDirty.Store(true)
		p.recordPatternSnapshotDedup(source, reason, droppedEntries)
	}
	return droppedEntries, droppedPatterns
}

func (p *Proxy) recordPatternSnapshotDedup(source, reason string, droppedEntries int) {
	if droppedEntries <= 0 {
		return
	}
	source = strings.ToLower(strings.TrimSpace(source))
	switch source {
	case patternDedupSourceDisk, patternDedupSourcePeer:
	default:
		source = patternDedupSourceMemory
	}
	if p.metrics != nil {
		p.metrics.RecordPatternsDeduplicated(source, droppedEntries)
	}
	p.log.Info(
		"patterns snapshot deduplicated",
		"component", "proxy",
		"source", source,
		"reason", reason,
		"duplicates_removed", droppedEntries,
	)
}

func (p *Proxy) recomputePatternSnapshotStatsLocked() {
	var patterns int64
	var payloadBytes int64
	for key, entry := range p.patternsSnapshotEntries {
		if strings.TrimSpace(key) == "" || len(entry.Value) == 0 {
			delete(p.patternsSnapshotEntries, key)
			continue
		}
		patternCount := entry.PatternCount
		if patternCount <= 0 {
			patternCount = patternCountFromPayload(entry.Value)
			entry.PatternCount = patternCount
			p.patternsSnapshotEntries[key] = entry
		}
		patterns += int64(patternCount)
		payloadBytes += int64(len(entry.Value))
	}
	p.patternsSnapshotPatternCount = patterns
	p.patternsSnapshotPayloadBytes = payloadBytes
}

func writeSnapshotFileIfChanged(path string, data []byte, compareDigest [sha256.Size]byte, digest *[sha256.Size]byte, ready *bool) (bool, error) {
	if *ready && *digest == compareDigest {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return false, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return false, err
	}
	*digest = compareDigest
	*ready = true
	return true, nil
}

func mustMarshalSnapshot(v interface{}) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return out
}

func (p *Proxy) persistPatternsNow(reason string) error {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return nil
	}
	if reason == "periodic" && !p.patternsPersistDirty.Load() {
		return nil
	}
	startedAt := time.Now()

	snapshot := p.buildPatternsSnapshot(time.Now().UTC())
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal patterns snapshot: %w", err)
	}
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	snapshotDigest := sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))

	path := p.patternsPersistPath
	wrote, err := writeSnapshotFileIfChanged(path, data, snapshotDigest, &p.patternsPersistDigest, &p.patternsPersistDigestReady)
	if err != nil {
		return fmt.Errorf("persist patterns snapshot: %w", err)
	}

	if p.cache != nil {
		ttl := p.patternsStartupStale * 3
		minTTL := p.patternsPersistInterval * 2
		if ttl < minTTL {
			ttl = minTTL
		}
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cacheSnapshotBlobLocally(patternsSnapshotCacheKey, data, ttl)
	}

	entryCount := len(snapshot.EntriesByKey)
	patternCount := patternCountFromSnapshot(snapshot)
	p.log.Info(
		"patterns snapshot persisted",
		"reason", reason,
		"path", path,
		"wrote_disk", wrote,
		"entries", entryCount,
		"patterns", patternCount,
		"bytes", len(data),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	p.metrics.SetPatternsPersistedDiskState(patternCount, entryCount, int64(len(data)))
	if wrote {
		p.metrics.RecordPatternsPersistWrite(int64(len(data)))
	}
	p.patternsPersistDirty.Store(false)
	return nil
}

func (p *Proxy) restorePatternsFromDisk() (bool, int64, error) {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return false, 0, nil
	}
	startedAt := time.Now()
	data, err := os.ReadFile(p.patternsPersistPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("read patterns snapshot: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return false, 0, nil
	}
	var snapshot patternsSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return false, 0, fmt.Errorf("decode patterns snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return false, 0, fmt.Errorf("unsupported patterns snapshot version: %d", snapshot.Version)
	}
	if snapshot.SavedAtUnixNano <= 0 {
		return false, 0, fmt.Errorf("invalid patterns snapshot timestamp: %d", snapshot.SavedAtUnixNano)
	}
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	p.patternsPersistDigest = sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))
	p.patternsPersistDigestReady = true

	snapshotEntryCount := len(snapshot.EntriesByKey)
	snapshotPatternCount := patternCountFromSnapshot(snapshot)
	appliedEntries, appliedPatterns := p.applyPatternsSnapshot(snapshot, patternDedupSourceDisk)
	p.metrics.RecordPatternsRestoredFromDisk(appliedPatterns, appliedEntries)
	p.metrics.RecordPatternsRestoreBytes("disk", int64(len(data)))
	p.metrics.SetPatternsPersistedDiskState(snapshotPatternCount, snapshotEntryCount, int64(len(data)))
	if p.cache != nil {
		ttl := p.patternsStartupStale * 3
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cacheSnapshotBlobLocally(patternsSnapshotCacheKey, data, ttl)
	}
	p.log.Info(
		"patterns snapshot restored from disk",
		"path", p.patternsPersistPath,
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"entries_applied", appliedEntries,
		"patterns_applied", appliedPatterns,
		"bytes", len(data),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) fetchPatternsSnapshotFromPeer(peerAddr string, timeout time.Duration) (*patternsSnapshot, int, error) {
	if strings.TrimSpace(peerAddr) == "" {
		return nil, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	endpoint := fmt.Sprintf("http://%s/_cache/get?key=%s", peerAddr, url.QueryEscape(patternsSnapshotCacheKey))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	if p.peerAuthToken != "" {
		req.Header.Set("X-Peer-Token", p.peerAuthToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, 0, fmt.Errorf("peer %s status %d: %s", peerAddr, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := decodeCompressedHTTPResponse(resp); err != nil {
		return nil, 0, err
	}
	body, err := readBodyLimited(resp.Body, maxPatternsPeerSnapshotBytes)
	if err != nil {
		return nil, 0, err
	}
	var snapshot patternsSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, 0, err
	}
	if snapshot.Version != 1 || snapshot.SavedAtUnixNano <= 0 {
		return nil, 0, fmt.Errorf("invalid patterns snapshot metadata from peer %s", peerAddr)
	}
	return &snapshot, len(body), nil
}

func (p *Proxy) restorePatternsFromPeers(minSavedAt int64) (bool, int64, error) {
	if !p.patternsEnabled || p.peerCache == nil {
		return false, 0, nil
	}
	startedAt := time.Now()
	peers := p.peerCache.Peers()
	if len(peers) == 0 {
		return false, 0, nil
	}

	timeout := p.patternsPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	type peerResult struct {
		snapshot  *patternsSnapshot
		bodyBytes int
		err       error
		peer      string
	}
	resCh := make(chan peerResult, len(peers))
	for _, peerAddr := range peers {
		peerAddr := strings.TrimSpace(peerAddr)
		if peerAddr == "" {
			continue
		}
		go func(addr string) {
			snapshot, bodyBytes, err := p.fetchPatternsSnapshotFromPeer(addr, timeout)
			resCh <- peerResult{snapshot: snapshot, bodyBytes: bodyBytes, err: err, peer: addr}
		}(peerAddr)
	}

	merged := patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: time.Now().UTC().UnixNano(),
		EntriesByKey:    make(map[string]patternSnapshotEntry),
	}
	mergedSavedAt := minSavedAt

	p.patternsSnapshotMu.RLock()
	for key, entry := range p.patternsSnapshotEntries {
		merged.EntriesByKey[key] = patternSnapshotEntry{
			Value:             append([]byte(nil), entry.Value...),
			UpdatedAtUnixNano: entry.UpdatedAtUnixNano,
		}
	}
	p.patternsSnapshotMu.RUnlock()

	deadline := time.After(timeout)
	received := 0
	totalPeerBytes := int64(0)
	for received < len(peers) {
		select {
		case res := <-resCh:
			received++
			if res.err != nil {
				p.log.Debug("patterns peer warm fetch failed", "peer", res.peer, "error", res.err)
				continue
			}
			if res.bodyBytes > 0 {
				totalPeerBytes += int64(res.bodyBytes)
			}
			if res.snapshot == nil || len(res.snapshot.EntriesByKey) == 0 {
				continue
			}
			if res.snapshot.SavedAtUnixNano > mergedSavedAt {
				mergedSavedAt = res.snapshot.SavedAtUnixNano
			}
			for key, incoming := range res.snapshot.EntriesByKey {
				if strings.TrimSpace(key) == "" || len(incoming.Value) == 0 {
					continue
				}
				existing, ok := merged.EntriesByKey[key]
				if ok && existing.UpdatedAtUnixNano >= incoming.UpdatedAtUnixNano {
					continue
				}
				merged.EntriesByKey[key] = patternSnapshotEntry{
					Value:             append([]byte(nil), incoming.Value...),
					UpdatedAtUnixNano: incoming.UpdatedAtUnixNano,
				}
			}
		case <-deadline:
			received = len(peers)
		}
	}
	p.metrics.RecordPatternsRestoreBytes("peer", totalPeerBytes)

	appliedEntries, appliedPatterns := p.applyPatternsSnapshot(merged, patternDedupSourcePeer)
	if appliedEntries == 0 || mergedSavedAt <= minSavedAt {
		return false, mergedSavedAt, nil
	}
	p.metrics.RecordPatternsRestoredFromPeers(appliedPatterns, appliedEntries)

	p.log.Info(
		"patterns snapshot warmed from peers",
		"saved_at", time.Unix(0, mergedSavedAt).UTC().Format(time.RFC3339Nano),
		"entries_applied", appliedEntries,
		"patterns_applied", appliedPatterns,
		"entries_updated", appliedEntries,
		"patterns_updated", appliedPatterns,
		"peers", len(peers),
		"bytes", totalPeerBytes,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, mergedSavedAt, nil
}

func (p *Proxy) warmPatternsOnStartup() {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return
	}
	p.patternsWarmReady.Store(false)
	defer p.patternsWarmReady.Store(true)

	var diskSavedAt int64
	loadedFromDisk, savedAt, err := p.restorePatternsFromDisk()
	if err != nil {
		p.log.Warn("patterns snapshot disk restore failed", "error", err)
	}
	if loadedFromDisk {
		diskSavedAt = savedAt
	}

	type peerWarmResult struct {
		ok      bool
		savedAt int64
		err     error
	}
	timeout := p.patternsPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	resCh := make(chan peerWarmResult, 1)
	go func() {
		ok, savedAt, peerErr := p.restorePatternsFromPeers(diskSavedAt)
		resCh <- peerWarmResult{ok: ok, savedAt: savedAt, err: peerErr}
	}()

	var res peerWarmResult
	select {
	case res = <-resCh:
	case <-time.After(timeout):
		p.log.Warn("patterns snapshot peer warm timed out", "timeout", timeout.String())
		res = peerWarmResult{ok: false}
	}
	if res.err != nil {
		p.log.Warn("patterns snapshot peer warm failed", "error", res.err)
	} else if res.ok {
		if persistErr := p.persistPatternsNow("startup_peer_warm"); persistErr != nil {
			p.log.Warn("patterns snapshot persistence after peer warm failed", "error", persistErr)
		}
	}
}

func (p *Proxy) startPatternsPersistenceLoop() {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" || p.patternsPersistInterval <= 0 {
		return
	}
	if p.patternsPersistStarted.Swap(true) {
		return
	}
	p.log.Info(
		"patterns snapshot backup enabled",
		"path", p.patternsPersistPath,
		"interval", p.patternsPersistInterval.String(),
	)

	go func() {
		defer close(p.patternsPersistDone)
		ticker := time.NewTicker(p.patternsPersistInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.compactPatternsSnapshot(patternDedupSourceMemory, "periodic")
				if err := p.persistPatternsNow("periodic"); err != nil {
					p.log.Warn("periodic patterns snapshot persistence failed", "error", err)
				}
			case <-p.patternsPersistStop:
				return
			}
		}
	}()
}

func (p *Proxy) stopPatternsPersistenceLoop(ctx context.Context) {
	if !p.patternsPersistStarted.Load() {
		return
	}
	select {
	case <-p.patternsPersistStop:
	default:
		close(p.patternsPersistStop)
	}

	if ctx == nil {
		<-p.patternsPersistDone
		return
	}
	select {
	case <-p.patternsPersistDone:
	case <-ctx.Done():
		p.log.Warn("timeout waiting for patterns persistence loop stop", "error", ctx.Err())
	}
}
