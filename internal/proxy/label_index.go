package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (p *Proxy) buildLabelValuesIndexSnapshot(now time.Time) labelValuesIndexSnapshot {
	p.labelValuesIndexMu.RLock()
	defer p.labelValuesIndexMu.RUnlock()

	states := make(map[string]map[string]labelValueIndexEntry, len(p.labelValuesIndex))
	for key, state := range p.labelValuesIndex {
		if state == nil || len(state.entries) == 0 {
			continue
		}
		entries := make(map[string]labelValueIndexEntry, len(state.entries))
		for value, entry := range state.entries {
			entries[value] = entry
		}
		states[key] = entries
	}

	return labelValuesIndexSnapshot{
		Version:         1,
		SavedAtUnixNano: now.UnixNano(),
		StatesByKey:     states,
	}
}

func (p *Proxy) applyLabelValuesIndexSnapshot(snapshot labelValuesIndexSnapshot) (states int, values int) {
	restored := make(map[string]*labelValuesIndexState, len(snapshot.StatesByKey))
	for key, entries := range snapshot.StatesByKey {
		if len(entries) == 0 {
			continue
		}
		copied := make(map[string]labelValueIndexEntry, len(entries))
		for value, entry := range entries {
			if strings.TrimSpace(value) == "" {
				continue
			}
			copied[value] = entry
			values++
		}
		if len(copied) == 0 {
			continue
		}
		restored[key] = &labelValuesIndexState{
			entries: copied,
			dirty:   true,
		}
		states++
	}

	p.labelValuesIndexMu.Lock()
	p.labelValuesIndex = restored
	p.labelValuesIndexMu.Unlock()
	p.labelValuesIndexPersistDirty.Store(false)
	return states, values
}

func (p *Proxy) persistLabelValuesIndexNow(reason string) error {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" {
		return nil
	}
	if reason == "periodic" && !p.labelValuesIndexPersistDirty.Load() {
		return nil
	}
	startedAt := time.Now()

	snapshot := p.buildLabelValuesIndexSnapshot(time.Now().UTC())
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal label index snapshot: %w", err)
	}
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	snapshotDigest := sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))

	path := p.labelValuesIndexPersistPath
	wrote, err := writeSnapshotFileIfChanged(path, data, snapshotDigest, &p.labelValuesIndexPersistDigest, &p.labelValuesIndexPersistDigestReady)
	if err != nil {
		return fmt.Errorf("persist label index snapshot: %w", err)
	}

	if p.cache != nil {
		ttl := p.labelValuesIndexStartupStale * 3
		minTTL := p.labelValuesIndexPersistInterval * 2
		if ttl < minTTL {
			ttl = minTTL
		}
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cacheSnapshotBlobLocally(labelValuesIndexSnapshotCacheKey, data, ttl)
	}

	states, values := p.labelValuesIndexCardinality()
	if reason == "periodic" {
		p.log.Debug("label values index snapshot persisted", "path", path, "wrote_disk", wrote, "states", states, "values", values, "bytes", len(data), "duration_ms", time.Since(startedAt).Milliseconds())
	} else {
		p.log.Info("label values index snapshot persisted", "reason", reason, "path", path, "wrote_disk", wrote, "states", states, "values", values, "bytes", len(data), "duration_ms", time.Since(startedAt).Milliseconds())
	}
	p.labelValuesIndexPersistDirty.Store(false)
	return nil
}

func (p *Proxy) restoreLabelValuesIndexFromDisk() (bool, int64, error) {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" {
		return false, 0, nil
	}
	startedAt := time.Now()

	data, err := os.ReadFile(p.labelValuesIndexPersistPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("read label index snapshot: %w", err)
	}
	var snapshot labelValuesIndexSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return false, 0, fmt.Errorf("decode label index snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return false, 0, fmt.Errorf("unsupported label index snapshot version: %d", snapshot.Version)
	}
	if snapshot.SavedAtUnixNano <= 0 {
		return false, 0, fmt.Errorf("invalid label index snapshot timestamp: %d", snapshot.SavedAtUnixNano)
	}
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	p.labelValuesIndexPersistDigest = sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))
	p.labelValuesIndexPersistDigestReady = true

	states, values := p.applyLabelValuesIndexSnapshot(snapshot)
	if p.cache != nil {
		ttl := p.labelValuesIndexStartupStale * 3
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cacheSnapshotBlobLocally(labelValuesIndexSnapshotCacheKey, data, ttl)
	}
	p.log.Info(
		"label values index restored from disk",
		"path", p.labelValuesIndexPersistPath,
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"states", states,
		"values", values,
		"bytes", len(data),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) fetchLabelValuesIndexSnapshotFromPeer(peerAddr string, timeout time.Duration) (*labelValuesIndexSnapshot, int, error) {
	if strings.TrimSpace(peerAddr) == "" {
		return nil, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	endpoint := fmt.Sprintf("http://%s/_cache/get?key=%s", peerAddr, url.QueryEscape(labelValuesIndexSnapshotCacheKey))
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
	body, err := readBodyLimited(resp.Body, maxLabelValuesPeerSnapshotBytes)
	if err != nil {
		return nil, 0, err
	}
	var snapshot labelValuesIndexSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, 0, err
	}
	if snapshot.Version != 1 || snapshot.SavedAtUnixNano <= 0 {
		return nil, 0, fmt.Errorf("invalid label-values snapshot metadata from peer %s", peerAddr)
	}
	return &snapshot, len(body), nil
}

func mergeLabelValuesIndexEntry(existing, incoming labelValueIndexEntry) labelValueIndexEntry {
	if incoming.SeenCount > existing.SeenCount {
		existing.SeenCount = incoming.SeenCount
	}
	if incoming.LastSeen > existing.LastSeen {
		existing.LastSeen = incoming.LastSeen
	}
	return existing
}

func mergeLabelValuesIndexSnapshot(dst *labelValuesIndexSnapshot, src *labelValuesIndexSnapshot) {
	if dst == nil || src == nil {
		return
	}
	if dst.StatesByKey == nil {
		dst.StatesByKey = make(map[string]map[string]labelValueIndexEntry)
	}
	if src.SavedAtUnixNano > dst.SavedAtUnixNano {
		dst.SavedAtUnixNano = src.SavedAtUnixNano
	}
	for key, entries := range src.StatesByKey {
		if len(entries) == 0 {
			continue
		}
		dstEntries, ok := dst.StatesByKey[key]
		if !ok {
			dstEntries = make(map[string]labelValueIndexEntry, len(entries))
			dst.StatesByKey[key] = dstEntries
		}
		for value, entry := range entries {
			if strings.TrimSpace(value) == "" {
				continue
			}
			if existing, exists := dstEntries[value]; exists {
				dstEntries[value] = mergeLabelValuesIndexEntry(existing, entry)
				continue
			}
			dstEntries[value] = entry
		}
	}
}

func (p *Proxy) restoreLabelValuesIndexFromPeers(minSavedAt int64) (bool, int64, error) {
	if !p.labelValuesIndexedCache || p.peerCache == nil {
		return false, 0, nil
	}
	startedAt := time.Now()
	peers := p.peerCache.Peers()
	if len(peers) == 0 {
		return false, 0, nil
	}

	timeout := p.labelValuesIndexPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	type peerResult struct {
		snapshot  *labelValuesIndexSnapshot
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
			snapshot, bodyBytes, err := p.fetchLabelValuesIndexSnapshotFromPeer(addr, timeout)
			resCh <- peerResult{snapshot: snapshot, bodyBytes: bodyBytes, err: err, peer: addr}
		}(peerAddr)
	}

	merged := p.buildLabelValuesIndexSnapshot(time.Now().UTC())
	if merged.Version == 0 {
		merged.Version = 1
	}
	if merged.StatesByKey == nil {
		merged.StatesByKey = make(map[string]map[string]labelValueIndexEntry)
	}
	mergedSavedAt := minSavedAt
	sawNewer := false
	totalPeerBytes := 0
	deadline := time.After(timeout)
	received := 0
	for received < len(peers) {
		select {
		case res := <-resCh:
			received++
			if res.err != nil {
				p.log.Debug("label values peer warm fetch failed", "peer", res.peer, "error", res.err)
				continue
			}
			totalPeerBytes += res.bodyBytes
			if res.snapshot == nil || len(res.snapshot.StatesByKey) == 0 || res.snapshot.SavedAtUnixNano <= minSavedAt {
				continue
			}
			sawNewer = true
			if res.snapshot.SavedAtUnixNano > mergedSavedAt {
				mergedSavedAt = res.snapshot.SavedAtUnixNano
			}
			mergeLabelValuesIndexSnapshot(&merged, res.snapshot)
		case <-deadline:
			received = len(peers)
		}
	}
	if !sawNewer {
		return false, mergedSavedAt, nil
	}

	states, values := p.applyLabelValuesIndexSnapshot(merged)
	p.log.Info(
		"label values index warmed from peers",
		"saved_at", time.Unix(0, mergedSavedAt).UTC().Format(time.RFC3339Nano),
		"states", states,
		"values", values,
		"peers", len(peers),
		"bytes", totalPeerBytes,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, mergedSavedAt, nil
}

func (p *Proxy) warmLabelValuesIndexOnStartup() {
	p.labelValuesIndexWarmReady.Store(false)
	defer p.labelValuesIndexWarmReady.Store(true)

	var diskSavedAt int64
	loadedFromDisk, savedAt, err := p.restoreLabelValuesIndexFromDisk()
	if err != nil {
		p.log.Warn("label values index disk restore failed", "error", err)
	}
	if loadedFromDisk {
		diskSavedAt = savedAt
	}

	diskFresh := loadedFromDisk && time.Since(time.Unix(0, diskSavedAt)) <= p.labelValuesIndexStartupStale
	if !diskFresh {
		if p.cache != nil {
			p.cache.Invalidate(labelValuesIndexSnapshotCacheKey)
		}
		type peerWarmResult struct {
			ok      bool
			savedAt int64
			err     error
		}
		timeout := p.labelValuesIndexPeerWarmTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		resCh := make(chan peerWarmResult, 1)
		go func() {
			ok, savedAt, peerErr := p.restoreLabelValuesIndexFromPeers(diskSavedAt)
			resCh <- peerWarmResult{ok: ok, savedAt: savedAt, err: peerErr}
		}()

		var res peerWarmResult
		select {
		case res = <-resCh:
		case <-time.After(timeout):
			p.log.Warn("label values index peer warm timed out", "timeout", timeout.String())
			res = peerWarmResult{ok: false}
		}
		if res.err != nil {
			p.log.Warn("label values index peer warm failed", "error", res.err)
		} else if res.ok {
			diskSavedAt = res.savedAt
			if persistErr := p.persistLabelValuesIndexNow("startup_peer_warm"); persistErr != nil {
				p.log.Warn("label values index persistence after peer warm failed", "error", persistErr)
			}
		}
	}
}

func (p *Proxy) startLabelValuesIndexPersistenceLoop() {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" || p.labelValuesIndexPersistInterval <= 0 {
		return
	}
	if p.labelValuesIndexPersistStarted.Swap(true) {
		return
	}
	go func() {
		defer close(p.labelValuesIndexPersistDone)
		ticker := time.NewTicker(p.labelValuesIndexPersistInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := p.persistLabelValuesIndexNow("periodic"); err != nil {
					p.log.Warn("periodic label values index persistence failed", "error", err)
				}
			case <-p.labelValuesIndexPersistStop:
				return
			}
		}
	}()
}

func (p *Proxy) stopLabelValuesIndexPersistenceLoop(ctx context.Context) {
	if !p.labelValuesIndexPersistStarted.Load() {
		return
	}
	select {
	case <-p.labelValuesIndexPersistStop:
	default:
		close(p.labelValuesIndexPersistStop)
	}

	if ctx == nil {
		<-p.labelValuesIndexPersistDone
		return
	}
	select {
	case <-p.labelValuesIndexPersistDone:
	case <-ctx.Done():
		p.log.Warn("timeout waiting for label values persistence loop stop", "error", ctx.Err())
	}
}

func (p *Proxy) labelValuesIndexCardinality() (states int, values int) {
	p.labelValuesIndexMu.RLock()
	defer p.labelValuesIndexMu.RUnlock()
	for _, state := range p.labelValuesIndex {
		if state == nil || len(state.entries) == 0 {
			continue
		}
		states++
		values += len(state.entries)
	}
	return states, values
}

func (p *Proxy) labelValuesBrowseMode(rawQuery string) bool {
	trimmed := strings.TrimSpace(rawQuery)
	return trimmed == "" || trimmed == "*"
}

func (p *Proxy) defaultLabelValuesLimit(limitRaw string) int {
	if strings.TrimSpace(limitRaw) != "" {
		limit := parsePositiveInt(limitRaw, p.labelValuesHotLimit)
		if limit > maxLimitValue {
			limit = maxLimitValue
		}
		return limit
	}
	limit := p.labelValuesHotLimit
	if limit <= 0 {
		limit = 200
	}
	if limit > maxLimitValue {
		limit = maxLimitValue
	}
	return limit
}

func parsePositiveInt(raw string, fallback int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func parseNonNegativeInt(raw string, fallback int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func normalizeLabelValueSearch(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func labelValuesIndexKey(orgID, labelName string) string {
	return orgID + "|" + strings.ToLower(strings.TrimSpace(labelName))
}

func labelValueIndexLess(aName string, a labelValueIndexEntry, bName string, b labelValueIndexEntry) bool {
	if a.SeenCount != b.SeenCount {
		return a.SeenCount > b.SeenCount
	}
	if a.LastSeen != b.LastSeen {
		return a.LastSeen > b.LastSeen
	}
	return aName < bName
}

func (p *Proxy) labelValueIndexEnsureOrderedLocked(index *labelValuesIndexState) {
	if index == nil {
		return
	}
	if !index.dirty && len(index.ordered) > 0 {
		return
	}
	ordered := make([]string, 0, len(index.entries))
	for value := range index.entries {
		ordered = append(ordered, value)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left := index.entries[ordered[i]]
		right := index.entries[ordered[j]]
		return labelValueIndexLess(ordered[i], left, ordered[j], right)
	})
	index.ordered = ordered
	index.dirty = false
}

func (p *Proxy) updateLabelValuesIndex(orgID, labelName string, values []string) {
	if !p.labelValuesIndexedCache || len(values) == 0 {
		return
	}
	key := labelValuesIndexKey(orgID, labelName)
	now := time.Now().UnixNano()

	p.labelValuesIndexMu.Lock()
	defer p.labelValuesIndexMu.Unlock()

	index, ok := p.labelValuesIndex[key]
	structuralChange := false
	if !ok {
		index = &labelValuesIndexState{entries: make(map[string]labelValueIndexEntry, len(values))}
		p.labelValuesIndex[key] = index
		structuralChange = true
	}

	for _, value := range values {
		if _, exists := index.entries[value]; !exists {
			structuralChange = true
		}
		entry := index.entries[value]
		if entry.SeenCount < ^uint32(0) {
			entry.SeenCount++
		}
		entry.LastSeen = now
		index.entries[value] = entry
	}
	index.dirty = true

	maxEntries := p.labelValuesIndexMaxEntries
	if maxEntries <= 0 || len(index.entries) <= maxEntries {
		if structuralChange {
			p.labelValuesIndexPersistDirty.Store(true)
		}
		return
	}
	p.labelValueIndexEnsureOrderedLocked(index)
	if len(index.ordered) <= maxEntries {
		if structuralChange {
			p.labelValuesIndexPersistDirty.Store(true)
		}
		return
	}
	keep := index.ordered[:maxEntries]
	pruned := make(map[string]labelValueIndexEntry, len(keep))
	for _, value := range keep {
		pruned[value] = index.entries[value]
	}
	index.entries = pruned
	index.ordered = append([]string(nil), keep...)
	index.dirty = false
	structuralChange = true
	if structuralChange {
		p.labelValuesIndexPersistDirty.Store(true)
	}
}

func (p *Proxy) selectLabelValuesFromIndex(orgID, labelName, search string, offset, limit int) ([]string, bool) {
	if !p.labelValuesIndexedCache {
		return nil, false
	}
	if limit <= 0 {
		limit = p.labelValuesHotLimit
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > maxLimitValue {
		limit = maxLimitValue
	}
	if offset < 0 {
		offset = 0
	}

	key := labelValuesIndexKey(orgID, labelName)
	search = normalizeLabelValueSearch(search)

	p.labelValuesIndexMu.Lock()
	defer p.labelValuesIndexMu.Unlock()

	index, ok := p.labelValuesIndex[key]
	if !ok || len(index.entries) == 0 {
		return nil, false
	}
	p.labelValueIndexEnsureOrderedLocked(index)
	if len(index.ordered) == 0 {
		return nil, false
	}

	// Keep preallocation fixed-size so allocation cannot scale with request input.
	values := make([]string, 0, maxUserDrivenSlicePrealloc)
	seen := 0
	for _, candidate := range index.ordered {
		if search != "" && !strings.Contains(strings.ToLower(candidate), search) {
			continue
		}
		if seen < offset {
			seen++
			continue
		}
		values = append(values, candidate)
		if len(values) >= limit {
			break
		}
	}
	return values, true
}

func selectLabelValuesWindow(values []string, search string, offset, limit int) []string {
	if limit <= 0 {
		limit = maxLimitValue
	}
	if limit > maxLimitValue {
		limit = maxLimitValue
	}
	if offset < 0 {
		offset = 0
	}

	search = normalizeLabelValueSearch(search)
	// Keep preallocation fixed-size so allocation cannot scale with request input.
	out := make([]string, 0, maxUserDrivenSlicePrealloc)
	seen := 0
	for _, value := range values {
		if search != "" && !strings.Contains(strings.ToLower(value), search) {
			continue
		}
		if seen < offset {
			seen++
			continue
		}
		out = append(out, value)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (p *Proxy) endpointUsesSharedReadCache(endpoint string) bool {
	switch endpoint {
	case "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels":
		return true
	default:
		return false
	}
}
