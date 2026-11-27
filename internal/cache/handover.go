package cache

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Restart hand-over of the memory tier (L1) through the disk tier (L2).
//
// FlushHandover runs once on a graceful shutdown. It walks L1 from the most to
// the least recently used entry and, within its context's deadline:
//   - gives every entry the disk would hold (not memory-only, not a non-owner
//     shadow, not a patterns entry) the disk recency of its rank, and writes
//     to the disk the ones it lacks (a write the queue refused under load, a
//     record the disk evicted while the entry stayed in memory);
//   - keeps the ranks and hit counts in the hand-over bucket;
//   - writes there the memory-only values marked restorable
//     (SetLocalOnlyRestorableWithTTL), hottest first, up to a byte budget.
//
// WarmFromHandover runs once at start-up. It loads the ranked entries back
// into L1, hottest first, within a byte budget and its context's deadline:
//   - a disk entry exactly as a disk hit would promote it (the record's expiry,
//     an unknown age, never over a fresher memory entry, fenced by a purge);
//   - a memory-only value only when the writer's scope (build and
//     configuration) equals this process's, and it stays memory-only;
//   - nothing when this process's clock reads earlier than the flush started.
//
// Entries go in behind the entries live requests stored, never evicting any.

// StartupStatsWindow is how long after TrackStartupWindow the memory tier's
// lookups and hits are counted for the start-up hit ratio.
const StartupStatsWindow = 10 * time.Minute

// StartupRatioWindows are the windows after TrackStartupWindow for which every
// tier's hit ratio is kept: live until the window has passed, then frozen.
var StartupRatioWindows = [startupRatioWindowCount]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

const (
	startupRatioWindowCount = 3
	// startupTierCount is the response cache's tiers: memory, disk, peer.
	startupTierCount = 3
)

// TierLookups counts one tier's lookups and the hits among them, as
// cache_tier_requests_total and cache_tier_hits_total do.
type TierLookups struct{ Requests, Hits int64 }

// handoverWarmShareDivisor caps a warm-up at half of the memory tier's bytes,
// leaving the rest to what live requests store meanwhile.
const handoverWarmShareDivisor = 2

// HandoverFlushOptions bounds a shutdown flush.
type HandoverFlushOptions struct {
	// Scope identifies the writer's build and configuration; only a process
	// with the same scope restores the memory-only values.
	Scope string
	// MemoryBytes is the most memory-only value bytes written (0: none).
	MemoryBytes int64
}

// HandoverFlushReport is what a shutdown flush left for the next start.
type HandoverFlushReport struct {
	Ranked        int
	DiskWrites    int
	MemoryEntries int
	MemoryBytes   int64
	Complete      bool
	Duration      time.Duration
}

// HandoverWarmOptions bounds a start-up warm-up.
type HandoverWarmOptions struct {
	Scope    string
	MaxBytes int64
	// OnPlan, when set, is called once with the plan, before any value is
	// read. It must not block: the warm-up waits for it.
	OnPlan func(RestoreProgress)
}

// HandoverWarmReport is what a start-up warm-up restored.
type HandoverWarmReport struct {
	Found         bool
	DiskEntries   int
	MemoryEntries int
	Bytes         int64
	// Skipped counts ranked entries not restored: expired, superseded by a
	// fresher memory entry, over the budget, for another scope, unreadable.
	Skipped  int
	Reason   string
	Duration time.Duration
	// Progress is the final plan and counts, with the skips by reason.
	Progress RestoreProgress
}

// HandoverStats is the restart hand-over's state for the metrics.
type HandoverStats struct {
	PreviousFlushFound         bool
	PreviousFlushComplete      bool
	PreviousFlushRanked        int64
	PreviousFlushDiskWrites    int64
	PreviousFlushMemoryEntries int64
	PreviousFlushMemoryBytes   int64
	PreviousFlushDuration      time.Duration
	WarmDone                   bool
	WarmDiskEntries            int64
	WarmMemoryEntries          int64
	WarmSkipped                int64
	WarmBytes                  int64
	WarmDuration               time.Duration
	// Restore is the warm-up's plan and progress, updated after every batch;
	// PeerRestore the peer bulk restore's (restore_peer.go).
	Restore     RestoreProgress
	PeerRestore RestoreProgress
	// IndexRebuildDuration and IndexRebuildEntries are the disk tier's start-up
	// index rebuild, which runs before the proxy listens.
	IndexRebuildDuration time.Duration
	IndexRebuildEntries  int64
	// StartupL1Requests and StartupL1Hits count the memory tier's lookups
	// and hits in the first StartupStatsWindow after TrackStartupWindow
	// (frozen once it passes).
	StartupTracked    bool
	StartupL1Requests int64
	StartupL1Hits     int64
	// SinceStart holds, per StartupRatioWindows entry, the lookups and hits of
	// the memory, disk and peer tiers (in that order) since TrackStartupWindow.
	SinceStart [startupRatioWindowCount][startupTierCount]TierLookups
}

type handoverCounters struct {
	// mu guards the warm-up's progress, written once per batch.
	mu                sync.Mutex
	progress          RestoreProgress
	peerProgress      RestoreProgress
	peerRestoring     atomic.Bool
	warmDiskEntries   int64
	warmMemoryEntries int64
	warmDone          atomic.Bool
	startupTracked    atomic.Bool
	startupFrozen     atomic.Bool
	startupRequests   atomic.Int64
	startupHits       atomic.Int64
	// sinceStart and sinceStartFrozen are the frozen windows (under mu).
	sinceStart       [startupRatioWindowCount][startupTierCount]TierLookups
	sinceStartFrozen [startupRatioWindowCount]bool
}

// EnableHandoverHits counts each memory entry's hits (in the promoter, off the
// request path) so a shutdown flush can keep them. Off by default, and a no-op
// without a disk tier (call it after SetL2).
func (c *Cache) EnableHandoverHits() {
	if c == nil || c.disabled || c.l2 == nil {
		return
	}
	c.hitsMu.Lock()
	if c.hits == nil {
		c.hits = make(map[string]uint32)
	}
	c.hitsMu.Unlock()
	c.hitsOn.Store(true)
}

// pruneHitsLocked drops counts of keys no longer stored (a promotion counted
// after its entry was removed). Called with mu held, by the cleanup loop.
func (c *Cache) pruneHitsLocked() {
	if !c.hitsOn.Load() {
		return
	}
	c.hitsMu.Lock()
	for key := range c.hits {
		if _, ok := c.entries[key]; !ok {
			delete(c.hits, key)
		}
	}
	c.hitsMu.Unlock()
}

func (c *Cache) hitsOf(key string) uint32 {
	if !c.hitsOn.Load() {
		return 0
	}
	c.hitsMu.Lock()
	defer c.hitsMu.Unlock()
	return c.hits[key]
}

// SetLocalOnlyRestorableWithTTL is SetLocalOnlyWithTTL for a completed answer
// whose key carries every input of the answer (tenant, routing scope, request,
// key versions): a shutdown flush may keep it for a process of the same build
// and configuration, which restores it memory-only.
func (c *Cache) SetLocalOnlyRestorableWithTTL(key string, value []byte, ttl time.Duration) {
	if c == nil || c.disabled {
		return
	}
	size := entrySize(key, value)
	if size > c.maxBytes/10 {
		c.layerL1[c.layer(key)].rejections.Add(1)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.storeEntryLocked(key, value, ttl, size, true, true, false, 0) {
		e := c.entries[key]
		e.restorable = true
		c.entries[key] = e
	}
}

// TrackStartupWindow starts counting the memory tier's lookups and hits for
// StartupStatsWindow, and every tier's for each of StartupRatioWindows; the
// counts of a window freeze when it passes. Close stops it.
func (c *Cache) TrackStartupWindow() {
	if c == nil || c.disabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.startupTimers != nil {
		return
	}
	c.handoverStats.startupTracked.Store(true)
	c.startupTimers = []*time.Timer{time.AfterFunc(StartupStatsWindow, c.freezeStartupWindow)}
	for i, window := range StartupRatioWindows {
		c.startupTimers = append(c.startupTimers, time.AfterFunc(window, func() { c.freezeRatioWindow(i) }))
	}
}

// tierLookups reads the memory, disk and peer tiers' counters. Hits are read
// first: a lookup counts its request before its hit, so a concurrent reading
// never holds more hits than requests.
func (c *Cache) tierLookups() (out [startupTierCount]TierLookups) {
	for i, s := range [startupTierCount]*tierStats{&c.l1Stats, &c.l2Stats, &c.l3Stats} {
		out[i].Hits = s.hits.Load()
		out[i].Requests = s.requests.Load()
	}
	return out
}

func (c *Cache) freezeRatioWindow(i int) {
	counts := c.tierLookups()
	s := &c.handoverStats
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sinceStart[i], s.sinceStartFrozen[i] = counts, true
}

func (c *Cache) freezeStartupWindow() {
	c.handoverStats.startupRequests.Store(c.l1Stats.requests.Load())
	c.handoverStats.startupHits.Store(c.l1Stats.hits.Load())
	c.handoverStats.startupFrozen.Store(true)
}

// HandoverStats returns the hand-over state for the metrics.
func (c *Cache) HandoverStats() HandoverStats {
	if c == nil || c.disabled {
		return HandoverStats{}
	}
	s := &c.handoverStats
	// Read done first: once it is set the progress below is final.
	out := HandoverStats{
		WarmDone:       s.warmDone.Load(),
		StartupTracked: s.startupTracked.Load(),
	}
	s.mu.Lock()
	out.Restore, out.PeerRestore = s.progress, s.peerProgress
	if out.StartupTracked {
		live := c.tierLookups()
		for i := range out.SinceStart {
			if s.sinceStartFrozen[i] {
				out.SinceStart[i] = s.sinceStart[i]
			} else {
				out.SinceStart[i] = live
			}
		}
	}
	out.WarmDiskEntries, out.WarmMemoryEntries = s.warmDiskEntries, s.warmMemoryEntries
	s.mu.Unlock()
	if out.Restore.Outcome == RestoreRunning {
		out.Restore.Elapsed = clockNow().Sub(out.Restore.Started)
	}
	if out.PeerRestore.Outcome == RestoreRunning {
		out.PeerRestore.Elapsed = clockNow().Sub(out.PeerRestore.Started)
	}
	out.WarmSkipped, out.WarmBytes, out.WarmDuration = out.Restore.SkippedEntries(), out.Restore.RestoredBytes, out.Restore.Elapsed
	if c.l2 != nil {
		out.IndexRebuildDuration, out.IndexRebuildEntries = c.l2.rebuildDuration, c.l2.rebuildEntries
	}
	if out.StartupTracked {
		if s.startupFrozen.Load() {
			out.StartupL1Requests, out.StartupL1Hits = s.startupRequests.Load(), s.startupHits.Load()
		} else {
			out.StartupL1Requests, out.StartupL1Hits = c.l1Stats.requests.Load(), c.l1Stats.hits.Load()
		}
	}
	if m, ok := c.l2.PreviousFlush(); ok {
		out.PreviousFlushFound = true
		out.PreviousFlushComplete = m.Complete
		out.PreviousFlushRanked = int64(m.Ranked)
		out.PreviousFlushDiskWrites = int64(m.DiskWrites)
		out.PreviousFlushMemoryEntries = int64(m.MemoryEntries)
		out.PreviousFlushMemoryBytes = m.MemoryBytes
		out.PreviousFlushDuration = m.Duration
	}
	return out
}

// HasHandover reports whether the disk tier holds a valid hand-over the
// warm-up has not taken yet.
func (c *Cache) HasHandover() bool {
	if c == nil || c.disabled || c.l2 == nil {
		return false
	}
	c.l2.writeMu.Lock()
	defer c.l2.writeMu.Unlock()
	return c.l2.handover != nil
}

// DropHandover deletes a hand-over the warm-up will not read.
func (c *Cache) DropHandover() {
	if c == nil || c.disabled || c.l2 == nil {
		return
	}
	c.l2.DropHandover()
}

type handoverCandidate struct {
	key    string
	value  []byte
	expiry int64
	memory bool
	hits   uint32
}

// handoverCandidates lists the entries a flush may keep, most recently used
// first: everything the disk would hold, and the restorable memory-only values.
func (c *Cache) handoverCandidates() []handoverCandidate {
	now := clockNow()
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]handoverCandidate, 0, len(c.entries))
	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		key := elem.Value.(*lruEntry).key
		e, ok := c.entries[key]
		if !ok || e.shadow || skipL2WriteForKey(key) || (e.memOnly && !e.restorable) || !handoverKeyFits(key) {
			continue
		}
		expiry := e.expiresAt
		if e.sourceExpiresAt != 0 {
			if source := time.Unix(0, e.sourceExpiresAt); source.Before(expiry) {
				expiry = source
			}
		}
		if !expiry.After(now) {
			continue
		}
		out = append(out, handoverCandidate{key: key, value: e.value, expiry: expiry.UnixNano(), memory: e.memOnly, hits: c.hitsOf(key)})
	}
	return out
}

// FlushHandover writes the memory tier's hand-over to the disk tier for the
// next start (see the comment at the top of this file). It returns once the
// records are written or the context ends; the manifest that makes them valid
// is written by the disk tier's Close, as the file's last transaction. A
// failed write leaves no manifest, so the next start ignores every record.
func (c *Cache) FlushHandover(ctx context.Context, opts HandoverFlushOptions) (HandoverFlushReport, error) {
	var report HandoverFlushReport
	if c == nil || c.disabled || c.l2 == nil {
		return report, nil
	}
	started := clockNow()
	dc := c.l2
	gen, err := dc.beginHandover()
	if err != nil {
		return report, err
	}
	candidates := c.handoverCandidates()
	written, complete := writeMissingDiskRecords(ctx, dc, gen, candidates)
	records := handoverRecordsFor(dc, candidates, written, opts.MemoryBytes, &report)
	report.Ranked, report.MemoryEntries, report.MemoryBytes = 0, 0, 0
	if err := putHandoverChunks(ctx, dc, gen, records, &report); err != nil {
		return report, err
	}
	report.Complete = complete && ctx.Err() == nil
	report.Duration = clockNow().Sub(started)
	if !dc.setHandoverManifest(gen, HandoverManifest{
		Scope:         opts.Scope,
		FlushedAt:     started.UnixNano(),
		Duration:      report.Duration,
		Complete:      report.Complete,
		Ranked:        report.Ranked,
		DiskWrites:    report.DiskWrites,
		MemoryEntries: report.MemoryEntries,
		MemoryBytes:   report.MemoryBytes,
	}) {
		return report, errHandoverSuperseded
	}
	return report, nil
}

// writeMissingDiskRecords gives the disk-eligible entries the disk recency of
// their rank and writes the ones the disk lacks, hottest first, so a deadline
// drops the coldest. It returns the keys written and whether it finished.
func writeMissingDiskRecords(ctx context.Context, dc *DiskCache, gen uint64, candidates []handoverCandidate) (map[string]bool, bool) {
	diskKeys := make([]string, 0, len(candidates))
	for _, cand := range candidates {
		if !cand.memory {
			diskKeys = append(diskKeys, cand.key)
		}
	}
	present, access := dc.planHandover(diskKeys)
	written := make(map[string]bool)
	complete := true
	di := 0
	for _, cand := range candidates {
		if cand.memory {
			continue
		}
		i := di
		di++
		if present[i] {
			continue
		}
		if ctx.Err() != nil {
			complete = false
			break
		}
		remaining := time.Unix(0, cand.expiry).Sub(clockNow())
		if remaining <= 0 {
			continue
		}
		if !dc.queueRoom(cand.key, cand.value) {
			dc.Flush()
		}
		dc.set(cand.key, cand.value, remaining, access[i], gen)
		written[cand.key] = true
	}
	// Commit the queue while there is time; the disk tier's Close commits
	// what is left before it writes the manifest.
	if ctx.Err() == nil {
		dc.Flush()
	}
	return written, complete
}

// handoverRecordsFor lists, in one rank order (the memory tier's recency), the
// ranks of what the disk now holds and the restorable memory-only values
// within the byte budget; it counts the disk writes that landed. Memory values
// stay the memory tier's own slices here: putHandoverChunks encodes them one
// chunk at a time, so the flush holds at most one chunk of encoded copies.
func handoverRecordsFor(dc *DiskCache, candidates []handoverCandidate, written map[string]bool, memoryBudget int64, report *HandoverFlushReport) []handoverRecord {
	records := make([]handoverRecord, 0, len(candidates))
	var memoryBytes int64
	for rank, cand := range candidates {
		if !cand.memory {
			if dc.holds(cand.key) {
				records = append(records, handoverRecord{key: cand.key, class: handoverClassDisk, rank: uint32(rank), hits: cand.hits})
				if written[cand.key] {
					report.DiskWrites++
				}
			}
			continue
		}
		if memoryBudget <= 0 || memoryBytes+int64(len(cand.value)) > memoryBudget {
			continue
		}
		records = append(records, handoverRecord{key: cand.key, class: handoverClassMemory, rank: uint32(rank), hits: cand.hits, value: cand.value, expiry: cand.expiry, size: len(cand.value)})
		memoryBytes += int64(len(cand.value))
	}
	return records
}

// putHandoverChunks commits the records in bounded transactions, checking the
// deadline between them, and counts what reached the disk.
func putHandoverChunks(ctx context.Context, dc *DiskCache, gen uint64, records []handoverRecord, report *HandoverFlushReport) error {
	chunk := make([]handoverRecord, 0, min(len(records), handoverChunkRecords))
	for start := 0; start < len(records); {
		if ctx.Err() != nil {
			return nil
		}
		end, chunkBytes := start, 0
		for end < len(records) && end-start < handoverChunkRecords && (end == start || chunkBytes+records[end].size <= handoverChunkBytes) {
			chunkBytes += records[end].size
			end++
		}
		chunk = chunk[:0]
		for _, r := range records[start:end] {
			if r.class == handoverClassMemory {
				encoded, err := encodeDiskRecord(diskEntry{Value: r.value, ExpiresAt: r.expiry}, dc.codec)
				if err != nil {
					continue
				}
				r.payload = encoded
			}
			chunk = append(chunk, r)
		}
		if err := dc.putHandoverRecords(gen, chunk); err != nil {
			return err
		}
		for i, r := range chunk {
			if r.class == handoverClassDisk {
				report.Ranked++
			} else {
				report.MemoryEntries++
				report.MemoryBytes += int64(r.size)
			}
			chunk[i].payload = nil // the encoded copy is garbage once committed
		}
		start = end
	}
	return nil
}

// WarmFromHandover loads the hand-over a graceful shutdown left into the
// memory tier (see the package comment above) and then deletes it. It is
// called once, at start-up; a second call finds nothing.
//
// It restores in batches (restore.go, disk_restore.go), hottest first. The
// context and a purge are checked before every batch, so it stops at most one
// batch after the deadline, and a batch a purge crossed is dropped whole. All
// its goroutines have returned when it does.
func (c *Cache) WarmFromHandover(ctx context.Context, opts HandoverWarmOptions) HandoverWarmReport {
	var report HandoverWarmReport
	if c == nil || c.disabled || c.l2 == nil {
		return report
	}
	run := restoreRun{c: c, progress: RestoreProgress{Source: RestoreSourceDisk, Started: clockNow()}}
	if deadline, ok := ctx.Deadline(); ok {
		run.progress.Timeout = max(time.Until(deadline), 0)
	}
	// Read before the hand-over is taken: any purge from here on stops the
	// warm-up (0, not allowed, while one runs).
	generation, idle := c.diskPromotionGeneration()
	state := c.l2.takeHandover()
	if state == nil {
		report.Reason = RestoreNone
		// A second call must not replace what the first one restored.
		if !c.handoverStats.warmDone.Load() {
			run.finish(RestoreNone)
		}
		report.Progress = run.progress
		c.handoverStats.warmDone.Store(true)
		return report
	}
	report.Found = true
	// Hottest first, so a budget or deadline keeps the hottest. Each entry goes
	// in behind the previous one: the hottest restored entry ends nearest the
	// front, every restored entry behind what live requests stored.
	items := state.items
	sort.SliceStable(items, func(i, j int) bool { return items[i].rank < items[j].rank })
	sizes := c.l2.storedSizes(items)
	budget := min(opts.MaxBytes, int64(c.maxBytes/handoverWarmShareDivisor))
	run.progress.Outcome = RestoreRunning
	run.progress.PlannedEntries, run.progress.BudgetBytes = int64(len(items)), max(budget, 0)
	for _, size := range sizes {
		run.progress.PlannedBytes += size
	}
	run.publish()
	if opts.OnPlan != nil {
		opts.OnPlan(run.progress)
	}
	switch {
	case clockNow().UnixNano() < state.manifest.FlushedAt:
		report.Reason = "clock_behind_flush"
		run.skipRest(sizes, RestoreSkipClock)
	default:
		sameScope := opts.Scope != "" && state.manifest.Scope == opts.Scope
		report.Reason = run.restoreDisk(ctx, items, sizes, sameScope, restoreEpoch{generation: generation, idle: idle})
	}
	c.l2.DropHandover()
	run.finish(report.Reason)
	report.DiskEntries, report.MemoryEntries = int(run.disk), int(run.memory)
	report.Bytes = run.progress.RestoredBytes
	report.Skipped = int(run.progress.SkippedEntries())
	report.Duration = run.progress.Elapsed
	report.Progress = run.progress
	c.handoverStats.warmDone.Store(true)
	return report
}

// restoreRun is one warm-up's own state; publish copies it to the metrics.
type restoreRun struct {
	c            *Cache
	progress     RestoreProgress
	disk, memory int64
	// cursor is the number of planned entries processed.
	cursor int
}

func (r *restoreRun) publish() {
	s := &r.c.handoverStats
	s.mu.Lock()
	defer s.mu.Unlock()
	s.progress, s.warmDiskEntries, s.warmMemoryEntries = r.progress, r.disk, r.memory
}

func (r *restoreRun) finish(outcome string) {
	r.progress.Outcome = outcome
	r.progress.Elapsed = clockNow().Sub(r.progress.Started)
	r.publish()
}

// skipRest counts every entry not processed yet as skipped for reason.
func (r *restoreRun) skipRest(sizes []int64, reason RestoreSkip) {
	for ; r.cursor < len(sizes); r.cursor++ {
		r.progress.Skipped[reason]++
		r.progress.ProcessedEntries++
		r.progress.ProcessedBytes += sizes[r.cursor]
	}
}

// restoreEpoch is the memory tier's purge generation when the warm-up began,
// and whether no purge was running then.
type restoreEpoch struct {
	generation uint64
	idle       bool
}

// restoreDisk restores the ranked items batch by batch and returns why it
// stopped.
func (r *restoreRun) restoreDisk(ctx context.Context, items []handoverItem, sizes []int64, sameScope bool, epoch restoreEpoch) string {
	c := r.c
	budget := r.progress.BudgetBytes
	workers := restoreWorkers()
	backing := make([]diskRestoreRead, restoreBatchKeys)
	reads := make([]*diskRestoreRead, 0, restoreBatchKeys)
	entries := make([]*restoreEntry, 0, restoreBatchKeys)
	// The records the next batches decode are read into the page cache first:
	// at most the budget's worth, as a stored record is not larger than its
	// decoded value.
	ahead := c.l2.startRestoreReadahead(ctx, items, sizes, budget)
	defer ahead.stop()
	for r.cursor < len(items) {
		if ctx.Err() != nil {
			r.skipRest(sizes, RestoreSkipDeadline)
			return "deadline"
		}
		if r.progress.RestoredBytes >= budget {
			r.skipRest(sizes, RestoreSkipOverBudget)
			break
		}
		if generation, idle := c.diskPromotionGeneration(); !epoch.idle || !idle || generation != epoch.generation {
			r.skipRest(sizes, RestoreSkipPurged)
			return "purged"
		}
		slots, room := c.restoreRoom()
		if slots == 0 {
			r.skipRest(sizes, RestoreSkipMemoryFull)
			break
		}
		reads, entries = reads[:0], entries[:0]
		for i := r.cursor; i < min(r.cursor+min(restoreBatchKeys, slots), len(items)); i++ {
			item := items[i]
			read := &backing[len(reads)]
			*read = diskRestoreRead{restoreEntry: restoreEntry{key: item.key, memOnly: item.memory, hits: item.hits}}
			switch {
			case item.memory && !sameScope:
				read.skip = RestoreSkipOtherScope
			// A record's stored size (key and encoded value) never exceeds what
			// it takes in memory (key and decoded value): skip before reading.
			case r.progress.RestoredBytes+sizes[i] > budget:
				read.skip = RestoreSkipOverBudget
			}
			reads = append(reads, read)
			entries = append(entries, &read.restoreEntry)
		}
		c.holdsKeys(entries)
		c.l2.planRestore(reads)
		n := c.l2.readRestoreBatch(reads, restoreLimits{
			budget: budget - r.progress.RestoredBytes, room: room, maxEntry: int64(c.maxBytes / 10),
			bytes: restoreBatchBytes, workers: workers,
		})
		restored := r.progress.RestoredBytes
		if !c.admitRestoreBatch(entries[:n], epoch.generation, &restored, budget, func([]*restoreEntry) { c.l2.restoreFence(reads[:n]) }) {
			r.skipRest(sizes, RestoreSkipPurged)
			return "purged"
		}
		r.progress.RestoredBytes = restored
		for _, e := range entries[:n] {
			switch {
			case e.skip != restoreKeep:
				r.progress.Skipped[e.skip]++
			case e.memOnly:
				r.memory++
				r.progress.RestoredEntries++
			default:
				r.disk++
				r.progress.RestoredEntries++
			}
			r.progress.ProcessedEntries++
			r.progress.ProcessedBytes += sizes[r.cursor]
			r.cursor++
		}
		clear(backing) // the batch's values belong to the memory tier now
		r.publish()
		ahead.advance(r.cursor, r.progress.ProcessedBytes)
	}
	return RestoreDone
}
