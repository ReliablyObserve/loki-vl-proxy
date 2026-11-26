package cache

import "time"

// Batched restore of entries into the memory tier. A source (the restart
// hand-over on disk) reads and decodes a bounded batch off the request path,
// then admits it under one short hold of the memory tier's lock. The admission
// and the progress record do not depend on where the entries came from, so
// another source (a peer) can use both.

// RestoreSource names where restored entries come from. It is a metric label:
// only these values exist.
type RestoreSource string

// RestoreSourceDisk is the restart hand-over in the disk tier.
const RestoreSourceDisk RestoreSource = "disk"

const (
	// A batch holds at most this many keys and about this many bytes of
	// decoded values (a single larger entry is a batch of its own), so one
	// read transaction, one lock hold and the memory in flight stay bounded.
	restoreBatchKeys  = 256
	restoreBatchBytes = 4 << 20
	// restoreMaxWorkers caps the goroutines that read and decode one batch
	// (also at most GOMAXPROCS).
	restoreMaxWorkers = 4
)

// RestoreSkip is why a planned entry was not restored.
type RestoreSkip uint8

const (
	restoreKeep RestoreSkip = iota
	// RestoreSkipExpired is an entry past its expiry.
	RestoreSkipExpired
	// RestoreSkipSuperseded is an entry for which a fresher one is already
	// there (in memory, or queued or rewritten on disk).
	RestoreSkipSuperseded
	// RestoreSkipTooLarge is an entry above one memory entry's share
	// (a tenth of the memory tier).
	RestoreSkipTooLarge
	// RestoreSkipOverBudget is an entry past the warm-up's byte budget.
	RestoreSkipOverBudget
	// RestoreSkipMemoryFull is an entry the memory tier has no free entry
	// slot or free bytes for.
	RestoreSkipMemoryFull
	// RestoreSkipOtherScope is a memory-only answer of another build or
	// configuration.
	RestoreSkipOtherScope
	// RestoreSkipUnreadable is a record that is missing, corrupt or cannot be
	// read.
	RestoreSkipUnreadable
	// RestoreSkipDeadline, RestoreSkipPurged and RestoreSkipClock count what a
	// stopped warm-up left behind.
	RestoreSkipDeadline
	RestoreSkipPurged
	RestoreSkipClock
	restoreSkipCount
)

var restoreSkipNames = [restoreSkipCount]string{
	"", "expired", "superseded", "too_large", "over_budget", "memory_full",
	"other_scope", "unreadable", "deadline", "purged", "clock_behind_flush",
}

func (s RestoreSkip) String() string {
	if s < restoreSkipCount {
		return restoreSkipNames[s]
	}
	return "unknown"
}

// RestoreSkipReasons lists every reason, for exporters.
func RestoreSkipReasons() []RestoreSkip {
	out := make([]RestoreSkip, 0, restoreSkipCount-1)
	for s := restoreKeep + 1; s < restoreSkipCount; s++ {
		out = append(out, s)
	}
	return out
}

// Restore outcomes (RestoreProgress.Outcome).
const (
	RestoreRunning = "running"
	RestoreDone    = "done"
	RestoreNone    = "none"
)

// RestoreOutcomes lists every outcome, for exporters.
func RestoreOutcomes() []string {
	return []string{RestoreRunning, RestoreDone, "deadline", "purged", "clock_behind_flush", RestoreNone}
}

// RestoreProgress is a restore's plan and how far it is, updated after every
// batch.
type RestoreProgress struct {
	Source RestoreSource
	// Outcome is empty before a restore, "running" during it, then why it
	// ended.
	Outcome string
	// PlannedEntries and PlannedBytes are what the source holds to restore;
	// the bytes are as stored (on disk, encoded), known before any value is
	// read. BudgetBytes caps the decoded bytes restored.
	PlannedEntries int64
	PlannedBytes   int64
	BudgetBytes    int64
	// ProcessedEntries and ProcessedBytes (as stored) are what was restored or
	// skipped so far.
	ProcessedEntries int64
	ProcessedBytes   int64
	// RestoredEntries and RestoredBytes (decoded, in memory) are what the
	// memory tier took.
	RestoredEntries int64
	RestoredBytes   int64
	Skipped         [restoreSkipCount]int64
	Started         time.Time
	// Timeout is the time the restore was given (0: none).
	Timeout time.Duration
	// Elapsed runs while the restore does, then stays.
	Elapsed time.Duration
}

// SkippedEntries is the number of planned entries not restored so far.
func (p RestoreProgress) SkippedEntries() int64 {
	var n int64
	for _, v := range p.Skipped {
		n += v
	}
	return n
}

// Fraction is how much of the restore is behind, 0 to 1: the share of the
// planned stored bytes processed, or of the byte budget filled, whichever is
// further (a restore ends at either).
func (p RestoreProgress) Fraction() float64 {
	if p.Outcome != RestoreRunning {
		if p.Outcome == "" {
			return 0
		}
		return 1
	}
	var f float64
	if p.PlannedBytes > 0 {
		f = float64(p.ProcessedBytes) / float64(p.PlannedBytes)
	}
	if p.BudgetBytes > 0 {
		f = max(f, float64(p.RestoredBytes)/float64(p.BudgetBytes))
	}
	return min(f, 1)
}

// BytesPerSecond is the decoded bytes restored per second so far.
func (p RestoreProgress) BytesPerSecond() float64 {
	if p.Elapsed <= 0 {
		return 0
	}
	return float64(p.RestoredBytes) / p.Elapsed.Seconds()
}

// Remaining estimates the time left from the rate so far; false while there is
// nothing to estimate from.
func (p RestoreProgress) Remaining() (time.Duration, bool) {
	if p.Outcome != RestoreRunning {
		return 0, p.Outcome != ""
	}
	f := p.Fraction()
	if f <= 0 || p.Elapsed <= 0 {
		return 0, false
	}
	return time.Duration(float64(p.Elapsed) * (1 - f) / f), true
}

// restoreEntry is one decoded value to put into the memory tier.
type restoreEntry struct {
	key     string
	value   []byte
	expiry  int64
	memOnly bool
	hits    uint32
	// skip, set by the source or the admission, keeps the entry out.
	skip RestoreSkip
}

// admitRestoreBatch stores one batch, in order, at the back of the LRU under
// one hold of the memory lock: only into free room and within budget (bytes
// already restored are in restored), never over an entry the memory tier holds,
// and nothing when a purge started since generation was read (it reports that
// as false). fence, when set, runs under the lock first and marks the entries
// its source has since superseded.
func (c *Cache) admitRestoreBatch(entries []*restoreEntry, generation uint64, restored *int64, budget int64, fence func([]*restoreEntry)) bool {
	now := clockNow()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.diskPurges != 0 || c.diskGeneration != generation {
		return false
	}
	if fence != nil {
		fence(entries)
	}
	countHits := c.hitsOn.Load()
	for _, e := range entries {
		if e.skip != restoreKeep {
			continue
		}
		remaining := time.Unix(0, e.expiry).Sub(now)
		size := entrySize(e.key, e.value)
		switch _, held := c.entries[e.key]; {
		case held:
			e.skip = RestoreSkipSuperseded
		case remaining <= 0:
			e.skip = RestoreSkipExpired
		case size > c.maxBytes/10:
			e.skip = RestoreSkipTooLarge
		case *restored+int64(size) > budget:
			e.skip = RestoreSkipOverBudget
		case len(c.entries) >= c.maxEntries || c.curBytes+size > c.maxBytes:
			e.skip = RestoreSkipMemoryFull
		default:
			c.entries[e.key] = entry{value: e.value, expiresAt: now.Add(remaining), sizeBytes: size, memOnly: e.memOnly, restorable: e.memOnly, sourceExpiresAt: e.expiry}
			c.curBytes += size
			stats := &c.layerL1[c.layer(e.key)]
			stats.entries.Add(1)
			stats.bytes.Add(int64(size))
			stats.admissions.Add(1)
			stats.writeBytes.Add(int64(len(e.value)))
			c.lruIndex[e.key] = c.lruList.PushBack(&lruEntry{key: e.key})
			if e.hits > 0 && countHits {
				c.hitsMu.Lock()
				c.hits[e.key] = e.hits
				c.hitsMu.Unlock()
			}
			*restored += int64(size)
		}
	}
	return true
}

// restoreRoom is the free entry slots and bytes of the memory tier now.
func (c *Cache) restoreRoom() (entries int, bytes int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return max(c.maxEntries-len(c.entries), 0), int64(max(c.maxBytes-c.curBytes, 0))
}

// holdsKeys marks the entries whose key the memory tier already holds, so
// they are not read.
func (c *Cache) holdsKeys(entries []*restoreEntry) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, e := range entries {
		if _, ok := c.entries[e.key]; ok && e.skip == restoreKeep {
			e.skip = RestoreSkipSuperseded
		}
	}
}
