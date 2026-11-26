package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Peer bulk restore: a replica that starts with nothing to warm from (no
// restart hand-over and an empty or no disk tier, for example a new volume)
// pulls the entries it owns or replicates from its peers before it reports
// ready. It is a separate, streamed and checksummed transfer
// (peer_bulk_wire.go), not the peer reads of single keys nor the hot-key
// read-ahead:
//
//  1. probe every peer (POST /_cache/bulk, probe): a peer without the route
//     (an older build), with serving off, itself restoring, or unreachable
//     after a few quick tries is left out at once;
//  2. ask the peers that answered, at most Concurrency at a time, each for
//     the keys it is the designated source of on this replica's ring (the
//     first of a key's other replicas in ring order among them), so each key
//     crosses the network once; a peer that is busy (429) is asked again
//     after its Retry-After until the deadline;
//  3. admit what arrives in batches through admitRestoreBatch, as the disk
//     warm-up does (budget, never over a live entry, behind live entries, a
//     purge since the start stops it), and queue the admitted values that are
//     not memory-only to the local disk, coldest-ranked last.
//
// Memory-only answers arrive only from a peer of the same build and
// configuration (the scope) and stay memory-only. The deadline is the
// warm-up's: /ready never waits past it.

// RestoreSourcePeer is the peer bulk restore.
const RestoreSourcePeer RestoreSource = "peer"

const (
	// DefaultPeerRestoreConcurrency is the peers a restore streams from at
	// once.
	DefaultPeerRestoreConcurrency = 4
	peerRestoreProbeConcurrency   = 8
	peerRestoreProbeAttempts      = 3
	peerRestoreProbeBackoff       = 300 * time.Millisecond
	peerRestoreMaxRetryAfter      = 5 * time.Second
	// peerRestoreBatchWait is the longest a received entry waits for its
	// batch to fill before the batch is admitted anyway.
	peerRestoreBatchWait = 100 * time.Millisecond
)

// Per-peer outcomes of a restore.
const (
	PeerRestoreDone         = "done"
	PeerRestoreBudget       = "budget"
	PeerRestorePurged       = "purged"
	PeerRestoreStopped      = "stopped"
	PeerRestoreTruncated    = "truncated"
	PeerRestoreCorrupt      = "corrupt"
	PeerRestoreDeadline     = "deadline"
	PeerRestoreBusy         = "busy"
	PeerRestoreRestoring    = "restoring"
	PeerRestoreDisabled     = "disabled"
	PeerRestoreUnsupported  = "unsupported"
	PeerRestoreUnreachable  = "unreachable"
	PeerRestoreUnauthorized = "unauthorized"
	PeerRestoreRefused      = "refused"
)

// PeerRestoreOptions bounds a peer bulk restore.
type PeerRestoreOptions struct {
	// Scope is this process's build and configuration scope (memory-only
	// answers arrive only from a peer with the same one; "" takes none).
	Scope string
	// MaxBytes is the most decoded bytes restored into memory (also at most
	// half of the memory tier).
	MaxBytes int64
	// Concurrency is the peers streamed from at once (0: the default).
	Concurrency int
}

// PeerRestorePeer is what one peer gave.
type PeerRestorePeer struct {
	Addr     string
	Outcome  string
	Planned  int64
	Received int64
	Bytes    int64
	Attempts int
}

// PeerRestoreReport is what a peer bulk restore did.
type PeerRestoreReport struct {
	Outcome string
	Peers   []PeerRestorePeer
	// DiskWrites are restored values queued to the local disk; DiskSkipped
	// the ones its write queue had no room for after one flush (the next
	// graceful shutdown's hand-over writes those).
	DiskWrites, DiskSkipped int64
	Progress                RestoreProgress
}

// PeerRestoreWanted reports whether a start-up has nothing local to warm from
// and peers to ask: no restart hand-over, a disk tier that held no record at
// start (or none), and at least one peer besides this replica.
func (c *Cache) PeerRestoreWanted() bool {
	if c == nil || c.disabled || c.l3 == nil || c.l3.selfAddr == "" {
		return false
	}
	if c.l2 != nil && (c.l2.rebuildEntries > 0 || c.HasHandover()) {
		return false
	}
	for _, p := range c.l3.Peers() {
		if p != c.l3.selfAddr {
			return true
		}
	}
	return false
}

// PeerRestoreRunning reports whether this replica restores from peers now; it
// then serves no bulk stream itself.
func (c *Cache) PeerRestoreRunning() bool {
	return c != nil && c.handoverStats.peerRestoring.Load()
}

// peerRestore is one restore's shared state.
type peerRestore struct {
	c          *Cache
	pc         *PeerCache
	ctx        context.Context
	cancel     context.CancelFunc
	generation uint64
	budget     int64
	maxEntry   int64
	scope      string
	request    bulkRequest

	mu        sync.Mutex
	progress  RestoreProgress
	purged    bool
	budgetHit bool
	// access is the next disk recency handed to a restored value, counting
	// down so the hottest restored values end newest (nextAccess).
	access      atomic.Int64
	accessFloor int64
	diskWrites  atomic.Int64
	diskSkipped atomic.Int64
}

// RestoreFromPeers runs a peer bulk restore (see the comment at the top of
// this file) and returns once every stream ended or ctx did; all its
// goroutines have returned when it does.
func (c *Cache) RestoreFromPeers(ctx context.Context, opts PeerRestoreOptions) PeerRestoreReport {
	var report PeerRestoreReport
	if c == nil || c.disabled || c.l3 == nil {
		report.Outcome = RestoreNone
		return report
	}
	pc := c.l3
	c.handoverStats.peerRestoring.Store(true)
	defer c.handoverStats.peerRestoring.Store(false)
	r := &peerRestore{c: c, pc: pc, maxEntry: int64(c.maxBytes / 10), scope: bulkScopeHash(opts.Scope)}
	r.ctx, r.cancel = context.WithCancel(ctx)
	defer r.cancel()
	r.progress = RestoreProgress{Source: RestoreSourcePeer, Outcome: RestoreRunning, Started: clockNow()}
	if deadline, ok := ctx.Deadline(); ok {
		r.progress.Timeout = max(time.Until(deadline), 0)
	}
	r.budget = max(min(opts.MaxBytes, int64(c.maxBytes/handoverWarmShareDivisor)), 0)
	r.progress.BudgetBytes = r.budget
	r.publish()
	generation, idle := c.diskPromotionGeneration()
	r.generation = generation
	peers := make([]string, 0)
	for _, p := range pc.Peers() {
		if p != pc.selfAddr {
			peers = append(peers, p)
		}
	}
	switch {
	case !idle:
		return r.finish("purged", nil)
	case pc.selfAddr == "" || len(peers) == 0 || r.budget <= 0:
		return r.finish(RestoreNone, nil)
	}
	ring := append(append(make([]string, 0, len(peers)+1), peers...), pc.selfAddr)
	sort.Strings(ring)
	r.request = bulkRequest{Version: 1, For: pc.selfAddr, Peers: ring, Replication: pc.replication, Scope: r.scope}
	if c.l2 != nil {
		base := c.l2.reserveAccess(uint64(c.l2.maxEntries) + 1)
		r.access.Store(int64(base))
		r.accessFloor = int64(base) - int64(c.l2.maxEntries)
	}

	results := r.probe(peers)
	var sources []string
	for _, res := range results {
		if res.Outcome == "" {
			sources = append(sources, res.Addr)
		}
	}
	if len(sources) == 0 {
		return r.finish(RestoreNone, results)
	}
	r.request.Sources = sources
	r.pull(results, max(opts.Concurrency, 0))
	outcome := RestoreDone
	switch {
	case r.purged:
		outcome = "purged"
	case r.budgetHit:
	case ctx.Err() != nil:
		outcome = "deadline"
	case r.progress.PlannedEntries == 0:
		outcome = RestoreNone
	}
	return r.finish(outcome, results)
}

// publish copies the progress to the metrics; never called with r.mu held.
func (r *peerRestore) publish() {
	r.mu.Lock()
	progress := r.progress
	r.mu.Unlock()
	s := &r.c.handoverStats
	s.mu.Lock()
	s.peerProgress = progress
	s.mu.Unlock()
}

func (r *peerRestore) finish(outcome string, peers []PeerRestorePeer) PeerRestoreReport {
	r.mu.Lock()
	r.progress.Outcome = outcome
	r.progress.Elapsed = clockNow().Sub(r.progress.Started)
	progress := r.progress
	r.mu.Unlock()
	r.publish()
	return PeerRestoreReport{Outcome: outcome, Peers: peers, DiskWrites: r.diskWrites.Load(), DiskSkipped: r.diskSkipped.Load(), Progress: progress}
}

// probe asks every peer whether it serves a bulk stream now; a peer it leaves
// out gets its outcome, the others an empty one.
func (r *peerRestore) probe(peers []string) []PeerRestorePeer {
	results := make([]PeerRestorePeer, len(peers))
	sem := make(chan struct{}, peerRestoreProbeConcurrency)
	var wg sync.WaitGroup
	for i, addr := range peers {
		results[i].Addr = addr
		wg.Add(1)
		sem <- struct{}{}
		go func(res *PeerRestorePeer) {
			defer wg.Done()
			defer func() { <-sem }()
			req := r.request
			req.Probe = true
			for attempt := 1; ; attempt++ {
				res.Attempts = attempt
				outcome, _ := r.call(res.Addr, req, nil)
				if outcome != PeerRestoreUnreachable || attempt == peerRestoreProbeAttempts || !sleepCtx(r.ctx, peerRestoreProbeBackoff*time.Duration(attempt)) {
					if outcome == PeerRestoreDone {
						outcome = ""
					}
					res.Outcome = outcome
					return
				}
			}
		}(&results[i])
	}
	wg.Wait()
	if r.ctx.Err() != nil {
		for i := range results {
			if results[i].Outcome == "" {
				results[i].Outcome = PeerRestoreDeadline
			}
		}
	}
	return results
}

// pull streams from the probed sources, at most concurrency at a time,
// asking a busy source again after its Retry-After until the deadline.
func (r *peerRestore) pull(results []PeerRestorePeer, concurrency int) {
	if concurrency <= 0 {
		concurrency = DefaultPeerRestoreConcurrency
	}
	type task struct {
		res       *PeerRestorePeer
		notBefore time.Time
	}
	var mu sync.Mutex
	var queue []task
	for i := range results {
		if results[i].Outcome == "" {
			results[i].Attempts = 0
			queue = append(queue, task{res: &results[i]})
		}
	}
	pop := func() (task, bool) {
		mu.Lock()
		defer mu.Unlock()
		if len(queue) == 0 {
			return task{}, false
		}
		best := 0
		for i := range queue {
			if queue[i].notBefore.Before(queue[best].notBefore) {
				best = i
			}
		}
		t := queue[best]
		queue = append(queue[:best], queue[best+1:]...)
		return t, true
	}
	workers := min(concurrency, len(queue))
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				t, ok := pop()
				if !ok {
					return
				}
				if !sleepCtx(r.ctx, time.Until(t.notBefore)) {
					t.res.Outcome = r.stoppedOutcome()
					continue
				}
				t.res.Attempts++
				outcome, retry := r.call(t.res.Addr, r.streamRequest(), t.res)
				if outcome == PeerRestoreBusy && r.ctx.Err() == nil {
					jitter := time.Duration(rand.Int63n(int64(retry)/2 + 1))
					mu.Lock()
					queue = append(queue, task{res: t.res, notBefore: time.Now().Add(retry + jitter)})
					mu.Unlock()
					continue
				}
				if outcome == PeerRestoreBusy {
					outcome = r.stoppedOutcome()
				}
				t.res.Outcome = outcome
			}
		}()
	}
	wg.Wait()
}

// stoppedOutcome is a peer's outcome when the restore itself stopped.
func (r *peerRestore) stoppedOutcome() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.purged:
		return PeerRestorePurged
	case r.budgetHit:
		return PeerRestoreBudget
	default:
		return PeerRestoreDeadline
	}
}

func (r *peerRestore) streamRequest() bulkRequest {
	req := r.request
	r.mu.Lock()
	req.MaxBytes = max(r.budget-r.progress.RestoredBytes, 1)
	r.mu.Unlock()
	return req
}

// call sends one bulk request and, for a stream, restores it into res. It
// returns the outcome and, for a busy peer, when to ask again.
func (r *peerRestore) call(addr string, req bulkRequest, res *PeerRestorePeer) (string, time.Duration) {
	resp, err := r.pc.bulkPost(r.ctx, addr, req)
	if err != nil {
		if r.ctx.Err() != nil {
			return r.stoppedOutcome(), 0
		}
		return PeerRestoreUnreachable, 0
	}
	defer func() {
		_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
		_ = resp.Body.Close()
	}()
	if resp.Header.Get(peerBulkVersionHeader) != peerBulkVersion {
		return PeerRestoreUnsupported, 0
	}
	switch resp.StatusCode {
	case http.StatusNoContent:
		if req.Probe {
			return PeerRestoreDone, 0
		}
		return PeerRestoreRefused, 0
	case http.StatusOK:
		if req.Probe || resp.Header.Get("Content-Type") != peerBulkContentType || res == nil {
			return PeerRestoreRefused, 0
		}
		return r.receive(resp.Body, res), 0
	case http.StatusTooManyRequests:
		retry := time.Second
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
			retry = time.Duration(s) * time.Second
		}
		return PeerRestoreBusy, min(retry, peerRestoreMaxRetryAfter)
	case http.StatusServiceUnavailable:
		switch resp.Header.Get(peerBulkStateHeader) {
		case "restoring":
			return PeerRestoreRestoring, 0
		case "disabled":
			return PeerRestoreDisabled, 0
		}
		return PeerRestoreRefused, 0
	case http.StatusUnauthorized, http.StatusForbidden:
		return PeerRestoreUnauthorized, 0
	}
	return PeerRestoreRefused, 0
}

// receive reads one stream into the memory tier and returns its outcome.
func (r *peerRestore) receive(body io.Reader, res *PeerRestorePeer) string {
	br := newBulkReader(body)
	if err := br.readMagic(); err != nil {
		return r.streamError(err)
	}
	var plannedBytes, receivedBytes int64
	planSeen := false
	batch := make([]*restoreEntry, 0, restoreBatchKeys)
	backing := make([]restoreEntry, restoreBatchKeys)
	sizes := make([]int64, 0, restoreBatchKeys)
	var batchBytes int64
	var batchStarted time.Time
	flush := func() bool {
		if len(batch) == 0 {
			return r.ctx.Err() == nil
		}
		ok := r.admit(batch, sizes)
		batch, sizes, batchBytes = batch[:0], sizes[:0], 0
		clear(backing)
		return ok
	}
	// skip counts an entry not admitted before it reached a batch.
	skip := func(reason RestoreSkip, size int64) {
		r.mu.Lock()
		r.progress.Skipped[reason]++
		r.progress.ProcessedEntries++
		r.progress.ProcessedBytes += size
		r.mu.Unlock()
	}
	// end settles what the peer planned and did not send.
	end := func(outcome string, serverSkipped int64) string {
		flush()
		left := max(res.Planned-res.Received, 0)
		fromServer := min(serverSkipped, left)
		r.mu.Lock()
		r.progress.Skipped[RestoreSkipExpired] += fromServer
		reason := RestoreSkipUnreadable
		switch outcome {
		case PeerRestoreDone:
		case PeerRestoreBudget:
			reason = RestoreSkipOverBudget
		case PeerRestorePurged:
			reason = RestoreSkipPurged
		case PeerRestoreDeadline:
			reason = RestoreSkipDeadline
		}
		r.progress.Skipped[reason] += left - fromServer
		r.progress.ProcessedEntries += left
		r.progress.ProcessedBytes += max(plannedBytes-receivedBytes, 0)
		r.mu.Unlock()
		r.publish()
		return outcome
	}
	for {
		f, err := br.next()
		if err != nil {
			if !planSeen {
				return r.streamError(err)
			}
			return end(r.streamError(err), 0)
		}
		switch f.typ {
		case bulkFramePlan:
			if planSeen {
				return end(PeerRestoreCorrupt, 0)
			}
			planSeen = true
			res.Planned, plannedBytes = int64(min(f.a, 1<<40)), int64(min(f.b, 1<<50))
			r.mu.Lock()
			r.progress.PlannedEntries += res.Planned
			r.progress.PlannedBytes += plannedBytes
			r.mu.Unlock()
			r.publish()
			continue
		case bulkFrameEnd:
			outcome := peerEndOutcome(f.flags)
			if r.budgetFilled() {
				outcome = PeerRestoreBudget
			}
			return end(outcome, int64(min(f.c, 1<<40)))
		}
		if !planSeen {
			return PeerRestoreCorrupt
		}
		size := int64(f.a + f.b)
		if int64(f.b) > r.maxEntry+diskRecordHeaderSize {
			if err := br.skipPayload(f); err != nil {
				return end(r.streamError(err), 0)
			}
			res.Received, res.Bytes, receivedBytes = res.Received+1, res.Bytes+size, receivedBytes+size
			skip(RestoreSkipTooLarge, size)
			continue
		}
		key, record, err := br.payload(f)
		if err != nil && !errors.Is(err, errBulkRecordCorrupt) {
			return end(r.streamError(err), 0)
		}
		res.Received, res.Bytes, receivedBytes = res.Received+1, res.Bytes+size, receivedBytes+size
		if err != nil {
			skip(RestoreSkipUnreadable, size)
			continue
		}
		e := &backing[len(batch)]
		if *e = r.decodeRecord(f, key, record); e.skip != restoreKeep {
			skip(e.skip, size)
			*e = restoreEntry{}
			continue
		}
		if len(batch) == 0 {
			batchStarted = time.Now()
		}
		batch = append(batch, e)
		sizes = append(sizes, size)
		batchBytes += int64(entrySize(key, e.value))
		// A slow stream still lands what it sent within peerRestoreBatchWait.
		if len(batch) >= restoreBatchKeys || batchBytes >= restoreBatchBytes || time.Since(batchStarted) >= peerRestoreBatchWait {
			if !flush() {
				return end(r.stoppedOutcome(), 0)
			}
		}
	}
}

const math64MaxMillis = int64(^uint64(0)>>1) / int64(time.Millisecond)

// decodeRecord checks and decodes one record frame's payload; a skip reason
// keeps it out.
func (r *peerRestore) decodeRecord(f bulkFrame, key string, record []byte) restoreEntry {
	memOnly := f.flags&bulkFlagMemoryOnly != 0
	ttl := time.Duration(min(f.c, uint64(math64MaxMillis))) * time.Millisecond
	switch {
	case memOnly && r.scope == "":
		return restoreEntry{skip: RestoreSkipOtherScope}
	case ttl < MinUsableTTL:
		return restoreEntry{skip: RestoreSkipExpired}
	}
	if _, decodedSize, _, err := diskRecordHeader(record, diskRecordMaxDecoded); err == nil && int64(decodedSize+len(key)) > r.maxEntry {
		return restoreEntry{skip: RestoreSkipTooLarge}
	}
	decoded, err := decodeNativeDiskRecordLimit(record, int(r.maxEntry))
	if err != nil {
		return restoreEntry{skip: RestoreSkipUnreadable}
	}
	return restoreEntry{key: key, value: decoded.Value, expiry: clockNow().Add(ttl).UnixNano(), memOnly: memOnly}
}

// peerEndOutcome is a peer's outcome for its end frame.
func peerEndOutcome(flags byte) string {
	switch flags {
	case bulkEndBudget:
		return PeerRestoreBudget
	case bulkEndPurged:
		return PeerRestorePurged
	case bulkEndStopped:
		return PeerRestoreStopped
	}
	return PeerRestoreDone
}

func (r *peerRestore) budgetFilled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.budgetHit
}

// streamError names how a stream that did not end with an end frame stopped.
func (r *peerRestore) streamError(err error) string {
	switch {
	case r.ctx.Err() != nil:
		return r.stoppedOutcome()
	case errors.Is(err, errBulkHeader), errors.Is(err, errBulkMagic):
		return PeerRestoreCorrupt
	default:
		return PeerRestoreTruncated
	}
}

// admit stores one batch and queues its values to the local disk; false when
// the restore must stop (a purge, the budget, the deadline).
func (r *peerRestore) admit(batch []*restoreEntry, sizes []int64) bool {
	c := r.c
	r.mu.Lock()
	if r.purged || r.budgetHit || r.ctx.Err() != nil {
		for i := range batch {
			r.progress.Skipped[r.stopReasonLocked()]++
			r.progress.ProcessedEntries++
			r.progress.ProcessedBytes += sizes[i]
		}
		r.mu.Unlock()
		return false
	}
	restored := r.progress.RestoredBytes
	if !c.admitRestoreBatch(batch, r.generation, &restored, r.budget, nil) {
		r.purged = true
		for i := range batch {
			r.progress.Skipped[RestoreSkipPurged]++
			r.progress.ProcessedEntries++
			r.progress.ProcessedBytes += sizes[i]
		}
		r.mu.Unlock()
		r.publish()
		r.cancel()
		return false
	}
	r.progress.RestoredBytes = restored
	full := restored >= r.budget
	for i, e := range batch {
		// Entries arrive hottest first: once one is over the budget, the
		// rest of the restore is not worth its transfer.
		full = full || e.skip == RestoreSkipOverBudget
		if e.skip != restoreKeep {
			r.progress.Skipped[e.skip]++
		} else {
			r.progress.RestoredEntries++
		}
		r.progress.ProcessedEntries++
		r.progress.ProcessedBytes += sizes[i]
	}
	if full {
		r.budgetHit = true
	}
	r.progress.Elapsed = clockNow().Sub(r.progress.Started)
	r.mu.Unlock()
	r.publish()
	r.persist(batch)
	if full {
		r.cancel()
		return false
	}
	return true
}

func (r *peerRestore) stopReasonLocked() RestoreSkip {
	switch {
	case r.purged:
		return RestoreSkipPurged
	case r.budgetHit:
		return RestoreSkipOverBudget
	default:
		return RestoreSkipDeadline
	}
}

// persist queues the batch's admitted values that are not memory-only to the
// local disk, as a replica keeps a pushed copy: only while the memory tier
// still holds the restored value (a live write since then is fresher), the
// disk holds nothing for the key, and no purge started since the restore did
// (all checked under the memory lock, which a purge takes first). A full
// write queue is committed once and the rest tried again.
func (r *peerRestore) persist(batch []*restoreEntry) {
	c := r.c
	dc := c.l2
	if dc == nil {
		return
	}
	pending := batch
	for round := 0; round < 2 && len(pending) > 0; round++ {
		var deferred []*restoreEntry
		now := clockNow()
		c.mu.Lock()
		if c.diskPurges != 0 || c.diskGeneration != r.generation {
			c.mu.Unlock()
			return
		}
		for _, e := range pending {
			if e.skip != restoreKeep || e.memOnly {
				continue
			}
			cur, ok := c.entries[e.key]
			if !ok || len(cur.value) != len(e.value) || (len(e.value) > 0 && &cur.value[0] != &e.value[0]) || dc.holds(e.key) {
				continue
			}
			remaining := time.Unix(0, e.expiry).Sub(now)
			if remaining <= 0 {
				continue
			}
			if !dc.queueRoom(e.key, e.value) {
				deferred = append(deferred, e)
				continue
			}
			dc.set(e.key, e.value, remaining, r.nextAccess(), 0)
			r.diskWrites.Add(1)
		}
		c.mu.Unlock()
		if len(deferred) > 0 && round == 0 {
			dc.Flush()
		} else {
			r.diskSkipped.Add(int64(len(deferred)))
		}
		pending = deferred
	}
}

// nextAccess hands out disk recencies downwards from the block reserved at
// the start, so a value restored earlier (hotter) ends newer than a later one
// and every live write after the start newer than all of them.
func (r *peerRestore) nextAccess() uint64 {
	v := r.access.Add(-1)
	if v <= r.accessFloor || v <= 0 {
		return max(uint64(r.accessFloor), 1)
	}
	return uint64(v)
}

// reserveAccess advances the disk recency counter by n and returns its new
// value: recencies below it are free for a restore to hand out.
func (dc *DiskCache) reserveAccess(n uint64) uint64 {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	dc.sequence += n
	return dc.sequence
}

// bulkHTTP is the restore's own client: no overall timeout (a stream runs
// until the restore's deadline), the response headers within the peer
// timeout, no transparent decompression, and never the breakers or the
// connection pool of the peer reads.
func (pc *PeerCache) bulkHTTP() *http.Client {
	pc.bulkClientOnce.Do(func() {
		dialer := &net.Dialer{Timeout: max(pc.client.Timeout, time.Second), KeepAlive: 30 * time.Second}
		pc.bulkClient.Store(&http.Client{Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			DisableCompression:    true,
			ResponseHeaderTimeout: max(2*pc.client.Timeout, 5*time.Second),
			MaxConnsPerHost:       2,
			MaxIdleConnsPerHost:   1,
			IdleConnTimeout:       30 * time.Second,
		}})
	})
	return pc.bulkClient.Load()
}

func (pc *PeerCache) bulkPost(ctx context.Context, addr string, req bulkRequest) (*http.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	endpoint := "http://" + addr + peerBulkPath
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept-Encoding", "identity")
	hreq.Header.Set(peerBulkVersionHeader, peerBulkVersion)
	if pc.authToken != "" {
		hreq.Header.Set("X-Peer-Token", pc.authToken)
	}
	return pc.bulkHTTP().Do(hreq)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
