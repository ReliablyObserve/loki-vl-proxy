package cache

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Serving side of the peer bulk restore (POST /_cache/bulk): a replica that
// starts with an empty disk (restore_peer.go) asks every peer for the entries
// it owns or replicates, and each peer streams the ones it is the designated
// source of, hottest first, in the wire format of peer_bulk_wire.go.
//
// What a serving peer spends is bounded whatever the number of replicas that
// start at once:
//   - at most BulkServeMaxStreams streams at a time; a request beyond them is
//     answered 429 at once (Retry-After: 1) and the requester tries later;
//   - every stream together sends at most BulkServeMaxBytesPerSecond;
//   - one batch (at most restoreBatchKeys entries or about restoreBatchBytes)
//     is in memory per stream; disk records are copied out of their read
//     transaction and sent as stored, memory values are encoded once;
//   - nothing it serves counts as a lookup, moves its LRU or disk recency, or
//     touches its breakers;
//   - a replica that is itself restoring from peers answers 503 (restoring),
//     so replicas that all start together never wait on each other;
//   - its streams end when its HTTP server starts shutting down (StopBulk).

const (
	// DefaultPeerBulkServeMaxStreams is the bulk streams one replica serves at
	// once.
	DefaultPeerBulkServeMaxStreams = 2
	// DefaultPeerBulkServeMaxBytesPerSecond is the bytes per second all bulk
	// streams of one replica send together.
	DefaultPeerBulkServeMaxBytesPerSecond int64 = 64 << 20

	peerBulkMaxRequestBytes = 256 << 10
	peerBulkMaxPeers        = 512
	peerBulkMaxAddrBytes    = 256
	// peerBulkWriteTimeout bounds one batch's write to the requester; the
	// server's -http-write-timeout would otherwise end a long stream.
	peerBulkWriteTimeout = 30 * time.Second
	peerBulkWriteBuffer  = 64 << 10
	// bulkPlanChunk bounds the entries a plan walks per hold of a cache lock.
	bulkPlanChunk = 4096
)

// bulkRequest is the requester's view of the ring and what it wants.
type bulkRequest struct {
	Version int  `json:"version"`
	Probe   bool `json:"probe,omitempty"`
	// For is the requester's ring address; Peers its peer list (itself
	// included) and Replication its replication factor: the server decides
	// on that ring which keys For owns or replicates.
	For         string   `json:"for"`
	Peers       []string `json:"peers"`
	Replication int      `json:"replication"`
	// Sources are the peers the requester pulls from: a key is sent by the
	// first of its other replicas in ring order that is a source, so each key
	// crosses the network once. Empty: every peer but For.
	Sources []string `json:"sources,omitempty"`
	// Scope is a SHA-256 of the requester's build and configuration scope;
	// memory-only answers are sent only when it equals the server's.
	Scope string `json:"scope,omitempty"`
	// MaxBytes stops the stream after about this many record bytes (0: none).
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// DeadlineMs is the requester's time left: the stream never runs past it.
	DeadlineMs int64 `json:"deadline_ms,omitempty"`
}

type bulkRing struct {
	ring    *hashRing
	rf      int
	self    string
	target  string
	sources map[string]bool
}

func decodeBulkRequest(body io.Reader) (bulkRequest, error) {
	var req bulkRequest
	raw, err := readPeerBodyLimited(body, peerBulkMaxRequestBytes)
	if err != nil {
		return req, err
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, err
	}
	if req.Version != 1 {
		return req, fmt.Errorf("unsupported bulk version %d", req.Version)
	}
	if req.Replication < 1 || req.Replication > MaxReplicationFactor {
		return req, fmt.Errorf("replication factor %d out of range", req.Replication)
	}
	if len(req.Peers) == 0 || len(req.Peers) > peerBulkMaxPeers || len(req.Sources) > peerBulkMaxPeers {
		return req, fmt.Errorf("peer list size out of range")
	}
	for _, list := range [][]string{req.Peers, req.Sources, {req.For}} {
		for _, p := range list {
			if p == "" || len(p) > peerBulkMaxAddrBytes || !strings.Contains(p, ":") {
				return req, fmt.Errorf("invalid peer address")
			}
		}
	}
	if req.MaxBytes < 0 || req.DeadlineMs < 0 {
		return req, fmt.Errorf("negative max_bytes or deadline_ms")
	}
	return req, nil
}

// onRing reports whether this replica and the requester are both on the
// requester's ring.
func (req bulkRequest) onRing(self string) bool {
	var hasSelf, hasFor bool
	for _, p := range req.Peers {
		hasSelf, hasFor = hasSelf || p == self, hasFor || p == req.For
	}
	return hasSelf && hasFor && self != req.For
}

// ringFor builds the requester's ring as PeerCache.setPeers does (the same
// virtual nodes in the same order), sorting once.
func (req bulkRequest) ringFor(self string) *bulkRing {
	peers := filterValidPeers(req.Peers)
	ring := newHashRing(150)
	seen := make(map[string]bool, len(peers))
	for _, p := range peers {
		if seen[p] {
			continue
		}
		seen[p] = true
		for i := 0; i < ring.vnodes; i++ {
			h := hashKey(fmt.Sprintf("%s#%d", p, i))
			ring.ring = append(ring.ring, h)
			ring.nodeMap[h] = p
		}
	}
	sort.Slice(ring.ring, func(i, j int) bool { return ring.ring[i] < ring.ring[j] })
	br := &bulkRing{ring: ring, rf: req.Replication, self: self, target: req.For}
	if len(req.Sources) > 0 {
		br.sources = make(map[string]bool, len(req.Sources))
		for _, s := range req.Sources {
			br.sources[strings.TrimSpace(s)] = true
		}
	}
	return br
}

// designated reports whether this replica sends key: the requester is one of
// its replicas and this replica is the first other replica that is a source.
func (br *bulkRing) designated(key string) bool {
	target, first := false, ""
	br.ring.walk(key, br.rf, func(addr string) bool {
		switch {
		case addr == br.target:
			target = true
		case first == "" && (br.sources == nil || br.sources[addr]):
			first = addr
		}
		return true
	})
	return target && first == br.self
}

// byteRate is a token bucket shared by a replica's bulk streams.
type byteRate struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newByteRate(perSecond int64) *byteRate {
	if perSecond <= 0 {
		return nil
	}
	r := float64(perSecond)
	return &byteRate{rate: r, burst: r / 4, tokens: r / 4, last: time.Now()}
}

// wait takes n bytes, sleeping while the bucket is in debt.
func (l *byteRate) wait(ctx context.Context, n int) error {
	if l == nil {
		return ctx.Err()
	}
	l.mu.Lock()
	now := time.Now()
	l.tokens = min(l.burst, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	l.tokens -= float64(n)
	var delay time.Duration
	if l.tokens < 0 {
		delay = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	if delay <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// peerBulkServe is the serving side's state.
type peerBulkServe struct {
	slots    chan struct{} // nil: serving is off
	gov      *bulkGovernor
	stop     chan struct{}
	stopOnce sync.Once
	scope    atomic.Pointer[func() string]
	// Counters for the metrics.
	streams  [bulkEndCount]atomic.Int64
	rejected [bulkRejectCount]atomic.Int64
	records  atomic.Int64
	bytes    atomic.Int64
	active   atomic.Int64
}

const (
	bulkRejectBusy = iota
	bulkRejectRestoring
	bulkRejectDisabled
	bulkRejectBadRequest
	// bulkRejectThrottled is a request refused because the governor is
	// paused or under pressure.
	bulkRejectThrottled
	bulkRejectCount
)

var bulkRejectNames = [bulkRejectCount]string{"busy", "restoring", "disabled", "bad_request", "throttled"}

func newPeerBulkServe(maxStreams int, bytesPerSecond int64, maxPause time.Duration, signals *bulkSignals, log *slog.Logger) *peerBulkServe {
	s := &peerBulkServe{stop: make(chan struct{})}
	if maxStreams == 0 {
		maxStreams = DefaultPeerBulkServeMaxStreams
	}
	if maxStreams > 0 {
		s.slots = make(chan struct{}, maxStreams)
	}
	if bytesPerSecond == 0 {
		bytesPerSecond = DefaultPeerBulkServeMaxBytesPerSecond
	}
	s.gov = newBulkGovernor(max(bytesPerSecond, 0), maxPause, signals, log)
	return s
}

// SetBulkScope sets the build and configuration scope this replica compares
// a requester's with before it sends memory-only answers ("" sends none).
func (pc *PeerCache) SetBulkScope(scope func() string) {
	if pc == nil || pc.bulk == nil || scope == nil {
		return
	}
	hashed := sync.OnceValue(func() string { return bulkScopeHash(scope()) })
	pc.bulk.scope.Store(&hashed)
}

func (pc *PeerCache) bulkScope() string {
	if pc.bulk == nil {
		return ""
	}
	if fn := pc.bulk.scope.Load(); fn != nil {
		return (*fn)()
	}
	return ""
}

func bulkScopeHash(scope string) string {
	if scope == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(scope))
	return hex.EncodeToString(sum[:])
}

// StopBulk ends the bulk streams this replica serves and refuses new ones;
// call it when the HTTP server starts shutting down, so a stream does not
// hold the drain.
func (pc *PeerCache) StopBulk() {
	if pc == nil || pc.bulk == nil {
		return
	}
	pc.bulk.stopOnce.Do(func() { close(pc.bulk.stop) })
}

func (pc *PeerCache) bulkReject(w http.ResponseWriter, status int, reason int, msg string) {
	pc.bulk.rejected[reason].Add(1)
	w.Header().Set(peerBulkStateHeader, bulkRejectNames[reason])
	if reason == bulkRejectBusy && w.Header().Get("Retry-After") == "" {
		w.Header().Set("Retry-After", "1")
	}
	http.Error(w, msg, status)
}

// serveBulk handles POST /_cache/bulk.
func (pc *PeerCache) serveBulk(w http.ResponseWriter, r *http.Request, local *Cache) {
	// Every answer names the protocol version, so a requester tells this
	// replica from one that does not know the route.
	w.Header().Set(peerBulkVersionHeader, peerBulkVersion)
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if pc.bulk == nil {
		w.Header().Set(peerBulkStateHeader, bulkRejectNames[bulkRejectDisabled])
		http.Error(w, "bulk restore disabled", http.StatusServiceUnavailable)
		return
	}
	req, err := decodeBulkRequest(r.Body)
	if err != nil {
		pc.bulkReject(w, http.StatusBadRequest, bulkRejectBadRequest, "invalid bulk request")
		return
	}
	select {
	case <-pc.bulk.stop:
		pc.bulkReject(w, http.StatusServiceUnavailable, bulkRejectDisabled, "bulk restore stopped")
		return
	default:
	}
	if pc.bulk.slots == nil || local == nil || local.disabled {
		pc.bulkReject(w, http.StatusServiceUnavailable, bulkRejectDisabled, "bulk restore disabled")
		return
	}
	if local.PeerRestoreRunning() {
		pc.bulkReject(w, http.StatusServiceUnavailable, bulkRejectRestoring, "restoring from peers")
		return
	}
	if !req.onRing(pc.selfAddr) {
		pc.bulkReject(w, http.StatusConflict, bulkRejectBadRequest, "not on the requester's ring")
		return
	}
	if req.Probe {
		w.Header().Set(peerBulkStateHeader, "ready")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Lower priority than everything else: no new stream while the serving
	// side is paused or under pressure.
	if ok, retry := pc.bulk.gov.admit(); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(int(retry/time.Second)))
		pc.bulkReject(w, http.StatusTooManyRequests, bulkRejectThrottled, "bulk restore throttled")
		return
	}
	select {
	case pc.bulk.slots <- struct{}{}:
	default:
		pc.bulkReject(w, http.StatusTooManyRequests, bulkRejectBusy, "bulk restore streams busy")
		return
	}
	defer func() { <-pc.bulk.slots }()
	pc.bulk.active.Add(1)
	defer pc.bulk.active.Add(-1)
	pc.bulk.gov.begin()
	defer pc.bulk.gov.end()
	ctx, cancel := context.WithCancel(r.Context())
	if req.DeadlineMs > 0 {
		ctx, cancel = context.WithTimeout(r.Context(), time.Duration(req.DeadlineMs)*time.Millisecond)
	}
	defer cancel()
	go func() {
		select {
		case <-pc.bulk.stop:
			cancel()
		case <-pc.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	memOK := req.Scope != "" && req.Scope == pc.bulkScope()
	outcome := pc.streamBulk(ctx, w, local, req, req.ringFor(pc.selfAddr), memOK)
	pc.bulk.streams[outcome].Add(1)
}

// bulkItem is one planned key and the bytes planned for it.
type bulkItem struct {
	key  string
	size int64
}

// bulkPlan lists what a bulk stream may send, hottest first: the memory
// tier's entries in recency order, then the disk records it lacks in disk
// recency order. It holds each lock only to copy keys; keep (ownership, push
// filter) runs outside them.
func (c *Cache) bulkPlan(keep func(string) bool, memOK bool) ([]bulkItem, int64) {
	now := clockNow()
	var items []bulkItem
	// The memory walk, bulkPlanChunk entries per hold of the read lock: a
	// query's write waits at most one chunk. It goes on from the element it
	// stopped at while that is still in the list, and ends otherwise (the
	// disk walk below covers most of the rest).
	c.mu.RLock()
	items = make([]bulkItem, 0, len(c.entries))
	for elem := c.lruList.Front(); elem != nil; {
		for n := 0; elem != nil && n < bulkPlanChunk; elem, n = elem.Next(), n+1 {
			key := elem.Value.(*lruEntry).key
			e, ok := c.entries[key]
			if !ok || !bulkSendable(e, memOK) || skipL2WriteForKey(key) || !handoverKeyFits(key) || bulkRemaining(e, now) < MinUsableTTL {
				continue
			}
			items = append(items, bulkItem{key: key, size: int64(len(key) + diskRecordHeaderSize + len(e.value))})
		}
		if elem == nil {
			break
		}
		key := elem.Value.(*lruEntry).key
		c.mu.RUnlock()
		runtime.Gosched()
		c.mu.RLock()
		if c.lruIndex[key] != elem {
			elem = nil
		}
	}
	c.mu.RUnlock()
	memoryItems := len(items)
	if dc := c.l2; dc != nil {
		// The disk index walk, the same way under the disk lock, which a
		// query's write takes while it queues its disk record.
		cutoff := now.Add(MinUsableTTL).UnixNano()
		dc.writeMu.Lock()
		for e := dc.newest; e != nil; {
			for n := 0; e != nil && n < bulkPlanChunk; e, n = e.next, n+1 {
				if !e.invalid && e.expiresAt > cutoff {
					items = append(items, bulkItem{key: e.key, size: e.size})
				}
			}
			if e == nil {
				break
			}
			dc.writeMu.Unlock()
			runtime.Gosched()
			dc.writeMu.Lock()
			if dc.index[e.key] != e {
				e = nil
			}
		}
		dc.writeMu.Unlock()
	}
	seen := make(map[string]struct{}, memoryItems)
	out := items[:0]
	var bytes int64
	for i, it := range items {
		if i%bulkPlanChunk == bulkPlanChunk-1 {
			runtime.Gosched() // the ownership hashes, a chunk at a time
		}
		if i < memoryItems {
			seen[it.key] = struct{}{}
		} else if _, dup := seen[it.key]; dup {
			continue
		}
		if !keep(it.key) {
			continue
		}
		out = append(out, it)
		bytes += it.size
	}
	return out, bytes
}

// bulkSendable reports whether a memory entry may go to a restoring peer: not
// a non-owner shadow, and memory-only only when restorable and the scopes
// match.
func bulkSendable(e entry, memOK bool) bool {
	return !e.shadow && (!e.memOnly || (e.restorable && memOK))
}

func bulkRemaining(e entry, now time.Time) time.Duration {
	expiry := e.expiresAt
	if e.sourceExpiresAt != 0 {
		if source := time.Unix(0, e.sourceExpiresAt); source.Before(expiry) {
			expiry = source
		}
	}
	return expiry.Sub(now)
}

// bulkRecord is one record ready to send.
type bulkRecord struct {
	key     string
	record  []byte
	ttl     time.Duration
	memOnly bool
	// value and expiry hold a memory or queued value until it is encoded;
	// disk marks a committed record to read.
	value  []byte
	expiry int64
	disk   bool
}

// bulkReadBatch reads one batch of planned keys: memory first, then the disk
// write queue, then committed disk records copied out of one read
// transaction. Nothing it reads counts as a lookup or moves a recency. It
// returns the records to send; the rest of the batch was removed, expired or
// unreadable since it was planned.
func (c *Cache) bulkReadBatch(items []bulkItem, memOK bool, codec diskCodec, workers int, arena *[]byte) []bulkRecord {
	now := clockNow()
	recs := make([]bulkRecord, len(items))
	// held marks keys the memory tier holds: its value is the current one,
	// so a disk record of such a key is never sent instead.
	held := make([]bool, len(items))
	c.mu.RLock()
	for i, it := range items {
		recs[i].key = it.key
		e, ok := c.entries[it.key]
		held[i] = ok
		if !ok || !bulkSendable(e, memOK) {
			continue
		}
		if remaining := bulkRemaining(e, now); remaining >= MinUsableTTL {
			recs[i].value, recs[i].ttl, recs[i].memOnly = e.value, remaining, e.memOnly
			recs[i].expiry = now.Add(remaining).UnixNano()
		}
	}
	c.mu.RUnlock()
	dc := c.l2
	if dc != nil {
		cutoff := now.Add(MinUsableTTL).UnixNano()
		dc.writeMu.Lock()
		for i := range recs {
			r := &recs[i]
			if held[i] {
				continue
			}
			entry, ok := dc.writeBuf[r.key]
			if !ok {
				entry, ok = dc.flushing[r.key]
			}
			switch {
			case ok:
				if entry.state != nil && !entry.state.cancelled.Load() && entry.ExpiresAt > cutoff {
					r.value, r.expiry = entry.Value, entry.ExpiresAt
					r.ttl = time.Duration(entry.ExpiresAt - now.UnixNano())
				}
			default:
				if e := dc.index[r.key]; e != nil && !e.invalid && e.expiresAt > cutoff {
					r.disk, r.expiry = true, e.expiresAt
					r.ttl = time.Duration(e.expiresAt - now.UnixNano())
				}
			}
		}
		dc.writeMu.Unlock()
		dc.readBulkRecords(recs, workers, arena)
	}
	out := recs[:0]
	for _, r := range recs {
		if r.value != nil {
			encoded, err := encodeDiskRecord(diskEntry{Value: r.value, ExpiresAt: r.expiry}, codec)
			if err != nil {
				continue
			}
			r.record, r.value = encoded, nil
		}
		if r.record != nil {
			out = append(out, r)
		}
	}
	return out
}

// readBulkRecords copies the committed native records the batch needs out of
// one read transaction, checked as a lookup checks them. The keys are looked
// up in order and the records copied by at most min(GOMAXPROCS, 4)
// goroutines, so a file that is not in the page cache is read with that much
// parallelism (as the warm-up decodes). Legacy records are not sent.
func (dc *DiskCache) readBulkRecords(recs []bulkRecord, workers int, arena *[]byte) {
	need := false
	for i := range recs {
		need = need || recs[i].disk
	}
	if !need {
		return
	}
	err := dc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		if b == nil {
			return nil
		}
		// The records are copied into one buffer the stream reuses for every
		// batch (valid until the next one), so serving allocates no copy per
		// record and adds little garbage for the queries' GC to collect.
		raws := make([][]byte, len(recs))
		offsets := make([]int, len(recs)+1)
		for i := range recs {
			if recs[i].disk {
				raws[i] = b.Get([]byte(recs[i].key))
			}
			offsets[i+1] = offsets[i] + len(raws[i])
		}
		if arena == nil {
			arena = new([]byte)
		}
		if cap(*arena) < offsets[len(recs)] {
			*arena = make([]byte, offsets[len(recs)])
		}
		buf := (*arena)[:offsets[len(recs)]]
		var next atomic.Int64
		copyOut := func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(recs) {
					return
				}
				if v := raws[i]; v != nil && dc.validateStoredDiskRecord(v, true) == nil {
					recs[i].record = buf[offsets[i]:offsets[i+1]:offsets[i+1]]
					copy(recs[i].record, v)
				}
			}
		}
		var wg sync.WaitGroup
		for n := 1; n < workers; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				copyOut()
			}()
		}
		copyOut()
		wg.Wait()
		return nil
	})
	if err != nil {
		for i := range recs {
			if recs[i].disk {
				recs[i].record = nil
			}
		}
	}
}

// bulkWriter writes frames to the requester under the rate limit, extending
// the write deadline as it goes.
type bulkWriter struct {
	ctx   context.Context
	bw    *bufio.Writer
	rc    *http.ResponseController
	gov   *bulkGovernor
	scrap []byte
	err   error
	// waited is the time spent waiting for the governor since it was reset,
	// so a write's own time can be told from the pacing.
	waited time.Duration
}

func (w *bulkWriter) write(p []byte) {
	if w.err == nil {
		_, w.err = w.bw.Write(p)
	}
}

func (w *bulkWriter) frame(f bulkFrame) {
	w.scrap = appendBulkHeader(w.scrap[:0], f)
	w.write(w.scrap)
}

func (w *bulkWriter) record(r bulkRecord) {
	if w.err != nil {
		return
	}
	start := time.Now()
	err := w.gov.wait(w.ctx, len(r.key)+len(r.record)+bulkMaxHeaderBytes)
	w.waited += time.Since(start)
	if err != nil {
		w.err = err
		return
	}
	var flags byte
	if r.memOnly {
		flags = bulkFlagMemoryOnly
	}
	ttlMs := uint64(max(r.ttl.Milliseconds(), 0))
	w.frame(bulkFrame{typ: bulkFrameRecord, flags: flags, a: uint64(len(r.key)), b: uint64(len(r.record)), c: ttlMs})
	w.write([]byte(r.key))
	w.write(r.record)
	sum := crc32.Update(crc32.Update(0, bulkCRC, []byte(r.key)), bulkCRC, r.record)
	w.scrap = binary.BigEndian.AppendUint32(w.scrap[:0], sum)
	w.write(w.scrap)
	// Let queries run between records: on a replica with few cores a batch
	// written in one go holds a thread for its whole length.
	runtime.Gosched()
}

func (w *bulkWriter) flush() {
	if w.err == nil {
		w.err = w.bw.Flush()
	}
	if w.err == nil {
		if err := w.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			w.err = err
		}
	}
}

func (w *bulkWriter) extend() {
	_ = w.rc.SetWriteDeadline(time.Now().Add(peerBulkWriteTimeout))
}

// streamBulk sends the plan, the records batch by batch and the end frame; it
// returns the end outcome.
func (pc *PeerCache) streamBulk(ctx context.Context, w http.ResponseWriter, local *Cache, req bulkRequest, ring *bulkRing, memOK bool) byte {
	generation, idle := local.diskPromotionGeneration()
	keep := func(key string) bool { return ring.designated(key) && pc.pushFilterAllows(key) }
	var items []bulkItem
	var planned int64
	if idle {
		items, planned = local.bulkPlan(keep, memOK)
	}
	codec := diskCodecS2
	if local.l2 != nil {
		codec = local.l2.codec
	}
	w.Header().Set("Content-Type", peerBulkContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	gov := pc.bulk.gov
	out := &bulkWriter{ctx: ctx, bw: bufio.NewWriterSize(w, peerBulkWriteBuffer), rc: http.NewResponseController(w), gov: gov}
	out.extend()
	out.write([]byte(peerBulkMagic))
	out.frame(bulkFrame{typ: bulkFramePlan, a: uint64(len(items)), b: uint64(planned)})
	out.flush()
	outcome := bulkEndDone
	if !idle {
		outcome = bulkEndPurged
	}
	var sent, sentBytes, skipped int64
	var arena []byte
	for i := 0; i < len(items) && outcome == bulkEndDone; {
		switch generationNow, idleNow := local.diskPromotionGeneration(); {
		case ctx.Err() != nil || out.err != nil:
			outcome = bulkEndStopped
			continue
		case !idleNow || generationNow != generation:
			outcome = bulkEndPurged
			continue
		case req.MaxBytes > 0 && sentBytes >= req.MaxBytes:
			outcome = bulkEndBudget
			continue
		}
		// A batch holds about bulkBatchWindow of the current rate, so the
		// reads, copies and encoding are paced as the sending is.
		j, batchBytes, limit := i, int64(0), gov.batchBytes()
		for j < len(items) && j-i < restoreBatchKeys && (j == i || batchBytes+items[j].size <= limit) {
			batchBytes += items[j].size
			j++
		}
		readAt := time.Now()
		recs := local.bulkReadBatch(items[i:j], memOK, codec, gov.workers(), &arena)
		skipped += int64(j-i) - int64(len(recs))
		out.extend()
		out.waited = 0
		var wrote int
		for _, r := range recs {
			// The TTL left when the record is sent, not when its batch was
			// read: the rate limit may hold it back, and the receiver starts
			// the TTL when the record arrives.
			if r.ttl -= time.Since(readAt); r.ttl < MinUsableTTL {
				skipped++
				continue
			}
			out.record(r)
			if out.err != nil {
				break
			}
			sent++
			sentBytes += int64(len(r.key) + len(r.record))
			wrote += len(r.key) + len(r.record)
		}
		out.flush()
		gov.wrote(wrote, time.Since(readAt)-out.waited)
		i = j
	}
	switch {
	case errors.Is(out.err, errBulkThrottled):
		outcome, out.err = bulkEndThrottled, nil
	case out.err != nil || ctx.Err() != nil:
		outcome = bulkEndStopped
	}
	pc.bulk.records.Add(sent)
	pc.bulk.bytes.Add(sentBytes)
	if outcome != bulkEndStopped {
		out.frame(bulkFrame{typ: bulkFrameEnd, flags: outcome, a: uint64(sent), b: uint64(sentBytes), c: uint64(skipped)})
		out.flush()
	}
	return outcome
}

// bulkServeText renders the serving side's metrics in the Prometheus text
// format.
func (pc *PeerCache) bulkServeText() string {
	if pc.bulk == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("# HELP loki_vl_proxy_peer_cache_bulk_serve_streams_total Peer bulk restore streams this replica served, by how they ended: done, budget (the requester's byte budget), purged (a local purge), stopped (the requester went away, a write failed, the deadline passed, or this replica began shutting down), throttled (paused under pressure past -peer-bulk-serve-max-pause).\n")
	b.WriteString("# TYPE loki_vl_proxy_peer_cache_bulk_serve_streams_total counter\n")
	for i, name := range bulkEndNames {
		fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_bulk_serve_streams_total{outcome=%q} %d\n", name, pc.bulk.streams[i].Load())
	}
	b.WriteString("# HELP loki_vl_proxy_peer_cache_bulk_serve_rejected_total Peer bulk restore requests this replica did not stream, by reason: busy (-peer-bulk-serve-max-streams in use), throttled (the serving side is paused or under pressure), restoring (it restores from peers itself), disabled, bad_request.\n")
	b.WriteString("# TYPE loki_vl_proxy_peer_cache_bulk_serve_rejected_total counter\n")
	for i, name := range bulkRejectNames {
		fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_bulk_serve_rejected_total{reason=%q} %d\n", name, pc.bulk.rejected[i].Load())
	}
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_records_total Records this replica sent in peer bulk restore streams.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_records_total counter\nloki_vl_proxy_peer_cache_bulk_serve_records_total %d\n", pc.bulk.records.Load())
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_bytes_total Key and record bytes this replica sent in peer bulk restore streams (under -peer-bulk-serve-max-bytes-per-second).\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_bytes_total counter\nloki_vl_proxy_peer_cache_bulk_serve_bytes_total %d\n", pc.bulk.bytes.Load())
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_active Peer bulk restore streams this replica serves now.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_active gauge\nloki_vl_proxy_peer_cache_bulk_serve_active %d\n", pc.bulk.active.Load())
	g := pc.bulk.gov
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_rate_bytes_per_second The rate the bulk governor lets this replica's restore streams send now (0 while idle or paused).\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_rate_bytes_per_second gauge\nloki_vl_proxy_peer_cache_bulk_serve_rate_bytes_per_second %d\n", min(g.rateNow.Load(), math.MaxInt64))
	b.WriteString("# HELP loki_vl_proxy_peer_cache_bulk_serve_state 1 for the bulk governor's state: idle, ramping, ceiling (-peer-bulk-serve-max-bytes-per-second), throttled (pressure halved the rate), paused.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_state gauge\n")
	for i, name := range bulkStateNames {
		fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_bulk_serve_state{state=%q} %d\n", name, boolInt(g.stateNow.Load() == int64(i)))
	}
	b.WriteString("# HELP loki_vl_proxy_peer_cache_bulk_serve_pressure_total Governor ticks (200 ms) that saw pressure, by signal: scheduler_latency, cpu, gc, memory, request_latency, peer_latency, disk_latency, network.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_pressure_total counter\n")
	for i, name := range bulkPressureNames {
		fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_bulk_serve_pressure_total{signal=%q} %d\n", name, g.pressure[i].Load())
	}
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_rate_decreases_total Times pressure halved the bulk restore rate.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_rate_decreases_total counter\nloki_vl_proxy_peer_cache_bulk_serve_rate_decreases_total %d\n", g.decreases.Load())
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_pauses_total Times a severe signal paused every bulk restore stream.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_pauses_total counter\nloki_vl_proxy_peer_cache_bulk_serve_pauses_total %d\n", g.pauses.Load())
	fmt.Fprintf(&b, "# HELP loki_vl_proxy_peer_cache_bulk_serve_paused_seconds_total Time the bulk restore streams spent paused.\n# TYPE loki_vl_proxy_peer_cache_bulk_serve_paused_seconds_total counter\nloki_vl_proxy_peer_cache_bulk_serve_paused_seconds_total %g\n", time.Duration(g.pausedNs.Load()).Seconds())
	return b.String()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// SetBulkLoad gives the bulk governor this replica's client request counters
// (completed requests and their total latency).
func (pc *PeerCache) SetBulkLoad(load func() BulkLoad) {
	if pc == nil || pc.bulk == nil || pc.bulk.gov.signals == nil {
		return
	}
	if load != nil {
		pc.bulk.gov.signals.load.Store(&load)
	}
}

// localDisk is the attached local cache's disk tier, for the governor.
func (pc *PeerCache) localDisk() *DiskCache {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.localCache == nil {
		return nil
	}
	return pc.localCache.l2
}
