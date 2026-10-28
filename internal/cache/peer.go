package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	gzip "github.com/klauspost/compress/gzip"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

var lookupHost = net.LookupHost
var lookupSRV = net.LookupSRV

const (
	maxPeerResponseBytes    int64 = 32 << 20
	maxPeerDecodedBodyBytes int64 = 32 << 20
	maxPeerHotIndexBytes    int64 = 4 << 20
	maxPeerSetBodyBytes     int64 = 8 << 20
	peerCompressionMinBytes       = 1024

	// MaxReplicationFactor bounds PeerConfig.ReplicationFactor: the replicas
	// a write is pushed to and a lookup may ask.
	MaxReplicationFactor = 5

	// peerMemoryOnlyHeader marks a value its replica keeps in memory only
	// (SetLocalOnlyWithTTL); the replica that fetches it does not write it to
	// its disk either.
	peerMemoryOnlyHeader = "X-Cache-Memory-Only"
)

// PeerCache implements a sharded distributed cache layer (L3).
//
// Simple design — no gossip, no background traffic:
//
//	L1 local cache (fresh?) → serve (0 hops)
//	L1 miss → hash ring: "peer X owns this key" → fetch from X (1 hop)
//	Store shadow copy in L1 (short TTL) → next request same peer = 0 hops
//	If I'm the owner → skip L3, go to VL directly
//
// The only network traffic is the actual cache fetch on L1 miss, and the
// write-through pushes.
//
// Replication: a key's replicas are its owner and the next distinct peers
// clockwise on the ring, ReplicationFactor in all. A write-through write is
// pushed to every replica; a replica stores a pushed value in memory and on
// disk and never pushes it again. A lookup that misses memory and disk asks
// the replicas in ring order (an owner that lost its disk asks its backup)
// and stores what it gets locally, without pushing. Non-replicas keep
// short-lived shadow copies after fetching.
type PeerCache struct {
	mu           sync.RWMutex
	ring         *hashRing
	selfAddr     string
	selfAZ       string   // this instance's availability zone (optional)
	peerAZs      sync.Map // string → string: peer addr → availability zone
	peers        []string
	peerHosts    map[string]struct{}
	client       *http.Client
	log          *slog.Logger
	done         chan struct{}
	discoveryFn  func() ([]string, error)
	discoveryInt time.Duration
	authToken    string
	writeThrough bool
	wtMinTTL     time.Duration
	replication  int                   // replicas per key, owner included (1..MaxReplicationFactor)
	pushFilter   func(key string) bool // keys that may be pushed; nil allows all
	localCache   *Cache

	// Bounded hot read-ahead (optional).
	readAheadEnabled          bool
	readAheadInterval         time.Duration
	readAheadJitter           time.Duration
	readAheadTopN             int
	readAheadMaxKeysPerTick   int
	readAheadMaxBytesPerTick  int64
	readAheadMaxConcurrency   int
	readAheadMinTTL           time.Duration
	readAheadMaxObjectBytes   int
	readAheadTenantFairShare  int // max % of key budget one tenant may consume in fairness pass
	readAheadErrorBackoff     time.Duration
	readAheadBackoffUntilUnix atomic.Int64
	readAheadErrorStreak      atomic.Int64
	readAheadStarted          atomic.Bool
	readAheadRandMu           sync.Mutex
	readAheadRand             *rand.Rand

	// Per-peer circuit breakers
	breakers sync.Map // string → *peerBreaker

	// Singleflight to prevent cache stampede
	inflight sync.Map // key → *inflightEntry

	// Low-cardinality fetch error reasons for observability.
	peerErrorReasons sync.Map // string → *atomic.Int64
	peerSetEncodings sync.Map // string → string

	// Stats
	PeerHits   atomic.Int64
	PeerMisses atomic.Int64
	PeerErrors atomic.Int64
	WTPushes   atomic.Int64
	WTErrors   atomic.Int64
	// BackupHits counts peer hits served by a replica other than the key's
	// owner (included in PeerHits).
	BackupHits atomic.Int64
	// PushTooLarge counts pushes skipped because the value is above the
	// peers' accepted body size.
	PushTooLarge atomic.Int64
	// PeerNotUp counts refused connections to peers that had not answered
	// yet since this process started (skipped briefly, not failures), and
	// PushPeerNotUp the pushes to such peers that were dropped (above the
	// pending bound, or the peer never answered within pendingPushMaxWait).
	PeerNotUp     atomic.Int64
	PushPeerNotUp atomic.Int64
	// PushThrottled counts pushes dropped because the push queue was full
	// (maxQueuedPushes pushes or maxQueuedPushBytes waiting behind
	// maxInFlightPushes running ones).
	PushThrottled atomic.Int64
	pushQueue     chan pushJob
	pushQueued    atomic.Int64 // bytes waiting in pushQueue
	pushers       atomic.Int32 // push workers running
	// fetchDurations times every replica fetch by the replica's role and the
	// outcome.
	fetchDurations [2][3]durationHistogram // [owner, backup][hit, miss, error]
	RAHotReq       atomic.Int64
	RAHotErr       atomic.Int64
	RAPrefetch     atomic.Int64
	RABytes        atomic.Int64
	RABudget       atomic.Int64
	RATenant       atomic.Int64
}

type inflightEntry struct {
	done    chan struct{}
	result  []byte
	ttl     time.Duration
	memOnly bool
	ok      bool
}

// PeerConfig configures the distributed peer cache.
type PeerConfig struct {
	SelfAddr                 string        // this instance's address (ip:port)
	SelfAZ                   string        // this instance's availability zone (e.g. "us-east-1a"); prefer same-AZ peers when set
	DiscoveryType            string        // "dns", "srv", "http", "static", or "" (disabled)
	DNSName                  string        // headless service DNS name (for "dns" mode)
	SRVName                  string        // full SRV name e.g. "_loki-vl-proxy._tcp.ns.svc.cluster.local" (for "srv" mode)
	HTTPPeersURL             string        // URL returning JSON peer list (for "http" mode)
	StaticPeers              string        // comma-separated peer addresses (for "static" mode)
	Port                     int           // peer cache HTTP port (default: 3100)
	DiscoveryInterval        time.Duration // peer list refresh interval (default: 15s)
	Timeout                  time.Duration // peer HTTP request timeout (default: 2s)
	AuthToken                string        // optional shared token for peer cache endpoints
	WriteThrough             bool          // push owner copies on local SetWithTTL
	WriteThroughMinTTL       time.Duration // minimum TTL to trigger write-through
	ReplicationFactor        int           // replicas per key, owner included; 0 means 1 (owner only)
	ReadAheadEnabled         bool          // periodically prefetch bounded hot keys from peers
	ReadAheadInterval        time.Duration // read-ahead base interval
	ReadAheadJitter          time.Duration // random jitter added to interval
	ReadAheadTopN            int           // owner hot-index top N keys
	ReadAheadMaxKeys         int           // max keys prefetched per interval
	ReadAheadMaxBytes        int64         // max bytes prefetched per interval
	ReadAheadMaxConcurrency  int           // max concurrent hot-index/prefetch pulls
	ReadAheadMinTTL          time.Duration // skip near-expiry entries
	ReadAheadMaxObjectBytes  int           // skip very large entries
	ReadAheadTenantFairShare int           // fairness pass cap (% of key budget per tenant)
	ReadAheadErrorBackoff    time.Duration // base cooldown after read-ahead errors
	Logger                   *slog.Logger
}

type hotIndexResponse struct {
	Owner   string          `json:"owner"`
	Entries []hotIndexEntry `json:"entries"`
}

type hotIndexEntry struct {
	Key            string `json:"key"`
	Tenant         string `json:"tenant"`
	Score          uint64 `json:"score"`
	SizeBytes      int    `json:"size_bytes"`
	RemainingTTLMS int64  `json:"remaining_ttl_ms"`
}

// NewPeerCache creates a sharded peer cache.
func NewPeerCache(cfg PeerConfig) *PeerCache {
	if cfg.Port == 0 {
		cfg.Port = 3100
	}
	if cfg.DiscoveryInterval == 0 {
		cfg.DiscoveryInterval = 15 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.ReadAheadInterval <= 0 {
		cfg.ReadAheadInterval = 30 * time.Second
	}
	if cfg.ReadAheadJitter < 0 {
		cfg.ReadAheadJitter = 0
	}
	if cfg.ReadAheadTopN <= 0 {
		cfg.ReadAheadTopN = 256
	}
	if cfg.ReadAheadMaxKeys <= 0 {
		cfg.ReadAheadMaxKeys = 64
	}
	if cfg.ReadAheadMaxBytes <= 0 {
		cfg.ReadAheadMaxBytes = 8 << 20 // 8MiB
	}
	if cfg.ReadAheadMaxConcurrency <= 0 {
		cfg.ReadAheadMaxConcurrency = 4
	}
	if cfg.ReadAheadMinTTL <= 0 {
		cfg.ReadAheadMinTTL = 30 * time.Second
	}
	if cfg.ReadAheadMaxObjectBytes <= 0 {
		cfg.ReadAheadMaxObjectBytes = 256 << 10 // 256KiB
	}
	if cfg.ReadAheadTenantFairShare <= 0 {
		cfg.ReadAheadTenantFairShare = 50
	}
	if cfg.ReadAheadTenantFairShare > 100 {
		cfg.ReadAheadTenantFairShare = 100
	}
	if cfg.ReadAheadErrorBackoff <= 0 {
		cfg.ReadAheadErrorBackoff = 15 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	pc := &PeerCache{
		selfAddr: cfg.SelfAddr,
		selfAZ:   strings.TrimSpace(cfg.SelfAZ),
		ring:     newHashRing(150),
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     20,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		log:                      cfg.Logger,
		done:                     make(chan struct{}),
		discoveryInt:             cfg.DiscoveryInterval,
		authToken:                strings.TrimSpace(cfg.AuthToken),
		writeThrough:             cfg.WriteThrough,
		wtMinTTL:                 cfg.WriteThroughMinTTL,
		readAheadEnabled:         cfg.ReadAheadEnabled,
		readAheadInterval:        cfg.ReadAheadInterval,
		readAheadJitter:          cfg.ReadAheadJitter,
		readAheadTopN:            cfg.ReadAheadTopN,
		readAheadMaxKeysPerTick:  cfg.ReadAheadMaxKeys,
		readAheadMaxBytesPerTick: cfg.ReadAheadMaxBytes,
		readAheadMaxConcurrency:  cfg.ReadAheadMaxConcurrency,
		readAheadMinTTL:          cfg.ReadAheadMinTTL,
		readAheadMaxObjectBytes:  cfg.ReadAheadMaxObjectBytes,
		readAheadTenantFairShare: cfg.ReadAheadTenantFairShare,
		readAheadErrorBackoff:    cfg.ReadAheadErrorBackoff,
		readAheadRand:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if pc.wtMinTTL <= 0 {
		pc.wtMinTTL = 30 * time.Second
	}
	pc.replication = min(max(cfg.ReplicationFactor, 1), MaxReplicationFactor)
	pc.pushQueue = make(chan pushJob, maxQueuedPushes)

	switch cfg.DiscoveryType {
	case "dns":
		pc.discoveryFn = func() ([]string, error) {
			return discoverDNS(cfg.DNSName, cfg.Port)
		}
	case "srv":
		srvName := cfg.SRVName
		pc.discoveryFn = func() ([]string, error) {
			return discoverSRV(srvName)
		}
	case "http":
		httpURL := cfg.HTTPPeersURL
		httpTimeout := cfg.Timeout
		pc.discoveryFn = func() ([]string, error) {
			peers, azMap, err := discoverHTTPPeersWithAZ(httpURL, httpTimeout)
			for peer, az := range azMap {
				if az != "" {
					pc.peerAZs.Store(peer, az)
				}
			}
			return peers, err
		}
	case "static":
		staticPeers := parsePeerList(cfg.StaticPeers)
		pc.discoveryFn = func() ([]string, error) {
			return staticPeers, nil
		}
	default:
		return pc
	}

	if pc.discoveryFn != nil {
		if peers, err := pc.discoveryFn(); err == nil {
			pc.updatePeers(peers)
		}
		go pc.discoveryLoop()
	}

	return pc
}

// AttachLocalCache links peer cache to the local L1 cache so read-ahead can
// prewarm shadows without backend fanout.
func (pc *PeerCache) AttachLocalCache(c *Cache) {
	if pc == nil || c == nil {
		return
	}
	pc.mu.Lock()
	pc.localCache = c
	startLoop := pc.readAheadEnabled && !pc.readAheadStarted.Load()
	if startLoop {
		pc.readAheadStarted.Store(true)
	}
	pc.mu.Unlock()
	if startLoop {
		go pc.readAheadLoop()
	}
}

// IsOwner returns true if this instance owns the given key.
// Owner = the peer that the hash ring maps this key to.
// If we're the owner, caller should skip L3 and go to VL directly.
func (pc *PeerCache) IsOwner(key string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if len(pc.peers) == 0 {
		return true
	}
	return pc.ring.get(key) == pc.selfAddr
}

// ReplicationFactor returns the replicas per key, owner included.
func (pc *PeerCache) ReplicationFactor() int {
	if pc == nil {
		return 1
	}
	return pc.replication
}

// SetPushFilter sets which keys may be pushed to peers (nil allows all): a
// key it rejects stays on the replica that wrote it.
func (pc *PeerCache) SetPushFilter(allow func(key string) bool) {
	pc.mu.Lock()
	pc.pushFilter = allow
	pc.mu.Unlock()
}

// replicasOf returns the key's replicas in ring order, owner first, or nil
// when no peers are known.
func (pc *PeerCache) replicasOf(key string) []string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if len(pc.peers) == 0 {
		return nil
	}
	return pc.ring.getN(key, pc.replication)
}

// IsReplica reports whether this instance is one of the key's replicas (true
// without peers). It does not allocate.
func (pc *PeerCache) IsReplica(key string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if len(pc.peers) == 0 {
		return true
	}
	return pc.ring.has(key, pc.replication, pc.selfAddr)
}

// pushAllowed reports whether a value may be pushed to peers.
func (pc *PeerCache) pushAllowed(key string, value []byte) bool {
	pc.mu.RLock()
	allow := pc.pushFilter
	pc.mu.RUnlock()
	if allow != nil && !allow(key) {
		return false
	}
	if int64(len(value)) > maxPeerSetBodyBytes {
		// A peer refuses it; sending it would only count a failure against
		// the peer's breaker, which also gates reads from it.
		pc.PushTooLarge.Add(1)
		return false
	}
	return true
}

// Get fetches a value from the key's replicas in ring order (owner first,
// skipping this instance), and returns the first hit.
// Returns (value, remainingTTL, true) on hit.
// The caller should use remainingTTL for the shadow copy — never extend the original.
func (pc *PeerCache) Get(key string) ([]byte, time.Duration, bool) {
	v, ttl, _, ok := pc.getReplica(key)
	return v, ttl, ok
}

// getReplica is Get, also reporting whether the replica that answered keeps
// the value in memory only.
func (pc *PeerCache) getReplica(key string) ([]byte, time.Duration, bool, bool) {
	replicas := pc.replicasOf(key)
	if len(replicas) == 0 || (len(replicas) == 1 && replicas[0] == pc.selfAddr) {
		return nil, 0, false, false
	}

	// Singleflight: coalesce concurrent fetches for the same key
	if entry, loaded := pc.inflight.LoadOrStore(key, &inflightEntry{done: make(chan struct{})}); loaded {
		inf := entry.(*inflightEntry)
		<-inf.done
		if inf.ok {
			pc.PeerHits.Add(1)
		} else {
			pc.PeerMisses.Add(1)
		}
		return inf.result, inf.ttl, inf.memOnly, inf.ok
	}

	inf := pc.getInflight(key)
	defer func() {
		close(inf.done)
		pc.inflight.Delete(key)
	}()

	asked := false
	for i, addr := range replicas {
		if addr == pc.selfAddr {
			continue
		}
		if !pc.peerAllowed(addr) {
			pc.RecordPeerErrorReason("breaker_open")
			pc.PeerErrors.Add(1)
			continue
		}
		asked = true
		started := time.Now()
		body, ttl, memOnly, status := pc.fetchFrom(addr, key)
		role, outcome := 0, 0
		if i > 0 {
			role = 1
		}
		switch status {
		case http.StatusNotFound:
			outcome = 1
		case 0:
			outcome = 2
		}
		pc.fetchDurations[role][outcome].observe(time.Since(started))
		if status != http.StatusOK {
			continue
		}
		inf.result, inf.ttl, inf.memOnly, inf.ok = body, ttl, memOnly, true
		pc.PeerHits.Add(1)
		if i > 0 {
			pc.BackupHits.Add(1)
		}
		return body, ttl, memOnly, true
	}
	if asked {
		pc.PeerMisses.Add(1)
	}
	return nil, 0, false, false
}

// fetchFrom asks one replica for key: the value, its remaining TTL and
// whether the replica keeps it in memory only, with status 200; 404 on a
// miss; 0 on an error (recorded against the peer).
func (pc *PeerCache) fetchFrom(owner, key string) ([]byte, time.Duration, bool, int) {
	endpoint := fmt.Sprintf("http://%s/_cache/get?%s", owner, url.Values{"key": []string{key}}.Encode())
	ctx, cancel := context.WithTimeout(pc.peerContext(owner), pc.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason("request_build")
		pc.PeerErrors.Add(1)
		return nil, 0, false, 0
	}
	if pc.authToken != "" {
		req.Header.Set("X-Peer-Token", pc.authToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := pc.client.Do(req)
	if err != nil {
		if pc.recordPeerCallError(owner, ctx, err) {
			pc.RecordPeerErrorReason("peer_not_up")
		} else {
			pc.RecordPeerErrorReason(classifyPeerFetchError(err))
		}
		pc.PeerErrors.Add(1)
		return nil, 0, false, 0
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		pc.recordPeerSuccess(owner)
		pc.recordPeerSetEncoding(owner, resp.Header.Get("X-Peer-Set-Encoding"))
		return nil, 0, false, http.StatusNotFound
	}
	if resp.StatusCode != http.StatusOK {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason(peerStatusReason(resp.StatusCode))
		pc.PeerErrors.Add(1)
		return nil, 0, false, 0
	}

	body, err := readPeerBodyLimited(resp.Body, maxPeerResponseBytes)
	if err != nil {
		pc.recordPeerCallError(owner, ctx, err)
		pc.RecordPeerErrorReason("body_read")
		pc.PeerErrors.Add(1)
		return nil, 0, false, 0
	}
	body, err = decodePeerResponseBody(resp.Header.Get("Content-Encoding"), body, maxPeerDecodedBodyBytes)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason("decode")
		pc.PeerErrors.Add(1)
		return nil, 0, false, 0
	}

	// Parse remaining TTL from owner's response
	var remainingTTL time.Duration
	if ttlMs := resp.Header.Get("X-Cache-TTL-Ms"); ttlMs != "" {
		var msVal int64
		if _, err := fmt.Sscanf(ttlMs, "%d", &msVal); err == nil {
			remainingTTL = time.Duration(msVal) * time.Millisecond
		}
	}

	pc.recordPeerSuccess(owner)
	pc.recordPeerSetEncoding(owner, resp.Header.Get("X-Peer-Set-Encoding"))
	return body, remainingTTL, resp.Header.Get(peerMemoryOnlyHeader) == "1", http.StatusOK
}

// Set optionally pushes the key to owner based on write-through config.
// Default behavior remains no-op for network writes.
func (pc *PeerCache) Set(key string, value []byte) {
	pc.SetWithTTL(key, value, 0)
}

// SetWithTTL optionally pushes the key to its replicas (owner and backups)
// so the ring stays warm even when client traffic is temporarily skewed to a
// subset of replicas, and a replica that loses its disk finds its keys on
// the others.
func (pc *PeerCache) SetWithTTL(key string, value []byte, ttl time.Duration) {
	if !pc.writeThrough || len(value) == 0 {
		return
	}
	if ttl > 0 && ttl < pc.wtMinTTL {
		return
	}
	pc.Replicate(key, value, ttl)
}

// Replicate pushes the key to its replicas other than this instance,
// whatever the write-through setting, in the background. Keys the push
// filter rejects and values above the peers' body limit are not pushed.
func (pc *PeerCache) Replicate(key string, value []byte, ttl time.Duration) {
	if pc == nil || len(value) == 0 || !pc.pushAllowed(key, value) {
		return
	}
	for _, addr := range pc.replicasOf(key) {
		if addr == pc.selfAddr {
			continue
		}
		if !pc.peerAllowed(addr) {
			if pb := pc.breaker(addr); !pb.seenUp.Load() && time.Now().UnixNano() < pb.notUpUntil.Load() {
				pc.deferPush(addr, key, value, ttl) // sent once the peer answers
			} else {
				pc.WTErrors.Add(1)
			}
			continue
		}
		pc.enqueuePush(pushJob{addr: addr, key: key, value: value, ttl: ttl})
	}
}

// Replica pushes run on at most maxInFlightPushes workers (each push holds a
// connection for up to the peer timeout, and under load pushes compete with
// requests for CPU); the others wait in a queue of at most maxQueuedPushes
// pushes and maxQueuedPushBytes. A push that does not fit is dropped and
// counted (PushThrottled); the replica that misses the copy fetches it from
// another replica on its first lookup.
const (
	maxInFlightPushes  = 64
	maxQueuedPushes    = 4096
	maxQueuedPushBytes = 64 << 20
)

type pushJob struct {
	addr, key string
	value     []byte
	ttl       time.Duration
}

func (pc *PeerCache) enqueuePush(job pushJob) {
	if pc.pushQueue == nil { // a PeerCache not built by NewPeerCache
		go pc.pushToOwner(job.addr, job.key, job.value, job.ttl)
		return
	}
	size := int64(len(job.value))
	if pc.pushQueued.Add(size) > maxQueuedPushBytes {
		pc.pushQueued.Add(-size)
		pc.PushThrottled.Add(1)
		return
	}
	select {
	case pc.pushQueue <- job:
	default:
		pc.pushQueued.Add(-size)
		pc.PushThrottled.Add(1)
		return
	}
	for {
		n := pc.pushers.Load()
		if n >= maxInFlightPushes {
			return
		}
		if pc.pushers.CompareAndSwap(n, n+1) {
			go pc.pushWorker()
			return
		}
	}
}

// pushWorker sends queued pushes until the queue is empty or the cache is
// closed.
func (pc *PeerCache) pushWorker() {
	for {
		select {
		case <-pc.done:
			pc.pushers.Add(-1)
			return
		case job := <-pc.pushQueue:
			pc.pushQueued.Add(-int64(len(job.value)))
			pc.pushToOwner(job.addr, job.key, job.value, job.ttl)
			continue
		default:
		}
		pc.pushers.Add(-1)
		// A push queued after the check above, while every worker was
		// counted, still needs one.
		if len(pc.pushQueue) == 0 {
			return
		}
		n := pc.pushers.Load()
		if n >= maxInFlightPushes || !pc.pushers.CompareAndSwap(n, n+1) {
			return
		}
	}
}

// WriteThroughMinTTL returns the minimum TTL a write needs to be pushed to its
// owner peer, or 0 when write-through is disabled.
func (pc *PeerCache) WriteThroughMinTTL() time.Duration {
	if pc == nil || !pc.writeThrough {
		return 0
	}
	return pc.wtMinTTL
}

// pushToOwner pushes the key to one replica and reports whether it landed.
// A push to a replica that is still starting is kept and sent once it
// answers (deferPush).
func (pc *PeerCache) pushToOwner(owner, key string, value []byte, ttl time.Duration) bool {
	return pc.push(owner, key, value, ttl, true)
}

// push is pushToOwner; deferIfNotUp keeps a push to a starting peer.
func (pc *PeerCache) push(owner, key string, value []byte, ttl time.Duration, deferIfNotUp bool) bool {
	ctx, cancel := context.WithTimeout(pc.peerContext(owner), pc.client.Timeout)
	defer cancel()
	ttlMs := int64(0)
	if ttl > 0 {
		ttlMs = ttl.Milliseconds()
	}
	body := value
	encoding := ""
	if len(value) >= peerCompressionMinBytes {
		if supported, ok := pc.peerPreferredSetEncoding(owner); ok && supported == "zstd" {
			encoded, err := encodePeerRequestBody(value, supported)
			if err != nil {
				pc.recordPeerFailure(owner)
				pc.RecordPeerErrorReason("encode")
				pc.WTErrors.Add(1)
				return false
			}
			body = encoded
			encoding = supported
		}
	}
	endpoint := fmt.Sprintf(
		"http://%s/_cache/set?%s",
		owner,
		url.Values{
			"key":    []string{key},
			"ttl_ms": []string{strconv.FormatInt(ttlMs, 10)},
		}.Encode(),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.WTErrors.Add(1)
		return false
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	if pc.authToken != "" {
		req.Header.Set("X-Peer-Token", pc.authToken)
	}
	resp, err := pc.client.Do(req)
	if err != nil {
		if pc.recordPeerCallError(owner, ctx, err) {
			if deferIfNotUp {
				pc.deferPush(owner, key, value, ttl)
			}
		} else {
			pc.WTErrors.Add(1)
		}
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		pc.recordPeerFailure(owner)
		pc.WTErrors.Add(1)
		return false
	}
	pc.recordPeerSuccess(owner)
	pc.WTPushes.Add(1)
	return true
}

// GossipHaveKey is a no-op in the sharded model.
// Kept for interface compatibility.
func (pc *PeerCache) GossipHaveKey(key string) {
	// No gossip in sharded mode.
}

func (pc *PeerCache) getInflight(key string) *inflightEntry {
	v, _ := pc.inflight.Load(key)
	return v.(*inflightEntry)
}

// MinUsableTTL is the minimum remaining TTL to serve a cached value.
// If the value expires in less than this, treat it as a miss (force refresh).
const MinUsableTTL = 5 * time.Second

// ServeHTTP handles incoming peer cache requests.
// GET  /_cache/get?key=...      — return cached value with remaining TTL header (or 404)
// GET  /_cache/has?keys=k1,k2   — batch presence check; returns JSON presence+TTL map
// GET  /_cache/hot              — hot key index
// POST /_cache/set?key=...      — store a value
// GET  /_cache/peers            — current known peer list (diagnostic)
// Header X-Cache-TTL-Ms: remaining TTL in milliseconds (for shadow copy)
func (pc *PeerCache) ServeHTTP(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	switch r.URL.Path {
	case "/_cache/hot":
		pc.serveHotIndex(w, r, localCache)
		return
	case "/_cache/has":
		pc.serveHas(w, r, localCache)
		return
	case "/_cache/peers":
		pc.servePeers(w, r)
		return
	}
	if r.Method == http.MethodPost {
		pc.serveSet(w, r, localCache)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pc.serveGet(w, r, localCache)
}

// PeerKeyPresence describes a single key's presence on a peer.
type PeerKeyPresence struct {
	OK    bool  `json:"ok"`
	TTLMs int64 `json:"ttl_ms,omitempty"`
}

// serveHas handles GET /_cache/has?keys=k1,k2,k3
// Returns a JSON object mapping each requested key to its presence and remaining TTL.
// Keys not found (or expiring soon) have ok=false. No value data is transferred.
func (pc *PeerCache) serveHas(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rawKeys := strings.TrimSpace(r.URL.Query().Get("keys"))
	if rawKeys == "" {
		http.Error(w, "missing keys", http.StatusBadRequest)
		return
	}
	parts := strings.Split(rawKeys, ",")
	if len(parts) > 200 {
		http.Error(w, "too many keys (max 200)", http.StatusBadRequest)
		return
	}
	result := make(map[string]PeerKeyPresence, len(parts))
	for _, raw := range parts {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		_, remaining, ok := localCache.GetWithTTL(key)
		if ok && remaining >= MinUsableTTL {
			result[key] = PeerKeyPresence{OK: true, TTLMs: remaining.Milliseconds()}
		} else {
			result[key] = PeerKeyPresence{OK: false}
		}
	}
	body, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Peer-Set-Encoding", "zstd")
	writePeerEncodedResponse(w, r, body)
}

func (pc *PeerCache) serveGet(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	// Memory, then disk: a replica that restarted keeps its copies there.
	value, remaining, memOnly, ok := localCache.getForPeer(key)
	if !ok || remaining < MinUsableTTL {
		// Expired or about to expire — treat as miss (force refresh from VL)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Cache-TTL-Ms", fmt.Sprintf("%d", remaining.Milliseconds()))
	if memOnly {
		w.Header().Set(peerMemoryOnlyHeader, "1")
	}
	w.Header().Set("X-Peer-Set-Encoding", "zstd")
	writePeerEncodedResponse(w, r, value)
}

func (pc *PeerCache) serveSet(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	ttlMsRaw := strings.TrimSpace(r.URL.Query().Get("ttl_ms"))
	var ttl time.Duration
	if ttlMsRaw != "" {
		var ttlMs int64
		if _, err := fmt.Sscanf(ttlMsRaw, "%d", &ttlMs); err == nil && ttlMs > 0 {
			ttl = time.Duration(ttlMs) * time.Millisecond
		}
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	body, err := readPeerBodyLimited(r.Body, maxPeerSetBodyBytes)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	body, err = decodePeerResponseBody(r.Header.Get("Content-Encoding"), body, maxPeerSetBodyBytes)
	if err != nil {
		http.Error(w, "decode body failed", http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty value", http.StatusBadRequest)
		return
	}
	// A replica's copy: memory and disk, never pushed again.
	localCache.storeReplica(key, body, ttl)
	w.Header().Set("X-Peer-Set-Encoding", "zstd")
	w.WriteHeader(http.StatusNoContent)
}

func (pc *PeerCache) serveHotIndex(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit := pc.readAheadTopN
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 2000 {
		limit = 2000
	}
	entries := localCache.TopHotKeys(limit, pc.readAheadMinTTL, pc.readAheadMaxObjectBytes)
	resp := hotIndexResponse{
		Owner:   pc.selfAddr,
		Entries: make([]hotIndexEntry, 0, len(entries)),
	}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, hotIndexEntry(e))
	}
	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode hot index failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Peer-Set-Encoding", "zstd")
	writePeerEncodedResponse(w, r, body)
}

func writePeerEncodedResponse(w http.ResponseWriter, r *http.Request, body []byte) {
	if len(body) >= peerCompressionMinBytes && acceptsPeerEncoding(r.Header.Get("Accept-Encoding"), "zstd") {
		w.Header().Set("Content-Encoding", "zstd")
		_, _ = w.Write(peerZstdEncoder.EncodeAll(body, make([]byte, 0, len(body)/2)))
		return
	}
	if len(body) >= peerCompressionMinBytes && acceptsPeerEncoding(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		zw, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			http.Error(w, "compression init error", http.StatusInternalServerError)
			return
		}
		if _, err := zw.Write(body); err != nil {
			_ = zw.Close()
			http.Error(w, "compression write error", http.StatusInternalServerError)
			return
		}
		_ = zw.Close()
		return
	}
	_, _ = w.Write(body)
}

func encodePeerRequestBody(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return body, nil
	case "zstd":
		return peerZstdEncoder.EncodeAll(body, make([]byte, 0, len(body)/2)), nil
	case "gzip":
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
		if _, err := zw.Write(body); err != nil {
			_ = zw.Close()
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported peer request encoding %q", encoding)
	}
}

func acceptsPeerEncoding(header, encoding string) bool {
	for _, part := range strings.Split(strings.ToLower(header), ",") {
		token := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if token == encoding || token == "*" {
			return true
		}
	}
	return false
}

// peerZstdMaxWindow is the largest history a peer's zstd frame may declare: the
// 8 MiB window of the encoder peers run. The decoder allocates the declared
// window before it decodes a byte, so an unbounded default (512 MiB) lets a few
// KiB of input cost hundreds of MiB, whatever the response cap is.
const peerZstdMaxWindow = 8 << 20

// The peer zstd encoder and decoder are shared: EncodeAll and DecodeAll are
// safe for concurrent use and reuse their state, where an encoder or decoder
// created per request allocated about 1 MiB each time (a peer fetch cost
// 1.08 MiB of allocation for a 4 KiB value). The decoder decodes at most
// maxPeerDecodedBodyBytes (callers check their own, lower limits) with the
// same window bound as before; the set decoder decodes at most
// maxPeerSetBodyBytes, as the per-request one did for pushes.
var (
	peerZstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	peerZstdDecoder, _ = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxWindow(peerZstdMaxWindow),
		zstd.WithDecoderMaxMemory(uint64(maxPeerDecodedBodyBytes)))
	peerZstdSetDecoder, _ = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxWindow(peerZstdMaxWindow),
		zstd.WithDecoderMaxMemory(uint64(max(maxPeerSetBodyBytes, peerZstdMaxWindow))))
)

func decodePeerResponseBody(contentEncoding string, body []byte, limit int64) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
		return body, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer func() { _ = zr.Close() }()
		return readPeerBodyLimited(zr, limit)
	case "zstd":
		dec := peerZstdDecoder
		if limit > 0 && limit <= maxPeerSetBodyBytes {
			dec = peerZstdSetDecoder
		}
		out, err := dec.DecodeAll(body, nil)
		if err != nil {
			return nil, err
		}
		if limit > 0 && int64(len(out)) > limit {
			return nil, fmt.Errorf("peer response exceeded limit of %d bytes", limit)
		}
		return out, nil
	default:
		return body, nil
	}
}

func (pc *PeerCache) readAheadLoop() {
	for {
		delay := pc.nextReadAheadDelay()
		select {
		case <-pc.done:
			return
		case <-time.After(delay):
		}
		backoffUntil := time.Unix(0, pc.readAheadBackoffUntilUnix.Load())
		if !backoffUntil.IsZero() && time.Now().Before(backoffUntil) {
			continue
		}
		runErrs := pc.runReadAheadCycle()
		pc.applyReadAheadBackoff(runErrs)
	}
}

func (pc *PeerCache) nextReadAheadDelay() time.Duration {
	delay := pc.readAheadInterval
	if pc.readAheadJitter <= 0 {
		return delay
	}
	pc.readAheadRandMu.Lock()
	j := time.Duration(pc.readAheadRand.Int63n(int64(pc.readAheadJitter) + 1))
	pc.readAheadRandMu.Unlock()
	return delay + j
}

func (pc *PeerCache) applyReadAheadBackoff(runErrs int) {
	if runErrs <= 0 {
		pc.readAheadErrorStreak.Store(0)
		pc.readAheadBackoffUntilUnix.Store(0)
		return
	}
	streak := pc.readAheadErrorStreak.Add(1)
	mult := streak
	if mult > 4 {
		mult = 4
	}
	until := time.Now().Add(time.Duration(mult) * pc.readAheadErrorBackoff)
	pc.readAheadBackoffUntilUnix.Store(until.UnixNano())
}

func (pc *PeerCache) runReadAheadCycle() int {
	pc.mu.RLock()
	local := pc.localCache
	peers := append([]string(nil), pc.peers...)
	self := pc.selfAddr
	pc.mu.RUnlock()
	if local == nil || len(peers) == 0 {
		return 0
	}

	candidates, collectErrs := pc.collectHotCandidates(peers, self)
	if len(candidates) == 0 {
		return collectErrs
	}
	selected := pc.selectReadAheadCandidates(candidates)
	if len(selected) == 0 {
		return collectErrs
	}

	sem := make(chan struct{}, pc.readAheadMaxConcurrency)
	var wg sync.WaitGroup
	var runErrs atomic.Int64
	for _, c := range selected {
		wg.Add(1)
		go func(entry hotIndexEntry) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if !pc.prefetchHotKey(local, entry) {
				runErrs.Add(1)
			}
		}(c)
	}
	wg.Wait()
	return collectErrs + int(runErrs.Load())
}

func (pc *PeerCache) collectHotCandidates(peers []string, self string) ([]hotIndexEntry, int) {
	sem := make(chan struct{}, pc.readAheadMaxConcurrency)
	var wg sync.WaitGroup
	resCh := make(chan []hotIndexEntry, len(peers))
	var runErrs atomic.Int64

	for _, peer := range peers {
		if peer == "" || peer == self {
			continue
		}
		if !pc.peerAllowed(peer) {
			continue
		}
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			entries, err := pc.fetchPeerHotIndex(peerAddr, pc.readAheadTopN)
			if err != nil {
				pc.RAHotErr.Add(1)
				runErrs.Add(1)
				return
			}
			if len(entries) > 0 {
				resCh <- entries
			}
		}(peer)
	}
	wg.Wait()
	close(resCh)

	all := make([]hotIndexEntry, 0)
	for entries := range resCh {
		all = append(all, entries...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Score == all[j].Score {
			return all[i].RemainingTTLMS > all[j].RemainingTTLMS
		}
		return all[i].Score > all[j].Score
	})
	return all, int(runErrs.Load())
}

func (pc *PeerCache) selectReadAheadCandidates(candidates []hotIndexEntry) []hotIndexEntry {
	if len(candidates) == 0 || pc.readAheadMaxKeysPerTick <= 0 {
		return nil
	}
	remainingKeys := pc.readAheadMaxKeysPerTick
	remainingBytes := pc.readAheadMaxBytesPerTick
	perTenantCap := int64(remainingKeys * pc.readAheadTenantFairShare / 100)
	if perTenantCap < 1 {
		perTenantCap = 1
	}

	selected := make([]hotIndexEntry, 0, remainingKeys)
	seen := make(map[string]struct{}, remainingKeys*2)
	perTenant := make(map[string]int64, 16)
	deferred := make([]hotIndexEntry, 0)

	tryAdd := func(e hotIndexEntry, enforceFair bool) bool {
		if e.Key == "" {
			return false
		}
		if _, ok := seen[e.Key]; ok {
			return false
		}
		if e.RemainingTTLMS < pc.readAheadMinTTL.Milliseconds() {
			return false
		}
		if pc.readAheadMaxObjectBytes > 0 && e.SizeBytes > pc.readAheadMaxObjectBytes {
			pc.RABudget.Add(1)
			return false
		}
		if e.SizeBytes <= 0 || int64(e.SizeBytes) > remainingBytes {
			pc.RABudget.Add(1)
			return false
		}
		tenant := e.Tenant
		if enforceFair && tenant != "" && perTenant[tenant] >= perTenantCap {
			pc.RATenant.Add(1)
			return false
		}
		seen[e.Key] = struct{}{}
		if tenant != "" {
			perTenant[tenant]++
		}
		remainingKeys--
		remainingBytes -= int64(e.SizeBytes)
		selected = append(selected, e)
		return true
	}

	for _, e := range candidates {
		if remainingKeys <= 0 || remainingBytes <= 0 {
			break
		}
		if !tryAdd(e, true) {
			deferred = append(deferred, e)
		}
	}
	for _, e := range deferred {
		if remainingKeys <= 0 || remainingBytes <= 0 {
			break
		}
		_ = tryAdd(e, false)
	}
	return selected
}

func (pc *PeerCache) prefetchHotKey(local *Cache, e hotIndexEntry) bool {
	if local == nil || e.Key == "" {
		return false
	}
	if _, ttl, ok := local.GetWithTTL(e.Key); ok && ttl >= pc.readAheadMinTTL {
		return true
	}
	val, ttl, ok := pc.Get(e.Key)
	if !ok {
		return false
	}
	if ttl <= 0 {
		ttl = time.Duration(e.RemainingTTLMS) * time.Millisecond
	}
	if ttl < pc.readAheadMinTTL {
		return false
	}
	if pc.readAheadMaxObjectBytes > 0 && len(val) > pc.readAheadMaxObjectBytes {
		pc.RABudget.Add(1)
		return false
	}
	local.SetShadowWithTTL(e.Key, val, ttl)
	pc.RAPrefetch.Add(1)
	pc.RABytes.Add(int64(len(val)))
	return true
}

func (pc *PeerCache) fetchPeerHotIndex(peerAddr string, limit int) ([]hotIndexEntry, error) {
	pc.RAHotReq.Add(1)
	if limit <= 0 {
		limit = pc.readAheadTopN
	}
	url := fmt.Sprintf("http://%s/_cache/hot?limit=%d", peerAddr, limit)
	ctx, cancel := context.WithTimeout(context.Background(), pc.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		pc.recordPeerFailure(peerAddr)
		return nil, err
	}
	if pc.authToken != "" {
		req.Header.Set("X-Peer-Token", pc.authToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := pc.client.Do(req)
	if err != nil {
		if !pc.peerNotUpYet(peerAddr, err) {
			pc.recordPeerFailure(peerAddr)
		}
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		pc.recordPeerFailure(peerAddr)
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	pc.recordPeerSetEncoding(peerAddr, resp.Header.Get("X-Peer-Set-Encoding"))
	body, err := readPeerBodyLimited(resp.Body, maxPeerHotIndexBytes)
	if err != nil {
		pc.recordPeerFailure(peerAddr)
		return nil, err
	}
	body, err = decodePeerResponseBody(resp.Header.Get("Content-Encoding"), body, maxPeerHotIndexBytes)
	if err != nil {
		pc.recordPeerFailure(peerAddr)
		return nil, err
	}
	var payload hotIndexResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		pc.recordPeerFailure(peerAddr)
		return nil, err
	}
	pc.recordPeerSuccess(peerAddr)
	if len(payload.Entries) == 0 {
		return nil, nil
	}
	return payload.Entries, nil
}

func (pc *PeerCache) recordPeerSetEncoding(peerAddr, encoding string) {
	if pc == nil || strings.TrimSpace(peerAddr) == "" {
		return
	}
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "zstd":
		pc.peerSetEncodings.Store(peerAddr, "zstd")
	default:
		pc.peerSetEncodings.Store(peerAddr, "")
	}
}

func (pc *PeerCache) peerPreferredSetEncoding(peerAddr string) (string, bool) {
	if pc == nil || strings.TrimSpace(peerAddr) == "" {
		return "", false
	}
	if v, ok := pc.peerSetEncodings.Load(peerAddr); ok {
		if encoding, ok := v.(string); ok && strings.TrimSpace(encoding) != "" {
			return encoding, true
		}
		return "", false
	}
	return "", false
}

// SelfAZ returns this instance's configured availability zone, or "" if not set.
func (pc *PeerCache) SelfAZ() string {
	if pc == nil {
		return ""
	}
	return pc.selfAZ
}

// PeerAZ returns the availability zone label for the given peer addr, or "" if unknown.
// AZ labels are populated from Prometheus HTTP SD response labels["az"] or labels["availability_zone"].
func (pc *PeerCache) PeerAZ(addr string) string {
	if pc == nil {
		return ""
	}
	if v, ok := pc.peerAZs.Load(addr); ok {
		if az, ok := v.(string); ok {
			return az
		}
	}
	return ""
}

// SetPeerAZ records the availability zone for the given peer address.
// Useful for tests and for static/manual AZ assignment outside of HTTP SD discovery.
func (pc *PeerCache) SetPeerAZ(addr, az string) {
	if pc == nil || addr == "" {
		return
	}
	if az != "" {
		pc.peerAZs.Store(addr, az)
	} else {
		pc.peerAZs.Delete(addr)
	}
}

// PeerCount returns the number of active peers (excluding self).
func (pc *PeerCache) PeerCount() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	count := 0
	for _, p := range pc.peers {
		if p != pc.selfAddr {
			count++
		}
	}
	return count
}

func readPeerBodyLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("peer response exceeded limit of %d bytes", limit)
	}
	return body, nil
}

// Peers returns the current peer list.
func (pc *PeerCache) Peers() []string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	result := make([]string, len(pc.peers))
	copy(result, pc.peers)
	return result
}

// SelfAddr returns this instance's configured address (ip:port). Static peer
// lists usually include self (so every node ships the same config), so callers
// fanning out to Peers() can skip the self entry to avoid a redundant round-trip.
func (pc *PeerCache) SelfAddr() string {
	return pc.selfAddr
}

// HasPeerHost reports whether host (without port) is a known peer.
func (pc *PeerCache) HasPeerHost(host string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	_, ok := pc.peerHosts[host]
	return ok
}

// WriteThroughEnabled reports whether owner write-through is active.
func (pc *PeerCache) WriteThroughEnabled() bool {
	return pc != nil && pc.writeThrough
}

// RequestTimeout reports the timeout used for peer-cache HTTP fetches.
func (pc *PeerCache) RequestTimeout() time.Duration {
	if pc == nil || pc.client == nil {
		return 0
	}
	return pc.client.Timeout
}

// Close stops the discovery loop.
func (pc *PeerCache) Close() {
	select {
	case <-pc.done:
	default:
		close(pc.done)
	}
}

// Stats returns peer cache statistics.
func (pc *PeerCache) Stats() map[string]interface{} {
	return map[string]interface{}{
		"peers":              pc.PeerCount(),
		"peer_hits":          pc.PeerHits.Load(),
		"peer_misses":        pc.PeerMisses.Load(),
		"peer_errors":        pc.PeerErrors.Load(),
		"peer_error_reasons": pc.PeerErrorReasons(),
		"wt_pushes":          pc.WTPushes.Load(),
		"wt_errors":          pc.WTErrors.Load(),
		"backup_hits":        pc.BackupHits.Load(),
		"push_too_large":     pc.PushTooLarge.Load(),
		"peer_not_up":        pc.PeerNotUp.Load(),
		"push_peer_not_up":   pc.PushPeerNotUp.Load(),
		"push_throttled":     pc.PushThrottled.Load(),
		"replication_factor": pc.replication,
		"ra_hot_requests":    pc.RAHotReq.Load(),
		"ra_hot_errors":      pc.RAHotErr.Load(),
		"ra_prefetches":      pc.RAPrefetch.Load(),
		"ra_prefetch_bytes":  pc.RABytes.Load(),
		"ra_budget_drops":    pc.RABudget.Load(),
		"ra_tenant_skips":    pc.RATenant.Load(),
	}
}

// RecordPeerErrorReason increments a low-cardinality peer fetch failure reason.
func (pc *PeerCache) RecordPeerErrorReason(reason string) {
	reason = strings.TrimSpace(reason)
	if pc == nil || reason == "" {
		return
	}
	counter, _ := pc.peerErrorReasons.LoadOrStore(reason, &atomic.Int64{})
	counter.(*atomic.Int64).Add(1)
}

// PeerErrorReasons returns a stable snapshot of peer fetch failure reasons.
func (pc *PeerCache) PeerErrorReasons() map[string]int64 {
	if pc == nil {
		return nil
	}
	out := map[string]int64{}
	pc.peerErrorReasons.Range(func(key, value any) bool {
		reason, ok := key.(string)
		if !ok || reason == "" {
			return true
		}
		counter, ok := value.(*atomic.Int64)
		if !ok {
			return true
		}
		out[reason] = counter.Load()
		return true
	})
	return out
}

// MetricsJSON returns stats as JSON bytes.
func (pc *PeerCache) MetricsJSON() []byte {
	b, _ := json.Marshal(pc.Stats())
	return b
}

// --- Internal ---

func (pc *PeerCache) updatePeers(peers []string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.peers = peers
	pc.peerHosts = make(map[string]struct{}, len(peers))
	for _, p := range peers {
		if host, _, err := net.SplitHostPort(p); err == nil {
			pc.peerHosts[host] = struct{}{}
		} else {
			pc.peerHosts[p] = struct{}{}
		}
	}
	pc.ring = newHashRing(150)
	for _, p := range peers {
		pc.ring.add(p)
	}
	pc.log.Info("peer cache updated", "peers", len(peers), "self", pc.selfAddr)
}

func classifyPeerFetchError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "transport"
}

func peerStatusReason(statusCode int) string {
	return "status_" + strconv.Itoa(statusCode)
}

func (pc *PeerCache) discoveryLoop() {
	ticker := time.NewTicker(pc.discoveryInt)
	defer ticker.Stop()
	for {
		select {
		case <-pc.done:
			return
		case <-ticker.C:
		}
		if pc.discoveryFn == nil {
			continue
		}
		peers, err := pc.discoveryFn()
		if err != nil {
			pc.log.Warn("peer discovery failed", "error", err)
			continue
		}
		pc.updatePeers(peers)
	}
}

// servePeers returns the current known peer list as JSON (diagnostic endpoint).
// GET /_cache/peers → {"peers":["host:port",...],"self":"host:port"}
func (pc *PeerCache) servePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pc.mu.RLock()
	peers := make([]string, len(pc.peers))
	copy(peers, pc.peers)
	pc.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"peers": peers,
		"self":  pc.selfAddr,
		"count": len(peers),
	})
}

// --- DNS Discovery ---

func discoverDNS(name string, port int) ([]string, error) {
	ips, err := lookupHost(name)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup %q: %w", name, err)
	}
	peers := make([]string, 0, len(ips))
	for _, ip := range ips {
		peers = append(peers, fmt.Sprintf("%s:%d", ip, port))
	}
	sort.Strings(peers)
	return peers, nil
}

// --- SRV Discovery ---

// discoverSRV resolves peers from a DNS SRV record.
// name must be a full SRV record name: "_service._proto.domain"
// (e.g. "_loki-vl-proxy._tcp.proxy-headless.default.svc.cluster.local").
// SRV records embed port numbers so no separate port flag is needed.
// In Kubernetes, StatefulSet headless services expose SRV records per pod,
// including only ready pods — giving the same readiness-gate behaviour as A-record headless DNS.
func discoverSRV(name string) ([]string, error) {
	if !strings.HasPrefix(name, "_") {
		return nil, fmt.Errorf("SRV name %q must start with underscore (e.g. _service._tcp.domain)", name)
	}
	// Split "_service._proto.domain" into parts.
	parts := strings.SplitN(strings.TrimPrefix(name, "_"), ".", 2)
	if len(parts) < 2 || !strings.HasPrefix(parts[1], "_") {
		return nil, fmt.Errorf("invalid SRV name %q: expected _service._proto.domain", name)
	}
	rest := strings.TrimPrefix(parts[1], "_")
	protoParts := strings.SplitN(rest, ".", 2)
	if len(protoParts) < 2 {
		return nil, fmt.Errorf("invalid SRV name %q: expected _service._proto.domain", name)
	}
	service := parts[0]
	proto := protoParts[0]
	domain := protoParts[1]

	_, addrs, err := lookupSRV(service, proto, domain)
	if err != nil {
		return nil, fmt.Errorf("SRV lookup %q: %w", name, err)
	}
	peers := make([]string, 0, len(addrs))
	for _, srv := range addrs {
		host := strings.TrimSuffix(srv.Target, ".")
		peers = append(peers, fmt.Sprintf("%s:%d", host, srv.Port))
	}
	sort.Strings(peers)
	return peers, nil
}

// --- HTTP Discovery ---

// discoverHTTPPeers fetches a JSON peer list from an HTTP endpoint.
// Supported response formats (auto-detected):
//
//   - Simple array:          ["host:port", ...]
//   - Object with peers key: {"peers": ["host:port", ...]}
//   - Prometheus targets:    [{"targets": ["host:port"]}]  (Prometheus HTTP SD format)
//   - Consul catalog:        [{"ServiceAddress":"ip", "ServicePort":8080}]
//
// This makes the proxy compatible with Consul, Nomad, custom service registries,
// and any Prometheus-compatible service discovery endpoint.
func discoverHTTPPeers(rawURL string, timeout time.Duration) ([]string, error) {
	peers, _, err := discoverHTTPPeersWithAZ(rawURL, timeout)
	return peers, err
}

// discoverHTTPPeersWithAZ is like discoverHTTPPeers but also returns a peer→AZ map.
// The AZ map is populated when the response uses Prometheus HTTP SD format and includes
// an "az" or "availability_zone" label on each target group.
func discoverHTTPPeersWithAZ(rawURL string, timeout time.Duration) ([]string, map[string]string, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build HTTP peers request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("HTTP peers fetch %q: %w", rawURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("HTTP peers fetch %q: status %d", rawURL, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("HTTP peers read body: %w", err)
	}
	return parseHTTPPeersWithAZ(body)
}

// parseHTTPPeers decodes a JSON body into a list of "host:port" peer addresses.
// It attempts each known format in order and returns on first success.
func parseHTTPPeers(body []byte) ([]string, error) {
	peers, _, err := parseHTTPPeersWithAZ(body)
	return peers, err
}

// parseHTTPPeersWithAZ decodes a JSON body into a peer list and an optional peer→AZ map.
// The AZ map is populated from Prometheus HTTP SD labels["az"] or labels["availability_zone"].
func parseHTTPPeersWithAZ(body []byte) ([]string, map[string]string, error) {
	body = bytes.TrimSpace(body)

	// Format 1: simple string array ["host:port", ...]
	var simple []string
	if err := json.Unmarshal(body, &simple); err == nil {
		return filterValidPeers(simple), nil, nil
	}

	// Format 2: {"peers": ["host:port", ...]}
	var withPeers struct {
		Peers []string `json:"peers"`
	}
	if json.Unmarshal(body, &withPeers) == nil && len(withPeers.Peers) > 0 {
		return filterValidPeers(withPeers.Peers), nil, nil
	}

	// Format 3: Prometheus HTTP SD — [{"targets":["host:port"],"labels":{...}}]
	// Labels may include "az" or "availability_zone" for AZ-aware peer selection.
	var promSD []struct {
		Targets []string          `json:"targets"`
		Labels  map[string]string `json:"labels"`
	}
	if json.Unmarshal(body, &promSD) == nil && len(promSD) > 0 {
		var peers []string
		azMap := make(map[string]string)
		for _, g := range promSD {
			az := g.Labels["az"]
			if az == "" {
				az = g.Labels["availability_zone"]
			}
			for _, t := range g.Targets {
				peers = append(peers, t)
				if az != "" {
					azMap[t] = az
				}
			}
		}
		if len(peers) > 0 {
			if len(azMap) == 0 {
				azMap = nil
			}
			return filterValidPeers(peers), azMap, nil
		}
	}

	// Format 4: Consul catalog — [{"ServiceAddress":"ip","ServicePort":8080}]
	var consul []struct {
		ServiceAddress string `json:"ServiceAddress"`
		Address        string `json:"Address"`
		ServicePort    int    `json:"ServicePort"`
	}
	if json.Unmarshal(body, &consul) == nil && len(consul) > 0 {
		var peers []string
		for _, svc := range consul {
			host := svc.ServiceAddress
			if host == "" {
				host = svc.Address
			}
			if host != "" && svc.ServicePort > 0 {
				peers = append(peers, fmt.Sprintf("%s:%d", host, svc.ServicePort))
			}
		}
		if len(peers) > 0 {
			return filterValidPeers(peers), nil, nil
		}
	}

	return nil, nil, fmt.Errorf("unrecognised HTTP peer list format (body: %.120s)", body)
}

func filterValidPeers(peers []string) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p != "" && strings.Contains(p, ":") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func parsePeerList(s string) []string {
	var peers []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			peers = append(peers, p)
		}
	}
	return peers
}

// --- Consistent Hash Ring ---

type hashRing struct {
	vnodes  int
	ring    []uint32
	nodeMap map[uint32]string
}

func newHashRing(vnodes int) *hashRing {
	return &hashRing{
		vnodes:  vnodes,
		nodeMap: make(map[uint32]string),
	}
}

func (hr *hashRing) add(addr string) {
	for i := 0; i < hr.vnodes; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", addr, i))
		hr.ring = append(hr.ring, h)
		hr.nodeMap[h] = addr
	}
	sort.Slice(hr.ring, func(i, j int) bool { return hr.ring[i] < hr.ring[j] })
}

func (hr *hashRing) get(key string) string {
	if len(hr.ring) == 0 {
		return ""
	}
	h := hashKey(key)
	idx := sort.Search(len(hr.ring), func(i int) bool { return hr.ring[i] >= h })
	if idx == len(hr.ring) {
		idx = 0
	}
	return hr.nodeMap[hr.ring[idx]]
}

// getN returns up to n distinct nodes for key, walking the ring clockwise
// from the key's owner.
func (hr *hashRing) getN(key string, n int) []string {
	if len(hr.ring) == 0 || n <= 0 {
		return nil
	}
	// walk never returns more than MaxReplicationFactor distinct peers. Use a
	// constant capacity even when a caller supplies a huge replica count.
	out := make([]string, 0, MaxReplicationFactor)
	hr.walk(key, n, func(addr string) bool {
		out = append(out, addr)
		return true
	})
	return out
}

// has reports whether addr is one of the first n distinct nodes for key. It
// does not allocate.
func (hr *hashRing) has(key string, n int, addr string) bool {
	if len(hr.ring) == 0 || n <= 0 {
		return false
	}
	h := hashKey(key)
	idx := sort.Search(len(hr.ring), func(i int) bool { return hr.ring[i] >= h })
	if idx == len(hr.ring) {
		idx = 0
	}
	first := hr.nodeMap[hr.ring[idx]]
	if first == addr {
		return true
	}
	if n == 1 {
		return false
	}
	var seen [MaxReplicationFactor]string
	seen[0] = first
	count := 1
	for i := 1; i < len(hr.ring) && count < n && count < len(seen); i++ {
		node := hr.nodeMap[hr.ring[(idx+i)%len(hr.ring)]]
		dup := false
		for _, s := range seen[:count] {
			if s == node {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		if node == addr {
			return true
		}
		seen[count] = node
		count++
	}
	return false
}

// walk calls visit with up to n distinct nodes for key in ring order until
// visit returns false. It does not allocate for n <= MaxReplicationFactor.
func (hr *hashRing) walk(key string, n int, visit func(string) bool) {
	if len(hr.ring) == 0 || n <= 0 {
		return
	}
	var seen [MaxReplicationFactor]string
	count := 0
	h := hashKey(key)
	idx := sort.Search(len(hr.ring), func(i int) bool { return hr.ring[i] >= h })
	for i := 0; i < len(hr.ring) && count < n && count < len(seen); i++ {
		addr := hr.nodeMap[hr.ring[(idx+i)%len(hr.ring)]]
		dup := false
		for _, s := range seen[:count] {
			if s == addr {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		seen[count] = addr
		count++
		if !visit(addr) {
			return
		}
	}
}

func hashKey(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// --- Per-Peer Circuit Breaker ---

type peerBreaker struct {
	failures  atomic.Int64
	lastFail  atomic.Int64
	threshold int64
	cooldown  time.Duration

	// A peer that timed out is suspended for the cooldown at once (a
	// paused or hung peer would otherwise cost every lookup of a request a
	// full timeout, five times over before the failure count opens the
	// breaker), and every call still waiting on it is cancelled. After the
	// cooldown one call probes it; its success lifts the suspension.
	suspendedUntil atomic.Int64 // Unix ns; 0 when not suspended
	// A peer that never answered since this process started and refuses
	// connections is still starting (replicas of a fleet start within
	// milliseconds of each other): it is skipped for peerStartupRetry and
	// tried again, without counting towards the breaker.
	seenUp     atomic.Bool
	notUpUntil atomic.Int64 // Unix ns
	// Pushes made while the peer was still starting wait here (bounded)
	// and are sent once it answers; one retrier per peer runs meanwhile.
	pending      []pendingPush
	pendingBytes int64
	retrying     bool
	probeAt      atomic.Int64 // Unix ns the running probe started; 0 when none
	mu           sync.Mutex
	ctx          context.Context // cancelled when the peer is suspended
	cancel       context.CancelFunc
}

func (pc *PeerCache) breaker(addr string) *peerBreaker {
	if v, ok := pc.breakers.Load(addr); ok {
		return v.(*peerBreaker)
	}
	v, _ := pc.breakers.LoadOrStore(addr, &peerBreaker{
		threshold: 5,
		cooldown:  10 * time.Second,
	})
	return v.(*peerBreaker)
}

func (pc *PeerCache) peerAllowed(addr string) bool {
	pb := pc.breaker(addr)
	if !pb.seenUp.Load() && time.Now().UnixNano() < pb.notUpUntil.Load() {
		return false
	}
	if until := pb.suspendedUntil.Load(); until != 0 {
		if time.Now().UnixNano() < until {
			return false
		}
		// Half open: one call at a time probes a suspended peer (a probe
		// that never reported back is replaced after the cooldown).
		now := time.Now().UnixNano()
		probe := pb.probeAt.Load()
		if probe != 0 && now-probe < int64(pb.cooldown) {
			return false
		}
		if !pb.probeAt.CompareAndSwap(probe, now) {
			return false
		}
	}
	if pb.failures.Load() >= pb.threshold {
		if time.Since(time.UnixMilli(pb.lastFail.Load())) < pb.cooldown {
			return false
		}
		pb.failures.Store(0)
	}
	return true
}

// peerContext returns the context calls to addr run under: it is cancelled
// when the peer is suspended, so they stop waiting on it.
func (pc *PeerCache) peerContext(addr string) context.Context {
	pb := pc.breaker(addr)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.ctx == nil || pb.ctx.Err() != nil {
		pb.ctx, pb.cancel = context.WithCancel(context.Background())
	}
	return pb.ctx
}

// recordPeerCallError records a failed call to addr: a timeout suspends the
// peer for the cooldown and cancels the calls still waiting on it; a call
// cancelled by that suspension is not a failure of its own; anything else
// counts towards the breaker's failure threshold as before.
// It reports whether the peer was found still starting (peerNotUpYet).
func (pc *PeerCache) recordPeerCallError(addr string, callCtx context.Context, err error) (notUp bool) {
	pb := pc.breaker(addr)
	if errors.Is(callCtx.Err(), context.Canceled) {
		return false // the peer was suspended meanwhile
	}
	if pc.peerNotUpYet(addr, err) {
		return true
	}
	if classifyPeerFetchError(err) != "timeout" {
		pc.recordPeerFailure(addr)
		return false
	}
	// A timeout suspends the peer instead of counting towards the failure
	// threshold, so the half-open probe after the cooldown reaches it.
	pb.probeAt.Store(0)
	pb.suspendedUntil.Store(time.Now().Add(pb.cooldown).UnixNano())
	pb.mu.Lock()
	if pb.cancel != nil {
		pb.cancel()
	}
	pb.mu.Unlock()
	return false
}

// peerStartupRetry is how long a peer that refused a connection before it
// ever answered is skipped before it is tried again.
const peerStartupRetry = 250 * time.Millisecond

// Pushes to a peer that is still starting are kept, at most
// pendingPushMaxEntries and pendingPushMaxBytes per peer (the oldest are
// dropped first), and sent once it answers; after pendingPushMaxWait without
// an answer they are dropped. Every drop counts in PushPeerNotUp.
const (
	pendingPushMaxEntries = 256
	pendingPushMaxBytes   = 8 << 20
	pendingPushMaxWait    = 30 * time.Second
)

type pendingPush struct {
	key   string
	value []byte
	ttl   time.Duration
}

// deferPush keeps a push to a peer that is still starting and starts its
// retrier.
func (pc *PeerCache) deferPush(addr, key string, value []byte, ttl time.Duration) {
	pb := pc.breaker(addr)
	pb.mu.Lock()
	pb.pending = append(pb.pending, pendingPush{key: key, value: value, ttl: ttl})
	pb.pendingBytes += int64(len(value))
	for len(pb.pending) > pendingPushMaxEntries || pb.pendingBytes > pendingPushMaxBytes {
		pb.pendingBytes -= int64(len(pb.pending[0].value))
		pb.pending = pb.pending[1:]
		pc.PushPeerNotUp.Add(1)
	}
	start := !pb.retrying
	pb.retrying = true
	pb.mu.Unlock()
	if start {
		go pc.retryPending(addr, pb)
	}
}

// retryPending sends the pushes kept for a starting peer once it answers:
// the oldest is tried every peerStartupRetry; when it lands the rest follow.
func (pc *PeerCache) retryPending(addr string, pb *peerBreaker) {
	deadline := time.Now().Add(pendingPushMaxWait)
	ticker := time.NewTicker(peerStartupRetry)
	defer ticker.Stop()
	for {
		select {
		case <-pc.done:
			pc.dropPending(pb)
			return
		case <-ticker.C:
		}
		pb.mu.Lock()
		if len(pb.pending) == 0 {
			pb.retrying = false
			pb.mu.Unlock()
			return
		}
		next := pb.pending[0]
		pb.mu.Unlock()
		if pc.push(addr, next.key, next.value, next.ttl, false) {
			pb.mu.Lock()
			rest := pb.pending
			pb.pending, pb.pendingBytes = nil, 0
			pb.mu.Unlock()
			for _, pending := range rest[1:] {
				pc.push(addr, pending.key, pending.value, pending.ttl, false)
			}
			continue // pushes deferred meanwhile are sent on the next tick
		}
		if time.Now().After(deadline) {
			pc.dropPending(pb)
			return
		}
	}
}

func (pc *PeerCache) dropPending(pb *peerBreaker) {
	pb.mu.Lock()
	pc.PushPeerNotUp.Add(int64(len(pb.pending)))
	pb.pending, pb.pendingBytes, pb.retrying = nil, 0, false
	pb.mu.Unlock()
}

// peerNotUpYet handles a refused connection to a peer that never answered
// since this process started: it is starting, not failing, so it is skipped
// for peerStartupRetry instead of counting towards the breaker (five
// refusals within a millisecond would otherwise open it for the cooldown).
// A peer that answered once and then refuses counts as before.
func (pc *PeerCache) peerNotUpYet(addr string, err error) bool {
	pb := pc.breaker(addr)
	if pb.seenUp.Load() || !errors.Is(err, syscall.ECONNREFUSED) {
		return false
	}
	pb.notUpUntil.Store(time.Now().Add(peerStartupRetry).UnixNano())
	pc.PeerNotUp.Add(1)
	return true
}

func (pc *PeerCache) recordPeerFailure(addr string) {
	pb := pc.breaker(addr)
	pb.failures.Add(1)
	pb.lastFail.Store(time.Now().UnixMilli())
	pb.probeAt.Store(0) // a failed probe leaves the peer suspended; the next call probes again
}

func (pc *PeerCache) recordPeerSuccess(addr string) {
	pb := pc.breaker(addr)
	pb.failures.Store(0)
	if !pb.seenUp.Load() {
		pb.seenUp.Store(true)
	}
	if pb.suspendedUntil.Load() != 0 {
		pb.suspendedUntil.Store(0)
	}
	pb.probeAt.Store(0)
}

// peerFetchBuckets are the upper bounds of the peer fetch duration
// histogram, in seconds.
var peerFetchBuckets = [...]float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// durationHistogram is a lock-free cumulative histogram over
// peerFetchBuckets.
type durationHistogram struct {
	buckets [len(peerFetchBuckets)]atomic.Int64
	count   atomic.Int64
	sumNs   atomic.Int64
}

func (h *durationHistogram) observe(d time.Duration) {
	sec := d.Seconds()
	for i, le := range peerFetchBuckets {
		if sec <= le {
			h.buckets[i].Add(1)
		}
	}
	h.count.Add(1)
	h.sumNs.Add(int64(d))
}

// FetchDurationText renders the replica fetch duration histogram in the
// Prometheus text format: loki_vl_proxy_peer_cache_fetch_duration_seconds
// by replica (owner, backup) and outcome (hit, miss, error).
func (pc *PeerCache) FetchDurationText() string {
	if pc == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("# HELP loki_vl_proxy_peer_cache_fetch_duration_seconds Duration of peer-cache fetches by the replica asked (owner, backup) and outcome (hit, miss, error).\n")
	b.WriteString("# TYPE loki_vl_proxy_peer_cache_fetch_duration_seconds histogram\n")
	for r, role := range []string{"owner", "backup"} {
		for o, outcome := range []string{"hit", "miss", "error"} {
			h := &pc.fetchDurations[r][o]
			for i, le := range peerFetchBuckets {
				fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_fetch_duration_seconds_bucket{replica=%q,outcome=%q,le=%q} %d\n", role, outcome, strconv.FormatFloat(le, 'g', -1, 64), h.buckets[i].Load())
			}
			count := h.count.Load()
			fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_fetch_duration_seconds_bucket{replica=%q,outcome=%q,le=\"+Inf\"} %d\n", role, outcome, count)
			fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_fetch_duration_seconds_sum{replica=%q,outcome=%q} %g\n", role, outcome, time.Duration(h.sumNs.Load()).Seconds())
			fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_fetch_duration_seconds_count{replica=%q,outcome=%q} %d\n", role, outcome, count)
		}
	}
	return b.String()
}
