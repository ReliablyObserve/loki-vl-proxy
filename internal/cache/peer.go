package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

var lookupHost = net.LookupHost

const (
	maxPeerResponseBytes    int64 = 32 << 20
	maxPeerDecodedBodyBytes int64 = 32 << 20
	maxPeerHotIndexBytes    int64 = 4 << 20
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
// The only network traffic is the actual cache fetch on L1 miss.
// With N replicas, each key lives on exactly 1 peer (the owner).
// Non-owners keep short-lived shadow copies after fetching.
type PeerCache struct {
	mu           sync.RWMutex
	ring         *hashRing
	selfAddr     string
	peers        []string
	client       *http.Client
	log          *slog.Logger
	done         chan struct{}
	discoveryFn  func() ([]string, error)
	discoveryInt time.Duration
	authToken    string
	writeThrough bool
	wtMinTTL     time.Duration
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

	// Stats
	PeerHits   atomic.Int64
	PeerMisses atomic.Int64
	PeerErrors atomic.Int64
	WTPushes   atomic.Int64
	WTErrors   atomic.Int64
	RAHotReq   atomic.Int64
	RAHotErr   atomic.Int64
	RAPrefetch atomic.Int64
	RABytes    atomic.Int64
	RABudget   atomic.Int64
	RATenant   atomic.Int64
}

type inflightEntry struct {
	done   chan struct{}
	result []byte
	ttl    time.Duration
	ok     bool
}

// PeerConfig configures the distributed peer cache.
type PeerConfig struct {
	SelfAddr                 string        // this instance's address (ip:port)
	DiscoveryType            string        // "dns", "static", or "" (disabled)
	DNSName                  string        // headless service DNS name
	StaticPeers              string        // comma-separated peer addresses
	Port                     int           // peer cache HTTP port (default: 3100)
	DiscoveryInterval        time.Duration // peer list refresh interval (default: 15s)
	Timeout                  time.Duration // peer HTTP request timeout (default: 2s)
	AuthToken                string        // optional shared token for peer cache endpoints
	WriteThrough             bool          // push owner copies on local SetWithTTL
	WriteThroughMinTTL       time.Duration // minimum TTL to trigger write-through
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

	switch cfg.DiscoveryType {
	case "dns":
		pc.discoveryFn = func() ([]string, error) {
			return discoverDNS(cfg.DNSName, cfg.Port)
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

// Get fetches a value from the owning peer.
// Returns (value, remainingTTL, true) on hit.
// The caller should use remainingTTL for the shadow copy — never extend the original.
func (pc *PeerCache) Get(key string) ([]byte, time.Duration, bool) {
	pc.mu.RLock()
	if len(pc.peers) == 0 {
		pc.mu.RUnlock()
		return nil, 0, false
	}
	owner := pc.ring.get(key)
	pc.mu.RUnlock()

	if owner == pc.selfAddr || owner == "" {
		return nil, 0, false
	}

	if !pc.peerAllowed(owner) {
		pc.RecordPeerErrorReason("breaker_open")
		pc.PeerErrors.Add(1)
		return nil, 0, false
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
		return inf.result, inf.ttl, inf.ok
	}

	inf := pc.getInflight(key)
	defer func() {
		close(inf.done)
		pc.inflight.Delete(key)
	}()

	// Fetch from owner
	url := fmt.Sprintf("http://%s/_cache/get?key=%s", owner, key)
	ctx, cancel := context.WithTimeout(context.Background(), pc.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason("request_build")
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}
	if pc.authToken != "" {
		req.Header.Set("X-Peer-Token", pc.authToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := pc.client.Do(req)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason(classifyPeerFetchError(err))
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		pc.recordPeerSuccess(owner)
		inf.ok = false
		pc.PeerMisses.Add(1)
		return nil, 0, false
	}
	if resp.StatusCode != http.StatusOK {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason(peerStatusReason(resp.StatusCode))
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}

	body, err := readPeerBodyLimited(resp.Body, maxPeerResponseBytes)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason("body_read")
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}
	body, err = decodePeerResponseBody(resp.Header.Get("Content-Encoding"), body, maxPeerDecodedBodyBytes)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.RecordPeerErrorReason("decode")
		pc.PeerErrors.Add(1)
		return nil, 0, false
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
	inf.result = body
	inf.ttl = remainingTTL
	inf.ok = true
	pc.PeerHits.Add(1)
	return body, remainingTTL, true
}

// Set optionally pushes the key to owner based on write-through config.
// Default behavior remains no-op for network writes.
func (pc *PeerCache) Set(key string, value []byte) {
	pc.SetWithTTL(key, value, 0)
}

// SetWithTTL optionally pushes the key to its owner peer so the ring stays warm
// even when client traffic is temporarily skewed to a subset of replicas.
func (pc *PeerCache) SetWithTTL(key string, value []byte, ttl time.Duration) {
	if !pc.writeThrough || len(value) == 0 {
		return
	}
	if ttl > 0 && ttl < pc.wtMinTTL {
		return
	}
	pc.mu.RLock()
	if len(pc.peers) == 0 {
		pc.mu.RUnlock()
		return
	}
	owner := pc.ring.get(key)
	pc.mu.RUnlock()
	if owner == "" || owner == pc.selfAddr {
		return
	}
	if !pc.peerAllowed(owner) {
		pc.WTErrors.Add(1)
		return
	}
	go pc.pushToOwner(owner, key, value, ttl)
}

func (pc *PeerCache) pushToOwner(owner, key string, value []byte, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), pc.client.Timeout)
	defer cancel()
	ttlMs := int64(0)
	if ttl > 0 {
		ttlMs = ttl.Milliseconds()
	}
	endpoint := fmt.Sprintf("http://%s/_cache/set?key=%s&ttl_ms=%d", owner, key, ttlMs)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(value))
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.WTErrors.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if pc.authToken != "" {
		req.Header.Set("X-Peer-Token", pc.authToken)
	}
	resp, err := pc.client.Do(req)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.WTErrors.Add(1)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		pc.recordPeerFailure(owner)
		pc.WTErrors.Add(1)
		return
	}
	pc.recordPeerSuccess(owner)
	pc.WTPushes.Add(1)
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
// GET /_cache/get?key=... — return cached value with remaining TTL header (or 404)
// Header X-Cache-TTL-Ms: remaining TTL in milliseconds (for shadow copy)
func (pc *PeerCache) ServeHTTP(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	switch r.URL.Path {
	case "/_cache/hot":
		pc.serveHotIndex(w, r, localCache)
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

func (pc *PeerCache) serveGet(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	value, remaining, ok := localCache.GetWithTTL(key)
	if !ok || remaining < MinUsableTTL {
		// Expired or about to expire — treat as miss (force refresh from VL)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Cache-TTL-Ms", fmt.Sprintf("%d", remaining.Milliseconds()))
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty value", http.StatusBadRequest)
		return
	}
	localCache.SetWithTTL(key, body, ttl)
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
	writePeerEncodedResponse(w, r, body)
}

func writePeerEncodedResponse(w http.ResponseWriter, r *http.Request, body []byte) {
	const peerCompressionMinBytes = 1024
	if len(body) >= peerCompressionMinBytes && acceptsPeerEncoding(r.Header.Get("Accept-Encoding"), "zstd") {
		w.Header().Set("Content-Encoding", "zstd")
		zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			http.Error(w, "compression init error", http.StatusInternalServerError)
			return
		}
		if _, err := zw.Write(body); err != nil {
			_ = zw.Close()
			http.Error(w, "compression write error", http.StatusInternalServerError)
			return
		}
		if err := zw.Close(); err != nil {
			http.Error(w, "compression close error", http.StatusInternalServerError)
		}
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

func acceptsPeerEncoding(header, encoding string) bool {
	for _, part := range strings.Split(strings.ToLower(header), ",") {
		token := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if token == encoding || token == "*" {
			return true
		}
	}
	return false
}

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
		zr, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return readPeerBodyLimited(zr, limit)
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
		pc.recordPeerFailure(peerAddr)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		pc.recordPeerFailure(peerAddr)
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
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
}

func (pc *PeerCache) peerAllowed(addr string) bool {
	v, _ := pc.breakers.LoadOrStore(addr, &peerBreaker{
		threshold: 5,
		cooldown:  10 * time.Second,
	})
	pb := v.(*peerBreaker)
	if pb.failures.Load() >= pb.threshold {
		if time.Since(time.UnixMilli(pb.lastFail.Load())) < pb.cooldown {
			return false
		}
		pb.failures.Store(0)
	}
	return true
}

func (pc *PeerCache) recordPeerFailure(addr string) {
	v, _ := pc.breakers.LoadOrStore(addr, &peerBreaker{threshold: 5, cooldown: 10 * time.Second})
	pb := v.(*peerBreaker)
	pb.failures.Add(1)
	pb.lastFail.Store(time.Now().UnixMilli())
}

func (pc *PeerCache) recordPeerSuccess(addr string) {
	if v, ok := pc.breakers.Load(addr); ok {
		v.(*peerBreaker).failures.Store(0)
	}
}
