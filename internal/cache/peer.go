package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

var lookupHost = net.LookupHost

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

	// Per-peer circuit breakers
	breakers sync.Map // string → *peerBreaker

	// Singleflight to prevent cache stampede
	inflight sync.Map // key → *inflightEntry

	// Stats
	PeerHits   atomic.Int64
	PeerMisses atomic.Int64
	PeerErrors atomic.Int64
}

type inflightEntry struct {
	done   chan struct{}
	result []byte
	ttl    time.Duration
	ok     bool
}

// PeerConfig configures the distributed peer cache.
type PeerConfig struct {
	SelfAddr          string        // this instance's address (ip:port)
	DiscoveryType     string        // "dns", "static", or "" (disabled)
	DNSName           string        // headless service DNS name
	StaticPeers       string        // comma-separated peer addresses
	Port              int           // peer cache HTTP port (default: 3100)
	DiscoveryInterval time.Duration // peer list refresh interval (default: 15s)
	Timeout           time.Duration // peer HTTP request timeout (default: 2s)
	Logger            *slog.Logger
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
		log:          cfg.Logger,
		done:         make(chan struct{}),
		discoveryInt: cfg.DiscoveryInterval,
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
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := pc.client.Do(req)
	if err != nil {
		pc.recordPeerFailure(owner)
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
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		pc.recordPeerFailure(owner)
		pc.PeerErrors.Add(1)
		return nil, 0, false
	}
	body, err = decodePeerResponseBody(resp.Header.Get("Content-Encoding"), body)
	if err != nil {
		pc.recordPeerFailure(owner)
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

// Set is a no-op for the sharded model.
// L1 Set already stores locally. The owner's L1 gets populated when
// it receives a /_cache/get request and fetches from VL.
// No write-through needed — saves network.
func (pc *PeerCache) Set(key string, value []byte) {
	// Intentionally empty: sharded model doesn't push data to peers.
	// Owner populates its own cache via VL fetch.
	// Non-owners populate via L3 Get → shadow copy in L1.
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
	if len(value) >= 256 && acceptsPeerEncoding(r.Header.Get("Accept-Encoding"), "zstd") {
		w.Header().Set("Content-Encoding", "zstd")
		zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			http.Error(w, "compression init error", http.StatusInternalServerError)
			return
		}
		if _, err := zw.Write(value); err != nil {
			_ = zw.Close()
			http.Error(w, "compression write error", http.StatusInternalServerError)
			return
		}
		if err := zw.Close(); err != nil {
			http.Error(w, "compression close error", http.StatusInternalServerError)
		}
		return
	}
	if len(value) >= 256 && acceptsPeerEncoding(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		zw, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			http.Error(w, "compression init error", http.StatusInternalServerError)
			return
		}
		if _, err := zw.Write(value); err != nil {
			_ = zw.Close()
			http.Error(w, "compression write error", http.StatusInternalServerError)
			return
		}
		_ = zw.Close()
		return
	}
	w.Write(value)
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

func decodePeerResponseBody(contentEncoding string, body []byte) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
		return body, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer func() { _ = zr.Close() }()
		return io.ReadAll(zr)
	case "zstd":
		zr, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return zr.DecodeAll(body, nil)
	default:
		return body, nil
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

// Peers returns the current peer list.
func (pc *PeerCache) Peers() []string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	result := make([]string, len(pc.peers))
	copy(result, pc.peers)
	return result
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
		"peers":       pc.PeerCount(),
		"peer_hits":   pc.PeerHits.Load(),
		"peer_misses": pc.PeerMisses.Load(),
		"peer_errors": pc.PeerErrors.Load(),
	}
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
