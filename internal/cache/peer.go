package cache

import (
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
)

// PeerCache implements a distributed cache layer (L3) between local cache and VL backend.
// Uses consistent hashing to route keys to specific peers, minimizing network chatter.
// On local miss, fetches from the owning peer before hitting VL.
type PeerCache struct {
	mu           sync.RWMutex
	ring         *hashRing
	selfAddr     string // this instance's address (e.g., "10.0.0.1:3100")
	peers        []string
	client       *http.Client
	log          *slog.Logger
	done         chan struct{}
	discoveryFn  func() ([]string, error) // returns peer addresses
	discoveryInt time.Duration

	// Per-peer circuit breakers (address → breaker)
	breakers   sync.Map // string → *peerBreaker

	// Singleflight to prevent cache stampede across peers
	inflight   sync.Map // key → *inflightEntry

	// Stats
	PeerHits   atomic.Int64
	PeerMisses atomic.Int64
	PeerErrors atomic.Int64
}

type inflightEntry struct {
	done   chan struct{}
	result []byte
	ok     bool
}

// PeerConfig configures the distributed peer cache.
type PeerConfig struct {
	// SelfAddr is this instance's address (ip:port).
	SelfAddr string

	// DiscoveryType: "dns" (headless service), "static" (comma-separated list), or "" (disabled).
	DiscoveryType string

	// DNSName is the headless service DNS name (e.g., "loki-vl-proxy-headless.default.svc.cluster.local").
	DNSName string

	// StaticPeers is a comma-separated list of peer addresses.
	StaticPeers string

	// Port is the peer cache HTTP port (default: 3100).
	Port int

	// DiscoveryInterval is how often to refresh peer list (default: 15s).
	DiscoveryInterval time.Duration

	// Timeout for peer HTTP requests (default: 2s).
	Timeout time.Duration

	Logger *slog.Logger
}

// NewPeerCache creates a distributed peer cache with the given configuration.
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
		ring:     newHashRing(150), // 150 virtual nodes per peer
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

	// Configure discovery function
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
		return pc // no discovery, peer cache disabled
	}

	// Initial discovery
	if pc.discoveryFn != nil {
		if peers, err := pc.discoveryFn(); err == nil {
			pc.updatePeers(peers)
		}
		go pc.discoveryLoop()
	}

	return pc
}

// Get fetches a value from the peer that owns this key.
// Returns (value, true) on hit, (nil, false) on miss.
// Skips self (L1/L2 already checked by caller).
func (pc *PeerCache) Get(key string) ([]byte, bool) {
	pc.mu.RLock()
	if len(pc.peers) == 0 {
		pc.mu.RUnlock()
		return nil, false
	}
	owner := pc.ring.get(key)
	pc.mu.RUnlock()

	// Don't fetch from self — L1/L2 already checked
	if owner == pc.selfAddr || owner == "" {
		return nil, false
	}

	// Check per-peer circuit breaker
	if !pc.peerAllowed(owner) {
		pc.PeerErrors.Add(1)
		return nil, false
	}

	// Singleflight: if another goroutine is already fetching this key, wait for it
	if entry, loaded := pc.inflight.LoadOrStore(key, &inflightEntry{done: make(chan struct{})}); loaded {
		inf := entry.(*inflightEntry)
		<-inf.done
		if inf.ok {
			pc.PeerHits.Add(1)
		} else {
			pc.PeerMisses.Add(1)
		}
		return inf.result, inf.ok
	}

	inf := pc.getInflight(key)
	defer func() {
		close(inf.done)
		pc.inflight.Delete(key)
	}()

	// HTTP GET to peer
	url := fmt.Sprintf("http://%s/_cache/get?key=%s", owner, key)
	ctx, cancel := context.WithTimeout(context.Background(), pc.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		pc.recordPeerFailure(owner)
		inf.ok = false
		pc.PeerErrors.Add(1)
		return nil, false
	}

	resp, err := pc.client.Do(req)
	if err != nil {
		pc.recordPeerFailure(owner)
		inf.ok = false
		pc.PeerErrors.Add(1)
		return nil, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		pc.recordPeerSuccess(owner)
		inf.ok = false
		pc.PeerMisses.Add(1)
		return nil, false
	}

	if resp.StatusCode != http.StatusOK {
		pc.recordPeerFailure(owner)
		inf.ok = false
		pc.PeerErrors.Add(1)
		return nil, false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		pc.recordPeerFailure(owner)
		inf.ok = false
		pc.PeerErrors.Add(1)
		return nil, false
	}

	pc.recordPeerSuccess(owner)
	inf.result = body
	inf.ok = true
	pc.PeerHits.Add(1)
	return body, true
}

func (pc *PeerCache) getInflight(key string) *inflightEntry {
	v, _ := pc.inflight.Load(key)
	return v.(*inflightEntry)
}

// ServeHTTP handles incoming peer cache requests.
// Mount on the proxy's mux as /_cache/get?key=...
func (pc *PeerCache) ServeHTTP(w http.ResponseWriter, r *http.Request, localCache *Cache) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	value, ok := localCache.Get(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
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

// Stats returns peer cache statistics as JSON.
func (pc *PeerCache) Stats() map[string]interface{} {
	return map[string]interface{}{
		"peers":       pc.PeerCount(),
		"peer_hits":   pc.PeerHits.Load(),
		"peer_misses": pc.PeerMisses.Load(),
		"peer_errors": pc.PeerErrors.Load(),
	}
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
	ips, err := net.LookupHost(name)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup %q: %w", name, err)
	}
	peers := make([]string, 0, len(ips))
	for _, ip := range ips {
		peers = append(peers, fmt.Sprintf("%s:%d", ip, port))
	}
	sort.Strings(peers) // deterministic ordering
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
	vnodes   int
	ring     []uint32
	nodeMap  map[uint32]string // hash → address
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
		idx = 0 // wrap around
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
	lastFail  atomic.Int64 // unix millis
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
		// Check cooldown
		lastFail := time.UnixMilli(pb.lastFail.Load())
		if time.Since(lastFail) < pb.cooldown {
			return false // still in cooldown
		}
		// Reset after cooldown (half-open)
		pb.failures.Store(0)
	}
	return true
}

func (pc *PeerCache) recordPeerFailure(addr string) {
	v, _ := pc.breakers.LoadOrStore(addr, &peerBreaker{
		threshold: 5,
		cooldown:  10 * time.Second,
	})
	pb := v.(*peerBreaker)
	pb.failures.Add(1)
	pb.lastFail.Store(time.Now().UnixMilli())
}

func (pc *PeerCache) recordPeerSuccess(addr string) {
	v, ok := pc.breakers.Load(addr)
	if ok {
		pb := v.(*peerBreaker)
		pb.failures.Store(0)
	}
}

// MetricsJSON returns peer cache metrics as JSON bytes.
func (pc *PeerCache) MetricsJSON() []byte {
	stats := pc.Stats()
	b, _ := json.Marshal(stats)
	return b
}
