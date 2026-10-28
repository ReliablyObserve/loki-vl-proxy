//go:build e2e

package e2e_fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Measurements of the fleet peer cache proof. Every request the proof sends
// is recorded with its latency and the replica's own counters; every phase
// samples each replica's CPU, RSS and cache tier sizes from its /metrics and
// the backend containers' CPU and memory from `docker stats`. The report is
// written as JSON to $FLEET_PROOF_REPORT (default: the system temp dir) so
// runs can be compared; only contract bounds are asserted, the rest is
// recorded.

type requestRecord struct {
	Phase        string             `json:"phase"`
	Layer        string             `json:"layer"`
	Replica      string             `json:"replica"`
	Request      string             `json:"request"`
	Repeat       int                `json:"repeat"` // 0 = first read on this replica in this phase
	LatencyMs    float64            `json:"latency_ms"`
	VLCalls      float64            `json:"vl_calls"`
	VLUpstreamMs float64            `json:"vl_upstream_ms"`
	FleetVLCalls float64            `json:"fleet_vl_calls"` // without background work's calls
	VLBackground float64            `json:"vl_background_calls,omitempty"`
	PeerFetches  float64            `json:"peer_fetches,omitempty"`
	PeerFetchMs  float64            `json:"peer_fetch_ms,omitempty"`
	PeerHits     float64            `json:"peer_hits"`
	BackupHits   float64            `json:"backup_hits"`
	InvUnits     map[string]float64 `json:"inventory_units,omitempty"`
	FleetRoutes  map[string]float64 `json:"fleet_vl_routes,omitempty"`
	SLNRefresh   float64            `json:"stream_label_names_refreshes,omitempty"`
	PeerMisses   float64            `json:"peer_misses"`
	PeerErrors   float64            `json:"peer_errors"`
	DiskHits     float64            `json:"disk_hits"`
	WindowHits   float64            `json:"window_hits"`
	Error        string             `json:"error,omitempty"`
}

type replicaResources struct {
	CPUSeconds        float64 `json:"cpu_seconds"`
	RSSPeakBytes      float64 `json:"rss_peak_bytes"`
	RSSMedianBytes    float64 `json:"rss_median_bytes"`
	RSSLastBytes      float64 `json:"rss_last_bytes"`
	MemCacheBytes     float64 `json:"memory_cache_bytes_end"`
	DiskCacheBytes    float64 `json:"disk_cache_bytes_end"`
	MemCacheObjects   float64 `json:"memory_cache_objects_end"`
	DiskCacheObjects  float64 `json:"disk_cache_objects_end"`
	Samples           int     `json:"samples"`
	lastCPU, totalCPU float64
	rss               []float64
}

type containerResources struct {
	CPUPercentMax float64 `json:"cpu_percent_max"`
	CPUPercentAvg float64 `json:"cpu_percent_avg"`
	MemPeakBytes  float64 `json:"mem_peak_bytes"`
	MemLastBytes  float64 `json:"mem_last_bytes"`
	Samples       int     `json:"samples"`
	cpuSum        float64
}

type phaseResources struct {
	Phase      string                         `json:"phase"`
	Seconds    float64                        `json:"seconds"`
	Replicas   map[string]*replicaResources   `json:"replicas"`
	Containers map[string]*containerResources `json:"containers"`
}

type latencyStats struct {
	Count int     `json:"count"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	MaxMs float64 `json:"max_ms"`
}

type layerSummary struct {
	Phase        string       `json:"phase"`
	Layer        string       `json:"layer"`
	Replica      string       `json:"replica"`
	First        latencyStats `json:"first"`
	Repeat       latencyStats `json:"repeat"`
	VLCalls      float64      `json:"vl_calls"`
	VLUpstreamMs float64      `json:"vl_upstream_ms"`
	PeerHits     float64      `json:"peer_hits"`
	PeerErrors   float64      `json:"peer_errors"`
	PeerFetchMs  float64      `json:"peer_fetch_ms"`
}

type proofReport struct {
	Started time.Time `json:"started"`
	// ReplicationFactor is the peer-cache replicas per key the proxies
	// reported (1 on builds without replication).
	ReplicationFactor int                `json:"replication_factor"`
	Seconds           float64            `json:"seconds"`
	Images            map[string]string  `json:"images"`
	Fixture           map[string]any     `json:"fixture"`
	Summary           []layerSummary     `json:"summary"`
	Phases            []phaseResources   `json:"phases"`
	Edges             []map[string]any   `json:"edge_cases"`
	Requests          []requestRecord    `json:"requests"`
	Bounds            map[string]float64 `json:"contract_bounds"`
}

type proofRecorder struct {
	mu     sync.Mutex
	phase  string
	report proofReport
}

var proofRec = &proofRecorder{}

func (r *proofRecorder) setPhase(p string) {
	r.mu.Lock()
	r.phase = p
	r.mu.Unlock()
}

func (r *proofRecorder) currentPhase() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.phase
}

func (r *proofRecorder) addRequest(rec requestRecord) {
	r.mu.Lock()
	if rec.Phase == "" {
		rec.Phase = r.phase
	}
	r.report.Requests = append(r.report.Requests, rec)
	r.mu.Unlock()
}

func (r *proofRecorder) addEdge(name string, fields map[string]any) {
	r.mu.Lock()
	fields["case"] = name
	r.report.Edges = append(r.report.Edges, fields)
	r.mu.Unlock()
}

func (r *proofRecorder) addPhase(p phaseResources) {
	r.mu.Lock()
	r.report.Phases = append(r.report.Phases, p)
	r.mu.Unlock()
}

func (r *proofRecorder) phaseResources(name string) (phaseResources, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.report.Phases {
		if p.Phase == name {
			return p, true
		}
	}
	return phaseResources{}, false
}

func requestRecordFrom(rep replica, req proofRequest, repeat int, latency time.Duration, d fleetCounters) requestRecord {
	return requestRecord{
		Layer: req.layer, Replica: rep.name, Request: req.name, Repeat: repeat,
		LatencyMs:    ms(latency),
		VLCalls:      d.vlSelect,
		VLUpstreamMs: d.vlSeconds * 1000,
		FleetVLCalls: d.fleetVLSelect,
		VLBackground: d.vlBackground,
		PeerFetches:  d.peerFetches,
		PeerFetchMs:  d.peerFetchSeconds * 1000,
		PeerHits:     d.peerHits, BackupHits: d.backupHits, PeerMisses: d.peerMisses, PeerErrors: d.peerErrors,
		InvUnits: inventoryUnits(d), FleetRoutes: d.fleetVLRoutes, SLNRefresh: d.slnRefreshes,
		DiskHits: d.tierL2Hits, WindowHits: d.windowHits,
	}
}

func inventoryUnits(d fleetCounters) map[string]float64 {
	if d.invMemory == 0 && d.invDisk == 0 && d.invPeer == 0 && d.invVL == 0 {
		return nil
	}
	return map[string]float64{"memory": d.invMemory, "disk": d.invDisk, "peer": d.invPeer, "vl": d.invVL}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func latencyOf(values []float64) latencyStats {
	if len(values) == 0 {
		return latencyStats{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	pick := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(sorted)))) - 1
		if i < 0 {
			i = 0
		}
		return sorted[i]
	}
	return latencyStats{Count: len(sorted), P50Ms: pick(0.5), P95Ms: pick(0.95), MaxMs: sorted[len(sorted)-1]}
}

// summarize groups the request records by phase, layer and replica.
func (r *proofRecorder) summarize() []layerSummary {
	type key struct{ phase, layer, replica string }
	type acc struct {
		first, repeat []float64
		sum           layerSummary
	}
	groups := map[key]*acc{}
	var order []key
	for _, rec := range r.report.Requests {
		k := key{rec.Phase, rec.Layer, rec.Replica}
		a, ok := groups[k]
		if !ok {
			a = &acc{sum: layerSummary{Phase: rec.Phase, Layer: rec.Layer, Replica: rec.Replica}}
			groups[k] = a
			order = append(order, k)
		}
		if rec.Error != "" {
			continue
		}
		if rec.Repeat == 0 {
			a.first = append(a.first, rec.LatencyMs)
		} else {
			a.repeat = append(a.repeat, rec.LatencyMs)
		}
		a.sum.VLCalls += rec.VLCalls
		a.sum.VLUpstreamMs += rec.VLUpstreamMs
		a.sum.PeerHits += rec.PeerHits
		a.sum.PeerErrors += rec.PeerErrors
		a.sum.PeerFetchMs += rec.PeerFetchMs
	}
	out := make([]layerSummary, 0, len(order))
	for _, k := range order {
		a := groups[k]
		a.sum.First = latencyOf(a.first)
		a.sum.Repeat = latencyOf(a.repeat)
		out = append(out, a.sum)
	}
	return out
}

// writeProofReport writes the JSON report and logs the summary table.
func writeProofReport(t *testing.T) {
	r := proofRec
	r.mu.Lock()
	r.report.Seconds = time.Since(r.report.Started).Seconds()
	r.report.Summary = r.summarize()
	data, err := json.MarshalIndent(r.report, "", "  ")
	summary := r.report.Summary
	phases := r.report.Phases
	r.mu.Unlock()
	if err != nil {
		t.Errorf("encode proof report: %v", err)
		return
	}
	path := os.Getenv("FLEET_PROOF_REPORT")
	if path == "" {
		path = filepath.Join(os.TempDir(), "fleet-peer-cache-proof.json")
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Errorf("write proof report %s: %v", path, err)
		return
	}
	t.Logf("peer cache proof report: %s", path)
	t.Logf("%-32s %-20s %-10s %6s %9s %9s %9s %9s %8s %10s %8s %8s %8s", "phase", "layer", "replica", "n", "first p50", "first p95", "first max", "rep p50", "vl calls", "vl ms", "peer hit", "peer err", "fetch ms")
	for _, s := range summary {
		t.Logf("%-32s %-20s %-10s %6d %9.1f %9.1f %9.1f %9.1f %8.0f %10.1f %8.0f %8.0f %8.1f", s.Phase, s.Layer, s.Replica, s.First.Count, s.First.P50Ms, s.First.P95Ms, s.First.MaxMs, s.Repeat.P50Ms, s.VLCalls, s.VLUpstreamMs, s.PeerHits, s.PeerErrors, s.PeerFetchMs)
	}
	for _, p := range phases {
		for name, res := range p.Replicas {
			t.Logf("resources %-32s %-16s cpu=%.2fs rss_peak=%.0fMiB rss_median=%.0fMiB mem_cache=%.1fMiB disk_cache=%.1fMiB", p.Phase, name, res.CPUSeconds, res.RSSPeakBytes/(1<<20), res.RSSMedianBytes/(1<<20), res.MemCacheBytes/(1<<20), res.DiskCacheBytes/(1<<20))
		}
		for name, res := range p.Containers {
			t.Logf("resources %-32s %-16s cpu_max=%.1f%% cpu_avg=%.1f%% mem_peak=%.0fMiB", p.Phase, name, res.CPUPercentMax, res.CPUPercentAvg, res.MemPeakBytes/(1<<20))
		}
	}
}

// --- resource sampling -------------------------------------------------------

type resourceSampler struct {
	phase   string
	started time.Time
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	res     phaseResources
}

func fleetContainerNames() []string {
	prefix := envOr("FLEET_NAME_PREFIX", "fleet")
	return []string{prefix + "-vl", prefix + "-loki"}
}

// startPhase names the phase for the request records and samples the given
// replicas (name -> address) and the backend containers until stop.
func startPhase(phase string, replicas []replica) *resourceSampler {
	proofRec.setPhase(phase)
	ctx, cancel := context.WithCancel(context.Background())
	s := &resourceSampler{phase: phase, started: time.Now(), cancel: cancel, res: phaseResources{
		Phase: phase, Replicas: map[string]*replicaResources{}, Containers: map[string]*containerResources{},
	}}
	for _, rep := range replicas {
		s.res.Replicas[rep.name] = &replicaResources{lastCPU: -1}
	}
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			// Concurrently and bounded: a paused replica must not stall the
			// samples of the others.
			var scrapes sync.WaitGroup
			for _, rep := range replicas {
				scrapes.Add(1)
				go func(rep replica) {
					defer scrapes.Done()
					scrapeCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					if m, err := scrapeMetricsCtx(scrapeCtx, rep.addr); err == nil {
						s.observeReplica(rep.name, m)
					}
				}(rep)
			}
			scrapes.Wait()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		for {
			s.observeContainers(ctx)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
	return s
}

func (s *resourceSampler) observeReplica(name string, m promScrape) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.res.Replicas[name]
	cpu := m.sum("loki_vl_proxy_process_cpu_seconds_total", nil)
	switch {
	case r.lastCPU < 0:
	case cpu >= r.lastCPU:
		r.totalCPU += cpu - r.lastCPU
	default: // the process restarted: its counter began again from zero
		r.totalCPU += cpu
	}
	r.lastCPU = cpu
	r.CPUSeconds = r.totalCPU
	rss := m.sum("loki_vl_proxy_process_resident_memory_bytes", nil)
	r.rss = append(r.rss, rss)
	r.RSSLastBytes = rss
	if rss > r.RSSPeakBytes {
		r.RSSPeakBytes = rss
	}
	r.MemCacheBytes = m.sum("loki_vl_proxy_cache_bytes", map[string]string{"tier": "l1_memory"})
	r.DiskCacheBytes = m.sum("loki_vl_proxy_cache_bytes", map[string]string{"tier": "l2_disk"})
	r.MemCacheObjects = m.sum("loki_vl_proxy_cache_objects", map[string]string{"tier": "l1_memory"})
	r.DiskCacheObjects = m.sum("loki_vl_proxy_cache_objects", map[string]string{"tier": "l2_disk"})
	r.Samples++
}

// observeContainers takes one `docker stats` sample of the backends.
func (s *resourceSampler) observeContainers(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, fleetContainerNames()...)
	out, err := exec.CommandContext(callCtx, "docker", args...).Output()
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		var row struct {
			Name     string `json:"Name"`
			CPUPerc  string `json:"CPUPerc"`
			MemUsage string `json:"MemUsage"`
		}
		if json.Unmarshal([]byte(line), &row) != nil || row.Name == "" {
			continue
		}
		cpu, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(row.CPUPerc), "%"), 64)
		mem := parseDockerBytes(strings.TrimSpace(strings.SplitN(row.MemUsage, "/", 2)[0]))
		s.mu.Lock()
		c, ok := s.res.Containers[row.Name]
		if !ok {
			c = &containerResources{}
			s.res.Containers[row.Name] = c
		}
		c.Samples++
		c.cpuSum += cpu
		c.CPUPercentAvg = c.cpuSum / float64(c.Samples)
		if cpu > c.CPUPercentMax {
			c.CPUPercentMax = cpu
		}
		if mem > c.MemPeakBytes {
			c.MemPeakBytes = mem
		}
		c.MemLastBytes = mem
		s.mu.Unlock()
	}
}

// parseDockerBytes reads docker's sizes: "12.5MiB", "1.2GiB", "800kB", "512B".
func parseDockerBytes(v string) float64 {
	units := []struct {
		suffix string
		scale  float64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"kB", 1e3}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"B", 1}}
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			n, err := strconv.ParseFloat(strings.TrimSuffix(v, u.suffix), 64)
			if err != nil {
				return 0
			}
			return n * u.scale
		}
	}
	return 0
}

// stop ends the sampling, stores the phase and returns it.
func (s *resourceSampler) stop() phaseResources {
	s.cancel()
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.res.Seconds = time.Since(s.started).Seconds()
	for _, r := range s.res.Replicas {
		if len(r.rss) > 0 {
			sorted := append([]float64(nil), r.rss...)
			sort.Float64s(sorted)
			r.RSSMedianBytes = sorted[len(sorted)/2]
		}
	}
	proofRec.addPhase(s.res)
	return s.res
}

func describeBytes(b float64) string { return fmt.Sprintf("%.1fMiB", b/(1<<20)) }
