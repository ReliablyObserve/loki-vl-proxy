//go:build e2e

package e2e_fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Helpers of the fleet peer cache proof (peer_cache_proof_test.go): stack
// control through the docker CLI, /metrics scraping and the fixture.

const fleetPeerToken = "e2e-test-token"

var (
	fleetVLURL   = envOr("FLEET_VL_URL", "http://localhost:9428")
	fleetLokiURL = envOr("FLEET_LOKI_URL", "http://localhost:3110")
	// fleetServices are the compose services behind proxyAddrs, in order.
	fleetServices = []string{"proxy-a", "proxy-b", "proxy-c"}
	// Edge-case proxies (compose profile "edge"), outside the ring of the
	// three above: one whose disk tier is a tiny tmpfs, one with no
	// writable disk at all.
	fleetDiskFullURL = envOr("FLEET_PROXY_DISKFULL_URL", "http://localhost:3103")
	fleetReadOnlyURL = envOr("FLEET_PROXY_READONLY_URL", "http://localhost:3104")
	fleetHTTP        = &http.Client{Timeout: 60 * time.Second}
)

// --- stack control -----------------------------------------------------------

// replica is one proxy of the fleet: its compose service and host address.
type replica struct {
	name string
	addr string
}

func fleetReplica(idx int) replica { return replica{name: fleetServices[idx], addr: proxyAddrs[idx]} }

func fleetReplicas() []replica {
	return []replica{fleetReplica(0), fleetReplica(1), fleetReplica(2)}
}

// fleetCompose runs `docker compose <args>` in this directory, where the
// stack's docker-compose.yml lives. COMPOSE_PROJECT_NAME, DOCKER_CONTEXT,
// PROXY_IMAGE and the port overrides come from the environment the stack was
// started with.
func fleetCompose(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// fleetComposeOutput is fleetCompose returning the command's output.
func fleetComposeOutput(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// containerState returns a container's status and exit code.
func containerState(t *testing.T, name string) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", name).CombinedOutput()
	if err != nil {
		t.Fatalf("docker inspect %s: %v\n%s", name, err, out)
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 {
		t.Fatalf("docker inspect %s: unexpected %q", name, out)
	}
	code, _ := strconv.Atoi(fields[1])
	return fields[0], code
}

// waitProxyReady polls /ready until the proxy answers 200.
func waitProxyReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		resp, err := client.Get(addr + "/ready")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become ready within 90s (last error %v)", addr, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// restartKeepDisk restarts a proxy container: its memory tier is lost, its
// disk tier (the container's anonymous /cache volume) is kept.
func restartKeepDisk(t *testing.T, idx int) {
	t.Helper()
	fleetCompose(t, "restart", fleetServices[idx])
	waitProxyReady(t, proxyAddrs[idx])
}

// recreateWithoutDisk removes the containers and their anonymous /cache
// volumes and starts them again: memory and disk tiers are both lost.
func recreateWithoutDisk(t *testing.T, idxs ...int) {
	t.Helper()
	services := make([]string, 0, len(idxs))
	for _, i := range idxs {
		services = append(services, fleetServices[i])
	}
	fleetCompose(t, append([]string{"rm", "--stop", "--force", "--volumes"}, services...)...)
	fleetCompose(t, append([]string{"up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120"}, services...)...)
	for _, i := range idxs {
		waitProxyReady(t, proxyAddrs[i])
	}
}

// --- metrics -----------------------------------------------------------------

type promSample struct {
	name   string
	labels map[string]string
	value  float64
}

type promScrape []promSample

func scrapeMetrics(t *testing.T, addr string) promScrape {
	t.Helper()
	m, err := scrapeMetricsErr(addr)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// scrapeMetricsErr is scrapeMetrics for goroutines, which must not fail the
// test while a replica restarts.
func scrapeMetricsErr(addr string) (promScrape, error) {
	return scrapeMetricsCtx(context.Background(), addr)
}

// scrapeMetricsCtx scrapes with ctx, so the resource sampler can bound a
// scrape of a paused replica and abort it when the phase ends.
func scrapeMetricsCtx(ctx context.Context, addr string) (promScrape, error) {
	// A proxy serves one /metrics scrape at a time (429 otherwise): the
	// sampler and the counters take turns per replica.
	lock, _ := scrapeLocks.LoadOrStore(addr, &sync.Mutex{})
	mu := lock.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	for attempt := 0; ; attempt++ {
		m, retry, err := scrapeOnce(ctx, addr)
		if !retry || attempt >= 20 || ctx.Err() != nil {
			return m, err
		}
		time.Sleep(25 * time.Millisecond)
	}
}

var scrapeLocks sync.Map // address -> *sync.Mutex

func scrapeOnce(ctx context.Context, addr string) (promScrape, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/metrics", nil)
	if err != nil {
		return nil, false, err
	}
	resp, err := fleetHTTP.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("scrape %s/metrics: %w", addr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, true, fmt.Errorf("scrape %s/metrics: 429", addr)
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("scrape %s/metrics: status %d err %v", addr, resp.StatusCode, err)
	}
	var out promScrape
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if s, ok := parsePromLine(line); ok {
			out = append(out, s)
		}
	}
	return out, false, nil
}

// parsePromLine parses `name{k="v",...} value` and `name value`.
func parsePromLine(line string) (promSample, bool) {
	s := promSample{labels: map[string]string{}}
	rest := line
	if i := strings.IndexAny(rest, "{ "); i < 0 {
		return s, false
	} else {
		s.name = rest[:i]
		rest = rest[i:]
	}
	if strings.HasPrefix(rest, "{") {
		rest = rest[1:]
		for {
			rest = strings.TrimLeft(rest, ", ")
			if strings.HasPrefix(rest, "}") {
				rest = rest[1:]
				break
			}
			eq := strings.Index(rest, "=")
			if eq < 0 || len(rest) < eq+2 || rest[eq+1] != '"' {
				return s, false
			}
			key := rest[:eq]
			rest = rest[eq+1:]
			// Find the closing quote, skipping escaped ones.
			end := 1
			for end < len(rest) && (rest[end] != '"' || rest[end-1] == '\\') {
				end++
			}
			if end >= len(rest) {
				return s, false
			}
			value, err := strconv.Unquote(rest[:end+1])
			if err != nil {
				return s, false
			}
			s.labels[key] = value
			rest = rest[end+1:]
		}
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return s, false
	}
	s.value = v
	return s, true
}

// sum adds every sample of name whose labels match all of want (a value
// ending in "*" matches as a prefix).
func (m promScrape) sum(name string, want map[string]string) float64 {
	total := 0.0
	for _, s := range m {
		if s.name != name {
			continue
		}
		matched := true
		for k, v := range want {
			got, ok := s.labels[k]
			if strings.HasSuffix(v, "*") {
				matched = ok && strings.HasPrefix(got, strings.TrimSuffix(v, "*"))
			} else {
				matched = ok && got == v
			}
			if !matched {
				break
			}
		}
		if matched {
			total += s.value
		}
	}
	return total
}

// fleetCounters are the counters one proof step compares before and after.
type fleetCounters struct {
	vlSelect     float64 // VictoriaLogs /select/* calls this replica made
	vlStats      float64 // of which /select/logsql/stats_query_range
	vlSeconds    float64 // time spent waiting on VictoriaLogs, all routes
	peerHits     float64 // values fetched from a peer replica (L3)
	backupHits   float64 // of which from a replica other than the owner (replication builds)
	peerMisses   float64
	peerErrors   float64
	wtPushes     float64 // write-through pushes to owner peers
	wtErrors     float64
	tierL2Hits   float64 // shared read path disk hits
	tierL3Hits   float64 // shared read path peer hits
	windowHits   float64 // query_range window cache hits (any tier)
	windowMisses float64
	patternsPeer float64 // pattern entries restored from peer snapshots at startup
	// missedAll counts shared-read lookups that missed memory, disk and
	// peers: 0 for a request means none of its answers was computed.
	missedAll float64
	// Where label stream inventory units came from, on builds that have the
	// inventory (metadata_stream_inventory_unit); its unit lookups use the
	// shared read path, so they are also in tierL2Hits and tierL3Hits.
	invMemory, invDisk, invPeer, invVL float64
	// VictoriaLogs calls made by every running replica of the fleet while
	// one request was answered (set by ask): a replica may forward work to
	// a peer, so "no VictoriaLogs call" is checked fleet-wide.
	fleetVLSelect, fleetVLStats, fleetVLSeconds float64
	// slnRefreshes counts the per-replica stream label names refreshes (one
	// a minute per replica, started by any query, listing the last hour).
	slnRefreshes float64
	// vlRoutes are this replica's VictoriaLogs calls per route; fleetVLRoutes
	// the fleet's (set by ask).
	vlRoutes, fleetVLRoutes map[string]float64
}

func readCounters(t *testing.T, addr string) fleetCounters {
	t.Helper()
	return countersFrom(scrapeMetrics(t, addr))
}

func countersFrom(m promScrape) fleetCounters {
	vl := map[string]string{"system": "vl", "direction": "upstream", "route": "/select/*"}
	stats := map[string]string{"system": "vl", "direction": "upstream", "route": "/select/logsql/stats_query_range"}
	inv := func(source string) float64 {
		return m.sum("loki_vl_proxy_internal_operation_total", map[string]string{"operation": "metadata_stream_inventory_unit", "outcome": source})
	}
	return fleetCounters{
		vlSelect:     m.sum("loki_vl_proxy_requests_total", vl),
		vlStats:      m.sum("loki_vl_proxy_requests_total", stats),
		vlSeconds:    m.sum("loki_vl_proxy_backend_duration_seconds_sum", nil),
		peerHits:     m.sum("loki_vl_proxy_peer_cache_hits_total", nil),
		backupHits:   m.sum("loki_vl_proxy_peer_cache_backup_hits_total", nil),
		peerMisses:   m.sum("loki_vl_proxy_peer_cache_misses_total", nil),
		peerErrors:   m.sum("loki_vl_proxy_peer_cache_errors_total", nil),
		wtPushes:     m.sum("loki_vl_proxy_peer_cache_write_through_pushes_total", nil),
		wtErrors:     m.sum("loki_vl_proxy_peer_cache_write_through_errors_total", nil),
		tierL2Hits:   m.sum("loki_vl_proxy_cache_tier_hits_total", map[string]string{"tier": "l2_disk"}),
		tierL3Hits:   m.sum("loki_vl_proxy_cache_tier_hits_total", map[string]string{"tier": "l3_peer"}),
		windowHits:   m.sum("loki_vl_proxy_window_cache_hit_total", nil),
		windowMisses: m.sum("loki_vl_proxy_window_cache_miss_total", nil),
		patternsPeer: m.sum("loki_vl_proxy_patterns_restored_from_peers_total", nil),
		missedAll:    m.sum("loki_vl_proxy_cache_backend_fallthrough_total", nil),
		invMemory:    inv("memory"),
		invDisk:      inv("disk"),
		invPeer:      inv("peer"),
		invVL:        inv("vl"),
		slnRefreshes: m.sum("loki_vl_proxy_internal_operation_total", map[string]string{"operation": "stream_label_names", "outcome": "refresh*"}),
		vlRoutes:     vlRouteCounts(m),
	}
}

func vlRouteCounts(m promScrape) map[string]float64 {
	out := map[string]float64{}
	for _, s := range m {
		if s.name == "loki_vl_proxy_requests_total" && s.labels["system"] == "vl" && s.labels["direction"] == "upstream" && strings.HasPrefix(s.labels["route"], "/select/") {
			out[s.labels["route"]] += s.value
		}
	}
	return out
}

func routesMinus(a, b map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range a {
		if d := v - b[k]; d != 0 {
			out[k] = d
		}
	}
	for k, v := range b {
		if _, ok := a[k]; !ok && v != 0 {
			out[k] = -v
		}
	}
	return out
}

func (a fleetCounters) minus(b fleetCounters) fleetCounters {
	return fleetCounters{
		vlSelect: a.vlSelect - b.vlSelect, vlStats: a.vlStats - b.vlStats, vlSeconds: a.vlSeconds - b.vlSeconds,
		peerHits: a.peerHits - b.peerHits, backupHits: a.backupHits - b.backupHits,
		peerMisses: a.peerMisses - b.peerMisses, peerErrors: a.peerErrors - b.peerErrors,
		wtPushes: a.wtPushes - b.wtPushes, wtErrors: a.wtErrors - b.wtErrors,
		tierL2Hits: a.tierL2Hits - b.tierL2Hits, tierL3Hits: a.tierL3Hits - b.tierL3Hits,
		windowHits: a.windowHits - b.windowHits, windowMisses: a.windowMisses - b.windowMisses,
		patternsPeer: a.patternsPeer - b.patternsPeer, missedAll: a.missedAll - b.missedAll,
		invMemory: a.invMemory - b.invMemory, invDisk: a.invDisk - b.invDisk, invPeer: a.invPeer - b.invPeer, invVL: a.invVL - b.invVL,
		fleetVLSelect: a.fleetVLSelect - b.fleetVLSelect, fleetVLStats: a.fleetVLStats - b.fleetVLStats, fleetVLSeconds: a.fleetVLSeconds - b.fleetVLSeconds,
		slnRefreshes: a.slnRefreshes - b.slnRefreshes,
		vlRoutes:     routesMinus(a.vlRoutes, b.vlRoutes), fleetVLRoutes: routesMinus(a.fleetVLRoutes, b.fleetVLRoutes),
	}
}

func (a fleetCounters) plus(b fleetCounters) fleetCounters {
	return a.minus(fleetCounters{}.minus(b))
}

// topLevelL2Hits and topLevelL3Hits are the shared-tier hits of the answer
// itself, without the inventory's unit lookups.
func (a fleetCounters) topLevelL2Hits() float64 { return a.tierL2Hits - a.invDisk }
func (a fleetCounters) topLevelL3Hits() float64 { return a.tierL3Hits - a.invPeer }

func (a fleetCounters) String() string {
	return fmt.Sprintf("vl_select=%.0f vl_stats=%.0f vl_time=%.3fs fleet_vl_select=%.0f fleet_vl_stats=%.0f peer_hits=%.0f backup_hits=%.0f peer_misses=%.0f peer_errors=%.0f wt_pushes=%.0f wt_errors=%.0f l2_hits=%.0f l3_hits=%.0f window_hits=%.0f window_misses=%.0f inventory_units(memory=%.0f disk=%.0f peer=%.0f vl=%.0f) stream_label_names_refreshes=%.0f fleet_vl_routes=%v",
		a.vlSelect, a.vlStats, a.vlSeconds, a.fleetVLSelect, a.fleetVLStats, a.peerHits, a.backupHits, a.peerMisses, a.peerErrors, a.wtPushes, a.wtErrors, a.tierL2Hits, a.tierL3Hits, a.windowHits, a.windowMisses, a.invMemory, a.invDisk, a.invPeer, a.invVL, a.slnRefreshes, a.fleetVLRoutes)
}

// fleetDown holds the services the proof stopped or paused: they are left
// out of fleet-wide counting (a paused one would hang the scrape).
var fleetDown sync.Map

func markDown(name string, down bool) {
	if down {
		fleetDown.Store(name, true)
	} else {
		fleetDown.Delete(name)
	}
}

// countedReplicas are the running ring replicas plus extra (an edge proxy).
func countedReplicas(extra replica) []replica {
	var out []replica
	seen := false
	for _, rep := range fleetReplicas() {
		if _, down := fleetDown.Load(rep.name); down {
			continue
		}
		seen = seen || rep.name == extra.name
		out = append(out, rep)
	}
	if !seen && extra.name != "" {
		out = append(out, extra)
	}
	return out
}

// scrapeAll reads the counters of every replica concurrently.
func scrapeAll(t *testing.T, reps []replica) map[string]fleetCounters {
	t.Helper()
	type result struct {
		name string
		c    fleetCounters
		err  error
	}
	ch := make(chan result, len(reps))
	for _, rep := range reps {
		go func(rep replica) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			m, err := scrapeMetricsCtx(ctx, rep.addr)
			ch <- result{rep.name, countersFrom(m), err}
		}(rep)
	}
	out := map[string]fleetCounters{}
	for range reps {
		r := <-ch
		if r.err != nil {
			t.Fatal(r.err)
		}
		out[r.name] = r.c
	}
	return out
}

// settleFleet waits until the VictoriaLogs calls and write-through pushes of
// every replica stop changing: pushes run asynchronously after the response
// was written, and a request may start background refreshes. Counting before
// they settle would charge them to the next request.
func settleFleet(t *testing.T, reps []replica) map[string]fleetCounters {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	last := scrapeAll(t, reps)
	for stable := 0; stable < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs calls or write-through pushes still changing after 15s: %v", last)
		}
		time.Sleep(150 * time.Millisecond)
		cur := scrapeAll(t, reps)
		same := true
		for name, c := range cur {
			l := last[name]
			same = same && c.vlSelect == l.vlSelect && c.wtPushes == l.wtPushes && c.wtErrors == l.wtErrors
		}
		if same {
			stable++
		} else {
			stable = 0
		}
		last = cur
	}
	return last
}

// waitSettled is settleFleet for one replica.
func waitSettled(t *testing.T, addr string) fleetCounters {
	t.Helper()
	return settleFleet(t, []replica{{name: addr, addr: addr}})[addr]
}

// fleetReplicationFactor is the replicas of every peer-cache key the
// proxies report; builds without replication report nothing and keep one.
func fleetReplicationFactor(t *testing.T) int {
	t.Helper()
	factor := 0
	for _, rep := range fleetReplicas() {
		f := int(scrapeMetrics(t, rep.addr).sum("loki_vl_proxy_peer_cache_replication_factor", nil))
		if f < 1 {
			f = 1
		}
		if factor != 0 && f != factor {
			t.Fatalf("the replicas disagree on the replication factor: %d and %d", factor, f)
		}
		factor = f
	}
	return factor
}

// --- peer endpoint -----------------------------------------------------------

// peerHasKey asks a replica's peer endpoint whether its memory holds key.
func peerHasKey(t *testing.T, addr, key string) bool {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, addr+"/_cache/has?keys="+url.QueryEscape(key), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Peer-Token", fleetPeerToken)
	resp, err := fleetHTTP.Do(req)
	if err != nil {
		t.Fatalf("%s /_cache/has: %v", addr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s /_cache/has: %d %s", addr, resp.StatusCode, body)
	}
	var presence map[string]struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(body, &presence); err != nil {
		t.Fatalf("%s /_cache/has: %v in %s", addr, err, body)
	}
	return presence[key].OK
}

// peerGet reads key from a replica's peer endpoint and returns the status
// and the size of the value.
func peerGet(addr, key string) (int, int, error) {
	req, err := http.NewRequest(http.MethodGet, addr+"/_cache/get?key="+url.QueryEscape(key), nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("X-Peer-Token", fleetPeerToken)
	resp, err := fleetHTTP.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, len(body), err
}

// peerSet stores value under key through a replica's peer endpoint, the
// call an owner receives from a write-through push.
func peerSet(addr, key string, value []byte) (int, error) {
	target := addr + "/_cache/set?" + url.Values{"key": {key}, "ttl_ms": {"120000"}}.Encode()
	req, err := http.NewRequest(http.MethodPost, target, strings.NewReader(string(value)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Peer-Token", fleetPeerToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := fleetHTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// waitPeerHasKey polls until one of addrs holds key in memory.
func waitPeerHasKey(t *testing.T, key string, timeout time.Duration, addrs ...string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, addr := range addrs {
			if peerHasKey(t, addr, key) {
				return addr
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("none of %v holds %q after %s", addrs, key, timeout)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// --- requests ----------------------------------------------------------------

// fleetGet sends a GET and returns the body, failing on any non-200 answer
// or a partial response.
func fleetGet(t *testing.T, base, path string) []byte {
	t.Helper()
	body, err := fleetGetErr(base, path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// fleetGetErr is fleetGet for goroutines, which must not fail the test.
func fleetGetErr(base, path string) ([]byte, error) {
	resp, err := fleetHTTP.Get(base + path)
	if err != nil {
		return nil, fmt.Errorf("GET %s%s: %w", base, path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GET %s%s: read: %w", base, path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s%s: status %d: %.400s", base, path, resp.StatusCode, body)
	}
	if resp.Header.Get("X-Loki-VL-Partial-Response") != "" {
		return nil, fmt.Errorf("GET %s%s: partial response, not a comparable answer", base, path)
	}
	return body, nil
}

// canonicalJSON re-encodes body so equal JSON compares equal byte for byte.
func canonicalJSON(t *testing.T, label string, body []byte) string {
	t.Helper()
	out, err := canonicalJSONErr(label, body)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func canonicalJSONErr(label string, body []byte) (string, error) {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return "", fmt.Errorf("%s: invalid JSON: %v: %.400s", label, err, body)
	}
	if m, ok := v.(map[string]any); ok {
		if s, _ := m["status"].(string); s != "" && s != "success" {
			return "", fmt.Errorf("%s: status %q: %.400s", label, s, body)
		}
		// Per-request execution statistics differ between a cached and a
		// computed answer; they are not part of what is compared.
		if data, ok := m["data"].(map[string]any); ok {
			delete(data, "stats")
		}
	}
	out, _ := json.Marshal(v)
	return string(out), nil
}

// --- fixture -----------------------------------------------------------------

type fleetFixture struct {
	probe string
	start time.Time // first bucket, hour aligned
	end   time.Time // start + 8h, hour aligned, at least an hour old
	lines int
}

// Four streams, one JSON line a minute each over 8 hours, each stream on its
// own sub-minute offset. The whole range ended more than 25 hours ago, so
// every window, bucket and metadata entry over it is historical: a label
// listing that ends within -max-metadata-cache-freshness (24h, Loki's
// max_metadata_cache_freshness) revalidates its recent edge from
// VictoriaLogs on every replica by design, which is not what is proven here.
func ingestFleetFixture(t *testing.T) fleetFixture {
	t.Helper()
	now := time.Now().UTC()
	fx := fleetFixture{probe: fmt.Sprintf("fp%d", now.UnixNano())}
	fx.start = now.Truncate(time.Hour).Add(-34 * time.Hour)
	fx.end = fx.start.Add(8 * time.Hour)

	type stream struct {
		app, env string
		offset   time.Duration
	}
	streams := []stream{{"web", "prod", 7 * time.Second}, {"web", "staging", 19 * time.Second}, {"api", "prod", 31 * time.Second}, {"api", "staging", 43 * time.Second}}
	methods := []string{"GET", "POST", "PUT"}
	statuses := []int{200, 200, 404, 500}
	var vlRows strings.Builder
	var lokiStreams []any
	for _, s := range streams {
		labels := map[string]string{"fleet_probe": fx.probe, "app": s.app, "env": s.env}
		var values [][]string
		i := 0
		for at := fx.start.Add(s.offset); at.Before(fx.end); at = at.Add(time.Minute) {
			status := statuses[i%len(statuses)]
			level := "info"
			if status >= 500 {
				level = "error"
			} else if status >= 400 {
				level = "warn"
			}
			line := fmt.Sprintf(`{"level":%q,"method":%q,"status":%d,"path":"/v1/items/%d","msg":"request served"}`, level, methods[i%len(methods)], status, i%7)
			values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
			row := map[string]string{"_time": at.Format(time.RFC3339Nano), "_msg": line}
			for k, v := range labels {
				row[k] = v
			}
			encoded, _ := json.Marshal(row)
			vlRows.Write(encoded)
			vlRows.WriteByte('\n')
			i++
		}
		fx.lines += len(values)
		lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
	}

	post := func(target, contentType, body string) {
		resp, err := fleetHTTP.Post(target, contentType, strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST %s: %v", target, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			t.Fatalf("POST %s: status %d: %.400s", target, resp.StatusCode, b)
		}
	}
	waitLokiReady(t)
	post(fleetVLURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("fleet_probe,app,env"), "application/stream+json", vlRows.String())
	payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	post(fleetLokiURL+"/loki/api/v1/push", "application/json", string(payload))
	post(fleetVLURL+"/internal/force_flush", "text/plain", "")
	// The fixture is older than Loki's query_ingesters_within: Loki serves it
	// from the store once the ingester flushed it.
	post(fleetLokiURL+"/flush", "text/plain", "")

	// VictoriaLogs must hold every line before the proxies read it. Loki is
	// checked only before the comparison (waitLokiFixture): its metric path
	// answers empty for a few minutes on a fresh stack, and the cache phases
	// do not need it.
	deadline := time.Now().Add(time.Minute)
	for {
		vlCount := vlCountLines(t, fx)
		if vlCount == fx.lines {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not ready after 1m: %d lines pushed, VictoriaLogs holds %d", fx.lines, vlCount)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fx
}

// waitLokiFixture waits until Loki counts every fixture line on its metric
// path, so a comparison with Loki never compares against its blank window.
func waitLokiFixture(t *testing.T, fx fleetFixture) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		lokiCount := lokiCountLines(t, fx)
		if lokiCount == fx.lines {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki counts %d of the %d fixture lines after 4m", lokiCount, fx.lines)
		}
		time.Sleep(time.Second)
	}
}

func waitLokiReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		resp, err := client.Get(fleetLokiURL + "/ready")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki at %s not ready after 2m", fleetLokiURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func vlCountLines(t *testing.T, fx fleetFixture) int {
	t.Helper()
	params := url.Values{
		"query": {fmt.Sprintf(`fleet_probe:=%q | stats count() n`, fx.probe)},
		"start": {strconv.FormatInt(fx.start.Add(-time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(fx.end.Add(time.Hour).UnixNano(), 10)},
	}
	body := fleetGet(t, fleetVLURL, "/select/logsql/query?"+params.Encode())
	var row struct {
		N string `json:"n"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &row); err != nil {
		return 0
	}
	n, _ := strconv.Atoi(row.N)
	return n
}

func lokiCountLines(t *testing.T, fx fleetFixture) int {
	t.Helper()
	params := url.Values{
		"query": {fmt.Sprintf(`sum(count_over_time({fleet_probe=%q}[9h]))`, fx.probe)},
		"time":  {strconv.FormatInt(fx.end.Add(time.Minute).Unix(), 10)},
	}
	body := fleetGet(t, fleetLokiURL, "/loki/api/v1/query?"+params.Encode())
	var resp struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Value) != 2 {
		return 0
	}
	n, _ := strconv.Atoi(fmt.Sprint(resp.Data.Result[0].Value[1]))
	return n
}

// --- answers compared with Loki ----------------------------------------------

// matrixPoints maps series (its `by` labels) -> timestamp -> value.
func matrixPoints(t *testing.T, label string, body []byte, by ...string) map[string]map[string]string {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "matrix" {
		t.Fatalf("%s: not a successful matrix: %v: %.400s", label, err, body)
	}
	out := map[string]map[string]string{}
	for _, series := range resp.Data.Result {
		parts := make([]string, 0, len(by))
		for _, name := range by {
			parts = append(parts, name+"="+series.Metric[name])
		}
		key := strings.Join(parts, ",")
		if _, dup := out[key]; dup {
			t.Fatalf("%s: two series for {%s}", label, key)
		}
		out[key] = map[string]string{}
		for _, pair := range series.Values {
			if len(pair) != 2 {
				t.Fatalf("%s: malformed sample %v", label, pair)
			}
			out[key][fmt.Sprint(pair[0])] = fmt.Sprint(pair[1])
		}
	}
	return out
}

// streamLines returns the sorted "app env ts line" entries of a streams answer.
func streamLines(t *testing.T, label string, body []byte) []string {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "streams" {
		t.Fatalf("%s: not a successful streams answer: %v: %.400s", label, err, body)
	}
	var out []string
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			if len(v) < 2 {
				t.Fatalf("%s: malformed entry %v", label, v)
			}
			out = append(out, fmt.Sprintf("%s %s %v %v", s.Stream["app"], s.Stream["env"], v[0], v[1]))
		}
	}
	sort.Strings(out)
	return out
}

// labelValues returns the sorted values of a /labels or /label/x/values answer.
func labelValues(t *testing.T, label string, body []byte) []string {
	t.Helper()
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" {
		t.Fatalf("%s: not a successful label answer: %v: %.400s", label, err, body)
	}
	sort.Strings(resp.Data)
	return resp.Data
}

// --- known gaps --------------------------------------------------------------

// proofKnownGaps are subtests of TestFleetPeerCacheProof (paths below it, as
// "phase/layer") that FLEET_PROOF_SKIP lists, comma separated, for local
// investigation of one phase only. CI never sets it: every subtest is a gate.
var proofKnownGaps = func() map[string]bool {
	out := map[string]bool{}
	for _, name := range strings.Split(envOr("FLEET_PROOF_SKIP", ""), ",") {
		if name = strings.TrimSpace(name); name != "" {
			out[name] = true
		}
	}
	return out
}()

// proofRun is t.Run that skips the subtests FLEET_PROOF_SKIP lists.
func proofRun(t *testing.T, name string, fn func(t *testing.T)) bool {
	t.Helper()
	return t.Run(name, func(t *testing.T) {
		rel := strings.TrimPrefix(t.Name(), "TestFleetPeerCacheProof/")
		if proofKnownGaps[rel] {
			t.Skipf("%s: skipped by FLEET_PROOF_SKIP (local investigation only)", rel)
		}
		fn(t)
	})
}
