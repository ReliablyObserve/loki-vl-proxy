//go:build e2e

package e2e_fleet

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// TestFleet_PeerRestoreUnderQueryLoad is the latency proof of the peer bulk
// restore's lower priority (opt-in: LVP_PEER_LOAD=1; not in the CI e2e-fleet
// job, it runs for about ten minutes and needs a quiet machine).
//
// Two ready replicas hold the same fixture of LVP_PEER_LOAD_MB (default 2048)
// MiB of s2 records (about 720 MiB stored): proxy-load-a serves the restores and takes the query load,
// proxy-load-b is its neighbour (it serves restores too and takes a smaller
// load). A load driver in a container (test/e2e-fleet/cmd/peerload) sends an
// open-loop mix all along: cached query_range hits and /_cache/get to a, a few
// VictoriaLogs-backed misses to a, cached hits to b. The phases, interleaved:
//
//	control 1 · restore by one replica (c) · control 2 ·
//	restore by two replicas at once (d, e) · control 3
//
// Pass bound (fixed before the first run): for the hit, peer and neighbour
// classes, the p50, p95 and p99 of the samples taken while a restore stream
// was being served are at most the worst control phase's × 1.10 + 1 ms, with
// no errors. The run is invalid when the controls differ from each other by
// more than that bound (reported, never a pass). The miss class depends on
// VictoriaLogs and is reported only.
//
// conformance: cache-restart-handover
func TestFleet_PeerRestoreUnderQueryLoad(t *testing.T) {
	if os.Getenv("LVP_PEER_LOAD") != "1" {
		t.Skip("set LVP_PEER_LOAD=1 to run the peer restore latency proof")
	}
	out := os.Getenv("LVP_PEER_LOAD_OUT")
	if out == "" {
		out = t.TempDir()
	}
	mb := envOr("LVP_PEER_LOAD_MB", "2048")
	control := 45 * time.Second
	if v := os.Getenv("LVP_PEER_LOAD_CONTROL"); v != "" {
		control, _ = time.ParseDuration(v)
	}
	compose := func(args ...string) {
		fleetCompose(t, append([]string{"-f", "docker-compose.yml", "-f", "docker-compose.peerload.yml", "--profile", "peerload"}, args...)...)
	}
	all := []string{"proxy-load-a", "proxy-load-b", "proxy-load-c", "proxy-load-d", "proxy-load-e"}
	prefix := envOr("FLEET_NAME_PREFIX", "fleet")
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", prefix+"-peerload-driver").Run()
		compose(append([]string{"rm", "--stop", "--force", "--volumes"}, all...)...)
	})
	addr := func(service string) string {
		port := map[string]string{"proxy-load-a": "3120", "proxy-load-b": "3121", "proxy-load-c": "3122", "proxy-load-d": "3123", "proxy-load-e": "3124"}[service]
		return "http://localhost:" + envOr("FLEET_PROXY_LOAD_"+strings.ToUpper(service[len(service)-1:])+"_PORT", port)
	}

	// The fixture both ready replicas serve.
	fixture := filepath.Join(out, "fixture-"+mb+".db")
	if _, err := os.Stat(fixture + ".entries"); err != nil {
		cmd := exec.Command("go", "test", "-count=1", "-run", "^TestWritePeerLoadFixture$", "./internal/cache/")
		cmd.Dir = "../.."
		cmd.Env = append(os.Environ(), "LVP_PEER_LOAD_FIXTURE="+fixture, "LVP_RESTORE_BENCH_MB="+mb)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fixture: %v\n%s", err, b)
		}
	}
	raw, _ := os.ReadFile(fixture + ".entries")
	entries, _ := strconv.Atoi(strings.TrimSpace(string(raw)))

	compose("up", "-d", "--wait", "--wait-timeout", "120", "victorialogs")
	// Lines for the query classes.
	app := fmt.Sprintf("peer-load-%d", time.Now().UnixNano())
	ingestLoadLines(t, app)

	for _, s := range []string{"proxy-load-a", "proxy-load-b"} {
		compose("create", "--no-build", s)
		if b, err := exec.Command("docker", "cp", fixture, prefix+"-"+s+":/cache/data.db").CombinedOutput(); err != nil {
			t.Fatalf("copy the fixture into %s: %v\n%s", s, err, b)
		}
	}
	compose("up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "300", "proxy-load-a", "proxy-load-b")
	a, b := addr("proxy-load-a"), addr("proxy-load-b")
	waitProxyReady(t, a)
	waitProxyReady(t, b)

	// The driver, on the compose network, from the host-built binary.
	driver := filepath.Join(out, "peerload")
	build := exec.Command("go", "build", "-o", driver, "./test/e2e-fleet/cmd/peerload")
	build.Dir = "../.."
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+envOr("LVP_PEER_LOAD_GOARCH", "arm64"))
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the driver: %v\n%s", err, b)
	}
	network := envOr("COMPOSE_PROJECT_NAME", "e2e-fleet") + "_default"
	samples := filepath.Join(out, "samples.csv")
	_ = os.Remove(samples)
	run := exec.Command("docker", "run", "-d", "--name", prefix+"-peerload-driver", "--network", network, "--user", "root",
		"-v", out+":/out", "--entrypoint", "/out/peerload", envOr("PROXY_IMAGE", "loki-vl-proxy:fleet-local"),
		"-token", "e2e-test-token", "-app", app, "-keys", strconv.Itoa(entries), "-duration", "30m",
		"-hit-qps", envOr("LVP_PEER_LOAD_HIT_QPS", "100"), "-peer-qps", envOr("LVP_PEER_LOAD_PEER_QPS", "100"),
		"-miss-qps", envOr("LVP_PEER_LOAD_MISS_QPS", "2"), "-neighbour-qps", envOr("LVP_PEER_LOAD_NEIGHBOUR_QPS", "50"))
	if b, err := run.CombinedOutput(); err != nil {
		t.Fatalf("start the driver: %v\n%s", err, b)
	}

	// The serving side's activity, once a second: samples count for a restore
	// phase only while a stream is served.
	tl := &activityTimeline{}
	stopPoll := tl.poll(a, b)
	stats := &resourceLog{}
	stopStats := stats.poll(prefix)

	phases := []loadPhase{}
	mark := func(name string, from time.Time) {
		phases = append(phases, loadPhase{name: name, from: from, to: time.Now()})
	}
	time.Sleep(10 * time.Second) // the hits become cached
	from := time.Now()
	time.Sleep(control)
	mark("control 1", from)
	restore := func(name string, restorers ...string) {
		from := time.Now()
		compose(append([]string{"up", "-d", "--no-deps", "--no-build"}, restorers...)...)
		for _, s := range restorers {
			waitProxyReadyWithin(t, addr(s), 5*time.Minute)
		}
		waitFor(t, "the restore streams to end", 2*time.Minute, func() bool { return tl.activeNow() == 0 })
		mark(name, from)
	}
	restore("restore 1 (c)", "proxy-load-c")
	from = time.Now()
	time.Sleep(control)
	mark("control 2", from)
	restore("restore 2 (d+e)", "proxy-load-d", "proxy-load-e")
	from = time.Now()
	time.Sleep(control)
	mark("control 3", from)

	_ = exec.Command("docker", "stop", "-t", "10", prefix+"-peerload-driver").Run()
	stopPoll()
	stopStats()

	report := analyseLoad(t, samples, phases, tl)
	for _, s := range []string{"proxy-load-c", "proxy-load-d", "proxy-load-e"} {
		m := scrapeMetrics(t, addr(s))
		peer := map[string]string{"source": "peer"}
		report = append(report, fmt.Sprintf("%s: restored %v of %v planned entries (%.0f MB), %v skipped, outcome done %v, deadline %v; %.1f MB/s",
			s, m.sum("loki_vl_proxy_cache_restart_warm_processed_entries", peer)-m.sum("loki_vl_proxy_cache_restart_warm_skipped_entries", peer),
			m.sum("loki_vl_proxy_cache_restart_warm_planned_entries", peer), m.sum("loki_vl_proxy_cache_restart_warm_planned_bytes", peer)/1e6,
			m.sum("loki_vl_proxy_cache_restart_warm_skipped_entries", peer),
			m.sum("loki_vl_proxy_cache_restart_warm_outcome", map[string]string{"source": "peer", "outcome": "done"}),
			m.sum("loki_vl_proxy_cache_restart_warm_outcome", map[string]string{"source": "peer", "outcome": "deadline"}),
			m.sum("loki_vl_proxy_cache_restart_warm_throughput_bytes_per_second", peer)/1e6))
	}
	for _, s := range []string{"proxy-load-a", "proxy-load-b"} {
		m := scrapeMetrics(t, addr(s))
		var pressure []string
		for _, sig := range []string{"scheduler_latency", "cpu", "gc", "memory", "request_latency", "peer_latency", "disk_latency", "network"} {
			if v := m.sum("loki_vl_proxy_peer_cache_bulk_serve_pressure_total", map[string]string{"signal": sig}); v > 0 {
				pressure = append(pressure, fmt.Sprintf("%s=%v", sig, v))
			}
		}
		report = append(report, fmt.Sprintf("%s served: %v streams done, %v throttled, %v stopped; refused busy %v, throttled %v; %.0f MB sent; rate halved %v times, paused %v times (%.1f s); pressure ticks %s",
			s, m.sum("loki_vl_proxy_peer_cache_bulk_serve_streams_total", map[string]string{"outcome": "done"}),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_streams_total", map[string]string{"outcome": "throttled"}),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_streams_total", map[string]string{"outcome": "stopped"}),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_rejected_total", map[string]string{"reason": "busy"}),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_rejected_total", map[string]string{"reason": "throttled"}),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_bytes_total", nil)/1e6,
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_rate_decreases_total", nil),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_pauses_total", nil),
			m.sum("loki_vl_proxy_peer_cache_bulk_serve_paused_seconds_total", nil),
			strings.Join(pressure, " ")))
	}
	report = append(report, stats.summary(phases)...)
	report = append(report, tl.summary(phases, map[string]string{a: "proxy-load-a", b: "proxy-load-b"})...)
	text := strings.Join(report, "\n")
	_ = os.WriteFile(filepath.Join(out, "report.txt"), []byte(text+"\n"), 0o600)
	t.Log("\n" + text)
	if strings.Contains(text, "FAIL") {
		t.Fatal("the restore degraded query latency beyond the bound (see the report)")
	}
	if strings.Contains(text, "INVALID") {
		t.Fatal("the control phases differ by more than the bound: the run is invalid")
	}
}

type loadPhase struct {
	name     string
	from, to time.Time
}

// activityTimeline is the serving replicas' bulk_serve_active, per second.
type activityTimeline struct {
	mu     sync.Mutex
	active map[int64]float64 // Unix second -> streams served by a and b
	now    float64
	// per second and replica: Go heap in use and the governor's rate
	heap, rate map[string]map[int64]float64
}

func (tl *activityTimeline) poll(addrs ...string) func() {
	tl.active = map[int64]float64{}
	tl.heap, tl.rate = map[string]map[int64]float64{}, map[string]map[int64]float64{}
	for _, a := range addrs {
		tl.heap[a], tl.rate[a] = map[int64]float64{}, map[int64]float64{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			total := 0.0
			sec := time.Now().Unix()
			for _, a := range addrs {
				if m, err := scrapeMetricsCtx(ctx, a); err == nil {
					total += m.sum("loki_vl_proxy_peer_cache_bulk_serve_active", nil)
					tl.mu.Lock()
					tl.heap[a][sec] = m.sum("go_memstats_heap_inuse_bytes", nil)
					tl.rate[a][sec] = m.sum("loki_vl_proxy_peer_cache_bulk_serve_rate_bytes_per_second", nil)
					tl.mu.Unlock()
				}
			}
			tl.mu.Lock()
			tl.active[sec] = total
			tl.now = total
			tl.mu.Unlock()
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()
	return func() { cancel(); <-done }
}

func (tl *activityTimeline) activeNow() float64 {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.now
}

// activeAt reports whether a stream was served in the second of t or the one
// before (the poll runs once a second).
func (tl *activityTimeline) activeAt(t time.Time) bool {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	s := t.Unix()
	return tl.active[s] > 0 && tl.active[s-1] > 0
}

type loadSample struct {
	at     time.Time
	class  string
	d      time.Duration
	status int
}

// analyseLoad reads the driver's samples and applies the pass bound.
func analyseLoad(t *testing.T, path string, phases []loadPhase, tl *activityTimeline) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var all []loadSample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), ",")
		if len(parts) != 4 {
			continue
		}
		at, _ := strconv.ParseInt(parts[0], 10, 64)
		d, _ := strconv.ParseInt(parts[2], 10, 64)
		st, _ := strconv.Atoi(parts[3])
		all = append(all, loadSample{at: time.Unix(0, at), class: parts[1], d: time.Duration(d), status: st})
	}
	type stat struct {
		n, errs       int
		p50, p95, p99 time.Duration
	}
	pct := func(ds []time.Duration, q float64) time.Duration {
		if len(ds) == 0 {
			return 0
		}
		return ds[min(int(q*float64(len(ds))), len(ds)-1)]
	}
	statOf := func(ph loadPhase, class string, restore bool) stat {
		var ds []time.Duration
		var st stat
		for _, s := range all {
			if s.class != class || s.at.Before(ph.from) || !s.at.Before(ph.to) {
				continue
			}
			if restore && !tl.activeAt(s.at) {
				continue
			}
			st.n++
			if s.status != 200 {
				st.errs++
				continue
			}
			ds = append(ds, s.d)
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		st.p50, st.p95, st.p99 = pct(ds, 0.50), pct(ds, 0.95), pct(ds, 0.99)
		return st
	}
	ms := func(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d)/1e6) }
	var lines []string
	lines = append(lines, "class | phase | samples | errors | p50 ms | p95 ms | p99 ms | verdict")
	for _, class := range []string{"hit", "peer", "neighbour", "miss"} {
		var controls []stat
		for _, ph := range phases {
			if strings.HasPrefix(ph.name, "control") {
				controls = append(controls, statOf(ph, class, false))
			}
		}
		worst, best := stat{}, stat{p50: 1 << 62, p95: 1 << 62, p99: 1 << 62}
		for _, c := range controls {
			worst.p50, worst.p95, worst.p99 = max(worst.p50, c.p50), max(worst.p95, c.p95), max(worst.p99, c.p99)
			best.p50, best.p95, best.p99 = min(best.p50, c.p50), min(best.p95, c.p95), min(best.p99, c.p99)
		}
		bound := func(d time.Duration) time.Duration { return d + d/10 + time.Millisecond }
		gated := class != "miss"
		for _, ph := range phases {
			restore := strings.HasPrefix(ph.name, "restore")
			st := statOf(ph, class, restore)
			verdict := ""
			switch {
			case !gated:
				verdict = "reported"
			case !restore:
				if worst.p99 > bound(best.p99) || worst.p95 > bound(best.p95) || worst.p50 > bound(best.p50) {
					verdict = "INVALID (controls differ beyond the bound)"
				}
			case st.n == 0:
				verdict = "no samples while a stream was served"
			case st.errs > 0 || st.p50 > bound(worst.p50) || st.p95 > bound(worst.p95) || st.p99 > bound(worst.p99):
				verdict = "FAIL"
			default:
				verdict = "pass"
			}
			lines = append(lines, fmt.Sprintf("%s | %s | %d | %d | %s | %s | %s | %s", class, ph.name, st.n, st.errs, ms(st.p50), ms(st.p95), ms(st.p99), verdict))
		}
	}
	for _, ph := range phases {
		lines = append(lines, fmt.Sprintf("phase %s: %s", ph.name, ph.to.Sub(ph.from).Round(time.Second)))
	}
	return lines
}

// resourceLog samples docker stats of the containers every 5 s.
type resourceLog struct {
	mu   sync.Mutex
	rows []resourceRow
}

type resourceRow struct {
	at        time.Time
	container string
	cpu, mem  float64 // percent of one core, MiB
	netIO     string
}

func (rl *resourceLog) poll(prefix string) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			out, err := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{json .}}").Output()
			if err == nil {
				now := time.Now()
				for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
					var s struct{ Name, CPUPerc, MemUsage, NetIO string }
					if json.Unmarshal([]byte(line), &s) != nil || !strings.HasPrefix(s.Name, prefix+"-") {
						continue
					}
					cpu, _ := strconv.ParseFloat(strings.TrimSuffix(s.CPUPerc, "%"), 64)
					rl.mu.Lock()
					rl.rows = append(rl.rows, resourceRow{at: now, container: s.Name, cpu: cpu, mem: parseMiB(strings.SplitN(s.MemUsage, " / ", 2)[0]), netIO: s.NetIO})
					rl.mu.Unlock()
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
		}
	}()
	return func() { cancel(); <-done }
}

func parseMiB(s string) float64 {
	s = strings.TrimSpace(s)
	for suffix, mult := range map[string]float64{"GiB": 1024, "MiB": 1, "KiB": 1.0 / 1024, "B": 1.0 / (1 << 20)} {
		if strings.HasSuffix(s, suffix) && !(suffix == "B" && (strings.HasSuffix(s, "iB"))) {
			v, _ := strconv.ParseFloat(strings.TrimSuffix(s, suffix), 64)
			return v * mult
		}
	}
	return 0
}

// summary is each phase's mean and peak CPU and peak memory per container,
// and the network counters at its end.
func (rl *resourceLog) summary(phases []loadPhase) []string {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	var out []string
	out = append(out, "container | phase | CPU mean % | CPU peak % | memory peak MiB | net I/O at end")
	for _, ph := range phases {
		byName := map[string][]resourceRow{}
		for _, r := range rl.rows {
			if !r.at.Before(ph.from) && r.at.Before(ph.to) {
				byName[r.container] = append(byName[r.container], r)
			}
		}
		names := make([]string, 0, len(byName))
		for n := range byName {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			rows := byName[n]
			var sum, peak, mem float64
			for _, r := range rows {
				sum += r.cpu
				peak, mem = max(peak, r.cpu), max(mem, r.mem)
			}
			out = append(out, fmt.Sprintf("%s | %s | %.0f | %.0f | %.0f | %s", n, ph.name, sum/float64(len(rows)), peak, mem, rows[len(rows)-1].netIO))
		}
	}
	return out
}

// ingestLoadLines writes an hour of lines for the query classes.
func ingestLoadLines(t *testing.T, app string) {
	t.Helper()
	end := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	var rows strings.Builder
	for i := 0; i < 3000; i++ {
		row, _ := json.Marshal(map[string]string{
			"_time": end.Add(-time.Hour + time.Duration(i)*1200*time.Millisecond).Format(time.RFC3339Nano),
			"_msg":  fmt.Sprintf("request %d done", i),
			"app":   app,
			"pod":   fmt.Sprintf("pod-%02d", i%10),
		})
		rows.Write(row)
		rows.WriteByte('\n')
	}
	resp, err := http.Post(fleetVLURL+"/insert/jsonline?_stream_fields=app,pod", "application/stream+json", strings.NewReader(rows.String()))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp, err := http.Post(fleetVLURL+"/internal/force_flush", "text/plain", nil); err == nil {
		resp.Body.Close()
	}
}

func waitProxyReadyWithin(t *testing.T, addr string, within time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	for deadline := time.Now().Add(within); ; time.Sleep(250 * time.Millisecond) {
		if resp, err := client.Get(addr + "/ready"); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s not ready within %s", addr, within)
		}
	}
}

// summary is each phase's peak Go heap in use and mean governor rate while it
// was not idle, per serving replica.
func (tl *activityTimeline) summary(phases []loadPhase, names map[string]string) []string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	out := []string{"replica | phase | Go heap in use peak MiB | governor rate mean MB/s (while streaming) | rate max MB/s"}
	addrs := make([]string, 0, len(names))
	for a := range names {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	for _, a := range addrs {
		for _, ph := range phases {
			var heap, rateSum, rateMax float64
			n := 0
			for sec := ph.from.Unix(); sec <= ph.to.Unix(); sec++ {
				heap = max(heap, tl.heap[a][sec])
				if r := tl.rate[a][sec]; r > 0 {
					rateSum += r
					rateMax = max(rateMax, r)
					n++
				}
			}
			mean := 0.0
			if n > 0 {
				mean = rateSum / float64(n)
			}
			out = append(out, fmt.Sprintf("%s | %s | %.0f | %.1f | %.1f", names[a], ph.name, heap/(1<<20), mean/1e6, rateMax/1e6))
		}
	}
	return out
}
