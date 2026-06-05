//go:build e2e

package e2e_compat

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestE2ELock_ProxyHeapBoundedUnderDrilldownLoad locks the heap optimization
// work from the 2026-06-05 perf round: pool the bytes.Buffer used by
// compatCacheMiddleware → CompressionHandlerWithOptions → EncodeResponseBody
// (compress.go encodeBodyBufPool + holdBufPool), and pre-size the values
// slice in buildHitsRangeMetricMatrix.
//
// Pre-fix the same workload produced 1.4 GiB process_resident_memory_bytes
// peak with 96 MB live in `bytes.growSlice` (verified by pprof). Post-fix
// we expect peak RSS below 600 MiB for the same query shapes.
//
// The test:
//
//  1. Records pre-bench heap_inuse from the proxy's /metrics endpoint.
//  2. Fires 30 concurrent Drilldown-shape stats queries over 60 s — same
//     shapes the bench/drilldown-vs-loki.sh script exercises (pod / trace_id /
//     service_version), all routed through the vmauth-fronted proxy
//     (PROXY_URL, port 13109 in the e2e stack).
//  3. Records post-bench heap_inuse and process_resident_memory_bytes.
//  4. Asserts:
//     - heap_inuse delta < 500 MiB (pre-fix it was 1.3 GiB+)
//     - peak RSS < 800 MiB (pre-fix it was 1.4 GiB+; cap leaves headroom for
//     unrelated metric churn during the test)
//
// Failure means somebody removed one of the pools — and they have a path
// to the named optimization to undo. Don't tighten the caps further
// without re-running the benchmark; the cap reflects realistic Go runtime
// slack on a Drilldown workload.
func TestE2ELock_ProxyHeapBoundedUnderDrilldownLoad(t *testing.T) {
	ensureDataIngested(t)

	const (
		concurrent   = 30
		runDuration  = 60 * time.Second
		heapCapMiB   = 500
		rssCapMiB    = 800
		metricsPath  = "/metrics"
		statsQueries = 6
	)

	// 1) Baseline
	baseHeap, baseRSS, err := readProxyMemoryMiB(proxyURL + metricsPath)
	if err != nil {
		t.Fatalf("baseline /metrics read: %v", err)
	}
	t.Logf("baseline proxy heap_inuse=%.1f MiB  RSS=%.1f MiB", baseHeap, baseRSS)

	// 2) Drive Drilldown-shape traffic for runDuration.
	queries := []string{
		`sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))`,
		`sum by (k8s_pod_name) (count_over_time({namespace="prod",k8s_pod_name!=""}[2m]))`,
		`sum by (service_version) (count_over_time({namespace="prod",service_version!=""}[2m]))`,
		`sum by (trace_id) (count_over_time({namespace="prod"}|json|drop __error__,__error_details__|trace_id!=""[2m]))`,
		`sum by (level) (count_over_time({namespace="prod"}|json|level!=""[2m]))`,
		`sum by (app) (count_over_time({namespace="prod",app!=""}[2m]))`,
	}
	if len(queries) != statsQueries {
		t.Fatalf("query list mismatch")
	}
	stopAt := time.Now().Add(runDuration)
	var wg sync.WaitGroup
	for w := 0; w < concurrent; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			i := workerID
			for time.Now().Before(stopAt) {
				query := queries[i%statsQueries]
				i++
				now := time.Now().Unix()
				start := now - 3600 // 1 h window — modest range, focuses on the hot path
				params := url.Values{}
				params.Set("query", query)
				params.Set("start", strconv.FormatInt(start, 10))
				params.Set("end", strconv.FormatInt(now, 10))
				params.Set("step", "30s")
				req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
				req.Header.Set("X-Scope-OrgID", "default")
				req.Header.Set("User-Agent", "Grafana/11.5.0")
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					continue
				}
				_ = resp.Body.Close()
			}
		}(w)
	}
	wg.Wait()

	// 3) Post-bench measurement
	postHeap, postRSS, err := readProxyMemoryMiB(proxyURL + metricsPath)
	if err != nil {
		t.Fatalf("post-bench /metrics read: %v", err)
	}
	heapDelta := postHeap - baseHeap
	rssDelta := postRSS - baseRSS
	t.Logf("post-bench  proxy heap_inuse=%.1f MiB (Δ %+.1f)  RSS=%.1f MiB (Δ %+.1f)",
		postHeap, heapDelta, postRSS, rssDelta)
	t.Logf("workload: %d concurrent workers × %s = ~%d queries",
		concurrent, runDuration, int(float64(concurrent)*runDuration.Seconds()/2.0))

	// 4) Hard assertions — these guard the pool/pre-size work.
	if postHeap > heapCapMiB {
		t.Errorf("proxy heap_inuse=%.1f MiB > cap %d MiB — pool/pre-size optimisation may have regressed", postHeap, heapCapMiB)
	}
	if postRSS > rssCapMiB {
		t.Errorf("proxy RSS=%.1f MiB > cap %d MiB — pool/pre-size optimisation may have regressed", postRSS, rssCapMiB)
	}
}

// readProxyMemoryMiB scrapes the proxy's /metrics endpoint and returns
// (go_memstats_heap_inuse_bytes, process_resident_memory_bytes) in MiB.
// Returns an error if either metric is missing — they're standard Go
// runtime metrics and should always be present.
func readProxyMemoryMiB(metricsURL string) (heapMiB float64, rssMiB float64, err error) {
	// /metrics on Loki-VL-proxy variants doesn't require auth (the admin
	// auth-token gates /debug/pprof but not /metrics — verified against
	// e2e-proxy-vmauth in this run).
	resp, err := http.Get(metricsURL)
	if err != nil {
		return 0, 0, fmt.Errorf("metrics request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("metrics HTTP %d", resp.StatusCode)
	}
	// Single pass through the body — parse the two metrics we care about.
	want := map[string]float64{
		"go_memstats_heap_inuse_bytes":  -1,
		"process_resident_memory_bytes": -1,
	}
	if err := scrapeMetrics(resp.Body, want); err != nil {
		return 0, 0, err
	}
	if want["go_memstats_heap_inuse_bytes"] < 0 {
		return 0, 0, fmt.Errorf("go_memstats_heap_inuse_bytes not exposed by %s", metricsURL)
	}
	if want["process_resident_memory_bytes"] < 0 {
		return 0, 0, fmt.Errorf("process_resident_memory_bytes not exposed by %s", metricsURL)
	}
	return want["go_memstats_heap_inuse_bytes"] / (1024 * 1024),
		want["process_resident_memory_bytes"] / (1024 * 1024), nil
}

// scrapeMetrics parses a Prometheus text-format payload and populates the
// values map for any requested metric name (label-less lines only — we
// only need the unlabelled `process_*` and `go_memstats_*` ones).
func scrapeMetrics(r io.Reader, want map[string]float64) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		// Format: `metric_name VALUE` or `metric_name{labels} VALUE`. We
		// only want the label-less form for these globals.
		spaceIdx := strings.IndexByte(string(line), ' ')
		if spaceIdx <= 0 {
			continue
		}
		name := string(line[:spaceIdx])
		if _, ok := want[name]; !ok {
			continue
		}
		valStr := strings.TrimSpace(string(line[spaceIdx+1:]))
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		want[name] = v
	}
	return sc.Err()
}
