// loki-bench: read-path performance comparison between Loki (direct),
// VictoriaLogs via loki-vl-proxy, and VictoriaLogs native LogsQL API.
// Measures throughput, latency percentiles, CPU/memory overhead, and
// network efficiency across configurable concurrency levels and workloads.
//
// Usage:
//
//	loki-bench \
//	  --loki=http://localhost:3101 \
//	  --proxy=http://localhost:3100 \
//	  --vl-direct=http://localhost:9428 \
//	  --loki-metrics=http://localhost:3101/metrics \
//	  --proxy-metrics=http://localhost:3100/metrics \
//	  --workloads=small,heavy,long_range \
//	  --clients=10,50,100,500 \
//	  --duration=30s \
//	  --output=results/
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/metricscrape"
	benchpprof "github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/pprof"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/report"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/runner"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/verify"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func main() {
	var (
		lokiURL      = flag.String("loki", "http://localhost:3101", "Loki direct API base URL")
		proxyURL     = flag.String("proxy", "http://localhost:3100", "loki-vl-proxy base URL")
		vlURL        = flag.String("vl", "", "VictoriaLogs API base URL (optional; for resource tracking)")
		vlDirectURL       = flag.String("vl-direct", "", "VictoriaLogs native LogsQL API URL (optional; enables 3-way comparison)")
		proxyNoCacheURL     = flag.String("proxy-no-cache", "", "loki-vl-proxy instance started with cache disabled (enables cold-cache comparison)")
		proxyCoalescerURL   = flag.String("proxy-coalescer", "", "loki-vl-proxy with coalescer but no cache (measures singleflight deduplication benefit)")
		proxyPartialURL     = flag.String("proxy-partial", "", "loki-vl-proxy with short cache TTL (~20% hit rate) and coalescing enabled (~25% forward rate)")
		lokiMetrics         = flag.String("loki-metrics", "", "Loki /metrics URL for resource tracking (optional)")
		proxyMetrics        = flag.String("proxy-metrics", "", "Proxy /metrics URL for resource tracking (optional)")
		proxyNoCacheMetrics = flag.String("proxy-no-cache-metrics", "", "No-cache proxy /metrics URL (optional)")
		proxyPartialMetrics = flag.String("proxy-partial-metrics", "", "Partial-cache proxy /metrics URL (optional)")
		vlMetrics           = flag.String("vl-metrics", "", "VictoriaLogs /metrics URL for resource tracking (optional)")
		workloadList      = flag.String("workloads", "small,heavy,long_range", "Comma-separated workloads: small,heavy,long_range")
		clientList        = flag.String("clients", "10,50,100,500", "Comma-separated concurrency levels")
		duration          = flag.Duration("duration", 30*time.Second, "Test duration per concurrency level per workload")
		outputDir         = flag.String("output", "results", "Output directory for JSON and markdown reports")
		warmup            = flag.Duration("warmup", 5*time.Second, "Warmup duration before each run (warms proxy cache)")
		skipLoki          = flag.Bool("skip-loki", false, "Skip Loki target (benchmark proxy only)")
		skipProxy         = flag.Bool("skip-proxy", false, "Skip proxy target (benchmark Loki only)")
		skipVLDirect      = flag.Bool("skip-vl-direct", false, "Skip VL-direct target (LogsQL native benchmark)")
		skipProxyNocache  = flag.Bool("skip-proxy-no-cache", false, "Skip no-cache proxy target")
		skipProxyPartial  = flag.Bool("skip-proxy-partial", false, "Skip partial-cache proxy target")
		verbose      = flag.Bool("verbose", false, "Print per-request errors")
		version      = flag.String("version", "", "Version tag attached to results (e.g. v1.17.1)")
		doVerify     = flag.Bool("verify", false, "Before benchmarking, verify Loki and proxy return equivalent data for each query")
		verifyStrict = flag.Bool("verify-strict", false, "Exit non-zero if any verification diff is found")
		jitter            = flag.Duration("jitter", 0, "Per-request time-window jitter: shifts each query's start/end/time by a random amount in [0, jitter] backward. Produces a realistic mix of cache hits, partial hits, and misses. Example: --jitter=2h")
		uniqueWindows     = flag.Bool("unique-windows", false, "Give each worker a distinct, non-overlapping time window (workerID × 1s offset). Defeats both the singleflight coalescer and the response cache, exposing raw proxy machinery overhead: translation + HTTP proxying + response shaping only.")
		pprofProxy        = flag.String("pprof-proxy", "", "Base URL of proxy pprof endpoint (e.g. http://localhost:3100). Captures CPU/heap/alloc profiles during each proxy run.")
		pprofNoCache      = flag.String("pprof-no-cache", "", "Base URL of no-cache proxy pprof endpoint. Captures CPU/heap/alloc profiles during each no-cache run.")
		pprofPartial      = flag.String("pprof-partial", "", "Base URL of partial-cache proxy pprof endpoint.")
		pprofDuration     = flag.Duration("pprof-duration", 30*time.Second, "Duration of CPU profile capture (should match or be shorter than --duration)")
		pprofAuthToken    = flag.String("pprof-auth-token", "", "Bearer token for proxy admin/pprof endpoints (set via -server.admin-auth-token)")
	)
	flag.Parse()

	concurrencies, err := parseInts(*clientList)
	if err != nil {
		fatalf("--clients: %v", err)
	}
	workloadNames := splitTrim(*workloadList)
	now := time.Now()
	workloads := workload.ByName(workloadNames, now)
	if len(workloads) == 0 {
		fatalf("no matching workloads (available: small,heavy,long_range)")
	}

	ctx := context.Background()

	if *doVerify {
		fmt.Println("─── Data Verification ───────────────────────────────────────")
		anyFailed := false
		for _, wl := range workloads {
			results := verify.Run(ctx, *lokiURL, *proxyURL, wl.Queries, 30*time.Second)
			for _, r := range results {
				if r.Passed {
					fmt.Printf("  ✓ %-40s  loki == proxy\n", r.QueryName)
				} else {
					anyFailed = true
					fmt.Printf("  ✗ %-40s  MISMATCH\n", r.QueryName)
					for _, d := range r.Diffs {
						fmt.Printf("      %s:\n        loki:  %s\n        proxy: %s\n", d.Field, d.Loki, d.Proxy)
					}
					if r.LokiErr != nil {
						fmt.Printf("      loki error: %v\n", r.LokiErr)
					}
					if r.ProxyErr != nil {
						fmt.Printf("      proxy error: %v\n", r.ProxyErr)
					}
				}
			}
		}
		fmt.Println()
		if *verifyStrict && anyFailed {
			fatalf("verify-strict: one or more queries mismatched — see above")
		}
	}

	// VL-native workloads use LogsQL syntax and VL-specific endpoints.
	vlWorkloads := workload.VLByName(workloadNames, now)

	var records []report.RunRecord

	for i, wl := range workloads {
		for _, conc := range concurrencies {
			// vlMetricsURL is scraped alongside proxy runs to show VL backend resource impact.
			vlMetricsURL := *vlMetrics
			if vlMetricsURL == "" && *vlURL != "" {
				vlMetricsURL = *vlURL + "/metrics"
			}
			// For vl_direct runs, we also scrape VL metrics (it IS the target).
			vlDirectMetrics := vlMetricsURL
			if vlDirectMetrics == "" && *vlDirectURL != "" {
				vlDirectMetrics = *vlDirectURL + "/metrics"
			}

			// VL-direct workload queries (LogsQL) for this workload slot.
			var vlDirectQueries []workload.Query
			if i < len(vlWorkloads) {
				vlDirectQueries = vlWorkloads[i].Queries
			}

			type target struct {
				name       string
				url        string
				queries    []workload.Query
				metricsURL string
				vlMetrics  string
				skip       bool
				noWarmup   bool // skip warmup (no-cache proxy — warmup has no benefit)
			}
			targets := []target{
				{"loki", *lokiURL, wl.Queries, *lokiMetrics, "", *skipLoki, false},
				{"proxy", *proxyURL, wl.Queries, *proxyMetrics, vlMetricsURL, *skipProxy, false},
				{"proxy_coalescer", *proxyCoalescerURL, wl.Queries, "", vlMetricsURL, *proxyCoalescerURL == "", true},
				{"proxy_nocache", *proxyNoCacheURL, wl.Queries, *proxyNoCacheMetrics, vlMetricsURL, *skipProxyNocache || *proxyNoCacheURL == "", true},
				{"proxy_partial", *proxyPartialURL, wl.Queries, *proxyPartialMetrics, vlMetricsURL, *skipProxyPartial || *proxyPartialURL == "", true},
				{"vl_direct", *vlDirectURL, vlDirectQueries, vlDirectMetrics, "", *skipVLDirect || *vlDirectURL == "", false},
			}

			for _, tgt := range targets {
				if tgt.skip || tgt.url == "" || len(tgt.queries) == 0 {
					continue
				}

				fmt.Printf("\n▶ workload=%-12s  concurrency=%4d  target=%s\n",
					wl.Name, conc, tgt.name)

				// Flush proxy cache before each run so targets start cold.
				// Skipped for Loki / VL-direct (no cache) and when the auth
				// token is not set (flush endpoint requires it). Failure is
				// non-fatal — benchmarks proceed whether or not the flush succeeds.
				isProxyTarget := tgt.name == "proxy" || tgt.name == "proxy_nocache" ||
					tgt.name == "proxy_partial" || tgt.name == "proxy_coalescer"
				if isProxyTarget && tgt.url != "" && *pprofAuthToken != "" {
					flushProxyCache(ctx, tgt.url, *pprofAuthToken)
				}

				// Warmup phase (warms caches, esp. proxy window cache).
				// Runs at full benchmark concurrency with jitter so the cache
				// is populated across the same time-window space the real run
				// will hit — warmup time is not counted in benchmark results.
				// Skipped for no-cache targets and when --unique-windows is set
				// (unique-windows defeats the cache, so warmup provides nothing).
				if *warmup > 0 && !tgt.noWarmup && !*uniqueWindows {
					fmt.Printf("  warming up for %s (concurrency=%d, jitter=%s)...\n", *warmup, conc, *jitter)
					wCfg := runner.Config{
						TargetURL:   tgt.url,
						Concurrency: conc,
						Duration:    *warmup,
						Queries:     tgt.queries,
						TimeJitter:  *jitter,
					}
					runner.Run(ctx, wCfg) // discard warmup result
				}

				// Snapshot before (target + VL backend if configured).
				var resBefore, vlBefore metricscrape.ResourceSnapshot
				if tgt.metricsURL != "" {
					resBefore, err = metricscrape.Scrape(tgt.metricsURL)
					if err != nil {
						fmt.Fprintf(os.Stderr, "  warn: resource scrape before: %v\n", err)
					}
				}
				if tgt.vlMetrics != "" {
					vlBefore, err = metricscrape.Scrape(tgt.vlMetrics)
					if err != nil {
						fmt.Fprintf(os.Stderr, "  warn: vl resource scrape before: %v\n", err)
					}
				}

				// Benchmark run — optionally concurrent CPU profile capture.
				jitterStr := ""
				if *jitter > 0 {
					jitterStr = fmt.Sprintf("  jitter=%s", *jitter)
				}
				fmt.Printf("  running %s (concurrency=%d duration=%s%s)...\n", tgt.name, conc, *duration, jitterStr)

				// Determine pprof base URL for this target.
				pprofBase := ""
				switch tgt.name {
				case "proxy":
					pprofBase = *pprofProxy
				case "proxy_nocache":
					pprofBase = *pprofNoCache
				case "proxy_partial":
					pprofBase = *pprofPartial
				}

				// Start CPU profile concurrently with the bench run.
				type cpuResult struct {
					data []byte
					err  error
				}
				var cpuCh chan cpuResult
				if pprofBase != "" {
					cpuCh = make(chan cpuResult, 1)
					go func() {
						d := *pprofDuration
						if d > *duration {
							d = *duration
						}
						data, err := benchpprof.CaptureCPU(ctx, pprofBase, *pprofAuthToken, d)
						cpuCh <- cpuResult{data, err}
					}()
				}

				cfg := runner.Config{
					TargetURL:     tgt.url,
					Concurrency:   conc,
					Duration:      *duration,
					Queries:       tgt.queries,
					Verbose:       *verbose,
					TimeJitter:    *jitter,
					UniqueWindows: *uniqueWindows,
				}
				result := runner.Run(ctx, cfg)

				// Collect CPU profile and capture heap/alloc/goroutine snapshots.
				if pprofBase != "" && cpuCh != nil {
					pprofDir := filepath.Join(*outputDir, "pprof")
					prefix := fmt.Sprintf("%s-c%d-%s", wl.Name, conc, tgt.name)
					cpuRes := <-cpuCh
					if cpuRes.err != nil {
						fmt.Fprintf(os.Stderr, "  warn: pprof CPU capture: %v\n", cpuRes.err)
					} else {
						p := filepath.Join(pprofDir, prefix+"-cpu.pprof")
						if err := benchpprof.Save(cpuRes.data, p); err != nil {
							fmt.Fprintf(os.Stderr, "  warn: pprof CPU save: %v\n", err)
						} else {
							fmt.Printf("  pprof cpu  → %s\n", p)
						}
					}
					for _, kind := range []struct {
						name string
						fn   func(context.Context, string, string) ([]byte, error)
					}{
						{"heap", benchpprof.CaptureHeap},
						{"allocs", benchpprof.CaptureAllocs},
						{"goroutine", benchpprof.CaptureGoroutine},
					} {
						data, err := kind.fn(ctx, pprofBase, *pprofAuthToken)
						if err != nil {
							fmt.Fprintf(os.Stderr, "  warn: pprof %s: %v\n", kind.name, err)
							continue
						}
						p := filepath.Join(pprofDir, prefix+"-"+kind.name+".pprof")
						if err := benchpprof.Save(data, p); err != nil {
							fmt.Fprintf(os.Stderr, "  warn: pprof %s save: %v\n", kind.name, err)
						} else {
							fmt.Printf("  pprof %-10s → %s\n", kind.name, p)
						}
					}
				}
				result.Workload = wl.Name

				// Snapshot after.
				var resAfter, vlAfter metricscrape.ResourceSnapshot
				if tgt.metricsURL != "" {
					resAfter, err = metricscrape.Scrape(tgt.metricsURL)
					if err != nil {
						fmt.Fprintf(os.Stderr, "  warn: resource scrape after: %v\n", err)
					}
				}
				if tgt.vlMetrics != "" {
					vlAfter, err = metricscrape.Scrape(tgt.vlMetrics)
					if err != nil {
						fmt.Fprintf(os.Stderr, "  warn: vl resource scrape after: %v\n", err)
					}
				}
				delta := resBefore.Delta(resAfter)
				vlDelta := vlBefore.Delta(vlAfter)

				// Print quick summary.
				s := result.Overall
				statusStr := ""
				if s.Status4xx > 0 || s.Status5xx > 0 {
					statusStr = fmt.Sprintf("  4xx=%d  5xx=%d", s.Status4xx, s.Status5xx)
				}
				symbol := "✓"
				if s.ErrorRate > 0.01 {
					symbol = "✗"
				}
				fmt.Printf("  %s throughput=%.0f req/s  p50=%s  p90=%s  p99=%s  errors=%.2f%%%s  bytes=%.1f KB/req\n",
					symbol,
					s.Throughput,
					fmtDur(s.P50), fmtDur(s.P90), fmtDur(s.P99),
					s.ErrorRate*100,
					statusStr,
					float64(s.TotalBytes)/float64(max(s.Count, 1))/1e3,
				)
				if tgt.metricsURL != "" {
					fmt.Printf("  ✓ cpu=%.3f s  rss=%.0f MB  heap=%.0f MB  gc_cycles=%.0f\n",
						delta.CPUSeconds, delta.MemRSSBytes/1e6, delta.HeapInUseBytes/1e6, delta.GCCycles)
				}
				if tgt.vlMetrics != "" {
					fmt.Printf("  ✓ vl backend: cpu=%.3f s  rss=%.0f MB  heap=%.0f MB\n",
						vlDelta.CPUSeconds, vlDelta.MemRSSBytes/1e6, vlDelta.HeapInUseBytes/1e6)
				}

				records = append(records, report.RunRecord{
					Timestamp:      now,
					Version:        *version,
					Target:         tgt.name,
					TargetURL:      tgt.url,
					WorkloadName:   wl.Name,
					Concurrency:    conc,
					Duration:       *duration,
					Result:         result,
					ResourceBefore: resBefore,
					ResourceAfter:  resAfter,
					ResourceDelta:  delta,
					VLBefore:       vlBefore,
					VLAfter:        vlAfter,
					VLDelta:        vlDelta,
				})
			}
		}
	}

	// Write outputs.
	fmt.Printf("\n%s\n", strings.Repeat("═", 90))
	report.WriteText(os.Stdout, records)

	ts := now.Format("2006-01-02T15-04-05")
	jsonPath := filepath.Join(*outputDir, fmt.Sprintf("bench-%s.json", ts))
	mdPath := filepath.Join(*outputDir, fmt.Sprintf("bench-%s.md", ts))

	if err := report.WriteJSON(jsonPath, records); err != nil {
		fmt.Fprintf(os.Stderr, "warn: write JSON: %v\n", err)
	} else {
		fmt.Printf("JSON results: %s\n", jsonPath)
	}
	if err := report.WriteMarkdown(mdPath, records); err != nil {
		fmt.Fprintf(os.Stderr, "warn: write markdown: %v\n", err)
	} else {
		fmt.Printf("Markdown results: %s\n", mdPath)
	}
}

func parseInts(s string) ([]int, error) {
	parts := splitTrim(s)
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("not an integer: %q", p)
		}
		out = append(out, n)
	}
	return out, nil
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	return fmt.Sprintf("%.1fms", float64(d.Milliseconds()))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loki-bench: "+format+"\n", args...)
	os.Exit(1)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// flushProxyCache calls POST /admin/cache/flush on a proxy instance to clear
// all in-memory cache entries before a benchmark run. This ensures each target
// run starts from a cold cache rather than inheriting warmup state.
// Errors are printed as warnings but never abort the benchmark.
func flushProxyCache(ctx context.Context, baseURL, authToken string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/admin/cache/flush", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  warn: cache flush request: %v\n", err)
		return
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  warn: cache flush: %v\n", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "  warn: cache flush returned %d\n", resp.StatusCode)
	} else {
		fmt.Printf("  ✓ cache flushed\n")
	}
}
