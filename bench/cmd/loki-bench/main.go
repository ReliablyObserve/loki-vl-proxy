// loki-bench: read-path performance comparison between Loki (direct),
// VictoriaLogs via loki-vl-proxy, and VictoriaLogs native LogsQL API.
// Measures throughput, latency percentiles, CPU/memory overhead, and
// network efficiency across configurable concurrency levels and workloads.
//
// Usage:
//
//	loki-bench \
//	  --loki=http://localhost:13101 \
//	  --proxy=http://localhost:13100 \
//	  --vl-direct=http://localhost:19428 \
//	  --data-end=auto \
//	  --wait-ingested=45m \
//	  --verify-strict \
//	  --cache-mode=warm \
//	  --loki-metrics=http://localhost:13101/metrics \
//	  --proxy-metrics=http://localhost:13100/metrics \
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

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/dataspan"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/metricscrape"
	benchpprof "github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/pprof"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/report"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/runner"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/verify"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func main() {
	var (
		lokiURL             = flag.String("loki", "http://localhost:13101", "Loki direct API base URL")
		proxyURL            = flag.String("proxy", "http://localhost:13100", "loki-vl-proxy base URL (cached; timed in --cache-mode=warm)")
		vlURL               = flag.String("vl", "", "VictoriaLogs API base URL (optional; for resource tracking)")
		vlDirectURL         = flag.String("vl-direct", "", "VictoriaLogs native LogsQL API URL (optional; enables 3-way comparison)")
		proxyNoCacheURL     = flag.String("proxy-no-cache", "", "loki-vl-proxy instance started with -cache-disabled (timed in --cache-mode=cold)")
		proxyCoalescerURL   = flag.String("proxy-coalescer", "", "loki-vl-proxy with coalescer but no cache (timed in --cache-mode=cold)")
		proxyPartialURL     = flag.String("proxy-partial", "", "loki-vl-proxy with short cache TTL (~20% hit rate) and coalescing enabled (timed in --cache-mode=warm)")
		lokiMetrics         = flag.String("loki-metrics", "", "Loki /metrics URL for resource tracking (optional)")
		proxyMetrics        = flag.String("proxy-metrics", "", "Proxy /metrics URL for resource tracking (optional)")
		proxyNoCacheMetrics = flag.String("proxy-no-cache-metrics", "", "No-cache proxy /metrics URL (optional)")
		proxyPartialMetrics = flag.String("proxy-partial-metrics", "", "Partial-cache proxy /metrics URL (optional)")
		vlMetrics           = flag.String("vl-metrics", "", "VictoriaLogs /metrics URL for resource tracking (optional)")
		workloadList        = flag.String("workloads", "small,heavy,long_range", "Comma-separated workloads: small,heavy,long_range")
		clientList          = flag.String("clients", "10,50,100,500", "Comma-separated concurrency levels")
		duration            = flag.Duration("duration", 30*time.Second, "Test duration per concurrency level per workload")
		outputDir           = flag.String("output", "results", "Output directory for JSON and markdown reports")
		warmup              = flag.Duration("warmup", 5*time.Second, "Warmup duration before each run in --cache-mode=warm (identical for every target)")
		skipLoki            = flag.Bool("skip-loki", false, "Skip Loki target (benchmark proxy only; the report is marked not publishable)")
		skipProxy           = flag.Bool("skip-proxy", false, "Skip proxy target")
		skipVLDirect        = flag.Bool("skip-vl-direct", false, "Skip VL-direct target (LogsQL native benchmark)")
		skipProxyNocache    = flag.Bool("skip-proxy-no-cache", false, "Skip no-cache proxy target")
		skipProxyPartial    = flag.Bool("skip-proxy-partial", false, "Skip partial-cache proxy target")
		verbose             = flag.Bool("verbose", false, "Print per-request errors")
		version             = flag.String("version", "", "Version tag attached to results (e.g. v1.17.1)")
		doVerify            = flag.Bool("verify", false, "Before benchmarking, compare every query on Loki and every timed proxy target: status, degradation, result shape and content")
		verifyStrict        = flag.Bool("verify-strict", false, "Verify (as --verify) and exit non-zero before any timing on a mismatch or degraded answer")
		jitter              = flag.Duration("jitter", 0, "Per-request time-window jitter: shifts each query's start/end/time backward by a random amount in [0, jitter), in whole steps for range queries and never before --data-start. Produces a realistic mix of cache hits, partial hits, and misses. Example: --jitter=2h")
		uniqueWindows       = flag.Bool("unique-windows", false, "Shift every request back by a distinct number of whole steps (whole seconds without a step), inside the data. Defeats the singleflight coalescer and response caches; requires --cache-mode=cold.")
		pprofProxy          = flag.String("pprof-proxy", "", "Base URL of proxy pprof endpoint (e.g. http://localhost:3100). Captures CPU/heap/alloc profiles during each proxy run.")
		pprofNoCache        = flag.String("pprof-no-cache", "", "Base URL of no-cache proxy pprof endpoint. Captures CPU/heap/alloc profiles during each no-cache run.")
		pprofPartial        = flag.String("pprof-partial", "", "Base URL of partial-cache proxy pprof endpoint.")
		pprofDuration       = flag.Duration("pprof-duration", 30*time.Second, "Duration of CPU profile capture (should match or be shorter than --duration)")
		pprofAuthToken      = flag.String("pprof-auth-token", "", "Bearer token for proxy admin/pprof endpoints (set via -server.admin-auth-token)")
		dataEnd             = flag.String("data-end", "", "Workload reference time: every window ends here. RFC3339, Unix seconds/ms/ns, or \"auto\" (max(_time) of the seeded streams in VictoriaLogs, from --vl or --vl-direct). Empty = wall clock (only valid while data is being written live)")
		dataStart           = flag.String("data-start", "", "Start of the seeded data, used to keep --jitter shifts inside the data. RFC3339, Unix timestamp or \"auto\"; defaults to the detected start when --data-end=auto")
		waitIngested        = flag.Duration("wait-ingested", 0, "Before verifying/benchmarking, wait up to this long until Loki and VictoriaLogs hold the same line count for every service in every 1h window of the seeded span (requires a detected or given data span and --vl/--vl-direct); 0 disables")
		waitPoll            = flag.Duration("wait-poll", 15*time.Second, "Poll interval for --wait-ingested")
		verifyTimeout       = flag.Duration("verify-timeout", 5*time.Minute, "Per-request timeout for --verify (matches Loki query_timeout in the compose config)")
		verifyOnly          = flag.Bool("verify-only", false, "Exit after the entry check and verification (no benchmark runs)")
		cacheMode           = flag.String("cache-mode", cacheModeWarm, "warm: Loki with its results caches vs the cached proxy, identical warm-up, no flushes; cold: Loki with every results cache off vs proxies without a response cache, no warm-up. loki-bench checks Loki's /config matches")
		maxErrorRate        = flag.Float64("max-error-rate", 0, "Largest error rate (transport errors, timeouts, HTTP >= 400) and degraded-answer rate a timed run may have, as a fraction (0.01 = 1%); a run above it stops the benchmark with a non-zero exit and non-publishable output")
	)
	flag.Var(aliasFlag{dataEnd}, "now", "Alias for --data-end")
	flag.Parse()

	concurrencies, err := parseInts(*clientList)
	if err != nil {
		fatalf("--clients: %v", err)
	}
	if *cacheMode != cacheModeWarm && *cacheMode != cacheModeCold {
		fatalf("--cache-mode must be %q or %q", cacheModeWarm, cacheModeCold)
	}
	if *uniqueWindows && *cacheMode != cacheModeCold {
		fatalf("--unique-windows requires --cache-mode=cold: Loki's results cache and the proxy's window cache answer shifted windows from overlapping cached extents, so warm targets would not do the work the shifts are meant to force")
	}
	if *maxErrorRate < 0 || *maxErrorRate > 1 {
		fatalf("--max-error-rate must be between 0 and 1")
	}
	workloadNames := splitTrim(*workloadList)
	wallNow := time.Now()
	ctx := context.Background()

	vlQueryURL := *vlDirectURL
	if vlQueryURL == "" {
		vlQueryURL = *vlURL
	}
	span, ref, err := resolveReference(ctx, *dataEnd, *dataStart, vlQueryURL, wallNow)
	if err != nil {
		fatalf("%v", err)
	}
	if *dataEnd == "" {
		fmt.Println("⚠ --data-end not set: windows end at the wall clock. With seeded (historical) data use --data-end=auto.")
	} else {
		fmt.Printf("✓ workload reference time (data end): %s\n", ref.UTC().Format(time.RFC3339Nano))
	}
	if !span.Start.IsZero() {
		fmt.Printf("✓ seeded data start: %s (no gap longer than %s; jitter and unique windows stay inside the data)\n", span.Start.UTC().Format(time.RFC3339Nano), dataspan.CheckWindow)
	}

	workloads := workload.ByName(workloadNames, ref)
	if len(workloads) == 0 {
		fatalf("no matching workloads (available: small,heavy,long_range,compute,unindexed_scan,high_cardinality,machinery)")
	}
	// Queries that do different work on Loki and the proxy targets in this
	// cache mode, or that --unique-windows cannot keep distinct, are excluded
	// from verification and timing, with the reason printed.
	workloads = withExclusions(workloads, func(q workload.Query) string { return cacheModeExclusion(q, *cacheMode) })
	if *uniqueWindows {
		maxConc := 0
		for _, c := range concurrencies {
			if c > maxConc {
				maxConc = c
			}
		}
		workloads = withExclusions(workloads, func(q workload.Query) string { return uniqueWindowsExclusion(q, span.Start, maxConc) })
	}
	for _, wl := range workloads {
		for _, q := range wl.ExcludedQueries() {
			fmt.Printf("  - %s/%-36s excluded: %s\n", wl.Name, q.Name, q.Excluded)
		}
	}
	// VL-native workloads use LogsQL syntax and VL-specific endpoints.
	vlWorkloads := map[string]workload.Workload{}
	for _, w := range workload.VLByName(workloadNames, ref) {
		vlWorkloads[w.Name] = w
	}

	vlBackendMetrics := *vlMetrics
	if vlBackendMetrics == "" && *vlURL != "" {
		vlBackendMetrics = *vlURL + "/metrics"
	}
	vlDirectMetrics := vlBackendMetrics
	if vlDirectMetrics == "" && *vlDirectURL != "" {
		vlDirectMetrics = *vlDirectURL + "/metrics"
	}
	targets := timedTargets(*cacheMode, targetURLs{
		loki: *lokiURL, proxy: *proxyURL, proxyNoCache: *proxyNoCacheURL, proxyCoalescer: *proxyCoalescerURL,
		proxyPartial: *proxyPartialURL, vlDirect: *vlDirectURL,
		lokiMetrics: *lokiMetrics, proxyMetrics: *proxyMetrics, proxyNoCacheMetrics: *proxyNoCacheMetrics,
		proxyPartialMetrics: *proxyPartialMetrics, vlBackendMetrics: vlBackendMetrics, vlDirectMetrics: vlDirectMetrics,
		skipLoki: *skipLoki, skipProxy: *skipProxy, skipProxyNoCache: *skipProxyNocache,
		skipProxyPartial: *skipProxyPartial, skipVLDirect: *skipVLDirect,
	})
	runVLDirect := false
	names := make([]string, 0, len(targets))
	for _, t := range targets {
		names = append(names, t.name)
		runVLDirect = runVLDirect || t.name == "vl_direct"
	}
	fmt.Printf("✓ cache mode: %s; timed targets: %s\n", *cacheMode, strings.Join(names, ", "))

	meta := report.Meta{CacheMode: *cacheMode, MaxErrorRate: *maxErrorRate}
	meta.NotPublishable = notPublishable(runSettings{
		dataEnd: *dataEnd, waitIngested: *waitIngested, verifyStrict: *verifyStrict, skipLoki: *skipLoki, targets: targets,
		uniqueWindows: *uniqueWindows, dataStart: span.Start,
	})
	if !meta.Publishable() {
		fmt.Print(notPublishableBanner(meta.NotPublishable))
	}

	if !*skipLoki {
		flags, err := fetchLokiCacheFlags(ctx, *lokiURL)
		if err == nil {
			err = checkLokiCacheMode(flags, *cacheMode)
		}
		if err != nil {
			if *verifyStrict {
				fatalf("%v", err)
			}
			fmt.Printf("⚠ %v\n", err)
			meta.NotPublishable = append(meta.NotPublishable, "Loki's results caches were not confirmed to match --cache-mode: "+err.Error())
		} else {
			fmt.Printf("✓ Loki /config matches --cache-mode=%s\n", *cacheMode)
		}
	}

	// Workloads that will actually be timed: the compared Loki/proxy set, plus
	// the VictoriaLogs-native mirrors when that target runs.
	var timed, vlTimed []workload.Workload
	for _, wl := range workloads {
		timed = append(timed, wl.Compared())
		if vw, ok := vlWorkloads[wl.Name]; ok && runVLDirect {
			timed = append(timed, vw)
			vlTimed = append(vlTimed, vw)
		}
	}
	if outside := windowsBeforeData(timed, span); len(outside) > 0 {
		for _, o := range outside {
			fmt.Printf("⚠ %s\n", o)
		}
		if *verifyStrict {
			fatalf("verify-strict: %d query windows read before the seeded data; seed more days (see bench/README.md) so every window, including range-vector lookback, is inside the data", len(outside))
		}
	}
	if w := jitterWarning(*jitter, span); w != "" {
		fmt.Printf("⚠ %s\n", w)
	}

	if *skipLoki && (*waitIngested > 0 || *doVerify || *verifyStrict) {
		fmt.Println("ℹ --skip-loki: entry check and Loki verification skipped")
	}
	if *waitIngested > 0 && !*skipLoki {
		if span.Start.IsZero() || span.End.IsZero() || vlQueryURL == "" {
			fatalf("--wait-ingested needs --vl or --vl-direct and a data span (--data-end=auto, or --data-start and --data-end)")
		}
		fmt.Println("─── Entry Check (Loki vs VictoriaLogs lines per service and window) ───")
		if err := dataspan.WaitEqual(ctx, *lokiURL, vlQueryURL, span, *waitIngested, *waitPoll,
			func(f string, a ...any) { fmt.Printf(f+"\n", a...) }); err != nil {
			fatalf("%v", err)
		}
		fmt.Println()
	}

	if (*doVerify || *verifyStrict) && !*skipLoki {
		proxies := verifyTargets(targets)
		fmt.Printf("─── Verification (Loki vs %s, before timing) ───\n", targetNames(proxies))
		var mismatches strings.Builder
		for _, wl := range workloads {
			results := verify.Run(ctx, *lokiURL, proxies, wl.Compared().Queries, *verifyTimeout)
			for _, r := range results {
				switch {
				case !r.Passed:
					fmt.Printf("  ✗ %-40s  %-16s MISMATCH\n", r.QueryName, r.Target)
				case r.Skipped != "":
					fmt.Printf("  ~ %-40s  %-16s shape not compared: %s\n", r.QueryName, r.Target, r.Skipped)
				default:
					fmt.Printf("  ✓ %-40s  %-16s %s\n", r.QueryName, r.Target, r.LokiShape)
				}
			}
			mismatches.WriteString(verify.Summary(wl.Name, results))
		}
		fmt.Println()
		if mismatches.Len() > 0 {
			fmt.Printf("Mismatches or degraded answers (Loki vs proxy targets):\n%s\n", mismatches.String())
			if *verifyStrict {
				fatalf("verify-strict: the targets do not return the same, non-degraded results for the queries above; timings would not compare equivalent work")
			}
		}
	}
	if (*doVerify || *verifyStrict) && len(vlTimed) > 0 {
		// Native LogsQL is not shape-compared with Loki, but every query must
		// succeed and return data, or vl_direct would time errors or empty scans.
		fmt.Println("─── VictoriaLogs-native check (HTTP 200, not degraded, non-empty) ───")
		var failures strings.Builder
		for _, vw := range vlTimed {
			results := verify.CheckVL(ctx, *vlDirectURL, vw.Queries, *verifyTimeout)
			for _, r := range results {
				if r.Passed {
					fmt.Printf("  ✓ %-40s  %s\n", r.QueryName, r.Skipped)
				} else {
					fmt.Printf("  ✗ %-40s  FAILED\n", r.QueryName)
				}
			}
			failures.WriteString(verify.VLSummary(vw.Name, results))
		}
		fmt.Println()
		if failures.Len() > 0 {
			fmt.Printf("VictoriaLogs-native failures:\n%s\n", failures.String())
			if *verifyStrict {
				fatalf("verify-strict: VictoriaLogs-native queries above failed or returned no data; vl_direct timings would not measure real work")
			}
		}
	}
	if *verifyOnly {
		return
	}

	var records []report.RunRecord
	var gateFailures []string

run:
	for _, fullWL := range workloads {
		// Loki and every proxy target run the same compared query mix.
		wl := fullWL.Compared()
		for _, conc := range concurrencies {
			for _, tgt := range targets {
				queries := wl.Queries
				if tgt.name == "vl_direct" {
					queries = vlWorkloads[wl.Name].Queries
				}
				if len(queries) == 0 {
					continue
				}

				fmt.Printf("\n▶ workload=%-12s  concurrency=%4d  target=%s\n",
					wl.Name, conc, tgt.name)

				// Warm-up (warm mode only): identical for every target, on top of the
				// verification pass that already requested every query from Loki and
				// each proxy target. No cache is flushed.
				if *warmup > 0 && tgt.warmup {
					fmt.Printf("  warming up for %s (concurrency=%d, jitter=%s)...\n", *warmup, conc, *jitter)
					runner.Run(ctx, runner.Config{
						TargetURL:   tgt.url,
						Concurrency: conc,
						Duration:    *warmup,
						Queries:     queries,
						TimeJitter:  *jitter,
						MinTime:     span.Start,
					}) // discard warmup result
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

				result := runner.Run(ctx, runner.Config{
					TargetURL:     tgt.url,
					Concurrency:   conc,
					Duration:      *duration,
					Queries:       queries,
					Verbose:       *verbose,
					TimeJitter:    *jitter,
					MinTime:       span.Start,
					UniqueWindows: *uniqueWindows,
				})

				// Collect CPU profile and capture heap/alloc/goroutine snapshots.
				if pprofBase != "" && cpuCh != nil {
					capturePprof(ctx, filepath.Join(*outputDir, "pprof"), fmt.Sprintf("%s-c%d-%s", wl.Name, conc, tgt.name), pprofBase, *pprofAuthToken, <-cpuCh)
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
				violations := report.Violations(s, *maxErrorRate)
				symbol := "✓"
				if len(violations) > 0 {
					symbol = "✗"
				}
				fmt.Printf("  %s throughput=%.0f req/s  p50=%s  p90=%s  p99=%s  errors=%.2f%%%s  degraded=%.2f%%  bytes=%.1f KB/req\n",
					symbol,
					s.Throughput,
					fmtDur(s.P50), fmtDur(s.P90), fmtDur(s.P99),
					s.ErrorRate*100,
					statusStr,
					s.DegradedRate*100,
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
					Timestamp:      wallNow,
					ReferenceTime:  ref,
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
				if len(violations) > 0 {
					for _, v := range violations {
						gateFailures = append(gateFailures, fmt.Sprintf("%s c=%d %s: %s", wl.Name, conc, tgt.name, v))
					}
					fmt.Printf("  ✗ error gate failed; stopping the benchmark:\n    - %s\n", strings.Join(violations, "\n    - "))
					break run
				}
			}
		}
	}

	meta.NotPublishable = append(meta.NotPublishable, gateFailures...)
	writeOutputs(*outputDir, wallNow, meta, records)
	if len(gateFailures) > 0 {
		fmt.Print(notPublishableBanner(meta.NotPublishable))
		fatalf("a timed run exceeded --max-error-rate %.4f; the raw results above are kept for debugging and are not publishable", *maxErrorRate)
	}
	if !meta.Publishable() {
		fmt.Print(notPublishableBanner(meta.NotPublishable))
	}
}

// writeOutputs prints the text report and writes the JSON and markdown files.
// Non-publishable runs get a -NOT-PUBLISHABLE file suffix and a marked header.
func writeOutputs(dir string, wallNow time.Time, meta report.Meta, records []report.RunRecord) (jsonPath, mdPath string) {
	report.Stamp(records, meta)
	fmt.Printf("\n%s\n", strings.Repeat("═", 90))
	report.WriteText(os.Stdout, meta, records)

	jsonPath, mdPath = outputPaths(dir, wallNow, meta)
	if err := report.WriteJSON(jsonPath, records); err != nil {
		fmt.Fprintf(os.Stderr, "warn: write JSON: %v\n", err)
	} else {
		fmt.Printf("JSON results: %s\n", jsonPath)
	}
	if err := report.WriteMarkdown(mdPath, meta, records); err != nil {
		fmt.Fprintf(os.Stderr, "warn: write markdown: %v\n", err)
	} else {
		fmt.Printf("Markdown results: %s\n", mdPath)
	}
	return jsonPath, mdPath
}

// outputPaths names the result files; non-publishable runs are suffixed so they
// cannot be mistaken for publishable results.
func outputPaths(dir string, wallNow time.Time, meta report.Meta) (string, string) {
	base := "bench-" + wallNow.Format("2006-01-02T15-04-05")
	if !meta.Publishable() {
		base += "-NOT-PUBLISHABLE"
	}
	return filepath.Join(dir, base+".json"), filepath.Join(dir, base+".md")
}

func targetNames(targets []verify.Target) string {
	names := make([]string, 0, len(targets))
	for _, t := range targets {
		names = append(names, t.Name)
	}
	if len(names) == 0 {
		return "no proxy target"
	}
	return strings.Join(names, ", ")
}

// capturePprof saves the CPU profile of a run and captures heap, allocs and
// goroutine profiles right after it.
func capturePprof(ctx context.Context, pprofDir, prefix, pprofBase, authToken string, cpu struct {
	data []byte
	err  error
}) {
	if cpu.err != nil {
		fmt.Fprintf(os.Stderr, "  warn: pprof CPU capture: %v\n", cpu.err)
	} else {
		p := filepath.Join(pprofDir, prefix+"-cpu.pprof")
		if err := benchpprof.Save(cpu.data, p); err != nil {
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
		data, err := kind.fn(ctx, pprofBase, authToken)
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
