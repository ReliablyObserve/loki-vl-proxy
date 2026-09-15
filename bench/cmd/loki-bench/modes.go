package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/runner"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/verify"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

// Cache modes. A comparison is only equal when Loki and the proxy targets run
// with the same kind of response caching.
const (
	// cacheModeWarm compares Loki with its results caches on against the cached
	// proxy (and the partial-cache variant). Both get the same warm-up: the
	// verification pass and --warmup before every run; no cache is flushed.
	cacheModeWarm = "warm"
	// cacheModeCold compares Loki with every results cache off against the
	// proxies without a response cache (proxy_nocache, proxy_coalescer). No
	// target gets a warm-up run.
	cacheModeCold = "cold"
)

// lokiResultsCacheKeys are Loki's query_range response cache switches.
var lokiResultsCacheKeys = []string{
	"cache_results",
	"cache_index_stats_results",
	"cache_volume_results",
	"cache_instant_metric_results",
	"cache_series_results",
	"cache_label_results",
}

// benchTarget is one timed endpoint.
type benchTarget struct {
	name       string
	url        string
	metricsURL string
	vlMetrics  string
	// proxy marks loki-vl-proxy targets, which are verified against Loki.
	proxy bool
	// warmup runs --warmup before each timed run.
	warmup bool
}

// targetURLs carries the endpoint flags that decide which targets run.
type targetURLs struct {
	loki, proxy, proxyNoCache, proxyCoalescer, proxyPartial, vlDirect                     string
	lokiMetrics, proxyMetrics, proxyNoCacheMetrics, proxyPartialMetrics, vlBackendMetrics string
	vlDirectMetrics                                                                       string
	skipLoki, skipProxy, skipProxyNoCache, skipProxyPartial, skipVLDirect                 bool
}

// timedTargets returns the targets timed in a cache mode, in run order.
func timedTargets(mode string, u targetURLs) []benchTarget {
	warm := mode == cacheModeWarm
	all := []struct {
		t   benchTarget
		run bool
	}{
		{benchTarget{name: "loki", url: u.loki, metricsURL: u.lokiMetrics, warmup: warm}, !u.skipLoki},
		{benchTarget{name: "proxy", url: u.proxy, metricsURL: u.proxyMetrics, vlMetrics: u.vlBackendMetrics, proxy: true, warmup: true}, warm && !u.skipProxy},
		{benchTarget{name: "proxy_coalescer", url: u.proxyCoalescer, vlMetrics: u.vlBackendMetrics, proxy: true}, !warm},
		{benchTarget{name: "proxy_nocache", url: u.proxyNoCache, metricsURL: u.proxyNoCacheMetrics, vlMetrics: u.vlBackendMetrics, proxy: true}, !warm && !u.skipProxyNoCache},
		{benchTarget{name: "proxy_partial", url: u.proxyPartial, metricsURL: u.proxyPartialMetrics, vlMetrics: u.vlBackendMetrics, proxy: true, warmup: true}, warm && !u.skipProxyPartial},
		{benchTarget{name: "vl_direct", url: u.vlDirect, metricsURL: u.vlDirectMetrics, warmup: warm}, !u.skipVLDirect},
	}
	var out []benchTarget
	for _, c := range all {
		if c.run && c.t.url != "" {
			out = append(out, c.t)
		}
	}
	return out
}

// verifyTargets lists every timed proxy target: each one is verified against
// Loki before any timing.
func verifyTargets(targets []benchTarget) []verify.Target {
	var out []verify.Target
	for _, t := range targets {
		if t.proxy {
			out = append(out, verify.Target{Name: t.name, URL: t.url})
		}
	}
	return out
}

// runSettings are the flags that decide whether the numbers can be published.
type runSettings struct {
	dataEnd      string
	waitIngested time.Duration
	verifyStrict bool
	skipLoki     bool
	targets      []benchTarget
	// uniqueWindows needs dataStart to keep shifted windows inside the data.
	uniqueWindows bool
	dataStart     time.Time
}

// notPublishable lists why a run's numbers cannot be published: every number
// needs a pinned data window, a passed entry check, strict verification and a
// Loki target to compare with.
func notPublishable(s runSettings) []string {
	var reasons []string
	if s.skipLoki {
		reasons = append(reasons, "--skip-loki: there is no Loki result to compare with")
	}
	if len(verifyTargets(s.targets)) == 0 {
		reasons = append(reasons, "no proxy target is timed")
	}
	if !s.verifyStrict {
		reasons = append(reasons, "--verify-strict not set: results were not verified equal and non-degraded before timing")
	}
	if s.waitIngested <= 0 {
		reasons = append(reasons, "--wait-ingested not set: Loki and VictoriaLogs were not checked to hold the same lines")
	}
	if s.uniqueWindows && s.dataStart.IsZero() {
		reasons = append(reasons, "--unique-windows without a data start: shifted windows may read before the seeded data")
	}
	if strings.TrimSpace(s.dataEnd) == "" {
		reasons = append(reasons, "--data-end not set: windows follow the wall clock instead of the seeded data")
	}
	return reasons
}

// notPublishableBanner is printed when a run starts or ends non-publishable.
func notPublishableBanner(reasons []string) string {
	line := strings.Repeat("!", 90)
	return fmt.Sprintf("%s\n!! NOT PUBLISHABLE: these numbers must not be published or compared\n!!   - %s\n%s\n",
		line, strings.Join(reasons, "\n!!   - "), line)
}

// fetchLokiCacheFlags reads Loki's running configuration (GET /config) and
// returns its query_range results-cache switches.
func fetchLokiCacheFlags(ctx context.Context, lokiURL string) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, lokiURL+"/config", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s/config: HTTP %d", lokiURL, resp.StatusCode)
	}
	return parseLokiCacheFlags(resp.Body)
}

// parseLokiCacheFlags extracts the top-level `query_range:` boolean keys from
// Loki's YAML configuration dump.
func parseLokiCacheFlags(r io.Reader) (map[string]bool, error) {
	flags := map[string]bool{}
	inQueryRange := false
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line != "" && line[0] != ' ' && line[0] != '#' {
			inQueryRange = strings.TrimSpace(line) == "query_range:"
			continue
		}
		if !inQueryRange || !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") {
			continue
		}
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(value) {
		case "true":
			flags[key] = true
		case "false":
			flags[key] = false
		}
	}
	return flags, sc.Err()
}

// checkLokiCacheMode fails when Loki's results caches do not match the mode:
// warm needs cache_results on; cold needs every results cache off.
func checkLokiCacheMode(flags map[string]bool, mode string) error {
	switch mode {
	case cacheModeWarm:
		// cache_instant_metric_results stays at Loki's default (off): the proxy
		// does not cache instant queries across the TTL of a benchmark run either.
		var off []string
		for _, k := range lokiResultsCacheKeys {
			if k != "cache_instant_metric_results" && !flags[k] {
				off = append(off, k)
			}
		}
		if len(off) > 0 {
			return fmt.Errorf("--cache-mode=warm needs Loki's results caches on, but query_range %s are off or not reported (use --cache-mode=cold for a cache-less Loki)", strings.Join(off, ", "))
		}
	case cacheModeCold:
		var on []string
		for _, k := range lokiResultsCacheKeys {
			v, ok := flags[k]
			if !ok {
				return fmt.Errorf("--cache-mode=cold: Loki /config does not report query_range.%s; cannot confirm its results caches are off", k)
			}
			if v {
				on = append(on, k)
			}
		}
		if len(on) > 0 {
			return fmt.Errorf("--cache-mode=cold needs every Loki results cache off, but query_range %s are on; start Loki with test/e2e-compat/loki-bench-nocache-config.yaml (see bench/README.md)", strings.Join(on, ", "))
		}
	default:
		return fmt.Errorf("--cache-mode must be %q or %q, got %q", cacheModeWarm, cacheModeCold, mode)
	}
	return nil
}

// cacheModeExclusion returns why a query does different work on Loki and on
// the proxy targets of a cache mode, or "":
//   - warm: Loki 3.7 caches metric, series and label results but not log lines
//     (its log results cache stores only empty answers), detected fields or
//     patterns, while the cached proxy serves all of them from its response and
//     query_range window caches.
//   - cold: a proxy started with -cache-disabled still keeps a 30s in-process
//     stream-field-names cache behind /labels and /label/{name}/values, while
//     Loki runs with cache_label_results off.
func cacheModeExclusion(q workload.Query, mode string) string {
	isMetadata := strings.HasSuffix(q.Path, "/labels") || (strings.Contains(q.Path, "/label/") && strings.HasSuffix(q.Path, "/values"))
	switch mode {
	case cacheModeWarm:
		switch {
		case (strings.HasSuffix(q.Path, "/query_range") || strings.HasSuffix(q.Path, "/query")) && workload.Lookback(q.Params.Get("query")) == 0:
			return "warm mode: Loki does not cache non-empty log query results while the proxy caches them; compared in --cache-mode=cold"
		case strings.HasSuffix(q.Path, "/detected_fields"), strings.HasSuffix(q.Path, "/patterns"):
			return "warm mode: Loki does not cache detected fields or patterns while the proxy caches them; compared in --cache-mode=cold"
		}
	case cacheModeCold:
		if isMetadata {
			return "cold mode: the proxy keeps a 30s stream-field-names cache for labels and label values even with -cache-disabled; compared in --cache-mode=warm"
		}
	}
	return ""
}

// withExclusions returns copies of the workloads where every query not
// already excluded gets the reason reason(q) returns, when non-empty.
func withExclusions(workloads []workload.Workload, reason func(workload.Query) string) []workload.Workload {
	out := make([]workload.Workload, len(workloads))
	for i, w := range workloads {
		queries := make([]workload.Query, len(w.Queries))
		for j, q := range w.Queries {
			if q.Excluded == "" {
				q.Excluded = reason(q)
			}
			queries[j] = q
		}
		out[i] = workload.Workload{Name: w.Name, Queries: queries}
	}
	return out
}

// uniqueWindowsExclusion excludes a query that has fewer distinct whole-step
// windows inside the data than the highest concurrency: --unique-windows would
// repeat its windows, so coalescing could answer one request from another.
func uniqueWindowsExclusion(q workload.Query, minTime time.Time, maxConcurrency int) string {
	if n, bounded := runner.DistinctWindows(q.Params, minTime); bounded && n < int64(maxConcurrency) {
		return fmt.Sprintf("--unique-windows: only %d distinct whole-step windows fit inside the seeded data, fewer than %d clients", n, maxConcurrency)
	}
	return ""
}
