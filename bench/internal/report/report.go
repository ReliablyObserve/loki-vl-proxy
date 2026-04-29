// Package report formats benchmark results as text tables, markdown, and JSON.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/histogram"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/metricscrape"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/runner"
)

// RunRecord holds a full benchmark run for one target × workload × concurrency.
type RunRecord struct {
	Timestamp    time.Time
	Version      string // optional version tag
	Target       string // "loki" | "proxy"
	TargetURL    string
	WorkloadName string
	Concurrency  int
	Duration     time.Duration
	Result       runner.Result
	ResourceBefore metricscrape.ResourceSnapshot
	ResourceAfter  metricscrape.ResourceSnapshot
	ResourceDelta  metricscrape.Delta
	// VL backend resource deltas captured alongside proxy runs.
	VLBefore metricscrape.ResourceSnapshot
	VLAfter  metricscrape.ResourceSnapshot
	VLDelta  metricscrape.Delta
}

// ComparisonRow holds Loki vs Proxy stats for one metric at one concurrency level.
type ComparisonRow struct {
	Workload    string
	Concurrency int
	Metric      string
	Loki        string
	Proxy       string
	Delta       string // proxy - loki or proxy/loki ratio
}

// WriteText writes a human-readable table to w.
func WriteText(w io.Writer, records []RunRecord) {
	// Group by workload × concurrency: loki vs proxy vs proxy_nocache vs vl_direct.
	type key struct {
		workload    string
		concurrency int
	}
	type quartet struct{ loki, proxy, proxyNocache, vlDirect *RunRecord }
	grouped := make(map[key]*quartet)
	for i := range records {
		r := &records[i]
		k := key{r.WorkloadName, r.Concurrency}
		if _, ok := grouped[k]; !ok {
			grouped[k] = &quartet{}
		}
		switch r.Target {
		case "loki":
			grouped[k].loki = r
		case "proxy":
			grouped[k].proxy = r
		case "proxy_nocache":
			grouped[k].proxyNocache = r
		case "vl_direct":
			grouped[k].vlDirect = r
		}
	}

	// Sort keys.
	keys := make([]key, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].workload != keys[j].workload {
			return keys[i].workload < keys[j].workload
		}
		return keys[i].concurrency < keys[j].concurrency
	})

	for _, k := range keys {
		p := grouped[k]
		sep90 := strings.Repeat("═", 110)
		sep70 := strings.Repeat("─", 110)
		fmt.Fprintf(w, "\n%s\n", sep90)
		fmt.Fprintf(w, "  Workload: %-20s  Concurrency: %d clients\n", k.workload, k.concurrency)
		fmt.Fprintf(w, "%s\n", sep90)
		fmt.Fprintf(w, "%-32s  %-18s  %-20s  %-20s  %-18s  %s\n",
			"Metric", "Loki (direct)", "VL+Proxy (warm)", "VL+Proxy (cold)", "VL (native)", "Δ warm vs Loki")
		fmt.Fprintf(w, "%s\n", sep70)

		printRow := func(metric, lv, pv, ncv, vv, delta string) {
			fmt.Fprintf(w, "%-32s  %-18s  %-20s  %-20s  %-18s  %s\n", metric, lv, pv, ncv, vv, delta)
		}

		fmtDur := func(d time.Duration) string {
			if d < time.Millisecond {
				return fmt.Sprintf("%.1f µs", float64(d.Microseconds()))
			}
			return fmt.Sprintf("%.1f ms", float64(d.Milliseconds()))
		}
		fmtRate := func(r float64) string { return fmt.Sprintf("%.0f req/s", r) }
		fmtBytes := func(b float64) string {
			if b > 1e9 {
				return fmt.Sprintf("%.2f GB/s", b/1e9)
			}
			if b > 1e6 {
				return fmt.Sprintf("%.2f MB/s", b/1e6)
			}
			return fmt.Sprintf("%.2f KB/s", b/1e3)
		}
		fmtPct := func(v float64) string { return fmt.Sprintf("%.2f%%", v*100) }
		fmtMB := func(b float64) string { return fmt.Sprintf("%.1f MB", b/1e6) }
		na := "—"

		durRatio := func(l, p time.Duration) string {
			if l == 0 || p == 0 {
				return na
			}
			return fmt.Sprintf("%.2fx", float64(p)/float64(l))
		}
		rateRatio := func(l, p float64) string {
			if l == 0 || p == 0 {
				return na
			}
			sign := "+"
			if p < l {
				sign = ""
			}
			return fmt.Sprintf("%s%.0f (%.2fx)", sign, p-l, p/l)
		}

		lStats, pStats, ncStats, vStats := histogram.Stats{}, histogram.Stats{}, histogram.Stats{}, histogram.Stats{}
		if p.loki != nil {
			lStats = p.loki.Result.Overall
		}
		if p.proxy != nil {
			pStats = p.proxy.Result.Overall
		}
		if p.proxyNocache != nil {
			ncStats = p.proxyNocache.Result.Overall
		}
		if p.vlDirect != nil {
			vStats = p.vlDirect.Result.Overall
		}

		col := func(r *RunRecord, d time.Duration) string {
			if r == nil {
				return na
			}
			return fmtDur(d)
		}
		colRate := func(r *RunRecord, v float64) string {
			if r == nil {
				return na
			}
			return fmtRate(v)
		}

		// Throughput
		printRow("Throughput",
			colRate(p.loki, lStats.Throughput),
			colRate(p.proxy, pStats.Throughput),
			colRate(p.proxyNocache, ncStats.Throughput),
			colRate(p.vlDirect, vStats.Throughput),
			func() string {
				if p.loki == nil {
					return na
				}
				parts := []string{}
				if p.proxy != nil {
					parts = append(parts, "warm:"+rateRatio(lStats.Throughput, pStats.Throughput))
				}
				if p.proxyNocache != nil {
					parts = append(parts, "cold:"+rateRatio(lStats.Throughput, ncStats.Throughput))
				}
				if len(parts) == 0 {
					return na
				}
				return strings.Join(parts, "  ")
			}())

		// Latencies
		for _, row := range []struct {
			label          string
			lv, pv, ncv, vv time.Duration
		}{
			{"P50 Latency", lStats.P50, pStats.P50, ncStats.P50, vStats.P50},
			{"P90 Latency", lStats.P90, pStats.P90, ncStats.P90, vStats.P90},
			{"P99 Latency", lStats.P99, pStats.P99, ncStats.P99, vStats.P99},
			{"P99.9 Latency", lStats.P999, pStats.P999, ncStats.P999, vStats.P999},
			{"Max Latency", lStats.Max, pStats.Max, ncStats.Max, vStats.Max},
		} {
			delta := na
			if p.loki != nil {
				parts := []string{}
				if p.proxy != nil {
					parts = append(parts, "warm:"+durRatio(row.lv, row.pv))
				}
				if p.proxyNocache != nil {
					parts = append(parts, "cold:"+durRatio(row.lv, row.ncv))
				}
				if len(parts) > 0 {
					delta = strings.Join(parts, "  ")
				}
			}
			printRow(row.label,
				col(p.loki, row.lv),
				col(p.proxy, row.pv),
				col(p.proxyNocache, row.ncv),
				col(p.vlDirect, row.vv),
				delta)
		}

		// Error rate
		printRow("Error Rate",
			func() string {
				if p.loki == nil {
					return na
				}
				return fmtPct(lStats.ErrorRate)
			}(),
			func() string {
				if p.proxy == nil {
					return na
				}
				return fmtPct(pStats.ErrorRate)
			}(),
			func() string {
				if p.proxyNocache == nil {
					return na
				}
				return fmtPct(ncStats.ErrorRate)
			}(),
			func() string {
				if p.vlDirect == nil {
					return na
				}
				return fmtPct(vStats.ErrorRate)
			}(),
			na)

		// Network bandwidth
		printRow("Response Bytes/s",
			func() string {
				if p.loki == nil {
					return na
				}
				return fmtBytes(lStats.BytesPerSec)
			}(),
			func() string {
				if p.proxy == nil {
					return na
				}
				return fmtBytes(pStats.BytesPerSec)
			}(),
			func() string {
				if p.proxyNocache == nil {
					return na
				}
				return fmtBytes(ncStats.BytesPerSec)
			}(),
			func() string {
				if p.vlDirect == nil {
					return na
				}
				return fmtBytes(vStats.BytesPerSec)
			}(),
			na)

		// Resource deltas
		fmt.Fprintf(w, "%s\n", sep70)
		fmt.Fprintf(w, "  Resource Usage During Run\n")
		fmt.Fprintf(w, "%s\n", sep70)

		printRow("CPU consumed",
			func() string {
				if p.loki == nil {
					return na
				}
				return fmt.Sprintf("%.3f cpu·s", p.loki.ResourceDelta.CPUSeconds)
			}(),
			func() string {
				if p.proxy == nil {
					return na
				}
				return fmt.Sprintf("%.3f cpu·s", p.proxy.ResourceDelta.CPUSeconds)
			}(),
			func() string {
				if p.proxyNocache == nil {
					return na
				}
				return fmt.Sprintf("%.3f cpu·s", p.proxyNocache.ResourceDelta.CPUSeconds)
			}(),
			func() string {
				if p.vlDirect == nil {
					return na
				}
				return fmt.Sprintf("%.3f cpu·s", p.vlDirect.ResourceDelta.CPUSeconds)
			}(),
			na)

		printRow("RSS Memory",
			func() string {
				if p.loki == nil {
					return na
				}
				return fmtMB(p.loki.ResourceDelta.MemRSSBytes)
			}(),
			func() string {
				if p.proxy == nil {
					return na
				}
				return fmtMB(p.proxy.ResourceDelta.MemRSSBytes)
			}(),
			func() string {
				if p.proxyNocache == nil {
					return na
				}
				return fmtMB(p.proxyNocache.ResourceDelta.MemRSSBytes)
			}(),
			func() string {
				if p.vlDirect == nil {
					return na
				}
				return fmtMB(p.vlDirect.ResourceDelta.MemRSSBytes)
			}(),
			na)

		// VL backend resource breakdown (captured alongside proxy runs).
		if p.proxy != nil && p.proxy.VLDelta.CPUSeconds > 0 {
			fmt.Fprintf(w, "%s\n", sep70)
			fmt.Fprintf(w, "  VictoriaLogs Backend (during proxy run)\n")
			fmt.Fprintf(w, "%s\n", sep70)
			printRow("VL CPU (via proxy)", na, fmt.Sprintf("%.3f cpu·s", p.proxy.VLDelta.CPUSeconds), na, na, na)
			printRow("VL RSS (via proxy)", na, fmtMB(p.proxy.VLDelta.MemRSSBytes), na, na, na)

			// Combined proxy+VL vs Loki.
			if p.loki != nil {
				combinedCPU := p.proxy.ResourceDelta.CPUSeconds + p.proxy.VLDelta.CPUSeconds
				combinedRSS := p.proxy.ResourceDelta.MemRSSBytes + p.proxy.VLDelta.MemRSSBytes
				lCPUv := p.loki.ResourceDelta.CPUSeconds
				lRSS := p.loki.ResourceDelta.MemRSSBytes
				// No-cache combined (proxy_nocache process + VL backend from VLDelta — use proxy VLDelta as approximation).
				ncCombinedCPU, ncCombinedRSS := na, na
				if p.proxyNocache != nil {
					ncc := p.proxyNocache.ResourceDelta.CPUSeconds + p.proxy.VLDelta.CPUSeconds
					ncr := p.proxyNocache.ResourceDelta.MemRSSBytes + p.proxy.VLDelta.MemRSSBytes
					ncCombinedCPU = fmt.Sprintf("%.3f cpu·s", ncc)
					ncCombinedRSS = fmtMB(ncr)
				}
				fmt.Fprintf(w, "%s\n", sep70)
				fmt.Fprintf(w, "  Summary: Loki vs VL+Proxy combined vs VL Native\n")
				fmt.Fprintf(w, "%s\n", sep70)
				vlNativeCPU := na
				vlNativeRSS := na
				if p.vlDirect != nil {
					vlNativeCPU = fmt.Sprintf("%.3f cpu·s", p.vlDirect.ResourceDelta.CPUSeconds)
					vlNativeRSS = fmtMB(p.vlDirect.ResourceDelta.MemRSSBytes)
				}
				printRow("Total CPU",
					fmt.Sprintf("%.3f cpu·s", lCPUv),
					fmt.Sprintf("%.3f cpu·s", combinedCPU),
					ncCombinedCPU,
					vlNativeCPU,
					func() string {
						if lCPUv == 0 {
							return na
						}
						return fmt.Sprintf("proxy+vl=%.2fx loki", lCPUv/combinedCPU)
					}())
				printRow("Total RSS",
					fmtMB(lRSS),
					fmtMB(combinedRSS),
					ncCombinedRSS,
					vlNativeRSS,
					func() string {
						if lRSS == 0 {
							return na
						}
						return fmt.Sprintf("proxy+vl=%.2fx loki", lRSS/combinedRSS)
					}())
			}
		}

		// Per-query breakdown for proxy, proxy_nocache, and VL-direct side by side.
		hasProxyQ := p.proxy != nil && len(p.proxy.Result.ByQuery) > 0
		hasNoCacheQ := p.proxyNocache != nil && len(p.proxyNocache.Result.ByQuery) > 0
		hasVLQ := p.vlDirect != nil && len(p.vlDirect.Result.ByQuery) > 0
		if hasProxyQ || hasNoCacheQ || hasVLQ {
			fmt.Fprintf(w, "%s\n", sep70)
			fmt.Fprintf(w, "  Per-Query Breakdown\n")
			fmt.Fprintf(w, "%s\n", sep70)
			fmt.Fprintf(w, "  %-36s  %-26s  %-26s  %-24s\n", "Query", "Proxy warm (P50/P99/rps)", "Proxy cold (P50/P99/rps)", "VL Native (P50/P99/rps)")
			src := p.proxy
			if src == nil {
				src = p.proxyNocache
			}
			if src != nil {
				names := make([]string, 0, len(src.Result.ByQuery))
				for n := range src.Result.ByQuery {
					names = append(names, n)
				}
				sort.Strings(names)
				for _, n := range names {
					warmCol, coldCol, vlCol := na, na, na
					if hasProxyQ {
						if s := p.proxy.Result.ByQuery[n]; s != nil && s.Count > 0 {
							warmCol = fmt.Sprintf("%s/%s/%.0f", fmtDur(s.P50), fmtDur(s.P99), s.Throughput)
						}
					}
					if hasNoCacheQ {
						if s := p.proxyNocache.Result.ByQuery[n]; s != nil && s.Count > 0 {
							coldCol = fmt.Sprintf("%s/%s/%.0f", fmtDur(s.P50), fmtDur(s.P99), s.Throughput)
						}
					}
					if hasVLQ {
						if vs := p.vlDirect.Result.ByQuery[n]; vs != nil && vs.Count > 0 {
							vlCol = fmt.Sprintf("%s/%s/%.0f", fmtDur(vs.P50), fmtDur(vs.P99), vs.Throughput)
						}
					}
					fmt.Fprintf(w, "  %-36s  %-26s  %-26s  %-24s\n", n, warmCol, coldCol, vlCol)
				}
			}
		}
	}
	fmt.Fprintln(w)
}

// WriteJSON writes all records as a JSON array to path.
func WriteJSON(path string, records []RunRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}

// WriteMarkdown writes a markdown summary table to path.
func WriteMarkdown(path string, records []RunRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "# loki-vl-proxy Read Performance Benchmark\n\n")
	fmt.Fprintf(f, "Generated: %s\n\n", time.Now().Format(time.RFC3339))

	// Group by workload × concurrency, emit markdown tables.
	type key struct {
		workload    string
		concurrency int
	}
	type quartet struct{ loki, proxy, proxyNocache, vlDirect *RunRecord }
	grouped := make(map[key]*quartet)
	for i := range records {
		r := &records[i]
		k := key{r.WorkloadName, r.Concurrency}
		if _, ok := grouped[k]; !ok {
			grouped[k] = &quartet{}
		}
		switch r.Target {
		case "loki":
			grouped[k].loki = r
		case "proxy":
			grouped[k].proxy = r
		case "proxy_nocache":
			grouped[k].proxyNocache = r
		case "vl_direct":
			grouped[k].vlDirect = r
		}
	}

	keys := make([]key, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].workload != keys[j].workload {
			return keys[i].workload < keys[j].workload
		}
		return keys[i].concurrency < keys[j].concurrency
	})

	fmtDur := func(d time.Duration) string {
		if d < time.Millisecond {
			return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
		}
		return fmt.Sprintf("%.0fms", float64(d.Milliseconds()))
	}

	for _, k := range keys {
		p := grouped[k]
		fmt.Fprintf(f, "## %s — %d clients\n\n", k.workload, k.concurrency)
		fmt.Fprintf(f, "| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |\n")
		fmt.Fprintf(f, "|--------|:-------------:|:---------------:|:---------------:|:-----------:|:--------------:|:--------------:|\n")

		na := "—"
		ls, ps, ncs, vs := histogram.Stats{}, histogram.Stats{}, histogram.Stats{}, histogram.Stats{}
		if p.loki != nil {
			ls = p.loki.Result.Overall
		}
		if p.proxy != nil {
			ps = p.proxy.Result.Overall
		}
		if p.proxyNocache != nil {
			ncs = p.proxyNocache.Result.Overall
		}
		if p.vlDirect != nil {
			vs = p.vlDirect.Result.Overall
		}

		row := func(name, l, pw, pc, vv, dw, dc string) {
			fmt.Fprintf(f, "| %s | %s | %s | %s | %s | %s | %s |\n", name, l, pw, pc, vv, dw, dc)
		}
		maybeRate := func(s histogram.Stats) string {
			if s.Count == 0 {
				return na
			}
			return fmt.Sprintf("%.0f req/s", s.Throughput)
		}
		maybeDur := func(d time.Duration) string {
			if d == 0 {
				return na
			}
			return fmtDur(d)
		}
		maybeRatio := func(l, q time.Duration) string {
			if l == 0 || q == 0 {
				return na
			}
			return fmt.Sprintf("%.2fx", float64(q)/float64(l))
		}
		maybeRateRatio := func(l, q float64) string {
			if l == 0 || q == 0 {
				return na
			}
			return fmt.Sprintf("%.2fx", q/l)
		}

		row("Throughput",
			maybeRate(ls), maybeRate(ps), maybeRate(ncs), maybeRate(vs),
			maybeRateRatio(ls.Throughput, ps.Throughput),
			maybeRateRatio(ls.Throughput, ncs.Throughput))
		row("P50", maybeDur(ls.P50), maybeDur(ps.P50), maybeDur(ncs.P50), maybeDur(vs.P50),
			maybeRatio(ls.P50, ps.P50), maybeRatio(ls.P50, ncs.P50))
		row("P90", maybeDur(ls.P90), maybeDur(ps.P90), maybeDur(ncs.P90), maybeDur(vs.P90),
			maybeRatio(ls.P90, ps.P90), maybeRatio(ls.P90, ncs.P90))
		row("P99", maybeDur(ls.P99), maybeDur(ps.P99), maybeDur(ncs.P99), maybeDur(vs.P99),
			maybeRatio(ls.P99, ps.P99), maybeRatio(ls.P99, ncs.P99))
		row("Error Rate",
			fmt.Sprintf("%.2f%%", ls.ErrorRate*100),
			fmt.Sprintf("%.2f%%", ps.ErrorRate*100),
			func() string {
				if p.proxyNocache == nil {
					return na
				}
				return fmt.Sprintf("%.2f%%", ncs.ErrorRate*100)
			}(),
			fmt.Sprintf("%.2f%%", vs.ErrorRate*100),
			na, na)

		// Resource rows.
		lCPUStr, pCPUStr, ncCPUStr, vCPUStr := na, na, na, na
		lRSSStr, pRSSStr, ncRSSStr, vRSSStr := na, na, na, na
		if p.loki != nil {
			lCPUStr = fmt.Sprintf("%.3f s", p.loki.ResourceDelta.CPUSeconds)
			lRSSStr = fmt.Sprintf("%.0f MB", p.loki.ResourceDelta.MemRSSBytes/1e6)
		}
		if p.proxy != nil {
			pCPUStr = fmt.Sprintf("%.3f s", p.proxy.ResourceDelta.CPUSeconds)
			pRSSStr = fmt.Sprintf("%.0f MB", p.proxy.ResourceDelta.MemRSSBytes/1e6)
		}
		if p.proxyNocache != nil {
			ncCPUStr = fmt.Sprintf("%.3f s", p.proxyNocache.ResourceDelta.CPUSeconds)
			ncRSSStr = fmt.Sprintf("%.0f MB", p.proxyNocache.ResourceDelta.MemRSSBytes/1e6)
		}
		if p.vlDirect != nil {
			vCPUStr = fmt.Sprintf("%.3f s", p.vlDirect.ResourceDelta.CPUSeconds)
			vRSSStr = fmt.Sprintf("%.0f MB", p.vlDirect.ResourceDelta.MemRSSBytes/1e6)
		}
		row("CPU consumed", lCPUStr, pCPUStr, ncCPUStr, vCPUStr, na, na)
		row("RSS Memory", lRSSStr, pRSSStr, ncRSSStr, vRSSStr, na, na)

		// Combined proxy+VL row when VL backend metrics are available.
		if p.proxy != nil && p.proxy.VLDelta.CPUSeconds > 0 && p.loki != nil {
			combinedCPU := p.proxy.ResourceDelta.CPUSeconds + p.proxy.VLDelta.CPUSeconds
			combinedRSS := p.proxy.ResourceDelta.MemRSSBytes + p.proxy.VLDelta.MemRSSBytes
			lCPUv := p.loki.ResourceDelta.CPUSeconds
			lRSSv := p.loki.ResourceDelta.MemRSSBytes
			row("CPU (proxy+VL combined)",
				fmt.Sprintf("%.3f s", lCPUv),
				fmt.Sprintf("%.3f s", combinedCPU),
				na, na,
				func() string {
					if lCPUv == 0 {
						return na
					}
					return fmt.Sprintf("%.2fx less", lCPUv/combinedCPU)
				}(),
				na)
			row("RSS (proxy+VL combined)",
				fmt.Sprintf("%.0f MB", lRSSv/1e6),
				fmt.Sprintf("%.0f MB", combinedRSS/1e6),
				na, na,
				func() string {
					if lRSSv == 0 {
						return na
					}
					return fmt.Sprintf("%.2fx less", lRSSv/combinedRSS)
				}(),
				na)
		}
		fmt.Fprintln(f)
	}

	return nil
}
