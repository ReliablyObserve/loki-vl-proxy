// Package metricscrape scrapes Prometheus text exposition from /metrics endpoints
// and extracts specific resource signals for before/after comparison.
package metricscrape

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ResourceSnapshot holds resource metrics scraped from a /metrics endpoint.
type ResourceSnapshot struct {
	Time              time.Time
	CPUSecondsTotal   float64 // process_cpu_seconds_total or loki_vl_proxy_process_cpu_usage_ratio
	MemRSSBytes       float64 // process_resident_memory_bytes or loki_vl_proxy_process_resident_memory_bytes
	HeapInUseBytes    float64 // go_memstats_heap_inuse_bytes or loki_vl_proxy_go_memstats_heap_inuse_bytes
	Goroutines        float64 // go_goroutines or loki_vl_proxy_go_goroutines
	GCCycles          float64 // go_gc_duration_seconds_count or loki_vl_proxy_go_gc_cycles_total
	NetRxBytesTotal   float64
	NetTxBytesTotal   float64
	DiskReadBytesTotal  float64
	DiskWriteBytesTotal float64
}

// Delta computes resource usage between two snapshots.
type Delta struct {
	CPUSeconds      float64
	MemRSSBytes     float64 // absolute (not delta)
	HeapInUseBytes  float64 // absolute
	Goroutines      float64 // absolute
	GCCycles        float64
	NetRxBytes      float64
	NetTxBytes      float64
	DiskReadBytes   float64
	DiskWriteBytes  float64
}

func (b ResourceSnapshot) Delta(a ResourceSnapshot) Delta {
	return Delta{
		CPUSeconds:      a.CPUSecondsTotal - b.CPUSecondsTotal,
		MemRSSBytes:     a.MemRSSBytes,
		HeapInUseBytes:  a.HeapInUseBytes,
		Goroutines:      a.Goroutines,
		GCCycles:        a.GCCycles - b.GCCycles,
		NetRxBytes:      a.NetRxBytesTotal - b.NetRxBytesTotal,
		NetTxBytes:      a.NetTxBytesTotal - b.NetTxBytesTotal,
		DiskReadBytes:   a.DiskReadBytesTotal - b.DiskReadBytesTotal,
		DiskWriteBytes:  a.DiskWriteBytesTotal - b.DiskWriteBytesTotal,
	}
}

// Scrape fetches /metrics from metricsURL and returns a ResourceSnapshot.
// Understands both standard Go/Prometheus process metrics and loki_vl_proxy_* variants.
func Scrape(metricsURL string) (ResourceSnapshot, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(metricsURL)
	if err != nil {
		return ResourceSnapshot{}, fmt.Errorf("scrape %s: %w", metricsURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ResourceSnapshot{}, fmt.Errorf("read metrics body: %w", err)
	}

	snap := ResourceSnapshot{Time: time.Now()}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := parseLine(line)
		if !ok {
			continue
		}
		switch {
		// CPU — standard Prometheus process metrics
		case name == "process_cpu_seconds_total":
			snap.CPUSecondsTotal = val
		// CPU — proxy-native (loki_vl_proxy_process_cpu_usage_ratio is a ratio, not total — skip for delta)
		case name == "loki_vl_proxy_process_cpu_usage_ratio" && snap.CPUSecondsTotal == 0:
			// Use as fallback only if standard not present
			snap.CPUSecondsTotal = val

		// Memory
		case name == "process_resident_memory_bytes",
			name == "loki_vl_proxy_process_resident_memory_bytes":
			snap.MemRSSBytes = val
		case name == "go_memstats_heap_inuse_bytes",
			name == "loki_vl_proxy_go_memstats_heap_inuse_bytes":
			snap.HeapInUseBytes = val

		// Goroutines
		case name == "go_goroutines",
			name == "loki_vl_proxy_go_goroutines":
			snap.Goroutines = val

		// GC
		case name == "go_gc_duration_seconds_count",
			name == "loki_vl_proxy_go_gc_cycles_total":
			snap.GCCycles = val

		// Network — proxy-native
		case name == "loki_vl_proxy_process_network_receive_bytes_total":
			snap.NetRxBytesTotal = val
		case name == "loki_vl_proxy_process_network_transmit_bytes_total":
			snap.NetTxBytesTotal = val

		// Disk — proxy-native
		case name == "loki_vl_proxy_process_disk_read_bytes_total":
			snap.DiskReadBytesTotal = val
		case name == "loki_vl_proxy_process_disk_written_bytes_total":
			snap.DiskWriteBytesTotal = val
		}
	}
	return snap, nil
}

// parseLine parses "metric_name{labels} value" or "metric_name value".
// Returns the bare metric name (no labels), value, and whether parsing succeeded.
func parseLine(line string) (string, float64, bool) {
	// strip labels
	name := line
	if i := strings.IndexByte(line, '{'); i >= 0 {
		name = line[:i]
		rest := line[strings.LastIndexByte(line, '}')+1:]
		line = name + rest
	}
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil {
		return "", 0, false
	}
	return parts[0], v, true
}
