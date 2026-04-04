package metrics

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SystemMetrics collects OS-level metrics from /proc for standalone observability.
// Only available on Linux; returns empty on other platforms.
type SystemMetrics struct {
	mu       sync.Mutex
	prevCPU  cpuStat
	prevTime time.Time
}

type cpuStat struct {
	user, nice, system, idle, iowait, irq, softirq, steal float64
}

func NewSystemMetrics() *SystemMetrics {
	sm := &SystemMetrics{prevTime: time.Now()}
	sm.prevCPU, _ = readCPUStat()
	return sm
}

// WritePrometheus writes system metrics in Prometheus text exposition format.
func (sm *SystemMetrics) WritePrometheus(sb *strings.Builder) {
	if runtime.GOOS != "linux" {
		// Non-Linux: only report Go runtime metrics
		sb.WriteString("# HELP process_resident_memory_bytes Resident memory size.\n")
		sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Fprintf(sb, "process_resident_memory_bytes %d\n", m.Sys)
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// CPU usage
	now := time.Now()
	cur, err := readCPUStat()
	if err == nil {
		elapsed := now.Sub(sm.prevTime).Seconds()
		if elapsed > 0 {
			totalPrev := sm.prevCPU.user + sm.prevCPU.nice + sm.prevCPU.system + sm.prevCPU.idle + sm.prevCPU.iowait + sm.prevCPU.irq + sm.prevCPU.softirq + sm.prevCPU.steal
			totalCur := cur.user + cur.nice + cur.system + cur.idle + cur.iowait + cur.irq + cur.softirq + cur.steal
			totalDiff := totalCur - totalPrev
			if totalDiff > 0 {
				userPct := (cur.user + cur.nice - sm.prevCPU.user - sm.prevCPU.nice) / totalDiff
				sysPct := (cur.system - sm.prevCPU.system) / totalDiff
				iowaitPct := (cur.iowait - sm.prevCPU.iowait) / totalDiff

				sb.WriteString("# HELP node_cpu_usage_ratio CPU usage ratio (0-1).\n")
				sb.WriteString("# TYPE node_cpu_usage_ratio gauge\n")
				fmt.Fprintf(sb, "node_cpu_usage_ratio{mode=\"user\"} %g\n", userPct)
				fmt.Fprintf(sb, "node_cpu_usage_ratio{mode=\"system\"} %g\n", sysPct)
				fmt.Fprintf(sb, "node_cpu_usage_ratio{mode=\"iowait\"} %g\n", iowaitPct)
			}
		}
		sm.prevCPU = cur
		sm.prevTime = now
	}

	// Memory from /proc/meminfo
	memTotal, memAvail, memFree := readMemInfo()
	if memTotal > 0 {
		sb.WriteString("# HELP node_memory_total_bytes Total memory.\n")
		sb.WriteString("# TYPE node_memory_total_bytes gauge\n")
		fmt.Fprintf(sb, "node_memory_total_bytes %d\n", memTotal)

		sb.WriteString("# HELP node_memory_available_bytes Available memory.\n")
		sb.WriteString("# TYPE node_memory_available_bytes gauge\n")
		fmt.Fprintf(sb, "node_memory_available_bytes %d\n", memAvail)

		sb.WriteString("# HELP node_memory_free_bytes Free memory.\n")
		sb.WriteString("# TYPE node_memory_free_bytes gauge\n")
		fmt.Fprintf(sb, "node_memory_free_bytes %d\n", memFree)

		used := memTotal - memAvail
		sb.WriteString("# HELP node_memory_usage_ratio Memory usage ratio (0-1).\n")
		sb.WriteString("# TYPE node_memory_usage_ratio gauge\n")
		fmt.Fprintf(sb, "node_memory_usage_ratio %g\n", float64(used)/float64(memTotal))
	}

	// Process RSS from /proc/self/status
	rss := readProcessRSS()
	if rss > 0 {
		sb.WriteString("# HELP process_resident_memory_bytes Process RSS.\n")
		sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
		fmt.Fprintf(sb, "process_resident_memory_bytes %d\n", rss)
	}

	// Disk IO from /proc/diskstats
	readBytes, writeBytes := readDiskIO()
	sb.WriteString("# HELP node_disk_read_bytes_total Disk read bytes.\n")
	sb.WriteString("# TYPE node_disk_read_bytes_total counter\n")
	fmt.Fprintf(sb, "node_disk_read_bytes_total %d\n", readBytes)
	sb.WriteString("# HELP node_disk_written_bytes_total Disk written bytes.\n")
	sb.WriteString("# TYPE node_disk_written_bytes_total counter\n")
	fmt.Fprintf(sb, "node_disk_written_bytes_total %d\n", writeBytes)

	// Network IO from /proc/net/dev
	rxBytes, txBytes := readNetIO()
	sb.WriteString("# HELP node_network_receive_bytes_total Network receive bytes.\n")
	sb.WriteString("# TYPE node_network_receive_bytes_total counter\n")
	fmt.Fprintf(sb, "node_network_receive_bytes_total %d\n", rxBytes)
	sb.WriteString("# HELP node_network_transmit_bytes_total Network transmit bytes.\n")
	sb.WriteString("# TYPE node_network_transmit_bytes_total counter\n")
	fmt.Fprintf(sb, "node_network_transmit_bytes_total %d\n", txBytes)

	// PSI (Pressure Stall Information) from /proc/pressure/*
	for _, resource := range []string{"cpu", "memory", "io"} {
		some10, some60, some300, full10, full60, full300 := readPSI(resource)
		if some10 >= 0 {
			sb.WriteString(fmt.Sprintf("# HELP node_pressure_%s_some_ratio PSI some pressure (%s).\n", resource, resource))
			sb.WriteString(fmt.Sprintf("# TYPE node_pressure_%s_some_ratio gauge\n", resource))
			fmt.Fprintf(sb, "node_pressure_%s_some_ratio{window=\"10s\"} %g\n", resource, some10/100)
			fmt.Fprintf(sb, "node_pressure_%s_some_ratio{window=\"60s\"} %g\n", resource, some60/100)
			fmt.Fprintf(sb, "node_pressure_%s_some_ratio{window=\"300s\"} %g\n", resource, some300/100)
		}
		if full10 >= 0 {
			sb.WriteString(fmt.Sprintf("# HELP node_pressure_%s_full_ratio PSI full pressure (%s).\n", resource, resource))
			sb.WriteString(fmt.Sprintf("# TYPE node_pressure_%s_full_ratio gauge\n", resource))
			fmt.Fprintf(sb, "node_pressure_%s_full_ratio{window=\"10s\"} %g\n", resource, full10/100)
			fmt.Fprintf(sb, "node_pressure_%s_full_ratio{window=\"60s\"} %g\n", resource, full60/100)
			fmt.Fprintf(sb, "node_pressure_%s_full_ratio{window=\"300s\"} %g\n", resource, full300/100)
		}
	}

	// Process open FDs
	fds := countOpenFDs()
	if fds >= 0 {
		sb.WriteString("# HELP process_open_fds Open file descriptors.\n")
		sb.WriteString("# TYPE process_open_fds gauge\n")
		fmt.Fprintf(sb, "process_open_fds %d\n", fds)
	}
}

// --- /proc readers ---

func readCPUStat() (cpuStat, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuStat{}, err
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) >= 9 {
				return cpuStat{
					user:    parseFloat(fields[1]),
					nice:    parseFloat(fields[2]),
					system:  parseFloat(fields[3]),
					idle:    parseFloat(fields[4]),
					iowait:  parseFloat(fields[5]),
					irq:     parseFloat(fields[6]),
					softirq: parseFloat(fields[7]),
					steal:   parseFloat(fields[8]),
				}, nil
			}
		}
	}
	return cpuStat{}, fmt.Errorf("cpu line not found")
}

func readMemInfo() (total, avail, free int64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			val, _ := strconv.ParseInt(fields[1], 10, 64)
			val *= 1024 // KB to bytes
			switch {
			case strings.HasPrefix(line, "MemTotal:"):
				total = val
			case strings.HasPrefix(line, "MemAvailable:"):
				avail = val
			case strings.HasPrefix(line, "MemFree:"):
				free = val
			}
		}
	}
	return
}

func readProcessRSS() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				val, _ := strconv.ParseInt(fields[1], 10, 64)
				return val * 1024 // KB to bytes
			}
		}
	}
	return 0
}

func readDiskIO() (readBytes, writeBytes int64) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 14 {
			// fields[5] = sectors read, fields[9] = sectors written (512 bytes/sector)
			r, _ := strconv.ParseInt(fields[5], 10, 64)
			w, _ := strconv.ParseInt(fields[9], 10, 64)
			readBytes += r * 512
			writeBytes += w * 512
		}
	}
	return
}

func readNetIO() (rxBytes, txBytes int64) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, ":") || strings.HasPrefix(line, "Inter") || strings.HasPrefix(line, " face") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue // skip loopback
		}
		fields := strings.Fields(parts[1])
		if len(fields) >= 9 {
			rx, _ := strconv.ParseInt(fields[0], 10, 64)
			tx, _ := strconv.ParseInt(fields[8], 10, 64)
			rxBytes += rx
			txBytes += tx
		}
	}
	return
}

func readPSI(resource string) (some10, some60, some300, full10, full60, full300 float64) {
	some10, some60, some300 = -1, -1, -1
	full10, full60, full300 = -1, -1, -1

	data, err := os.ReadFile("/proc/pressure/" + resource)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "some ") {
			some10, some60, some300 = parsePSILine(line)
		}
		if strings.HasPrefix(line, "full ") {
			full10, full60, full300 = parsePSILine(line)
		}
	}
	return
}

func parsePSILine(line string) (avg10, avg60, avg300 float64) {
	// "some avg10=0.00 avg60=0.00 avg300=0.00 total=0"
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, "avg10=") {
			avg10, _ = strconv.ParseFloat(field[6:], 64)
		}
		if strings.HasPrefix(field, "avg60=") {
			avg60, _ = strconv.ParseFloat(field[6:], 64)
		}
		if strings.HasPrefix(field, "avg300=") {
			avg300, _ = strconv.ParseFloat(field[7:], 64)
		}
	}
	return
}

func countOpenFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
