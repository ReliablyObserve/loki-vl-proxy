package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// SystemMetrics collects OS-level metrics from /proc for standalone observability.
// Only available on Linux; returns empty on other platforms.
type SystemMetrics struct {
	mu          sync.Mutex
	prevCPU     cpuStat
	prevProcCPU processCPUStat
	prevTime    time.Time
}

type cpuStat struct {
	user, nice, system, idle, iowait, irq, softirq, steal float64
}

type processCPUStat struct {
	user, system float64
}

var (
	procRoot     = "/proc"
	selfProcRoot = "/proc"
	cgroupRoot   = "/sys/fs/cgroup"
	procReadFile = os.ReadFile
	procReadDir  = os.ReadDir
	systemGOOS   = runtime.GOOS
)

// SystemStartupCheck describes startup-time availability of /proc-backed metric families.
type SystemStartupCheck struct {
	GOOS            string
	ProcRoot        string
	Scope           string
	Availability    map[string]bool
	Issues          map[string]string
	Recommendations []string
}

// MissingFamilies returns a stable list of unavailable metric families.
func (c SystemStartupCheck) MissingFamilies() []string {
	missing := make([]string, 0, len(c.Availability))
	for family, ok := range c.Availability {
		if !ok {
			missing = append(missing, family)
		}
	}
	sort.Strings(missing)
	return missing
}

// IssueList returns issue strings in stable key order.
func (c SystemStartupCheck) IssueList() []string {
	keys := make([]string, 0, len(c.Issues))
	for k := range c.Issues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%s: %s", k, c.Issues[k]))
	}
	return out
}

// SetProcRoot overrides the root used for /proc-backed system metrics.
func SetProcRoot(root string) {
	root = strings.TrimSpace(root)
	if root == "" {
		procRoot = "/proc"
		return
	}
	procRoot = filepath.Clean(root)
}

// ProcRoot returns the configured /proc root path.
func ProcRoot() string {
	return procRoot
}

func detectProcScope(root string) string {
	clean := filepath.Clean(strings.TrimSpace(root))
	switch clean {
	case "/proc":
		return "container"
	case "/host/proc", "/rootfs/proc":
		return "host"
	default:
		if strings.Contains(clean, "host") && strings.HasSuffix(clean, "/proc") {
			return "host"
		}
		return "custom"
	}
}

// InspectSystemStartup validates startup prerequisites for /proc-backed metric families.
func InspectSystemStartup() SystemStartupCheck {
	check := SystemStartupCheck{
		GOOS:         systemGOOS,
		ProcRoot:     procRoot,
		Scope:        detectProcScope(procRoot),
		Availability: map[string]bool{},
		Issues:       map[string]string{},
	}

	if systemGOOS != "linux" {
		check.Recommendations = append(check.Recommendations, "System CPU/memory/disk/network/PSI metrics are exported only on Linux; non-Linux builds expose runtime/process metrics only.")
		return check
	}

	if _, err := readCPUStat(); err != nil {
		check.Availability["cpu"] = false
		check.Issues["cpu"] = fmt.Sprintf("failed to read %s: %v", procPath("stat"), err)
	} else {
		check.Availability["cpu"] = true
	}

	if data, err := procReadFile(selfProcPath("meminfo")); err != nil {
		check.Availability["memory"] = false
		check.Issues["memory"] = fmt.Sprintf("failed to read %s: %v", selfProcPath("meminfo"), err)
	} else {
		memTotal, _, _ := parseMemInfoData(string(data))
		if memTotal <= 0 {
			check.Availability["memory"] = false
			check.Issues["memory"] = fmt.Sprintf("parsed MemTotal=0 from %s", selfProcPath("meminfo"))
		} else {
			check.Availability["memory"] = true
		}
	}

	if data, err := procReadFile(selfProcPath("self", "status")); err != nil {
		check.Availability["process_rss"] = false
		check.Issues["process_rss"] = fmt.Sprintf("failed to read %s: %v", selfProcPath("self", "status"), err)
	} else {
		if parseProcessRSSData(string(data)) <= 0 {
			check.Availability["process_rss"] = false
			check.Issues["process_rss"] = fmt.Sprintf("unable to parse VmRSS from %s", selfProcPath("self", "status"))
		} else {
			check.Availability["process_rss"] = true
		}
	}

	if data, err := procReadFile(selfProcPath("self", "io")); err != nil {
		check.Availability["disk_io"] = false
		check.Issues["disk_io"] = fmt.Sprintf("failed to read %s: %v", selfProcPath("self", "io"), err)
	} else {
		text := string(data)
		if !strings.Contains(text, "read_bytes:") || !strings.Contains(text, "write_bytes:") {
			check.Availability["disk_io"] = false
			check.Issues["disk_io"] = fmt.Sprintf("unable to parse read_bytes/write_bytes from %s", selfProcPath("self", "io"))
		} else {
			check.Availability["disk_io"] = true
		}
	}

	if _, err := procReadFile(selfProcPath("net", "dev")); err != nil {
		check.Availability["network_io"] = false
		check.Issues["network_io"] = fmt.Sprintf("failed to read %s: %v", selfProcPath("net", "dev"), err)
	} else {
		check.Availability["network_io"] = true
	}

	for _, resource := range []string{"cpu", "memory", "io"} {
		family := "pressure_" + resource
		data, path, err := readPSIRaw(resource)
		if err != nil {
			check.Availability[family] = false
			check.Issues[family] = fmt.Sprintf("failed to read %s: %v", path, err)
			continue
		}
		some10, some60, some300, full10, full60, full300 := parsePSIData(string(data))
		if some10 < 0 && some60 < 0 && some300 < 0 && full10 < 0 && full60 < 0 && full300 < 0 {
			check.Availability[family] = false
			check.Issues[family] = fmt.Sprintf("unable to parse PSI data from %s", path)
			continue
		}
		check.Availability[family] = true
	}

	if _, err := procReadDir(selfProcPath("self", "fd")); err != nil {
		check.Availability["open_fds"] = false
		check.Issues["open_fds"] = fmt.Sprintf("failed to read %s: %v", selfProcPath("self", "fd"), err)
	} else {
		check.Availability["open_fds"] = true
	}

	if len(check.MissingFamilies()) > 0 {
		check.Recommendations = append(check.Recommendations, "Verify container permissions and mount path for proc files; inaccessible /proc paths result in missing system metric families.")
	}
	return check
}

func procPath(parts ...string) string {
	all := append([]string{procRoot}, parts...)
	return filepath.Join(all...)
}

func selfProcPath(parts ...string) string {
	all := append([]string{selfProcRoot}, parts...)
	return filepath.Join(all...)
}

func cgroupPath(parts ...string) string {
	all := append([]string{cgroupRoot}, parts...)
	return filepath.Join(all...)
}

func NewSystemMetrics() *SystemMetrics {
	sm := &SystemMetrics{prevTime: time.Now()}
	sm.prevCPU, _ = readCPUStat()
	sm.prevProcCPU, _ = readProcessCPUStat()
	return sm
}

// WritePrometheus writes system metrics in Prometheus text exposition format.
func (sm *SystemMetrics) WritePrometheus(sb *strings.Builder) {
	if systemGOOS != "linux" {
		// Non-Linux: report process CPU counter and Go runtime memory metrics.
		cpuSecs := readProcessCPUSeconds()
		sb.WriteString("# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.\n")
		sb.WriteString("# TYPE process_cpu_seconds_total counter\n")
		sb.WriteString("# HELP loki_vl_proxy_process_cpu_seconds_total Total user and system CPU time spent in seconds.\n")
		sb.WriteString("# TYPE loki_vl_proxy_process_cpu_seconds_total counter\n")
		fmt.Fprintf(sb, "process_cpu_seconds_total %g\n", cpuSecs)
		fmt.Fprintf(sb, "loki_vl_proxy_process_cpu_seconds_total %g\n", cpuSecs)

		sb.WriteString("# HELP process_resident_memory_bytes Resident memory size.\n")
		sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
		sb.WriteString("# HELP loki_vl_proxy_process_resident_memory_bytes Resident memory size.\n")
		sb.WriteString("# TYPE loki_vl_proxy_process_resident_memory_bytes gauge\n")
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Fprintf(sb, "process_resident_memory_bytes %d\n", m.Sys)
		fmt.Fprintf(sb, "loki_vl_proxy_process_resident_memory_bytes %d\n", m.Sys)
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// CPU usage (always export; falls back to 0s when /proc is unavailable).
	userPct, sysPct, iowaitPct := 0.0, 0.0, 0.0
	now := time.Now()
	cur, cpuErr := readCPUStat()
	curProc, procErr := readProcessCPUStat()
	if cpuErr == nil && procErr == nil {
		elapsed := now.Sub(sm.prevTime).Seconds()
		if elapsed > 0 {
			totalPrev := sm.prevCPU.user + sm.prevCPU.nice + sm.prevCPU.system + sm.prevCPU.idle + sm.prevCPU.iowait + sm.prevCPU.irq + sm.prevCPU.softirq + sm.prevCPU.steal
			totalCur := cur.user + cur.nice + cur.system + cur.idle + cur.iowait + cur.irq + cur.softirq + cur.steal
			totalDiff := totalCur - totalPrev
			userDiff := curProc.user - sm.prevProcCPU.user
			sysDiff := curProc.system - sm.prevProcCPU.system
			if totalDiff > 0 && userDiff >= 0 {
				userPct = userDiff / totalDiff
			}
			if totalDiff > 0 && sysDiff >= 0 {
				sysPct = sysDiff / totalDiff
			}
			// Per-process iowait isn't exposed by procfs; keep a stable series with zero.
			iowaitPct = 0
		}
		sm.prevCPU = cur
		sm.prevProcCPU = curProc
		sm.prevTime = now
	}
	sb.WriteString("# HELP process_cpu_usage_ratio CPU usage ratio (0-1).\n")
	sb.WriteString("# TYPE process_cpu_usage_ratio gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_cpu_usage_ratio CPU usage ratio (0-1).\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_cpu_usage_ratio gauge\n")
	fmt.Fprintf(sb, "process_cpu_usage_ratio{mode=\"user\"} %g\n", userPct)
	fmt.Fprintf(sb, "process_cpu_usage_ratio{mode=\"system\"} %g\n", sysPct)
	fmt.Fprintf(sb, "process_cpu_usage_ratio{mode=\"iowait\"} %g\n", iowaitPct)
	fmt.Fprintf(sb, "loki_vl_proxy_process_cpu_usage_ratio{mode=\"user\"} %g\n", userPct)
	fmt.Fprintf(sb, "loki_vl_proxy_process_cpu_usage_ratio{mode=\"system\"} %g\n", sysPct)
	fmt.Fprintf(sb, "loki_vl_proxy_process_cpu_usage_ratio{mode=\"iowait\"} %g\n", iowaitPct)

	cpuSecs := readProcessCPUSeconds()
	sb.WriteString("# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.\n")
	sb.WriteString("# TYPE process_cpu_seconds_total counter\n")
	sb.WriteString("# HELP loki_vl_proxy_process_cpu_seconds_total Total user and system CPU time spent in seconds.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_cpu_seconds_total counter\n")
	fmt.Fprintf(sb, "process_cpu_seconds_total %g\n", cpuSecs)
	fmt.Fprintf(sb, "loki_vl_proxy_process_cpu_seconds_total %g\n", cpuSecs)

	// Memory from /proc/meminfo
	memTotal, memAvail, memFree := readMemInfo()
	usageRatio := 0.0
	if memTotal > 0 {
		used := memTotal - memAvail
		usageRatio = float64(used) / float64(memTotal)
	}
	sb.WriteString("# HELP process_memory_total_bytes Total memory.\n")
	sb.WriteString("# TYPE process_memory_total_bytes gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_memory_total_bytes Total memory.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_memory_total_bytes gauge\n")
	fmt.Fprintf(sb, "process_memory_total_bytes %d\n", memTotal)
	fmt.Fprintf(sb, "loki_vl_proxy_process_memory_total_bytes %d\n", memTotal)

	sb.WriteString("# HELP process_memory_available_bytes Available memory.\n")
	sb.WriteString("# TYPE process_memory_available_bytes gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_memory_available_bytes Available memory.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_memory_available_bytes gauge\n")
	fmt.Fprintf(sb, "process_memory_available_bytes %d\n", memAvail)
	fmt.Fprintf(sb, "loki_vl_proxy_process_memory_available_bytes %d\n", memAvail)

	sb.WriteString("# HELP process_memory_free_bytes Free memory.\n")
	sb.WriteString("# TYPE process_memory_free_bytes gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_memory_free_bytes Free memory.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_memory_free_bytes gauge\n")
	fmt.Fprintf(sb, "process_memory_free_bytes %d\n", memFree)
	fmt.Fprintf(sb, "loki_vl_proxy_process_memory_free_bytes %d\n", memFree)

	sb.WriteString("# HELP process_memory_usage_ratio Memory usage ratio (0-1).\n")
	sb.WriteString("# TYPE process_memory_usage_ratio gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_memory_usage_ratio Memory usage ratio (0-1).\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_memory_usage_ratio gauge\n")
	fmt.Fprintf(sb, "process_memory_usage_ratio %g\n", usageRatio)
	fmt.Fprintf(sb, "loki_vl_proxy_process_memory_usage_ratio %g\n", usageRatio)

	// Process RSS from /proc/self/status
	rss := readProcessRSS()
	sb.WriteString("# HELP process_resident_memory_bytes Process RSS.\n")
	sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_resident_memory_bytes Process RSS.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_resident_memory_bytes gauge\n")
	fmt.Fprintf(sb, "process_resident_memory_bytes %d\n", rss)
	fmt.Fprintf(sb, "loki_vl_proxy_process_resident_memory_bytes %d\n", rss)

	// Disk IO from /proc/self/io
	readBytes, writeBytes, readOps, writeOps := readDiskIO()
	sb.WriteString("# HELP process_disk_read_bytes_total Disk read bytes.\n")
	sb.WriteString("# TYPE process_disk_read_bytes_total counter\n")
	sb.WriteString("# HELP loki_vl_proxy_process_disk_read_bytes_total Disk read bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_disk_read_bytes_total counter\n")
	fmt.Fprintf(sb, "process_disk_read_bytes_total %d\n", readBytes)
	fmt.Fprintf(sb, "loki_vl_proxy_process_disk_read_bytes_total %d\n", readBytes)
	sb.WriteString("# HELP process_disk_written_bytes_total Disk written bytes.\n")
	sb.WriteString("# TYPE process_disk_written_bytes_total counter\n")
	sb.WriteString("# HELP loki_vl_proxy_process_disk_written_bytes_total Disk written bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_disk_written_bytes_total counter\n")
	fmt.Fprintf(sb, "process_disk_written_bytes_total %d\n", writeBytes)
	fmt.Fprintf(sb, "loki_vl_proxy_process_disk_written_bytes_total %d\n", writeBytes)
	sb.WriteString("# HELP loki_vl_proxy_process_disk_read_operations_total Disk read operations.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_disk_read_operations_total counter\n")
	fmt.Fprintf(sb, "loki_vl_proxy_process_disk_read_operations_total %d\n", readOps)
	sb.WriteString("# HELP loki_vl_proxy_process_disk_write_operations_total Disk write operations.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_disk_write_operations_total counter\n")
	fmt.Fprintf(sb, "loki_vl_proxy_process_disk_write_operations_total %d\n", writeOps)

	// Network IO from /proc/net/dev in the process network namespace.
	rxBytes, txBytes := readNetIO()
	sb.WriteString("# HELP process_network_receive_bytes_total Network receive bytes.\n")
	sb.WriteString("# TYPE process_network_receive_bytes_total counter\n")
	sb.WriteString("# HELP loki_vl_proxy_process_network_receive_bytes_total Network receive bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_network_receive_bytes_total counter\n")
	fmt.Fprintf(sb, "process_network_receive_bytes_total %d\n", rxBytes)
	fmt.Fprintf(sb, "loki_vl_proxy_process_network_receive_bytes_total %d\n", rxBytes)
	sb.WriteString("# HELP process_network_transmit_bytes_total Network transmit bytes.\n")
	sb.WriteString("# TYPE process_network_transmit_bytes_total counter\n")
	sb.WriteString("# HELP loki_vl_proxy_process_network_transmit_bytes_total Network transmit bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_network_transmit_bytes_total counter\n")
	fmt.Fprintf(sb, "process_network_transmit_bytes_total %d\n", txBytes)
	fmt.Fprintf(sb, "loki_vl_proxy_process_network_transmit_bytes_total %d\n", txBytes)

	// PSI (Pressure Stall Information) from /proc/pressure/*
	for _, resource := range []string{"cpu", "memory", "io"} {
		some10, some60, some300, full10, full60, full300 := readPSI(resource)
		if some10 < 0 {
			some10, some60, some300 = 0, 0, 0
		}
		if full10 < 0 {
			full10, full60, full300 = 0, 0, 0
		}
		fmt.Fprintf(sb, "# HELP process_pressure_%s_some_ratio PSI some pressure (%s).\n", resource, resource)
		fmt.Fprintf(sb, "# TYPE process_pressure_%s_some_ratio gauge\n", resource)
		fmt.Fprintf(sb, "# HELP loki_vl_proxy_process_pressure_%s_some_ratio PSI some pressure (%s).\n", resource, resource)
		fmt.Fprintf(sb, "# TYPE loki_vl_proxy_process_pressure_%s_some_ratio gauge\n", resource)
		fmt.Fprintf(sb, "process_pressure_%s_some_ratio{window=\"10s\"} %g\n", resource, some10/100)
		fmt.Fprintf(sb, "process_pressure_%s_some_ratio{window=\"60s\"} %g\n", resource, some60/100)
		fmt.Fprintf(sb, "process_pressure_%s_some_ratio{window=\"300s\"} %g\n", resource, some300/100)
		fmt.Fprintf(sb, "loki_vl_proxy_process_pressure_%s_some_ratio{window=\"10s\"} %g\n", resource, some10/100)
		fmt.Fprintf(sb, "loki_vl_proxy_process_pressure_%s_some_ratio{window=\"60s\"} %g\n", resource, some60/100)
		fmt.Fprintf(sb, "loki_vl_proxy_process_pressure_%s_some_ratio{window=\"300s\"} %g\n", resource, some300/100)

		fmt.Fprintf(sb, "# HELP process_pressure_%s_full_ratio PSI full pressure (%s).\n", resource, resource)
		fmt.Fprintf(sb, "# TYPE process_pressure_%s_full_ratio gauge\n", resource)
		fmt.Fprintf(sb, "# HELP loki_vl_proxy_process_pressure_%s_full_ratio PSI full pressure (%s).\n", resource, resource)
		fmt.Fprintf(sb, "# TYPE loki_vl_proxy_process_pressure_%s_full_ratio gauge\n", resource)
		fmt.Fprintf(sb, "process_pressure_%s_full_ratio{window=\"10s\"} %g\n", resource, full10/100)
		fmt.Fprintf(sb, "process_pressure_%s_full_ratio{window=\"60s\"} %g\n", resource, full60/100)
		fmt.Fprintf(sb, "process_pressure_%s_full_ratio{window=\"300s\"} %g\n", resource, full300/100)
		fmt.Fprintf(sb, "loki_vl_proxy_process_pressure_%s_full_ratio{window=\"10s\"} %g\n", resource, full10/100)
		fmt.Fprintf(sb, "loki_vl_proxy_process_pressure_%s_full_ratio{window=\"60s\"} %g\n", resource, full60/100)
		fmt.Fprintf(sb, "loki_vl_proxy_process_pressure_%s_full_ratio{window=\"300s\"} %g\n", resource, full300/100)
	}

	// Process open FDs
	fds := countOpenFDs()
	if fds < 0 {
		fds = 0
	}
	sb.WriteString("# HELP process_open_fds Open file descriptors.\n")
	sb.WriteString("# TYPE process_open_fds gauge\n")
	sb.WriteString("# HELP loki_vl_proxy_process_open_fds Open file descriptors.\n")
	sb.WriteString("# TYPE loki_vl_proxy_process_open_fds gauge\n")
	fmt.Fprintf(sb, "process_open_fds %d\n", fds)
	fmt.Fprintf(sb, "loki_vl_proxy_process_open_fds %d\n", fds)
}

// --- /proc readers ---

func readCPUStat() (cpuStat, error) {
	data, err := procReadFile(selfProcPath("stat"))
	if err != nil {
		return cpuStat{}, err
	}
	return parseCPUStatData(string(data))
}

func parseCPUStatData(data string) (cpuStat, error) {
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
	data, err := procReadFile(selfProcPath("meminfo"))
	if err != nil {
		return 0, 0, 0
	}
	return parseMemInfoData(string(data))
}

func parseMemInfoData(data string) (total, avail, free int64) {
	for _, line := range strings.Split(data, "\n") {
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
	data, err := procReadFile(selfProcPath("self", "status"))
	if err != nil {
		return 0
	}
	return parseProcessRSSData(string(data))
}

func parseProcessRSSData(data string) int64 {
	for _, line := range strings.Split(data, "\n") {
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

func readDiskIO() (readBytes, writeBytes, readOps, writeOps int64) {
	data, err := procReadFile(selfProcPath("self", "io"))
	if err != nil {
		return 0, 0, 0, 0
	}
	return parseProcessIOData(string(data))
}

func parseDiskIOData(data string) (readBytes, writeBytes int64) {
	for _, line := range strings.Split(data, "\n") {
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

func parseProcessIOData(data string) (readBytes, writeBytes, readOps, writeOps int64) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		val, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "syscr":
			readOps = val
		case "syscw":
			writeOps = val
		case "read_bytes":
			readBytes = val
		case "write_bytes":
			writeBytes = val
		}
	}
	return
}

func readNetIO() (rxBytes, txBytes int64) {
	data, err := procReadFile(selfProcPath("net", "dev"))
	if err != nil {
		return 0, 0
	}
	return parseNetIOData(string(data))
}

func parseNetIOData(data string) (rxBytes, txBytes int64) {
	for _, line := range strings.Split(data, "\n") {
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

	data, _, err := readPSIRaw(resource)
	if err != nil {
		return
	}
	return parsePSIData(string(data))
}

func readPSIRaw(resource string) ([]byte, string, error) {
	cgroupPressure := cgroupPath(resource + ".pressure")
	if data, err := procReadFile(cgroupPressure); err == nil {
		return data, cgroupPressure, nil
	}
	procPressure := procPath("pressure", resource)
	data, err := procReadFile(procPressure)
	if err != nil {
		return nil, procPressure, err
	}
	return data, procPressure, nil
}

func parsePSIData(data string) (some10, some60, some300, full10, full60, full300 float64) {
	some10, some60, some300 = -1, -1, -1
	full10, full60, full300 = -1, -1, -1
	for _, line := range strings.Split(data, "\n") {
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
	entries, err := procReadDir(selfProcPath("self", "fd"))
	if err != nil {
		return -1
	}
	return len(entries)
}

func readProcessCPUStat() (processCPUStat, error) {
	data, err := procReadFile(selfProcPath("self", "stat"))
	if err != nil {
		return processCPUStat{}, err
	}
	return parseProcessCPUStatData(string(data))
}

func parseProcessCPUStatData(data string) (processCPUStat, error) {
	line := strings.TrimSpace(data)
	if line == "" {
		return processCPUStat{}, fmt.Errorf("empty stat data")
	}
	end := strings.LastIndex(line, ")")
	if end < 0 || end+2 >= len(line) {
		return processCPUStat{}, fmt.Errorf("malformed stat line")
	}
	rest := strings.Fields(line[end+2:])
	if len(rest) < 13 {
		return processCPUStat{}, fmt.Errorf("insufficient stat fields")
	}
	utime, err := strconv.ParseFloat(rest[11], 64)
	if err != nil {
		return processCPUStat{}, fmt.Errorf("parse utime: %w", err)
	}
	stime, err := strconv.ParseFloat(rest[12], 64)
	if err != nil {
		return processCPUStat{}, fmt.Errorf("parse stime: %w", err)
	}
	return processCPUStat{user: utime, system: stime}, nil
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func readProcessCPUSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6 +
		float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
}
