package metrics

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func withSyntheticProcFS(t *testing.T, files map[string]string) string {
	t.Helper()

	root := t.TempDir()
	for rel, content := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return root
}

func setSyntheticProcEnv(t *testing.T, root string) {
	t.Helper()

	oldRoot := procRoot
	oldReadFile := procReadFile
	oldReadDir := procReadDir
	oldGOOS := systemGOOS
	procRoot = root
	procReadFile = os.ReadFile
	procReadDir = os.ReadDir
	systemGOOS = "linux"
	t.Cleanup(func() {
		procRoot = oldRoot
		procReadFile = oldReadFile
		procReadDir = oldReadDir
		systemGOOS = oldGOOS
	})
}

func TestSetProcRoot(t *testing.T) {
	oldRoot := procRoot
	t.Cleanup(func() { procRoot = oldRoot })

	SetProcRoot("/host/proc/../proc")
	if got := ProcRoot(); got != "/host/proc" {
		t.Fatalf("expected cleaned proc root, got %q", got)
	}

	SetProcRoot("   ")
	if got := ProcRoot(); got != "/proc" {
		t.Fatalf("expected default proc root, got %q", got)
	}
}

func TestDetectProcScope(t *testing.T) {
	cases := []struct {
		root string
		want string
	}{
		{root: "/proc", want: "container"},
		{root: "/host/proc", want: "host"},
		{root: "/rootfs/proc", want: "host"},
		{root: " /tmp/mounted-host/proc ", want: "host"},
		{root: "/tmp/custom/proc", want: "custom"},
	}

	for _, tc := range cases {
		got := detectProcScope(tc.root)
		if got != tc.want {
			t.Fatalf("detectProcScope(%q)=%q, want %q", tc.root, got, tc.want)
		}
	}
}

func TestInspectSystemStartup_NonLinux(t *testing.T) {
	oldGOOS := systemGOOS
	oldRoot := procRoot
	systemGOOS = "darwin"
	procRoot = "/proc"
	t.Cleanup(func() {
		systemGOOS = oldGOOS
		procRoot = oldRoot
	})

	check := InspectSystemStartup()
	if check.GOOS != "darwin" {
		t.Fatalf("unexpected GOOS in check: %q", check.GOOS)
	}
	if len(check.Availability) != 0 {
		t.Fatalf("expected no linux /proc availability map on non-linux, got %+v", check.Availability)
	}
	if len(check.Recommendations) == 0 || !strings.Contains(check.Recommendations[0], "only on Linux") {
		t.Fatalf("expected non-linux recommendation, got %+v", check.Recommendations)
	}
}

func TestInspectSystemStartup_WithSyntheticHostProc(t *testing.T) {
	root := withSyntheticProcFS(t, map[string]string{
		"stat":            "cpu  100 5 20 300 7 0 1 2\n",
		"meminfo":         "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n",
		"self/status":     "Name:\tproxy\nVmRSS:\t123 kB\n",
		"diskstats":       "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"net/dev":         "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"pressure/cpu":    "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n",
		"pressure/memory": "some avg10=0.50 avg60=1.00 avg300=1.50 total=10\nfull avg10=2.00 avg60=2.50 avg300=3.00 total=20\n",
		"pressure/io":     "some avg10=0.25 avg60=0.50 avg300=0.75 total=10\nfull avg10=1.00 avg60=1.25 avg300=1.50 total=20\n",
		"self/fd/0":       "",
		"self/fd/1":       "",
		"self/fd/2":       "",
	})
	setSyntheticProcEnv(t, root)

	check := InspectSystemStartup()
	if check.Scope != "custom" {
		t.Fatalf("expected custom scope for synthetic path, got %q", check.Scope)
	}
	if len(check.MissingFamilies()) != 0 {
		t.Fatalf("expected no missing families, got %v (%v)", check.MissingFamilies(), check.Issues)
	}
}

func TestInspectSystemStartup_ReportsMissingFamilies(t *testing.T) {
	root := withSyntheticProcFS(t, map[string]string{
		"stat":            "cpu  100 5 20 300 7 0 1 2\n",
		"self/status":     "Name:\tproxy\nVmRSS:\t123 kB\n",
		"diskstats":       "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"net/dev":         "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"pressure/cpu":    "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n",
		"pressure/memory": "some avg10=0.50 avg60=1.00 avg300=1.50 total=10\nfull avg10=2.00 avg60=2.50 avg300=3.00 total=20\n",
		"self/fd/0":       "",
	})
	setSyntheticProcEnv(t, root)

	check := InspectSystemStartup()
	missing := check.MissingFamilies()
	wantSubset := []string{"memory", "pressure_io"}
	for _, family := range wantSubset {
		if check.Availability[family] {
			t.Fatalf("expected %s to be unavailable, got available map=%v", family, check.Availability)
		}
	}
	if len(missing) == 0 {
		t.Fatalf("expected missing families, got none")
	}
	issues := check.IssueList()
	if len(issues) == 0 {
		t.Fatalf("expected startup issues, got none")
	}
}

func TestInspectSystemStartup_HostScope_NoHostMountRecommendation(t *testing.T) {
	base := withSyntheticProcFS(t, map[string]string{
		"host/proc/stat":            "cpu  100 5 20 300 7 0 1 2\n",
		"host/proc/meminfo":         "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n",
		"host/proc/self/status":     "Name:\tproxy\nVmRSS:\t123 kB\n",
		"host/proc/diskstats":       "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"host/proc/net/dev":         "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"host/proc/pressure/cpu":    "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n",
		"host/proc/pressure/memory": "some avg10=0.50 avg60=1.00 avg300=1.50 total=10\nfull avg10=2.00 avg60=2.50 avg300=3.00 total=20\n",
		"host/proc/pressure/io":     "some avg10=0.25 avg60=0.50 avg300=0.75 total=10\nfull avg10=1.00 avg60=1.25 avg300=1.50 total=20\n",
		"host/proc/self/fd/0":       "",
	})
	setSyntheticProcEnv(t, filepath.Join(base, "host", "proc"))

	check := InspectSystemStartup()
	if check.Scope != "host" {
		t.Fatalf("expected host scope, got %q", check.Scope)
	}
	if len(check.MissingFamilies()) != 0 {
		t.Fatalf("expected no missing families, got %v", check.MissingFamilies())
	}
	for _, rec := range check.Recommendations {
		if strings.Contains(rec, "mount host /proc") {
			t.Fatalf("unexpected host /proc recommendation when already in host scope: %+v", check.Recommendations)
		}
	}
}

func TestInspectSystemStartup_ReportsPSIParseFailure(t *testing.T) {
	root := withSyntheticProcFS(t, map[string]string{
		"stat":            "cpu  100 5 20 300 7 0 1 2\n",
		"meminfo":         "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n",
		"self/status":     "Name:\tproxy\nVmRSS:\t123 kB\n",
		"diskstats":       "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"net/dev":         "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"pressure/cpu":    "invalid",
		"pressure/memory": "some avg10=0.50 avg60=1.00 avg300=1.50 total=10\nfull avg10=2.00 avg60=2.50 avg300=3.00 total=20\n",
		"pressure/io":     "some avg10=0.25 avg60=0.50 avg300=0.75 total=10\nfull avg10=1.00 avg60=1.25 avg300=1.50 total=20\n",
		"self/fd/0":       "",
	})
	setSyntheticProcEnv(t, root)

	check := InspectSystemStartup()
	if check.Availability["pressure_cpu"] {
		t.Fatalf("expected pressure_cpu to be unavailable, got %+v", check.Availability)
	}
	if !strings.Contains(check.Issues["pressure_cpu"], "unable to parse PSI data") {
		t.Fatalf("expected parse failure issue for pressure_cpu, got %+v", check.Issues)
	}
}

func TestSystemMetrics_WritePrometheus_NoPanic(t *testing.T) {
	sm := NewSystemMetrics()
	var sb strings.Builder
	// Should not panic on any platform
	sm.WritePrometheus(&sb)

	output := sb.String()
	if output == "" {
		t.Error("expected non-empty system metrics output")
	}

	// process_resident_memory_bytes should always be present (Go runtime fallback)
	if !strings.Contains(output, "process_resident_memory_bytes") {
		t.Error("expected process_resident_memory_bytes in output")
	}
}

func TestSystemMetrics_Linux_IncludesNodeMetrics(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux-only metrics")
	}

	sm := NewSystemMetrics()

	// First call establishes CPU baseline
	var sb1 strings.Builder
	sm.WritePrometheus(&sb1)

	// Wait briefly for CPU counters to advance
	time.Sleep(100 * time.Millisecond)

	// Second call should show CPU delta
	var sb2 strings.Builder
	sm.WritePrometheus(&sb2)
	output := sb2.String()

	// These metrics should always be present on Linux (not CPU-delta dependent)
	required := []string{
		"process_memory_total_bytes",
		"process_memory_available_bytes",
		"process_disk_read_bytes_total",
		"process_network_receive_bytes_total",
		"process_open_fds",
		"process_resident_memory_bytes",
	}

	for _, metric := range required {
		if !strings.Contains(output, metric) {
			t.Errorf("missing Linux metric %q in output:\n%s", metric, output)
		}
	}

	// CPU ratio may or may not appear depending on whether counters advanced
	// (it's delta-based; on idle CI runners the delta can be 0)
	if strings.Contains(output, "process_cpu_usage_ratio") {
		t.Log("CPU usage ratio present (good)")
	} else {
		t.Log("CPU usage ratio not present (acceptable on idle CI)")
	}
}

func TestSystemMetrics_CalledTwice_CPUDelta(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux-only")
	}

	sm := NewSystemMetrics()

	// First call establishes baseline
	var sb1 strings.Builder
	sm.WritePrometheus(&sb1)

	// Second call should show CPU delta
	var sb2 strings.Builder
	sm.WritePrometheus(&sb2)

	// Both should produce output
	if sb1.Len() == 0 || sb2.Len() == 0 {
		t.Error("expected non-empty output from both calls")
	}
}

func TestSystemMetrics_IntegratedWithMetrics(t *testing.T) {
	m := NewMetrics()

	var sb strings.Builder
	m.system.WritePrometheus(&sb)

	if sb.Len() == 0 {
		t.Error("system metrics should be available via Metrics.system")
	}
}

func TestParsePSILine(t *testing.T) {
	avg10, avg60, avg300 := parsePSILine("some avg10=1.25 avg60=0.50 avg300=0.05 total=123")
	if avg10 != 1.25 || avg60 != 0.50 || avg300 != 0.05 {
		t.Fatalf("unexpected PSI parse result: %g %g %g", avg10, avg60, avg300)
	}
}

func TestParseCPUStatData(t *testing.T) {
	got, err := parseCPUStatData("cpu  100 5 20 300 7 0 1 2\ncpu0 1 2 3 4 5 6 7 8\n")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if got.user != 100 || got.nice != 5 || got.system != 20 || got.idle != 300 || got.iowait != 7 || got.softirq != 1 || got.steal != 2 {
		t.Fatalf("unexpected cpu stat: %+v", got)
	}
}

func TestParseCPUStatData_MissingCPU(t *testing.T) {
	if _, err := parseCPUStatData("intr 1\nctxt 2\n"); err == nil {
		t.Fatal("expected cpu parse error when aggregate cpu line is missing")
	}
}

func TestParseMemInfoData(t *testing.T) {
	total, avail, free := parseMemInfoData("MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n")
	if total != 1024*1024 || avail != 512*1024 || free != 128*1024 {
		t.Fatalf("unexpected meminfo values: total=%d avail=%d free=%d", total, avail, free)
	}
}

func TestParseProcessRSSData(t *testing.T) {
	if got := parseProcessRSSData("Name:\tproxy\nVmRSS:\t123 kB\n"); got != 123*1024 {
		t.Fatalf("unexpected rss value: %d", got)
	}
	if got := parseProcessRSSData("Name:\tproxy\n"); got != 0 {
		t.Fatalf("expected rss fallback 0, got %d", got)
	}
}

func TestParseDiskIOData(t *testing.T) {
	readBytes, writeBytes := parseDiskIOData("   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n")
	if readBytes != 6*512 || writeBytes != 10*512 {
		t.Fatalf("unexpected disk io values: read=%d write=%d", readBytes, writeBytes)
	}
}

func TestParseNetIOData(t *testing.T) {
	input := "Inter-|   Receive                                                |  Transmit\n" +
		" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n" +
		"    lo: 11 0 0 0 0 0 0 0 22 0 0 0 0 0 0 0\n" +
		"  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n"
	rxBytes, txBytes := parseNetIOData(input)
	if rxBytes != 101 || txBytes != 202 {
		t.Fatalf("unexpected net io values: rx=%d tx=%d", rxBytes, txBytes)
	}
}

func TestParsePSIData(t *testing.T) {
	input := "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n"
	some10, some60, some300, full10, full60, full300 := parsePSIData(input)
	if some10 != 1 || some60 != 2 || some300 != 3 || full10 != 4 || full60 != 5 || full300 != 6 {
		t.Fatalf("unexpected psi data parse: %g %g %g %g %g %g", some10, some60, some300, full10, full60, full300)
	}
}

func TestParseFloat(t *testing.T) {
	if got := parseFloat("12.5"); got != 12.5 {
		t.Fatalf("expected 12.5, got %g", got)
	}
	if got := parseFloat("bad"); got != 0 {
		t.Fatalf("expected invalid parse to return 0, got %g", got)
	}
}

func TestCountOpenFDs(t *testing.T) {
	got := countOpenFDs()
	if runtime.GOOS == "linux" {
		if got < 0 {
			t.Fatalf("expected non-negative fd count on linux, got %d", got)
		}
		return
	}
	if got != -1 {
		t.Fatalf("expected unsupported platforms to return -1, got %d", got)
	}
}

func TestProcReaders_UseSyntheticProcFS(t *testing.T) {
	root := withSyntheticProcFS(t, map[string]string{
		"stat":         "cpu  100 5 20 300 7 0 1 2\n",
		"meminfo":      "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n",
		"self/status":  "Name:\tproxy\nVmRSS:\t123 kB\n",
		"diskstats":    "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"net/dev":      "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n    lo: 11 0 0 0 0 0 0 0 22 0 0 0 0 0 0 0\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"pressure/cpu": "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n",
		"self/fd/0":    "",
		"self/fd/1":    "",
		"self/fd/2":    "",
	})
	setSyntheticProcEnv(t, root)

	cpu, err := readCPUStat()
	if err != nil {
		t.Fatalf("readCPUStat: %v", err)
	}
	if cpu.user != 100 || cpu.idle != 300 {
		t.Fatalf("unexpected cpu stat: %+v", cpu)
	}

	total, avail, free := readMemInfo()
	if total != 1024*1024 || avail != 512*1024 || free != 128*1024 {
		t.Fatalf("unexpected meminfo values: total=%d avail=%d free=%d", total, avail, free)
	}

	if rss := readProcessRSS(); rss != 123*1024 {
		t.Fatalf("unexpected rss value: %d", rss)
	}

	readBytes, writeBytes := readDiskIO()
	if readBytes != 6*512 || writeBytes != 10*512 {
		t.Fatalf("unexpected disk io values: read=%d write=%d", readBytes, writeBytes)
	}

	rxBytes, txBytes := readNetIO()
	if rxBytes != 101 || txBytes != 202 {
		t.Fatalf("unexpected net io values: rx=%d tx=%d", rxBytes, txBytes)
	}

	some10, some60, some300, full10, full60, full300 := readPSI("cpu")
	if some10 != 1 || some60 != 2 || some300 != 3 || full10 != 4 || full60 != 5 || full300 != 6 {
		t.Fatalf("unexpected psi values: %g %g %g %g %g %g", some10, some60, some300, full10, full60, full300)
	}

	if fds := countOpenFDs(); fds != 3 {
		t.Fatalf("expected 3 synthetic fds, got %d", fds)
	}
}

func TestSystemMetrics_WritePrometheus_UsesSyntheticLinuxProcFS(t *testing.T) {
	root := withSyntheticProcFS(t, map[string]string{
		"stat":            "cpu  150 5 40 400 10 0 2 3\n",
		"meminfo":         "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n",
		"self/status":     "Name:\tproxy\nVmRSS:\t123 kB\n",
		"diskstats":       "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"net/dev":         "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"pressure/cpu":    "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n",
		"pressure/memory": "some avg10=0.50 avg60=1.00 avg300=1.50 total=10\nfull avg10=2.00 avg60=2.50 avg300=3.00 total=20\n",
		"pressure/io":     "some avg10=0.25 avg60=0.50 avg300=0.75 total=10\nfull avg10=1.00 avg60=1.25 avg300=1.50 total=20\n",
		"self/fd/0":       "",
		"self/fd/1":       "",
	})
	setSyntheticProcEnv(t, root)

	sm := &SystemMetrics{
		prevCPU:  cpuStat{user: 100, nice: 5, system: 20, idle: 300, iowait: 7, irq: 0, softirq: 1, steal: 2},
		prevTime: time.Now().Add(-1 * time.Second),
	}

	var sb strings.Builder
	sm.WritePrometheus(&sb)
	output := sb.String()

	for _, metric := range []string{
		"process_cpu_usage_ratio",
		"process_memory_total_bytes",
		"process_disk_read_bytes_total",
		"process_network_receive_bytes_total",
		"process_pressure_cpu_some_ratio",
		"process_pressure_memory_full_ratio",
		"process_open_fds",
		"process_resident_memory_bytes",
	} {
		if !strings.Contains(output, metric) {
			t.Fatalf("missing %q in output:\n%s", metric, output)
		}
	}
}

func TestSystemMetrics_WritePrometheus_EmitsZeroWhenProcDataMissing(t *testing.T) {
	root := withSyntheticProcFS(t, map[string]string{
		// Intentionally sparse proc tree to force fallback zeros.
		"self/status": "Name:\tproxy\n",
	})
	setSyntheticProcEnv(t, root)

	sm := NewSystemMetrics()
	var sb strings.Builder
	sm.WritePrometheus(&sb)
	output := sb.String()

	for _, expected := range []string{
		"process_memory_total_bytes 0",
		"process_memory_usage_ratio 0",
		"process_resident_memory_bytes 0",
		"process_open_fds 0",
		"process_pressure_cpu_some_ratio{window=\"60s\"} 0",
		"process_pressure_memory_full_ratio{window=\"60s\"} 0",
		"loki_vl_proxy_process_memory_total_bytes 0",
		"loki_vl_proxy_process_open_fds 0",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected zero fallback line %q, output:\n%s", expected, output)
		}
	}
}

func TestProcReaders_DoNotReturnNegativeValues(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux-only /proc readers")
	}

	total, avail, free := readMemInfo()
	if total < 0 || avail < 0 || free < 0 {
		t.Fatalf("unexpected negative meminfo values: total=%d avail=%d free=%d", total, avail, free)
	}
	if rss := readProcessRSS(); rss < 0 {
		t.Fatalf("unexpected negative rss: %d", rss)
	}
	if readBytes, writeBytes := readDiskIO(); readBytes < 0 || writeBytes < 0 {
		t.Fatalf("unexpected negative disk io: read=%d write=%d", readBytes, writeBytes)
	}
	if rxBytes, txBytes := readNetIO(); rxBytes < 0 || txBytes < 0 {
		t.Fatalf("unexpected negative net io: rx=%d tx=%d", rxBytes, txBytes)
	}
	some10, some60, some300, full10, full60, full300 := readPSI("cpu")
	for _, v := range []float64{some10, some60, some300, full10, full60, full300} {
		if v < -1 {
			t.Fatalf("unexpected PSI value %g", v)
		}
	}
	if _, err := readCPUStat(); err != nil {
		t.Fatalf("expected cpu stat to be readable on linux, got %v", err)
	}
}
