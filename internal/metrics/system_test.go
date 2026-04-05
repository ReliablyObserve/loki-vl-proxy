package metrics

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

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
		"node_memory_total_bytes",
		"node_memory_available_bytes",
		"node_disk_read_bytes_total",
		"node_network_receive_bytes_total",
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
	if strings.Contains(output, "node_cpu_usage_ratio") {
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
