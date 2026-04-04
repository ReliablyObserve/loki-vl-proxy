package metrics

import (
	"runtime"
	"strings"
	"testing"
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
	var sb strings.Builder
	sm.WritePrometheus(&sb)
	output := sb.String()

	required := []string{
		"node_cpu_usage_ratio",
		"node_memory_total_bytes",
		"node_memory_available_bytes",
		"node_disk_read_bytes_total",
		"node_network_receive_bytes_total",
		"process_open_fds",
	}

	for _, metric := range required {
		if !strings.Contains(output, metric) {
			t.Errorf("missing Linux metric %q", metric)
		}
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
