//go:build helm

package helm

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func renderChart(t *testing.T, args ...string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate chart")
	}
	chart := filepath.Join(filepath.Dir(file), "..", "..", "charts", "loki-vl-proxy")
	output, err := exec.Command("helm", append([]string{"template", "scrape", chart, "--namespace", "scrape-test"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, output)
	}
	return string(output)
}

// The proxy's "endpoint" label collides with the Operator's target label, so the ServiceMonitor honours the
// exposed labels unless the operator turns that off.
// conformance: observability-metrics-histograms-and-labels
func TestServiceMonitorHonorLabels(t *testing.T) {
	scoped := []string{"--set", "serviceMonitor.enabled=true", "--set", "networkPolicy.monitoringNamespace=monitoring"}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "default", want: "honorLabels: true"},
		// A "helm upgrade --reuse-values" from a chart without the key renders it unset.
		{name: "key unset", args: []string{"--set", "serviceMonitor.honorLabels=null"}, want: "honorLabels: true"},
		{name: "off", args: []string{"--set", "serviceMonitor.honorLabels=false"}, want: "honorLabels: false"},
		{name: "off as a string", args: []string{"--set-string", "serviceMonitor.honorLabels=false"}, want: "honorLabels: false"},
		{name: "scraping the proxy port", args: []string{"--set", "service.metrics.enabled=false", "--set", "extraArgs.metrics-listen="}, want: "honorLabels: true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rendered := renderChart(t, append(append([]string{}, scoped...), tc.args...)...)
			if got := strings.Count(rendered, "honorLabels:"); got != 1 || !strings.Contains(rendered, "\n      "+tc.want+"\n") {
				t.Fatalf("want one endpoint with %q, found %d honorLabels lines:\n%s", tc.want, got, rendered)
			}
		})
	}
	if strings.Contains(renderChart(t), "kind: ServiceMonitor") {
		t.Fatal("the ServiceMonitor is off by default")
	}
}

// conformance: observability-metrics-histograms-and-labels
func TestMetricsDurationBucketsValue(t *testing.T) {
	flag := "-metrics.duration-buckets="
	if strings.Contains(renderChart(t), flag) {
		t.Fatal("the default render must keep the binary's buckets")
	}
	set := renderChart(t, "--set-string", `observability.metrics.durationBuckets=0.1\,1\,60`)
	if strings.Count(set, flag) != 1 || !strings.Contains(set, `"`+flag+`0.1,1,60"`) {
		t.Fatalf("observability.metrics.durationBuckets did not reach the container arguments:\n%s", set)
	}
	both := renderChart(t, "--set-string", `observability.metrics.durationBuckets=0.1\,1`, "--set-string", `extraArgs.metrics\.duration-buckets=5\,10`)
	if strings.Count(both, flag) != 1 || !strings.Contains(both, flag+"5,10") {
		t.Fatalf("extraArgs must win and the flag must appear once:\n%s", both)
	}
}
