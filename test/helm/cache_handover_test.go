//go:build helm

package helm

import (
	"bytes"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart's restart hand-over options (cacheHandover.*): the proxy flags,
// a grace period that covers the drain and the flush, the preStop sleep, and
// the refusal to enable it without a disk cache.
// conformance: cache-restart-handover
func TestCacheHandoverChartOptions(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		pod := renderPodSpec(t, nil)
		if args := containerArgs(pod); strings.Contains(args, "-cache-shutdown-flush-timeout") || strings.Contains(args, "-cache-startup-warm") {
			t.Fatalf("hand-over flags rendered by default: %s", args)
		}
		if got := pod["terminationGracePeriodSeconds"]; got != 30 {
			t.Fatalf("terminationGracePeriodSeconds %v, want the default 30", got)
		}
		if _, ok := container(pod)["lifecycle"]; ok {
			t.Fatal("lifecycle rendered by default")
		}
	})
	t.Run("on", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true"})
		args := containerArgs(pod)
		for _, want := range []string{"-cache-shutdown-flush-timeout=15s", "-cache-startup-warm-bytes=134217728", "-cache-startup-warm-timeout=10s", "-disk-cache-path="} {
			if !strings.Contains(args, want) {
				t.Fatalf("args %s: missing %s", args, want)
			}
		}
		// preStop 5 + HTTP drain 30 + 30 + flush 15 + close 5.
		if got := pod["terminationGracePeriodSeconds"]; got != 85 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 85", got)
		}
		preStop, _ := container(pod)["lifecycle"].(map[string]any)["preStop"].(map[string]any)
		if sleep, _ := preStop["sleep"].(map[string]any); sleep["seconds"] != 5 {
			t.Fatalf("preStop %v, want a 5 s sleep", preStop)
		}
	})
	t.Run("operator values win", func(t *testing.T) {
		pod := renderPodSpec(t, []string{
			"cacheHandover.enabled=true", "persistence.enabled=true",
			"terminationGracePeriodSeconds=120",
			"extraArgs.cache-shutdown-flush-timeout=40s",
			"lifecycle.preStop.sleep.seconds=9",
		})
		args := containerArgs(pod)
		if strings.Count(args, "-cache-shutdown-flush-timeout") != 1 || !strings.Contains(args, "-cache-shutdown-flush-timeout=40s") {
			t.Fatalf("args %s: the extraArgs value must be the only one", args)
		}
		if got := pod["terminationGracePeriodSeconds"]; got != 120 {
			t.Fatalf("terminationGracePeriodSeconds %v, want the larger configured 120", got)
		}
		preStop, _ := container(pod)["lifecycle"].(map[string]any)["preStop"].(map[string]any)
		if sleep, _ := preStop["sleep"].(map[string]any); sleep["seconds"] != 9 {
			t.Fatalf("preStop %v, want the configured lifecycle", preStop)
		}
	})
	t.Run("no preStop sleep before Kubernetes 1.30", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true"}, "--kube-version", "1.29.0")
		if _, ok := container(pod)["lifecycle"]; ok {
			t.Fatal("preStop sleep rendered for Kubernetes 1.29")
		}
		if got := pod["terminationGracePeriodSeconds"]; got != 80 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 80 without the sleep", got)
		}
	})
	t.Run("a configured sleep counts", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true", "lifecycle.preStop.sleep.seconds=20"})
		if got := pod["terminationGracePeriodSeconds"]; got != 100 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 100 (20 + 60 + 15 + 5)", got)
		}
	})
	t.Run("needs a disk cache", func(t *testing.T) {
		out, err := helmTemplate(t, []string{"cacheHandover.enabled=true"})
		if err == nil || !strings.Contains(out, "cacheHandover.enabled=true needs the disk cache") {
			t.Fatalf("render without a disk cache: err=%v\n%s", err, out)
		}
		if _, err := helmTemplate(t, []string{"cacheHandover.enabled=true", "extraArgs.disk-cache-path=/data/cache.db"}); err != nil {
			t.Fatalf("extraArgs.disk-cache-path must be accepted: %v", err)
		}
	})
}

func helmTemplate(t *testing.T, sets []string, extra ...string) (string, error) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate chart")
	}
	chart := filepath.Join(filepath.Dir(file), "..", "..", "charts", "loki-vl-proxy")
	args := []string{"template", "handover", chart, "--namespace", "cache-test"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	args = append(args, extra...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	return string(out), err
}

func renderPodSpec(t *testing.T, sets []string, extra ...string) map[string]any {
	t.Helper()
	out, err := helmTemplate(t, sets, extra...)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	decoder := yaml.NewDecoder(bytes.NewReader([]byte(out)))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode Helm manifest: %v", err)
		}
		if doc["kind"] == "Deployment" || doc["kind"] == "StatefulSet" {
			spec := doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
			return spec
		}
	}
	t.Fatal("no workload rendered")
	return nil
}

func container(pod map[string]any) map[string]any {
	return pod["containers"].([]any)[0].(map[string]any)
}

func containerArgs(pod map[string]any) string {
	var parts []string
	for _, a := range container(pod)["args"].([]any) {
		parts = append(parts, a.(string))
	}
	return strings.Join(parts, " ")
}
