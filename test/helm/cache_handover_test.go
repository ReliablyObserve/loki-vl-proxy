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
// a grace period that covers the drain and the flush, the preStop sleep, the
// startup probe, one pod at a time (rollout strategy, PodDisruptionBudget,
// minReadySeconds), explicit settings winning over each, and the refusal to
// enable it without a disk cache.
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
		if _, ok := container(pod)["startupProbe"]; ok {
			t.Fatal("startupProbe rendered by default")
		}
		for _, sets := range [][]string{nil, {"replicaCount=3", "persistence.enabled=true"}, {"horizontalPodAutoscaling.enabled=true"}, {"workload.kind=StatefulSet", "replicaCount=3"}} {
			docs := renderDocs(t, sets)
			if pdb, ok := docs["PodDisruptionBudget"]; ok {
				t.Fatalf("%v: PodDisruptionBudget rendered without the hand-over: %v", sets, pdb)
			}
			spec := workloadSpec(t, docs)
			for _, key := range []string{"strategy", "minReadySeconds"} {
				if v, ok := spec[key]; ok {
					t.Fatalf("%v: %s rendered without the hand-over: %v", sets, key, v)
				}
			}
		}
		// A startup probe enabled without the hand-over keeps its 30 failures.
		enabled := renderPodSpec(t, []string{"probe.startup.enabled=true"})
		assertProbe(t, enabled, "/health", 5, 30)
	})
	t.Run("on", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true"})
		args := containerArgs(pod)
		for _, want := range []string{"-cache-shutdown-flush-timeout=60s", "-cache-startup-warm-bytes=134217728", "-cache-startup-warm-timeout=120s", "-disk-cache-path="} {
			if !strings.Contains(args, want) {
				t.Fatalf("args %s: missing %s", args, want)
			}
		}
		// The peer restore follows the flag default (on) unless turned off.
		if strings.Contains(args, "-cache-startup-peer-restore") {
			t.Fatalf("args %s: the peer restore flag rendered at its default", args)
		}
		for _, sets := range [][]string{
			{"cacheHandover.enabled=true", "persistence.enabled=true", "cacheHandover.peerRestore=false"},
			{"cacheHandover.enabled=true", "persistence.enabled=true", "cacheHandover.peerRestore=false", "extraArgs.cache-startup-peer-restore=true"},
		} {
			off := containerArgs(renderPodSpec(t, sets))
			if got := strings.Count(off, "-cache-startup-peer-restore"); got != 1 {
				t.Fatalf("%v: peer restore flag rendered %d times: %s", sets, got, off)
			}
		}
		// preStop 5 + HTTP drain 30 + 30 + flush 60 + close 5.
		if got := pod["terminationGracePeriodSeconds"]; got != 130 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 130", got)
		}
		preStop, _ := container(pod)["lifecycle"].(map[string]any)["preStop"].(map[string]any)
		if sleep, _ := preStop["sleep"].(map[string]any); sleep["seconds"] != 5 {
			t.Fatalf("preStop %v, want a 5 s sleep", preStop)
		}
	})
	t.Run("operator values win", func(t *testing.T) {
		pod := renderPodSpec(t, []string{
			"cacheHandover.enabled=true", "persistence.enabled=true",
			"terminationGracePeriodSeconds=180",
			"extraArgs.cache-shutdown-flush-timeout=40s",
			"lifecycle.preStop.sleep.seconds=9",
		})
		args := containerArgs(pod)
		if strings.Count(args, "-cache-shutdown-flush-timeout") != 1 || !strings.Contains(args, "-cache-shutdown-flush-timeout=40s") {
			t.Fatalf("args %s: the extraArgs value must be the only one", args)
		}
		if got := pod["terminationGracePeriodSeconds"]; got != 180 {
			t.Fatalf("terminationGracePeriodSeconds %v, want the larger configured 180", got)
		}
		preStop, _ := container(pod)["lifecycle"].(map[string]any)["preStop"].(map[string]any)
		if sleep, _ := preStop["sleep"].(map[string]any); sleep["seconds"] != 9 {
			t.Fatalf("preStop %v, want the configured lifecycle", preStop)
		}
	})
	t.Run("a configured flush budget wins", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true", "cacheHandover.shutdownFlushTimeoutSeconds=15"})
		if args := containerArgs(pod); !strings.Contains(args, "-cache-shutdown-flush-timeout=15s") {
			t.Fatalf("args %s: the configured flush budget", args)
		}
		if got := pod["terminationGracePeriodSeconds"]; got != 85 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 85 for a 15 s flush", got)
		}
	})
	on := []string{"cacheHandover.enabled=true", "persistence.enabled=true"}
	with := func(sets ...string) []string { return append(append([]string{}, on...), sets...) }
	t.Run("startup probe", func(t *testing.T) {
		pod := renderPodSpec(t, on)
		// 5 s + 120 x 5 s for the listener to open; never /ready, which also
		// checks VictoriaLogs.
		assertProbe(t, pod, "/health", 5, 120)
		if path := probeField(pod, "livenessProbe", "path"); path != "/alive" {
			t.Fatalf("liveness path %v, want /alive", path)
		}
		if path := probeField(pod, "readinessProbe", "path"); path != "/ready" {
			t.Fatalf("readiness path %v, want /ready", path)
		}
		pod = renderPodSpec(t, with("probe.startup.path=/alive", "probe.startup.periodSeconds=10", "probe.startup.failureThreshold=90"))
		assertProbe(t, pod, "/alive", 10, 90)
		// An explicit probe.startup.enabled=true keeps the other settings' rules.
		assertProbe(t, renderPodSpec(t, with("probe.startup.enabled=true", "probe.startup.failureThreshold=7")), "/health", 5, 7)
		if probe, ok := container(renderPodSpec(t, with("probe.startup.enabled=false")))["startupProbe"]; ok {
			t.Fatalf("startupProbe %v rendered although disabled", probe)
		}
	})
	t.Run("one pod at a time", func(t *testing.T) {
		docs := renderDocs(t, on)
		spec := workloadSpec(t, docs)
		assertRolling(t, spec["strategy"], 0, 1)
		if spec["minReadySeconds"] != 15 {
			t.Fatalf("minReadySeconds %v, want 15", spec["minReadySeconds"])
		}
		if pdb, ok := docs["PodDisruptionBudget"]; ok {
			t.Fatalf("PodDisruptionBudget rendered for one replica: %v", pdb)
		}
		for _, sets := range [][]string{with("replicaCount=3"), with("horizontalPodAutoscaling.enabled=true")} {
			assertBudget(t, renderDocs(t, sets), "maxUnavailable", 1)
		}
		spec = workloadSpec(t, renderDocs(t, with("cacheHandover.minReadySeconds=0")))
		if v, ok := spec["minReadySeconds"]; ok {
			t.Fatalf("minReadySeconds %v rendered for 0", v)
		}
	})
	t.Run("a StatefulSet updates one pod at a time already", func(t *testing.T) {
		docs := renderDocs(t, with("workload.kind=StatefulSet", "replicaCount=3"))
		spec := workloadSpec(t, docs)
		// RollingUpdate without maxUnavailable is one pod at a time, whatever
		// the pod management policy; the policy is immutable and stays.
		strategy, _ := spec["updateStrategy"].(map[string]any)
		if len(strategy) != 1 || strategy["type"] != "RollingUpdate" {
			t.Fatalf("updateStrategy %v, want only type RollingUpdate", strategy)
		}
		if spec["podManagementPolicy"] != "Parallel" {
			t.Fatalf("podManagementPolicy %v, want the unchanged Parallel", spec["podManagementPolicy"])
		}
		if v, ok := spec["strategy"]; ok {
			t.Fatalf("Deployment strategy %v in a StatefulSet", v)
		}
		if spec["minReadySeconds"] != 15 {
			t.Fatalf("minReadySeconds %v, want 15", spec["minReadySeconds"])
		}
		assertBudget(t, docs, "maxUnavailable", 1)
	})
	t.Run("explicit rollout settings win", func(t *testing.T) {
		spec := workloadSpec(t, renderDocs(t, with("workload.deploymentStrategy.type=Recreate", "cacheHandover.minReadySeconds=40")))
		if strategy, _ := spec["strategy"].(map[string]any); len(strategy) != 1 || strategy["type"] != "Recreate" {
			t.Fatalf("strategy %v, want the configured Recreate alone", spec["strategy"])
		}
		if spec["minReadySeconds"] != 40 {
			t.Fatalf("minReadySeconds %v, want 40", spec["minReadySeconds"])
		}
		spec = workloadSpec(t, renderDocs(t, with("workload.deploymentStrategy.rollingUpdate.maxSurge=1")))
		rolling, _ := spec["strategy"].(map[string]any)["rollingUpdate"].(map[string]any)
		if len(rolling) != 1 || rolling["maxSurge"] != 1 {
			t.Fatalf("rollingUpdate %v, want the configured value alone", rolling)
		}
		three := with("replicaCount=3")
		set := func(sets ...string) []string { return append(append([]string{}, three...), sets...) }
		if pdb, ok := renderDocs(t, set("podDisruptionBudget.enabled=false"))["PodDisruptionBudget"]; ok {
			t.Fatalf("PodDisruptionBudget %v rendered although disabled", pdb)
		}
		assertBudget(t, renderDocs(t, set("podDisruptionBudget.enabled=true")), "minAvailable", 1)
		assertBudget(t, renderDocs(t, set("podDisruptionBudget.enabled=true", "podDisruptionBudget.minAvailable=null", "podDisruptionBudget.maxUnavailable=2")), "maxUnavailable", 2)
		assertBudget(t, renderDocs(t, set("podDisruptionBudget.maxUnavailable=2")), "maxUnavailable", 2)
		assertBudget(t, renderDocs(t, set("podDisruptionBudget.maxUnavailable=0")), "maxUnavailable", 0)
		assertBudget(t, renderDocs(t, set("podDisruptionBudget.minAvailable=2")), "minAvailable", 2)
		// An explicit budget does not depend on the replica count.
		assertBudget(t, renderDocs(t, with("podDisruptionBudget.enabled=true")), "minAvailable", 1)
	})
	t.Run("no preStop sleep before Kubernetes 1.30", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true"}, "--kube-version", "1.29.0")
		if _, ok := container(pod)["lifecycle"]; ok {
			t.Fatal("preStop sleep rendered for Kubernetes 1.29")
		}
		if got := pod["terminationGracePeriodSeconds"]; got != 125 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 125 without the sleep", got)
		}
	})
	t.Run("a configured sleep counts", func(t *testing.T) {
		pod := renderPodSpec(t, []string{"cacheHandover.enabled=true", "persistence.enabled=true", "lifecycle.preStop.sleep.seconds=20"})
		if got := pod["terminationGracePeriodSeconds"]; got != 145 {
			t.Fatalf("terminationGracePeriodSeconds %v, want 145 (20 + 60 + 60 + 5)", got)
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

// The chart's startup window and Deployment progress deadline for a disk cache
// (#1766): without one nothing changes, with one the startup probe allows
// 605 s for the cache index rebuild before the listener opens and a Deployment's
// progressDeadlineSeconds covers one replica's replacement; explicit probe and
// deadline values win.
// conformance: cache-restart-handover
func TestDiskCacheStartupWindowAndProgressDeadline(t *testing.T) {
	const (
		persistence = "persistence.enabled=true"
		handover    = "cacheHandover.enabled=true"
	)
	deadline := func(t *testing.T, sets []string, want int) {
		t.Helper()
		got, ok := workloadSpec(t, renderDocs(t, sets))["progressDeadlineSeconds"]
		if want == 0 && ok {
			t.Fatalf("%v: progressDeadlineSeconds %v rendered, want none", sets, got)
		}
		if want != 0 && got != want {
			t.Fatalf("%v: progressDeadlineSeconds %v, want %d", sets, got, want)
		}
	}
	t.Run("no disk cache", func(t *testing.T) {
		for _, sets := range [][]string{
			nil,
			{"replicaCount=3"},
			{"horizontalPodAutoscaling.enabled=true"},
			{"workload.kind=StatefulSet", "replicaCount=3"},
			{"extraArgs.disk-cache-path="},
			// The chart passes no path of its own beside the key.
			{persistence, "extraArgs.disk-cache-path="},
		} {
			if probe, ok := container(renderPodSpec(t, sets))["startupProbe"]; ok {
				t.Fatalf("%v: startupProbe %v rendered without a disk cache", sets, probe)
			}
			deadline(t, sets, 0)
		}
		// A probe asked for without a disk cache keeps 30 failures.
		enabled := []string{"probe.startup.enabled=true", "probe.startup.periodSeconds=60"}
		assertProbe(t, renderPodSpec(t, enabled), "/health", 60, 30)
		deadline(t, enabled, 0)
	})
	t.Run("disk cache without the hand-over", func(t *testing.T) {
		for _, sets := range [][]string{{persistence}, {"extraArgs.disk-cache-path=/data/cache.db"}} {
			pod := renderPodSpec(t, sets)
			// 5 s + 120 x 5 s = 605 s.
			assertProbe(t, pod, "/health", 5, 120)
			if got := probeField(pod, "startupProbe", "initialDelaySeconds"); got != 5 {
				t.Fatalf("%v: startupProbe initialDelaySeconds %v, want 5", sets, got)
			}
			if got := pod["terminationGracePeriodSeconds"]; got != 30 {
				t.Fatalf("%v: terminationGracePeriodSeconds %v, want the default 30", sets, got)
			}
			// grace 30 + window 605 + margin 300.
			deadline(t, sets, 935)
			if v, ok := workloadSpec(t, renderDocs(t, sets))["minReadySeconds"]; ok {
				t.Fatalf("%v: minReadySeconds %v rendered without the hand-over", sets, v)
			}
		}
	})
	t.Run("disk cache with the hand-over", func(t *testing.T) {
		sets := []string{persistence, handover}
		assertProbe(t, renderPodSpec(t, sets), "/health", 5, 120)
		// grace 130 + window 605 + warm-up 120 + minReady 15 + margin 300.
		deadline(t, sets, 1170)
		deadline(t, append(sets, "cacheHandover.startupWarmTimeoutSeconds=600", "cacheHandover.minReadySeconds=0",
			"terminationGracePeriodSeconds=200"), 200+605+600+300)
	})
	t.Run("probe values win and move the deadline", func(t *testing.T) {
		sets := []string{persistence, "probe.startup.initialDelaySeconds=10", "probe.startup.periodSeconds=10", "probe.startup.failureThreshold=100"}
		assertProbe(t, renderPodSpec(t, sets), "/health", 10, 100)
		deadline(t, sets, 30+1010+300)
		// A short window keeps the Kubernetes default of 600 s.
		short := []string{persistence, "probe.startup.failureThreshold=7"}
		assertProbe(t, renderPodSpec(t, short), "/health", 5, 7)
		deadline(t, short, 0)
		// No startup probe, no longer window.
		for _, off := range [][]string{{persistence, "probe.startup.enabled=false"}, {persistence, handover, "probe.startup.enabled=false"}} {
			if probe, ok := container(renderPodSpec(t, off))["startupProbe"]; ok {
				t.Fatalf("%v: startupProbe %v rendered although disabled", off, probe)
			}
			deadline(t, off, 0)
		}
	})
	t.Run("a configured deadline wins", func(t *testing.T) {
		for _, sets := range [][]string{nil, {persistence}, {persistence, handover}, {persistence, "probe.startup.enabled=false"}} {
			deadline(t, append(sets, "workload.progressDeadlineSeconds=700"), 700)
		}
	})
	t.Run("a StatefulSet has no progress deadline", func(t *testing.T) {
		for _, sets := range [][]string{
			{"workload.kind=StatefulSet", persistence},
			{"workload.kind=StatefulSet", persistence, handover},
			{"workload.kind=StatefulSet", persistence, "workload.progressDeadlineSeconds=700"},
		} {
			assertProbe(t, renderPodSpec(t, sets), "/health", 5, 120)
			deadline(t, sets, 0)
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

// renderDocs renders the chart and returns its manifests by kind.
func renderDocs(t *testing.T, sets []string, extra ...string) map[string]map[string]any {
	t.Helper()
	out, err := helmTemplate(t, sets, extra...)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	docs := map[string]map[string]any{}
	decoder := yaml.NewDecoder(bytes.NewReader([]byte(out)))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err == io.EOF {
			return docs
		} else if err != nil {
			t.Fatalf("decode Helm manifest: %v", err)
		}
		if kind, ok := doc["kind"].(string); ok {
			docs[kind] = doc
		}
	}
}

func workloadSpec(t *testing.T, docs map[string]map[string]any) map[string]any {
	t.Helper()
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		if doc, ok := docs[kind]; ok {
			return doc["spec"].(map[string]any)
		}
	}
	t.Fatal("no workload rendered")
	return nil
}

func renderPodSpec(t *testing.T, sets []string, extra ...string) map[string]any {
	t.Helper()
	spec := workloadSpec(t, renderDocs(t, sets, extra...))
	return spec["template"].(map[string]any)["spec"].(map[string]any)
}

func probeField(pod map[string]any, probe, field string) any {
	p, _ := container(pod)[probe].(map[string]any)
	if field == "path" {
		get, _ := p["httpGet"].(map[string]any)
		return get[field]
	}
	return p[field]
}

func assertProbe(t *testing.T, pod map[string]any, path string, period, failures int) {
	t.Helper()
	if got := probeField(pod, "startupProbe", "path"); got != path {
		t.Fatalf("startupProbe path %v, want %s", got, path)
	}
	if got := probeField(pod, "startupProbe", "periodSeconds"); got != period {
		t.Fatalf("startupProbe periodSeconds %v, want %d", got, period)
	}
	if got := probeField(pod, "startupProbe", "failureThreshold"); got != failures {
		t.Fatalf("startupProbe failureThreshold %v, want %d", got, failures)
	}
}

func assertRolling(t *testing.T, strategy any, surge, unavailable int) {
	t.Helper()
	s, _ := strategy.(map[string]any)
	rolling, _ := s["rollingUpdate"].(map[string]any)
	if s["type"] != "RollingUpdate" || rolling["maxSurge"] != surge || rolling["maxUnavailable"] != unavailable {
		t.Fatalf("strategy %v, want RollingUpdate with maxSurge %d and maxUnavailable %d", strategy, surge, unavailable)
	}
}

// assertBudget requires a PodDisruptionBudget with exactly one of
// minAvailable and maxUnavailable: the API server refuses both.
func assertBudget(t *testing.T, docs map[string]map[string]any, field string, want int) {
	t.Helper()
	pdb, ok := docs["PodDisruptionBudget"]
	if !ok {
		t.Fatal("no PodDisruptionBudget rendered")
	}
	spec := pdb["spec"].(map[string]any)
	other := "minAvailable"
	if field == other {
		other = "maxUnavailable"
	}
	if _, both := spec[other]; both || spec[field] != want {
		t.Fatalf("PodDisruptionBudget spec %v, want only %s: %d", spec, field, want)
	}
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
