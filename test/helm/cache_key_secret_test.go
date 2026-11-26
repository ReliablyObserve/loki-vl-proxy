//go:build helm

package helm

import (
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

const (
	cacheKeyLookupExisting = `lookup "v1" "Secret" .Release.Namespace (printf "%s-cache-key" .Release.Name)`
	cacheKeyLookupProbe    = `lookup "v1" "Secret" .Release.Namespace ""`
)

// renderChartWithLookups renders a copy of the chart whose cache key lookups
// answer as a cluster would: existing is the value the <release>-cache-key
// Secret holds ("" = no such Secret), reachable whether listing Secrets
// answers. helm template has no cluster, so its lookups return nothing.
func renderChartWithLookups(t *testing.T, existing string, reachable bool, args ...string) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	src := filepath.Join(filepath.Dir(file), "..", "..", "charts", "loki-vl-proxy")
	dst := filepath.Join(t.TempDir(), "loki-vl-proxy")
	if out, err := exec.Command("cp", "-R", src, dst).CombinedOutput(); err != nil {
		t.Fatalf("copy chart: %v %s", err, out)
	}
	existingStub := `(dict)`
	if existing != "" {
		existingStub = `(dict "data" (dict "secret" "` + base64.StdEncoding.EncodeToString([]byte(existing)) + `"))`
	}
	probeStub := `(dict)`
	if reachable {
		probeStub = `(dict "kind" "SecretList" "items" (list))`
	}
	replaced := 0
	for _, name := range []string{"_helpers.tpl", "cache-key-secret.yaml"} {
		path := filepath.Join(dst, "templates", name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		replaced += strings.Count(text, cacheKeyLookupExisting) + strings.Count(text, cacheKeyLookupProbe)
		text = strings.ReplaceAll(text, cacheKeyLookupExisting, existingStub)
		text = strings.ReplaceAll(text, cacheKeyLookupProbe, probeStub)
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if replaced != 3 {
		t.Fatalf("expected 3 cache key lookups in the chart, found %d: update the stubs", replaced)
	}
	out, err := exec.Command("helm", append([]string{"template", "scrape", dst, "--namespace", "scrape-test"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, out)
	}
	return string(out)
}

var cacheKeyValue = regexp.MustCompile(`(?s)kind: Secret\nmetadata:\n  name: scrape-cache-key\n.*?\ndata:\n  secret: "([^"]+)"`)

func generatedCacheKey(t *testing.T, rendered string) string {
	t.Helper()
	m := cacheKeyValue.FindStringSubmatch(rendered)
	if m == nil {
		return ""
	}
	raw, err := base64.StdEncoding.DecodeString(m[1])
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

const cacheKeyMount = `"-cache-key-secret-file=/var/run/secrets/loki-vl-proxy/cache-key/secret"`

// The chart keeps one cache key secret per release: created on the first
// install, kept on every upgrade, never replaced by a render that cannot see
// the cluster, and the operator's Secret wins.
//
// conformance: cache-key-credential-digest, secrets-outside-process-arguments
func TestCacheKeySecretGeneratedAndKept(t *testing.T) {
	// First install: a reachable cluster without the Secret.
	first := renderChartWithLookups(t, "", true)
	key := generatedCacheKey(t, first)
	if len(key) != 64 || !strings.Contains(first, cacheKeyMount) || !strings.Contains(first, `secretName: "scrape-cache-key"`) {
		t.Fatalf("first install: key %q, mount %v", key, strings.Contains(first, cacheKeyMount))
	}
	// Upgrade: the stored value is kept.
	for i := 0; i < 2; i++ {
		up := renderChartWithLookups(t, "kept-value-0123456789", true)
		if got := generatedCacheKey(t, up); got != "kept-value-0123456789" || !strings.Contains(up, cacheKeyMount) {
			t.Fatalf("upgrade %d: key %q, want the stored value", i, got)
		}
	}
	// No cluster access (helm template, Argo CD): nothing rendered or mounted,
	// so no render replaces the key.
	for name, rendered := range map[string]string{
		"helm template":      renderChart(t),
		"stubbed no-cluster": renderChartWithLookups(t, "", false),
		"peer cache":         renderChart(t, "--set", "peerCache.enabled=true"),
	} {
		if strings.Contains(rendered, "scrape-cache-key") || strings.Contains(rendered, "cache-key-secret") {
			t.Errorf("%s: a render without cluster access renders a cache key", name)
		}
	}
	// The operator's Secret, generate=false and an extraArgs value win.
	for name, args := range map[string][]string{
		"existingSecret": {"--set", "cacheKeySecret.existingSecret=mine"},
		"generate=false": {"--set", "cacheKeySecret.generate=false"},
		"extraArgs file": {"--set", "extraArgs.cache-key-secret-file=/mnt/k"},
	} {
		rendered := renderChartWithLookups(t, "kept-value-0123456789", true, args...)
		if generatedCacheKey(t, rendered) != "" || strings.Contains(rendered, `secretName: "scrape-cache-key"`) {
			t.Errorf("%s: the chart still renders its own cache key", name)
		}
	}
	if r := renderChartWithLookups(t, "kept-value-0123456789", true, "--set", "cacheKeySecret.existingSecret=mine"); !strings.Contains(r, `secretName: "mine"`) {
		t.Error("existingSecret is not mounted")
	}
}
