//go:build helm

package helm

import (
	"os/exec"
	"strings"
	"testing"
)

var everySecret = []string{
	"--set", "peerCache.enabled=true",
	"--set", "adminAuth.existingSecret=admin-sec",
	"--set", "deleteAuth.existingSecret=delete-sec",
	"--set", "extraArgs.delete-api-enabled=true",
	"--set", "backendAuth.existingSecret=backend-sec",
	"--set", "backendVersionAuth.existingSecret=version-sec",
	"--set", "backendVersionAuth.existingSecretKey=custom-key",
	"--set", "cacheKeySecret.existingSecret=cache-key-sec",
}

// Every secret the chart takes from a Secret is a mounted file named by a
// -file flag: nothing in the arguments or environment holds it, and a binary
// that predates the flag exits on it instead of starting without the secret.
//
// conformance: secrets-outside-process-arguments
func TestSecretsAreMountedFiles(t *testing.T) {
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		rendered := renderChart(t, append([]string{"--set", "workload.kind=" + kind}, everySecret...)...)
		for _, s := range []struct{ flag, volume, dir, file, secret, key string }{
			{"peer-auth-token", "secret-peer-auth", "peer-auth", "token", "scrape-peer-auth", "token"},
			{"server.admin-auth-token", "secret-admin-auth", "admin-auth", "token", "admin-sec", "token"},
			{"delete-api-auth-token", "secret-delete-auth", "delete-auth", "token", "delete-sec", "token"},
			{"backend-basic-auth", "secret-backend-auth", "backend-auth", "basic-auth", "backend-sec", "basic-auth"},
			{"backend-version-basic-auth", "secret-backend-version-auth", "backend-version-auth", "basic-auth", "version-sec", "custom-key"},
			{"cache-key-secret", "secret-cache-key", "cache-key", "secret", "cache-key-sec", "secret"},
		} {
			dir := "/var/run/secrets/loki-vl-proxy/" + s.dir
			for _, want := range []string{
				"\n            - \"-" + s.flag + "-file=" + dir + "/" + s.file + "\"\n",
				"\n            - name: " + s.volume + "\n              mountPath: " + dir + "\n              readOnly: true\n",
				"\n        - name: " + s.volume + "\n          secret:\n            secretName: \"" + s.secret + "\"\n            items:\n              - key: \"" + s.key + "\"\n                path: " + s.file + "\n",
			} {
				if !strings.Contains(rendered, want) {
					t.Errorf("%s %s: missing %q", kind, s.flag, want)
				}
			}
			if strings.Contains(rendered, "-"+s.flag+"=") {
				t.Errorf("%s: -%s is still rendered with a value", kind, s.flag)
			}
		}
		for _, gone := range []string{
			"secretKeyRef", "PEER_AUTH_TOKEN", "ADMIN_AUTH_TOKEN", "DELETE_API_AUTH_TOKEN", "BACKEND_BASIC_AUTH", "BACKEND_VERSION_BASIC_AUTH", "CACHE_KEY_SECRET",
		} {
			if strings.Contains(rendered, gone) {
				t.Errorf("%s: rendered workload still holds %q", kind, gone)
			}
		}
	}
}

// conformance: secrets-outside-process-arguments
func TestPeerSecretSource(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		args       []string
	}{
		{"generated", "secretName: \"scrape-peer-auth\"", nil},
		{"literal", "secretName: \"scrape-peer-auth-literal\"", []string{"--set", "peerCache.authToken=sentinel-literal"}},
		{"existing", "secretName: \"mine\"", []string{"--set", "peerCache.existingSecret=mine", "--set", "peerCache.existingSecretKey=k"}},
	} {
		rendered := renderChart(t, append([]string{"--set", "peerCache.enabled=true"}, tc.args...)...)
		if !strings.Contains(rendered, tc.want) {
			t.Errorf("%s: missing %q", tc.name, tc.want)
		}
		if strings.Contains(rendered, "sentinel-literal") {
			t.Errorf("%s: the literal token is rendered in clear", tc.name)
		}
	}
}

// conformance: secrets-outside-process-arguments
func TestNoSecretNoSecretVolumes(t *testing.T) {
	rendered := renderChart(t)
	for _, gone := range []string{"auth-token", "basic-auth", "secret-", "/var/run/secrets/loki-vl-proxy"} {
		if strings.Contains(rendered, gone) {
			t.Errorf("the default render must not hold %q", gone)
		}
	}
}

// A secret mount inside another volume's mount cannot be created when that
// volume is read-only, so no secret path lies under a path an operator mounts
// the chart's own files or configuration at.
//
// conformance: secrets-outside-process-arguments
func TestSecretMountsNestUnderNoMount(t *testing.T) {
	rendered := renderChart(t, append([]string{
		"--set", "persistence.enabled=true",
		"--set", "patternsCustom.file.enabled=true",
		"--set", "patternsCustom.file.path=/etc/loki-vl-proxy/custom-patterns.json",
		"--set", "extraVolumes[0].name=config",
		"--set", "extraVolumes[0].configMap.name=config",
		"--set", "extraVolumeMounts[0].name=config",
		"--set", "extraVolumeMounts[0].mountPath=/etc/loki-vl-proxy",
	}, everySecret...)...)
	var paths []string
	for _, line := range strings.Split(rendered, "\n") {
		if _, path, found := strings.Cut(strings.TrimSpace(line), "mountPath: "); found {
			paths = append(paths, strings.Trim(path, `"`)+"/")
		}
	}
	secrets := 0
	for _, secret := range paths {
		if !strings.HasPrefix(secret, "/var/run/secrets/loki-vl-proxy/") {
			continue
		}
		secrets++
		for _, other := range paths {
			if other != secret && strings.HasPrefix(secret, other) {
				t.Errorf("secret mount %s lies under %s", secret, other)
			}
		}
	}
	if secrets != 6 {
		t.Fatalf("want 6 secret mounts, got %d among %v", secrets, paths)
	}
}

// Plain values in extraArgs, value or file, keep rendering as written; next to
// the same secret from a Secret they fail rendering.
//
// conformance: secrets-outside-process-arguments
func TestSecretExtraArgs(t *testing.T) {
	for _, s := range []struct {
		flag, key string
		from      []string
	}{
		{"peer-auth-token", "peer-auth-token", []string{"--set", "peerCache.enabled=true"}},
		{"server.admin-auth-token", `server\.admin-auth-token`, []string{"--set", "adminAuth.existingSecret=s"}},
		{"delete-api-auth-token", "delete-api-auth-token", []string{"--set", "deleteAuth.existingSecret=s"}},
		{"backend-basic-auth", "backend-basic-auth", []string{"--set", "backendAuth.existingSecret=s"}},
		{"backend-version-basic-auth", "backend-version-basic-auth", []string{"--set", "backendVersionAuth.existingSecret=s"}},
		{"cache-key-secret", "cache-key-secret", []string{"--set", "cacheKeySecret.existingSecret=s"}},
	} {
		for _, suffix := range []string{"", "-file"} {
			set := []string{"--set", "extraArgs." + s.key + suffix + "=user:plain"}
			rendered := renderChart(t, set...)
			if want := "\"-" + s.flag + suffix + "=user:plain\""; !strings.Contains(rendered, want) {
				t.Errorf("extraArgs %s%s: missing %s", s.flag, suffix, want)
			}
			if strings.Contains(rendered, "/var/run/secrets/loki-vl-proxy") {
				t.Errorf("extraArgs %s%s: unexpected secret mount", s.flag, suffix)
			}
			args := append([]string{"template", "scrape", "../../charts/loki-vl-proxy"}, append(append([]string{}, s.from...), set...)...)
			if out, err := exec.Command("helm", args...).CombinedOutput(); err == nil || !strings.Contains(string(out), "cannot be combined") {
				t.Errorf("extraArgs %s%s next to the Secret: error %v, output %s", s.flag, suffix, err, out)
			}
		}
	}
	renderChart(t, "--set", "extraArgs.delete-api-enabled=true", "--set", "extraArgs.delete-api-auth-token-file=/mnt/token")
}
