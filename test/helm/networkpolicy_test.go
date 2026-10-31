//go:build helm

package helm

import (
	"bytes"
	"io"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// These tests require Helm and run explicitly in the CI helm job.
func TestPeerCacheNetworkPolicy(t *testing.T) {
	defaultEgress := []map[string]any{
		{"to": []any{}, "ports": []any{map[string]any{"port": 9428, "protocol": "TCP"}}},
		{"to": []any{}, "ports": []any{
			map[string]any{"port": 53, "protocol": "UDP"},
			map[string]any{"port": 53, "protocol": "TCP"},
		}},
	}
	customEgress := []map[string]any{{
		"to":    []any{map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]any{"name": "backend"}}}},
		"ports": []any{map[string]any{"port": 8443, "protocol": "TCP"}},
	}}
	for _, tc := range []struct {
		name     string
		override string
		egress   []map[string]any
	}{
		{name: "default", egress: defaultEgress},
		{name: "custom", override: `networkPolicy:
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: backend
      ports:
        - port: 8443
          protocol: TCP
`, egress: customEgress},
		{name: "empty", override: "networkPolicy:\n  egress: []\n"},
	} {
		for _, enabled := range []bool{false, true} {
			t.Run(tc.name+"/peer="+strconv.FormatBool(enabled), func(t *testing.T) {
				policy := renderNetworkPolicy(t, "cache", tc.override, enabled, true)
				want := append([]map[string]any(nil), tc.egress...)
				if enabled {
					want = append(want, peerEgress("cache"))
				}
				got := egressRules(t, policy)
				// Exact structured equality also rejects extra ports (admin/metrics),
				// unrestricted peers, namespace selectors and changed operator rules.
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("egress = %#v; want %#v", got, want)
				}
			})
		}
	}
	for _, enabled := range []bool{false, true} {
		t.Run("disabled-policy/peer="+strconv.FormatBool(enabled), func(t *testing.T) {
			if policy := renderNetworkPolicy(t, "cache", "", enabled, false); policy != nil {
				t.Fatalf("disabled NetworkPolicy rendered: %#v", policy)
			}
		})
	}
	t.Run("release-isolation", func(t *testing.T) {
		// A selector must follow the release, rather than permitting all pods
		// with the same chart name. No namespaceSelector means same namespace.
		for _, release := range []string{"cache", "other-cache"} {
			policy := renderNetworkPolicy(t, release, "networkPolicy:\n  egress: []\n", true, true)
			egress := egressRules(t, policy)
			if len(egress) != 1 || !reflect.DeepEqual(egress[0], peerEgress(release)) {
				t.Fatalf("release %q: peer egress = %#v", release, egress)
			}
		}
	})
}

func egressRules(t *testing.T, policy map[string]any) []map[string]any {
	t.Helper()
	data, err := yaml.Marshal(policy["spec"].(map[string]any)["egress"])
	if err != nil {
		t.Fatal(err)
	}
	var rules []map[string]any
	if err := yaml.Unmarshal(data, &rules); err != nil {
		t.Fatal(err)
	}
	return rules
}

func peerEgress(release string) map[string]any {
	return map[string]any{
		"to": []any{map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{
			"app.kubernetes.io/name": "loki-vl-proxy", "app.kubernetes.io/instance": release,
		}}}},
		"ports": []any{map[string]any{"port": 3100, "protocol": "TCP"}},
	}
}

func renderNetworkPolicy(t *testing.T, release, override string, peer, enabled bool) map[string]any {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate chart")
	}
	chart := filepath.Join(filepath.Dir(file), "..", "..", "charts", "loki-vl-proxy")
	cmd := exec.Command("helm", "template", release, chart, "--namespace", "cache-test", "-f", "-",
		"--set", "peerCache.enabled="+strconv.FormatBool(peer), "--set", "networkPolicy.enabled="+strconv.FormatBool(enabled),
		"--set", "peerCache.existingSecret=peer-auth")
	cmd.Stdin = strings.NewReader(override)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, output)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(output))
	var policy map[string]any
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode Helm manifest: %v", err)
		}
		if document["kind"] == "NetworkPolicy" {
			if policy != nil {
				t.Fatal("multiple NetworkPolicies rendered")
			}
			policy = document
		}
	}
	if enabled && policy == nil {
		t.Fatal("NetworkPolicy missing")
	}
	return policy
}
