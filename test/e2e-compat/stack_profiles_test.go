//go:build e2e

package e2e_compat

import (
	"net/url"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type composeFile struct {
	Services map[string]struct {
		ContainerName string         `yaml:"container_name"`
		Profiles      []string       `yaml:"profiles"`
		Ports         []string       `yaml:"ports"`
		Command       composeCommand `yaml:"command"`
	} `yaml:"services"`
}

// composeCommand accepts both the list form (proxies) and the string form
// (Loki) of a compose command.
type composeCommand []string

func (c *composeCommand) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		*c = strings.Fields(node.Value)
		return nil
	}
	var list []string
	if err := node.Decode(&list); err != nil {
		return err
	}
	*c = list
	return nil
}

type datasourceFile struct {
	Datasources []struct {
		Name      string `yaml:"name"`
		Type      string `yaml:"type"`
		URL       string `yaml:"url"`
		IsDefault bool   `yaml:"isDefault"`
	} `yaml:"datasources"`
}

// proxyFlag returns the value of -name in a compose command list, or the
// proxy's built-in default when the service does not set it.
func proxyFlag(command []string, name, def string) string {
	for _, arg := range command {
		if v, ok := strings.CutPrefix(arg, "-"+name+"="); ok {
			return v
		}
	}
	return def
}

// The stack is a Loki-compatibility testing target: the datasources a user
// opens in Grafana - Explore's "Loki (via VL proxy)" (and its multi-tenant
// twin) and the default datasource Logs Drilldown uses - must run the
// Loki-compatible profile, so the owner and the Playwright shards see
// exactly what a Loki user sees and any leak of a proxy-only name fails.
// The OTel hybrid and native metadata profiles stay available as explicitly
// named datasources. Every proxy variant but the parity proxy and the
// Grafana-facing ones skips the background label warm-up, and the
// benchmark-only peer ring sits behind the "peers" profile, so one
// VictoriaLogs is not flooded by eleven keep-warm loops.
//
// conformance: loki-compatible-profile, quality/label-cache-warm
func TestCompat_StackProfilesMatchDatasources(t *testing.T) {
	raw, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse docker-compose.yml: %v", err)
	}
	raw, err = os.ReadFile("grafana-datasources.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var datasources datasourceFile
	if err := yaml.Unmarshal(raw, &datasources); err != nil {
		t.Fatalf("parse grafana-datasources.yaml: %v", err)
	}

	profileOf := func(service string) string {
		svc, ok := compose.Services[service]
		if !ok {
			t.Fatalf("compose has no service %q", service)
		}
		style := proxyFlag(svc.Command, "label-style", "underscores")
		mode := proxyFlag(svc.Command, "metadata-field-mode", "translated")
		switch {
		case style == "underscores" && mode == "translated":
			return "loki"
		case mode == "hybrid":
			return "otel-hybrid"
		default:
			return "native"
		}
	}
	serviceOf := func(dsURL string) string {
		u, err := url.Parse(dsURL)
		if err != nil {
			t.Fatalf("datasource url %q: %v", dsURL, err)
		}
		return u.Hostname()
	}

	want := map[string]string{
		"Loki (via VL proxy)":                     "loki",
		"Loki (via VL proxy multi-tenant)":        "loki",
		"Loki (via VL proxy patterns autodetect)": "loki",
		"Loki (via VL proxy OTel hybrid)":         "otel-hybrid",
		"Loki (via VL proxy native metadata)":     "native",
	}
	seen := map[string]bool{}
	defaults := 0
	for _, ds := range datasources.Datasources {
		if ds.IsDefault {
			defaults++
			if got := profileOf(serviceOf(ds.URL)); got != "loki" {
				t.Errorf("default datasource %q (Logs Drilldown) runs the %s profile, want the Loki-compatible profile", ds.Name, got)
			}
		}
		profile, ok := want[ds.Name]
		if !ok {
			continue
		}
		seen[ds.Name] = true
		if got := profileOf(serviceOf(ds.URL)); got != profile {
			t.Errorf("datasource %q -> %s runs the %s profile, want %s", ds.Name, serviceOf(ds.URL), got, profile)
		}
	}
	if defaults != 1 {
		t.Errorf("want exactly one default datasource, got %d", defaults)
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("grafana-datasources.yaml has no %q datasource", name)
		}
	}

	// The parity proxy and every proxy the CI parity groups compare with Loki
	// run the Loki-compatible profile.
	for _, service := range []string{"loki-vl-proxy", "loki-vl-proxy-underscore", "loki-vl-proxy-patterns-autodetect", "loki-vl-proxy-vmauth", "loki-vl-proxy-no-metadata", "loki-vl-proxy-translated-metadata"} {
		if got := profileOf(service); got != "loki" {
			t.Errorf("%s runs the %s profile, want the Loki-compatible profile", service, got)
		}
	}
	if got := proxyFlag(compose.Services["loki-vl-proxy-no-metadata"].Command, "emit-structured-metadata", "true"); got != "false" {
		t.Errorf("loki-vl-proxy-no-metadata must keep -emit-structured-metadata=false, got %s", got)
	}

	warmed := map[string]bool{"loki-vl-proxy": true, "loki-vl-proxy-underscore": true, "loki-vl-proxy-patterns-autodetect": true}
	for name, svc := range compose.Services {
		if !strings.HasPrefix(name, "loki-vl-proxy") {
			continue
		}
		warm := proxyFlag(svc.Command, "labels-cache-warm", "true") == "true"
		if warm != warmed[name] {
			t.Errorf("%s: labels-cache-warm=%v, want %v (only the parity and Grafana-facing Loki-mode proxies warm)", name, warm, warmed[name])
		}
	}
	for _, name := range []string{"loki-vl-proxy-peer-a", "loki-vl-proxy-peer-b", "vmauth-ring"} {
		if profiles := compose.Services[name].Profiles; len(profiles) != 1 || profiles[0] != "peers" {
			t.Errorf("%s must sit behind the benchmark-only \"peers\" compose profile, got %v", name, profiles)
		}
	}
}
