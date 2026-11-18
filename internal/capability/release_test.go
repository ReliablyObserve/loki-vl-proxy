package capability

import (
	"strings"
	"testing"
)

// conformance: vl-capability-layer
func TestParseBuildAcceptsOnlyVictoriaLogs(t *testing.T) {
	for _, c := range []struct {
		in      string
		want    string // canonical release, "" when refused
		flavour Flavour
	}{
		{"victoria-logs-20261113-102924-tags-v1.54.0-0-g6433dae586", "v1.54.0", FlavourOSS},
		{"victoria-logs-20260414-184331-tags-v1.50.0-0-g80d223f95f", "v1.50.0", FlavourOSS},
		{"victoria-logs-20261101-000000-tags-v1.52.2-enterprise-0-gabcdef0123", "v1.52.2-enterprise", FlavourEnterprise},
		{"victoria-logs-20261101-000000-tags-v1.54.0-enterprise-fips-0-gabcdef0123", "v1.54.0-enterprise-fips", FlavourEnterpriseFIPS},
		{"victoria-logs-20261101-000000-tags-v1.54.0-1-gabcdef0123-dirty", "v1.54.0", FlavourOSS},
		{"VictoriaLogs/v1.50.0", "v1.50.0", FlavourOSS},
		{"victoria-logs v1.49.0", "v1.49.0", FlavourOSS},
		{"VictoriaLogs/1.52.1", "v1.52.1", FlavourOSS},
		{"victorialogs v1.53.0-enterprise", "v1.53.0-enterprise", FlavourEnterprise},
		{"victoria-logs-20261101-000000-tags-v1.54.0-cluster-0-gabc", "v1.54.0", FlavourUnknown},
		// Other products of the same version library are not VictoriaLogs.
		{"vmauth-20261113-102924-tags-v1.155.0-0-g6433dae586", "", 0},
		{"victoria-metrics-20261113-102924-tags-v1.155.0-0-g6433dae586", "", 0},
		{"vlagent-20261113-102924-tags-v1.54.0-0-g6433dae586", "", 0},
		{"vmagent-20261113-102924-tags-v1.155.0-0-g6433dae586", "", 0},
		{"vmalert-20261113-102924-tags-v1.155.0-0-g6433dae586", "", 0},
		{"VictoriaMetrics/v1.155.0", "", 0},
		// A bare version names nothing.
		{"v1.54.0", "", 0},
		{"1.54.0", "", 0},
		{"nginx/1.25.0", "", 0},
		// Not a version, or a name that only starts like the product.
		{"victoria-logs", "", 0},
		{"victoria-logs-proxy v1.54.0", "", 0},
		{"victoria-logsv1.54.0", "", 0},
		{"victoria-logs-20261113-102924-tags-v1.54", "", 0},
		{"victoria-logs-20261113-102924-tags-v1.54.0x", "", 0},
		{"victoria-logs-20261113-102924-tags-v1.54.0.1", "", 0},
		{"victoria-logs v10000.1.1", "", 0},
		{"", "", 0},
		{strings.Repeat("victoria-logs v1.54.0 ", 20), "", 0},
	} {
		r, ok := ParseBuild(c.in, SourceHeader)
		if c.want == "" {
			if ok {
				t.Errorf("%q accepted as %s", c.in, r)
			}
			continue
		}
		if !ok || r.String() != c.want || r.Flavour != c.flavour || !r.Known || r.Source != SourceHeader || r.Raw != c.in {
			t.Errorf("%q: %+v %v, want %s %v", c.in, r, ok, c.want, c.flavour)
		}
	}
}

func TestParseReleaseTrusted(t *testing.T) {
	for in, want := range map[string]string{
		"v1.53.0": "v1.53.0", "1.52.1": "v1.52.1", "v1.52.2-enterprise": "v1.52.2-enterprise", "v1.54.0-enterprise-fips": "v1.54.0-enterprise-fips",
		"v1.50": "v1.50.0", " v1.49.0 ": "v1.49.0", "": "unknown", "garbage": "unknown", "v1": "unknown", "v1.2.3.4": "unknown",
	} {
		if got := ParseRelease(in).String(); got != want {
			t.Errorf("ParseRelease(%q) = %s, want %s", in, got, want)
		}
	}
	if r := ParseRelease("v1.50.0"); r.Source != SourceConfig || r.Raw != "v1.50.0" {
		t.Fatalf("%+v", r)
	}
}

func TestParseMetrics(t *testing.T) {
	vl := "# HELP vm_app_version x\nvm_app_version{version=\"victoria-logs-20261113-102924-tags-v1.54.0-0-g6433dae586\", short_version=\"v1.54.0\"} 1\n"
	vlEnt := `vm_app_version{short_version="v1.52.2-enterprise",version="victoria-logs-20261101-000000-tags-v1.52.2-enterprise-0-gabc"} 1` + "\n"
	for _, c := range []struct {
		name, text, want string
	}{
		{"victorialogs", vl, "v1.54.0"},
		{"enterprise", vlEnt, "v1.52.2-enterprise"},
		{"build info series", `victorialogs_build_info{version="v1.53.0"} 1` + "\n", "v1.53.0"},
		{"build info short", `victorialogs_build_info{short_version="v1.51.1"} 1` + "\n", "v1.51.1"},
		// The stack's vmauth answers /metrics with its own version: VictoriaMetrics v1.155.0.
		{"vmauth", `vm_app_version{version="vmauth-20260929-120000-tags-v1.155.0-0-gabcdef0",short_version="v1.155.0"} 1` + "\n", ""},
		{"victoria-metrics", `vm_app_version{version="victoria-metrics-20260929-tags-v1.155.0-0-gabc",short_version="v1.155.0"} 1` + "\n", ""},
		{"vlagent", `vm_app_version{version="vlagent-20261113-102924-tags-v1.54.0-0-g6433dae586",short_version="v1.54.0"} 1` + "\n", ""},
		{"bare version on vm_app_version", `vm_app_version{short_version="v1.50.0",version="v1.50.0"} 1` + "\n", ""},
		{"other series", `go_info{version="victoria-logs v1.50.0"} 1` + "\n", ""},
		{"vmauth first then victorialogs", `vm_app_version{version="vmauth-1-tags-v1.155.0-0-g1"} 1` + "\n" + vl, "v1.54.0"},
		{"unterminated", `vm_app_version{version="victoria-logs-1-tags-v1.50.0`, ""},
		{"empty", "", ""},
	} {
		r, ok := ParseMetrics(c.text)
		if c.want == "" {
			if ok {
				t.Errorf("%s: accepted as %s", c.name, r)
			}
			continue
		}
		if !ok || r.String() != c.want || r.Source != SourceMetrics {
			t.Errorf("%s: %+v %v, want %s", c.name, r, ok, c.want)
		}
	}
}

func TestReleaseOrderAndAtLeast(t *testing.T) {
	order := []string{"v1.49.0", "v1.50.0", "v1.52.0", "v1.52.2", "v1.52.2-enterprise", "v1.54.0"}
	for i := 0; i < len(order); i++ {
		for j := 0; j < len(order); j++ {
			a, b := ParseRelease(order[i]), ParseRelease(order[j])
			if got := a.Compare(b); (got < 0) != (i < j) || (got == 0) != (i == j) {
				t.Errorf("%s vs %s: %d", order[i], order[j], got)
			}
		}
	}
	if (Release{}).Compare(ParseRelease("v1.49.0")) >= 0 || !ParseRelease("v1.52.2-enterprise").AtLeast(ParseRelease("v1.52.2")) || ParseRelease("v1.48.0").AtLeast(ParseRelease("v1.49.0")) || (Release{}).AtLeast(ParseRelease("v1.49.0")) || !(Release{}).AtLeast(Release{}) {
		t.Fatal("ordering")
	}
}

// ParseBuild and ParseMetrics read text a backend (or something in front of it) chose: they terminate fast and never
// panic on any input.
func FuzzParseBuild(f *testing.F) {
	for _, s := range []string{"victoria-logs-20261113-102924-tags-v1.54.0-0-g6433dae586", "VictoriaLogs/v1.50.0", "vmauth-1-tags-v1.155.0", "victoria-logs v", "victoria-logs-", "", "victoria-logs-tags-v1.2.3-enterprise-fips-fips-0-g"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if r, ok := ParseBuild(s, SourceHeader); ok && (!r.Known || r.Major > 9999 || len(r.Raw) > maxRaw) {
			t.Fatalf("%q -> %+v", s, r)
		}
		if r, ok := ParseMetrics("vm_app_version{version=\"" + s + "\"} 1\nvictorialogs_build_info{version=\"" + s + "\"} 1"); ok && !r.Known {
			t.Fatalf("%q -> %+v", s, r)
		}
		ParseRelease(s)
	})
}
