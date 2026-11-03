package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The client profile labels the proxy logs (grafana.runtime.family, grafana.drilldown.profile and
// grafana.datasource.profile, from detectGrafanaClientProfile) must be the capability_profiles of the
// compatibility matrix, the way TestVictoriaLogsSupportPolicy holds the VictoriaLogs profiles to it. Today's
// drift is listed in conformance/registry/profile-known-drift.json; the list may only shrink.

const (
	matrixPath     = "../../test/e2e-compat/compatibility-matrix.json"
	knownDriftPath = "../../conformance/registry/profile-known-drift.json"
)

type matrixProfile struct {
	Profile      string `json:"profile"`
	VersionRange string `json:"version_range"`
	// WireProfile names the profile a client of this profile is labelled with when the request cannot tell them
	// apart (the Logs Drilldown app version is not on the wire). Empty: the profile is its own.
	WireProfile  string   `json:"wire_profile"`
	Capabilities []string `json:"capabilities"`
}

type clientProfileMatrix struct {
	Stack struct {
		Grafana struct {
			RuntimeProfiles []struct {
				Version string `json:"version"`
			} `json:"runtime_profiles"`
			CapabilityProfiles []matrixProfile `json:"capability_profiles"`
		} `json:"grafana"`
		Datasource struct {
			MatrixVersions     []string        `json:"matrix_versions"`
			CapabilityProfiles []matrixProfile `json:"capability_profiles"`
		} `json:"grafana_loki_datasource_contract"`
		Drilldown struct {
			MatrixVersions     []string        `json:"matrix_versions"`
			CapabilityProfiles []matrixProfile `json:"capability_profiles"`
		} `json:"logs_drilldown_contract"`
	} `json:"stack"`
}

type semver [3]int

func parseSemver(s string) (semver, bool) {
	var v semver
	parts := strings.Split(strings.TrimSpace(s), ".")
	if len(parts) != 3 {
		return v, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return v, false
		}
		v[i] = n
	}
	return v, true
}

func (a semver) less(b semver) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// profileBounds reads ">= 13.0.0" and ">= 12.0.0 < 13.0.0": the lower bound, and the exclusive upper bound if any.
func profileBounds(rng string) (lo semver, hi semver, hasHi bool, err error) {
	fields := strings.Fields(rng)
	if len(fields) != 2 && len(fields) != 4 || fields[0] != ">=" || (len(fields) == 4 && fields[2] != "<") {
		return lo, hi, false, fmt.Errorf("version_range %q is not \">= A\" or \">= A < B\"", rng)
	}
	var ok bool
	if lo, ok = parseSemver(fields[1]); !ok {
		return lo, hi, false, fmt.Errorf("version_range %q: %q is not MAJOR.MINOR.PATCH", rng, fields[1])
	}
	if len(fields) == 4 {
		if hi, ok = parseSemver(fields[3]); !ok {
			return lo, hi, false, fmt.Errorf("version_range %q: %q is not MAJOR.MINOR.PATCH", rng, fields[3])
		}
		hasHi = true
	}
	return lo, hi, hasHi, nil
}

func (m matrixProfile) contains(v semver) bool {
	lo, hi, hasHi, err := profileBounds(m.VersionRange)
	return err == nil && !v.less(lo) && (!hasHi || v.less(hi))
}

// emittedClientProfiles is what the proxy labels a Grafana runtime of the given version: the Logs Drilldown
// profile and the runtime family for Drilldown traffic, the datasource profile for datasource traffic.
func emittedClientProfiles(version string) (drilldown, datasource, family string) {
	dd := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume", nil)
	dd.Header.Set("User-Agent", "Grafana/"+version)
	dd.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	p := detectGrafanaClientProfile(dd, "volume", "/loki/api/v1/index/volume")
	drilldown, family = p.drilldownProfile, p.runtimeFamily
	ds := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
	ds.Header.Set("User-Agent", "Grafana/"+version)
	datasource = detectGrafanaClientProfile(ds, "query_range", "/loki/api/v1/query_range").datasourceProfile
	return drilldown, datasource, family
}

// clientProfileDrift lists every way the emitted labels differ from the matrix, as "<kind>:<profile>" ids with a
// detail; the ids are what the known-drift list holds.
func clientProfileDrift(m clientProfileMatrix) (map[string]string, error) {
	drift := map[string]string{}
	probes := []semver{}
	for major := 1; major <= 20; major++ {
		probes = append(probes, semver{major, 0, 0})
	}

	// Datasource profiles: ranges are Grafana versions, so every profile must be emitted for its lower bound, for
	// the last version below its upper bound and for each version the matrix names inside it.
	named := map[string]bool{}
	for _, v := range m.Stack.Datasource.MatrixVersions {
		if sv, ok := parseSemver(v); ok {
			probes = append(probes, sv)
		}
	}
	for _, rp := range m.Stack.Grafana.RuntimeProfiles {
		if sv, ok := parseSemver(rp.Version); ok {
			probes = append(probes, sv)
		}
	}
	for _, prof := range m.Stack.Datasource.CapabilityProfiles {
		named[prof.Profile] = true
		lo, hi, hasHi, err := profileBounds(prof.VersionRange)
		if err != nil {
			return nil, err
		}
		points := []semver{lo}
		if hasHi {
			points = append(points, semver{hi[0], hi[1], hi[2] - 1})
			if hi[2] == 0 {
				points[len(points)-1] = semver{hi[0] - 1, 99, 99}
			}
		}
		for _, v := range probes {
			if prof.contains(v) {
				points = append(points, v)
			}
		}
		for _, v := range points {
			if _, got, _ := emittedClientProfiles(fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])); got != prof.Profile {
				drift["datasource:"+prof.Profile] = fmt.Sprintf("Grafana %d.%d.%d is labelled %q, the matrix profile is %q (%s)",
					v[0], v[1], v[2], got, prof.Profile, prof.VersionRange)
				break
			}
		}
	}
	for _, v := range probes {
		if _, got, _ := emittedClientProfiles(fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])); got != "" && !named[got] {
			drift["datasource:emits-unknown:"+got] = "the proxy emits a datasource profile the matrix does not define"
		}
	}

	// Logs Drilldown profiles: ranges are Drilldown app versions, which are not on the wire, so names are compared,
	// not ranges. The proxy can only label what a request shows, so a matrix profile may name the profile it is
	// indistinguishable from (wire_profile). The profile pointing at it must extend it: keep every capability, and
	// may add some the request does not show. The profiles the proxy can tell apart are then the wire profiles: the
	// matrix defines none that is never emitted, and the proxy emits none the matrix lacks.
	emitted := map[string]bool{}
	for _, v := range probes {
		if got, _, _ := emittedClientProfiles(fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])); got != "" {
			emitted[got] = true
		}
	}
	byName := map[string]matrixProfile{}
	for _, prof := range m.Stack.Drilldown.CapabilityProfiles {
		byName[prof.Profile] = prof
	}
	defined := map[string]bool{}
	for _, prof := range m.Stack.Drilldown.CapabilityProfiles {
		wire := prof.Profile
		if prof.WireProfile != "" && prof.WireProfile != prof.Profile {
			wire = prof.WireProfile
			target, known := byName[wire]
			switch {
			case !known:
				drift["drilldown:wire-profile:"+prof.Profile] = "wire_profile " + wire + " is not a profile of the matrix"
				continue
			case target.WireProfile != "" && target.WireProfile != target.Profile:
				drift["drilldown:wire-profile:"+prof.Profile] = "wire_profile " + wire + " itself points at " + target.WireProfile
				continue
			}
			have := map[string]bool{}
			for _, c := range prof.Capabilities {
				have[c] = true
			}
			for _, c := range target.Capabilities {
				if !have[c] {
					drift["drilldown:wire-profile:"+prof.Profile] = fmt.Sprintf("%s lacks capability %s of its wire profile %s", prof.Profile, c, wire)
				}
			}
		}
		defined[wire] = true
		if !emitted[wire] {
			drift["drilldown:"+wire] = "the matrix defines this Logs Drilldown profile but the proxy never emits it"
		}
	}
	for name := range emitted {
		if !defined[name] {
			drift["drilldown:emits-unknown:"+name] = "the proxy emits a Logs Drilldown profile the matrix does not define"
		}
	}

	// Grafana runtime profiles: the proxy labels the runtime family, so two matrix profiles must not share a label.
	type bound struct {
		lo      semver
		profile string
	}
	var runtime []bound
	for _, prof := range m.Stack.Grafana.CapabilityProfiles {
		lo, _, _, err := profileBounds(prof.VersionRange)
		if err != nil {
			return nil, err
		}
		runtime = append(runtime, bound{lo, prof.Profile})
	}
	sort.Slice(runtime, func(i, j int) bool { return runtime[i].lo.less(runtime[j].lo) })
	seen := map[string]string{}
	for _, b := range runtime {
		_, _, family := emittedClientProfiles(fmt.Sprintf("%d.%d.%d", b.lo[0], b.lo[1], b.lo[2]))
		switch earlier, shared := seen[family]; {
		case family == "":
			drift["runtime:"+b.profile] = "the proxy emits no runtime family for " + b.profile
		case shared:
			drift["runtime:"+b.profile] = fmt.Sprintf("runtime family %q is shared with %s", family, earlier)
		default:
			seen[family] = b.profile
		}
	}
	return drift, nil
}

func loadClientProfileMatrix(t *testing.T) clientProfileMatrix {
	t.Helper()
	raw, err := os.ReadFile(matrixPath)
	if err != nil {
		t.Fatal(err)
	}
	var m clientProfileMatrix
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

type knownProfileDrift struct {
	Ceiling int `json:"ceiling"`
	Drift   map[string]struct {
		Reason string `json:"reason"`
	} `json:"drift"`
}

func TestClientProfilesMatchMatrix(t *testing.T) {
	m := loadClientProfileMatrix(t)
	for name, profiles := range map[string][]matrixProfile{
		"grafana": m.Stack.Grafana.CapabilityProfiles, "grafana_loki_datasource_contract": m.Stack.Datasource.CapabilityProfiles,
		"logs_drilldown_contract": m.Stack.Drilldown.CapabilityProfiles} {
		if len(profiles) == 0 {
			t.Fatalf("stack.%s has no capability_profiles to compare the proxy's labels with", name)
		}
	}
	got, err := clientProfileDrift(m)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(knownDriftPath)
	if err != nil {
		t.Fatal(err)
	}
	var known knownProfileDrift
	if err := json.Unmarshal(raw, &known); err != nil {
		t.Fatal(err)
	}
	if known.Ceiling != len(known.Drift) {
		t.Errorf("%s: %d listed drift(s) but ceiling %d; the list may not grow, and a fix lowers the ceiling with it", knownDriftPath, len(known.Drift), known.Ceiling)
	}
	for id, entry := range known.Drift {
		if strings.TrimSpace(entry.Reason) == "" {
			t.Errorf("%s: %s needs a reason", knownDriftPath, id)
		}
		if _, still := got[id]; !still {
			t.Errorf("%s lists %s but the labels now match the matrix: remove it and lower the ceiling", knownDriftPath, id)
		}
	}
	for id, detail := range got {
		if _, listed := known.Drift[id]; !listed {
			t.Errorf("new client profile drift %s: %s. Fix the labels in detectGrafanaClientProfile or the matrix; the known-drift list may not grow", id, detail)
		}
	}
}

// The guard itself: a matrix that agrees with the labels is clean, and each kind of drift is found.
func TestClientProfileDriftIsDetected(t *testing.T) {
	var clean clientProfileMatrix
	clean.Stack.Datasource.MatrixVersions = []string{"12.4.12", "11.6.6"}
	clean.Stack.Datasource.CapabilityProfiles = []matrixProfile{
		{Profile: "grafana-datasource-v12", VersionRange: ">= 12.0.0"}, {Profile: "grafana-datasource-v11", VersionRange: ">= 11.0.0 < 12.0.0"}}
	clean.Stack.Drilldown.CapabilityProfiles = []matrixProfile{
		{Profile: "drilldown-v2", VersionRange: ">= 2.0.0 < 3.0.0"}, {Profile: "drilldown-v1", VersionRange: ">= 1.0.34 < 2.0.0"}}
	clean.Stack.Grafana.CapabilityProfiles = []matrixProfile{
		{Profile: "grafana-runtime-v12-plus", VersionRange: ">= 12.0.0"}, {Profile: "grafana-runtime-v11", VersionRange: ">= 11.0.0 < 12.0.0"}}
	if drift, err := clientProfileDrift(clean); err != nil || len(drift) != 0 {
		t.Fatalf("a matrix that agrees with the labels reported drift %v (err %v)", drift, err)
	}

	// A profile the request cannot tell from another (Drilldown 3 from 2) is not drift when it extends that profile.
	wired := clean
	wired.Stack.Drilldown.CapabilityProfiles = []matrixProfile{
		{Profile: "drilldown-v3.1", VersionRange: ">= 3.1.0", WireProfile: "drilldown-v2", Capabilities: []string{"a", "b", "footer"}},
		{Profile: "drilldown-v3", VersionRange: ">= 3.0.0 < 3.1.0", WireProfile: "drilldown-v2", Capabilities: []string{"a", "b"}},
		{Profile: "drilldown-v2", VersionRange: ">= 2.0.0 < 3.0.0", Capabilities: []string{"a", "b"}},
		{Profile: "drilldown-v1", VersionRange: ">= 1.0.34 < 2.0.0", Capabilities: []string{"legacy"}}}
	if drift, err := clientProfileDrift(wired); err != nil || len(drift) != 0 {
		t.Fatalf("profiles that extend a wire profile reported drift %v (err %v)", drift, err)
	}

	for name, tc := range map[string]struct {
		edit func(*clientProfileMatrix)
		want string
	}{
		"Grafana 13 datasource profile": {func(m *clientProfileMatrix) {
			m.Stack.Datasource.CapabilityProfiles = []matrixProfile{{Profile: "grafana-datasource-v13", VersionRange: ">= 13.0.0"},
				{Profile: "grafana-datasource-v12", VersionRange: ">= 12.0.0 < 13.0.0"}, {Profile: "grafana-datasource-v11", VersionRange: ">= 11.0.0 < 12.0.0"}}
		}, "datasource:grafana-datasource-v13"},
		"Logs Drilldown 3 profile": {func(m *clientProfileMatrix) {
			m.Stack.Drilldown.CapabilityProfiles = append([]matrixProfile{{Profile: "drilldown-v3", VersionRange: ">= 3.0.0 < 4.0.0"}}, m.Stack.Drilldown.CapabilityProfiles...)
		}, "drilldown:drilldown-v3"},
		"Logs Drilldown 3.1 profile": {func(m *clientProfileMatrix) {
			m.Stack.Drilldown.CapabilityProfiles = append([]matrixProfile{{Profile: "drilldown-v3.1", VersionRange: ">= 3.1.0"}}, m.Stack.Drilldown.CapabilityProfiles...)
		}, "drilldown:drilldown-v3.1"},
		"Grafana 13 runtime profile": {func(m *clientProfileMatrix) {
			m.Stack.Grafana.CapabilityProfiles = []matrixProfile{{Profile: "grafana-runtime-v13-plus", VersionRange: ">= 13.0.0"},
				{Profile: "grafana-runtime-v12-plus", VersionRange: ">= 12.0.0 < 13.0.0"}, {Profile: "grafana-runtime-v11", VersionRange: ">= 11.0.0 < 12.0.0"}}
		}, "runtime:grafana-runtime-v13-plus"},
		"a profile the proxy emits is missing from the matrix": {func(m *clientProfileMatrix) {
			m.Stack.Drilldown.CapabilityProfiles = m.Stack.Drilldown.CapabilityProfiles[:1]
		}, "drilldown:emits-unknown:drilldown-v1"},
		"a wire profile that is not in the matrix": {func(m *clientProfileMatrix) {
			m.Stack.Drilldown.CapabilityProfiles = append([]matrixProfile{{Profile: "drilldown-v3", VersionRange: ">= 3.0.0", WireProfile: "drilldown-v9"}}, m.Stack.Drilldown.CapabilityProfiles...)
		}, "drilldown:wire-profile:drilldown-v3"},
		"a wire profile that drops a capability": {func(m *clientProfileMatrix) {
			m.Stack.Drilldown.CapabilityProfiles = []matrixProfile{
				{Profile: "drilldown-v3", VersionRange: ">= 3.0.0", WireProfile: "drilldown-v2", Capabilities: []string{"b"}},
				{Profile: "drilldown-v2", VersionRange: ">= 2.0.0 < 3.0.0", Capabilities: []string{"a", "b"}},
				{Profile: "drilldown-v1", VersionRange: ">= 1.0.34 < 2.0.0"}}
		}, "drilldown:wire-profile:drilldown-v3"},
		"a datasource profile that moved its lower bound": {func(m *clientProfileMatrix) {
			m.Stack.Datasource.CapabilityProfiles = []matrixProfile{{Profile: "grafana-datasource-v12", VersionRange: ">= 12.0.0"}, {Profile: "grafana-datasource-v11", VersionRange: ">= 10.0.0 < 12.0.0"}}
		}, "datasource:grafana-datasource-v11"},
	} {
		t.Run(name, func(t *testing.T) {
			m := clean
			m.Stack.Datasource.CapabilityProfiles = append([]matrixProfile(nil), clean.Stack.Datasource.CapabilityProfiles...)
			m.Stack.Drilldown.CapabilityProfiles = append([]matrixProfile(nil), clean.Stack.Drilldown.CapabilityProfiles...)
			m.Stack.Grafana.CapabilityProfiles = append([]matrixProfile(nil), clean.Stack.Grafana.CapabilityProfiles...)
			tc.edit(&m)
			drift, err := clientProfileDrift(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, found := drift[tc.want]; !found {
				t.Fatalf("drift %v lacks %s", drift, tc.want)
			}
		})
	}
	if _, err := clientProfileDrift(clientProfileMatrix{}); err != nil {
		t.Fatalf("an empty matrix is not an error: %v", err)
	}
	var bad clientProfileMatrix
	bad.Stack.Datasource.CapabilityProfiles = []matrixProfile{{Profile: "x", VersionRange: "13.0.0"}}
	if _, err := clientProfileDrift(bad); err == nil {
		t.Fatal("an unreadable version_range must be an error")
	}
}

// scripts/compat/check-drilldown-contracts.sh dispatches on the Logs Drilldown major family. The families it has an
// arm for must be the families the matrix runs: a matrix version of a family without an arm fails the weekly contract
// matrix ("unsupported Logs Drilldown family"), and an arm for a family no matrix version has is dead.
func scriptFamilyDrift(script string, versions []string, profiles []matrixProfile) []string {
	block := regexp.MustCompile(`(?s)case "\$\{major_family\}" in(.*?)\nesac`).FindStringSubmatch(script)
	arms := map[int]bool{}
	if block != nil {
		for _, m := range regexp.MustCompile(`(?m)^\s+(\d+)\)\s*$`).FindAllStringSubmatch(block[1], -1) {
			n, _ := strconv.Atoi(m[1])
			arms[n] = true
		}
	}
	var drift []string
	matrixMajors := map[int]bool{}
	for _, v := range versions {
		if sv, ok := parseSemver(v); ok {
			matrixMajors[sv[0]] = true
		}
	}
	for _, prof := range profiles {
		if lo, _, _, err := profileBounds(prof.VersionRange); err == nil && !matrixMajors[lo[0]] {
			drift = append(drift, fmt.Sprintf("profile %s starts at %d.x but no matrix version is of that family", prof.Profile, lo[0]))
		}
	}
	for major := range matrixMajors {
		if !arms[major] {
			drift = append(drift, fmt.Sprintf("the matrix runs Logs Drilldown %d.x but the script has no arm for it", major))
		}
	}
	for major := range arms {
		if !matrixMajors[major] {
			drift = append(drift, fmt.Sprintf("the script has an arm for Logs Drilldown %d.x but the matrix runs none", major))
		}
	}
	if block == nil {
		drift = append(drift, "the script has no case on major_family")
	}
	sort.Strings(drift)
	return drift
}

func TestDrilldownContractScriptFamiliesMatchMatrix(t *testing.T) {
	m := loadClientProfileMatrix(t)
	raw, err := os.ReadFile("../../scripts/compat/check-drilldown-contracts.sh")
	if err != nil {
		t.Fatal(err)
	}
	if drift := scriptFamilyDrift(string(raw), m.Stack.Drilldown.MatrixVersions, m.Stack.Drilldown.CapabilityProfiles); len(drift) != 0 {
		t.Fatalf("scripts/compat/check-drilldown-contracts.sh and the matrix disagree on the Logs Drilldown families: %s", strings.Join(drift, "; "))
	}
}

func TestScriptFamilyDriftIsDetected(t *testing.T) {
	script := "x\ncase \"${major_family}\" in\n  1)\n    a\n    ;;\n  2)\n    b\n    ;;\n  *)\n    exit 1\n    ;;\nesac\n"
	profiles := []matrixProfile{{Profile: "drilldown-v2", VersionRange: ">= 2.0.0 < 3.0.0"}, {Profile: "drilldown-v1", VersionRange: ">= 1.0.34 < 2.0.0"}}
	if drift := scriptFamilyDrift(script, []string{"2.6.0", "1.0.41"}, profiles); len(drift) != 0 {
		t.Fatalf("agreeing script and matrix reported %v", drift)
	}
	for name, tc := range map[string]struct {
		versions []string
		profiles []matrixProfile
		want     string
	}{
		"a family without an arm":     {[]string{"3.1.0", "2.6.0", "1.0.41"}, profiles, "matrix runs Logs Drilldown 3.x but the script has no arm"},
		"an arm for a dropped family": {[]string{"2.6.0"}, profiles[:1], "arm for Logs Drilldown 1.x but the matrix runs none"},
		"a profile without versions":  {[]string{"2.6.0", "1.0.41"}, append([]matrixProfile{{Profile: "drilldown-v3", VersionRange: ">= 3.0.0"}}, profiles...), "drilldown-v3 starts at 3.x"},
	} {
		t.Run(name, func(t *testing.T) {
			if drift := strings.Join(scriptFamilyDrift(script, tc.versions, tc.profiles), "; "); !strings.Contains(drift, tc.want) {
				t.Fatalf("drift %q lacks %q", drift, tc.want)
			}
		})
	}
	if drift := scriptFamilyDrift("no dispatch here", []string{"2.6.0"}, nil); len(drift) == 0 {
		t.Fatal("a script without the family dispatch must be reported")
	}
}
