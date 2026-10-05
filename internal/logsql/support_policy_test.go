package logsql_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The VictoriaLogs support policy (owner decision 2026-10-05): the latest minor
// line and the previous one are fully supported (today v1.5x and v1.4x), and the
// tested matrix holds at most the latest 3 releases of each line. When a new line
// appears the window shifts one line; this test fails until the matrix, the
// -backend-min-version default and logsql.MinSupportedMinor move together.

var lineRE = regexp.MustCompile(`^v1\.(\d)x(?:\.x)?$`)
var versionRE = regexp.MustCompile(`^v1\.(\d+)\.(\d+)$`)

func TestVictoriaLogsSupportPolicy(t *testing.T) {
	raw, err := os.ReadFile("../../test/e2e-compat/compatibility-matrix.json")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Stack struct {
			VL struct {
				PinnedVersion string `json:"pinned_version"`
				SupportWindow struct {
					CurrentFamily      string   `json:"current_family"`
					PreviousFamily     string   `json:"previous_family"`
					AdditionalFamilies []string `json:"additional_families"`
					MaxPerFamily       int      `json:"max_versions_per_family"`
					MinimumSupported   string   `json:"minimum_supported_version"`
				} `json:"support_window"`
				MatrixVersions []string `json:"matrix_versions"`
				Profiles       []struct {
					Profile      string `json:"profile"`
					VersionRange string `json:"version_range"`
				} `json:"capability_profiles"`
			} `json:"victorialogs"`
		} `json:"stack"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	w := m.Stack.VL.SupportWindow

	cur, prev := lineRE.FindStringSubmatch(w.CurrentFamily), lineRE.FindStringSubmatch(w.PreviousFamily)
	if cur == nil || prev == nil {
		t.Fatalf("current_family %q and previous_family %q must look like v1.5x.x", w.CurrentFamily, w.PreviousFamily)
	}
	curLine, _ := strconv.Atoi(cur[1])
	prevLine, _ := strconv.Atoi(prev[1])
	if prevLine != curLine-1 {
		t.Fatalf("the two supported lines must be adjacent, got %s and %s", w.CurrentFamily, w.PreviousFamily)
	}
	if len(w.AdditionalFamilies) != 0 {
		t.Fatalf("only the latest two lines are supported; additional_families = %v", w.AdditionalFamilies)
	}
	if w.MaxPerFamily != 3 {
		t.Fatalf("max_versions_per_family must be 3, got %d", w.MaxPerFamily)
	}

	minMinor := prevLine * 10
	if want := "v1." + strconv.Itoa(minMinor) + ".0"; w.MinimumSupported != want {
		t.Fatalf("minimum_supported_version = %q, want %q (first minor of the oldest supported line)", w.MinimumSupported, want)
	}
	if logsql.MinSupportedMajor != 1 || logsql.MinSupportedMinor != minMinor {
		t.Fatalf("logsql.MinSupported = %d.%d, want 1.%d", logsql.MinSupportedMajor, logsql.MinSupportedMinor, minMinor)
	}

	main, err := os.ReadFile("../../cmd/proxy/main.go")
	if err != nil {
		t.Fatal(err)
	}
	flagDefault := regexp.MustCompile(`"backend-min-version", "([^"]+)"`).FindStringSubmatch(string(main))
	if flagDefault == nil || flagDefault[1] != w.MinimumSupported {
		t.Fatalf("-backend-min-version default = %v, want %q", flagDefault, w.MinimumSupported)
	}

	// The floor must be the same everywhere an operator can see it: the Go
	// fallback used when the flag is empty and the Helm default.
	proxyGo, err := os.ReadFile("../proxy/proxy.go")
	if err != nil {
		t.Fatal(err)
	}
	if fb := regexp.MustCompile(`backendMinVersion == ""\s*\{\s*backendMinVersion = "([^"]+)"`).FindStringSubmatch(string(proxyGo)); fb == nil || fb[1] != w.MinimumSupported {
		t.Fatalf("internal/proxy/proxy.go backendMinVersion fallback = %v, want %q", fb, w.MinimumSupported)
	}
	values, err := os.ReadFile("../../charts/loki-vl-proxy/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if hv := regexp.MustCompile(`(?m)^\s+backend-min-version: "([^"]+)"`).FindStringSubmatch(string(values)); hv == nil || hv[1] != w.MinimumSupported {
		t.Fatalf("charts values.yaml backend-min-version = %v, want %q", hv, w.MinimumSupported)
	}

	// Capability profiles in the manifest must be the ones deriveBackendCapabilities
	// returns, with the same lower bound: the names describe where a capability
	// starts, so they do not move with the support floor.
	backendGo, err := os.ReadFile("../proxy/backend.go")
	if err != nil {
		t.Fatal(err)
	}
	derived := map[string]string{} // profile -> lower bound "1.49" ("" for the default case)
	for _, c := range regexp.MustCompile(`case semverAtLeast\(semver, 1, (\d+), 0\):\s*return backendCapabilities\{"([^"]+)"`).FindAllStringSubmatch(string(backendGo), -1) {
		derived[c[2]] = "v1." + c[1] + ".0"
	}
	if def := regexp.MustCompile(`default:\s*return backendCapabilities\{"([^"]+)"`).FindStringSubmatch(string(backendGo)); def != nil {
		derived[def[1]] = ""
	}
	if len(derived) != len(m.Stack.VL.Profiles) {
		t.Fatalf("manifest capability_profiles %v and deriveBackendCapabilities %v differ in count", m.Stack.VL.Profiles, derived)
	}
	for _, pr := range m.Stack.VL.Profiles {
		lower, ok := derived[pr.Profile]
		if !ok {
			t.Fatalf("manifest profile %q is not derived by deriveBackendCapabilities (%v)", pr.Profile, derived)
		}
		if lower != "" && !strings.HasPrefix(pr.VersionRange, ">= "+lower) {
			t.Fatalf("manifest profile %q range %q must start at %s as in deriveBackendCapabilities", pr.Profile, pr.VersionRange, lower)
		}
		if lower == "" && !strings.HasPrefix(pr.VersionRange, "< ") {
			t.Fatalf("manifest fallback profile %q range %q must be an upper-bounded range", pr.Profile, pr.VersionRange)
		}
	}

	minors := map[int]string{}
	perLine := map[int]int{}
	for _, v := range m.Stack.VL.MatrixVersions {
		sm := versionRE.FindStringSubmatch(v)
		if sm == nil {
			t.Fatalf("matrix version %q is not vMAJOR.MINOR.PATCH", v)
		}
		minor, _ := strconv.Atoi(sm[1])
		if other, dup := minors[minor]; dup {
			t.Fatalf("matrix lists minor v1.%d twice (%s, %s); keep the latest patch only", minor, other, v)
		}
		minors[minor] = v
		line := minor / 10
		if line != curLine && line != prevLine {
			t.Fatalf("matrix version %s is outside the supported lines %s and %s", v, w.CurrentFamily, w.PreviousFamily)
		}
		perLine[line]++
	}
	for _, line := range []int{curLine, prevLine} {
		if perLine[line] == 0 || perLine[line] > w.MaxPerFamily {
			t.Fatalf("line v1.%dx has %d matrix versions, want 1..%d", line, perLine[line], w.MaxPerFamily)
		}
	}
	if _, tested := minors[pinMinorOf(t, m.Stack.VL.PinnedVersion)]; !tested || minors[pinMinorOf(t, m.Stack.VL.PinnedVersion)] != m.Stack.VL.PinnedVersion {
		t.Fatalf("pinned_version %q must be one of the tested matrix_versions %v", m.Stack.VL.PinnedVersion, m.Stack.VL.MatrixVersions)
	}
	pin := versionRE.FindStringSubmatch(m.Stack.VL.PinnedVersion)
	if pin == nil {
		t.Fatalf("pinned_version %q is not vMAJOR.MINOR.PATCH", m.Stack.VL.PinnedVersion)
	}
	if pinMinor, _ := strconv.Atoi(pin[1]); pinMinor/10 != curLine {
		t.Fatalf("pinned_version %q must stay in the current line %s", m.Stack.VL.PinnedVersion, w.CurrentFamily)
	}
}

func pinMinorOf(t *testing.T, v string) int {
	t.Helper()
	sm := versionRE.FindStringSubmatch(v)
	if sm == nil {
		t.Fatalf("%q is not vMAJOR.MINOR.PATCH", v)
	}
	minor, _ := strconv.Atoi(sm[1])
	return minor
}
