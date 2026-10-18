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

// The VictoriaLogs support policy: the latest v1.4x release and every v1.5x
// release are supported, and the tested matrix holds each supported minor at its
// latest patch with a published image. The floor is the one v1.4x entry; this
// test fails until the manifest, the -backend-min-version default (flag, Go
// fallback, Helm), logsql.MinSupportedMinor and the README table agree.

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

	floor := versionRE.FindStringSubmatch(w.MinimumSupported)
	if floor == nil {
		t.Fatalf("minimum_supported_version %q is not vMAJOR.MINOR.PATCH", w.MinimumSupported)
	}
	floorMinor, _ := strconv.Atoi(floor[1])
	if floorMinor/10 != prevLine {
		t.Fatalf("minimum_supported_version = %q must be a release of the previous line %s", w.MinimumSupported, w.PreviousFamily)
	}
	if logsql.MinSupportedMajor != 1 || logsql.MinSupportedMinor != floorMinor {
		t.Fatalf("logsql.MinSupported = %d.%d, want 1.%d", logsql.MinSupportedMajor, logsql.MinSupportedMinor, floorMinor)
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

	// The tested matrix: exactly one entry of the previous line (the floor) and
	// every minor of the current line from its first one to the newest, each once.
	minors := map[int]string{}
	lowest, highest := 1<<30, 0
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
		switch minor / 10 {
		case prevLine:
			if v != w.MinimumSupported {
				t.Fatalf("the only %s entry in matrix_versions must be the floor %s, found %s", w.PreviousFamily, w.MinimumSupported, v)
			}
		case curLine:
			lowest, highest = min(lowest, minor), max(highest, minor)
		default:
			t.Fatalf("matrix version %s is outside the supported lines %s and %s", v, w.CurrentFamily, w.PreviousFamily)
		}
	}
	if minors[floorMinor] != w.MinimumSupported {
		t.Fatalf("matrix_versions %v must contain the floor %s", m.Stack.VL.MatrixVersions, w.MinimumSupported)
	}
	if lowest != curLine*10 {
		t.Fatalf("matrix_versions %v must start the %s line at v1.%d", m.Stack.VL.MatrixVersions, w.CurrentFamily, curLine*10)
	}
	for minor := lowest; minor <= highest; minor++ {
		if _, ok := minors[minor]; !ok {
			t.Fatalf("matrix_versions %v skips supported minor v1.%d", m.Stack.VL.MatrixVersions, minor)
		}
	}
	pin := versionRE.FindStringSubmatch(m.Stack.VL.PinnedVersion)
	if pin == nil {
		t.Fatalf("pinned_version %q is not vMAJOR.MINOR.PATCH", m.Stack.VL.PinnedVersion)
	}
	pinMinor, _ := strconv.Atoi(pin[1])
	if minors[pinMinor] != m.Stack.VL.PinnedVersion {
		t.Fatalf("pinned_version %q must be one of the tested matrix_versions %v", m.Stack.VL.PinnedVersion, m.Stack.VL.MatrixVersions)
	}
	if pinMinor/10 != curLine {
		t.Fatalf("pinned_version %q must stay in the current line %s", m.Stack.VL.PinnedVersion, w.CurrentFamily)
	}

	// The README names the same matrix, pin and floor between its markers.
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	block := regexp.MustCompile(`(?s)<!-- vl-support:start -->(.*?)<!-- vl-support:end -->`).FindStringSubmatch(string(readme))
	if block == nil {
		t.Fatal("README.md has no <!-- vl-support:start --> ... <!-- vl-support:end --> table")
	}
	rows := map[string][]string{} // first cell -> versions named in the second cell
	for _, line := range strings.Split(block[1], "\n") {
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		if len(cells) == 2 {
			rows[strings.TrimSpace(cells[0])] = regexp.MustCompile(`v\d+\.\d+\.\d+`).FindAllString(cells[1], -1)
		}
	}
	for row, want := range map[string][]string{
		"Tested in CI":                           m.Stack.VL.MatrixVersions,
		"Pull-request CI pin":                    {m.Stack.VL.PinnedVersion},
		"Startup floor (`-backend-min-version`)": {w.MinimumSupported},
	} {
		if got := rows[row]; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("README supported-versions row %q lists %v, want %v", row, got, want)
		}
	}
	if got := rows["Supported"]; len(got) == 0 || got[0] != w.MinimumSupported {
		t.Fatalf("README supported-versions row \"Supported\" lists %v, want it to start with the floor %s", got, w.MinimumSupported)
	}
}
