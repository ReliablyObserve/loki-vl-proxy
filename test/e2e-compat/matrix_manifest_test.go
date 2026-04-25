//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func familyPrefix(family string) string {
	family = strings.TrimSuffix(family, ".x")
	family = strings.TrimSuffix(family, "x")
	return family
}

func TestPinnedCompatibilityMatrixMatchesCompose(t *testing.T) {
	matrixBytes, err := os.ReadFile("compatibility-matrix.json")
	if err != nil {
		t.Fatalf("read compatibility matrix: %v", err)
	}
	var matrix struct {
		Stack struct {
			Loki struct {
				ImageRepo     string `json:"image_repo"`
				PinnedVersion string `json:"pinned_version"`
				SupportWindow struct {
					Policy             string   `json:"policy"`
					CurrentFamily      string   `json:"current_family"`
					PreviousFamily     string   `json:"previous_family"`
					AdditionalFamilies []string `json:"additional_families"`
				} `json:"support_window"`
				MatrixVersions []string `json:"matrix_versions"`
			} `json:"loki"`
			VictoriaLogs struct {
				ImageRepo     string `json:"image_repo"`
				PinnedVersion string `json:"pinned_version"`
				SupportWindow struct {
					Policy             string   `json:"policy"`
					CurrentFamily      string   `json:"current_family"`
					PreviousFamily     string   `json:"previous_family"`
					AdditionalFamilies []string `json:"additional_families"`
				} `json:"support_window"`
				MatrixVersions []string `json:"matrix_versions"`
			} `json:"victorialogs"`
			Grafana struct {
				ImageRepo     string `json:"image_repo"`
				PinnedVersion string `json:"pinned_version"`
				SupportWindow struct {
					Policy             string   `json:"policy"`
					CurrentFamily      string   `json:"current_family"`
					PreviousFamily     string   `json:"previous_family"`
					AdditionalFamilies []string `json:"additional_families"`
				} `json:"support_window"`
				RuntimeProfiles []struct {
					Version   string `json:"version"`
					Profile   string `json:"profile"`
					RunOnPR   bool   `json:"run_on_pr"`
					TestRegex string `json:"test_regex"`
				} `json:"runtime_profiles"`
			} `json:"grafana"`
			LogsDrilldownContract struct {
				PinnedVersion string `json:"pinned_version"`
				PinnedCommit  string `json:"pinned_commit"`
				SupportWindow struct {
					Policy             string   `json:"policy"`
					CurrentFamily      string   `json:"current_family"`
					PreviousFamily     string   `json:"previous_family"`
					AdditionalFamilies []string `json:"additional_families"`
				} `json:"support_window"`
				MatrixVersions []string `json:"matrix_versions"`
			} `json:"logs_drilldown_contract"`
		} `json:"stack"`
		Tracks map[string]struct {
			Workflow  string `json:"workflow"`
			ScoreTest string `json:"score_test"`
		} `json:"tracks"`
	}
	if err := json.Unmarshal(matrixBytes, &matrix); err != nil {
		t.Fatalf("decode compatibility matrix: %v", err)
	}

	composeBytes, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatalf("read docker-compose: %v", err)
	}
	compose := string(composeBytes)

	expectedComposeVars := []struct {
		repo    string
		version string
	}{
		{repo: matrix.Stack.Loki.ImageRepo, version: matrix.Stack.Loki.PinnedVersion},
		{repo: matrix.Stack.VictoriaLogs.ImageRepo, version: matrix.Stack.VictoriaLogs.PinnedVersion},
		{repo: matrix.Stack.Grafana.ImageRepo, version: matrix.Stack.Grafana.PinnedVersion},
	}
	for _, image := range expectedComposeVars {
		if image.repo == "" || image.version == "" {
			t.Fatalf("compatibility matrix contains empty image pin")
		}
		expected := image.repo + ":" + image.version
		if !strings.Contains(compose, expected) {
			t.Fatalf("docker-compose.yml missing pinned image %q", expected)
		}
	}

	if !strings.Contains(compose, "${LOKI_IMAGE:-"+matrix.Stack.Loki.ImageRepo+":"+matrix.Stack.Loki.PinnedVersion+"}") {
		t.Fatalf("docker-compose.yml must expose LOKI_IMAGE override")
	}
	if !strings.Contains(compose, "${VICTORIALOGS_IMAGE:-"+matrix.Stack.VictoriaLogs.ImageRepo+":"+matrix.Stack.VictoriaLogs.PinnedVersion+"}") {
		t.Fatalf("docker-compose.yml must expose VICTORIALOGS_IMAGE override")
	}
	if !strings.Contains(compose, "${GRAFANA_IMAGE:-"+matrix.Stack.Grafana.ImageRepo+":"+matrix.Stack.Grafana.PinnedVersion+"}") {
		t.Fatalf("docker-compose.yml must expose GRAFANA_IMAGE override")
	}

	// Keep translation profile matrix pinned in compose so CI always exercises:
	// - hybrid + underscores + emit=true
	// - translated + underscores + emit=true
	// - native + underscores + emit=true
	// - translated + underscores + emit=false
	profileExpectations := []string{
		"loki-vl-proxy:",
		`- "-label-values-indexed-cache=true"`,
		`- "-label-values-hot-limit=200"`,
		`- "-label-values-index-max-entries=50000"`,
		`- "-label-values-index-persist-path=/cache/label-values-index.json"`,
		`- "-label-values-index-persist-interval=5s"`,
		"loki-vl-proxy-underscore:",
		`- "-metadata-field-mode=hybrid"`,
		`- "-emit-structured-metadata=true"`,
		"loki-vl-proxy-translated-metadata:",
		`- "-metadata-field-mode=translated"`,
		"loki-vl-proxy-native-metadata:",
		`- "-metadata-field-mode=native"`,
		"loki-vl-proxy-no-metadata:",
		`- "-emit-structured-metadata=false"`,
	}
	for _, expected := range profileExpectations {
		if !strings.Contains(compose, expected) {
			t.Fatalf("docker-compose.yml missing translation profile expectation %q", expected)
		}
	}

	if matrix.Stack.LogsDrilldownContract.PinnedVersion == "" || matrix.Stack.LogsDrilldownContract.PinnedCommit == "" {
		t.Fatalf("logs drilldown contract pin must include version and commit: %+v", matrix.Stack.LogsDrilldownContract)
	}
	if len(matrix.Stack.Loki.MatrixVersions) == 0 || len(matrix.Stack.VictoriaLogs.MatrixVersions) == 0 {
		t.Fatalf("runtime matrices must not be empty")
	}
	assertSupportWindow := func(name, policyName, currentFamily, previousFamily string, additionalFamilies, versions []string) {
		t.Helper()
		if policyName == "" || currentFamily == "" || previousFamily == "" {
			t.Fatalf("%s support window must define policy, current family, and previous family", name)
		}
		if len(versions) == 0 {
			t.Fatalf("%s matrix versions must not be empty", name)
		}
		currentPrefix := familyPrefix(currentFamily)
		previousPrefix := familyPrefix(previousFamily)
		allowedPrefixes := []string{currentPrefix, previousPrefix}
		for _, family := range additionalFamilies {
			if family == "" {
				continue
			}
			allowedPrefixes = append(allowedPrefixes, familyPrefix(family))
		}
		foundCurrent := false
		foundPrevious := false
		for _, version := range versions {
			matched := false
			for _, prefix := range allowedPrefixes {
				if strings.HasPrefix(version, prefix) {
					matched = true
					break
				}
			}
			switch {
			case strings.HasPrefix(version, currentPrefix):
				foundCurrent = true
			case strings.HasPrefix(version, previousPrefix):
				foundPrevious = true
			case !matched:
				t.Fatalf("%s version %q falls outside support window %q / %q", name, version, currentFamily, previousFamily)
			}
		}
		if !foundCurrent {
			t.Fatalf("%s matrix missing current-family coverage for %q", name, currentFamily)
		}
		if !foundPrevious {
			t.Fatalf("%s matrix missing previous-family coverage for %q", name, previousFamily)
		}
	}
	assertSupportWindow(
		"loki",
		matrix.Stack.Loki.SupportWindow.Policy,
		matrix.Stack.Loki.SupportWindow.CurrentFamily,
		matrix.Stack.Loki.SupportWindow.PreviousFamily,
		matrix.Stack.Loki.SupportWindow.AdditionalFamilies,
		matrix.Stack.Loki.MatrixVersions,
	)
	assertSupportWindow(
		"victorialogs",
		matrix.Stack.VictoriaLogs.SupportWindow.Policy,
		matrix.Stack.VictoriaLogs.SupportWindow.CurrentFamily,
		matrix.Stack.VictoriaLogs.SupportWindow.PreviousFamily,
		matrix.Stack.VictoriaLogs.SupportWindow.AdditionalFamilies,
		matrix.Stack.VictoriaLogs.MatrixVersions,
	)
	assertSupportWindow(
		"logs-drilldown",
		matrix.Stack.LogsDrilldownContract.SupportWindow.Policy,
		matrix.Stack.LogsDrilldownContract.SupportWindow.CurrentFamily,
		matrix.Stack.LogsDrilldownContract.SupportWindow.PreviousFamily,
		matrix.Stack.LogsDrilldownContract.SupportWindow.AdditionalFamilies,
		matrix.Stack.LogsDrilldownContract.MatrixVersions,
	)
	if !strings.HasPrefix(matrix.Stack.Loki.PinnedVersion, familyPrefix(matrix.Stack.Loki.SupportWindow.CurrentFamily)) {
		t.Fatalf("loki pinned version %q must remain in current family %q", matrix.Stack.Loki.PinnedVersion, matrix.Stack.Loki.SupportWindow.CurrentFamily)
	}
	if !strings.HasPrefix(matrix.Stack.VictoriaLogs.PinnedVersion, familyPrefix(matrix.Stack.VictoriaLogs.SupportWindow.CurrentFamily)) {
		t.Fatalf("victorialogs pinned version %q must remain in current family %q", matrix.Stack.VictoriaLogs.PinnedVersion, matrix.Stack.VictoriaLogs.SupportWindow.CurrentFamily)
	}
	if !strings.HasPrefix(matrix.Stack.LogsDrilldownContract.PinnedVersion, familyPrefix(matrix.Stack.LogsDrilldownContract.SupportWindow.CurrentFamily)) {
		t.Fatalf("logs-drilldown pinned version %q must remain in current family %q", matrix.Stack.LogsDrilldownContract.PinnedVersion, matrix.Stack.LogsDrilldownContract.SupportWindow.CurrentFamily)
	}
	if matrix.Stack.Grafana.SupportWindow.Policy == "" || matrix.Stack.Grafana.SupportWindow.CurrentFamily == "" || matrix.Stack.Grafana.SupportWindow.PreviousFamily == "" {
		t.Fatalf("grafana support window must define policy, current family, and previous family")
	}
	if !strings.HasPrefix(matrix.Stack.Grafana.PinnedVersion, familyPrefix(matrix.Stack.Grafana.SupportWindow.CurrentFamily)) {
		t.Fatalf("grafana pinned version %q must remain in current family %q", matrix.Stack.Grafana.PinnedVersion, matrix.Stack.Grafana.SupportWindow.CurrentFamily)
	}
	if len(matrix.Stack.Grafana.RuntimeProfiles) == 0 {
		t.Fatalf("grafana runtime profiles must not be empty")
	}
	fullProfileSeen := false
	currentSmokeSeen := false
	previousSmokeSeen := false
	for _, profile := range matrix.Stack.Grafana.RuntimeProfiles {
		if profile.Version == "" || profile.Profile == "" || profile.TestRegex == "" {
			t.Fatalf("grafana runtime profile must define version, profile, and test_regex: %+v", profile)
		}
		switch profile.Profile {
		case "full":
			fullProfileSeen = true
			if profile.Version != matrix.Stack.Grafana.PinnedVersion {
				t.Fatalf("grafana full profile must run on pinned version %q, got %q", matrix.Stack.Grafana.PinnedVersion, profile.Version)
			}
			if profile.RunOnPR {
				t.Fatalf("grafana full profile should stay out of PR smoke selection: %+v", profile)
			}
		case "current_smoke":
			currentSmokeSeen = true
			if !profile.RunOnPR {
				t.Fatalf("grafana current-family smoke must run on PRs: %+v", profile)
			}
			if !strings.HasPrefix(profile.Version, familyPrefix(matrix.Stack.Grafana.SupportWindow.CurrentFamily)) {
				t.Fatalf("grafana current-family smoke %q must stay within current family %q", profile.Version, matrix.Stack.Grafana.SupportWindow.CurrentFamily)
			}
			// current_smoke may use the same version as full when only one release
			// exists in the current family (e.g., Grafana 13.0.1 is the only 13.x).
			if !strings.Contains(profile.TestRegex, "TestDrilldown_RuntimeFamilyContracts") {
				t.Fatalf("grafana current-family smoke must exercise runtime-family assertions: %+v", profile)
			}
		case "previous_smoke":
			previousSmokeSeen = true
			if !profile.RunOnPR {
				t.Fatalf("grafana previous-family smoke must run on PRs: %+v", profile)
			}
			if !strings.HasPrefix(profile.Version, familyPrefix(matrix.Stack.Grafana.SupportWindow.PreviousFamily)) {
				t.Fatalf("grafana previous-family smoke %q must stay within previous family %q", profile.Version, matrix.Stack.Grafana.SupportWindow.PreviousFamily)
			}
			if !strings.Contains(profile.TestRegex, "TestDrilldown_RuntimeFamilyContracts") {
				t.Fatalf("grafana previous-family smoke must exercise runtime-family assertions: %+v", profile)
			}
		default:
			t.Fatalf("unsupported grafana runtime profile %q", profile.Profile)
		}
	}
	if !fullProfileSeen || !currentSmokeSeen || !previousSmokeSeen {
		t.Fatalf("grafana runtime profiles must include full, current-family smoke, and previous-family smoke coverage: %+v", matrix.Stack.Grafana.RuntimeProfiles)
	}
	for name, track := range matrix.Tracks {
		if track.Workflow == "" || track.ScoreTest == "" {
			t.Fatalf("track %q must define workflow and score test", name)
		}
	}
}
