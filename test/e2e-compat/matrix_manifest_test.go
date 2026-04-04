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
					Policy         string `json:"policy"`
					CurrentFamily  string `json:"current_family"`
					PreviousFamily string `json:"previous_family"`
				} `json:"support_window"`
				MatrixVersions []string `json:"matrix_versions"`
			} `json:"loki"`
			VictoriaLogs struct {
				ImageRepo     string `json:"image_repo"`
				PinnedVersion string `json:"pinned_version"`
				SupportWindow struct {
					Policy         string `json:"policy"`
					CurrentFamily  string `json:"current_family"`
					PreviousFamily string `json:"previous_family"`
				} `json:"support_window"`
				MatrixVersions []string `json:"matrix_versions"`
			} `json:"victorialogs"`
			Grafana struct {
				ImageRepo     string `json:"image_repo"`
				PinnedVersion string `json:"pinned_version"`
			} `json:"grafana"`
			LogsDrilldownContract struct {
				PinnedVersion string `json:"pinned_version"`
				PinnedCommit  string `json:"pinned_commit"`
				SupportWindow struct {
					Policy         string `json:"policy"`
					CurrentFamily  string `json:"current_family"`
					PreviousFamily string `json:"previous_family"`
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

	if matrix.Stack.LogsDrilldownContract.PinnedVersion == "" || matrix.Stack.LogsDrilldownContract.PinnedCommit == "" {
		t.Fatalf("logs drilldown contract pin must include version and commit: %+v", matrix.Stack.LogsDrilldownContract)
	}
	if len(matrix.Stack.Loki.MatrixVersions) == 0 || len(matrix.Stack.VictoriaLogs.MatrixVersions) == 0 {
		t.Fatalf("runtime matrices must not be empty")
	}
	assertSupportWindow := func(name, policyName, currentFamily, previousFamily string, versions []string) {
		t.Helper()
		if policyName == "" || currentFamily == "" || previousFamily == "" {
			t.Fatalf("%s support window must define policy, current family, and previous family", name)
		}
		if len(versions) == 0 {
			t.Fatalf("%s matrix versions must not be empty", name)
		}
		currentPrefix := familyPrefix(currentFamily)
		previousPrefix := familyPrefix(previousFamily)
		foundCurrent := false
		foundPrevious := false
		for _, version := range versions {
			switch {
			case strings.HasPrefix(version, currentPrefix):
				foundCurrent = true
			case strings.HasPrefix(version, previousPrefix):
				foundPrevious = true
			default:
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
		matrix.Stack.Loki.MatrixVersions,
	)
	assertSupportWindow(
		"victorialogs",
		matrix.Stack.VictoriaLogs.SupportWindow.Policy,
		matrix.Stack.VictoriaLogs.SupportWindow.CurrentFamily,
		matrix.Stack.VictoriaLogs.SupportWindow.PreviousFamily,
		matrix.Stack.VictoriaLogs.MatrixVersions,
	)
	assertSupportWindow(
		"logs-drilldown",
		matrix.Stack.LogsDrilldownContract.SupportWindow.Policy,
		matrix.Stack.LogsDrilldownContract.SupportWindow.CurrentFamily,
		matrix.Stack.LogsDrilldownContract.SupportWindow.PreviousFamily,
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
	for name, track := range matrix.Tracks {
		if track.Workflow == "" || track.ScoreTest == "" {
			t.Fatalf("track %q must define workflow and score test", name)
		}
	}
}
