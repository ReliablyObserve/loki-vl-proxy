package main

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestHelmExtraArgsFlagParity ensures every flag referenced in the Helm chart's
// values.yaml (both active and commented extraArgs) corresponds to a flag
// registered by the binary's FlagSet. This prevents the class of bug where the
// chart ships a flag name that does not exist in the binary (issue #391).
func TestHelmExtraArgsFlagParity(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source file path")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

	goFlags := extractGoFlags(t, filepath.Join(repoRoot, "cmd", "proxy", "main.go"))
	if len(goFlags) == 0 {
		t.Fatal("extracted 0 Go flags from main.go — parser is broken")
	}

	valuesPath := filepath.Join(repoRoot, "charts", "loki-vl-proxy", "values.yaml")
	helmActive, helmCommented := extractHelmExtraArgs(t, valuesPath)

	for flag := range helmActive {
		if !goFlags[flag] {
			t.Errorf("active extraArgs flag %q in values.yaml is not a valid binary flag", flag)
		}
	}

	for flag := range helmCommented {
		if !goFlags[flag] {
			t.Errorf("commented extraArgs flag %q in values.yaml is not a valid binary flag", flag)
		}
	}
}

// TestHelmExtraArgsCoverage ensures every Go flag is documented in values.yaml
// (either as an active key, a commented key, or handled by dedicated Helm values).
func TestHelmExtraArgsCoverage(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source file path")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

	goFlags := extractGoFlags(t, filepath.Join(repoRoot, "cmd", "proxy", "main.go"))

	valuesPath := filepath.Join(repoRoot, "charts", "loki-vl-proxy", "values.yaml")
	helmActive, helmCommented := extractHelmExtraArgs(t, valuesPath)

	deploymentPath := filepath.Join(repoRoot, "charts", "loki-vl-proxy", "templates", "deployment.yaml")
	templateFlags := extractTemplateFlags(t, deploymentPath)

	// Flags handled by dedicated Helm values (not extraArgs).
	// These are injected by deployment.yaml template logic or by dedicated
	// Helm values sections, so they don't need extraArgs entries.
	chartManaged := map[string]bool{
		"peer-self":       true, // injected by peerCache template logic
		"peer-discovery":  true, // injected by peerCache template logic
		"peer-dns":        true, // injected by peerCache template logic
		"peer-static":     true, // injected by peerCache template logic
		"peer-srv":        true, // passed via extraArgs for srv discovery mode
		"peer-http-url":   true, // passed via extraArgs for http discovery mode
		"disk-cache-path": true, // injected when persistence.enabled=true
	}

	for flag := range goFlags {
		if chartManaged[flag] {
			continue
		}
		if templateFlags[flag] {
			continue
		}
		if helmActive[flag] || helmCommented[flag] {
			continue
		}
		t.Errorf("Go flag %q has no entry in values.yaml (active, commented, or template-injected)", flag)
	}
}

var flagDefRe = regexp.MustCompile(`fs\.\w+\("([a-zA-Z][a-zA-Z0-9._-]*)"`)

func extractGoFlags(t *testing.T, mainGoPath string) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(mainGoPath)
	if err != nil {
		t.Fatalf("read %s: %v", mainGoPath, err)
	}
	flags := make(map[string]bool)
	for _, m := range flagDefRe.FindAllSubmatch(data, -1) {
		flags[string(m[1])] = true
	}
	return flags
}

func extractHelmExtraArgs(t *testing.T, valuesPath string) (active, commented map[string]bool) {
	t.Helper()
	f, err := os.Open(valuesPath)
	if err != nil {
		t.Fatalf("open %s: %v", valuesPath, err)
	}
	defer f.Close()

	active = make(map[string]bool)
	commented = make(map[string]bool)

	// Match lines like:
	//   flag-name: "value"         (active)
	//   # flag-name: "value"       (commented)
	activeRe := regexp.MustCompile(`^\s{2}([a-zA-Z][a-zA-Z0-9._-]+):\s`)
	commentRe := regexp.MustCompile(`^\s{2}#\s*([a-zA-Z][a-zA-Z0-9._-]+):\s`)

	inExtraArgs := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "extraArgs:") {
			inExtraArgs = true
			continue
		}
		if inExtraArgs && len(line) > 0 && line[0] != ' ' && line[0] != '#' {
			break
		}
		if !inExtraArgs {
			continue
		}

		if m := activeRe.FindStringSubmatch(line); m != nil && !strings.HasPrefix(strings.TrimSpace(line), "#") {
			active[m[1]] = true
		}
		if m := commentRe.FindStringSubmatch(line); m != nil {
			commented[m[1]] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", valuesPath, err)
	}
	return active, commented
}

var templateFlagRe = regexp.MustCompile(`-([a-zA-Z][a-zA-Z0-9._-]+)=`)

func extractTemplateFlags(t *testing.T, deploymentPath string) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(deploymentPath)
	if err != nil {
		t.Fatalf("read %s: %v", deploymentPath, err)
	}
	flags := make(map[string]bool)
	for _, m := range templateFlagRe.FindAllSubmatch(data, -1) {
		flags[string(m[1])] = true
	}
	return flags
}
