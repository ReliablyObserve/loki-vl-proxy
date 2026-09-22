package config

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

func flags(t *testing.T) []Flag {
	t.Helper()
	parsed, err := ParseFlags(filepath.Join(repoRoot(t), MainGoPath))
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if len(parsed) < 100 {
		t.Fatalf("parsed only %d flags; the parser is broken", len(parsed))
	}
	return parsed
}

// The generated reference documents must match the flags and the registry, so
// documentation cannot drift from the binary.
// conformance: operator-configurable-limits, limits/every-cap-is-a-flag
func TestGeneratedDocsAreUpToDate(t *testing.T) {
	root := repoRoot(t)
	documents, err := Generate(root)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for path, want := range documents {
		got, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatalf("%s: %v (run `go run ./cmd/configdoc`)", path, err)
		}
		if string(got) != want {
			t.Errorf("%s is out of date; run `go run ./cmd/configdoc`", path)
		}
	}
}

// Every flag belongs to a category, so no flag disappears from the reference.
// conformance: operator-configurable-limits, limits/every-cap-is-a-flag
func TestEveryFlagHasACategory(t *testing.T) {
	for _, flag := range flags(t) {
		if CategoryOf(flag.Name) == "other" {
			t.Errorf("flag -%s has no category; add its prefix to categories in registry.go", flag.Name)
		}
		if strings.TrimSpace(flag.Usage) == "" {
			t.Errorf("flag -%s has no description", flag.Name)
		}
	}
}

// Every limit names a flag the binary declares, with a resolved default, a
// Helm value that the chart documents, and an error or an explicit "none".
// conformance: operator-configurable-limits, limits/every-cap-is-a-flag
func TestLimitsRegistryMatchesFlagsAndChart(t *testing.T) {
	root := repoRoot(t)
	byName := map[string]Flag{}
	for _, flag := range flags(t) {
		byName[flag.Name] = flag
	}
	helm := helmExtraArgs(t, filepath.Join(root, "charts", "loki-vl-proxy", "values.yaml"))
	for _, limit := range Limits {
		flag, ok := byName[limit.Flag]
		if !ok {
			t.Errorf("limit %q has no flag in %s", limit.Flag, MainGoPath)
			continue
		}
		if strings.HasPrefix(flag.Default, "proxy.") {
			t.Errorf("limit %q: default %q was not resolved to a value", limit.Flag, flag.Default)
		}
		if !helm[limit.Flag] {
			t.Errorf("limit %q has no extraArgs entry in the chart values", limit.Flag)
		}
		for name, value := range map[string]string{"unit": limit.Unit, "bounds": limit.Bounds, "error": limit.Error, "metric": limit.Metric, "alert": limit.Alert, "sizing": limit.Sizing} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("limit %q has an empty %s", limit.Flag, name)
			}
		}
	}
}

// Alerts named by a limit must exist in the shipped alerting rules.
// conformance: operator-configurable-limits, limits/every-cap-is-a-flag
func TestLimitAlertsExist(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "alerting", "loki-vl-proxy-alerting-rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	rules := string(data)
	for _, limit := range Limits {
		alert := limit.Alert
		if alert == "none" {
			continue
		}
		if name, _, found := strings.Cut(alert, " "); found {
			alert = name
		}
		if !strings.Contains(rules, "alert: "+alert) {
			t.Errorf("limit %q names alert %q, which is not in alerting/loki-vl-proxy-alerting-rules.yaml", limit.Flag, alert)
		}
	}
}

var helmArgRe = regexp.MustCompile(`^\s{2}#?\s*([a-zA-Z][a-zA-Z0-9._-]+):\s`)

func helmExtraArgs(t *testing.T, valuesPath string) map[string]bool {
	t.Helper()
	file, err := os.Open(valuesPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	out := map[string]bool{}
	inExtraArgs := false
	scanner := bufio.NewScanner(file)
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
		if m := helmArgRe.FindStringSubmatch(line); m != nil {
			out[m[1]] = true
		}
	}
	return out
}
