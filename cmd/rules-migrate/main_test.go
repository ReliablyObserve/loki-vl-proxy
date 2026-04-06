package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadInputFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte("groups: []\n"), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	got, err := readInput(path, strings.NewReader("ignored"))
	if err != nil {
		t.Fatalf("readInput failed: %v", err)
	}
	if string(got) != "groups: []\n" {
		t.Fatalf("unexpected input content: %q", string(got))
	}
}

func TestReadInputFromStdin(t *testing.T) {
	got, err := readInput("", strings.NewReader("groups: []\n"))
	if err != nil {
		t.Fatalf("readInput failed: %v", err)
	}
	if string(got) != "groups: []\n" {
		t.Fatalf("unexpected stdin content: %q", string(got))
	}
}

func TestWriteOutputToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.yaml")
	if err := writeOutput(path, []byte("groups: []\n"), &strings.Builder{}); err != nil {
		t.Fatalf("writeOutput failed: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(got) != "groups: []\n" {
		t.Fatalf("unexpected output content: %q", string(got))
	}
}

func TestWriteOutputToStdout(t *testing.T) {
	var out strings.Builder
	if err := writeOutput("", []byte("groups: []\n"), &out); err != nil {
		t.Fatalf("writeOutput failed: %v", err)
	}
	if out.String() != "groups: []\n" {
		t.Fatalf("unexpected stdout content: %q", out.String())
	}
}

func TestWriteReportToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "migration-review.txt")
	if err := writeReport(path, []byte("warning\n")); err != nil {
		t.Fatalf("writeReport failed: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report file: %v", err)
	}
	if string(got) != "warning\n" {
		t.Fatalf("unexpected report content: %q", string(got))
	}
}

func TestWriteReportNoPathIsNoop(t *testing.T) {
	if err := writeReport("", []byte("warning\n")); err != nil {
		t.Fatalf("writeReport with empty path should be a no-op, got %v", err)
	}
}

func TestRunReadsFromStdinAndWritesToStdout(t *testing.T) {
	code, stdout, stderr := runWithInput(nil, "groups: []\n")
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "groups:") {
		t.Fatalf("expected converted rules on stdout, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}

func TestRunWritesReportAndFailsClosedOnRiskyRules(t *testing.T) {
	dir := t.TempDir()
	reportPath := filepath.Join(dir, "report.txt")
	risky := "groups:\n- name: risky\n  rules:\n  - record: foo\n    expr: sum without(instance) (rate({app=\"api\"}[5m]))\n"

	code, stdout, stderr := runWithInput([]string{"-report", reportPath}, risky)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "convert rules:") {
		t.Fatalf("expected convert failure on stderr, got %q", stderr)
	}
	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if !strings.Contains(string(reportBytes), "Rules needing manual review:") {
		t.Fatalf("expected warning report, got %q", string(reportBytes))
	}
}

func TestRunAllowsRiskyRulesWhenFlagSet(t *testing.T) {
	risky := "groups:\n- name: risky\n  rules:\n  - record: foo\n    expr: sum without(instance) (rate({app=\"api\"}[5m]))\n"

	code, stdout, stderr := runWithInput([]string{"-allow-risky"}, risky)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "type: vlogs") {
		t.Fatalf("expected converted vmalert output, got %q", stdout)
	}
	if !strings.Contains(stderr, "Rules needing manual review:") {
		t.Fatalf("expected warning on stderr, got %q", stderr)
	}
}

func TestRunReturnsUsageErrorForBadFlag(t *testing.T) {
	code, stdout, stderr := runWithInput([]string{"-does-not-exist"}, "")
	if code != 2 {
		t.Fatalf("expected exit code 2, got %d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "flag provided but not defined") {
		t.Fatalf("expected flag parse error, got %q", stderr)
	}
}

func TestRunReturnsReadErrorForMissingFile(t *testing.T) {
	code, stdout, stderr := runWithInput([]string{"-in", filepath.Join(t.TempDir(), "missing.yaml")}, "")
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "read input:") {
		t.Fatalf("expected read error, got %q", stderr)
	}
}
