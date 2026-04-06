package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadInputFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte("groups: []\n"), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	got, err := readInput(path)
	if err != nil {
		t.Fatalf("readInput failed: %v", err)
	}
	if string(got) != "groups: []\n" {
		t.Fatalf("unexpected input content: %q", string(got))
	}
}

func TestWriteOutputToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.yaml")
	if err := writeOutput(path, []byte("groups: []\n")); err != nil {
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
