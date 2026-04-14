package proxy

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestEnsureWritableSnapshotPath_CreatesTargetFile(t *testing.T) {
	target := filepath.Join(t.TempDir(), "nested", "snapshot.json")
	if err := ensureWritableSnapshotPath(target); err != nil {
		t.Fatalf("expected writable snapshot path, got error: %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("expected snapshot file to exist, got error: %v", err)
	}
}

func TestNew_FailsFastOnUnwritablePatternsPersistPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission semantics differ on Windows")
	}

	parent := t.TempDir()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatalf("chmod parent dir: %v", err)
	}
	defer func() { _ = os.Chmod(parent, 0o700) }()

	_, err := New(Config{
		BackendURL:          "http://unused",
		Cache:               cache.New(60*time.Second, 1000),
		LogLevel:            "error",
		PatternsPersistPath: filepath.Join(parent, "patterns.json"),
	})
	if err == nil {
		t.Fatalf("expected New to fail for unwritable patterns persistence path")
	}
	if !strings.Contains(err.Error(), "patterns persistence path") {
		t.Fatalf("expected patterns persistence path error, got: %v", err)
	}
}

func TestNew_FailsFastOnUnwritableLabelValuesPersistPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission semantics differ on Windows")
	}

	parent := t.TempDir()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatalf("chmod parent dir: %v", err)
	}
	defer func() { _ = os.Chmod(parent, 0o700) }()

	_, err := New(Config{
		BackendURL:                  "http://unused",
		Cache:                       cache.New(60*time.Second, 1000),
		LogLevel:                    "error",
		LabelValuesIndexPersistPath: filepath.Join(parent, "label-values-index.json"),
	})
	if err == nil {
		t.Fatalf("expected New to fail for unwritable label-values persistence path")
	}
	if !strings.Contains(err.Error(), "label-values index persistence path") {
		t.Fatalf("expected label-values persistence path error, got: %v", err)
	}
}
