package proxy

import (
	"context"
	"os"
	"path/filepath"
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
	parent := filepath.Join(t.TempDir(), "occupied-parent")
	if err := os.WriteFile(parent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create occupied parent file: %v", err)
	}

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
	parent := filepath.Join(t.TempDir(), "occupied-parent")
	if err := os.WriteFile(parent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create occupied parent file: %v", err)
	}

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

// New creates the label-values index snapshot file empty when it checks that
// the path is writable, so a first start finds an empty file: that is no
// snapshot yet, like a missing file, not a corrupt one.
func TestLabelValuesIndexRestoreFromDisk_EmptyFileIsNoop(t *testing.T) {
	for name, content := range map[string]string{"empty": "", "whitespace": " \n\t\n"} {
		t.Run(name, func(t *testing.T) {
			snapshotPath := filepath.Join(t.TempDir(), "cache", "label-values-index.json")
			p, err := New(Config{
				BackendURL:                      "http://unused",
				Cache:                           cache.New(60*time.Second, 1000),
				LogLevel:                        "error",
				LabelValuesIndexedCache:         true,
				LabelValuesIndexPersistPath:     snapshotPath,
				LabelValuesIndexPersistInterval: time.Hour,
			})
			if err != nil {
				t.Fatalf("create proxy: %v", err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			if info, err := os.Stat(snapshotPath); err != nil || info.Size() != 0 {
				t.Fatalf("New should leave an empty snapshot file at the persist path: info=%v err=%v", info, err)
			}
			if err := os.WriteFile(snapshotPath, []byte(content), 0o644); err != nil {
				t.Fatalf("write snapshot: %v", err)
			}
			ok, savedAt, err := p.restoreLabelValuesIndexFromDisk()
			if err != nil {
				t.Fatalf("restore from a %s snapshot file returned an error: %v", name, err)
			}
			if ok || savedAt != 0 {
				t.Fatalf("restore from a %s snapshot file should restore nothing, got ok=%v savedAt=%d", name, ok, savedAt)
			}
		})
	}
}
