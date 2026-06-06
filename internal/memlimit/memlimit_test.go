package memlimit

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"testing"
)

// newSilentLogger returns a logger that writes to a bytes.Buffer the test can
// inspect, plus the buffer itself.
func newSilentLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// restoreRuntime captures GOMEMLIMIT/GOGC env + runtime values so a test can
// restore them on exit without affecting sibling tests.
func restoreRuntime(t *testing.T) {
	t.Helper()
	origMemEnv, hadMemEnv := os.LookupEnv("GOMEMLIMIT")
	origGCEnv, hadGCEnv := os.LookupEnv("GOGC")
	origMem := debug.SetMemoryLimit(-1)
	origGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(origGC) // SetGCPercent(-1) actually disables GC; restore immediately
	t.Cleanup(func() {
		debug.SetMemoryLimit(origMem)
		debug.SetGCPercent(origGC)
		if hadMemEnv {
			os.Setenv("GOMEMLIMIT", origMemEnv)
		} else {
			os.Unsetenv("GOMEMLIMIT")
		}
		if hadGCEnv {
			os.Setenv("GOGC", origGCEnv)
		} else {
			os.Unsetenv("GOGC")
		}
	})
}

func TestApply_ExplicitLimitSetsRuntime(t *testing.T) {
	restoreRuntime(t)
	os.Unsetenv("GOMEMLIMIT")
	logger, buf := newSilentLogger()

	const want int64 = 256 << 20
	Apply(want, 0, 100, logger)

	if got := debug.SetMemoryLimit(-1); got != want {
		t.Fatalf("expected GOMEMLIMIT=%d, got %d", want, got)
	}
	if !strings.Contains(buf.String(), "GOMEMLIMIT set from explicit flag") {
		t.Fatalf("expected explicit-flag log line, got: %s", buf.String())
	}
}

func TestApply_EnvVarShortCircuits(t *testing.T) {
	restoreRuntime(t)
	os.Setenv("GOMEMLIMIT", "123456789")
	logger, buf := newSilentLogger()

	// explicit=0 + GOMEMLIMIT env set should follow the env-path branch.
	Apply(0, 50, 100, logger)

	if !strings.Contains(buf.String(), "GOMEMLIMIT from environment variable") {
		t.Fatalf("expected env-var log line, got: %s", buf.String())
	}
}

func TestApply_InvalidPercentIsNoop(t *testing.T) {
	restoreRuntime(t)
	os.Unsetenv("GOMEMLIMIT")
	before := debug.SetMemoryLimit(-1)
	logger, _ := newSilentLogger()

	Apply(0, 0, 100, logger)   // percent <= 0
	Apply(0, 150, 100, logger) // percent > 100

	if after := debug.SetMemoryLimit(-1); after != before {
		t.Fatalf("expected GOMEMLIMIT unchanged (=%d), got %d", before, after)
	}
}

func TestApply_PercentWithoutCgroupIsNoop(t *testing.T) {
	// On non-Linux (or Linux without cgroup files), containerMemoryLimit
	// returns (0, false) and Apply should leave GOMEMLIMIT alone.
	if _, err := os.Stat(cgroupV2MemFile); err == nil {
		t.Skip("cgroup v2 file present — cannot test no-cgroup path")
	}
	if _, err := os.Stat(cgroupV1MemFile); err == nil {
		t.Skip("cgroup v1 file present — cannot test no-cgroup path")
	}
	restoreRuntime(t)
	os.Unsetenv("GOMEMLIMIT")
	before := debug.SetMemoryLimit(-1)
	logger, _ := newSilentLogger()

	Apply(0, 50, 100, logger)

	if after := debug.SetMemoryLimit(-1); after != before {
		t.Fatalf("expected GOMEMLIMIT unchanged on no-cgroup host, got %d (was %d)", after, before)
	}
}

func TestReadCgroupV2_FileMissingReturnsFalse(t *testing.T) {
	if _, err := os.Stat(cgroupV2MemFile); err == nil {
		t.Skip("cgroup v2 file present on this host")
	}
	if v, ok := readCgroupV2(); ok || v != 0 {
		t.Fatalf("expected (0,false) for missing cgroup v2 file, got (%d,%v)", v, ok)
	}
}

func TestReadCgroupV1_FileMissingReturnsFalse(t *testing.T) {
	if _, err := os.Stat(cgroupV1MemFile); err == nil {
		t.Skip("cgroup v1 file present on this host")
	}
	if v, ok := readCgroupV1(); ok || v != 0 {
		t.Fatalf("expected (0,false) for missing cgroup v1 file, got (%d,%v)", v, ok)
	}
}

func TestContainerMemoryLimit_FalsePathOnNonContainer(t *testing.T) {
	if _, err := os.Stat(cgroupV2MemFile); err == nil {
		t.Skip("cgroup v2 file present on this host")
	}
	if _, err := os.Stat(cgroupV1MemFile); err == nil {
		t.Skip("cgroup v1 file present on this host")
	}
	if v, ok := containerMemoryLimit(); ok || v != 0 {
		t.Fatalf("expected (0,false) without any cgroup file, got (%d,%v)", v, ok)
	}
}

func TestApplyGCPercent_EnvVarShortCircuits(t *testing.T) {
	restoreRuntime(t)
	os.Setenv("GOGC", "200")
	logger, buf := newSilentLogger()

	applyGCPercent(50, logger) // env var present → should log "from environment variable"

	if !strings.Contains(buf.String(), "GOGC from environment variable") {
		t.Fatalf("expected env-var GOGC log, got: %s", buf.String())
	}
}

func TestApplyGCPercent_ExplicitValueIsApplied(t *testing.T) {
	restoreRuntime(t)
	os.Unsetenv("GOGC")
	logger, _ := newSilentLogger()

	// SetGCPercent returns the previous value; we read it back to assert
	// the new one took effect.
	applyGCPercent(123, logger)
	if got := debug.SetGCPercent(-1); got != 123 {
		// SetGCPercent(-1) returns prev value AND disables GC — but that's fine for asserting.
		// Restore happens in the cleanup.
		t.Fatalf("expected GOGC=123 after applyGCPercent, got %d", got)
	}
}

// silenceLogger is a no-op writer used when we don't care about logger output
// but still need to satisfy the slog API.
type silenceLogger struct{}

func (silenceLogger) Write(p []byte) (int, error) { return len(p), nil }

// Compile-time check that silenceLogger satisfies io.Writer (used as a sanity
// guard that the test file itself compiles cleanly).
var _ io.Writer = silenceLogger{}
