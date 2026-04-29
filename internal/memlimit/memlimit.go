// Package memlimit sets GOMEMLIMIT from container-awareness or explicit config.
//
// Priority order at startup:
//  1. GOMEMLIMIT env var (already set externally, e.g., by Helm) — no-op, Go
//     runtime already read it at program start.
//  2. Explicit -go-mem-limit flag (non-zero value in bytes).
//  3. -go-mem-limit-percent of the detected container memory limit (cgroups v2,
//     then cgroups v1 fallback).  When no cgroup limit is found, nothing is set
//     and the Go runtime uses its default GC target ratio.
package memlimit

import (
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	cgroupV2MemFile = "/sys/fs/cgroup/memory.max"
	cgroupV1MemFile = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	// The cgroup v1 "no limit" sentinel is a very large value close to int64 max.
	cgroupV1Unlimited = int64(math.MaxInt64 - (1 << 20))
)

// Apply sets GOMEMLIMIT according to the configured priority chain.
// It is safe to call multiple times (idempotent after the first effective set).
func Apply(explicit int64, percent int, logger *slog.Logger) {
	if explicit > 0 {
		debug.SetMemoryLimit(explicit)
		logger.Info("GOMEMLIMIT set from explicit flag", "bytes", explicit)
		return
	}

	// If GOMEMLIMIT was already set via environment variable, Go already picked
	// it up before main() ran.  Reflect the effective value in logs only.
	if envVal := os.Getenv("GOMEMLIMIT"); envVal != "" {
		current := debug.SetMemoryLimit(-1) // -1 = query only
		logger.Info("GOMEMLIMIT from environment variable", "bytes", current)
		return
	}

	if percent <= 0 || percent > 100 {
		return
	}

	cLimit, ok := containerMemoryLimit()
	if !ok || cLimit <= 0 {
		return
	}

	goLimit := cLimit * int64(percent) / 100
	if goLimit <= 0 {
		return
	}
	debug.SetMemoryLimit(goLimit)
	logger.Info("GOMEMLIMIT set from container memory limit",
		"container_bytes", cLimit,
		"percent", percent,
		"go_limit_bytes", goLimit,
	)
}

// containerMemoryLimit reads the container (cgroup) memory hard limit.
// Returns (limit, true) or (0, false) when no valid limit is found.
func containerMemoryLimit() (int64, bool) {
	if v, ok := readCgroupV2(); ok {
		return v, true
	}
	if v, ok := readCgroupV1(); ok {
		return v, true
	}
	return 0, false
}

func readCgroupV2() (int64, bool) {
	data, err := os.ReadFile(cgroupV2MemFile)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, false // unlimited
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

func readCgroupV1() (int64, bool) {
	data, err := os.ReadFile(cgroupV1MemFile)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v >= cgroupV1Unlimited {
		return 0, false // parse error or unlimited sentinel
	}
	return v, true
}
