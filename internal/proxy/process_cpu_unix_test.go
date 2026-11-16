//go:build unix

package proxy

import (
	"syscall"
	"time"
)

// processCPUTime is the CPU time (user and system) the test process used so far.
func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
