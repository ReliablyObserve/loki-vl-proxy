//go:build !unix

package proxy

import "time"

// processCPUTime is not measured on this platform.
func processCPUTime() time.Duration { return 0 }
