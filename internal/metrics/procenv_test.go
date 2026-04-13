package metrics

import (
	"sync"
	"testing"
)

var procEnvTestMu sync.Mutex

func lockProcEnvTest(t *testing.T) {
	t.Helper()
	procEnvTestMu.Lock()
	t.Cleanup(procEnvTestMu.Unlock)
}

