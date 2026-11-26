package cache

import (
	"os"
	"syscall"
)

// writeUncached turns off page caching for the file's writes (F_NOCACHE), so a
// following read comes from the disk.
func writeUncached(f *os.File) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_NOCACHE, 1); errno != 0 {
		return errno
	}
	return nil
}

func dropFromPageCache(f *os.File) error { return f.Sync() }
