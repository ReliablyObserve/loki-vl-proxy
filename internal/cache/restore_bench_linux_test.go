//go:build amd64 || arm64

package cache

import (
	"os"
	"syscall"
)

func writeUncached(*os.File) error { return nil }

// dropFromPageCache writes the file back and asks the kernel to drop its pages
// (POSIX_FADV_DONTNEED), so a following read comes from the disk.
func dropFromPageCache(f *os.File) error {
	if err := f.Sync(); err != nil {
		return err
	}
	const dontNeed = 4
	if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, dontNeed, 0, 0); errno != 0 {
		return errno
	}
	return nil
}
