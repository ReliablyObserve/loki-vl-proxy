package cache

import (
	"context"
	"io"
	"os"
	"sync"
)

// Read-ahead of the start-up index rebuild. The rebuild walks every record in
// key order through the mapped file, so on a cold page cache it waits for one
// page fault at a time, scattered over the file: about one device read per
// record. This reads the file into the page cache in file order with a few
// positioned reads at once while the rebuild runs, so the walk finds most pages
// already read and the device serves large sequential reads instead. It only
// reads, never changes what the rebuild sees, ends when the rebuild ends (before
// the listener opens, so no query waits behind it) and holds
// restoreReadaheadReaders buffers of restoreReadaheadBuffer bytes.

// startRebuildReadahead starts reading the file at path ahead of the rebuild;
// stop ends the reads and joins every goroutine, and reports the bytes read:
// after it is called, each reader finishes at most the read it is in. A file
// that cannot be opened means no read-ahead.
func startRebuildReadahead(path string) (stop func() int64) {
	file, err := os.Open(path)
	if err != nil {
		return func() int64 { return 0 }
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return func() int64 { return 0 }
	}
	size := info.Size()
	ctx, cancel := context.WithCancel(context.Background())
	work := make(chan int64, restoreReadaheadReaders)
	var readers sync.WaitGroup
	var mu sync.Mutex
	var read int64
	for range restoreReadaheadReaders {
		readers.Add(1)
		go func() {
			defer readers.Done()
			buffer := make([]byte, restoreReadaheadBuffer)
			var n int64
			defer func() { mu.Lock(); read += n; mu.Unlock() }()
			for at := range work {
				if ctx.Err() != nil {
					return // stopped: the queued ranges are not read
				}
				got, err := file.ReadAt(buffer[:min(size-at, restoreReadaheadBuffer)], at)
				n += int64(got)
				if err != nil && err != io.EOF {
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = file.Close() }()
		defer readers.Wait()
		defer close(work)
		for at := int64(0); at < size && ctx.Err() == nil; at += restoreReadaheadBuffer {
			select {
			case work <- at:
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() int64 {
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		return read
	}
}
