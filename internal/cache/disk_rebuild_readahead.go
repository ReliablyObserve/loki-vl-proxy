package cache

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
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
//
// It pays only while it covers the file faster than the walk does on its own.
// The walk reads a few KiB of each record, so with large records it passes the
// file's bytes faster than a throughput-bound read of all of them, and under
// memory pressure the pages read ahead can be reclaimed before the walk gets
// there. Once a probe of the file has been read and rebuildReadaheadProbeTime
// has passed, the read-ahead compares, once, the stored bytes the walk has
// passed with the bytes it has read, and stops when the walk is ahead. A
// device that reads the whole file within that time never stops it (the
// read-ahead's first reads are slower than its steady rate, so an earlier
// comparison would stop it on fast disks where it pays most).

// rebuildReadaheadProbeMin is the least the read-ahead reads before it decides
// whether to go on; 1/rebuildReadaheadProbeShare of the file when that is more.
const (
	rebuildReadaheadProbeMin   = 16 << 20
	rebuildReadaheadProbeShare = 50 // 2%
)

// rebuildReadaheadProbeTime is a variable for the tests only.
var rebuildReadaheadProbeTime = 100 * time.Millisecond

// rebuildReadahead is one rebuild's read-ahead.
type rebuildReadahead struct {
	read    atomic.Int64 // bytes read so far
	stopped atomic.Bool  // ended early: the walk was ahead
	cancel  context.CancelFunc
	done    chan struct{}
}

// startRebuildReadahead starts reading the file at path ahead of the rebuild;
// walked is the stored bytes of the records the walk has passed. A file that
// cannot be opened means no read-ahead.
func startRebuildReadahead(path string, walked *atomic.Int64) *rebuildReadahead {
	started := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	ra := &rebuildReadahead{cancel: cancel, done: make(chan struct{})}
	file, err := os.Open(path)
	if err != nil {
		close(ra.done)
		return ra
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		close(ra.done)
		return ra
	}
	size := info.Size()
	probe := max(int64(rebuildReadaheadProbeMin), size/rebuildReadaheadProbeShare)
	work := make(chan int64, restoreReadaheadReaders)
	var readers sync.WaitGroup
	for range restoreReadaheadReaders {
		readers.Add(1)
		go func() {
			defer readers.Done()
			buffer := make([]byte, restoreReadaheadBuffer)
			for at := range work {
				if ctx.Err() != nil {
					return // stopped: the queued ranges are not read
				}
				got, err := file.ReadAt(buffer[:min(size-at, restoreReadaheadBuffer)], at)
				ra.read.Add(int64(got))
				if err != nil && err != io.EOF {
					return
				}
			}
		}()
	}
	go func() {
		defer close(ra.done)
		defer func() { _ = file.Close() }()
		defer readers.Wait()
		defer close(work)
		decided := false
		for at := int64(0); at < size && ctx.Err() == nil; at += restoreReadaheadBuffer {
			if read := ra.read.Load(); !decided && read >= probe && time.Since(started) >= rebuildReadaheadProbeTime {
				decided = true
				if walked.Load() >= read {
					ra.stopped.Store(true)
					return
				}
			}
			select {
			case work <- at:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ra
}

// stop ends the reads and joins every goroutine; after it is called each
// reader finishes at most the read it is in. It reports the bytes read and
// whether the read-ahead had ended early because the walk was ahead.
func (ra *rebuildReadahead) stop() (read int64, stoppedEarly bool) {
	ra.cancel()
	<-ra.done
	return ra.read.Load(), ra.stopped.Load()
}
