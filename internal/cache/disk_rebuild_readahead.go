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
// there. So the read-ahead first lets the walk run alone for
// rebuildReadaheadWalkProbe from its first record and takes its rate (stored
// bytes passed per second; measured alone, because the read-ahead takes the
// volume's throughput and would slow the walk it is compared with). Once it has then read a probe of
// the file over at least rebuildReadaheadProbeTime it compares its own rate
// with the walk's, once, and stops when the walk alone is as fast. A device
// that reads the whole file within that time is never stopped (the first
// reads are slower than the steady rate, so an earlier comparison would stop
// it on fast disks where it pays most).

// rebuildReadaheadProbeMin is the least the read-ahead reads before it decides
// whether to go on.
const rebuildReadaheadProbeMin = 16 << 20

// rebuildReadaheadWalkProbe and rebuildReadaheadProbeTime are variables for
// the tests only.
var (
	rebuildReadaheadWalkProbe = 50 * time.Millisecond
	rebuildReadaheadProbeTime = 100 * time.Millisecond
)

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
	walkProbe, probeTime := rebuildReadaheadWalkProbe, rebuildReadaheadProbeTime
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
	work := make(chan int64, restoreReadaheadReaders)
	var readers sync.WaitGroup
	for range restoreReadaheadReaders {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var buffer []byte // allocated on the first read: a rebuild done within the walk probe holds none
			for at := range work {
				if ctx.Err() != nil {
					return // stopped: the queued ranges are not read
				}
				if buffer == nil {
					buffer = make([]byte, restoreReadaheadBuffer)
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
		// The walk's rate from its first record (the rebuild reads the
		// hand-over's ranks before it walks), over walkProbe.
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		var walkFrom time.Time
		var walkedFrom int64
		for walkFrom.IsZero() || time.Since(walkFrom) < walkProbe {
			select {
			case <-tick.C:
			case <-ctx.Done():
				return
			}
			if n := walked.Load(); walkFrom.IsZero() && n > 0 {
				walkFrom, walkedFrom = time.Now(), n
			}
		}
		walkRate := float64(walked.Load()-walkedFrom) / float64(max(time.Since(walkFrom), time.Nanosecond))
		readStart := time.Now()
		decided := false
		for at := int64(0); at < size && ctx.Err() == nil; at += restoreReadaheadBuffer {
			if read, elapsed := ra.read.Load(), time.Since(readStart); !decided && read >= rebuildReadaheadProbeMin && elapsed >= probeTime {
				decided = true
				if walkRate >= float64(read)/float64(elapsed) {
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
