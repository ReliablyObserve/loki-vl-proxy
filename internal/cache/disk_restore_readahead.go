package cache

import (
	"context"
	"io"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// Read-ahead of the start-up warm-up. A warm-up that faults a mapped file in
// one value at a time reads a cold disk with little parallelism. This reads the
// records the next batches will decode into the page cache first, with plain
// positioned reads (which, unlike page faults, do not hold a scheduler
// processor), in file order, a bounded window ahead of the restore. It only
// reads: the batches still take every value through their own read transaction,
// so what it reads can never change what is restored.

const (
	// restoreReadaheadWindow is how many stored bytes the read-ahead stays
	// ahead of the restore at most.
	restoreReadaheadWindow = 64 << 20
	// restoreReadaheadReaders positioned reads run at once, each through its
	// own buffer of restoreReadaheadBuffer bytes: all the memory it holds.
	restoreReadaheadReaders = 8
	restoreReadaheadBuffer  = 512 << 10
	// Ranges closer than this are read as one.
	restoreReadaheadGap = 64 << 10
)

type fileRange struct{ from, to int64 }

// restoreRanges returns where in the file the records of items are, merged and
// in file order. A value of a read transaction points into the mapped file, so
// its offset is its address less the mapping's; the value is not read.
func (dc *DiskCache) restoreRanges(items []handoverItem) []fileRange {
	out := make([]fileRange, 0, len(items))
	_ = dc.db.View(func(tx *bolt.Tx) error {
		base, size := dc.db.Info().Data, tx.Size()
		data, legacy, handover := tx.Bucket(dataBucket), tx.Bucket(legacyDataBucket), tx.Bucket(handoverBucket)
		var key []byte // reused: Get keeps no reference to it
		for _, item := range items {
			var value []byte
			switch {
			case item.memory:
				if handover != nil {
					value = handover.Get(handoverBucketKey(handoverClassMemory, item.key))
				}
			default:
				key = append(key[:0], item.key...)
				if data != nil {
					value = data.Get(key)
				}
				if value == nil && legacy != nil {
					value = legacy.Get(key)
				}
			}
			if len(value) == 0 {
				continue
			}
			from := int64(reflect.ValueOf(value).Pointer() - base) // a file offset, checked against the size below
			if to := from + int64(len(value)); from >= 0 && to <= size {
				out = append(out, fileRange{from, to})
			}
		}
		return nil
	})
	return mergeFileRanges(out)
}

func mergeFileRanges(ranges []fileRange) []fileRange {
	slices.SortFunc(ranges, func(a, b fileRange) int {
		switch {
		case a.from < b.from:
			return -1
		case a.from > b.from:
			return 1
		}
		return 0
	})
	out := ranges[:0]
	for _, r := range ranges {
		if n := len(out); n > 0 && r.from <= out[n-1].to+restoreReadaheadGap {
			out[n-1].to = max(out[n-1].to, r.to)
			continue
		}
		out = append(out, r)
	}
	return out
}

// restoreReadahead is one warm-up's read-ahead.
type restoreReadahead struct {
	// cursor and bytes are the entries and stored bytes the restore is past.
	cursor, bytes atomic.Int64
	advanced      chan struct{}
	cancel        context.CancelFunc
	done          chan struct{}
}

// startRestoreReadahead reads ahead of a restore of items, whose stored sizes
// are sizes, up to limit stored bytes in all. Call stop before the restore
// returns: it ends the reads and joins every goroutine. A file that cannot be
// opened only means no read-ahead.
func (dc *DiskCache) startRestoreReadahead(ctx context.Context, items []handoverItem, sizes []int64, limit int64) *restoreReadahead {
	ctx, cancel := context.WithCancel(ctx)
	ra := &restoreReadahead{advanced: make(chan struct{}, 1), cancel: cancel, done: make(chan struct{})}
	file, err := os.Open(dc.db.Path())
	if err != nil {
		dc.log.Debug("cache restore read-ahead unavailable", "error", err)
		close(ra.done)
		return ra
	}
	work := make(chan fileRange, restoreReadaheadReaders)
	var readers sync.WaitGroup
	for range restoreReadaheadReaders {
		readers.Add(1)
		go func() {
			defer readers.Done()
			buffer := make([]byte, restoreReadaheadBuffer)
			for r := range work {
				for at := r.from; at < r.to && ctx.Err() == nil; at += restoreReadaheadBuffer {
					if _, err := file.ReadAt(buffer[:min(r.to-at, restoreReadaheadBuffer)], at); err != nil && err != io.EOF {
						break
					}
				}
			}
		}()
	}
	go func() {
		defer close(ra.done)
		defer func() { _ = file.Close() }()
		defer readers.Wait()
		defer close(work)
		var next int
		var issued int64
		for ctx.Err() == nil && next < len(items) && issued < limit {
			// Never read behind the restore.
			if passed := int(ra.cursor.Load()); passed > next {
				next, issued = passed, ra.bytes.Load()
				continue
			}
			if issued-ra.bytes.Load() >= restoreReadaheadWindow {
				select {
				case <-ra.advanced:
				case <-ctx.Done():
				}
				continue
			}
			end, chunk := next, int64(0)
			for end < len(items) && end-next < restoreBatchKeys && chunk < restoreReadaheadWindow/4 {
				chunk += sizes[end]
				end++
			}
			for _, r := range dc.restoreRanges(items[next:end]) {
				// Large merged ranges are shared among the readers.
				for at := r.from; at < r.to; at += 2 * restoreReadaheadBuffer {
					select {
					case work <- fileRange{at, min(at+2*restoreReadaheadBuffer, r.to)}:
					case <-ctx.Done():
						return
					}
				}
			}
			next, issued = end, issued+chunk
		}
	}()
	return ra
}

// advance tells the read-ahead how far the restore is.
func (ra *restoreReadahead) advance(cursor int, bytes int64) {
	ra.cursor.Store(int64(cursor))
	ra.bytes.Store(bytes)
	select {
	case ra.advanced <- struct{}{}:
	default:
	}
}

func (ra *restoreReadahead) stop() {
	ra.cancel()
	<-ra.done
}
