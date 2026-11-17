package proxy

import (
	"hash/maphash"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

// patternMineLinesPerWorker is how many lines of a read keep one mining worker busy: a read of fewer than twice
// as many is mined on the request's own goroutine, a larger one on a worker per this many lines, up to
// patternMineCores workers. (A variable for the tests.)
var patternMineLinesPerWorker = 20_000

// patternMineCores is how many workers the process runs at most, over all requests: GOMAXPROCS. A worker is a
// pair of goroutines, one decoding the read's rows and one mining its streams, and each parallel read adds the
// goroutine that splits its lines. (A variable for the tests.)
var patternMineCores = func() int { return runtime.GOMAXPROCS(0) }

const (
	// patternMineBatchRows is how many lines the reader hands a worker at a time, and patternMineBatchBytes how
	// many bytes of lines at most (a batch is handed over at whichever comes first), so a read holds a bounded
	// number of bytes whatever its lines' sizes.
	patternMineBatchRows  = 1024
	patternMineBatchBytes = 256 << 10
	// patternMineQueueBatches is how many batches wait for a worker at most: the reader stops reading when its
	// workers fall behind, so a read holds at most (patternMineQueueBatches+2) batches per worker.
	patternMineQueueBatches = 2
)

// patternMineWorkersRunning counts the workers of every request: together they never exceed patternMineCores,
// so concurrent cold reads share the cores instead of multiplying goroutines; a read that finds none free is
// mined on its own goroutine.
var patternMineWorkersRunning atomic.Int64

// acquirePatternMineWorkers takes up to want workers from the process's patternMineCores and returns how many it took:
// 0 when fewer than two are free (one worker mines no faster than the reader alone).
func acquirePatternMineWorkers(want int) int {
	limit := int64(patternMineCores())
	for {
		running := patternMineWorkersRunning.Load()
		n := limit - running
		if w := int64(want); w < n {
			n = w
		}
		if n < 2 {
			return 0
		}
		if patternMineWorkersRunning.CompareAndSwap(running, running+n) {
			return int(n)
		}
	}
}

func releasePatternMineWorkers(n int) {
	if n > 0 {
		patternMineWorkersRunning.Add(-int64(n))
	}
}

// patternMineWorkersFor is how many workers a read of lines lines asks for: one per patternMineLinesPerWorker,
// up to patternMineCores; under 2 the read is mined on the request's goroutine.
func patternMineWorkersFor(lines int) int {
	if lines <= 0 {
		return 0
	}
	return min(patternMineCores(), lines/patternMineLinesPerWorker)
}

// patternMineBatches keeps the batches of finished reads for the next ones.
var patternMineBatches = sync.Pool{New: func() any {
	batch := make([]patternMineRow, 0, patternMineBatchRows)
	return &batch
}}

// patternMineRow is a line handed to a mining worker, with its place in the read.
type patternMineRow struct {
	stream, level, msg string
	bucket             int64
	count              int
	seq                int
}

// patternMineMade is a miner a worker made, with the place in the read of the line that made it.
type patternMineMade struct {
	seq int
	key patternMinerKey
	c   *patternClusterer
}

type patternMineWorker struct {
	miner    *patternMiner
	queue    chan []patternMineRow
	made     []patternMineMade
	panicked any
}

// patternParallelMiner mines the lines of one read on several workers, each the only one to mine the streams it
// is handed: Loki mines each stream at each level apart, so a stream's lines, mined in the order they were read
// by the worker that holds the stream, train the clusters they train when one goroutine mines them all. finish
// puts the workers' miners into the read's miner in the order serial mining makes them (that of the line that
// made each), so the miner, its answer and its snapshot are those of serial mining, whatever the routing.
type patternParallelMiner struct {
	into    *patternMiner
	workers []*patternMineWorker
	pending [][]patternMineRow
	// pendingBytes is the bytes of the lines of each pending batch.
	pendingBytes []int
	seed         maphash.Seed
	wg           sync.WaitGroup
	seq          int
	// acquired is how many of patternMineWorkersRunning the miner holds, released by finish.
	acquired int
}

// startPatternMining is the parallel miner for a read of about lines lines into into, or nil when the read is
// mined on the caller's goroutine: too few lines, too few free workers, a miner that already holds miners (a
// refresh trains clusters a worker's own miner does not have), or -patterns-scope=selector (one miner per level
// for every stream). The caller calls finish on a non-nil miner once the read ends, whether it failed or not.
func startPatternMining(into *patternMiner, lines int) *patternParallelMiner {
	want := patternMineWorkersFor(lines)
	if want < 2 || into.oneStream || len(into.miners) > 0 {
		return nil
	}
	n := acquirePatternMineWorkers(want)
	if n == 0 {
		return nil
	}
	pm := newPatternParallelMiner(into, n)
	pm.acquired = n
	patternMineParallelReads.Add(1)
	return pm
}

// patternMineParallelReads counts the reads mined on workers.
var patternMineParallelReads atomic.Int64

// newPatternParallelMiner starts workers workers mining into into, which holds no miner yet.
func newPatternParallelMiner(into *patternMiner, workers int) *patternParallelMiner {
	pm := &patternParallelMiner{
		into:         into,
		workers:      make([]*patternMineWorker, workers),
		pending:      make([][]patternMineRow, workers),
		pendingBytes: make([]int, workers),
		seed:         maphash.MakeSeed(),
	}
	for i := range pm.workers {
		m := newPatternMiner()
		m.fillStep, m.sampleFromNs, m.sampleToNs = into.fillStep, into.sampleFromNs, into.sampleToNs
		m.shortWholeLine = into.shortWholeLine
		w := &patternMineWorker{miner: m, queue: make(chan []patternMineRow, patternMineQueueBatches)}
		pm.workers[i] = w
		pm.wg.Add(1)
		go pm.run(w)
	}
	return pm
}

func (pm *patternParallelMiner) run(w *patternMineWorker) {
	defer pm.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			// Raised again on the request's goroutine by finish, as a panic of serial mining would be.
			w.panicked = r
			for batch := range w.queue { // drained so the reader is never blocked
				_ = batch
			}
		}
	}()
	for batch := range w.queue {
		for i := range batch {
			row := &batch[i]
			made := len(w.miner.order)
			w.miner.observeStreamLine(row.stream, row.level, row.msg, row.bucket, row.count)
			if len(w.miner.order) > made {
				w.made = append(w.made, patternMineMade{seq: row.seq, key: patternMinerKey{stream: row.stream, level: row.level}, c: w.miner.order[made]})
			}
		}
		clear(batch)
		batch = batch[:0]
		patternMineBatches.Put(&batch)
	}
}

// observe hands a line to the worker that holds its stream.
func (pm *patternParallelMiner) observe(stream, level, msg string, bucket int64, count int) {
	i := int(maphash.String(pm.seed, stream) % uint64(len(pm.workers)))
	batch := pm.pending[i]
	if batch == nil {
		batch = *patternMineBatches.Get().(*[]patternMineRow)
	}
	batch = append(batch, patternMineRow{stream: stream, level: level, msg: msg, bucket: bucket, count: count, seq: pm.seq})
	pm.seq++
	pm.pendingBytes[i] += len(msg)
	if len(batch) == patternMineBatchRows || pm.pendingBytes[i] >= patternMineBatchBytes {
		pm.workers[i].queue <- batch
		batch, pm.pendingBytes[i] = nil, 0
	}
	pm.pending[i] = batch
}

// finish waits for the workers to mine every line handed to them and puts their miners into the read's miner, in
// the order serial mining makes them. A read that failed part way keeps what its lines before the failure mined,
// as serial mining does.
func (pm *patternParallelMiner) finish() {
	for i, w := range pm.workers {
		if len(pm.pending[i]) > 0 {
			w.queue <- pm.pending[i]
		}
		pm.pending[i] = nil
		close(w.queue)
	}
	pm.wg.Wait()
	releasePatternMineWorkers(pm.acquired)
	pm.acquired = 0
	var made []patternMineMade
	for _, w := range pm.workers {
		if w.panicked != nil {
			panic(w.panicked)
		}
		made = append(made, w.made...)
	}
	slices.SortFunc(made, func(a, b patternMineMade) int { return a.seq - b.seq })
	for _, m := range made {
		pm.into.miners[m.key] = m.c
		pm.into.order = append(pm.into.order, m.c)
	}
}

// patternLineSink is where a read's decoded lines are mined: the parallel miner when there is one, the read's
// miner otherwise.
func patternLineSink(miner *patternMiner, pm *patternParallelMiner) func(stream, level, msg string, bucket int64, count int) {
	if pm != nil {
		return pm.observe
	}
	return func(stream, level, msg string, bucket int64, count int) {
		miner.observeStreamLine(stream, level, msg, bucket, count)
	}
}
