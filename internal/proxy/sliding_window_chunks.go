package proxy

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Time chunks of a ranked window recurrence (writeSlidingWindowStatsRange).
//
// A topk or bottomk over a window metric keeps k series per step, so its answer is small whatever the step, while the
// window grids it ranks from hold a row per non-empty step bucket and series: a fine step over a busy grouping
// (Grafana's step over a short range, thousands of client addresses) holds more rows than one read may
// (-manual-range-metric-row-limit). Each step k depends only on the window W(k) of every series, and the recurrence
// W(k+1) = W(k) + A(k) - C(k) carries W from one step to the next. So the steps are ranked in chunks: a chunk of the
// buckets [a, b) reads the A and C buckets of those steps, ranks the steps a..b-1 from W(a) (carried from the chunk
// before, the seed for the first) and carries W(b) to the next. The answer is the unchunked one: every step is ranked
// from the same windows, every window is the same exact integer sum, and each step's heaps are filled by the one
// chunk that holds it.
//
// The first chunk is the whole range, read as before. A chunk whose read passes the row bound is cut to a quarter
// (listedChunkShrink) and read again; after a read, the next chunk is sized from the share of the bound the read used,
// at most half of it (listedChunkLoad). The size a request needed is remembered on this replica for
// listedChunkSizeTTL, so its repeats start at it. A single step whose buckets pass the bound, and a window whose
// series do (the seed), are the only refusals left: Loki's limit error naming -manual-range-metric-row-limit.
//
// The request holds one chunk's rows, the carried window of each series (two numbers), the names of the series met
// (at most three times -manual-range-metric-row-limit, what the three grids of one read can name) and the heaps (k samples per step), and reads
// its chunks one after another; a chunk ranks only the series its rows hold.

// slidingChunksOff ranks in one read, as before the chunks (tests measure the two).
var slidingChunksOff bool

// slidingChunksObserve, when set, sees each chunk a ranking read (tests).
var slidingChunksObserve func(first, last, rows int, ok bool)

// rankSlidingWindowChunks reads the grids of a ranked recurrence in time chunks and fills the ranker; stats keeps
// the series names.
func (p *Proxy) rankSlidingWindowChunks(ctx context.Context, r *slidingRanker, stats *slidingStats, baseQuery string, withBytes bool, scale float64, steps int, start time.Time, step, window time.Duration) error {
	c := &slidingChunkRun{p: p, ctx: ctx, r: r, stats: stats, baseQuery: baseQuery, withBytes: withBytes, scale: scale,
		start: start, step: step, window: window, tumbling: window == step, buckets: steps}
	// The buckets of the grids: steps+1 windows of a tumbling grid, steps A and C buckets otherwise.
	if c.tumbling {
		stats.windowGrid = true
		c.buckets = steps + 1
	}
	if !c.tumbling {
		if err := p.fetchSlidingStatsGrid(ctx, stats, baseQuery, withBytes, slidingGridSeed, start.Add(-window), 1, window); err != nil {
			return err
		}
		c.carried = carryWindows(nil, stats, len(stats.records))
		stats.records = stats.records[:0]
	}
	rowLimit, err := p.manualMetricRowBudget()
	if err != nil {
		return err
	}
	sizeKey := slidingChunkSizeKey(ctx, baseQuery, step, window, withBytes)
	size := p.slidingChunkStartSize(ctx, sizeKey, c.buckets)
	shrunk, fitted := false, 0
	for first := 0; first < c.buckets || first == 0 && c.buckets == 0; {
		last := min(c.buckets, first+size) // exclusive
		n := last - first
		rowsC, err := c.read(first, n)
		var rows *rowLimitError
		if err != nil && errors.As(err, &rows) && n > 1 && !slidingChunksOff {
			size, shrunk = max(1, n/listedChunkShrink), true
			continue
		}
		if err != nil {
			return err
		}
		// The series the ranking names are kept for the answer: they are bounded as the three grids of one read bound
		// them (a seed and two grids of at most rowLimit rows each).
		if len(stats.keys) > 3*rowLimit {
			return newRowLimitError(rowLimit, "series in the windows of one ranking", 0)
		}
		// The rows of the larger grid: the row bound holds each grid read on its own.
		gridRows := max(rowsC, len(stats.records)-rowsC)
		final := last >= c.buckets
		if shrunk && (fitted == 0 || n < fitted) && (!final || fitted == 0) {
			fitted = n // the smallest whole chunk a read held after the cut (a final remainder only when alone)
		}
		if err := c.rank(first, n, final); err != nil {
			return err
		}
		if final {
			break
		}
		first = last
		size = int(nextListedChunkSize(int64(n), int64(c.buckets), statsReadMeter{series: gridRows}, rowLimit, 0))
	}
	if shrunk && fitted > 0 {
		// A repeat starts at a chunk a read held after the cut instead of reading the whole range again.
		p.setLocalReadCacheWithTTL(sizeKey, []byte(strconv.Itoa(fitted)), listedChunkSizeTTL)
	}
	return nil
}

// slidingChunkStartSize is the first chunk's size: the whole range of buckets, or less when this replica measured a
// smaller size for the same read (a hint from the local cache, held to [1, buckets]).
func (p *Proxy) slidingChunkStartSize(ctx context.Context, key string, buckets int) int {
	size := max(1, buckets)
	if p.cache == nil || slidingChunksOff {
		return size
	}
	raw, ok := p.cachePeek(ctx, p.cache, key)
	if !ok {
		return size
	}
	// Read as an int: a value past the int range is an error, not a wrapped number.
	if known, err := strconv.Atoi(string(raw)); err == nil && known > 0 && known < size {
		size = known
	}
	return size
}

// slidingChunkRun is one ranked recurrence read in chunks: its request, grids and the window carried between chunks.
type slidingChunkRun struct {
	p            *Proxy
	ctx          context.Context
	r            *slidingRanker
	stats        *slidingStats
	baseQuery    string
	withBytes    bool
	scale        float64
	start        time.Time
	step, window time.Duration
	tumbling     bool
	buckets      int
	carried      []slidingCarry
}

// read reads the buckets [first, first+n) of the chunk's grids into stats, and returns the rows of its C grid (the
// rest are A's).
func (c *slidingChunkRun) read(first, n int) (int, error) {
	stats := c.stats
	stats.records = stats.records[:0]
	var err error
	rowsC := 0
	if n > 0 {
		at := time.Duration(first) * c.step
		if c.tumbling {
			err = c.p.fetchSlidingStatsGrid(c.ctx, stats, c.baseQuery, c.withBytes, slidingGridA, c.start.Add(-c.window).Add(at), n, c.step)
		} else {
			// Lines leaving the windows are read before the lines entering them, as in one read. Across chunks a
			// line ingested between two chunks' reads into a span one chunk read as entering lines and a later
			// one as leaving lines is subtracted without being added; a series whose window falls to zero then
			// drops out (a live range's newest data only, as a refresh reads it again).
			err = c.p.fetchSlidingStatsGrid(c.ctx, stats, c.baseQuery, c.withBytes, slidingGridC, c.start.Add(-c.window).Add(at), n, c.step)
			rowsC = len(stats.records)
			if err == nil {
				err = c.p.fetchSlidingStatsGrid(c.ctx, stats, c.baseQuery, c.withBytes, slidingGridA, c.start.Add(at), n, c.step)
			}
		}
	}
	if slidingChunksObserve != nil {
		slidingChunksObserve(first, first+n, len(stats.records), err == nil)
	}
	return rowsC, err
}

// rank ranks the steps first..first+n-1 of the chunk just read (and the last step after the last chunk): the windows
// of a tumbling grid are its buckets; otherwise W(first), carried, then the chunk's buckets, and W(first+n) is carried
// on.
func (c *slidingChunkRun) rank(first, n int, final bool) error {
	stats := c.stats
	limit := int32(n)
	localSteps := n
	if c.tumbling {
		localSteps = n - 1
	} else if final {
		limit = int32(n) + 1
	}
	if !c.tumbling {
		for _, w := range c.carried {
			stats.records = append(stats.records, slidingRecord{series: w.series, grid: slidingGridSeed, count: w.count, bytes: w.bytes})
		}
	}
	if err := c.r.rank(c.ctx, stats, c.withBytes, c.scale, localSteps, int32(first), limit, n < c.buckets); err != nil {
		return err
	}
	if !c.tumbling && !final {
		c.carried = carryWindows(c.carried[:0], stats, len(stats.records))
	}
	return nil
}

// slidingCarry is the window of one series carried to the next chunk.
type slidingCarry struct {
	series       int32
	count, bytes float64
}

// carryWindows is the window after the records of stats (the seed and the chunk's A and C buckets): the sum of every
// record of a series, A and the seed added, C subtracted. Series whose window holds no line are left out.
func carryWindows(out []slidingCarry, stats *slidingStats, n int) []slidingCarry {
	count := make(map[int32]*slidingCarry, len(out))
	order := make([]int32, 0)
	for _, rec := range stats.records[:n] {
		c := count[rec.series]
		if c == nil {
			c = &slidingCarry{series: rec.series}
			count[rec.series] = c
			order = append(order, rec.series)
		}
		sign := 1.0
		if rec.grid == slidingGridC {
			sign = -1
		}
		c.count += sign * rec.count
		c.bytes += sign * rec.bytes
	}
	out = out[:0]
	for _, id := range order {
		if c := count[id]; c.count > 0 {
			out = append(out, *c)
		}
	}
	return out
}

// slidingChunkSizeKey names the chunk size a ranked recurrence of query needed, in the tenant's scope.
func slidingChunkSizeKey(ctx context.Context, query string, step, window time.Duration, withBytes bool) string {
	kind := "c"
	if withBytes {
		kind = "b"
	}
	return "sliding-window-chunk:v1:" + getOrgID(ctx) + ":" + kind + ":" + strconv.FormatInt(int64(step), 10) + ":" + strconv.FormatInt(int64(window), 10) + ":" + strings.TrimSpace(query)
}
