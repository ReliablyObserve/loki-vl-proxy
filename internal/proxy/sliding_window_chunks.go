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
	tumbling := window == step
	// The buckets of the grids: steps+1 windows of a tumbling grid, steps A and C buckets otherwise.
	buckets := steps
	if tumbling {
		stats.windowGrid = true
		buckets = steps + 1
	}
	var carried []slidingCarry
	if !tumbling {
		if err := p.fetchSlidingStatsGrid(ctx, stats, baseQuery, withBytes, slidingGridSeed, start.Add(-window), 1, window); err != nil {
			return err
		}
		carried = carryWindows(nil, stats, len(stats.records))
		stats.records = stats.records[:0]
	}
	rowLimit, err := p.manualMetricRowBudget()
	if err != nil {
		return err
	}
	sizeKey := slidingChunkSizeKey(ctx, baseQuery, step, window, withBytes)
	size := max(1, buckets)
	if known, measured := p.listedChunkSize(ctx, sizeKey); measured && !slidingChunksOff {
		size = min(size, int(known))
	}
	shrunk, fitted := false, 0
	for first := 0; first < buckets || first == 0 && buckets == 0; {
		last := min(buckets, first+size) // exclusive
		n := last - first
		stats.records = stats.records[:0]
		err := error(nil)
		rowsC := 0
		if n > 0 {
			if tumbling {
				err = p.fetchSlidingStatsGrid(ctx, stats, baseQuery, withBytes, slidingGridA, start.Add(-window).Add(time.Duration(first)*step), n, step)
			} else {
				// Lines leaving the windows are read before the lines entering them, as in one read. Across chunks a
				// line ingested between two chunks' reads into a span one chunk read as entering lines and a later
				// one as leaving lines is subtracted without being added; a series whose window falls to zero then
				// drops out (a live range's newest data only, as a refresh reads it again).
				err = p.fetchSlidingStatsGrid(ctx, stats, baseQuery, withBytes, slidingGridC, start.Add(-window).Add(time.Duration(first)*step), n, step)
				rowsC = len(stats.records)
				if err == nil {
					err = p.fetchSlidingStatsGrid(ctx, stats, baseQuery, withBytes, slidingGridA, start.Add(time.Duration(first)*step), n, step)
				}
			}
		}
		if slidingChunksObserve != nil {
			slidingChunksObserve(first, last, len(stats.records), err == nil)
		}
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
		// The steps first..last-1 (and the last step after the last chunk): the windows of a tumbling grid are its
		// buckets; otherwise W(first), carried, then the chunk's buckets.
		final := last >= buckets
		if shrunk && (fitted == 0 || n < fitted) && (!final || fitted == 0) {
			fitted = n // the smallest whole chunk a read held after the cut (a final remainder only when alone)
		}
		limit := int32(n)
		localSteps := n
		if tumbling {
			localSteps = n - 1
		} else if final {
			limit = int32(n) + 1
		}
		if !tumbling {
			for _, c := range carried {
				stats.records = append(stats.records, slidingRecord{series: c.series, grid: slidingGridSeed, count: c.count, bytes: c.bytes})
			}
		}
		if err := r.rank(ctx, stats, withBytes, scale, localSteps, int32(first), limit, n < buckets); err != nil {
			return err
		}
		if !tumbling && !final {
			carried = carryWindows(carried[:0], stats, len(stats.records))
		}
		if final {
			break
		}
		first = last
		size = int(nextListedChunkSize(int64(n), int64(buckets), statsReadMeter{series: gridRows}, rowLimit, 0))
	}
	if shrunk && fitted > 0 {
		// A repeat starts at a chunk a read held after the cut instead of reading the whole range again.
		p.setLocalReadCacheWithTTL(sizeKey, []byte(strconv.Itoa(fitted)), listedChunkSizeTTL)
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
