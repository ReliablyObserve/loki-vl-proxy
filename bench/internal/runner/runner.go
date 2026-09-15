// Package runner executes concurrent query workloads against a target endpoint.
package runner

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/degraded"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/histogram"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

// Config controls a single benchmark run.
type Config struct {
	TargetURL   string
	Concurrency int
	Duration    time.Duration
	Queries     []workload.Query
	Verbose     bool
	// TimeJitter, if non-zero, randomly shifts each request's time window by a
	// uniform random amount in [-TimeJitter, 0].  Shifting only backward keeps
	// every window at or before the workload reference time (the seeded data
	// end) while producing a realistic mix of cache hits (small shift →
	// overlapping windows), partial hits, and misses.  For range queries the
	// shift is a whole number of steps so the step-aligned axis is preserved.
	TimeJitter time.Duration
	// MinTime, when set, is the start of the seeded data: jitter never shifts a
	// query's evaluated window (start or time minus its range-vector lookback)
	// before it.
	MinTime time.Time
	// UniqueWindows, if true, shifts every request backward by a distinct
	// number of whole steps (whole seconds for queries without a step), so
	// step alignment on the backend cannot map two requests onto the same
	// window and neither the singleflight coalescer nor a response cache can
	// answer one request from another. With MinTime set the shifted window
	// stays inside the data; once a query runs out of distinct windows the
	// sequence wraps. Only meaningful against targets without response caches
	// (loki-bench allows it only with --cache-mode=cold).
	UniqueWindows bool
}

// Result holds the outcome of one benchmark run.
type Result struct {
	Target      string
	Workload    string
	Concurrency int
	Duration    time.Duration
	// Per-query stats keyed by query name.
	ByQuery map[string]*histogram.Stats
	// Aggregate across all queries.
	Overall histogram.Stats
}

var httpClient = &http.Client{
	Timeout: 120 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	},
}

// Run executes the benchmark and returns aggregated results.
func Run(ctx context.Context, cfg Config) Result {
	if len(cfg.Queries) == 0 {
		return Result{}
	}

	type sample struct {
		name       string
		latency    time.Duration
		bytes      int64
		isErr      bool
		degraded   bool
		statusCode int
	}

	samples := make(chan sample, cfg.Concurrency*4)
	var wg sync.WaitGroup

	deadline := time.Now().Add(cfg.Duration)

	// Spawn workers.
	for i := range cfg.Concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			qi := workerID % len(cfg.Queries) // round-robin query selection
			rng := rand.New(rand.NewSource(int64(workerID) ^ time.Now().UnixNano()))
			requestSeq := 0 // monotonically increments per request within worker
			for {
				if ctx.Err() != nil || time.Now().After(deadline) {
					return
				}
				q := cfg.Queries[qi%len(cfg.Queries)]
				qi++

				rawURL := q.URL(cfg.TargetURL)
				switch {
				case cfg.UniqueWindows:
					rawURL = shiftTimeParams(rawURL, uniqueShift(q.Params, workerID, cfg.Concurrency, requestSeq, cfg.MinTime))
					requestSeq++
				case cfg.TimeJitter > 0:
					rawURL = applyJitter(rawURL, cfg.TimeJitter, cfg.MinTime, rng)
				}
				start := time.Now()
				n, statusCode, isDegraded, err := doRequest(rawURL)
				elapsed := time.Since(start)
				if err != nil && cfg.Verbose {
					fmt.Printf("    error %s: %v\n", q.Name, err)
				}
				samples <- sample{
					name:       q.Name,
					latency:    elapsed,
					bytes:      n,
					isErr:      err != nil,
					degraded:   isDegraded,
					statusCode: statusCode,
				}
			}
		}(i)
	}

	// Close samples channel when all workers finish.
	go func() {
		wg.Wait()
		close(samples)
	}()

	// Collect into per-query histograms.
	hists := make(map[string]*histogram.Histogram)
	overall := histogram.New()

	for s := range samples {
		h, ok := hists[s.name]
		if !ok {
			h = histogram.New()
			hists[s.name] = h
		}
		h.Record(s.latency, s.bytes, s.isErr, s.statusCode)
		overall.Record(s.latency, s.bytes, s.isErr, s.statusCode)
		if s.degraded {
			h.RecordDegraded()
			overall.RecordDegraded()
		}
	}

	byQuery := make(map[string]*histogram.Stats, len(hists))
	queryDuration := cfg.Duration // approximate — each query ran for cfg.Duration total
	for name, h := range hists {
		snap := h.Snapshot(queryDuration)
		byQuery[name] = &snap
	}
	overallSnap := overall.Snapshot(cfg.Duration)

	return Result{
		Target:      cfg.TargetURL,
		Workload:    "",
		Concurrency: cfg.Concurrency,
		Duration:    cfg.Duration,
		ByQuery:     byQuery,
		Overall:     overallSnap,
	}
}

// applyJitter shifts the start/end/time nanosecond params of a query URL
// backward by a random amount in [0, jitter).  All three params are shifted
// by the same offset so window sizes are preserved; shifting only backward
// keeps every timestamp at or before the reference time.
func applyJitter(rawURL string, jitter time.Duration, minTime time.Time, rng *rand.Rand) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	shift := jitterShift(u.Query(), time.Duration(rng.Int63n(int64(jitter))), minTime)
	return shiftTimeParams(rawURL, shift)
}

// jitterShift bounds a raw backward shift for one request: range queries shift
// by whole steps (keeping start/end aligned to the step), and no shift moves
// the earliest evaluated time (start or time minus the range-vector lookback)
// before minTime (the start of the seeded data).
func jitterShift(params url.Values, raw time.Duration, minTime time.Time) time.Duration {
	step := workload.StepOf(params)
	shift := raw
	if step > 0 {
		shift -= shift % step
	}
	if !minTime.IsZero() {
		if earliest, ok := workload.EarliestEvaluated(params); ok {
			room := time.Duration(earliest - minTime.UnixNano())
			if room < 0 {
				room = 0
			}
			if step > 0 {
				room -= room % step
			}
			if shift > room {
				shift = room
			}
		}
	}
	return shift
}

// shiftTimeParams shifts start/end/time nanosecond params of a query URL
// backward by exactly shift.  Window sizes are preserved.
func shiftTimeParams(rawURL string, shift time.Duration) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	changed := false
	for _, key := range []string{"start", "end", "time"} {
		v := q.Get(key)
		if v == "" {
			continue
		}
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		q.Set(key, strconv.FormatInt(ns-shift.Nanoseconds(), 10))
		changed = true
	}
	if !changed {
		return rawURL
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// uniqueShift returns the backward shift of request seq from worker workerID
// among concurrency workers: (seq × concurrency + workerID) whole units, where
// the unit is the query's step or one second without a step. The index is
// distinct for every (worker, request) pair. With minTime set it wraps within
// the DistinctWindows that keep the evaluated window (start or time minus the
// range-vector lookback) at or after minTime.
func uniqueShift(params url.Values, workerID, concurrency, seq int, minTime time.Time) time.Duration {
	unit := shiftUnit(params)
	idx := int64(seq)*int64(concurrency) + int64(workerID)
	if n, bounded := DistinctWindows(params, minTime); bounded {
		idx %= n
	}
	return time.Duration(idx) * unit
}

func shiftUnit(params url.Values) time.Duration {
	if unit := workload.StepOf(params); unit > 0 {
		return unit
	}
	return time.Second
}

// DistinctWindows returns how many distinct whole-unit backward shifts (the
// unshifted window included) keep a query's evaluated window at or after
// minTime. bounded is false when minTime is zero or the query has no time
// bound. A query with fewer distinct windows than concurrent workers repeats
// windows under --unique-windows, so coalescing can answer one request from
// another.
func DistinctWindows(params url.Values, minTime time.Time) (n int64, bounded bool) {
	if minTime.IsZero() {
		return 0, false
	}
	earliest, ok := workload.EarliestEvaluated(params)
	if !ok {
		return 0, false
	}
	slots := (earliest - minTime.UnixNano()) / int64(shiftUnit(params))
	if slots < 0 {
		slots = 0
	}
	return slots + 1, true
}

// doRequest sends one request and drains the body. err is set for transport
// errors and HTTP status >= 400; degraded is set for a successful response that
// carries a degraded-response header or a non-empty "warnings" array.
func doRequest(url string) (int64, int, bool, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return 0, 0, false, err
	}
	defer resp.Body.Close()
	scanner := &degraded.Scanner{}
	n, err := io.Copy(scanner, resp.Body)
	if err != nil {
		return n, resp.StatusCode, false, err
	}
	if resp.StatusCode >= 400 {
		return n, resp.StatusCode, false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return n, resp.StatusCode, scanner.Warnings || len(degraded.FromHeaders(resp.Header)) > 0, nil
}
