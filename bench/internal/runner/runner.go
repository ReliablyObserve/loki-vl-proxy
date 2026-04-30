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
	// queries valid (no future timestamps) while producing a realistic mix of
	// cache hits (small shift → overlapping windows), partial hits, and misses.
	TimeJitter time.Duration
	// UniqueWindows, if true, applies a deterministic per-worker time offset so
	// every worker sends a distinct URL on every request.  The offset is
	// workerID × 1 s shifted backward, guaranteeing the singleflight coalescer
	// never fires and the response cache never warms.  Use this to measure raw
	// proxy machinery overhead (translation + HTTP proxying + response shaping)
	// without cache or coalescer short-circuiting the result.
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
			// Deterministic per-worker offset: each worker shifts its queries
			// backward by workerID × 1 s.  Combined with the per-request
			// request-count increment below, this guarantees every URL is unique
			// across all workers and all requests within a worker, so the
			// singleflight coalescer never fires and the response cache never
			// warms.  The shift stays well within any workload window (even a
			// 1-minute window accommodates up to 60 unique workers).
			workerOffset := time.Duration(workerID) * time.Second
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
					// Deterministic offset = worker offset + per-request sequential
					// step (1 ms per request) so even within a single worker no two
					// requests share the same URL key.
					uniqueShift := workerOffset + time.Duration(requestSeq)*time.Millisecond
					rawURL = shiftTimeParams(rawURL, uniqueShift)
					requestSeq++
				case cfg.TimeJitter > 0:
					rawURL = applyJitter(rawURL, cfg.TimeJitter, rng)
				}
				start := time.Now()
				n, statusCode, err := doRequest(rawURL)
				elapsed := time.Since(start)
				samples <- sample{
					name:       q.Name,
					latency:    elapsed,
					bytes:      n,
					isErr:      err != nil,
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
// backward by a uniform random amount in [0, jitter].  All three params are
// shifted by the same offset so window sizes are preserved; shifting only
// backward keeps every timestamp in the past (no future queries).
func applyJitter(rawURL string, jitter time.Duration, rng *rand.Rand) string {
	shift := time.Duration(rng.Int63n(int64(jitter))) // uniform in [0, jitter)
	return shiftTimeParams(rawURL, shift)
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

func doRequest(url string) (int64, int, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return n, resp.StatusCode, err
	}
	if resp.StatusCode >= 400 {
		return n, resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return n, resp.StatusCode, nil
}
