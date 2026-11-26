package proxy

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Restart hand-over of the response cache (internal/cache/handover.go): a
// graceful Shutdown keeps the memory tier's hot set in the disk tier, and the
// next Init loads it back before /ready answers 200. Both are off by default
// (-cache-shutdown-flush-timeout, -cache-startup-warm-bytes).

const defaultCacheStartupWarmTimeout = 120 * time.Second

const (
	// cacheWarmProgressInterval is how often a running warm-up logs its
	// progress.
	cacheWarmProgressInterval = 2 * time.Second
	// cacheWarmEstimateBytesPerSecond is the disk throughput the start line's
	// estimate assumes, before anything was measured: a modest network disk.
	cacheWarmEstimateBytesPerSecond = 125_000_000
)

type cacheHandoverState struct {
	flushTimeout time.Duration
	warmBytes    int64
	warmTimeout  time.Duration
	scope        func() string
	// peerRestore lets the warm-up restore from peers when there is nothing
	// local to warm from; peerConcurrency is the peers streamed at once.
	peerRestore     bool
	peerConcurrency int
	// progressEvery is the progress log's interval.
	progressEvery time.Duration

	// ready is false while the start-up warm-up runs; /ready also stops
	// waiting once warmDeadline (UnixNano) passes, whatever the goroutine does.
	ready        atomic.Bool
	warmDeadline atomic.Int64
	mu           sync.Mutex
	cancel       context.CancelFunc
	done         chan struct{}
}

func newCacheHandoverState(cfg Config) *cacheHandoverState {
	timeout := cfg.CacheStartupWarmTimeout
	if timeout <= 0 {
		timeout = defaultCacheStartupWarmTimeout
	}
	s := &cacheHandoverState{
		flushTimeout: max(cfg.CacheShutdownFlushTimeout, 0),
		warmBytes:    max(cfg.CacheStartupWarmBytes, 0),
		warmTimeout:  timeout,
		scope:        sync.OnceValue(func() string { return "" }),

		peerRestore:     cfg.CacheStartupPeerRestore,
		peerConcurrency: max(cfg.CacheStartupPeerRestoreConcurrency, 0),

		progressEvery: cacheWarmProgressInterval,
	}
	if cfg.CacheHandoverScope != nil {
		s.scope = sync.OnceValue(cfg.CacheHandoverScope)
	}
	s.ready.Store(true)
	if s.flushTimeout > 0 && cfg.Cache != nil {
		cfg.Cache.EnableHandoverHits()
	}
	return s
}

// startResponseCacheWarm starts the warm-up from a hand-over the disk tier
// found, or drops the hand-over when the warm-up is off. With nothing local to
// warm from (no hand-over, an empty or no disk tier) and peers, it restores
// from the peers instead (internal/cache/restore_peer.go), under the same
// budget and deadline. Shutdown stops it.
func (p *Proxy) startResponseCacheWarm() {
	h := p.cacheHandover
	if h == nil || p.cache == nil {
		return
	}
	fromDisk := p.cache.HasHandover()
	if fromDisk && h.warmBytes <= 0 {
		p.cache.DropHandover()
		return
	}
	fromPeers := !fromDisk && h.warmBytes > 0 && h.peerRestore && p.cache.PeerRestoreWanted()
	if !fromDisk && !fromPeers {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.warmTimeout)
	done := make(chan struct{})
	h.mu.Lock()
	h.cancel, h.done = cancel, done
	h.mu.Unlock()
	h.warmDeadline.Store(time.Now().Add(h.warmTimeout).UnixNano())
	h.ready.Store(false)
	go func() {
		defer close(done)
		defer cancel()
		defer h.ready.Store(true)
		if fromPeers {
			p.restoreResponseCacheFromPeers(ctx, h)
			return
		}
		stopProgress := p.logResponseCacheWarmProgress(ctx, h.progressEvery, func(s cache.HandoverStats) cache.RestoreProgress { return s.Restore })
		report := p.cache.WarmFromHandover(ctx, cache.HandoverWarmOptions{Scope: h.scope(), MaxBytes: h.warmBytes, OnPlan: p.logResponseCacheWarmPlan})
		stopProgress()
		p.logResponseCacheWarmEnd(report)
	}()
}

// restoreResponseCacheFromPeers runs the peer bulk restore and logs its start,
// progress and end.
func (p *Proxy) restoreResponseCacheFromPeers(ctx context.Context, h *cacheHandoverState) {
	timeout := time.Duration(0)
	if deadline, ok := ctx.Deadline(); ok {
		timeout = max(time.Until(deadline), 0)
	}
	known := 0
	if p.peerCache != nil {
		known = len(p.peerCache.Peers())
	}
	p.log.Info("response cache restore from peers started",
		"source", string(cache.RestoreSourcePeer),
		"peers", known,
		"budget_bytes", h.warmBytes,
		"timeout_ms", timeout.Milliseconds(),
		"concurrency", h.peerConcurrency,
	)
	stopProgress := p.logResponseCacheWarmProgress(ctx, h.progressEvery, func(s cache.HandoverStats) cache.RestoreProgress { return s.PeerRestore })
	report := p.cache.RestoreFromPeers(ctx, cache.PeerRestoreOptions{Scope: h.scope(), MaxBytes: h.warmBytes, Concurrency: h.peerConcurrency})
	stopProgress()
	progress := report.Progress
	var reasons, peers []any
	for _, reason := range cache.RestoreSkipReasons() {
		if n := progress.Skipped[reason]; n > 0 {
			reasons = append(reasons, slog.Int64(reason.String(), n))
		}
	}
	for _, peer := range report.Peers {
		peers = append(peers, slog.Group(peer.Addr, "outcome", peer.Outcome, "planned", peer.Planned, "received", peer.Received, "bytes", peer.Bytes, "attempts", peer.Attempts))
	}
	level := slog.LevelInfo
	if report.Outcome == "deadline" {
		level = slog.LevelWarn
	}
	p.log.Log(context.Background(), level, "response cache restored from peers",
		"source", string(progress.Source),
		"outcome", report.Outcome,
		"entries", progress.RestoredEntries,
		"planned_entries", progress.PlannedEntries,
		"skipped", progress.SkippedEntries(),
		slog.Group("skipped_by", reasons...),
		"bytes", progress.RestoredBytes,
		"wire_bytes_processed", progress.ProcessedBytes,
		"bytes_per_second", int64(progress.BytesPerSecond()),
		"disk_writes", report.DiskWrites,
		"disk_writes_skipped", report.DiskSkipped,
		"duration_ms", progress.Elapsed.Milliseconds(),
		"timeout_ms", progress.Timeout.Milliseconds(),
		slog.Group("peers", peers...),
	)
}

// logResponseCacheWarmPlan logs what a warm-up is about to restore.
func (p *Proxy) logResponseCacheWarmPlan(plan cache.RestoreProgress) {
	read := plan.PlannedBytes
	if plan.BudgetBytes < read {
		read = plan.BudgetBytes
	}
	p.log.Info("response cache warm-up started",
		"source", string(plan.Source),
		"planned_entries", plan.PlannedEntries,
		"planned_disk_bytes", plan.PlannedBytes,
		"budget_bytes", plan.BudgetBytes,
		"timeout_ms", plan.Timeout.Milliseconds(),
		"estimated_duration_ms", read*1000/cacheWarmEstimateBytesPerSecond,
		"estimate_assumes_disk_bytes_per_second", cacheWarmEstimateBytesPerSecond,
	)
}

// logResponseCacheWarmProgress logs the running warm-up every interval, and
// warns once when it is not going to finish before ctx's deadline. The returned
// function stops it and waits for it.
func (p *Proxy) logResponseCacheWarmProgress(ctx context.Context, interval time.Duration, of func(cache.HandoverStats) cache.RestoreProgress) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		warned := false
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			progress := of(p.cache.HandoverStats())
			if progress.Outcome != cache.RestoreRunning {
				continue
			}
			fields := warmProgressFields(progress)
			eta, known := progress.Remaining()
			if known {
				fields = append(fields, "eta_ms", eta.Milliseconds())
			}
			var left time.Duration
			deadline, bounded := ctx.Deadline()
			if bounded {
				left = max(time.Until(deadline), 0)
				fields = append(fields, "time_left_ms", left.Milliseconds())
			}
			p.log.Info("response cache warm-up progress", fields...)
			if !warned && bounded && known && eta > left {
				warned = true
				p.log.Warn("response cache warm-up will not finish before its timeout (-cache-startup-warm-timeout); the hottest entries are restored first", fields...)
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func warmProgressFields(progress cache.RestoreProgress) []any {
	return []any{
		"source", string(progress.Source),
		"percent", int(progress.Fraction() * 100),
		"entries", progress.RestoredEntries,
		"planned_entries", progress.PlannedEntries,
		"skipped", progress.SkippedEntries(),
		"bytes", progress.RestoredBytes,
		"disk_bytes_processed", progress.ProcessedBytes,
		"planned_disk_bytes", progress.PlannedBytes,
		"bytes_per_second", int64(progress.BytesPerSecond()),
		"elapsed_ms", progress.Elapsed.Milliseconds(),
	}
}

// logResponseCacheWarmEnd logs how a warm-up ended: a warning when the timeout
// cut it.
func (p *Proxy) logResponseCacheWarmEnd(report cache.HandoverWarmReport) {
	progress := report.Progress
	var reasons []any
	for _, reason := range cache.RestoreSkipReasons() {
		if n := progress.Skipped[reason]; n > 0 {
			reasons = append(reasons, slog.Int64(reason.String(), n))
		}
	}
	level := slog.LevelInfo
	if report.Reason == "deadline" {
		level = slog.LevelWarn
	}
	p.log.Log(context.Background(), level, "response cache warmed from the restart hand-over",
		"source", string(progress.Source),
		"outcome", report.Reason,
		"disk_entries", report.DiskEntries,
		"memory_entries", report.MemoryEntries,
		"planned_entries", progress.PlannedEntries,
		"skipped", report.Skipped,
		slog.Group("skipped_by", reasons...),
		"bytes", report.Bytes,
		"disk_bytes_processed", progress.ProcessedBytes,
		"bytes_per_second", int64(progress.BytesPerSecond()),
		"duration_ms", report.Duration.Milliseconds(),
		"timeout_ms", progress.Timeout.Milliseconds(),
	)
}

// responseCacheWarming reports whether /ready waits for the warm-up: until it
// finishes, and never past its timeout.
func (p *Proxy) responseCacheWarming() bool {
	h := p.cacheHandover
	return h != nil && !h.ready.Load() && time.Now().UnixNano() < h.warmDeadline.Load()
}

// StopPeerBulkStreams ends the peer bulk restore streams this replica serves
// and refuses new ones; the HTTP server calls it when it starts shutting
// down, so a stream does not hold the drain.
func (p *Proxy) StopPeerBulkStreams() {
	if p != nil && p.peerCache != nil {
		p.peerCache.StopBulk()
	}
}

// stopResponseCacheWarm cancels a running warm-up and waits for it.
func (p *Proxy) stopResponseCacheWarm() {
	h := p.cacheHandover
	if h == nil {
		return
	}
	h.mu.Lock()
	cancel, done := h.cancel, h.done
	h.cancel, h.done = nil, nil
	h.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// flushResponseCacheHandover writes the hand-over within its time budget.
func (p *Proxy) flushResponseCacheHandover() {
	h := p.cacheHandover
	if h == nil || p.cache == nil || h.flushTimeout <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.flushTimeout)
	defer cancel()
	report, err := p.cache.FlushHandover(ctx, cache.HandoverFlushOptions{Scope: h.scope(), MemoryBytes: h.warmBytes})
	if err != nil {
		p.log.Warn("response cache hand-over flush failed", "error", err)
		return
	}
	p.log.Info("response cache hand-over flushed",
		"complete", report.Complete,
		"ranked", report.Ranked,
		"disk_writes", report.DiskWrites,
		"memory_entries", report.MemoryEntries,
		"memory_bytes", report.MemoryBytes,
		"duration_ms", report.Duration.Milliseconds(),
	)
}
