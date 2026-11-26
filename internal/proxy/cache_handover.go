package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Restart hand-over of the response cache (internal/cache/handover.go): a
// graceful Shutdown keeps the memory tier's hot set in the disk tier, and the
// next Init loads it back before /ready answers 200. Both are off by default
// (-cache-shutdown-flush-timeout, -cache-startup-warm-bytes).

const defaultCacheStartupWarmTimeout = 10 * time.Second

type cacheHandoverState struct {
	flushTimeout time.Duration
	warmBytes    int64
	warmTimeout  time.Duration
	scope        string

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
		scope:        cfg.CacheHandoverScope,
	}
	s.ready.Store(true)
	if s.flushTimeout > 0 && cfg.Cache != nil {
		cfg.Cache.EnableHandoverHits()
	}
	return s
}

// startResponseCacheWarm starts the warm-up from a hand-over the disk tier
// found, or drops the hand-over when the warm-up is off. Shutdown stops it.
func (p *Proxy) startResponseCacheWarm() {
	h := p.cacheHandover
	if h == nil || p.cache == nil || !p.cache.HasHandover() {
		return
	}
	if h.warmBytes <= 0 {
		p.cache.DropHandover()
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
		report := p.cache.WarmFromHandover(ctx, cache.HandoverWarmOptions{Scope: h.scope, MaxBytes: h.warmBytes})
		p.log.Info("response cache warmed from the restart hand-over",
			"outcome", report.Reason,
			"disk_entries", report.DiskEntries,
			"memory_entries", report.MemoryEntries,
			"skipped", report.Skipped,
			"bytes", report.Bytes,
			"duration_ms", report.Duration.Milliseconds(),
		)
	}()
}

// responseCacheWarming reports whether /ready waits for the warm-up: until it
// finishes, and never past its timeout.
func (p *Proxy) responseCacheWarming() bool {
	h := p.cacheHandover
	return h != nil && !h.ready.Load() && time.Now().UnixNano() < h.warmDeadline.Load()
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
	report, err := p.cache.FlushHandover(ctx, cache.HandoverFlushOptions{Scope: h.scope, MemoryBytes: h.warmBytes})
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
