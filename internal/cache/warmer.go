package cache

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// WarmFunc is called to refresh a query — returns cached value or nil.
type WarmFunc func(ctx context.Context, query string) ([]byte, error)

// Warmer periodically re-fetches top-N queries to keep the cache hot.
type Warmer struct {
	cache    *Cache
	topN     func(n int) []string // returns top N queries to warm
	warmFn   WarmFunc
	interval time.Duration
	count    int // how many queries to warm
	log      *slog.Logger
	done     chan struct{}

	WarmedTotal atomic.Int64
	ErrorsTotal atomic.Int64
}

// WarmerConfig configures the cache warmer.
type WarmerConfig struct {
	Interval time.Duration // How often to warm (default: 60s)
	Count    int           // How many top queries to warm (default: 10)
}

// NewWarmer creates a cache warmer.
func NewWarmer(c *Cache, topN func(int) []string, warmFn WarmFunc, cfg WarmerConfig) *Warmer {
	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.Count == 0 {
		cfg.Count = 10
	}
	return &Warmer{
		cache:    c,
		topN:     topN,
		warmFn:   warmFn,
		interval: cfg.Interval,
		count:    cfg.Count,
		log:      slog.Default().With("component", "cache_warmer"),
		done:     make(chan struct{}),
	}
}

// Start begins the warming loop in a background goroutine.
func (w *Warmer) Start() {
	go w.loop()
}

// Stop signals the warmer to stop.
func (w *Warmer) Stop() {
	select {
	case <-w.done:
	default:
		close(w.done)
	}
}

func (w *Warmer) loop() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.warm()
		}
	}
}

func (w *Warmer) warm() {
	queries := w.topN(w.count)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, q := range queries {
		// Skip if already in cache
		if _, ok := w.cache.Get(q); ok {
			continue
		}

		val, err := w.warmFn(ctx, q)
		if err != nil {
			w.ErrorsTotal.Add(1)
			w.log.Debug("cache warm error", "query", q, "error", err)
			continue
		}
		if val != nil {
			w.cache.Set(q, val)
			w.WarmedTotal.Add(1)
		}
	}
}
