package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects proxy metrics in Prometheus exposition format.
type Metrics struct {
	mu sync.RWMutex

	// Request counters by endpoint and status
	requestsTotal    map[string]*atomic.Int64 // "endpoint:status" → count
	requestDurations map[string]*histogram     // "endpoint" → duration histogram

	// Cache stats
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64

	// Translation stats
	translationsTotal  atomic.Int64
	translationErrors  atomic.Int64

	// Active connections
	activeRequests atomic.Int64

	// Startup time
	startTime time.Time
}

type histogram struct {
	mu    sync.Mutex
	sum   float64
	count int64
	// Buckets: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
	buckets []float64
	counts  []int64
}

var defaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func newHistogram() *histogram {
	return &histogram{
		buckets: defaultBuckets,
		counts:  make([]int64, len(defaultBuckets)),
	}
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.count++
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

func NewMetrics() *Metrics {
	return &Metrics{
		requestsTotal:    make(map[string]*atomic.Int64),
		requestDurations: make(map[string]*histogram),
		startTime:        time.Now(),
	}
}

func (m *Metrics) RecordRequest(endpoint string, statusCode int, duration time.Duration) {
	key := fmt.Sprintf("%s:%d", endpoint, statusCode)
	m.mu.RLock()
	counter, ok := m.requestsTotal[key]
	hist, hok := m.requestDurations[endpoint]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		counter, ok = m.requestsTotal[key]
		if !ok {
			counter = &atomic.Int64{}
			m.requestsTotal[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)

	if !hok {
		m.mu.Lock()
		hist, hok = m.requestDurations[endpoint]
		if !hok {
			hist = newHistogram()
			m.requestDurations[endpoint] = hist
		}
		m.mu.Unlock()
	}
	hist.observe(duration.Seconds())
}

func (m *Metrics) RecordCacheHit()  { m.cacheHits.Add(1) }
func (m *Metrics) RecordCacheMiss() { m.cacheMisses.Add(1) }
func (m *Metrics) RecordTranslation()      { m.translationsTotal.Add(1) }
func (m *Metrics) RecordTranslationError() { m.translationErrors.Add(1) }

// Handler serves Prometheus metrics at /metrics.
func (m *Metrics) Handler(w http.ResponseWriter, r *http.Request) {
	var sb strings.Builder

	sb.WriteString("# HELP loki_vl_proxy_requests_total Total number of proxied requests.\n")
	sb.WriteString("# TYPE loki_vl_proxy_requests_total counter\n")

	m.mu.RLock()
	keys := make([]string, 0, len(m.requestsTotal))
	for k := range m.requestsTotal {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 2)
		endpoint := parts[0]
		status := parts[1]
		count := m.requestsTotal[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_requests_total{endpoint=%q,status=%q} %d\n", endpoint, status, count)
	}

	sb.WriteString("# HELP loki_vl_proxy_request_duration_seconds Request duration histogram.\n")
	sb.WriteString("# TYPE loki_vl_proxy_request_duration_seconds histogram\n")
	endpoints := make([]string, 0, len(m.requestDurations))
	for ep := range m.requestDurations {
		endpoints = append(endpoints, ep)
	}
	sort.Strings(endpoints)
	for _, ep := range endpoints {
		h := m.requestDurations[ep]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_bucket{endpoint=%q,le=\"%g\"} %d\n", ep, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_bucket{endpoint=%q,le=\"+Inf\"} %d\n", ep, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_sum{endpoint=%q} %g\n", ep, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_count{endpoint=%q} %d\n", ep, h.count)
		h.mu.Unlock()
	}
	m.mu.RUnlock()

	sb.WriteString("# HELP loki_vl_proxy_cache_hits_total Cache hits.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_hits_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_cache_hits_total %d\n", m.cacheHits.Load())

	sb.WriteString("# HELP loki_vl_proxy_cache_misses_total Cache misses.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_misses_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_cache_misses_total %d\n", m.cacheMisses.Load())

	sb.WriteString("# HELP loki_vl_proxy_translations_total LogQL to LogsQL translations.\n")
	sb.WriteString("# TYPE loki_vl_proxy_translations_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_translations_total %d\n", m.translationsTotal.Load())

	sb.WriteString("# HELP loki_vl_proxy_translation_errors_total Failed translations.\n")
	sb.WriteString("# TYPE loki_vl_proxy_translation_errors_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_translation_errors_total %d\n", m.translationErrors.Load())

	sb.WriteString("# HELP loki_vl_proxy_uptime_seconds Proxy uptime.\n")
	sb.WriteString("# TYPE loki_vl_proxy_uptime_seconds gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_uptime_seconds %g\n", time.Since(m.startTime).Seconds())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(sb.String()))
}
