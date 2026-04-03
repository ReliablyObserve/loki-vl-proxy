package metrics

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// QueryTracker records query fingerprints for analytics:
// top-N by frequency, top-N by latency, recent errors.
type QueryTracker struct {
	mu      sync.RWMutex
	queries map[string]*QueryStats // fingerprint → stats
	maxSize int

	TotalQueries atomic.Int64
}

// QueryStats tracks per-query statistics.
type QueryStats struct {
	Fingerprint string  `json:"fingerprint"`
	Query       string  `json:"query"`       // original LogQL
	Endpoint    string  `json:"endpoint"`
	Count       int64   `json:"count"`
	TotalMs     float64 `json:"total_ms"`
	MaxMs       float64 `json:"max_ms"`
	LastSeen    int64   `json:"last_seen"`   // unix millis
	Errors      int64   `json:"errors"`
}

func NewQueryTracker(maxSize int) *QueryTracker {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &QueryTracker{
		queries: make(map[string]*QueryStats),
		maxSize: maxSize,
	}
}

// Record tracks a query execution.
func (qt *QueryTracker) Record(endpoint, logqlQuery string, duration time.Duration, isError bool) {
	fp := fingerprint(logqlQuery)
	qt.TotalQueries.Add(1)

	qt.mu.Lock()
	defer qt.mu.Unlock()

	qs, ok := qt.queries[fp]
	if !ok {
		if len(qt.queries) >= qt.maxSize {
			qt.evictOldest()
		}
		qs = &QueryStats{
			Fingerprint: fp,
			Query:       logqlQuery,
			Endpoint:    endpoint,
		}
		qt.queries[fp] = qs
	}

	ms := float64(duration.Microseconds()) / 1000.0
	qs.Count++
	qs.TotalMs += ms
	if ms > qs.MaxMs {
		qs.MaxMs = ms
	}
	qs.LastSeen = time.Now().UnixMilli()
	if isError {
		qs.Errors++
	}
}

// TopByFrequency returns top-N queries by call count.
func (qt *QueryTracker) TopByFrequency(n int) []QueryStats {
	qt.mu.RLock()
	defer qt.mu.RUnlock()
	return qt.topN(n, func(a, b *QueryStats) bool { return a.Count > b.Count })
}

// TopByLatency returns top-N queries by max latency.
func (qt *QueryTracker) TopByLatency(n int) []QueryStats {
	qt.mu.RLock()
	defer qt.mu.RUnlock()
	return qt.topN(n, func(a, b *QueryStats) bool { return a.MaxMs > b.MaxMs })
}

// TopByErrors returns top-N queries by error count.
func (qt *QueryTracker) TopByErrors(n int) []QueryStats {
	qt.mu.RLock()
	defer qt.mu.RUnlock()
	return qt.topN(n, func(a, b *QueryStats) bool { return a.Errors > b.Errors })
}

// TopQueries returns all top queries sorted by frequency (for cache warming).
func (qt *QueryTracker) TopQueries(n int) []string {
	top := qt.TopByFrequency(n)
	result := make([]string, len(top))
	for i, q := range top {
		result[i] = q.Query
	}
	return result
}

func (qt *QueryTracker) topN(n int, less func(a, b *QueryStats) bool) []QueryStats {
	all := make([]*QueryStats, 0, len(qt.queries))
	for _, qs := range qt.queries {
		all = append(all, qs)
	}
	sort.Slice(all, func(i, j int) bool { return less(all[i], all[j]) })
	if n > len(all) {
		n = len(all)
	}
	result := make([]QueryStats, n)
	for i := 0; i < n; i++ {
		result[i] = *all[i]
	}
	return result
}

func (qt *QueryTracker) evictOldest() {
	var oldest string
	var oldestTime int64 = 1<<63 - 1
	for fp, qs := range qt.queries {
		if qs.LastSeen < oldestTime {
			oldestTime = qs.LastSeen
			oldest = fp
		}
	}
	delete(qt.queries, oldest)
}

// Handler serves query analytics at /debug/queries.
func (qt *QueryTracker) Handler(w http.ResponseWriter, r *http.Request) {
	n := 20
	result := map[string]interface{}{
		"total_queries":  qt.TotalQueries.Load(),
		"unique_queries": qt.size(),
		"top_by_frequency": qt.TopByFrequency(n),
		"top_by_latency":   qt.TopByLatency(n),
		"top_by_errors":    qt.TopByErrors(n),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (qt *QueryTracker) size() int {
	qt.mu.RLock()
	defer qt.mu.RUnlock()
	return len(qt.queries)
}

func fingerprint(query string) string {
	h := sha256.Sum256([]byte(query))
	return fmt.Sprintf("%x", h[:8])
}
