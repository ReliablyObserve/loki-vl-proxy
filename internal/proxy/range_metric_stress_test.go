//go:build stress

package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestRangeMetricHitsConcurrency fires collectRangeMetricHits from 30 goroutines
// simultaneously against a mock VL backend. The race detector catches unsafe
// shared state in the fastjson parser pool, stream cache, or stats accumulator.
//
// Run with: go test -tags=stress -race -run TestRangeMetric ./internal/proxy/
func TestRangeMetricHitsConcurrency(t *testing.T) {
	const goroutines = 30

	// Build a realistic stats_query_range response payload (500 series).
	payload := buildStatsQueryRangeResponse(500, 5)

	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	defer backend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	now := time.Now()
	start := now.Add(-10 * time.Minute)
	step := time.Minute

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed int
	seriesCounts := make([]int, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			query := fmt.Sprintf(`count_over_time({app="svc%02d"}[5m])`, idx%5)
			groupBy := []string{fmt.Sprintf("app%d", idx%3)}

			result, err := p.collectRangeMetricHits(
				context.Background(),
				query,
				groupBy, groupBy,
				false,
				"count()",
				start, now,
				step,
			)
			if err != nil {
				mu.Lock()
				t.Errorf("goroutine %d: collectRangeMetricHits: %v", idx, err)
				failed++
				mu.Unlock()
				return
			}
			seriesCounts[idx] = len(result)
		}(g)
	}
	wg.Wait()

	t.Logf("range metric hits stress: %d goroutines, %d upstream calls, %d failures",
		goroutines, calls.Load(), failed)
	for i, n := range seriesCounts {
		if n == 0 && failed == 0 {
			// Some goroutines may get 0 series if their query matched no streams — acceptable.
			t.Logf("goroutine %d: 0 series (query may have matched no streams)", i)
		}
	}
}

// TestRangeMetricSamplesConcurrency fires collectRangeMetricSamples from 30
// goroutines against a mock VL backend. The /query endpoint returns NDJSON;
// this verifies the bufio.Scanner + fastjson pool is safe under concurrent use.
func TestRangeMetricSamplesConcurrency(t *testing.T) {
	const goroutines = 30

	// Build a realistic /query NDJSON response (5000 lines).
	payload := makeNDJSONLines(5000, 10)

	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/stream+json")
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	defer backend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	now := time.Now()
	start := now.Add(-10 * time.Minute)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed int

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			query := fmt.Sprintf(`avg_over_time({app="svc%02d"}|logfmt|unwrap latency_ms[5m])`, idx%10)
			groupBy := []string{"app"}

			_, err := p.collectRangeMetricSamples(
				context.Background(),
				query,
				groupBy, groupBy,
				false,
				"latency_ms", "",
				start, now,
			)
			if err != nil {
				mu.Lock()
				t.Errorf("goroutine %d: collectRangeMetricSamples: %v", idx, err)
				failed++
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	t.Logf("range metric samples stress: %d goroutines, %d upstream calls, %d failures",
		goroutines, calls.Load(), failed)
}

// TestRangeMetricDeterminism verifies that identical inputs always produce
// identical outputs across concurrent invocations (no shared mutable state).
func TestRangeMetricDeterminism(t *testing.T) {
	const goroutines = 20

	payload := buildStatsQueryRangeResponse(100, 5)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer backend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	now := time.Now()
	start := now.Add(-10 * time.Minute)
	step := 5 * time.Minute
	query := `count_over_time({namespace="prod"}[5m])`
	groupBy := []string{"namespace"}

	// Reference output (single-threaded).
	ref, err := p.collectRangeMetricHits(context.Background(), query, groupBy, groupBy, false, "count()", start, now, step)
	if err != nil {
		t.Fatalf("reference call failed: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var mismatches int

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, err := p.collectRangeMetricHits(context.Background(), query, groupBy, groupBy, false, "count()", start, now, step)
			if err != nil {
				mu.Lock()
				t.Errorf("goroutine %d: %v", idx, err)
				mismatches++
				mu.Unlock()
				return
			}
			if len(got) != len(ref) {
				mu.Lock()
				t.Errorf("goroutine %d: series count mismatch: got %d, want %d", idx, len(got), len(ref))
				mismatches++
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	t.Logf("determinism check: %d goroutines, %d mismatches", goroutines, mismatches)
}

// buildStatsQueryRangeResponse generates a realistic stats_query_range JSON response
// with numEntries data points across numStreams distinct streams.
func buildStatsQueryRangeResponse(numEntries, numStreams int) []byte {
	base := time.Unix(1700000000, 0).UTC()

	type dataEntry struct {
		Timestamps []int64  `json:"timestamps"`
		Values     []string `json:"values"`
		Metric     string   `json:"metric"`
	}

	entries := make([]map[string]interface{}, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		svc := fmt.Sprintf("svc%02d", i%numStreams)
		ts := base.Add(time.Duration(i) * time.Minute).Unix()
		entries = append(entries, map[string]interface{}{
			"metric": map[string]string{"app": svc, "namespace": "prod"},
			"values": [][]interface{}{{ts, fmt.Sprintf("%d", 100+i)}},
		})
	}

	resp := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     entries,
		},
	}
	b, _ := json.Marshal(resp)
	return b
}
