// Command peerload is the load driver of the peer restore latency proof
// (test/e2e-fleet/peer_restore_load_test.go). It runs in a container on the
// fleet's compose network and sends an open-loop mix of requests at fixed
// rates, so a slower server cannot slow the arrivals down (no coordinated
// omission):
//
//   - hit: cached query_range requests to the serving replica;
//   - peer: /_cache/get of keys the serving replica holds, as other replicas
//     ask it;
//   - miss: query_range requests the serving replica computes from
//     VictoriaLogs (a unique line filter each);
//   - neighbour: cached query_range requests to another ready replica.
//
// Every request is one CSV line: start (Unix ns), class, duration (ns),
// status (0 for a transport error). It stops at -duration or on SIGTERM.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	serving := flag.String("serving", "http://proxy-load-a:3100", "serving replica")
	neighbour := flag.String("neighbour", "http://proxy-load-b:3100", "another ready replica")
	token := flag.String("token", "", "peer token for /_cache/get")
	app := flag.String("app", "", "app label of the ingested lines")
	keys := flag.Int("keys", 0, "fixture keys the serving replica holds (restoreKey 0..keys-1)")
	hitQPS := flag.Float64("hit-qps", 40, "cached query_range per second to the serving replica")
	peerQPS := flag.Float64("peer-qps", 40, "/_cache/get per second to the serving replica")
	missQPS := flag.Float64("miss-qps", 2, "VictoriaLogs-backed query_range per second")
	neighbourQPS := flag.Float64("neighbour-qps", 20, "cached query_range per second to the neighbour")
	duration := flag.Duration("duration", 10*time.Minute, "how long to run")
	out := flag.String("out", "/out/samples.csv", "CSV output")
	flag.Parse()

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var mu sync.Mutex
	record := func(start time.Time, class string, d time.Duration, status int) {
		mu.Lock()
		fmt.Fprintf(w, "%d,%s,%d,%d\n", start.UnixNano(), class, d.Nanoseconds(), status)
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-sig; cancel() }()

	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 512}}
	end := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	rangeQuery := func(base, q string) string {
		v := url.Values{"query": {q}, "start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		return base + "/loki/api/v1/query_range?" + v.Encode()
	}
	hitQueries := make([]string, 20)
	for i := range hitQueries {
		hitQueries[i] = fmt.Sprintf(`sum by (pod) (count_over_time({app=%q} != "hit-%d" [5m]))`, *app, i)
	}
	var seq atomic.Int64
	do := func(class, target string, header http.Header) {
		start := time.Now()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
		for k, v := range header {
			req.Header[k] = v
		}
		status := 0
		if resp, err := client.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			status = resp.StatusCode
		}
		record(start, class, time.Since(start), status)
	}
	peerHeader := http.Header{"X-Peer-Token": {*token}}
	classes := []struct {
		name string
		qps  float64
		next func() (string, http.Header)
	}{
		{"hit", *hitQPS, func() (string, http.Header) {
			return rangeQuery(*serving, hitQueries[seq.Add(1)%int64(len(hitQueries))]), nil
		}},
		{"peer", *peerQPS, func() (string, http.Header) {
			k := int(seq.Add(7919) % int64(max(*keys, 1)))
			return *serving + "/_cache/get?key=" + url.QueryEscape(restoreKey(k)), peerHeader
		}},
		{"miss", *missQPS, func() (string, http.Header) {
			return rangeQuery(*serving, fmt.Sprintf(`sum by (pod) (count_over_time({app=%q} != "miss-%d-%d" [5m]))`, *app, time.Now().UnixNano(), seq.Add(1))), nil
		}},
		{"neighbour", *neighbourQPS, func() (string, http.Header) {
			return rangeQuery(*neighbour, hitQueries[seq.Add(1)%int64(len(hitQueries))]), nil
		}},
	}
	var wg sync.WaitGroup
	inflight := make(chan struct{}, 1024)
	for _, c := range classes {
		if c.qps <= 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(time.Duration(float64(time.Second) / c.qps))
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				target, header := c.next()
				select {
				case inflight <- struct{}{}:
				default:
					record(time.Now(), c.name, 0, -1) // the driver itself was saturated
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-inflight }()
					do(c.name, target, header)
				}()
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	mu.Lock()
	_ = w.Flush()
	mu.Unlock()
	_ = f.Close()
}

// restoreKey is the key of fixture record i (internal/cache's restore
// benchmark fixture).
func restoreKey(i int) string {
	return fmt.Sprintf("query_range:v7:tenant-%02d:%s:%08d", i%16,
		strings.Repeat(`sum by (level) (count_over_time({app="checkout"} | json [5m]))|`, 3), i)
}
