package proxy

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// recentErrorsOf is the conversion-error memory the Proxy reads and writes.
func recentErrorsOf(p *Proxy) *unwrapRecent { return &p.unwrapRecentErrors }

// A conversion error one Proxy met must not start another Proxy's early first-window lookup: the memory is the
// Proxy's own, not a package global shared by every instance (and every test) in the process.
// conformance: semantics/unwrap-conversion-error
func TestUnwrapRecentErrorsAreProxyScoped(t *testing.T) {
	a, b := &Proxy{}, &Proxy{}
	key := "tenant\x00sum_over_time({app=\"x\"} | unwrap n [1m])\x00n\x00"
	recentErrorsOf(a).mark(key)
	if !recentErrorsOf(a).seen(key) {
		t.Fatal("the proxy that met the error must remember it")
	}
	if recentErrorsOf(b).seen(key) {
		t.Fatal("another proxy remembers an error it never met")
	}
}

// The memory stays bounded and expires under concurrent use (run with -race).
func TestUnwrapRecentErrorsBoundedAndConcurrent(t *testing.T) {
	p := &Proxy{}
	r := recentErrorsOf(p)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2*unwrapRecentMax; i++ {
				k := fmt.Sprintf("k-%d-%d", g, i)
				r.mark(k)
				r.seen(k)
			}
		}(g)
	}
	wg.Wait()
	r.mu.Lock()
	n := len(r.at)
	r.at["old"] = time.Now().Add(-2 * unwrapRecentTTL)
	r.mu.Unlock()
	if n > unwrapRecentMax {
		t.Fatalf("memory grew past its bound: %d > %d", n, unwrapRecentMax)
	}
	if r.seen("old") {
		t.Fatal("an expired key is still seen")
	}
}
