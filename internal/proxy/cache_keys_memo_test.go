package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The read-cache key memo is shared with Handler.State. Resetting it at its
// cap must empty that shared map, not swap in a new one: a new map left
// Handler.State holding the full old memo for the process lifetime, so a
// proxy past its first reset kept two full memos alive.
func TestReadCacheKeyMemoResetKeepsTheMapSharedWithState(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	keyFor := func(i int) string {
		req := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/labels?start=1&end=2&query={app=%q}", fmt.Sprintf("svc%d", i)), nil)
		return p.canonicalReadCacheKey("labels", "", req)
	}
	for i := 0; i < maxReadCacheKeyMemoEntries; i++ {
		keyFor(i)
	}
	if got := len(p.readCacheKeyMemo); got != maxReadCacheKeyMemoEntries {
		t.Fatalf("memo holds %d entries before the reset, want %d", got, maxReadCacheKeyMemoEntries)
	}
	last := keyFor(maxReadCacheKeyMemoEntries)

	p.readCacheKeyMemoMu.RLock()
	defer p.readCacheKeyMemoMu.RUnlock()
	if got := len(p.readCacheKeyMemo); got != 1 {
		t.Fatalf("memo holds %d entries after the reset, want 1", got)
	}
	state := p.handler.State.readCacheKeyMemo
	if got := len(state); got != 1 {
		t.Fatalf("Handler.State's memo holds %d entries after the reset, want the shared map's 1", got)
	}
	for _, v := range state {
		if v != last {
			t.Fatalf("Handler.State's memo holds %q, want the newest key %q", v, last)
		}
	}
}

// BenchmarkCanonicalReadCacheKey_MemoMissesWithResets keys one more distinct URL than the memo holds, round
// robin, so every call misses and the memo resets once per maxReadCacheKeyMemoEntries calls.
func BenchmarkCanonicalReadCacheKey_MemoMissesWithResets(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	reqs := make([]*http.Request, maxReadCacheKeyMemoEntries+1)
	for i := range reqs {
		reqs[i] = httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/labels?start=1&end=2&query={app=%q}", fmt.Sprintf("svc%d", i)), nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.canonicalReadCacheKey("labels", "", reqs[i%len(reqs)])
	}
}
