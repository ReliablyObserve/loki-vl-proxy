package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// A lookup waits for the stores of its extent key in progress, and only those:
// until every store of the key ends, a bound passes, or its request ends.
// conformance: quality/query-range-extent-cache
func TestStatsExtentPendingStores(t *testing.T) {
	var s statsExtentPendingStores
	returns := func(key string, ctx context.Context) <-chan struct{} {
		out := make(chan struct{})
		go func() {
			s.wait(ctx, key)
			close(out)
		}()
		return out
	}
	quick := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}
	if !quick(returns("none", context.Background())) {
		t.Fatal("a key with no store in progress waited")
	}
	first, second := s.begin("k"), s.begin("k")
	other := s.begin("other")
	waiting := returns("k", context.Background())
	if quick(waiting) {
		t.Fatal("returned while two stores of the key run")
	}
	first()
	first() // ending twice ends one store
	if quick(waiting) {
		t.Fatal("returned while a store of the key still runs")
	}
	second()
	if !quick(waiting) {
		t.Fatal("still waiting after every store of the key ended")
	}
	if !quick(returns("k", context.Background())) {
		t.Fatal("an ended key still holds lookups")
	}
	// Another key's store holds no lookup of this one, and a request that ends stops waiting.
	ctx, cancel := context.WithCancel(context.Background())
	held := returns("other", ctx)
	if quick(held) {
		t.Fatal("returned while the store of its key runs")
	}
	cancel()
	if !quick(held) {
		t.Fatal("a cancelled request kept waiting")
	}
	other()
	if len(s.m) != 0 {
		t.Fatalf("ended stores left entries: %v", s.m)
	}
}

// The gate's cost on every extent cache lookup with no store of its key in
// progress, the common case.
func BenchmarkStatsExtentPendingWaitNone(b *testing.B) {
	var s statsExtentPendingStores
	end := s.begin("another key")
	defer end()
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.wait(ctx, "stats_extent:v1:key")
	}
}

// A lookup of buckets that a read just fetched waits for that read's store,
// even while the read's caller is still parsing the answer and holds its
// stats slot: the second lookup is answered from the stored buckets, so
// VictoriaLogs is read once.
// conformance: quality/query-range-extent-cache
func TestStatsExtentLookupWaitsForTheReadBeingParsed(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	emu := (&vlemu.Emulator{T: t, Rows: queryStepsRows(s0, 4*time.Hour, nil), Field: "amount", Version: "v1.50.0"}).Handler()
	var mu sync.Mutex
	var forms []url.Values
	shimSrv, shim := newExtentShim(t, emu)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == statsQueryRangePath {
			_ = r.ParseForm()
			mu.Lock()
			forms = append(forms, r.PostForm)
			mu.Unlock()
			r.Body = io.NopCloser(strings.NewReader(r.PostForm.Encode()))
		}
		shim.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	_ = shimSrv
	// The form the window evaluator sends through the extent cache, from another proxy.
	end := s0.Add(3 * time.Hour)
	// A max of max_over_time folds the rows of its groups: no series hash, whose folded reads the cache does not keep.
	queryStepsGet(t, newExtentTestProxy(t, srv.URL, true, 256<<20), `max by (app) (max_over_time({env="production"} | logfmt | unwrap amount [5m]))`, extentWindow{end.Add(-90 * time.Minute), end, 10 * time.Second})
	mu.Lock()
	if len(forms) == 0 {
		mu.Unlock()
		t.Fatal("no stats_query_range read")
	}
	form := forms[0]
	forms = nil
	mu.Unlock()

	p := newExtentTestProxy(t, srv.URL, true, 256<<20)
	ctx := context.WithValue(context.Background(), statsExtentRequestKey{}, true)
	first, ok, err := p.statsExtentPost(ctx, statsQueryRangePath, form)
	if !ok || err != nil || first.StatusCode != http.StatusOK {
		t.Fatalf("first lookup: ok=%v err=%v", ok, err)
	}
	type answer struct {
		body []byte
		err  error
	}
	second := make(chan answer, 1)
	go func() {
		resp, _, err := p.statsExtentPost(ctx, statsQueryRangePath, form)
		if err != nil {
			second <- answer{err: err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		second <- answer{body, err}
	}()
	time.Sleep(100 * time.Millisecond) // the first caller is still parsing
	want, err := io.ReadAll(first.Body)
	_ = first.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	got := <-second
	if got.err != nil || !bytes.Equal(got.body, want) {
		t.Fatalf("second lookup: err=%v, %d bytes, want the first answer (%d bytes)", got.err, len(got.body), len(want))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(forms) != 1 {
		t.Logf("outcomes %v", extentOutcomes(t, p))
		t.Fatalf("VictoriaLogs read %d times, want once", len(forms))
	}
}

// A read whose caller does not take the whole answer is not stored, and its
// close ends the wait of the lookups behind it at once.
// conformance: quality/query-range-extent-cache
func TestStatsExtentTeeAbandonedEndsTheRegistration(t *testing.T) {
	var s statsExtentPendingStores
	for name, read := range map[string]func(io.Reader){
		"read in part": func(r io.Reader) { _, _ = io.ReadFull(r, make([]byte, 10)) },
		"over limit":   func(r io.Reader) { _, _ = io.ReadAll(r) },
	} {
		t.Run(name, func(t *testing.T) {
			end := s.begin("k")
			tee := &statsExtentTee{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("x", 100))), limit: 50, abandoned: end, done: func([]byte) { t.Fatal("handed on") }}
			read(tee)
			_ = tee.Close()
			_ = tee.Close()
			if len(s.m) != 0 {
				t.Fatal("the registration outlived the abandoned read")
			}
		})
	}
}
