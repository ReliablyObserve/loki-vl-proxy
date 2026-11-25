package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: semantics/two-parsers-keep-one
func TestMixedCertMemoExpiresAndStaysBounded(t *testing.T) {
	var m mixedCertMemo
	now := time.Unix(1700000000, 0)
	if _, ok := m.get("a", now); ok {
		t.Fatal("an empty memo answered")
	}
	m.put("a", mixedCertEntry{names: []string{"x"}}, now)
	if e, ok := m.get("a", now.Add(mixedCertMemoTTL-time.Nanosecond)); !ok || len(e.names) != 1 {
		t.Fatalf("entry lost before its TTL: %v %v", e, ok)
	}
	if _, ok := m.get("a", now.Add(mixedCertMemoTTL)); ok {
		t.Fatal("entry served at its TTL")
	}
	m.put("b", mixedCertEntry{}, now)
	m.drop("b")
	if _, ok := m.get("b", now); ok {
		t.Fatal("dropped entry served")
	}
	for i := 0; i < 3*mixedCertMemoMax; i++ {
		m.put(fmt.Sprintf("k%d", i), mixedCertEntry{}, now)
	}
	m.mu.Lock()
	n := len(m.m)
	m.mu.Unlock()
	if n > mixedCertMemoMax {
		t.Fatalf("memo holds %d entries, bound is %d", n, mixedCertMemoMax)
	}
	for window, want := range map[time.Duration]string{time.Minute: "1h", time.Hour: "1h", time.Hour + 1: "6h", 6 * time.Hour: "6h", 24 * time.Hour: "24h", 25 * time.Hour: "long"} {
		if got := mixedCertWindowClass(window); got != want {
			t.Fatalf("window %s class %q, want %q", window, got, want)
		}
	}
}

// A certificate is reused by the next request, which then skips the listing and the measurement before its scan; the
// listing after the scan still runs, and a name outside the remembered ones sends the request to the class and
// drops the entry. A listing that did not certify is remembered too.
// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedCertificateMemoLifecycle(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	var mu sync.Mutex
	listing := []string{"_msg", "_stream", "_time", "meta"}
	var listings int
	var statsQueries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			mu.Lock()
			listings++
			names := append([]string(nil), listing...)
			mu.Unlock()
			var values []string
			for _, n := range names {
				values = append(values, fmt.Sprintf(`{"value":%q,"hits":1}`, n))
			}
			fmt.Fprintf(w, `{"values":[%s]}`, strings.Join(values, ","))
		case statsQueryRangePath:
			_ = r.ParseForm()
			mu.Lock()
			statsQueries = append(statsQueries, r.FormValue("query"))
			mu.Unlock()
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":"blue"},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Unix())
		default:
			t.Errorf("unexpected backend read %s", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	plan, ok := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`)
	if !ok {
		t.Fatal("typed mixed plan missing")
	}
	start, end, step := at.Add(-time.Minute), at.Add(time.Minute), 30*time.Second
	packs := func(q string) bool { return strings.Contains(q, "| pack_json as "+mixedClassOriginalField) }
	// request runs one collection and returns the listings and stats calls it made and whether its (last) stats call packs.
	request := func() (listed, stats int, lastPacks bool) {
		t.Helper()
		mu.Lock()
		l0, s0 := listings, len(statsQueries)
		mu.Unlock()
		series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, start, end, step)
		if err != nil || !served || len(series) == 0 {
			t.Fatalf("served=%v series=%d error=%v", served, len(series), err)
		}
		mu.Lock()
		defer mu.Unlock()
		return listings - l0, len(statsQueries) - s0, packs(statsQueries[len(statsQueries)-1])
	}
	// 1. miss: listing before and after the scan, certified native count.
	if l, s, pk := request(); l != 2 || s != 1 || pk {
		t.Fatalf("miss: listings=%d stats=%d packs=%v, want 2/1/false", l, s, pk)
	}
	// 2. hit: only the listing after the scan.
	if l, s, pk := request(); l != 1 || s != 1 || pk {
		t.Fatalf("hit: listings=%d stats=%d packs=%v, want 1/1/false", l, s, pk)
	}
	// 3. a name that is not remembered appears: the certified scan runs, the listing after it catches the name, and
	// the class answers; the entry is dropped.
	mu.Lock()
	listing = append(listing, "a__b")
	mu.Unlock()
	if l, s, pk := request(); l != 1 || s != 2 || !pk {
		t.Fatalf("name appeared: listings=%d stats=%d packs=%v, want 1/2/true (certified scan, then the class)", l, s, pk)
	}
	// 4. the entry is gone: the listing is read again, the unclean name does not certify, the class runs and the
	// outcome is remembered.
	if l, s, pk := request(); l != 1 || s != 1 || !pk {
		t.Fatalf("after the drop: listings=%d stats=%d packs=%v, want 1/1/true", l, s, pk)
	}
	// 5. the negative entry: no listing at all, straight to the class.
	if l, s, pk := request(); l != 0 || s != 1 || !pk {
		t.Fatalf("negative hit: listings=%d stats=%d packs=%v, want 0/1/true", l, s, pk)
	}
	// 6. another tenant does not share the entry.
	other := context.WithValue(context.Background(), orgIDKey, "other")
	mu.Lock()
	l0 := listings
	mu.Unlock()
	if _, served, err := p.collectOrderedMixedJSONStats(other, plan, start, end, step); err != nil || !served {
		t.Fatalf("other tenant: served=%v error=%v", served, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if listings == l0 {
		t.Fatal("another tenant reused this tenant's remembered certificate")
	}
}
