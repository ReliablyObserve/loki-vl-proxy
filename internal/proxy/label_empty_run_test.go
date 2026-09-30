package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// backfillVL is a fake VictoriaLogs holding rows only in the last 20 minutes.
// Once backfilled is set it also holds one row at ts (hours ago), as after a
// shipper outage is repaired. It answers row counts and name listings by range
// and counts both kinds of call.
type backfillVL struct {
	backfilled   atomic.Bool
	ts           int64
	counts, scan atomic.Int64
}

func (f *backfillVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		a, _ := parseLokiTimeToUnixNano(r.URL.Query().Get("start"))
		b, ok := parseLokiTimeToUnixNano(r.URL.Query().Get("end"))
		if !ok {
			b = time.Now().UnixNano()
		}
		live := b > time.Now().Add(-20*time.Minute).UnixNano()
		late := f.backfilled.Load() && a <= f.ts && f.ts < b
		if strings.Contains(r.URL.Query().Get("query"), "count()") {
			f.counts.Add(1)
			n := 0
			if live {
				n += 100
			}
			if late {
				n++
			}
			fmt.Fprintf(w, `{"rows":"%d"}`+"\n", n)
			return
		}
		f.scan.Add(1)
		var hits []fieldHit
		if live {
			hits = append(hits, fieldHit{"app", 100})
		}
		if late {
			hits = append(hits, fieldHit{"backfill_label", 1})
		}
		writeVLFieldNames(w, hits)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Empty hours cached during a shipper outage, then rows backfilled into them
// with old timestamps: the next near-now request after the negative TTL lists
// them, for one count per run of empty buckets and a rescan of the bucket that
// received the rows only.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_BackfilledRowsInCachedEmptyHoursAppearWithinTheNegativeTTL(t *testing.T) {
	vl := &backfillVL{ts: time.Now().Add(-10 * time.Hour).UnixNano()}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := func() string {
		now := time.Now().UnixNano()
		return fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", now-int64(7*24*time.Hour), now)
	}
	if got := getMetadataList(t, mux, path()); contains(got, "backfill_label") {
		t.Fatalf("before the backfill: %v", got)
	}
	coldScans, coldCounts := vl.scan.Load(), vl.counts.Load()

	// Unchanged data, past the negative TTL: one count per run, no bucket scans
	// beyond the live edges.
	restore := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + 5*time.Second)
	defer restore()
	time.Sleep(200 * time.Millisecond)
	_ = getMetadataList(t, mux, path())
	idleScans, idleCounts := vl.scan.Load()-coldScans, vl.counts.Load()-coldCounts
	t.Logf("cold: %d scans, %d counts; unchanged refresh: %d scans, %d counts", coldScans, coldCounts, idleScans, idleCounts)
	if idleCounts > 2 {
		t.Fatalf("unchanged refresh made %d counts, want about one per run of empty buckets", idleCounts)
	}

	// Backfill a row ten hours ago, then refresh after the negative TTL.
	vl.backfilled.Store(true)
	restore2 := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + 5*time.Second)
	defer restore2()
	time.Sleep(200 * time.Millisecond)
	scansBefore, countsBefore := vl.scan.Load(), vl.counts.Load()
	if got := getMetadataList(t, mux, path()); !contains(got, "backfill_label") {
		t.Fatalf("rows backfilled into cached empty hours were not listed: %v", got)
	}
	scans, counts := vl.scan.Load()-scansBefore, vl.counts.Load()-countsBefore
	t.Logf("backfill refresh: %d scans, %d counts", scans, counts)
	if scans > idleScans+1 {
		t.Fatalf("backfill refresh made %d scans (idle refresh %d): more than the one bucket that received rows", scans, idleScans)
	}
	if counts > 12 {
		t.Fatalf("backfill refresh made %d counts", counts)
	}
}
