package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// minuteListingVL answers field_values with k distinct values per minute of
// the requested range, as one call over any range would. poisoned, when set,
// makes a call whose range is shorter than an hour and holds the minute at
// poisonAt answer with 5000 values; delay slows every values call.
type minuteListingVL struct {
	k          int
	delay      time.Duration
	poisonAt   int64
	poisoned   atomic.Bool
	calls      atomic.Int64
	poisonHits atomic.Int64
}

func (m *minuteListingVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"pod", 1}})
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
			m.calls.Add(1)
			if m.delay > 0 {
				time.Sleep(m.delay)
			}
			s, _ := parseLokiTimeToUnixNano(r.FormValue("start"))
			e, _ := parseLokiTimeToUnixNano(r.FormValue("end"))
			var vals []fieldHit
			if in := m.poisonAt != 0 && s <= m.poisonAt && m.poisonAt < e && e-s < int64(time.Hour); in {
				m.poisonHits.Add(1)
				if m.poisoned.Load() {
					for i := 0; i < 5000; i++ {
						vals = append(vals, fieldHit{Value: fmt.Sprintf("huge-%06d", i), Hits: 1})
					}
					writeVLFieldValues(w, vals)
					return
				}
			}
			m0 := s / int64(time.Minute)
			m1 := (e + int64(time.Minute) - 1) / int64(time.Minute)
			for minute := m0; minute < m1; minute++ {
				for i := 0; i < m.k; i++ {
					vals = append(vals, fieldHit{Value: "v-" + strconv.FormatInt(minute, 10) + "-" + strconv.Itoa(i), Hits: 1})
				}
			}
			writeVLFieldValues(w, vals)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func hourListingParams() url.Values {
	now := time.Now()
	return url.Values{"query": {"*"}, "field": {"pod"}, "start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}}
}

// The response cap bounds the merged listing, as it bounds the one response
// VictoriaLogs sends when the inventory is off: every bucket under the cap
// with the union over it fails with Loki's ResourceExhausted, and the
// inventory and one-call paths agree on each side of the boundary.
// conformance: operator-configurable-limits, limits/label-values-response-cap, semantics/label-inventory-exact-and-incremental
func TestLabelValuesResponseCap_AppliesToTheMergedInventoryListing(t *testing.T) {
	newVL := func() *minuteListingVL { return &minuteListingVL{k: 10} }
	// The size of the whole listing, from one uncapped call without the inventory.
	ref := newVL()
	refProxy := newGuardTestProxy(t, Config{BackendURL: ref.server(t).URL, MetadataInventoryParallelism: -1})
	params := hourListingParams()
	items, err := refProxy.fetchVLListing(context.Background(), "/select/logsql/field_values", params)
	if err != nil {
		t.Fatal(err)
	}
	size := vlListingEncodedSize(items)

	for _, tc := range []struct {
		name    string
		limit   int64
		wantErr bool
	}{
		{"one byte under the listing", size - 1, true},
		{"exactly the listing", size, false},
		{"above the listing", size + 1, false},
		{"far above the largest bucket, below the union", size / 2, true},
	} {
		for _, inventory := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/inventory=%v", tc.name, inventory), func(t *testing.T) {
				vl := newVL()
				cfg := Config{BackendURL: vl.server(t).URL}
				if !inventory {
					cfg.MetadataInventoryParallelism = -1
				}
				p := newGuardTestProxy(t, cfg)
				ctx := withLabelValuesResponseCap(context.Background(), int(tc.limit))
				got, err := p.fetchVLListing(ctx, "/select/logsql/field_values", params)
				if tc.wantErr {
					if err == nil || !isLabelValuesResponseTooLarge(err) || !resourceExhaustedRE.MatchString(err.Error()) {
						t.Fatalf("err = %v (%d items), want Loki's ResourceExhausted", err, len(got))
					}
					return
				}
				if err != nil || len(got) != len(items) {
					t.Fatalf("err = %v, items = %d, want %d without error", err, len(got), len(items))
				}
			})
		}
	}
}

// The same holds through the HTTP handler: the client gets Loki's 500 with
// the inventory on and with it off.
// conformance: operator-configurable-limits, limits/label-values-response-cap, semantics/label-inventory-exact-and-incremental
func TestLabelValuesResponseCap_HandlerUnionOverCapFailsWithInventory(t *testing.T) {
	for _, inventory := range []bool{true, false} {
		vl := &minuteListingVL{k: 10}
		cfg := Config{BackendURL: vl.server(t).URL, ExecutionLimits: ExecutionLimitsConfig{LabelValuesMaxResponseBytes: 16384}}
		if !inventory {
			cfg.MetadataInventoryParallelism = -1
		}
		p := newGuardTestProxy(t, cfg)
		rec := getLabelValues(p, "0")
		if rec.Code != http.StatusInternalServerError || !resourceExhaustedRE.MatchString(lokiErrorText(t, rec)) {
			t.Fatalf("inventory=%v: status %d body %s, want Loki's ResourceExhausted 500", inventory, rec.Code, bodyHead(rec))
		}
	}
}

// Buckets another request filled without a cap do not lift the cap of a later
// request for the same listing.
// conformance: operator-configurable-limits, limits/label-values-response-cap, semantics/label-inventory-exact-and-incremental
func TestLabelValuesResponseCap_CachedBucketsFromUncappedFillAreCapped(t *testing.T) {
	vl := &minuteListingVL{k: 10}
	p := newGuardTestProxy(t, Config{BackendURL: vl.server(t).URL})
	params := hourListingParams()
	items, err := p.fetchVLListing(context.Background(), "/select/logsql/field_values", params)
	if err != nil || len(items) != 610 {
		t.Fatalf("uncapped fill: %d items, err %v", len(items), err)
	}
	filled := vl.calls.Load()
	got, err := p.fetchVLListing(withLabelValuesResponseCap(context.Background(), 2048), "/select/logsql/field_values", params)
	if err == nil || !resourceExhaustedRE.MatchString(err.Error()) {
		t.Fatalf("capped listing over cached buckets: %d items, err %v, want ResourceExhausted", len(got), err)
	}
	if vl.calls.Load() != filled {
		t.Logf("note: the capped listing read %d more bucket(s)", vl.calls.Load()-filled)
	}
	// An uncapped caller still gets the whole listing.
	if again, err := p.fetchVLListing(context.Background(), "/select/logsql/field_values", params); err != nil || len(again) != 610 {
		t.Fatalf("uncapped listing after a capped failure: %d items, err %v", len(again), err)
	}
}

// One sealed bucket above the cap fails the listing; the failure is neither
// cached nor merged into a partial answer, and the range is read from
// VictoriaLogs again on the next request.
// conformance: operator-configurable-limits, limits/label-values-response-cap, semantics/label-inventory-exact-and-incremental
func TestLabelValuesResponseCap_InventoryBucketOverCapNeverCachedOrMerged(t *testing.T) {
	poisonAt := time.Now().Add(-30 * time.Minute).UnixNano()
	vl := &minuteListingVL{k: 3, poisonAt: poisonAt}
	vl.poisoned.Store(true)
	p := newGuardTestProxy(t, Config{BackendURL: vl.server(t).URL, ExecutionLimits: ExecutionLimitsConfig{LabelValuesMaxResponseBytes: 16384}})
	for attempt := 1; attempt <= 2; attempt++ {
		before := vl.poisonHits.Load()
		rec := getLabelValues(p, "0")
		if rec.Code != http.StatusInternalServerError || !resourceExhaustedRE.MatchString(lokiErrorText(t, rec)) {
			t.Fatalf("attempt %d: status %d body %s, want Loki's ResourceExhausted 500", attempt, rec.Code, bodyHead(rec))
		}
		if vl.poisonHits.Load() == before {
			t.Fatalf("attempt %d: the over-limit range was not read from VictoriaLogs (served from the inventory)", attempt)
		}
	}
	vl.poisoned.Store(false)
	rec := getLabelValues(p, "0")
	if rec.Code != http.StatusOK {
		t.Fatalf("healed: status %d body %s", rec.Code, bodyHead(rec))
	}
	want := "v-" + strconv.FormatInt(poisonAt/int64(time.Minute), 10) + "-0"
	if !strings.Contains(rec.Body.String(), `"`+want+`"`) {
		t.Fatalf("healed answer lacks %s from the bucket that failed: a partial union was served or cached", want)
	}
}

// A capped fill and an uncapped request for the same bucket do not share a
// flight: the uncapped caller never inherits the capped leader's failure.
// conformance: operator-configurable-limits, limits/label-values-response-cap, semantics/label-inventory-exact-and-incremental
func TestInventoryFlight_CappedLeaderDoesNotFailUncappedFollower(t *testing.T) {
	vl := &minuteListingVL{k: 10, delay: 300 * time.Millisecond}
	p := newGuardTestProxy(t, Config{BackendURL: vl.server(t).URL})
	params := hourListingParams()
	var wg sync.WaitGroup
	var leaderErr, followerErr error
	var followerItems int
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, leaderErr = p.fetchVLListing(withLabelValuesResponseCap(context.Background(), 1000), "/select/logsql/field_values", params)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		var items []vlValueHits
		items, followerErr = p.fetchVLListing(context.Background(), "/select/logsql/field_values", params)
		followerItems = len(items)
	}()
	wg.Wait()
	if leaderErr == nil || !resourceExhaustedRE.MatchString(leaderErr.Error()) {
		t.Fatalf("capped leader err = %v, want ResourceExhausted", leaderErr)
	}
	if followerErr != nil || followerItems != 610 {
		t.Fatalf("uncapped follower: %d items, err %v, want the whole listing", followerItems, followerErr)
	}
}
