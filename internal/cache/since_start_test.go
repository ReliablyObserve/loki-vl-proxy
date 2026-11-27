package cache

import (
	"testing"
	"testing/synctest"
	"time"
)

// Each window is live until it has passed and then keeps its counts, whatever happens afterwards: 1, 5 and 15
// minutes, beside the 10-minute memory figure.
//
// conformance: cache-restart-handover
func TestSinceStartWindowsFreezeAfterEachWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New(time.Hour, 100)
		defer c.Close()
		c.TrackStartupWindow()
		c.TrackStartupWindow() // starts once
		c.SetWithTTL("query:t:a", []byte("a"), time.Hour)
		hit := func() { c.GetSharedWithTTL("query:t:a") }
		miss := func() { c.GetSharedWithTTL("query:t:missing") }
		expect := func(when string, one, five, fifteen TierLookups) {
			t.Helper()
			s := c.HandoverStats()
			for w, want := range []TierLookups{one, five, fifteen} {
				if got := s.SinceStart[w][0]; got != want {
					t.Fatalf("%s: %s window = %+v, want %+v", when, StartupRatioWindows[w], got, want)
				}
				if s.SinceStart[w][1] != (TierLookups{}) || s.SinceStart[w][2] != (TierLookups{}) {
					t.Fatalf("%s: tiers that are not attached were counted: %+v", when, s.SinceStart[w])
				}
			}
		}
		miss()
		expect("running", TierLookups{1, 0}, TierLookups{1, 0}, TierLookups{1, 0})
		time.Sleep(time.Minute - time.Second)
		hit()
		expect("just before 1m", TierLookups{2, 1}, TierLookups{2, 1}, TierLookups{2, 1})
		time.Sleep(2 * time.Second)
		synctest.Wait()
		hit()
		expect("after 1m", TierLookups{2, 1}, TierLookups{3, 2}, TierLookups{3, 2})
		time.Sleep(4 * time.Minute)
		synctest.Wait()
		hit()
		expect("after 5m", TierLookups{2, 1}, TierLookups{3, 2}, TierLookups{4, 3})
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		hit()
		miss()
		expect("after 15m", TierLookups{2, 1}, TierLookups{3, 2}, TierLookups{4, 3})
		// The 10-minute memory figure froze on the way, with the lookups made up to then.
		if s := c.HandoverStats(); s.StartupL1Requests != 4 || s.StartupL1Hits != 3 {
			t.Fatalf("10m figure %d/%d", s.StartupL1Hits, s.StartupL1Requests)
		}
	})
}

// Disk and peer lookups are counted in their tier; a closed cache keeps no timer running.
//
// conformance: cache-restart-handover
func TestSinceStartCountsTheDiskTierAndStopsOnClose(t *testing.T) {
	c := New(time.Hour, 100)
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/l2.db", FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c.SetL2(dc)
	c.TrackStartupWindow()
	dc.Set("query:t:disk", []byte("v"), time.Hour)
	c.GetWithTier("query:t:disk")
	c.GetWithTier("query:t:missing")
	s := c.HandoverStats()
	for w := range StartupRatioWindows {
		if s.SinceStart[w][0] != (TierLookups{2, 0}) || s.SinceStart[w][1] != (TierLookups{2, 1}) {
			t.Fatalf("window %d: %+v", w, s.SinceStart[w])
		}
	}
	c.Close()
	for i, timer := range c.startupTimers {
		if timer.Stop() {
			t.Fatalf("timer %d still pending after Close", i)
		}
	}
	if len(c.startupTimers) != 1+len(StartupRatioWindows) {
		t.Fatalf("timers = %d", len(c.startupTimers))
	}
}
