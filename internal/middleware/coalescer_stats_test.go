package middleware

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// A plain Do leader and a guarded follower share the same singleflight group.
// The follower's earlier inflight-map count is zero; it still saves a call.
func TestCoalescerStatsCountActualFollowers(t *testing.T) {
	for _, failing := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "error"}[failing], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := NewCoalescer()
				started, release := make(chan struct{}), make(chan struct{})
				var calls atomic.Int64
				upstreamErr := errors.New("controlled failure")
				fn := func() (*http.Response, error) {
					calls.Add(1)
					close(started)
					<-release
					if failing {
						return nil, upstreamErr
					}
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("answer"))}, nil
				}
				go func() { _, _, _, _ = c.Do("key", fn) }()
				<-started
				var got []byte
				var gotErr error
				go func() { _, _, got, gotErr = c.DoWithGuard("key", func() bool { return true }, fn) }()
				synctest.Wait()                  // leader and follower are durably blocked
				time.Sleep(7 * time.Millisecond) // advance the bubble's virtual clock
				close(release)
				synctest.Wait()
				if calls.Load() != 1 || c.CoalescedSaved.Load() != 1 || c.WaitCount.Load() != 1 {
					t.Fatalf("calls=%d saved=%d waits=%d, want 1 each", calls.Load(), c.CoalescedSaved.Load(), c.WaitCount.Load())
				}
				if got := c.WaitNanos.Load(); got != int64(7*time.Millisecond) {
					t.Fatalf("wait nanoseconds=%d want %d", got, int64(7*time.Millisecond))
				}
				if failing && !errors.Is(gotErr, upstreamErr) || !failing && (gotErr != nil || string(got) != "answer") {
					t.Fatalf("follower body=%q err=%v", got, gotErr)
				}
			})
		})
	}
}

func TestCoalescerStatsNoInventedSavings(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, allowed := range []bool{false, true} {
			c := NewCoalescer()
			if disabled {
				c = NewCoalescerDisabled()
			}
			calls := 0
			_, _, _, err := c.DoWithGuard("key", func() bool { return allowed }, func() (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("answer"))}, nil
			})
			if allowed && (calls != 1 || err != nil) || !allowed && (calls != 0 || !errors.Is(err, ErrGuardRejected)) {
				t.Fatalf("disabled=%t allowed=%t calls=%d err=%v", disabled, allowed, calls, err)
			}
			if c.CoalescedSaved.Load() != 0 || c.WaitCount.Load() != 0 || c.WaitNanos.Load() != 0 {
				t.Fatal("single, bypassed or rejected request invented a saved call")
			}
		}
	}
}
