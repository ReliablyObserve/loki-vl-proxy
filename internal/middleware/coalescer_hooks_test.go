package middleware

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type hookKey struct{}

// The shared call runs on the context OnCall gives, and OnShare hears of every caller that got the call's answer
// without running it, with that caller's context and the call's: the leader is never told, nor a caller whose
// shared call failed.
//
// conformance: observability-vl-savings
func TestCoalescerHooksSeeTheCallAndEachWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		c.OnCall = func(ctx context.Context) context.Context { return context.WithValue(ctx, hookKey{}, "call") }
		var shares atomic.Int64
		c.OnShare = func(waiter, call context.Context) {
			if call.Value(hookKey{}) != "call" || waiter.Value(hookKey{}) != "waiter" {
				t.Errorf("waiter %v, call %v", waiter.Value(hookKey{}), call.Value(hookKey{}))
			}
			shares.Add(1)
		}
		release := make(chan struct{})
		started := make(chan struct{}, 1)
		fn := func(ctx context.Context) (*http.Response, error) {
			if ctx.Value(hookKey{}) != "call" {
				t.Error("the call does not run on OnCall's context")
			}
			started <- struct{}{}
			<-release
			return answer("x")
		}
		go func() { _, _, _, _ = c.DoWithGuardCtx(context.WithValue(context.Background(), hookKey{}, "leader"), "k", allow, fn) }()
		<-started
		for i := 0; i < 3; i++ {
			go func() { _, _, _, _ = c.DoWithGuardCtx(context.WithValue(context.Background(), hookKey{}, "waiter"), "k", allow, fn) }()
		}
		synctest.Wait()
		close(release)
		synctest.Wait()
		if shares.Load() != 3 || c.CoalescedSaved.Load() != 3 {
			t.Fatalf("shares %d, saved %d, want 3", shares.Load(), c.CoalescedSaved.Load())
		}
	})
}

// A follower that starts the call again after the call it waited on ended with the leader's deadline avoided no
// backend call: it is not counted as saved, and OnShare is not told.
//
// conformance: observability-vl-savings, semantics/error-coalescing-no-inherited-cancel
func TestCoalescerInheritedRetryIsNotASaving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		var shares atomic.Int64
		c.OnShare = func(context.Context, context.Context) { shares.Add(1) }
		started := make(chan struct{}, 2)
		var calls atomic.Int64
		fn := func(ctx context.Context) (*http.Response, error) {
			n := calls.Add(1)
			started <- struct{}{}
			if n == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return answer("fresh")
		}
		leaderCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan struct{}, 2)
		go func() { _, _, _, _ = c.DoWithGuardCtx(leaderCtx, "key", allow, fn); done <- struct{}{} }()
		<-started
		go func() { _, _, _, _ = c.DoWithGuardCtx(context.Background(), "key", allow, fn); done <- struct{}{} }()
		<-done
		<-done
		if calls.Load() != 2 || c.CoalescedSaved.Load() != 0 || c.WaitCount.Load() != 0 || shares.Load() != 0 {
			t.Fatalf("calls %d, saved %d, waits %d, shares %d; want 2, 0, 0, 0", calls.Load(), c.CoalescedSaved.Load(), c.WaitCount.Load(), shares.Load())
		}
	})
}
