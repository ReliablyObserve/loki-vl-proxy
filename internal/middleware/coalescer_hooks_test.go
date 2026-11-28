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

// The shared call's context carries its work: what is measured with it is the call's. OnShare hears of every
// caller that got the call's answer without running it, with that caller's context and the call's work: the
// leader is never told. A call that runs inside another shared call adds its work to that call's too.
//
// conformance: observability-vl-savings
func TestCoalescerSharesTheCallsWorkWithEachWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		var shares atomic.Int64
		c.OnShare = func(waiter context.Context, work *CallWork) {
			if waiter.Value(hookKey{}) != "waiter" || work.Calls.Load() != 1 || work.Bytes.Load() != 5 {
				t.Errorf("waiter %v, work calls %d bytes %d", waiter.Value(hookKey{}), work.Calls.Load(), work.Bytes.Load())
			}
			shares.Add(1)
		}
		release := make(chan struct{})
		started := make(chan struct{}, 1)
		fn := func(ctx context.Context) (*http.Response, error) {
			if ctx.Value(hookKey{}) != "leader" {
				t.Error("the call lost the values of the context that started it")
			}
			CallWorkFrom(ctx).Add(1, 10, 5, 0)
			started <- struct{}{}
			<-release
			return answer("x")
		}
		done := make(chan struct{}, 4)
		go func() {
			_, _, _, _ = c.DoWithGuardCtx(context.WithValue(context.Background(), hookKey{}, "leader"), "k", allow, fn)
			done <- struct{}{}
		}()
		<-started
		for i := 0; i < 3; i++ {
			go func() {
				_, _, _, _ = c.DoWithGuardCtx(context.WithValue(context.Background(), hookKey{}, "waiter"), "k", allow, fn)
				done <- struct{}{}
			}()
		}
		synctest.Wait()
		close(release)
		for i := 0; i < 4; i++ {
			<-done
		}
		if shares.Load() != 3 || c.CoalescedSaved.Load() != 3 {
			t.Fatalf("shares %d, saved %d, want 3", shares.Load(), c.CoalescedSaved.Load())
		}
	})
	outer := NewCoalescer()
	inner := NewCoalescer()
	var outerWork *CallWork
	_, _, _, err := outer.DoWithGuardCtx(context.Background(), "outer", allow, func(ctx context.Context) (*http.Response, error) {
		outerWork = CallWorkFrom(ctx)
		_, _, _, err := inner.DoWithGuardCtx(ctx, "inner", allow, func(ctx context.Context) (*http.Response, error) {
			CallWorkFrom(ctx).Add(2, 0, 7, 3)
			return answer("y")
		})
		if err != nil {
			return nil, err
		}
		return answer("x")
	})
	if err != nil || outerWork == nil || outerWork.Calls.Load() != 2 || outerWork.Rows.Load() != 3 {
		t.Fatalf("outer work %+v err %v", outerWork, err)
	}
	if CallWorkFrom(context.Background()) != nil {
		t.Fatal("work outside a call")
	}
	var none *CallWork
	none.Add(1, 1, 1, 1) // outside a call: nothing to record
}

// A follower that starts the call again after the call it waited on ended with the leader's deadline avoided no
// backend call: it is not counted as saved, and OnShare is not told.
//
// conformance: observability-vl-savings, semantics/error-coalescing-no-inherited-cancel
func TestCoalescerInheritedRetryIsNotASaving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		var shares atomic.Int64
		c.OnShare = func(context.Context, *CallWork) { shares.Add(1) }
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
