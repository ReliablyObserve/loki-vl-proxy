package middleware

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func answer(body string) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func allow() bool { return true }

// The shared call runs on its own context: when the caller that started it
// cancels while another caller still waits, the call goes on and the follower
// gets the answer; only the leaver sees its cancellation.
//
// conformance: backend-deadlines-and-cancellation
func TestCoalescerLeaderCancelKeepsCallForFollower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		started, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int64
		var sawCancel atomic.Bool
		fn := func(ctx context.Context) (*http.Response, error) {
			calls.Add(1)
			close(started)
			select {
			case <-release:
				return answer("answer")
			case <-ctx.Done():
				sawCancel.Store(true)
				return nil, ctx.Err()
			}
		}

		leaderCtx, cancelLeader := context.WithCancel(context.Background())
		var leaderErr error
		leaderDone := make(chan struct{})
		go func() {
			defer close(leaderDone)
			_, _, _, leaderErr = c.DoWithGuardCtx(leaderCtx, "key", allow, fn)
		}()
		<-started

		var body []byte
		var followerErr error
		followerDone := make(chan struct{})
		go func() {
			defer close(followerDone)
			_, _, body, followerErr = c.DoWithGuardCtx(context.Background(), "key", allow, fn)
		}()
		synctest.Wait()

		cancelLeader()
		<-leaderDone
		if !errors.Is(leaderErr, context.Canceled) {
			t.Fatalf("leader err = %v, want context canceled", leaderErr)
		}
		synctest.Wait()
		if sawCancel.Load() {
			t.Fatal("the shared call was cancelled while a follower still waited")
		}

		close(release)
		<-followerDone
		if followerErr != nil || string(body) != "answer" {
			t.Fatalf("follower got %q, %v; want the answer", body, followerErr)
		}
		if calls.Load() != 1 {
			t.Fatalf("upstream calls = %d, want 1", calls.Load())
		}
	})
}

// When every waiter has left, the shared call is cancelled, and a caller that
// arrives while the abandoned call winds down starts a call of its own instead
// of inheriting the cancelled one.
//
// conformance: backend-deadlines-and-cancellation
func TestCoalescerCancelsCallWhenLastWaiterLeaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		started := make(chan struct{}, 2)
		windDown := make(chan struct{})
		var calls atomic.Int64
		fn := func(ctx context.Context) (*http.Response, error) {
			if calls.Add(1) == 1 {
				started <- struct{}{}
				<-ctx.Done()
				<-windDown // the abandoned call is still registered while it stops
				return nil, ctx.Err()
			}
			started <- struct{}{}
			return answer("fresh")
		}

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		errs := make(chan error, 2)
		for _, ctx := range []context.Context{ctx1, ctx2} {
			go func() {
				_, _, _, err := c.DoWithGuardCtx(ctx, "key", allow, fn)
				errs <- err
			}()
		}
		<-started
		synctest.Wait()

		cancel1()
		synctest.Wait()
		cancel2()
		for range 2 {
			if err := <-errs; !errors.Is(err, context.Canceled) {
				t.Fatalf("waiter err = %v, want context canceled", err)
			}
		}

		_, _, body, err := c.DoWithGuardCtx(context.Background(), "key", allow, fn)
		if err != nil || string(body) != "fresh" {
			t.Fatalf("later caller got %q, %v; want its own fresh call", body, err)
		}
		close(windDown)
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatalf("upstream calls = %d, want 2", calls.Load())
		}
	})
}

// A shared call keeps the values of the context that started it, so the
// tenant and telemetry it carries still reach the upstream request.
//
// conformance: backend-deadlines-and-cancellation
func TestCoalescerSharedCallKeepsLeaderValues(t *testing.T) {
	type ctxKey struct{}
	c := NewCoalescer()
	ctx := context.WithValue(context.Background(), ctxKey{}, "tenant-a")
	var got any
	_, _, _, err := c.DoWithGuardCtx(ctx, "key", allow, func(callCtx context.Context) (*http.Response, error) {
		got = callCtx.Value(ctxKey{})
		return answer("x")
	})
	if err != nil || got != "tenant-a" {
		t.Fatalf("value = %v, err = %v; want tenant-a", got, err)
	}
}

// The shared call carries the deadline of the caller that started it. A
// follower whose own context is alive when that deadline ends the call never
// answers with the leader's timeout: it starts the call again as the new
// leader and gets the backend's answer.
//
// conformance: backend-deadlines-and-cancellation, semantics/error-coalescing-no-inherited-cancel
func TestCoalescerFollowerRetriesAfterLeaderDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		started := make(chan struct{}, 2)
		var calls atomic.Int64
		fn := func(ctx context.Context) (*http.Response, error) {
			n := calls.Add(1)
			started <- struct{}{}
			if n == 1 {
				<-ctx.Done() // the first call outlives the leader's deadline
				return nil, ctx.Err()
			}
			return answer("fresh")
		}
		leaderCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		leaderDone := make(chan error, 1)
		go func() {
			_, _, _, err := c.DoWithGuardCtx(leaderCtx, "key", allow, fn)
			leaderDone <- err
		}()
		<-started
		followerDone := make(chan []byte, 1)
		go func() {
			_, _, body, err := c.DoWithGuardCtx(context.Background(), "key", allow, fn)
			if err != nil {
				t.Errorf("follower inherited the leader's failure: %v", err)
			}
			followerDone <- body
		}()
		synctest.Wait()
		if err := <-leaderDone; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("leader: want its own deadline, got %v", err)
		}
		if body := <-followerDone; string(body) != "fresh" {
			t.Fatalf("follower body = %q, want the new call's answer", body)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("backend calls = %d, want 2 (the timed-out call and the follower's own)", got)
		}
	})
}

// A backend timeout (an error that wraps DeadlineExceeded while the shared
// call's context is alive) is the backend's failure, shared as it is: the
// follower does not send the heavy call again.
//
// conformance: semantics/error-coalescing-no-inherited-cancel
func TestCoalescerBackendTimeoutIsNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCoalescer()
		release := make(chan struct{})
		started := make(chan struct{}, 2)
		var calls atomic.Int64
		fn := func(ctx context.Context) (*http.Response, error) {
			calls.Add(1)
			started <- struct{}{}
			<-release
			return nil, fmt.Errorf("backend read: %w", context.DeadlineExceeded)
		}
		errs := make(chan error, 2)
		go func() { _, _, _, err := c.DoWithGuardCtx(context.Background(), "key", allow, fn); errs <- err }()
		<-started
		go func() { _, _, _, err := c.DoWithGuardCtx(context.Background(), "key", allow, fn); errs <- err }()
		synctest.Wait()
		close(release)
		for i := 0; i < 2; i++ {
			if err := <-errs; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("caller %d: %v", i, err)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("backend calls = %d, want 1", got)
		}
	})
}
