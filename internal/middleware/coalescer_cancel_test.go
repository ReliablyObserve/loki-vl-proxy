package middleware

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
