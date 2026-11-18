package cache

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func failed(class ErrorClass, body string) *Answer {
	return &Answer{Status: 500, Header: http.Header{"Content-Type": {"text/plain; charset=utf-8"}}, Body: []byte(body), Class: class}
}

var testPolicy = NewPolicy(time.Second, 5*time.Second, 30*time.Second)

// A leader's failure reaches every follower waiting on it and is held for its
// class's TTL; after the TTL the next request leads a new flight.
//
// conformance: semantics/error-cache-classes-and-ttl
func TestErrorFlightsHoldFailureForClassTTL(t *testing.T) {
	e := NewErrorFlights(16, 1<<20, 1<<10, true)
	_, f, lead := e.Lookup("k")
	if !lead {
		t.Fatal("first request must lead")
	}
	_, g, lead2 := e.Lookup("k")
	if lead2 || g != f {
		t.Fatal("second request must wait on the first")
	}
	done := make(chan *Answer)
	go func() {
		o, a, ok := g.Wait(context.Background())
		if !ok || o != OutcomeFailed {
			t.Errorf("follower outcome %v ok=%v", o, ok)
		}
		done <- a
	}()
	ans := failed(ErrorTransient, "Internal Server Error")
	if ttl := finish(e, "k", f, OutcomeFailed, ans, testPolicy); ttl != time.Second {
		t.Fatalf("held for %v, want 1s", ttl)
	}
	if got := <-done; got != ans {
		t.Fatal("follower must get the leader's answer")
	}
	if held, _, _ := e.Lookup("k"); held != ans {
		t.Fatal("repeat within the TTL must answer the held failure")
	}
	restore := AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	if held, _, lead := e.Lookup("k"); held != nil || !lead {
		t.Fatal("after the TTL the next request must lead a new flight")
	}
	if entries, _ := e.Len(); entries != 0 {
		t.Fatalf("expired answer kept: %d entries", entries)
	}
}

// Policy: a class the plan rules out is neither held nor shared; a zero TTL
// shares with waiting followers but holds nothing; ErrorNever never.
//
// conformance: semantics/error-cache-classes-and-ttl
func TestErrorPolicyClasses(t *testing.T) {
	pol := NewPolicy(time.Second, 0, 0).Only(func(c ErrorClass) bool { return c != ErrorData })
	cases := []struct {
		class  ErrorClass
		ttl    time.Duration
		shares bool
	}{
		{ErrorNever, 0, false},
		{ErrorTransient, time.Second, true},
		{ErrorData, 0, false},
		{ErrorRequest, 0, true},
	}
	for _, c := range cases {
		if got := pol.TTL(c.class); got != c.ttl {
			t.Errorf("%s: TTL %v, want %v", c.class, got, c.ttl)
		}
		if got := pol.Shares(c.class); got != c.shares {
			t.Errorf("%s: Shares %v, want %v", c.class, got, c.shares)
		}
	}
	e := NewErrorFlights(16, 1<<20, 1<<10, true)
	_, f, _ := e.Lookup("k")
	if ttl := finish(e, "k", f, OutcomeFailed, failed(ErrorData, "x"), pol); ttl != 0 {
		t.Fatal("a class the policy excludes must not be held")
	}
	if o, _, _ := f.Wait(context.Background()); o != OutcomeUnshared {
		t.Fatalf("a class the policy excludes must not be shared, got %v", o)
	}
	if got := ErrorClasses(); len(got) != 3 || got[0].String() != "transient" || got[1].String() != "data" || got[2].String() != "request" {
		t.Fatalf("classes %v", got)
	}
	if ErrorClass(9).String() != "never" || NewPolicy(-time.Second, 0, 0).TTL(ErrorTransient) != 0 {
		t.Fatal("unknown class or negative TTL must hold nothing")
	}
}

// Held answers stay within the entry and byte caps; the oldest goes first,
// and an answer larger than the per-answer cap is neither held nor shared.
//
// conformance: semantics/error-cache-bounded-memory
func TestErrorFlightsBounded(t *testing.T) {
	e := NewErrorFlights(4, 1<<20, 64, true)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		_, f, _ := e.Lookup(key)
		finish(e, key, f, OutcomeFailed, failed(ErrorTransient, "boom"), testPolicy)
	}
	if entries, _ := e.Len(); entries != 4 {
		t.Fatalf("entries %d, want the cap 4", entries)
	}
	if held, _, _ := e.Lookup("k0"); held != nil {
		t.Fatal("the oldest answer must be evicted first")
	}
	if held, _, _ := e.Lookup("k9"); held == nil {
		t.Fatal("the newest answer must be kept")
	}

	small := NewErrorFlights(1000, 600, 256, true)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%d", i)
		_, f, _ := small.Lookup(key)
		finish(small, key, f, OutcomeFailed, failed(ErrorTransient, string(make([]byte, 100))), testPolicy)
	}
	if _, bytes := small.Len(); bytes > 600 {
		t.Fatalf("bytes %d over the cap 600", bytes)
	}
	_, f, _ := small.Lookup("big")
	if ttl := finish(small, "big", f, OutcomeFailed, failed(ErrorTransient, string(make([]byte, 300))), testPolicy); ttl != 0 {
		t.Fatal("an answer above the per-answer cap must not be held")
	}
	if o, _, _ := f.Wait(context.Background()); o != OutcomeUnshared {
		t.Fatal("an answer above the per-answer cap must not be shared")
	}
	small.Clear()
	if entries, bytes := small.Len(); entries != 0 || bytes != 0 {
		t.Fatal("Clear must drop every held answer")
	}
	tiny := NewErrorFlights(0, 10, 256, true)
	_, f, _ = tiny.Lookup("k")
	finish(tiny, "k", f, OutcomeFailed, failed(ErrorTransient, "x"), testPolicy)
	if entries, _ := tiny.Len(); entries != 0 {
		t.Fatal("a zero cap holds nothing")
	}
}

// Released and stored outcomes reach the followers; a follower whose context
// ends stops waiting; without join every request leads.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsOutcomes(t *testing.T) {
	e := NewErrorFlights(16, 1<<20, 1<<10, true)
	_, f, _ := e.Lookup("k")
	f.Release()
	f.Release()
	if o, _, ok := f.Wait(context.Background()); !ok || o != OutcomeReleased {
		t.Fatalf("released flight: %v", o)
	}
	finish(e, "k", f, OutcomeStored, nil, testPolicy) // after a release the outcome stays released
	if o, _, _ := f.Wait(context.Background()); o != OutcomeReleased {
		t.Fatal("the first outcome wins")
	}

	_, f, _ = e.Lookup("s")
	finish(e, "s", f, OutcomeSucceeded, nil, testPolicy)
	if o, _, _ := f.Wait(context.Background()); o != OutcomeReleased {
		t.Fatal("a success without an answer releases the followers")
	}

	_, f, _ = e.Lookup("c")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, ok := f.Wait(ctx); ok {
		t.Fatal("a cancelled follower must stop waiting")
	}
	if f.Waiters() != 1 || e.Waiting() != 1 {
		t.Fatalf("waiters %d, waiting %d", f.Waiters(), e.Waiting())
	}

	solo := NewErrorFlights(16, 1<<20, 1<<10, false)
	_, a, l1 := solo.Lookup("k")
	_, b, l2 := solo.Lookup("k")
	if !l1 || !l2 || a == b {
		t.Fatal("without join every request leads its own flight")
	}
	finish(solo, "k", a, OutcomeFailed, failed(ErrorTransient, "x"), testPolicy)
	if held, _, _ := solo.Lookup("k"); held == nil {
		t.Fatal("without join a failure is still held")
	}
}

// Concurrent lookups and finishes keep the maps consistent (run with -race).
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsConcurrent(t *testing.T) {
	e := NewErrorFlights(64, 1<<20, 1<<10, true)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%4)
			for j := 0; j < 200; j++ {
				held, f, lead := e.Lookup(key)
				switch {
				case held != nil:
				case lead:
					finish(e, key, f, OutcomeFailed, failed(ErrorTransient, "x"), testPolicy)
				default:
					f.Wait(context.Background())
				}
			}
		}(i)
	}
	wg.Wait()
	if entries, _ := e.Len(); entries > 4 {
		t.Fatalf("entries %d for 4 keys", entries)
	}
}

// finish ends f with a ready answer; it returns how long the answer is held.
func finish(e *ErrorFlights, key string, f *Flight, o Outcome, a *Answer, pol Policy) time.Duration {
	var build AnswerBuilder
	if a != nil {
		build = builtAnswer{a}
	}
	_, ttl := e.Finish(key, f, o, build, pol)
	return ttl
}

type builtAnswer struct{ a *Answer }

func (b builtAnswer) BuildAnswer() *Answer { return b.a }

type countingBuilder struct {
	a *Answer
	n *int
}

func (b countingBuilder) BuildAnswer() *Answer { *b.n++; return b.a }

// A success is built and handed over only when a request joined; Peek and
// Hold read and write a held answer under a second key.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsSuccessAndSecondKey(t *testing.T) {
	e := NewErrorFlights(16, 1<<20, 1<<10, true)
	built := 0
	ok := &Answer{Status: 200, Body: []byte("{}")}
	build := countingBuilder{ok, &built}
	_, f, _ := e.Lookup("s")
	if a, _ := e.Finish("s", f, OutcomeSucceeded, build, testPolicy); a != nil || built != 0 {
		t.Fatal("nobody joined: nothing is built")
	}
	_, f, _ = e.Lookup("s")
	_, g, _ := e.Lookup("s")
	if g != f || f.Joined() != 1 {
		t.Fatal("join")
	}
	if a, ttl := e.Finish("s", f, OutcomeSucceeded, build, testPolicy); a != ok || ttl != 0 || built != 1 {
		t.Fatal("a joined success is built once and never held")
	}
	if o, a, _ := g.Wait(context.Background()); o != OutcomeSucceeded || a != ok {
		t.Fatal("the follower reads the success")
	}
	if held, _, _ := e.Lookup("s"); held != nil {
		t.Fatal("a success is never held")
	}
	if e.Peek("w") != nil {
		t.Fatal("nothing held yet")
	}
	if !e.Hold("w", nil, time.Second).IsZero() || !e.Hold("w", failed(ErrorTransient, "x"), 0).IsZero() {
		t.Fatal("nothing to hold")
	}
	if e.Hold("w", failed(ErrorTransient, "x"), time.Second).IsZero() || e.Peek("w") == nil {
		t.Fatal("held under the second key")
	}
	restore := AdvanceClockForTesting(2 * time.Second)
	defer restore()
	if e.Peek("w") != nil {
		t.Fatal("expired")
	}
}

// joinProbe is an answer builder that, while the leader builds its failed
// answer (after it stopped running), sends one more identical request: that
// request must wait on the flight or read the held failure, never lead.
type joinProbe struct {
	e    *ErrorFlights
	key  string
	a    *Answer
	lead bool
	f    *Flight
}

func (b *joinProbe) BuildAnswer() *Answer {
	_, b.f, b.lead = b.e.Join(b.key, nil)
	return b.a
}

// A request arriving while the leader's failure is being ended joins the
// flight or reads the held failure: there is no moment where the flight is
// gone and its failure not yet held.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsFailureHeldBeforeFlightEnds(t *testing.T) {
	e := NewErrorFlights(16, 1<<20, 1<<10, true)
	_, f, lead := e.Lookup("k")
	if !lead {
		t.Fatal("first request must lead")
	}
	ans := failed(ErrorData, "maximum of series (500) reached for a single query")
	probe := &joinProbe{e: e, key: "k", a: ans}
	if _, ttl := e.Finish("k", f, OutcomeFailed, probe, testPolicy); ttl != 5*time.Second {
		t.Fatalf("held for %v, want 5s", ttl)
	}
	if probe.lead {
		t.Fatal("a request arriving while the failure is ended led a second flight")
	}
	if probe.f != f {
		t.Fatal("a request arriving before the failure is held waits on the leader's flight")
	}
	if o, a, ok := probe.f.Wait(context.Background()); !ok || o != OutcomeFailed || a != ans {
		t.Fatalf("the late follower got %v %v", o, a)
	}
	if held, _, lead := e.Lookup("k"); held != ans || lead {
		t.Fatal("after the flight ends its failure is held")
	}

	// A flight joined on a coarser key holds the failure under the leader's
	// exact key and marks the coarser key in the same step.
	_, f, _ = e.Lookup("pre")
	exact := failed(ErrorData, "x")
	exact.Key = "exact"
	probe = &joinProbe{e: e, key: "pre", a: exact}
	e.Finish("pre", f, OutcomeFailed, probe, testPolicy)
	if probe.lead || probe.f != f {
		t.Fatal("a request arriving while a pre-key failure is ended waits on the flight")
	}
	if e.Peek("exact") != exact || e.Peek(HeldHintKey("pre")) != HeldHint || e.Peek("pre") != nil {
		t.Fatal("the failure is held under the exact key, the hint under the pre-key")
	}
}

// slowBuilder builds its answer after a pause, widening the moment between
// the leader's end and its failure being held.
type slowBuilder struct{ a *Answer }

func (b slowBuilder) BuildAnswer() *Answer { time.Sleep(200 * time.Microsecond); return b.a }

// Identical requests racing a failing leader send one request in total: every
// other one waits on the flight or reads the held failure (run with -race and
// a high -count).
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsBurstLeadsOnce(t *testing.T) {
	e := NewErrorFlights(16, 1<<20, 1<<10, true)
	ans := failed(ErrorData, "maximum of series (500) reached for a single query")
	var leads atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				held, f, lead := e.Lookup("k")
				switch {
				case held != nil:
					if held != ans {
						t.Error("held answer differs")
					}
				case lead:
					leads.Add(1)
					e.Finish("k", f, OutcomeFailed, slowBuilder{ans}, testPolicy)
				default:
					if o, a, _ := f.Wait(context.Background()); o != OutcomeFailed || a != ans {
						t.Errorf("follower got %v %v", o, a)
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if n := leads.Load(); n != 1 {
		t.Fatalf("%d leaders for one failing request held for 5s, want 1", n)
	}
}
