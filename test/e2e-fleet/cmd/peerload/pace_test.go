package main

import (
	"context"
	"testing"
	"time"
)

// TestPaceDeliversItsRate checks the driver keeps its rate, including rates
// above one per tick. The lower bound leaves room for a loaded CI runner under
// -race (a stall at the end of the window is not caught up); the old
// one-request-per-tick driver delivered about half its rate in a VM.
func TestPaceDeliversItsRate(t *testing.T) {
	for _, qps := range []float64{200, 1600, 5000} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var n int
		start := time.Now()
		pace(ctx, qps, func() { n++ })
		cancel()
		want := qps * time.Since(start).Seconds()
		if float64(n) < want*0.8 || float64(n) > want*1.01+1 {
			t.Errorf("qps %v: sent %d in %v, want about %.0f", qps, n, time.Since(start), want)
		}
	}
}

// TestPaceCatchesUpLateTicks checks a slow fire (ticks missed while it runs)
// does not lower the rate: the owed sends go out on the next tick.
func TestPaceCatchesUpLateTicks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var n int
	start := time.Now()
	pace(ctx, 1000, func() {
		n++
		if n%100 == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	})
	want := 1000 * time.Since(start).Seconds()
	if float64(n) < want*0.8 {
		t.Errorf("sent %d in %v, want about %.0f", n, time.Since(start), want)
	}
}

func TestPaceZeroRate(t *testing.T) {
	pace(context.Background(), 0, func() { t.Fatal("fired at rate 0") })
}
