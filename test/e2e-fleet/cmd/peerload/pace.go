package main

import (
	"context"
	"time"
)

// paceTick is how often pace checks the schedule. A ticker drops the ticks its
// receiver misses, so one send per tick delivers less than its rate whenever
// ticks are late (in a VM, about half of 1,600 per second). pace instead sends
// every request owed since the start on each tick, so late or dropped ticks
// are caught up on the next one and the arrivals stay open-loop.
const paceTick = time.Millisecond

// pace calls fire qps times a second until ctx ends, counting from the
// schedule's start rather than from tick to tick.
func pace(ctx context.Context, qps float64, fire func()) {
	if qps <= 0 {
		return
	}
	t := time.NewTicker(paceTick)
	defer t.Stop()
	start := time.Now()
	var sent int64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			owed := int64(now.Sub(start).Seconds()*qps) - sent
			for ; owed > 0; owed-- {
				if ctx.Err() != nil {
					return
				}
				fire()
				sent++
			}
		}
	}
}
