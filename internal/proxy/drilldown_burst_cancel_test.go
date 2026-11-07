package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// burstVL is a VictoriaLogs stand-in that holds every stats_query_range request
// until release is closed and records when a request's context is cancelled.
type burstVL struct {
	*httptest.Server
	requests  atomic.Int64
	cancelled chan struct{}
	arrived   chan struct{}
	release   chan struct{}
}

func newBurstVL(t *testing.T) *burstVL {
	t.Helper()
	v := &burstVL{cancelled: make(chan struct{}, 8), arrived: make(chan struct{}, 8), release: make(chan struct{})}
	v.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm() // reading the body lets the server notice a client that hangs up
		v.requests.Add(1)
		v.arrived <- struct{}{}
		select {
		case <-r.Context().Done():
			v.cancelled <- struct{}{}
		case <-v.release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
				`{"metric":{"__name__":"_f0"},"values":[[1700000060,"1"]]},` +
				`{"metric":{"__name__":"_f1"},"values":[[1700000060,"2"]]},` +
				`{"metric":{"__name__":"_f2"},"values":[[1700000060,"3"]]}]}}`))
		}
	}))
	t.Cleanup(v.Close)
	return v
}

type burstOutcome struct {
	series map[string]manualSeriesSamples
	err    error
}

// submitFields joins one waiter per field to the same burst group and returns
// each waiter's cancel and result channel.
func submitFields(p *Proxy, c *DrilldownBurstCoalescer, fields []string) ([]context.CancelFunc, []chan burstOutcome) {
	base := time.Unix(1700000000, 0).UTC()
	key := burstKey{orgID: "default", base: `{app="foo"}`, startNs: base.UnixNano(), endNs: base.Add(5 * time.Minute).UnixNano(), stepNs: int64(time.Minute)}
	fireFn := p.fusedFieldHits("default", `{app="foo"}`, base, base.Add(5*time.Minute), time.Minute)
	var cancels []context.CancelFunc
	var outs []chan burstOutcome
	for _, f := range fields {
		ctx, cancel := context.WithCancel(context.Background())
		out := make(chan burstOutcome, 1)
		go func() {
			s, err := c.Submit(ctx, key, f, fireFn)
			out <- burstOutcome{s, err}
		}()
		cancels = append(cancels, cancel)
		outs = append(outs, out)
	}
	return cancels, outs
}

func waitOutcome(t *testing.T, out chan burstOutcome) burstOutcome {
	t.Helper()
	select {
	case o := <-out:
		return o
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not return")
		return burstOutcome{}
	}
}

// When every client of a fused Drilldown scan has gone, the VictoriaLogs
// request is cancelled promptly instead of running on for the 30 s cap, and a
// later client starts a fresh scan rather than reading the abandoned one.
//
// conformance: backend-deadlines-and-cancellation
func TestDrilldownBurstCancelsVLScanWhenAllWaitersLeave(t *testing.T) {
	vl := newBurstVL(t)
	p := newTestProxy(t, vl.URL)
	c := newDrilldownBurstCoalescer(20, 30)

	cancels, outs := submitFields(p, c, []string{"a", "b", "c"})
	select {
	case <-vl.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("the fused request never reached VictoriaLogs")
	}
	if got := vl.requests.Load(); got != 1 {
		t.Fatalf("VictoriaLogs requests = %d, want 1 fused request", got)
	}
	for _, cancel := range cancels {
		cancel()
	}
	for _, out := range outs {
		if o := waitOutcome(t, out); !errors.Is(o.err, context.Canceled) {
			t.Fatalf("waiter err = %v, want context canceled", o.err)
		}
	}
	select {
	case <-vl.cancelled:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("VictoriaLogs scan was not cancelled after the last client left")
	}

	// Nothing of the abandoned scan is served to a later client.
	close(vl.release)
	cancels, outs = submitFields(p, c, []string{"a"})
	defer cancels[0]()
	o := waitOutcome(t, outs[0])
	if o.err != nil || len(o.series[""].Samples) != 1 {
		t.Fatalf("later client got %+v; want a fresh answer", o)
	}
	if got := vl.requests.Load(); got != 2 {
		t.Fatalf("VictoriaLogs requests = %d, want 2 (a fresh scan for the later client)", got)
	}
}

// One client leaving a fused scan does not cancel it for the others.
//
// conformance: backend-deadlines-and-cancellation
func TestDrilldownBurstKeepsVLScanWhileAWaiterRemains(t *testing.T) {
	vl := newBurstVL(t)
	p := newTestProxy(t, vl.URL)
	c := newDrilldownBurstCoalescer(20, 30)

	cancels, outs := submitFields(p, c, []string{"a", "b", "c"})
	defer cancels[1]()
	defer cancels[2]()
	select {
	case <-vl.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("the fused request never reached VictoriaLogs")
	}

	cancels[0]()
	if o := waitOutcome(t, outs[0]); !errors.Is(o.err, context.Canceled) {
		t.Fatalf("leaver err = %v, want context canceled", o.err)
	}
	select {
	case <-vl.cancelled:
		t.Fatal("VictoriaLogs scan was cancelled while two clients still waited")
	case <-time.After(200 * time.Millisecond):
	}

	close(vl.release)
	for i := 1; i < 3; i++ {
		if o := waitOutcome(t, outs[i]); o.err != nil || len(o.series[""].Samples) != 1 {
			t.Fatalf("waiter %d got %+v; want the answer", i, o)
		}
	}
	if got := vl.requests.Load(); got != 1 {
		t.Fatalf("VictoriaLogs requests = %d, want 1", got)
	}
}

// If the first client of a burst leaves, the clients that joined it still get
// the answer: the scan does not follow the client that started the group.
//
// conformance: backend-deadlines-and-cancellation
func TestDrilldownBurstFirstWaiterLeavingKeepsGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newDrilldownBurstCoalescer(50, 30)
		key := burstKey{orgID: "default", base: `{app="foo"}`, stepNs: int64(time.Minute)}
		var scanCancelled atomic.Bool
		release := make(chan struct{})
		fireFn := func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				scanCancelled.Store(true)
				return nil, ctx.Err()
			}
			out := map[string]fieldResult{}
			for _, f := range fields {
				out[f] = fieldResult{series: map[string]manualSeriesSamples{"": {}}}
			}
			return out, nil
		}

		firstCtx, cancelFirst := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		errs := make([]error, 3)
		for i, ctx := range []context.Context{firstCtx, context.Background(), context.Background()} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = c.Submit(ctx, key, string(rune('a'+i)), fireFn)
			}()
			synctest.Wait()
		}
		time.Sleep(60 * time.Millisecond) // the window closes and the fused scan starts
		synctest.Wait()

		cancelFirst()
		synctest.Wait()
		if scanCancelled.Load() {
			t.Fatal("the scan was cancelled when only the first waiter left")
		}
		close(release)
		wg.Wait()
		if !errors.Is(errs[0], context.Canceled) || errs[1] != nil || errs[2] != nil {
			t.Fatalf("errs = %v; want [canceled nil nil]", errs)
		}
	})
}

// Waiters that all leave during the window, or after the scan, leave no
// goroutine behind and do not start a scan nobody waits for.
//
// conformance: backend-deadlines-and-cancellation
func TestDrilldownBurstLeavesNoGoroutines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newDrilldownBurstCoalescer(50, 30)
		key := burstKey{orgID: "default", base: `{app="foo"}`, stepNs: int64(time.Minute)}
		var scans atomic.Int64
		fireFn := func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
			scans.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		}

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for _, f := range []string{"a", "b"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = c.Submit(ctx, key, f, fireFn)
			}()
		}
		synctest.Wait()
		cancel() // every waiter leaves inside the window
		wg.Wait()
		time.Sleep(time.Second) // the window timer fires on the abandoned group
		synctest.Wait()
		if scans.Load() != 0 {
			t.Fatalf("scans = %d, want none for an abandoned group", scans.Load())
		}

		// A scan that has started stops with its last waiter.
		ctx2, cancel2 := context.WithCancel(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Submit(ctx2, key, "a", fireFn)
		}()
		time.Sleep(60 * time.Millisecond)
		synctest.Wait()
		cancel2()
		wg.Wait()
	})
}
