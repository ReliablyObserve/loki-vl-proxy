package proxy

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// What reuse saves VictoriaLogs. Every VictoriaLogs call is measured once, where its body closes (recordFlowVL,
// recordFlowVLRows): calls, time to the end of the body, decoded bytes, raw-query rows. The work of a cached answer
// is the work measured while it was built, kept in buildCosts under its cache key; a client request that reuses the
// answer avoided that work, and counts it (noteReuse). An answer this replica did not build since it started (read
// from disk after a restart, from a peer, pushed by a peer) has no recorded work: it counts as reuse of unknown
// cost and adds no avoided work, never a guess. A replica that serves a peer counts nothing: only the replica whose
// client request would otherwise have asked VictoriaLogs counts the reuse.
//
// Build work: for a whole client answer (the response cache) the work the request did up to the store, including
// the work of calls it shared with an identical request in flight; for a piece (one query_range window) the work
// done inside its work scope. Work avoided by reuse is never part of a build's work, so summing a request's own
// calls and the work its reuse avoided never counts one VictoriaLogs call twice.

// workScope collects the VictoriaLogs work done under a context: a piece being built, or one shared call. A call
// adds to its scope and to every enclosing scope.
type workScope struct {
	parent                    *workScope
	calls, nanos, bytes, rows atomic.Int64
}

type workScopeKey struct{}

func workScopeFrom(ctx context.Context) *workScope {
	s, _ := ctx.Value(workScopeKey{}).(*workScope)
	return s
}

// withWorkScope opens a work scope under ctx.
func withWorkScope(ctx context.Context) (context.Context, *workScope) {
	s := &workScope{parent: workScopeFrom(ctx)}
	return context.WithValue(ctx, workScopeKey{}, s), s
}

func (s *workScope) work() metrics.VLWork {
	return metrics.VLWork{Calls: s.calls.Load(), Duration: time.Duration(s.nanos.Load()), Bytes: s.bytes.Load(), Rows: s.rows.Load()}
}

// addScopeWork adds w to every work scope of ctx.
func addScopeWork(ctx context.Context, w metrics.VLWork) {
	for s := workScopeFrom(ctx); s != nil; s = s.parent {
		s.calls.Add(w.Calls)
		s.nanos.Add(int64(w.Duration))
		s.bytes.Add(w.Bytes)
		s.rows.Add(w.Rows)
	}
}

// flowReuse counts a request's reused answers: those with a recorded build cost, those without, and whether it
// shared an identical request's answer or calls. Two 8-bit counts and a flag fit in padding of the request's
// telemetry; a count stops at 255.
type flowReuse struct {
	recorded, unknown uint8
	shared            bool
}

func (f *flowReuse) add(recorded bool) {
	n := &f.unknown
	if recorded {
		n = &f.recorded
	}
	if *n < 255 {
		*n++
	}
}

func (f flowReuse) any() bool { return f.recorded > 0 || f.unknown > 0 || f.shared }

// sharedWork is the VictoriaLogs work a request received from calls it shared, allocated on its first shared call.
type sharedWork struct{ work metrics.VLWork }

// buildWork is the VictoriaLogs work the request of rt did so far, its shared calls included: the build cost of a
// whole answer it stores now.
func (rt *requestTelemetry) buildWork() metrics.VLWork {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	w := metrics.VLWork{Calls: int64(rt.flow.vlCalls), Duration: rt.flow.vlDur, Bytes: rt.flow.vlBytes, Rows: rt.flow.vlRows}
	if rt.shared != nil {
		w = w.Add(rt.shared.work)
	}
	return w
}

// buildCostLedger holds the build work of cached answers by key, in a fixed table: a slot keeps the full 64-bit
// hash of its key and is overwritten by a later key of the same slot, whose earlier answer then reads as unknown.
// It never grows, and a lookup allocates nothing.
type buildCostLedger struct {
	seed  maphash.Seed
	mask  uint64
	slots []buildCostSlot
	locks [64]sync.Mutex
}

type buildCostSlot struct {
	hash uint64
	work metrics.VLWork
}

// buildCostSlots is the ledger's size: 32768 slots of 40 bytes (1.25 MiB), over three times the default
// -cache-max entries.
const buildCostSlots = 1 << 15

func newBuildCostLedger() *buildCostLedger {
	return &buildCostLedger{seed: maphash.MakeSeed(), mask: buildCostSlots - 1, slots: make([]buildCostSlot, buildCostSlots)}
}

func (l *buildCostLedger) slot(key string) (uint64, uint64) {
	h := maphash.String(l.seed, key)
	if h == 0 {
		h = 1 // 0 marks an empty slot
	}
	return h, h & l.mask
}

func (l *buildCostLedger) put(key string, w metrics.VLWork) {
	if l == nil || key == "" {
		return
	}
	h, i := l.slot(key)
	mu := &l.locks[i%uint64(len(l.locks))]
	mu.Lock()
	l.slots[i] = buildCostSlot{hash: h, work: w}
	mu.Unlock()
}

func (l *buildCostLedger) get(key string) (metrics.VLWork, bool) {
	if l == nil || key == "" {
		return metrics.VLWork{}, false
	}
	h, i := l.slot(key)
	mu := &l.locks[i%uint64(len(l.locks))]
	mu.Lock()
	s := l.slots[i]
	mu.Unlock()
	if s.hash != h {
		return metrics.VLWork{}, false
	}
	return s.work, true
}

// noteBuilt records w as the build work of the answers stored under keys.
func (p *Proxy) noteBuilt(w metrics.VLWork, keys ...string) {
	for _, key := range keys {
		p.buildCosts.put(key, w)
	}
}

// noteReuse counts a reused answer of the client request of ctx: built is its build work when recorded. Work done
// outside a client request (peer serves, background refreshes) counts nothing.
func (p *Proxy) noteReuse(ctx context.Context, layer metrics.ReuseLayer, tier metrics.ReuseTier, built metrics.VLWork, recorded bool) {
	rt := getRequestTelemetry(ctx)
	meta, ok := ctx.Value(requestRouteMetaKey).(requestRouteMeta)
	if rt == nil || !ok {
		return
	}
	rt.mu.Lock()
	if tier == metrics.ReuseInFlight {
		rt.reuse.shared = true
	} else {
		rt.reuse.add(recorded)
	}
	rt.mu.Unlock()
	p.metrics.RecordVLReuse(meta.endpoint, meta.route, layer, tier, built, recorded)
}

// noteCachedReuse counts a reused cached answer stored under key, with its build work when the ledger has it.
func (p *Proxy) noteCachedReuse(ctx context.Context, key string, layer metrics.ReuseLayer, tier string) {
	w, ok := p.buildCosts.get(key)
	p.noteReuse(ctx, layer, metrics.ReuseTierOf(tier), w, ok)
}

// noteSharedWork counts work the request of ctx received from a call it shared: a reuse in flight, part of the
// build work of what the request then stores (its enclosing work scopes and its own total).
func (p *Proxy) noteSharedWork(ctx context.Context, layer metrics.ReuseLayer, w metrics.VLWork, recorded bool) {
	if rt := getRequestTelemetry(ctx); rt != nil && recorded {
		rt.mu.Lock()
		if rt.shared == nil {
			rt.shared = &sharedWork{}
		}
		rt.shared.work = rt.shared.work.Add(w)
		rt.mu.Unlock()
	}
	if recorded {
		addScopeWork(ctx, w)
	}
	p.noteReuse(ctx, layer, metrics.ReuseInFlight, w, recorded)
}

// servedClass is how the request was served, from its status, its own VictoriaLogs calls, its cache hits and its
// reuse.
func servedClass(status, vlCalls, cacheHits int, reuse flowReuse) metrics.ServedClass {
	switch {
	case status >= 400:
		return metrics.ServedError
	case vlCalls > 0 && reuse.any():
		return metrics.ServedPartial
	case vlCalls > 0:
		return metrics.ServedVL
	case reuse.shared:
		return metrics.ServedShared
	case cacheHits > 0 || reuse.any():
		return metrics.ServedCache
	}
	return metrics.ServedLocal
}

// coalescedCallScope is the coalescer's OnCall: a shared call runs in its own work scope, under the scopes of the
// request that started it.
func (p *Proxy) coalescedCallScope(ctx context.Context) context.Context {
	ctx, _ = withWorkScope(ctx)
	return ctx
}

// coalescedCallShared is the coalescer's OnShare: the waiter avoided the shared call, whose work the call's scope
// measured (the call's body is read and closed before its waiters are released).
func (p *Proxy) coalescedCallShared(waiter, call context.Context) {
	s := workScopeFrom(call)
	if s == nil {
		return
	}
	p.noteSharedWork(waiter, metrics.ReuseVLCall, s.work(), true)
}
