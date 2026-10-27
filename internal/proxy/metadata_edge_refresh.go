package proxy

import (
	"context"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Bounded staleness for near-now label listings.
//
// Loki answers /labels and /label/{name}/values from its index, in
// milliseconds whatever its queriers are doing. The proxy lists them from the
// metadata inventory: settled buckets come from its cache, but a request
// ending near now (Loki's max_metadata_cache_freshness) whose cached answer
// is older than -recent-tail-refresh-max-staleness reads the near-now edge
// from VictoriaLogs again. Under load that call waits behind heavy selects
// for VictoriaLogs CPU: stream_field_names took 1-2.6 s with VictoriaLogs at
// 360-470 % CPU, while the same call answers in 5-40 ms when it is idle.
//
// With -metadata-max-staleness > 0 such a request starts the refresh detached
// from itself and waits at most -metadata-edge-refresh-budget for it. A
// refresh that finishes in time answers exactly. Otherwise the request is
// answered with the cached answer, as long as the VictoriaLogs read behind it
// started at most -metadata-max-staleness ago; the refresh completes in the
// background and the next request reads its result. Past the bound the
// request waits for VictoriaLogs as before. One refresh runs per cache key
// (tenant, auth scope, endpoint and window), within the background timeout,
// and Shutdown cancels it.
//
// A stale answer is marked like a stale-on-error answer (staleResponseHeader),
// so the compatibility-edge cache and the multi-tenant merge cache never keep
// it and no layer serves it past the bound. An empty answer is never served
// stale. An exact answer taken from a refresh another request started may
// reflect a read that began up to the background timeout earlier, as for any
// request that joins an in-flight listing.

// metadataEdgeState is the per-proxy record of near-now answers and the
// background refreshes of their edges.
type metadataEdgeState struct {
	mu        sync.Mutex
	read      map[string]time.Time // cache key -> start of the VictoriaLogs read behind the stored answer
	pruneAt   int                  // map size that triggers the next prune
	base      context.Context      // cancelled by Shutdown
	stop      context.CancelFunc
	wg        sync.WaitGroup
	refreshes singleflight.Group
}

// metadataEdgePruneFloor is the map size below which the record is not pruned.
const metadataEdgePruneFloor = 1024

// recordMetadataEdgeRead notes that the answer just stored for key was read
// from VictoriaLogs starting at started. Callers store the answer first, so a
// reader that sees the record finds an answer at least that fresh. A later
// read never moves the record back.
func (p *Proxy) recordMetadataEdgeRead(key string, started time.Time) {
	if p.metadataMaxStaleness <= 0 || started.IsZero() {
		return
	}
	s := &p.metadataEdges
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.read == nil {
		s.read = map[string]time.Time{}
	}
	if prev, ok := s.read[key]; ok && !started.After(prev) {
		return
	}
	s.read[key] = started
	// Entries older than the bound are never used again; the map holds the
	// keys read within the last -metadata-max-staleness, plus at most as
	// many again between prunes.
	if len(s.read) >= max(s.pruneAt, metadataEdgePruneFloor) {
		cutoff := time.Now().Add(-p.metadataMaxStaleness)
		for k, at := range s.read {
			if at.Before(cutoff) {
				delete(s.read, k)
			}
		}
		s.pruneAt = 2 * len(s.read)
	}
}

// lastMetadataEdgeRead returns when the VictoriaLogs read behind the stored
// answer for key started, if this replica stored one.
func (p *Proxy) lastMetadataEdgeRead(key string) (time.Time, bool) {
	s := &p.metadataEdges
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.read[key]
	return at, ok
}

// startMetadataEdgeRefresh runs answer for key detached from r, or joins the
// refresh already running for key, and returns its result channel; nil after
// Shutdown. ctx carries the request's tenant, routing and auth scope; the
// refresh keeps them, gets the background timeout, and ends at Shutdown.
func (p *Proxy) startMetadataEdgeRefresh(ctx context.Context, r *http.Request, key string, answer func(context.Context) ([]byte, error)) <-chan singleflight.Result {
	s := &p.metadataEdges
	s.mu.Lock()
	if s.base == nil {
		s.base, s.stop = context.WithCancel(context.Background())
	}
	base := s.base
	s.mu.Unlock()
	if base.Err() != nil {
		return nil
	}
	detached := r.Clone(context.WithoutCancel(ctx))
	return s.refreshes.DoChan(key, func() (interface{}, error) {
		s.mu.Lock()
		if base.Err() != nil {
			s.mu.Unlock()
			return nil, base.Err()
		}
		s.wg.Add(1)
		s.mu.Unlock()
		defer s.wg.Done()
		started := time.Now()
		refreshCtx, cancel := context.WithTimeout(context.WithValue(detached.Context(), origRequestKey, detached), p.labelBackgroundTimeout())
		defer cancel()
		defer context.AfterFunc(base, cancel)()
		body, err := answer(refreshCtx)
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		p.observeInternalOperation(refreshCtx, "metadata_edge_refresh", outcome, time.Since(started))
		return body, err
	})
}

// stopMetadataEdgeRefreshes cancels the background edge refreshes and waits
// for them, at most until ctx ends.
func (p *Proxy) stopMetadataEdgeRefreshes(ctx context.Context) {
	s := &p.metadataEdges
	s.mu.Lock()
	if s.base == nil {
		s.base, s.stop = context.WithCancel(context.Background())
	}
	s.stop()
	s.mu.Unlock()
	waitKeepWarm(ctx, &s.wg)
}

// metadataEdgeAnswer is what serveMetadataEdge decided.
type metadataEdgeAnswer struct {
	served bool  // the response was written
	err    error // the refresh failed within the budget: answer it like a failed fetch
}

// serveMetadataEdge answers a near-now request whose cached answer is past
// -recent-tail-refresh-max-staleness without waiting for VictoriaLogs longer
// than -metadata-edge-refresh-budget, when this replica read that answer at
// most -metadata-max-staleness ago. served false (and no error) leaves the
// request to the synchronous fetch: stale serving is off, the answer is
// older than the bound, or the proxy is shutting down.
func (p *Proxy) serveMetadataEdge(w http.ResponseWriter, r *http.Request, ctx context.Context, endpoint, cacheKey string, started time.Time, answer func(context.Context) ([]byte, error)) metadataEdgeAnswer {
	if p.metadataMaxStaleness <= 0 {
		return metadataEdgeAnswer{}
	}
	readAt, ok := p.lastMetadataEdgeRead(cacheKey)
	if !ok || time.Since(readAt) > p.metadataMaxStaleness {
		return metadataEdgeAnswer{}
	}
	// Read after the record: the stored answer is at least as fresh as it.
	// An empty answer is a negative entry, not a last known answer (as for
	// stale-on-error: data may not have been searchable yet), and a disk or
	// peer copy (age unknown) is not the answer this replica recorded.
	stale, _, age, _, hit := p.endpointReadCacheEntryAge(endpoint, cacheKey)
	if !hit || age < 0 || metadataListPayloadEmpty(stale) {
		return metadataEdgeAnswer{}
	}
	results := p.startMetadataEdgeRefresh(ctx, r, cacheKey, answer)
	if results == nil {
		return metadataEdgeAnswer{}
	}
	budget := time.NewTimer(max(p.metadataEdgeRefreshBudget, 0))
	defer budget.Stop()
	select {
	case res := <-results:
		return p.writeMetadataEdgeResult(w, ctx, endpoint, started, res)
	case <-r.Context().Done():
		return metadataEdgeAnswer{served: true}
	case <-budget.C:
	}
	staleness := time.Since(readAt)
	if staleness > p.metadataMaxStaleness {
		// The wait used up the rest of the bound: answer exactly.
		select {
		case res := <-results:
			return p.writeMetadataEdgeResult(w, ctx, endpoint, started, res)
		case <-r.Context().Done():
			return metadataEdgeAnswer{served: true}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	markStaleResponse(w.Header())
	_, _ = w.Write(stale)
	p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(started))
	p.observeInternalOperation(ctx, "metadata_edge_answer", "stale", time.Since(started))
	p.observeInternalOperation(ctx, "metadata_edge_staleness", endpoint, staleness)
	return metadataEdgeAnswer{served: true}
}

func (p *Proxy) writeMetadataEdgeResult(w http.ResponseWriter, ctx context.Context, endpoint string, started time.Time, res singleflight.Result) metadataEdgeAnswer {
	if res.Err != nil {
		return metadataEdgeAnswer{err: res.Err}
	}
	body, _ := res.Val.([]byte)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
	p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(started))
	p.observeInternalOperation(ctx, "metadata_edge_answer", "exact", time.Since(started))
	return metadataEdgeAnswer{served: true}
}

// observeMetadataEdgeWait counts a near-now answer that waited for
// VictoriaLogs: no answer read within the bound, or the proxy is stopping.
func (p *Proxy) observeMetadataEdgeWait(ctx context.Context, freshFetch bool, started time.Time) {
	if freshFetch && p.metadataMaxStaleness > 0 {
		p.observeInternalOperation(ctx, "metadata_edge_answer", "waited", time.Since(started))
	}
}
