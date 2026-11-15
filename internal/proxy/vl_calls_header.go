package proxy

import (
	"net/http"
	"strconv"
)

// With -debug-vl-calls-header every Loki read response says what it cost
// VictoriaLogs, so a cache reuse check (a repeat must ask VictoriaLogs nothing)
// can count the calls of one request. The process-wide metrics cannot: they
// also count other clients, background refreshes, and calls an earlier request
// abandoned that VictoriaLogs serves late.
//
//	X-Loki-VL-Backend-Calls  the VictoriaLogs calls this request started before
//	                         its response headers were written: a call is
//	                         counted when it is handed to the transport, a
//	                         retry is a call, and a call shared with another
//	                         request in flight is counted once, for the request
//	                         that started it. A streamed answer can start calls
//	                         after its headers, so for it the count is a lower
//	                         bound.
//	X-Loki-VL-Cache-Result   the response cache's result for the request: hit,
//	                         miss, or bypass when no response cache was
//	                         consulted. A miss or bypass with 0 calls was
//	                         answered without VictoriaLogs by another layer; a
//	                         hit is a reused answer.
//
// Loki sends neither header, so it is off by default.
const (
	vlBackendCallsHeader = "X-Loki-VL-Backend-Calls"
	vlCacheResultHeader  = "X-Loki-VL-Cache-Result"
)

// vlCallCounter counts, in the request's flow, every call to VictoriaLogs the
// proxy's backend client starts. Calls to other hosts through the same client
// (peer cache reads) are not VictoriaLogs calls.
type vlCallCounter struct {
	inner http.RoundTripper
	host  string
}

func (c *vlCallCounter) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL != nil && req.URL.Host == c.host {
		if rt := getRequestTelemetry(req.Context()); rt != nil {
			rt.mu.Lock()
			rt.flow.vlStarted++
			rt.mu.Unlock()
		}
	}
	return c.inner.RoundTrip(req)
}

// countVLCalls makes the backend client count the calls each request starts;
// New gives the client its transport before it is called.
func (p *Proxy) countVLCalls() {
	p.client.Transport = &vlCallCounter{inner: p.client.Transport, host: p.backend.Host}
}

// setVLCallsHeaders writes the request's calls so far and its cache result.
func (rt *requestTelemetry) setVLCallsHeaders(h http.Header) {
	rt.mu.Lock()
	calls, result := rt.flow.vlStarted, rt.cacheResult
	rt.mu.Unlock()
	h.Set(vlBackendCallsHeader, strconv.Itoa(calls))
	if result != "" {
		h.Set(vlCacheResultHeader, result)
	}
}
