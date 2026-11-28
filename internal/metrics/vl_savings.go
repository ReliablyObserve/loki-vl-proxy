package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// What the proxy saved VictoriaLogs and its clients: per route, the client requests by how they were served (with
// their latency, the bytes sent to the client and the VictoriaLogs work each made), and the answers and VictoriaLogs
// calls reused instead of being asked again, with the VictoriaLogs work measured when the reused answer was built.
// Every label comes from a fixed set or the route set of the other client metrics, so the series count is bounded.

// ServedClass is how a client request was served.
type ServedClass uint8

const (
	// ServedCache: no VictoriaLogs call, at least one cache hit.
	ServedCache ServedClass = iota
	// ServedShared: no VictoriaLogs call, the answer or its calls shared with an identical request in flight.
	ServedShared
	// ServedPartial: VictoriaLogs calls plus at least one reused piece (a window, stats buckets, a shared call).
	ServedPartial
	// ServedVL: VictoriaLogs calls and nothing reused.
	ServedVL
	// ServedLocal: no VictoriaLogs call and nothing reused (an answer the proxy holds or computes alone).
	ServedLocal
	// ServedError: a 4xx or 5xx answer, apart so it never blurs a comparison of answers.
	ServedError
	servedClassCount
)

var servedClassNames = [servedClassCount]string{"cache", "shared", "partial", "vl", "local", "error"}

func (c ServedClass) String() string {
	if c < servedClassCount {
		return servedClassNames[c]
	}
	return "local"
}

// ReuseLayer is what a reused answer was.
type ReuseLayer uint8

const (
	// ReuseResponse: a whole client answer from the response cache.
	ReuseResponse ReuseLayer = iota
	// ReuseWindow: one query_range log window.
	ReuseWindow
	// ReuseStatsExtent: stored stats buckets of a stats_query_range call.
	ReuseStatsExtent
	// ReuseRequestFlight: the answer of an identical client request in flight.
	ReuseRequestFlight
	// ReuseHeldFailure: a failure held for its class instead of asking again.
	ReuseHeldFailure
	// ReuseVLCall: one VictoriaLogs call shared with identical calls in flight.
	ReuseVLCall
	// ReuseBurst: a Logs Drilldown field count read by another request's fused call.
	ReuseBurst
	reuseLayerCount
)

var reuseLayerNames = [reuseLayerCount]string{"response", "window", "stats_extent", "request_flight", "held_failure", "vl_call", "burst"}

// ReuseTier is where a reused answer came from.
type ReuseTier uint8

const (
	ReuseL1Memory ReuseTier = iota
	ReuseL2Disk
	ReuseL3Peer
	// ReuseInFlight: work in flight for another request on this replica.
	ReuseInFlight
	reuseTierCount
)

var reuseTierNames = [reuseTierCount]string{"l1_memory", "l2_disk", "l3_peer", "in_flight"}

// ReuseTierOf maps a cache tier name (cache.Cache's) to its tier; anything else is memory.
func ReuseTierOf(tier string) ReuseTier {
	switch tier {
	case "l2_disk":
		return ReuseL2Disk
	case "l3_peer":
		return ReuseL3Peer
	}
	return ReuseL1Memory
}

// VLWork is VictoriaLogs work: calls, their summed time to the end of the body, the decoded body bytes and the log
// rows of raw query bodies (/select/logsql/query, one row per NDJSON line).
type VLWork struct {
	Calls    int64
	Duration time.Duration
	Bytes    int64
	Rows     int64
}

// Add returns w plus o.
func (w VLWork) Add(o VLWork) VLWork {
	return VLWork{Calls: w.Calls + o.Calls, Duration: w.Duration + o.Duration, Bytes: w.Bytes + o.Bytes, Rows: w.Rows + o.Rows}
}

type workCounters struct{ calls, nanos, bytes, rows atomic.Int64 }

func (c *workCounters) add(w VLWork) {
	c.calls.Add(max(w.Calls, 0))
	c.nanos.Add(max(int64(w.Duration), 0))
	c.bytes.Add(max(w.Bytes, 0))
	c.rows.Add(max(w.Rows, 0))
}

type servedRouteMetrics struct {
	duration    [servedClassCount]*histogram
	clientBytes [servedClassCount]atomic.Int64
	vl          [servedClassCount]workCounters
}

type reuseRouteMetrics struct {
	recorded, unknown [reuseLayerCount][reuseTierCount]atomic.Int64
	avoided           [reuseLayerCount][reuseTierCount]workCounters
}

type savingsMetrics struct {
	mu              sync.RWMutex
	served          map[string]*servedRouteMetrics
	reuse           map[string]*reuseRouteMetrics
	durationBuckets []float64
}

func newSavingsMetrics(durationBuckets []float64) *savingsMetrics {
	return &savingsMetrics{served: map[string]*servedRouteMetrics{}, reuse: map[string]*reuseRouteMetrics{}, durationBuckets: durationBuckets}
}

func (s *savingsMetrics) servedRoute(route string) *servedRouteMetrics {
	s.mu.RLock()
	r := s.served[route]
	s.mu.RUnlock()
	if r != nil {
		return r
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r = s.served[route]; r == nil {
		r = &servedRouteMetrics{}
		for i := range r.duration {
			r.duration[i] = newHistogramWithBuckets(s.durationBuckets)
		}
		s.served[route] = r
	}
	return r
}

func (s *savingsMetrics) reuseRoute(route string) *reuseRouteMetrics {
	s.mu.RLock()
	r := s.reuse[route]
	s.mu.RUnlock()
	if r != nil {
		return r
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r = s.reuse[route]; r == nil {
		r = &reuseRouteMetrics{}
		s.reuse[route] = r
	}
	return r
}

// RecordServedRequest records one client request by how it was served: its latency, the bytes written to the
// client and the VictoriaLogs work it made itself.
func (m *Metrics) RecordServedRequest(endpoint, route string, class ServedClass, d time.Duration, clientBytes int64, vl VLWork) {
	if m == nil || m.savings == nil {
		return
	}
	if class >= servedClassCount {
		class = ServedLocal
	}
	r := m.savings.servedRoute(normalizeMetricRoute(route, endpoint))
	r.duration[class].observe(max(d, 0).Seconds())
	r.clientBytes[class].Add(max(clientBytes, 0))
	r.vl[class].add(vl)
}

// RecordVLReuse records one reused answer of a client request: by layer and tier, and, when the VictoriaLogs work
// that built it was recorded, that work as avoided. An answer whose build was not recorded on this replica (read
// from disk after a restart, from a peer, or built where nothing recorded it) counts as unknown and adds no work.
func (m *Metrics) RecordVLReuse(endpoint, route string, layer ReuseLayer, tier ReuseTier, built VLWork, recorded bool) {
	if m == nil || m.savings == nil || layer >= reuseLayerCount || tier >= reuseTierCount {
		return
	}
	r := m.savings.reuseRoute(normalizeMetricRoute(route, endpoint))
	if !recorded {
		r.unknown[layer][tier].Add(1)
		return
	}
	r.recorded[layer][tier].Add(1)
	r.avoided[layer][tier].add(built)
}

const (
	servedDurationHelp  = "Client request latency by route and how the request was served (served: cache = no VictoriaLogs call and a cache hit, shared = no VictoriaLogs call and an identical request's answer or calls in flight, partial = VictoriaLogs calls plus reused windows, stats buckets or shared calls, vl = VictoriaLogs calls and nothing reused, local = neither, error = a 4xx or 5xx answer)."
	servedBytesHelp     = "Response bytes written to clients, by route and how the request was served."
	servedVLCallsHelp   = "VictoriaLogs calls client requests made themselves, by route and how the request was served."
	servedVLSecondsHelp = "Summed time of those VictoriaLogs calls to the end of their bodies (calls in parallel add up)."
	servedVLBytesHelp   = "Decoded body bytes those VictoriaLogs calls returned."
	servedVLRowsHelp    = "Log rows those VictoriaLogs calls returned from raw queries (/select/logsql/query, one row per line); stats and metadata calls add no rows."
	reuseHelp           = "Answers client requests reused instead of asking VictoriaLogs, by route, layer (response, window, stats_extent, request_flight, held_failure, vl_call, burst), tier (l1_memory, l2_disk, l3_peer, in_flight) and cost: recorded when this replica measured the VictoriaLogs work that built the answer, unknown otherwise (restored from disk, read from a peer, stats buckets, a fused burst read). One answer counts once, at the tier that answered it."
	avoidedCallsHelp    = "VictoriaLogs calls the reused answers with a recorded cost did not make again: the calls measured when each answer was built, by route, layer and tier."
	avoidedSecondsHelp  = "VictoriaLogs time those calls took when the answers were built (summed to the end of their bodies)."
	avoidedBytesHelp    = "Decoded VictoriaLogs body bytes those calls returned when the answers were built."
	avoidedRowsHelp     = "Raw-query log rows those calls returned when the answers were built."
)

func (m *Metrics) savingsRoutes() ([]string, map[string]*servedRouteMetrics, []string, map[string]*reuseRouteMetrics) {
	s := m.savings
	s.mu.RLock()
	served := make(map[string]*servedRouteMetrics, len(s.served))
	for k, v := range s.served {
		served[k] = v
	}
	reuse := make(map[string]*reuseRouteMetrics, len(s.reuse))
	for k, v := range s.reuse {
		reuse[k] = v
	}
	s.mu.RUnlock()
	servedRoutes := make([]string, 0, len(served))
	for k := range served {
		servedRoutes = append(servedRoutes, k)
	}
	sort.Strings(servedRoutes)
	reuseRoutes := make([]string, 0, len(reuse))
	for k := range reuse {
		reuseRoutes = append(reuseRoutes, k)
	}
	sort.Strings(reuseRoutes)
	return servedRoutes, served, reuseRoutes, reuse
}

// observedCount is the number of observations, read under the histogram's lock.
func (h *histogram) observedCount() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}

func writeCounterHeader(sb *strings.Builder, name, help string) {
	sb.WriteString("# HELP " + name + " " + help + "\n# TYPE " + name + " counter\n")
}

func (m *Metrics) writeSavingsMetrics(sb *strings.Builder) {
	if m.savings == nil {
		return
	}
	servedRoutes, served, reuseRoutes, reuse := m.savingsRoutes()

	sb.WriteString("# HELP loki_vl_proxy_served_request_duration_seconds " + servedDurationHelp + "\n")
	sb.WriteString("# TYPE loki_vl_proxy_served_request_duration_seconds histogram\n")
	for _, route := range servedRoutes {
		for c := range servedClassCount {
			if s := served[route].duration[c].snapshot(); s.count > 0 {
				writeHistogramSeries(sb, "loki_vl_proxy_served_request_duration_seconds", fmt.Sprintf("route=%q,served=%q", route, servedClassNames[c]), s)
			}
		}
	}
	servedCounter := func(name, help string, value func(r *servedRouteMetrics, c ServedClass) string) {
		writeCounterHeader(sb, name, help)
		for _, route := range servedRoutes {
			for c := range servedClassCount {
				if served[route].duration[c].observedCount() > 0 {
					fmt.Fprintf(sb, "%s{route=%q,served=%q} %s\n", name, route, servedClassNames[c], value(served[route], c))
				}
			}
		}
	}
	servedCounter("loki_vl_proxy_served_response_bytes_total", servedBytesHelp, func(r *servedRouteMetrics, c ServedClass) string {
		return fmt.Sprint(r.clientBytes[c].Load())
	})
	servedCounter("loki_vl_proxy_served_vl_calls_total", servedVLCallsHelp, func(r *servedRouteMetrics, c ServedClass) string {
		return fmt.Sprint(r.vl[c].calls.Load())
	})
	servedCounter("loki_vl_proxy_served_vl_seconds_total", servedVLSecondsHelp, func(r *servedRouteMetrics, c ServedClass) string {
		return fmt.Sprintf("%g", time.Duration(r.vl[c].nanos.Load()).Seconds())
	})
	servedCounter("loki_vl_proxy_served_vl_bytes_total", servedVLBytesHelp, func(r *servedRouteMetrics, c ServedClass) string {
		return fmt.Sprint(r.vl[c].bytes.Load())
	})
	servedCounter("loki_vl_proxy_served_vl_rows_total", servedVLRowsHelp, func(r *servedRouteMetrics, c ServedClass) string {
		return fmt.Sprint(r.vl[c].rows.Load())
	})

	writeCounterHeader(sb, "loki_vl_proxy_vl_reuse_total", reuseHelp)
	for _, route := range reuseRoutes {
		r := reuse[route]
		for l := range reuseLayerCount {
			for t := range reuseTierCount {
				for _, cost := range [...]struct {
					name string
					n    int64
				}{{"recorded", r.recorded[l][t].Load()}, {"unknown", r.unknown[l][t].Load()}} {
					if cost.n > 0 {
						fmt.Fprintf(sb, "loki_vl_proxy_vl_reuse_total{route=%q,layer=%q,tier=%q,cost=%q} %d\n", route, reuseLayerNames[l], reuseTierNames[t], cost.name, cost.n)
					}
				}
			}
		}
	}
	avoided := func(name, help string, value func(w *workCounters) string) {
		writeCounterHeader(sb, name, help)
		for _, route := range reuseRoutes {
			r := reuse[route]
			for l := range reuseLayerCount {
				for t := range reuseTierCount {
					if r.recorded[l][t].Load() > 0 {
						fmt.Fprintf(sb, "%s{route=%q,layer=%q,tier=%q} %s\n", name, route, reuseLayerNames[l], reuseTierNames[t], value(&r.avoided[l][t]))
					}
				}
			}
		}
	}
	avoided("loki_vl_proxy_vl_avoided_calls_total", avoidedCallsHelp, func(w *workCounters) string { return fmt.Sprint(w.calls.Load()) })
	avoided("loki_vl_proxy_vl_avoided_seconds_total", avoidedSecondsHelp, func(w *workCounters) string {
		return fmt.Sprintf("%g", time.Duration(w.nanos.Load()).Seconds())
	})
	avoided("loki_vl_proxy_vl_avoided_bytes_total", avoidedBytesHelp, func(w *workCounters) string { return fmt.Sprint(w.bytes.Load()) })
	avoided("loki_vl_proxy_vl_avoided_rows_total", avoidedRowsHelp, func(w *workCounters) string { return fmt.Sprint(w.rows.Load()) })
}

// savingsMetrics mirrors writeSavingsMetrics' counters for OTLP (the latency histogram stays scrape-only, like
// request_phase_seconds).
func (p *OTLPPusher) savingsMetrics(now int64) []map[string]interface{} {
	if p.metrics.savings == nil {
		return nil
	}
	servedRoutes, served, reuseRoutes, reuse := p.metrics.savingsRoutes()
	var out []map[string]interface{}
	servedSum := func(name, help, unit string, value func(r *servedRouteMetrics, c ServedClass) int64) {
		var points []map[string]interface{}
		for _, route := range servedRoutes {
			for c := range servedClassCount {
				if served[route].duration[c].observedCount() > 0 {
					points = append(points, p.counterDP(name, value(served[route], c), now, attr("http.route", route), attr("served", servedClassNames[c])))
				}
			}
		}
		if len(points) > 0 {
			out = append(out, p.sumMetric(name, help, unit, points...))
		}
	}
	servedSum("loki_vl_proxy_served_requests_total", "Client requests by route and how they were served (the count of loki_vl_proxy_served_request_duration_seconds).", "{request}",
		func(r *servedRouteMetrics, c ServedClass) int64 { return r.duration[c].observedCount() })
	servedSum("loki_vl_proxy_served_response_bytes_total", servedBytesHelp, "By", func(r *servedRouteMetrics, c ServedClass) int64 { return r.clientBytes[c].Load() })
	servedSum("loki_vl_proxy_served_vl_calls_total", servedVLCallsHelp, "{call}", func(r *servedRouteMetrics, c ServedClass) int64 { return r.vl[c].calls.Load() })
	var secondPoints []map[string]interface{}
	for _, route := range servedRoutes {
		for c := range servedClassCount {
			if served[route].duration[c].observedCount() > 0 {
				secondPoints = append(secondPoints, p.gaugeDP("", time.Duration(served[route].vl[c].nanos.Load()).Seconds(), now, attr("http.route", route), attr("served", servedClassNames[c])))
			}
		}
	}
	if len(secondPoints) > 0 {
		out = append(out, p.sumMetric("loki_vl_proxy_served_vl_seconds_total", servedVLSecondsHelp, "s", secondPoints...))
	}
	servedSum("loki_vl_proxy_served_vl_bytes_total", servedVLBytesHelp, "By", func(r *servedRouteMetrics, c ServedClass) int64 { return r.vl[c].bytes.Load() })
	servedSum("loki_vl_proxy_served_vl_rows_total", servedVLRowsHelp, "{row}", func(r *servedRouteMetrics, c ServedClass) int64 { return r.vl[c].rows.Load() })

	var reusePoints []map[string]interface{}
	avoidedPoints := [4][]map[string]interface{}{}
	for _, route := range reuseRoutes {
		r := reuse[route]
		for l := range reuseLayerCount {
			for t := range reuseTierCount {
				labels := []map[string]interface{}{attr("http.route", route), attr("layer", reuseLayerNames[l]), attr("tier", reuseTierNames[t])}
				if n := r.unknown[l][t].Load(); n > 0 {
					reusePoints = append(reusePoints, p.counterDP("loki_vl_proxy_vl_reuse_total", n, now, append(labels, attr("cost", "unknown"))...))
				}
				n := r.recorded[l][t].Load()
				if n == 0 {
					continue
				}
				reusePoints = append(reusePoints, p.counterDP("loki_vl_proxy_vl_reuse_total", n, now, append(labels[:3:3], attr("cost", "recorded"))...))
				w := &r.avoided[l][t]
				avoidedPoints[0] = append(avoidedPoints[0], p.counterDP("", w.calls.Load(), now, labels...))
				avoidedPoints[1] = append(avoidedPoints[1], p.gaugeDP("", time.Duration(w.nanos.Load()).Seconds(), now, labels...))
				avoidedPoints[2] = append(avoidedPoints[2], p.counterDP("", w.bytes.Load(), now, labels...))
				avoidedPoints[3] = append(avoidedPoints[3], p.counterDP("", w.rows.Load(), now, labels...))
			}
		}
	}
	if len(reusePoints) > 0 {
		out = append(out, p.sumMetric("loki_vl_proxy_vl_reuse_total", reuseHelp, "{answer}", reusePoints...))
	}
	for i, family := range [...]struct{ name, help, unit string }{
		{"loki_vl_proxy_vl_avoided_calls_total", avoidedCallsHelp, "{call}"},
		{"loki_vl_proxy_vl_avoided_seconds_total", avoidedSecondsHelp, "s"},
		{"loki_vl_proxy_vl_avoided_bytes_total", avoidedBytesHelp, "By"},
		{"loki_vl_proxy_vl_avoided_rows_total", avoidedRowsHelp, "{row}"},
	} {
		if len(avoidedPoints[i]) > 0 {
			out = append(out, p.sumMetric(family.name, family.help, family.unit, avoidedPoints[i]...))
		}
	}
	return out
}
