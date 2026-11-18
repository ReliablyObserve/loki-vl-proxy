package metrics

import (
	"fmt"
	"strings"
	"sync"
)

// ErrorFlightStats are the request flight and error cache counters: requests
// answered from a held failure, followers handed the leader's failure, and
// failures held, by error class; followers that read the leader's success;
// and the held answers' entries and bytes.
type ErrorFlightStats struct {
	Classes   [4]string
	Held      [4]int64
	Coalesced [4]int64
	Stored    [4]int64
	Succeeded int64
	Entries   int64
	Bytes     int64
	// Rules and Predicted count the VictoriaLogs calls answered by a predicted
	// refusal instead of being sent, by rule.
	Rules     []string
	Predicted []int64
	// PeerSync counts the held VictoriaLogs refusals sent to peers and
	// received from them, by outcome (PeerSyncOutcomes).
	PeerSync [4]int64
}

// PeerSyncOutcomes name ErrorFlightStats.PeerSync: a held refusal sent to
// every peer, one not sent because the replica's send budget was spent, one
// received and held, and one received for another VictoriaLogs release,
// proxy build or configuration (ignored).
var PeerSyncOutcomes = [4]string{"sent", "throttled", "received", "ignored"}

// Saved is the requests answered without running: each would have run the
// request and its backend calls again.
func (s ErrorFlightStats) Saved() int64 {
	n := s.Succeeded
	for i := range s.Classes {
		n += s.Held[i] + s.Coalesced[i]
	}
	return n
}

type errorFlightProvider struct {
	mu sync.RWMutex
	fn func() ErrorFlightStats
}

// SetErrorFlightStatsProvider sets the source of the error flight metrics.
func (m *Metrics) SetErrorFlightStatsProvider(fn func() ErrorFlightStats) {
	if m == nil {
		return
	}
	m.errorFlights.mu.Lock()
	m.errorFlights.fn = fn
	m.errorFlights.mu.Unlock()
}

func (m *Metrics) errorFlightStats() (ErrorFlightStats, bool) {
	if m == nil {
		return ErrorFlightStats{}, false
	}
	m.errorFlights.mu.RLock()
	fn := m.errorFlights.fn
	m.errorFlights.mu.RUnlock()
	if fn == nil {
		return ErrorFlightStats{}, false
	}
	return fn(), true
}

// errorFlightFamily is one metric family of the error flights.
type errorFlightFamily struct {
	name, help string
	gauge      bool
	byClass    func(ErrorFlightStats, int) int64
	total      func(ErrorFlightStats) int64
	// labeled, when set, is the family's counts by one label.
	labeled func(ErrorFlightStats) (label string, values []string, counts []int64)
}

var errorFlightFamilies = []errorFlightFamily{
	{name: "loki_vl_proxy_error_cache_hits_total", help: "Requests answered from a held failed answer, by error class.", byClass: func(s ErrorFlightStats, i int) int64 { return s.Held[i] }},
	{name: "loki_vl_proxy_error_cache_stores_total", help: "Failed answers held by the error cache, by error class.", byClass: func(s ErrorFlightStats, i int) int64 { return s.Stored[i] }},
	{name: "loki_vl_proxy_error_coalesced_total", help: "Requests handed the failed answer of an identical request in flight instead of running, by error class.", byClass: func(s ErrorFlightStats, i int) int64 { return s.Coalesced[i] }},
	{name: "loki_vl_proxy_request_coalesced_success_total", help: "Requests that read the successful answer of an identical request in flight instead of running.", total: func(s ErrorFlightStats) int64 { return s.Succeeded }},
	{name: "loki_vl_proxy_request_flight_saved_total", help: "Requests answered without running (held failures, coalesced failures and successes); each saved its own backend calls.", total: ErrorFlightStats.Saved},
	{name: "loki_vl_proxy_error_cache_entries", help: "Failed answers held by the error cache.", gauge: true, total: func(s ErrorFlightStats) int64 { return s.Entries }},
	{name: "loki_vl_proxy_error_cache_bytes", help: "Bytes of the failed answers held by the error cache.", gauge: true, total: func(s ErrorFlightStats) int64 { return s.Bytes }},
	{name: "loki_vl_proxy_vl_refusals_predicted_total", help: "VictoriaLogs calls not sent because the running VictoriaLogs release refuses their query before reading data, by rule (the vl/capabilities registry item).", labeled: func(s ErrorFlightStats) (string, []string, []int64) {
		return "rule", s.Rules, s.Predicted
	}},
	{name: "loki_vl_proxy_error_cache_peer_sync_total", help: "Held VictoriaLogs refusals sent to every peer or received from one, by outcome.", labeled: func(s ErrorFlightStats) (string, []string, []int64) {
		return "outcome", PeerSyncOutcomes[:], s.PeerSync[:]
	}},
}

func (m *Metrics) writeErrorFlightMetrics(sb *strings.Builder) {
	s, ok := m.errorFlightStats()
	if !ok {
		return
	}
	for _, f := range errorFlightFamilies {
		kind := "counter"
		if f.gauge {
			kind = "gauge"
		}
		fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, kind)
		if f.labeled != nil {
			label, values, counts := f.labeled(s)
			for i, value := range values {
				if i < len(counts) {
					fmt.Fprintf(sb, "%s{%s=%q} %d\n", f.name, label, value, counts[i])
				}
			}
			continue
		}
		if f.byClass != nil {
			for i, class := range s.Classes {
				fmt.Fprintf(sb, "%s{class=%q} %d\n", f.name, class, f.byClass(s, i))
			}
			continue
		}
		fmt.Fprintf(sb, "%s %d\n", f.name, f.total(s))
	}
}

func (p *OTLPPusher) errorFlightMetrics(now int64) []map[string]interface{} {
	s, ok := p.metrics.errorFlightStats()
	if !ok {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(errorFlightFamilies))
	for _, f := range errorFlightFamilies {
		var points []map[string]interface{}
		if f.labeled != nil {
			label, values, counts := f.labeled(s)
			for i, value := range values {
				if i < len(counts) {
					points = append(points, p.counterDP(f.name, counts[i], now, attr(label, value)))
				}
			}
		} else if f.byClass != nil {
			for i, class := range s.Classes {
				points = append(points, p.counterDP(f.name, f.byClass(s, i), now, attr("class", class)))
			}
		} else if f.gauge {
			points = append(points, p.gaugeDP(f.name, float64(f.total(s)), now))
		} else {
			points = append(points, p.counterDP(f.name, f.total(s), now))
		}
		if f.gauge {
			out = append(out, p.gaugeMetric(f.name, f.help, "", points...))
		} else {
			out = append(out, p.sumMetric(f.name, f.help, "", points...))
		}
	}
	return out
}
