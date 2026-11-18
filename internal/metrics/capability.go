package metrics

import (
	"fmt"
	"strings"
	"sync"
)

// CapabilityStats are the capability layer's gauges and counters (internal/capability). Every label is a closed or
// bounded set: the release rows are limited by the layer's window of at most 16 releases, the gates and verdicts
// are fixed, and the client series are capped by the layer (32 values per version label, 512 combinations).
type CapabilityStats struct {
	Epoch uint64
	// Releases are the releases in the window, one series each.
	Releases []CapabilityRelease
	// Gates are every gate's state.
	Gates []CapabilityGate
	// Mismatches counts the times a probe contradicted the release, by gate.
	Mismatches []CapabilityCount
	// Probes counts probe results by gate and verdict (on, off, inconclusive).
	Probes []CapabilityProbe
	// Clients counts requests by client profile.
	Clients []CapabilityClient
}

// CapabilityRelease is one release in the window.
type CapabilityRelease struct{ Version, Flavour, Source string }

// CapabilityGate is one gate: 1 when on.
type CapabilityGate struct {
	Gate, Side, Source string
	On                 bool
}

// CapabilityCount is a count by gate.
type CapabilityCount struct {
	Gate string
	N    uint64
}

// CapabilityProbe is a probe result count.
type CapabilityProbe struct {
	Gate, Verdict string
	N             uint64
}

// CapabilityClient is a count of requests by client profile.
type CapabilityClient struct {
	Surface, Grafana, Datasource, Drilldown string
	N                                       uint64
}

type capabilityProvider struct {
	mu sync.RWMutex
	fn func() CapabilityStats
}

// SetCapabilityStatsProvider sets the source of the capability metrics.
func (m *Metrics) SetCapabilityStatsProvider(fn func() CapabilityStats) {
	if m == nil {
		return
	}
	m.capability.mu.Lock()
	m.capability.fn = fn
	m.capability.mu.Unlock()
}

func (m *Metrics) capabilityStats() (CapabilityStats, bool) {
	if m == nil {
		return CapabilityStats{}, false
	}
	m.capability.mu.RLock()
	fn := m.capability.fn
	m.capability.mu.RUnlock()
	if fn == nil {
		return CapabilityStats{}, false
	}
	return fn(), true
}

type capabilityPoint struct {
	labels [][2]string
	value  float64
}

type capabilityFamily struct {
	name, help string
	counter    bool
	points     func(CapabilityStats) []capabilityPoint
}

var capabilityFamilies = []capabilityFamily{
	{name: "loki_vl_proxy_backend_version_info", help: "The VictoriaLogs releases seen within the capability window (1 each); the lowest one decides the gates.", points: func(s CapabilityStats) []capabilityPoint {
		out := make([]capabilityPoint, 0, len(s.Releases))
		for _, r := range s.Releases {
			out = append(out, capabilityPoint{[][2]string{{"version", r.Version}, {"flavour", r.Flavour}, {"source", r.Source}}, 1})
		}
		return out
	}},
	{name: "loki_vl_proxy_capability_gate", help: "Capability gates: 1 when on, by gate, side and what decided it (version, probe, both, implied, default, mismatch).", points: func(s CapabilityStats) []capabilityPoint {
		out := make([]capabilityPoint, 0, len(s.Gates))
		for _, g := range s.Gates {
			v := 0.0
			if g.On {
				v = 1
			}
			out = append(out, capabilityPoint{[][2]string{{"gate", g.Gate}, {"side", g.Side}, {"source", g.Source}}, v})
		}
		return out
	}},
	{name: "loki_vl_proxy_capability_mismatch_total", help: "Times a feature probe contradicted the VictoriaLogs release for a gate (the observed behaviour wins).", counter: true, points: func(s CapabilityStats) []capabilityPoint {
		out := make([]capabilityPoint, 0, len(s.Mismatches))
		for _, c := range s.Mismatches {
			out = append(out, capabilityPoint{[][2]string{{"gate", c.Gate}}, float64(c.N)})
		}
		return out
	}},
	{name: "loki_vl_proxy_capability_epoch", help: "Changes of the capability gate set since the process started; the held-refusal keys and the peer sync carry the set's digest.", points: func(s CapabilityStats) []capabilityPoint {
		return []capabilityPoint{{nil, float64(s.Epoch)}}
	}},
	{name: "loki_vl_proxy_backend_feature_probe_total", help: "Feature probe results by gate and verdict (on, off, inconclusive); an inconclusive answer keeps the previous verdict.", counter: true, points: func(s CapabilityStats) []capabilityPoint {
		out := make([]capabilityPoint, 0, len(s.Probes))
		for _, c := range s.Probes {
			out = append(out, capabilityPoint{[][2]string{{"gate", c.Gate}, {"verdict", c.Verdict}}, float64(c.N)})
		}
		return out
	}},
	{name: "loki_vl_proxy_client_profile_requests_total", help: "Requests by client profile: surface and the Grafana, bundled Loki datasource and Logs Drilldown versions read from the request (unknown when absent, other beyond 32 values per label).", counter: true, points: func(s CapabilityStats) []capabilityPoint {
		out := make([]capabilityPoint, 0, len(s.Clients))
		for _, c := range s.Clients {
			out = append(out, capabilityPoint{[][2]string{{"surface", c.Surface}, {"grafana_version", c.Grafana}, {"datasource_version", c.Datasource}, {"drilldown_version", c.Drilldown}}, float64(c.N)})
		}
		return out
	}},
}

func (m *Metrics) writeCapabilityMetrics(sb *strings.Builder) {
	s, ok := m.capabilityStats()
	if !ok {
		return
	}
	for _, f := range capabilityFamilies {
		kind := "gauge"
		if f.counter {
			kind = "counter"
		}
		fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, kind)
		for _, pt := range f.points(s) {
			sb.WriteString(f.name)
			if len(pt.labels) > 0 {
				sb.WriteByte('{')
				for i, l := range pt.labels {
					if i > 0 {
						sb.WriteByte(',')
					}
					fmt.Fprintf(sb, "%s=%q", l[0], l[1])
				}
				sb.WriteByte('}')
			}
			fmt.Fprintf(sb, " %g\n", pt.value)
		}
	}
}

func (p *OTLPPusher) capabilityMetrics(now int64) []map[string]interface{} {
	s, ok := p.metrics.capabilityStats()
	if !ok {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(capabilityFamilies))
	for _, f := range capabilityFamilies {
		var points []map[string]interface{}
		for _, pt := range f.points(s) {
			attrs := make([]map[string]interface{}, 0, len(pt.labels))
			for _, l := range pt.labels {
				attrs = append(attrs, attr(l[0], l[1]))
			}
			if f.counter {
				points = append(points, p.counterDP(f.name, int64(pt.value), now, attrs...))
			} else {
				points = append(points, p.gaugeDP(f.name, pt.value, now, attrs...))
			}
		}
		if f.counter {
			out = append(out, p.sumMetric(f.name, f.help, "", points...))
		} else {
			out = append(out, p.gaugeMetric(f.name, f.help, "", points...))
		}
	}
	return out
}
