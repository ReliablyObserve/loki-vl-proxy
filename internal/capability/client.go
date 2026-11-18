package capability

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientprofile"
)

// MaxClientLabelValues bounds the distinct values each label of the client request metric may take; further values
// count as OtherLabel. MaxClientSeries bounds the label combinations the same way: a header the proxy does not
// control can never grow the metric past it.
const (
	MaxClientLabelValues = 32
	MaxClientSeries      = 512
	OtherLabel           = "other"
)

// ClientSeries is one series of loki_vl_proxy_client_profile_requests_total.
type ClientSeries struct {
	Surface, Grafana, Datasource, Drilldown string
	Requests                                uint64
}

type clientKey [4]string

// clientCounts counts requests by client profile. The request path loads one immutable map (no lock, no shared
// reader counter) and bumps an atomic counter, allocating nothing; a profile not seen before takes the slow path, which
// applies the label caps, points the profile at the series it counts into and publishes a copy of the map.
type clientCounts struct {
	raw    atomic.Pointer[map[clientprofile.Profile]*atomic.Uint64] // profile -> its series counter (several share the other series)
	mu     sync.Mutex                                               // the slow path and the series
	values [3]map[string]struct{}                                   // distinct version labels seen, per label
	series map[clientKey]*atomic.Uint64
}

// maxClientProfiles bounds the distinct raw profiles remembered: beyond it a new profile counts under the surface's
// other series without being stored.
const maxClientProfiles = 4096

// ObserveClient counts one request of the client profile p, with the label cardinality bounded: at most
// MaxClientLabelValues distinct versions per label and MaxClientSeries combinations, the rest under OtherLabel.
func (l *Layer) ObserveClient(p clientprofile.Profile) {
	if l == nil {
		return // a proxy value built without New (a test) counts nothing
	}
	c := &l.clients
	if m := c.raw.Load(); m != nil {
		if n := (*m)[p]; n != nil {
			n.Add(1)
			return
		}
	}
	c.slow(p).Add(1)
}

func (c *clientCounts) slow(p clientprofile.Profile) *atomic.Uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cur map[clientprofile.Profile]*atomic.Uint64
	if m := c.raw.Load(); m != nil {
		cur = *m
	}
	if n := cur[p]; n != nil {
		return n
	}
	if c.series == nil {
		c.series = map[clientKey]*atomic.Uint64{}
		for i := range c.values {
			c.values[i] = map[string]struct{}{}
		}
	}
	key := clientKey{p.Surface.Label(), p.Grafana.Label(), p.Datasource.Label(), p.Drilldown.Label()}
	for i := 1; i < len(key); i++ { // the surface is a closed enum; the versions are not
		if _, ok := c.values[i-1][key[i]]; ok {
			continue
		}
		if len(c.values[i-1]) >= MaxClientLabelValues {
			key[i] = OtherLabel
			continue
		}
		c.values[i-1][key[i]] = struct{}{}
	}
	if _, ok := c.series[key]; !ok && len(c.series) >= MaxClientSeries {
		key = clientKey{key[0], OtherLabel, OtherLabel, OtherLabel}
	}
	n := c.series[key]
	if n == nil {
		n = new(atomic.Uint64)
		c.series[key] = n
	}
	if len(cur) < maxClientProfiles {
		next := make(map[clientprofile.Profile]*atomic.Uint64, len(cur)+1)
		for k, v := range cur {
			next[k] = v
		}
		next[p] = n
		c.raw.Store(&next)
	}
	return n
}

// ClientSeriesSnapshot lists the client series, sorted for a stable exposition.
func (l *Layer) ClientSeriesSnapshot() []ClientSeries {
	c := &l.clients
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ClientSeries, 0, len(c.series))
	for k, n := range c.series {
		out = append(out, ClientSeries{Surface: k[0], Grafana: k[1], Datasource: k[2], Drilldown: k[3], Requests: n.Load()})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.Surface+"\x00"+a.Grafana+"\x00"+a.Datasource+"\x00"+a.Drilldown < b.Surface+"\x00"+b.Grafana+"\x00"+b.Datasource+"\x00"+b.Drilldown
	})
	return out
}

// Stats is everything the metrics and the status route read, in one lock.
type Stats struct {
	Epoch      uint64
	Releases   []ReleaseView
	Gates      []GateView
	Mismatches [numGates]uint64
	// Probes counts probe results by gate and verdict (Verdict labels).
	Probes  [numGates][3]uint64
	Clients []ClientSeries
}

// Stats snapshots the counters.
func (l *Layer) Stats() Stats {
	st := l.Status()
	l.mu.Lock()
	mm, pc := l.mismatches, l.probeCount
	l.mu.Unlock()
	return Stats{Epoch: st.Epoch, Releases: st.Releases, Gates: st.Gates, Mismatches: mm, Probes: pc, Clients: l.ClientSeriesSnapshot()}
}
