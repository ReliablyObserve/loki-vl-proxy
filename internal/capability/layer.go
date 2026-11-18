// Package capability is the one place that decides which optimisations and behaviours the proxy uses: the
// VictoriaLogs release(s) behind it (and what probes observed of them), and, as hooks, the Loki compatibility
// target and the client profile of a request. Handlers, the lowering and the refusal prediction ask a Set for a
// typed gate; nothing else compares release strings.
//
// A Set is an immutable snapshot, rebuilt in one place when an input changes, and the Layer publishes it with one
// atomic pointer, so reading it on the request path takes no lock and allocates nothing.
package capability

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultWindow is how long a release counts after it was last seen.
const DefaultWindow = 10 * time.Minute

// maxReleases bounds the releases the layer remembers (and so the series of the version metric): the values come
// from responses the proxy does not control.
const maxReleases = 16

// Options configure a Layer.
type Options struct {
	// Window is how long a release counts after it was last seen; zero is DefaultWindow.
	Window time.Duration
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// observed is a release seen on a backend. Its last-seen time is atomic so the repeat of a response header refreshes
// it without the layer's lock; detached marks an entry that left the window or was evicted, so a repeat of its text
// takes the slow path and is counted again.
type observed struct {
	rel        Release
	backend    string
	firstSeen  time.Time
	lastSeenNs atomic.Int64
	detached   atomic.Bool
}

func (o *observed) lastSeen() time.Time { return time.Unix(0, o.lastSeenNs.Load()) }

// Layer decides the gate set from the releases and probe verdicts it is told about.
type Layer struct {
	window time.Duration
	now    func() time.Time

	cur atomic.Pointer[Set]
	// fast is the last release text observed, so a response that repeats it costs one string compare.
	fast atomic.Pointer[fastObs]

	mu         sync.Mutex
	seen       []*observed
	probes     verdicts
	probeAt    [numGates]time.Time
	probeCount [numGates][3]uint64 // by Verdict
	mismatch   [numGates]bool
	mismatches [numGates]uint64
	epoch      uint64
	changes    uint64
	subs       []func(old, now Set)
	lokiTarget string
	clients    clientCounts // its own lock: the request path counts clients
}

type fastObs struct {
	raw     string
	src     Source
	backend string
	entry   *observed
}

// New builds a Layer with no release known: every post-floor gate is off, every pre-floor gate holds its default.
func New(o Options) *Layer {
	l := &Layer{window: o.Window, now: o.Now}
	if l.window <= 0 {
		l.window = DefaultWindow
	}
	if l.now == nil {
		l.now = time.Now
	}
	s := compute(Release{}, verdicts{})
	l.cur.Store(&s)
	return l
}

// SetWindow widens the window to at least w (the proxy keeps it above two re-probe intervals, so a release is not
// forgotten between two probes).
func (l *Layer) SetWindow(w time.Duration) {
	l.mu.Lock()
	if w > l.window {
		l.window = w
	}
	l.mu.Unlock()
}

// Subscribe registers a function called, outside the layer's lock and in order, after every change of the set that
// moves the digest (old, then new). The proxy's cache epoch, log line and metrics hang off it.
func (l *Layer) Subscribe(fn func(old, now Set)) {
	l.mu.Lock()
	l.subs = append(l.subs, fn)
	l.mu.Unlock()
}

// SetLokiTarget records the Loki compatibility target the status reports (a hook: no gate reads it yet).
func (l *Layer) SetLokiTarget(v string) {
	l.mu.Lock()
	l.lokiTarget = v
	l.mu.Unlock()
}

// ObserveRaw reads what a backend says about itself and counts it when it names VictoriaLogs. It reports whether the
// text was a VictoriaLogs release. A text equal to the last one costs one compare: no lock, no allocation.
func (l *Layer) ObserveRaw(backend, raw string, src Source) bool {
	if f := l.fast.Load(); f != nil && f.raw == raw && f.src == src && f.backend == backend && !f.entry.detached.Load() {
		f.entry.lastSeenNs.Store(l.now().UnixNano())
		return true
	}
	rel, ok := ParseBuild(raw, src)
	if !ok {
		return false
	}
	if e := l.observe(backend, rel); e != nil {
		l.fast.Store(&fastObs{raw: raw, src: src, backend: backend, entry: e})
	}
	return true
}

// Observe records that backend runs release r now. The effective release is the lowest one seen in the window; a
// release leaves the window only when a newer observation exists, so a backend that goes silent (an outage, a
// disabled re-probe) never empties it.
func (l *Layer) Observe(backend string, r Release) { l.observe(backend, r) }

// observe records the release and returns its entry (nil when r is not a release).
func (l *Layer) observe(backend string, r Release) *observed {
	if !r.Known {
		return nil
	}
	now := l.now()
	r.SeenAt = now
	l.mu.Lock()
	var entry *observed
	for _, o := range l.seen {
		if o.backend == backend && o.rel.Compare(r) == 0 {
			entry = o
			break
		}
	}
	if entry != nil {
		entry.lastSeenNs.Store(now.UnixNano())
	} else {
		entry = &observed{rel: r, backend: backend, firstSeen: now}
		entry.lastSeenNs.Store(now.UnixNano())
		l.seen = append(l.seen, entry)
		if len(l.seen) > maxReleases {
			oldest := 0
			for i := range l.seen {
				if l.seen[i].lastSeenNs.Load() < l.seen[oldest].lastSeenNs.Load() {
					oldest = i
				}
			}
			l.seen[oldest].detached.Store(true)
			l.seen = append(l.seen[:oldest], l.seen[oldest+1:]...)
		}
	}
	l.pruneLocked(now)
	old, nw, changed := l.recomputeLocked()
	subs := l.subs
	l.mu.Unlock()
	if changed {
		notify(subs, old, nw)
	}
	return entry
}

// Prune drops the releases that left the window (see Observe) and republishes the set if that moved it. The
// proxy's re-probe loop calls it every tick.
func (l *Layer) Prune() {
	l.mu.Lock()
	l.pruneLocked(l.now())
	old, nw, changed := l.recomputeLocked()
	subs := l.subs
	l.mu.Unlock()
	if changed {
		notify(subs, old, nw)
	}
}

// pruneLocked removes releases last seen before the window, unless every release is that old (nothing newer says
// the old ones are gone).
func (l *Layer) pruneLocked(now time.Time) {
	cutoff := now.Add(-l.window).UnixNano()
	fresh := false
	for _, o := range l.seen {
		if o.lastSeenNs.Load() >= cutoff {
			fresh = true
			break
		}
	}
	if !fresh {
		return
	}
	kept := l.seen[:0]
	for _, o := range l.seen {
		if o.lastSeenNs.Load() >= cutoff {
			kept = append(kept, o)
		} else {
			o.detached.Store(true)
		}
	}
	l.seen = kept
}

// effectiveLocked is the lowest release seen in the window.
func (l *Layer) effectiveLocked() Release {
	var eff Release
	for i, o := range l.seen {
		if i == 0 || o.rel.Compare(eff) < 0 {
			eff = o.rel
		}
	}
	return eff
}

// ObserveProbe records a feature probe's verdict for a gate. A VerdictNone keeps the previous verdict (it is
// still counted): an outage never flips a gate.
func (l *Layer) ObserveProbe(id GateID, v Verdict) {
	if id >= numGates || gates[id].Probe == nil || gates[id].Kind == Safety {
		return
	}
	l.mu.Lock()
	l.probeCount[id][v]++
	l.probeAt[id] = l.now()
	if v != VerdictNone {
		l.probes[id] = v
	}
	old, nw, changed := l.recomputeLocked()
	subs := l.subs
	l.mu.Unlock()
	if changed {
		notify(subs, old, nw)
	}
}

// recomputeLocked rebuilds the set from the releases and verdicts and publishes it. The epoch moves only when the
// digest does, so a mixed pool under the lowest-in-window rule, or a probe that repeats its verdict, does not flap.
func (l *Layer) recomputeLocked() (old, nw Set, changed bool) {
	cur := *l.cur.Load()
	nw = compute(l.effectiveLocked(), l.probes)
	for i := range gates {
		mm := nw.meta.why[i] == ReasonMismatch
		if mm && !l.mismatch[i] {
			l.mismatches[i]++
		}
		l.mismatch[i] = mm
	}
	if nw.meta.digest == cur.meta.digest {
		nw.epoch = cur.epoch
		if nw.meta.why != cur.meta.why {
			l.cur.Store(&nw) // the reasons moved (a probe agreed with the release): no gate did
		}
		return cur, nw, false
	}
	l.epoch++
	l.changes++
	nw.epoch = l.epoch
	l.cur.Store(&nw)
	return cur, nw, true
}

func notify(subs []func(old, now Set), old, nw Set) {
	for _, fn := range subs {
		fn(old, nw)
	}
}

// Flipped lists the gates whose state differs between two sets, with their new state, in table order.
func Flipped(old, nw Set) []GateID {
	var out []GateID
	for i := GateID(0); i < numGates; i++ {
		if old.On(i) != nw.On(i) || old.Decided(i) != nw.Decided(i) {
			out = append(out, i)
		}
	}
	return out
}

// Mismatches lists the gates whose probe contradicts the release right now.
func (s Set) Mismatches() []GateID {
	var out []GateID
	for i := GateID(0); i < numGates; i++ {
		if s.Why(i) == ReasonMismatch {
			out = append(out, i)
		}
	}
	return out
}

// ReleaseView is one release of the window as the status route shows it.
type ReleaseView struct {
	Raw       string    `json:"raw"`
	Release   string    `json:"release"`
	Flavour   string    `json:"flavour"`
	Source    string    `json:"source"`
	Backend   string    `json:"backend"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	InWindow  bool      `json:"in_window"`
}

// backendID names a backend in the status without showing its address: a short hash of it.
func backendID(backend string) string {
	sum := sha256.Sum256([]byte(backend))
	return "backend-" + hex.EncodeToString(sum[:3])
}

// GateView is one gate as the status route shows it.
type GateView struct {
	Name         string    `json:"name"`
	Side         string    `json:"side"`
	Kind         string    `json:"kind"`
	On           bool      `json:"on"`
	Decided      bool      `json:"decided"`
	Source       string    `json:"source"`
	From         string    `json:"from"`
	Registry     string    `json:"registry"`
	LastProbe    time.Time `json:"last_probe,omitzero"`
	ProbeVerdict string    `json:"probe_verdict,omitempty"`
}

// Status is the state of the layer for the status route.
type Status struct {
	Epoch      uint64        `json:"epoch"`
	Digest     string        `json:"digest"`
	Effective  string        `json:"effective_release"`
	Known      bool          `json:"effective_release_known"`
	Changes    uint64        `json:"changes"`
	LokiTarget string        `json:"loki_target,omitempty"`
	Releases   []ReleaseView `json:"releases"`
	Gates      []GateView    `json:"gates"`
	Mismatches []string      `json:"mismatches"`
	// Probing is whether feature probes run (set by the caller).
	Probing bool `json:"feature_probes"`
}

// Status snapshots the layer.
func (l *Layer) Status() Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cur := *l.cur.Load()
	st := Status{Epoch: cur.epoch, Digest: cur.Digest(), Effective: cur.release.String(), Known: cur.release.Known, Changes: l.changes, LokiTarget: l.lokiTarget,
		Releases: make([]ReleaseView, 0, len(l.seen)), Gates: make([]GateView, 0, numGates), Mismatches: []string{}}
	for _, o := range l.seen {
		st.Releases = append(st.Releases, ReleaseView{Raw: o.rel.Raw, Release: o.rel.String(), Flavour: o.rel.Flavour.String(), Source: o.rel.Source.String(),
			Backend: backendID(o.backend), FirstSeen: o.firstSeen, LastSeen: o.lastSeen(), InWindow: !o.lastSeen().Before(now.Add(-l.window))})
	}
	sort.Slice(st.Releases, func(i, j int) bool { return st.Releases[i].FirstSeen.Before(st.Releases[j].FirstSeen) })
	for i := range gates {
		g := &gates[i]
		id := GateID(i)
		gv := GateView{Name: g.Name, Side: g.Side.String(), Kind: g.Kind.String(), On: cur.On(id), Decided: cur.Decided(id), Source: cur.Why(id).String(),
			From: g.FromString(), Registry: g.Registry, LastProbe: l.probeAt[i]}
		if l.probes[i] != VerdictNone {
			gv.ProbeVerdict = l.probes[i].String()
		}
		st.Gates = append(st.Gates, gv)
		if cur.Why(id) == ReasonMismatch {
			st.Mismatches = append(st.Mismatches, g.Name)
		}
	}
	return st
}
