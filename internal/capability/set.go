package capability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientprofile"
)

// Reason says what decided a gate.
type Reason uint8

const (
	// ReasonDefault: nothing is known, the gate holds its default.
	ReasonDefault Reason = iota
	// ReasonVersion: the effective release decided it.
	ReasonVersion
	// ReasonProbe: a feature probe decided it (no release is known, or the gate has no release rule that applies).
	ReasonProbe
	// ReasonBoth: the release and the probe agree.
	ReasonBoth
	// ReasonMismatch: a probe found the feature absent that the release carries (a member lacks it); the gate is off.
	ReasonMismatch
	// ReasonImplied: no release is known, and a probe found a feature of a later release present, so a capability
	// of that release or an earlier one is present too (a release never loses a feature a previous one had).
	ReasonImplied
	// ReasonNamed: a release the operator named (-backend-version-url, -backend-version) decided it: on because a
	// probe confirmed a feature of the gate's release or a later one, or off because the named release is older.
	ReasonNamed
)

var reasonLabels = [...]string{"default", "version", "probe", "both", "mismatch", "implied", "named"}

// String is the reason's label (the `source` label of the gate gauge).
func (r Reason) String() string { return reasonLabels[r] }

// setMeta is what a Set shares between its copies: computed once when the set is built.
type setMeta struct {
	digest string
	header string // the gates that are on, for the debug header
	why    [numGates]Reason
	// observed: a release was read, from the backend or the version URL (not only declared by the operator).
	observed bool
	// behind is the named release a probe shows the backend to be newer than ("" when none).
	behind string
}

// Set is an immutable snapshot of every gate. It is a small value: copying it allocates nothing.
type Set struct {
	epoch   uint64
	on      uint32
	decided uint32
	release Release
	meta    *setMeta
	client  clientprofile.Profile
}

// On reports whether gate id is on. An undecided Capability or Behaviour gate holds its default (off after the
// floor); a Safety gate is on only when the release is known to carry the fix.
func (s Set) On(id GateID) bool { return id < numGates && s.on&(1<<id) != 0 }

// Decided reports whether the release or a probe decided the gate, rather than its default.
func (s Set) Decided(id GateID) bool { return id < numGates && s.decided&(1<<id) != 0 }

// Release is the effective release: the lowest release seen in the window. Known is false until one was read.
func (s Set) Release() Release { return s.release }

// Observed reports whether the effective release set holds a release read from the backend or from
// -backend-version-url, rather than only the one the operator declared.
func (s Set) Observed() bool { return s.meta != nil && s.meta.observed }

// Epoch counts the changes of the backend gates of this process: it moves when the digest does.
func (s Set) Epoch() uint64 { return s.epoch }

// Digest is a content hash of the effective release, its flavour and every gate's on and decided bits. It is
// stable across the fleet (replicas that see the same backend compute the same one), so cross-replica keys carry it
// and a peer's entry computed under another gate set is a miss. A counter would never match another replica's.
func (s Set) Digest() string {
	if s.meta == nil {
		return emptyDigest
	}
	return s.meta.digest
}

// Why is what decided the gate.
func (s Set) Why(id GateID) Reason {
	if id >= numGates || s.meta == nil {
		return ReasonDefault
	}
	return s.meta.why[id]
}

// Client is the client profile of the request the set was built for (the zero Profile outside Layer.For).
func (s Set) Client() clientprofile.Profile { return s.client }

// HeaderValue is the debug header's text: the epoch, the digest and the gates that are on.
func (s Set) HeaderValue() string {
	if s.meta == nil {
		return ""
	}
	return "epoch=" + itoa(s.epoch) + "; digest=" + s.meta.digest + "; release=" + s.release.String() + "; on=" + s.meta.header
}

// Profile is the capability profile of the backend as the drilldown-limits answer names it. It is the one place
// that reads the release in terms of the supported lines.
func (s Set) Profile() string {
	r := s.release
	switch {
	case r.Known && s.On(DensePatternWindowing):
		return "vl-v1.50-plus"
	case r.Known && cmp3(r.Major, r.Minor, r.Patch, [3]int{1, 49, 0}) >= 0:
		return "vl-v1.49-plus"
	case r.Known && s.On(StreamMetadataEndpoints):
		return "vl-v1.30-plus"
	case r.Known:
		return "legacy-pre-v1.30"
	case s.Decided(StreamMetadataEndpoints) && s.On(StreamMetadataEndpoints):
		return "vl-probed-v1.49-plus"
	case s.Decided(StreamMetadataEndpoints):
		return "legacy-probed-pre-v1.30"
	}
	return ""
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

var emptyDigest = compute(Release{}, verdicts{}).meta.digest

func computeDigest(r Release, on, decided uint32) string {
	h := sha256.New()
	h.Write([]byte(r.String()))
	h.Write([]byte{0, byte(on), byte(on >> 8), byte(on >> 16), byte(on >> 24), byte(decided), byte(decided >> 8), byte(decided >> 16), byte(decided >> 24)})
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// verdicts are the probes' standing answers, by gate.
type verdicts [numGates]Verdict

// compute decides every gate from the effective release and the probes' verdicts.
//
// Evidence is read as the lowest member of the backend would give it: a probe may turn a gate off below what the
// release allows (a member that lacks the feature answered) but never on above it (a newer member answered); with
// no release known, a gate is on only when every probe answer of the window said so.
func compute(r Release, probes verdicts) Set {
	var s Set
	meta := &setMeta{}
	r.SeenAt = time.Time{}
	s.release = r
	floor, haveFloor := impliedFloor(r, probes)
	var names []string
	for i := range gates {
		g := &gates[i]
		id := GateID(i)
		byVersion := r.Known && g.In(r)
		var probe Verdict
		if g.Probe != nil && g.Kind != Safety {
			probe = probes[id]
		}
		var on, decided bool
		var why Reason
		switch {
		case g.Kind == Safety:
			on, decided = safetyHolds(g, r, byVersion, probes), r.Known
			why = ReasonDefault
			if r.Known {
				why = ReasonVersion
			}
		case r.Known && probe == VerdictOff && byVersion:
			// A member lacks what the release carries (an older member of the pool, a patched build): off.
			on, decided, why = false, true, ReasonMismatch
		case r.Known && probe == VerdictOff:
			on, decided, why = false, true, ReasonBoth
		case r.Known && probe == VerdictOn && byVersion:
			on, decided, why = true, true, ReasonBoth
		case r.Known:
			// No verdict, or a newer member answered above the lowest release: the release decides.
			on, decided, why = byVersion, true, ReasonVersion
		case probe != VerdictNone:
			on, decided, why = probe == VerdictOn, true, ReasonProbe
		case haveFloor && !g.RequiresKnownRelease && g.Kind == Capability && g.Flavours == 0 && cmp3(g.From[0], g.From[1], g.From[2], floor) <= 0:
			on, decided, why = true, true, ReasonImplied
		default:
			on, decided, why = g.Default, false, ReasonDefault
		}
		if on {
			s.on |= 1 << id
			names = append(names, g.Name)
		}
		if decided {
			s.decided |= 1 << id
		}
		meta.why[id] = why
	}
	meta.digest = computeDigest(r, s.on, s.decided)
	meta.header = strings.Join(names, ",")
	meta.observed = r.Known
	s.meta = meta
	return s
}

// contradicts lists the probed gates release r carries that a probe found absent: evidence that the queried backend
// is older than r, so a release the operator named (-backend-version, -backend-version-url) is not used.
func contradicts(r Release, probes verdicts) []GateID {
	var out []GateID
	for i := range gates {
		g := &gates[i]
		if r.Known && g.Probe != nil && g.Kind != Safety && probes[i] == VerdictOff && g.In(r) {
			out = append(out, GateID(i))
		}
	}
	return out
}

// confirmed reports whether the probes prove the queried backend is at least as new as g: a probe found present a
// feature of g's release or a later one, and none found absent a feature of g's release or an earlier one.
func confirmed(g *Gate, probes verdicts) bool {
	proven := false
	for i := range gates {
		o := &gates[i]
		if o.Probe == nil || o.Kind == Safety {
			continue
		}
		switch c := cmp3(o.From[0], o.From[1], o.From[2], g.From); {
		// A feature back-ported to an older line, or in some builds only, says nothing of the releases between.
		case probes[i] == VerdictOn && c >= 0 && len(o.Lines) == 0 && o.Flavours == 0:
			proven = true
		case probes[i] == VerdictOff && c <= 0:
			return false
		}
	}
	return proven
}

// decide is compute for the releases the layer holds: observed is the lowest release read from the backend (not
// Known when none was), fromURL the lowest one read from -backend-version-url that no probe contradicts, declared
// the operator's -backend-version, and rejected the gates whose probes contradicted a version URL release that was
// therefore left out.
//
// A named release (the version URL or -backend-version) is a claim about the backend, not its own answer: the URL
// may name another server. It never turns a gate on unless a probe confirms it (confirmed), it turns off a gate
// newer than itself (the lowest release wins, and what it turns off is left undecided so nothing is predicted from
// it), and a probe that contradicts it discards it.
func decide(observed, fromURL, declared Release, probes verdicts, rejected []GateID) Set {
	base := compute(observed, probes)
	if declared.Known {
		if c := contradicts(declared, probes); len(c) > 0 {
			rejected = append(rejected, c...)
			declared = Release{}
		}
	}
	named := fromURL
	if declared.Known && (!named.Known || declared.Compare(named) < 0) {
		named = declared
	}
	if !named.Known && len(rejected) == 0 {
		return base
	}
	s, meta := base, *base.meta
	meta.observed = observed.Known || fromURL.Known
	if named.Known {
		for i := range gates {
			g := &gates[i]
			bit := uint32(1) << i
			if !observed.Known && probes[i] == VerdictOn && !g.In(named) {
				meta.behind = named.String()
			}
			switch {
			case !g.In(named):
				if s.on&bit != 0 && !g.Default {
					s.on &^= bit
					s.decided &^= bit
					meta.why[i] = ReasonNamed
				}
			case !observed.Known && s.on&bit == 0 && confirmed(g, probes) && (g.Kind != Safety || named.Flavour != FlavourUnknown):
				s.on |= bit
				s.decided |= bit
				meta.why[i] = ReasonNamed
			}
		}
		if !s.release.Known || named.Compare(s.release) < 0 {
			named.SeenAt = time.Time{}
			s.release = named
		}
	}
	for _, id := range rejected {
		meta.why[id] = ReasonMismatch
	}
	var names []string
	for i := range gates {
		if s.on&(1<<i) != 0 {
			names = append(names, gates[i].Name)
		}
	}
	meta.digest = computeDigest(s.release, s.on, s.decided)
	meta.header = strings.Join(names, ",")
	s.meta = &meta
	return s
}

// impliedFloor is the release a probe's answers bound the backend from below when no release is known: the newest
// feature found present, if the answers are consistent with one release.
func impliedFloor(r Release, probes verdicts) (floor [3]int, haveFloor bool) {
	if r.Known {
		return floor, false
	}
	// Without a known release, a probe that finds a feature present bounds the release from below: every capability
	// the release's line had by then is present too (dense pattern windowing from a backend that answers coalesce).
	// That holds for one release only, so it is used when the verdicts are consistent with one (every feature found
	// present is no newer than every feature found absent); any contradiction means a pool the probes cannot tell
	// apart, and nothing is implied.
	var minOff [3]int
	haveOff := false
	for i := range gates {
		g := &gates[i]
		if g.Kind == Safety || g.Probe == nil || g.Flavours != 0 {
			continue
		}
		switch probes[i] {
		case VerdictOn:
			if !haveFloor || cmp3(g.From[0], g.From[1], g.From[2], floor) > 0 {
				floor, haveFloor = g.From, true
			}
		case VerdictOff:
			if !haveOff || cmp3(g.From[0], g.From[1], g.From[2], minOff) < 0 {
				minOff, haveOff = g.From, true
			}
		}
	}
	if haveFloor && haveOff && cmp3(floor[0], floor[1], floor[2], minOff) >= 0 {
		return [3]int{}, false
	}
	return floor, haveFloor
}

// safetyHolds reports whether a Safety gate is on (the release carries the fix): only for a known release of a
// recognised flavour, and never when a probe found absent a feature of a release no newer than the gate's own (some
// member of the backend is older than the fix).
func safetyHolds(g *Gate, r Release, byVersion bool, probes verdicts) bool {
	if !byVersion || !r.Known || r.Flavour == FlavourUnknown {
		return false
	}
	for i := range gates {
		o := &gates[i]
		if o.Kind != Safety && o.Probe != nil && probes[i] == VerdictOff && cmp3(o.From[0], o.From[1], o.From[2], g.From) <= 0 {
			return false
		}
	}
	return true
}

// ForRelease is the gate set of a release alone: no probes, no window. The translator's tests and
// logsql.CapabilitiesFor read the gates of a named release through it.
func ForRelease(r Release) Set { return compute(r, verdicts{}) }

// For returns the backend set and the client gates of the request: the profile ctx carries rides on the copy, and
// no client gate is stored globally.
func (l *Layer) For(ctx context.Context) Set {
	s := l.Backend()
	s.client = clientprofile.From(ctx)
	return s
}

// Backend is the current backend set: one atomic load, no lock, no allocation.
func (l *Layer) Backend() Set {
	if l == nil {
		return unknownSet // a Proxy value built without New (a test): nothing is known
	}
	return *l.cur.Load()
}

var unknownSet = compute(Release{}, verdicts{})
