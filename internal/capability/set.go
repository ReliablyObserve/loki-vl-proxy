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
	// ReasonMismatch: the probe contradicts the release; the observed behaviour wins (a Safety gate never reads a
	// probe).
	ReasonMismatch
)

var reasonLabels = [...]string{"default", "version", "probe", "both", "mismatch"}

// String is the reason's label (the `source` label of the gate gauge).
func (r Reason) String() string { return reasonLabels[r] }

// setMeta is what a Set shares between its copies: computed once when the set is built.
type setMeta struct {
	digest string
	header string // the gates that are on, for the debug header
	why    [numGates]Reason
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
func compute(r Release, probes verdicts) Set {
	var s Set
	meta := &setMeta{}
	r.SeenAt = time.Time{}
	s.release = r
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
			on, decided = byVersion, r.Known
			why = ReasonDefault
			if r.Known {
				why = ReasonVersion
			}
		case probe != VerdictNone:
			on, decided = probe == VerdictOn, true
			switch {
			case !r.Known:
				why = ReasonProbe
			case on == byVersion:
				why = ReasonBoth
			default:
				why = ReasonMismatch
			}
		case r.Known:
			on, decided, why = byVersion, true, ReasonVersion
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
	s.meta = meta
	return s
}

// ForRelease is the gate set of a release alone: no probes, no window. The translator's tests and
// logsql.CapabilitiesFor read the gates of a named release through it.
func ForRelease(r Release) Set { return compute(r, verdicts{}) }

// For returns the backend set and the client gates of the request: the profile ctx carries rides on the copy, and
// no client gate is stored globally.
func (l *Layer) For(ctx context.Context) Set {
	s := *l.cur.Load()
	s.client = clientprofile.From(ctx)
	return s
}

// Backend is the current backend set: one atomic load, no lock, no allocation.
func (l *Layer) Backend() Set { return *l.cur.Load() }
