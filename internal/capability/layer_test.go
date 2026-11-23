package capability

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientprofile"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestLayer(c *fakeClock) *Layer { return New(Options{Now: c.Now}) }

func rel(s string) Release { return ParseRelease(s) }

// conformance: vl-capability-layer
func TestLayerStartsUnknownAtTheFloor(t *testing.T) {
	l := newTestLayer(newClock())
	s := l.Backend()
	if s.Release().Known || s.Epoch() != 0 || s.On(PipeCoalesce) || s.Decided(PipeCoalesce) || !s.On(StreamMetadataEndpoints) || s.On(MathQuotedConstantClone) {
		t.Fatalf("unknown release: %s %+v", s.HeaderValue(), s)
	}
	if s.Digest() != emptyDigest || s.Profile() != "" {
		t.Fatalf("digest %s profile %q", s.Digest(), s.Profile())
	}
}

// Two backends of a pool on different releases: the lowest decides, and the set neither flaps nor moves the digest
// while both keep answering.
//
// conformance: vl-capability-layer
func TestLowestReleaseInTheWindowDecidesWithoutFlapping(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	var changes int
	l.Subscribe(func(old, now Set) { changes++ })
	for i := 0; i < 200; i++ {
		l.Observe("a:9428", rel("v1.54.0"))
		l.Observe("b:9428", rel("v1.51.1"))
		clk.Advance(time.Second)
	}
	s := l.Backend()
	if s.Release().String() != "v1.51.1" || !s.On(PipeCoalesce) || s.On(ExtraFiltersLen) || s.On(MathQuotedConstantClone) {
		t.Fatalf("lowest of the window must decide: %s", s.HeaderValue())
	}
	// First sight of a.9428 at v1.54.0 sets the set, the second backend lowers it: two changes, then none.
	if changes != 2 || s.Epoch() != 2 {
		t.Fatalf("changes %d epoch %d: a mixed pool must not flap", changes, s.Epoch())
	}
	digest := s.Digest()
	for i := 0; i < 50; i++ {
		l.Observe("b:9428", rel("v1.51.1"))
		l.Observe("a:9428", rel("v1.54.0"))
		clk.Advance(time.Second)
		if l.Backend().Digest() != digest {
			t.Fatal("digest flapped")
		}
	}
}

// An upgrade and a rollback flip the gates, move the digest and the epoch, and the digest of a state is the same
// every time it is reached (replicas of one backend agree). A cache or peer key built with the old digest differs.
//
// conformance: vl-capability-layer
func TestUpgradeAndRollbackMoveTheDigest(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	window := l.window
	var flips [][]GateID
	l.Subscribe(func(old, now Set) { flips = append(flips, Flipped(old, now)) })

	l.Observe("vl:9428", rel("v1.53.0"))
	old := l.Backend()
	if old.On(ExtraFiltersLen) || !old.On(RangeTimeWritersRefused) {
		t.Fatal("v1.53.0 gates")
	}
	// Upgrade: v1.54.0 answers, v1.53.0 is not seen again and leaves the window.
	clk.Advance(window / 2)
	l.Observe("vl:9428", rel("v1.54.0"))
	if l.Backend().Release().String() != "v1.53.0" {
		t.Fatal("until the old release leaves the window the lowest still decides")
	}
	clk.Advance(window)
	l.Observe("vl:9428", rel("v1.54.0"))
	up := l.Backend()
	if up.Release().String() != "v1.54.0" || !up.On(ExtraFiltersLen) || up.Digest() == old.Digest() || up.Epoch() <= old.Epoch() {
		t.Fatalf("upgrade: %s vs %s", up.HeaderValue(), old.HeaderValue())
	}
	// Rollback: the old release answers again and is the lowest at once.
	l.Observe("vl:9428", rel("v1.53.0"))
	down := l.Backend()
	if down.Release().String() != "v1.53.0" || down.On(ExtraFiltersLen) || down.Digest() != old.Digest() || down.Epoch() <= up.Epoch() {
		t.Fatalf("rollback: %s", down.HeaderValue())
	}
	if len(flips) != 3 || len(flips[1]) != 1 || flips[1][0] != ExtraFiltersLen {
		t.Fatalf("flips %v", flips)
	}
	// What a key built under the old digest does after the upgrade: it differs, so the entry is a miss.
	key := func(s Set) string { return "flight:v1:" + s.Digest() + ":GET:/q" }
	if key(old) == key(up) || key(old) != key(down) {
		t.Fatal("keys follow the digest")
	}
}

// An outage never flips a gate or moves the digest: a probe round that gets no answer says nothing, and a backend
// that goes silent keeps the release it last showed, past the window.
//
// conformance: vl-capability-layer
func TestOutageChangesNothing(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	l.Observe("vl:9428", rel("v1.54.0"))
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	before := l.Backend()
	var changed int
	l.Subscribe(func(Set, Set) { changed++ })
	for i := 0; i < 5; i++ {
		clk.Advance(l.window) // longer than the window, again and again
		for _, g := range Gates() {
			if g.Probe != nil {
				l.ObserveProbe(g.ID, VerdictNone) // timeouts, 5xx, 401, an open breaker
			}
		}
		l.Prune()
	}
	after := l.Backend()
	if changed != 0 || after.Digest() != before.Digest() || after.Epoch() != before.Epoch() || after.Release().String() != "v1.54.0" || !after.On(PipeCoalesce) {
		t.Fatalf("an outage moved the set: %s -> %s (%d changes)", before.HeaderValue(), after.HeaderValue(), changed)
	}
	if st := l.Stats(); st.Probes[PipeCoalesce][VerdictNone] != 5 {
		t.Fatalf("inconclusive probes are counted: %+v", st.Probes[PipeCoalesce])
	}
}

// With no release readable, probes decide the gates they cover; the rest holds the floor.
//
// conformance: vl-capability-layer
func TestUnknownReleaseProbesDecide(t *testing.T) {
	l := newTestLayer(newClock())
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	l.ObserveProbe(StatsLimitAfterStats, VerdictOn)
	l.ObserveProbe(RangeTimeWritersRefused, VerdictOff)
	l.ObserveProbe(StreamMetadataEndpoints, VerdictOn)
	s := l.Backend()
	if s.Release().Known || !s.On(PipeCoalesce) || !s.Decided(PipeCoalesce) || s.Why(PipeCoalesce) != ReasonProbe ||
		s.On(RangeTimeWritersRefused) || !s.Decided(RangeTimeWritersRefused) {
		t.Fatalf("probes decide: %s", s.HeaderValue())
	}
	// A coalesce answered with the v1.53 shape accepted means v1.51 or v1.52: the capabilities of v1.50 follow
	// (implied), the v1.52 ones do not. A Safety gate and an undecided Behaviour gate stay as they were.
	if !s.On(DensePatternWindowing) || s.Why(DensePatternWindowing) != ReasonImplied || !s.On(ColumnFieldValues) || s.On(PipeJSONArrayConcat) || s.Decided(PipeJSONArrayConcat) ||
		s.Decided(ExtraFiltersLen) || s.On(MathQuotedConstantClone) || s.Decided(MathQuotedConstantClone) {
		t.Fatalf("implied and undecided gates: %s", s.HeaderValue())
	}
	if s.Profile() != "vl-probed-v1.49-plus" {
		t.Fatalf("profile %q", s.Profile())
	}
	// A probe that finds nothing present implies nothing.
	l3 := newTestLayer(newClock())
	l3.ObserveProbe(PipeCoalesce, VerdictOff)
	l3.ObserveProbe(StatsLimitAfterStats, VerdictNone)
	if s3 := l3.Backend(); s3.On(DensePatternWindowing) || s3.Decided(DensePatternWindowing) || s3.Why(DensePatternWindowing) != ReasonDefault {
		t.Fatalf("a refused probe implied a capability: %s", s3.HeaderValue())
	}
	// Contradicting answers (a pool the probes cannot tell apart: coalesce present, limit-after-stats absent) imply
	// nothing, and the absent answer wins for its own gate.
	l5 := newTestLayer(newClock())
	l5.ObserveProbe(PipeCoalesce, VerdictOn)
	l5.ObserveProbe(StatsLimitAfterStats, VerdictOff)
	if s5 := l5.Backend(); s5.On(DensePatternWindowing) || s5.Why(DensePatternWindowing) != ReasonDefault || s5.On(StatsLimitAfterStats) {
		t.Fatalf("contradicting answers implied a capability: %s", s5.HeaderValue())
	}
	// A known release is never overridden by an implication.
	l4 := newTestLayer(newClock())
	l4.Observe("a", rel("v1.49.0"))
	l4.ObserveProbe(PipeCoalesce, VerdictOn)
	if l4.Backend().On(DensePatternWindowing) || l4.Backend().On(PipeCoalesce) {
		t.Fatal("v1.49.0 has neither dense windowing nor coalesce")
	}
	// A probe cannot decide a Safety gate or a gate without a probe.
	l.ObserveProbe(MathQuotedConstantClone, VerdictOn)
	l.ObserveProbe(ExtraFiltersLen, VerdictOn)
	if l.Backend().On(MathQuotedConstantClone) || l.Backend().On(ExtraFiltersLen) {
		t.Fatal("a probe decided a gate that has none")
	}
}

// A release that says the gate is on and a probe that observes it absent (a member without it, a patched build): the
// gate is off, the mismatch is reported and counted once.
//
// conformance: vl-capability-layer
func TestProbeAbsentBelowTheReleaseTurnsTheGateOff(t *testing.T) {
	l := newTestLayer(newClock())
	l.Observe("vl:9428", rel("v1.54.0"))
	var seen []Set
	l.Subscribe(func(_, now Set) { seen = append(seen, now) })
	l.ObserveProbe(PipeCoalesce, VerdictOff)
	l.ObserveProbe(PipeCoalesce, VerdictOff)
	s := l.Backend()
	if s.On(PipeCoalesce) || s.Why(PipeCoalesce) != ReasonMismatch || len(s.Mismatches()) != 1 || s.Mismatches()[0] != PipeCoalesce {
		t.Fatalf("mismatch: %s", s.HeaderValue())
	}
	if len(seen) != 1 || l.Stats().Mismatches[PipeCoalesce] != 1 {
		t.Fatalf("one change and one count: %d changes, %d mismatches", len(seen), l.Stats().Mismatches[PipeCoalesce])
	}
	if st := l.Status(); len(st.Mismatches) != 1 || st.Mismatches[0] != "vl.pipe_coalesce" {
		t.Fatalf("status mismatches %v", st.Mismatches)
	}
	// A present answer in the same window does not undo an absent one (the lowest member decides).
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	if l.Backend().On(PipeCoalesce) {
		t.Fatal("a present answer overrode an absent one inside the window")
	}
	// A gate both agree on is "both", with no change of digest or epoch.
	epoch := l.Backend().Epoch()
	l.ObserveProbe(StatsLimitAfterStats, VerdictOn)
	if got := l.Backend(); got.Why(StatsLimitAfterStats) != ReasonBoth || got.Epoch() != epoch {
		t.Fatalf("agreeing probes keep the digest: %s", got.HeaderValue())
	}
}

// A probe never turns a gate on above the release: a newer member answered, or a patched build.
//
// conformance: vl-capability-layer
func TestProbeNeverRaisesAboveTheRelease(t *testing.T) {
	l := newTestLayer(newClock())
	l.Observe("vl:9428", rel("v1.52.0"))
	before := l.Backend()
	l.ObserveProbe(RangeTimeWritersRefused, VerdictOn)
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	after := l.Backend()
	if after.On(RangeTimeWritersRefused) || after.Digest() != before.Digest() || after.Epoch() != before.Epoch() || len(after.Mismatches()) != 0 {
		t.Fatalf("a probe raised a gate above v1.52.0: %s", after.HeaderValue())
	}
}

// Behind a load balancer every member shares one backend key and the probes and /metrics reads reach random
// members. The lowest member decides, whatever the order of the answers, and alternating answers do not move the
// digest or the epoch.
//
// conformance: vl-capability-layer
func TestSharedKeyMixedMembersNeverAboveTheLowest(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	l.Observe("lb", rel("v1.54.0"))
	l.Observe("lb", rel("v1.50.0"))
	s := l.Backend()
	if s.Release().String() != "v1.50.0" || s.On(PipeCoalesce) {
		t.Fatalf("lowest member: %s", s.HeaderValue())
	}
	l.ObserveProbe(PipeCoalesce, VerdictOn) // answered by the v1.54 member
	if got := l.Backend(); got.On(PipeCoalesce) || got.Why(PipeCoalesce) == ReasonMismatch {
		t.Fatalf("a newer member's answer raised a gate above the lowest release: %s", got.HeaderValue())
	}
	l.ObserveProbe(PipeCoalesce, VerdictOff) // the v1.50 member
	epoch, digest := l.Backend().Epoch(), l.Backend().Digest()
	for i := 0; i < 40; i++ {
		clk.Advance(time.Second)
		l.Observe("lb", rel("v1.54.0"))
		l.Observe("lb", rel("v1.50.0"))
		for _, g := range []GateID{PipeCoalesce, StatsLimitAfterStats, RangeTimeWritersRefused} {
			if i%2 == 0 {
				l.ObserveProbe(g, VerdictOn)
			} else {
				l.ObserveProbe(g, VerdictOff)
			}
		}
		if got := l.Backend(); got.Epoch() > epoch+1 {
			t.Fatalf("alternating answers moved the epoch: %d -> %d", epoch, got.Epoch())
		}
	}
	got := l.Backend()
	if got.Release().String() != "v1.50.0" || got.On(PipeCoalesce) || got.On(StatsLimitAfterStats) || got.On(RangeTimeWritersRefused) || got.On(ExtraFiltersLen) {
		t.Fatalf("a gate is above the lowest member: %s", got.HeaderValue())
	}
	_ = digest
	// Unknown release, mixed members, one absent answer: absent wins for that gate and nothing is implied.
	u := newTestLayer(clk)
	u.ObserveProbe(PipeCoalesce, VerdictOn)
	u.ObserveProbe(PipeCoalesce, VerdictOff)
	u.ObserveProbe(PipeCoalesce, VerdictOn)
	if us := u.Backend(); us.On(PipeCoalesce) || us.On(DensePatternWindowing) {
		t.Fatalf("an absent answer inside the window lost to a later present one: %s", us.HeaderValue())
	}
}

// An absent answer holds for the whole window and then gives way to a newer present one; a silent backend keeps the
// last verdict past the window.
//
// conformance: vl-capability-layer
func TestProbeVerdictsAgeLikeReleases(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	l.Observe("a", rel("v1.54.0"))
	l.ObserveProbe(PipeCoalesce, VerdictOff)
	clk.Advance(l.window / 2)
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	if l.Backend().On(PipeCoalesce) {
		t.Fatal("the absent answer must hold inside the window")
	}
	clk.Advance(l.window)
	l.ObserveProbe(PipeCoalesce, VerdictOn) // the absent answer is older than the window now
	if !l.Backend().On(PipeCoalesce) {
		t.Fatal("a stale absent answer outlived a newer present one")
	}
	// Silence: every answer older than the window, the last one stands.
	l.ObserveProbe(StatsLimitAfterStats, VerdictOff)
	clk.Advance(5 * l.window)
	l.Prune()
	if l.Backend().On(StatsLimitAfterStats) || !l.Backend().On(PipeCoalesce) {
		t.Fatalf("silence flipped a gate: %s", l.Backend().HeaderValue())
	}
}

// The Safety gate fails closed: a build with a suffix the layer does not recognise, a release some probe says has an
// older member, and an unknown release are all guarded; the canonical name keeps the suffix apart.
//
// conformance: vl-capability-layer
func TestSafetyGateFailsClosed(t *testing.T) {
	for _, raw := range []string{"v1.53.0-rc1", "v1.54.0-cluster", "v1.54.0-custom-build"} {
		r := ParseRelease(raw)
		if !r.Known || r.Flavour != FlavourUnknown || r.Suffix == "" || ForRelease(r).On(MathQuotedConstantClone) || !ForRelease(r).Decided(MathQuotedConstantClone) {
			t.Errorf("%s must stay guarded: %+v", raw, r)
		}
		if r.String() == ParseRelease(raw[:len("v1.53.0")]).String() {
			t.Errorf("%s has the canonical name of the plain release", raw)
		}
		// The gates a version decides follow the version, as for any flavour.
		if !ForRelease(r).On(PipeCoalesce) {
			t.Errorf("%s: coalesce follows the version", raw)
		}
	}
	b, ok := ParseBuild("victoria-logs-20261113-102924-tags-v1.53.0-cluster-0-gabc", SourceMetrics)
	if !ok || b.String() != "v1.53.0-cluster" || ForRelease(b).On(MathQuotedConstantClone) {
		t.Fatalf("cluster build: %+v", b)
	}
	if ForRelease(ParseRelease("v1.53.0")).Digest() == ForRelease(ParseRelease("v1.53.0-rc1")).Digest() {
		t.Fatal("the digest must tell v1.53.0-rc1 from v1.53.0")
	}
	if ParseRelease("v1.53.0-rc1").Compare(ParseRelease("v1.53.0-rc2")) == 0 || !ForRelease(ParseRelease("v1.53.0")).On(MathQuotedConstantClone) {
		t.Fatal("suffixes compare apart; the plain release is fixed")
	}
	// A probe that finds a feature of v1.53 or earlier absent on a release that carries the fix: some member is
	// older than the fix.
	for _, g := range []GateID{RangeTimeWritersRefused, PipeCoalesce, StatsLimitAfterStats} {
		l := newTestLayer(newClock())
		l.Observe("lb", rel("v1.54.0"))
		if !l.Backend().On(MathQuotedConstantClone) {
			t.Fatal("v1.54.0 carries the fix")
		}
		l.ObserveProbe(g, VerdictOff)
		if l.Backend().On(MathQuotedConstantClone) {
			t.Errorf("the clone guard stayed off after %s was found absent", g)
		}
	}
}

// A release leaves the window only when a newer observation exists, and the layer never holds more than
// maxReleases of them.
func TestWindowNeverDropsTheLastReleaseAndIsBounded(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	l.Observe("a", rel("v1.50.0"))
	clk.Advance(10 * l.window)
	l.Prune()
	if !l.Backend().Release().Known || len(l.Status().Releases) != 1 {
		t.Fatal("the only release stays through silence")
	}
	for i := 0; i < 100; i++ {
		l.Observe(fmt.Sprintf("backend-%d", i), rel(fmt.Sprintf("v1.54.%d", i)))
		clk.Advance(time.Second)
	}
	if n := len(l.Status().Releases); n > maxReleases {
		t.Fatalf("%d releases remembered", n)
	}
	l.SetWindow(time.Hour)
	if l.window != time.Hour {
		t.Fatal("window")
	}
	l.SetWindow(time.Minute)
	if l.window != time.Minute {
		t.Fatal("the window follows the interval it is derived from, up or down")
	}
	l.SetWindow(0)
	if l.window != time.Minute {
		t.Fatal("a window that is not positive is ignored")
	}
	l.Observe("x", Release{}) // not Known: ignored
}

func TestObserveRawCountsOnlyVictoriaLogs(t *testing.T) {
	l := newTestLayer(newClock())
	if l.ObserveRaw("a", "vmauth-1-tags-v1.155.0-0-gabc", SourceHeader) || l.ObserveRaw("a", "v1.54.0", SourceHeader) || l.Backend().Release().Known {
		t.Fatal("not VictoriaLogs")
	}
	const raw = "victoria-logs-20261113-102924-tags-v1.54.0-0-g6433dae586"
	if !l.ObserveRaw("a", raw, SourceHeader) || l.Backend().Release().String() != "v1.54.0" || l.Backend().Release().Raw != raw {
		t.Fatal("VictoriaLogs build")
	}
	// The same text again costs a string compare: no allocation.
	if n := testing.AllocsPerRun(100, func() { l.ObserveRaw("a", raw, SourceHeader) }); n != 0 {
		t.Fatalf("repeat observation allocates %v", n)
	}
}

// A header that repeats on every response keeps its release in the window through the allocation-free path, and a
// release that was dropped is counted again when its header comes back.
func TestObserveRawRepeatKeepsTheReleaseInTheWindow(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	const old, newer = "victoria-logs-20260901-120000-tags-v1.52.0-0-gabcdef0123", "victoria-logs-20261113-102924-tags-v1.54.0-0-g6433dae586"
	l.ObserveRaw("a", old, SourceHeader)
	for i := 0; i < 30; i++ {
		clk.Advance(l.window / 4)
		l.ObserveRaw("a", old, SourceHeader)      // every response of backend a
		l.Observe("b", ParseBuildOrDie(t, newer)) // a probe round finds b
	}
	if got := l.Backend().Release().String(); got != "v1.52.0" {
		t.Fatalf("the repeating release was forgotten: effective %s", got)
	}
	// Silence on a, activity on b: a leaves the window, and its text counts again when it returns.
	clk.Advance(2 * l.window)
	l.Observe("b", ParseBuildOrDie(t, newer))
	if got := l.Backend().Release().String(); got != "v1.54.0" {
		t.Fatalf("the silent release stayed: effective %s", got)
	}
	l.ObserveRaw("a", old, SourceHeader)
	if got := l.Backend().Release().String(); got != "v1.52.0" {
		t.Fatalf("a dropped release was not counted again: effective %s", got)
	}
}

// The request path: For(ctx) and Backend() allocate nothing and carry the client profile of the request.
//
// conformance: vl-capability-layer
func TestForIsAllocationFreeAndCarriesTheClientProfile(t *testing.T) {
	l := newTestLayer(newClock())
	l.Observe("a", rel("v1.54.0"))
	plain := context.Background()
	prof := clientprofile.Profile{Surface: clientprofile.SurfaceDrilldown, Grafana: clientprofile.ParseVersion("13.3.1")}
	withProfile := clientprofile.With(plain, prof)
	for name, ctx := range map[string]context.Context{"no profile": plain, "profile": withProfile} {
		ctx := ctx
		if n := testing.AllocsPerRun(200, func() {
			s := l.For(ctx)
			_ = s.On(PipeCoalesce) && s.Decided(ExtraFiltersLen)
		}); n != 0 {
			t.Errorf("%s: For allocates %v", name, n)
		}
	}
	if n := testing.AllocsPerRun(200, func() { _ = l.Backend().Digest() }); n != 0 {
		t.Errorf("Backend allocates %v", n)
	}
	if got := l.For(withProfile).Client(); got != prof {
		t.Fatalf("client profile %+v", got)
	}
	if l.For(plain).Client() != (clientprofile.Profile{}) || l.Backend().Client() != (clientprofile.Profile{}) {
		t.Fatal("no client gate is stored globally")
	}
}

func TestSubscribersRunInOrderOutsideTheLock(t *testing.T) {
	l := newTestLayer(newClock())
	var order []int
	for i := 1; i <= 3; i++ {
		i := i
		l.Subscribe(func(Set, Set) {
			l.Status() // takes the layer's lock: a subscriber runs outside it
			order = append(order, i)
		})
	}
	l.Observe("a", rel("v1.54.0"))
	if fmt.Sprint(order) != "[1 2 3]" {
		t.Fatalf("order %v", order)
	}
}

func TestStatusShape(t *testing.T) {
	clk := newClock()
	l := newTestLayer(clk)
	l.SetLokiTarget("3.7")
	l.Observe("a:9428", ParseBuildOrDie(t, "victoria-logs-20261101-000000-tags-v1.52.2-enterprise-0-gabc"))
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	st := l.Status()
	if st.Effective != "v1.52.2-enterprise" || !st.Known || st.LokiTarget != "3.7" || st.Digest == "" || st.Epoch != 1 || st.Changes != 1 || len(st.Gates) != len(Gates()) || len(st.Releases) != 1 {
		t.Fatalf("%+v", st)
	}
	r := st.Releases[0]
	if r.Flavour != "enterprise" || r.Source != "header" || r.Backend != backendID("a:9428") || strings.Contains(r.Backend, "9428") || !r.InWindow || r.Raw == "" {
		t.Fatalf("%+v", r)
	}
	var found bool
	for _, g := range st.Gates {
		if g.Name == "vl.pipe_coalesce" {
			found = true
			if !g.On || !g.Decided || g.Source != "both" || g.Registry != "pipe-coalesce" || g.ProbeVerdict != "on" || g.LastProbe.IsZero() || g.Kind != "capability" || g.Side != "victorialogs" || g.From != "v1.51.0" {
				t.Fatalf("%+v", g)
			}
		}
	}
	if !found {
		t.Fatal("pipe_coalesce missing")
	}
	clk.Advance(2 * l.window)
	if !l.Status().Releases[0].InWindow {
		t.Fatal("the only release stays counted through silence (an outage empties nothing)")
	}
	l.Observe("b:9428", rel("v1.54.0"))
	if st := l.Status(); len(st.Releases) != 1 || st.Releases[0].Release != "v1.54.0" {
		t.Fatalf("a release not seen for the window stops counting once a newer observation exists: %+v", st.Releases)
	}
}

// ParseBuildOrDie parses a VictoriaLogs build string for a test.
func ParseBuildOrDie(t *testing.T, raw string) Release {
	t.Helper()
	r, ok := ParseBuild(raw, SourceHeader)
	if !ok {
		t.Fatalf("not a VictoriaLogs build: %q", raw)
	}
	return r
}

// The client request metric stays bounded however many distinct versions headers name.
//
// conformance: vl-capability-layer
func TestClientSeriesCardinalityIsCapped(t *testing.T) {
	l := newTestLayer(newClock())
	for i := 0; i < 5000; i++ {
		l.ObserveClient(clientprofile.Profile{
			Surface:    clientprofile.Surface(i % 7),
			Grafana:    clientprofile.Version{Major: uint16(i % 997), Minor: uint16(i % 13), Patch: uint16(i), Known: true},
			Datasource: clientprofile.Version{Major: uint16(i), Known: true},
			Drilldown:  clientprofile.Version{Minor: uint16(i), Known: i%2 == 0},
		})
	}
	series := l.ClientSeriesSnapshot()
	if len(series) > MaxClientSeries {
		t.Fatalf("%d series", len(series))
	}
	values := [3]map[string]bool{{}, {}, {}}
	var total uint64
	for _, s := range series {
		values[0][s.Grafana], values[1][s.Datasource], values[2][s.Drilldown] = true, true, true
		total += s.Requests
	}
	for i, v := range values {
		// 32 distinct values and the other bucket.
		if len(v) > MaxClientLabelValues+1 {
			t.Errorf("label %d takes %d values", i, len(v))
		}
	}
	if total != 5000 {
		t.Fatalf("requests counted %d", total)
	}
	if !values[0][OtherLabel] {
		t.Fatal("values beyond the cap are counted as other")
	}
	if st := l.Stats(); len(st.Clients) != len(series) {
		t.Fatal("stats")
	}
}

func TestObserveClientIsAllocationFreeOnTheRepeat(t *testing.T) {
	l := newTestLayer(newClock())
	p := clientprofile.Profile{Surface: clientprofile.SurfaceDatasource, Grafana: clientprofile.ParseVersion("13.3.1"), Datasource: clientprofile.ParseVersion("13.2.5")}
	l.ObserveClient(p)
	if n := testing.AllocsPerRun(200, func() { l.ObserveClient(p) }); n != 0 {
		t.Fatalf("a profile seen before allocates %v", n)
	}
	if s := l.ClientSeriesSnapshot(); len(s) != 1 || s[0].Requests < 200 || s[0].Drilldown != "unknown" || s[0].Surface != "datasource" || s[0].Grafana != "13.3.1" {
		t.Fatalf("%+v", s)
	}
}

func BenchmarkLayerFor(b *testing.B) {
	l := newTestLayer(newClock())
	l.Observe("a", rel("v1.54.0"))
	ctx := clientprofile.With(context.Background(), clientprofile.Profile{Surface: clientprofile.SurfaceExplore})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !l.For(ctx).On(PipeCoalesce) {
			b.Fatal("off")
		}
	}
}

func BenchmarkObserveClientParallel(b *testing.B) {
	l := newTestLayer(newClock())
	profiles := make([]clientprofile.Profile, 8)
	for i := range profiles {
		profiles[i] = clientprofile.Profile{Surface: clientprofile.SurfaceDatasource, Grafana: clientprofile.Version{Major: 13, Minor: uint16(i), Known: true}}
		l.ObserveClient(profiles[i])
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.ObserveClient(profiles[i&7])
			i++
		}
	})
}

// Upgrade, rollback and upgrade again with a fake clock: each time the backend changes, the digest follows (at once
// downwards, within the window upwards), every change is one set change, and the releases listed are the ones that
// count now.
//
// conformance: vl-capability-layer
func TestUpgradeRollbackUpgradeCycles(t *testing.T) {
	const interval = 10 * time.Second
	clk := newClock()
	l := newTestLayer(clk)
	l.SetWindow(6*interval + 55*time.Second)
	var changes int
	l.Subscribe(func(Set, Set) { changes++ })
	tick := func(release string) {
		clk.Advance(interval)
		l.Observe("lb", rel(release)) // the /metrics read of one round
		l.Prune()
	}
	counted := func() []string {
		var out []string
		for _, r := range l.Status().Releases {
			if r.InWindow {
				out = append(out, r.Release)
			}
		}
		return out
	}
	for i := 0; i < 3; i++ {
		tick("v1.54.0")
	}
	d54 := l.Backend().Digest()
	if changes != 1 {
		t.Fatalf("one change for the first release, got %d", changes)
	}
	for cycle := 0; cycle < 3; cycle++ {
		before := changes
		tick("v1.49.0") // the backend is now v1.49.0 (the other one is gone)
		d49 := l.Backend().Digest()
		if d49 == d54 || l.Backend().Release().String() != "v1.49.0" || changes != before+1 {
			t.Fatalf("cycle %d: the lower release must decide at once: %s (%d changes)", cycle, l.Backend().Release(), changes-before)
		}
		// Back to v1.54.0: v1.49.0 stops counting a window after it was last seen, no later than window + one interval.
		rounds := 0
		for l.Backend().Digest() != d54 {
			tick("v1.54.0")
			if rounds++; rounds > 6+6+1 { // the window (six intervals and a probe round) plus one interval
				t.Fatalf("cycle %d: the digest did not return to the first value after %d rounds; counted %v", cycle, rounds, counted())
			}
		}
		if changes != before+2 {
			t.Fatalf("cycle %d: every change is one set change: %d", cycle, changes-before)
		}
		if got := counted(); len(got) != 1 || got[0] != "v1.54.0" {
			t.Fatalf("cycle %d: only the releases that count are listed, got %v", cycle, got)
		}
	}
}

// conformance: vl-capability-layer
func TestMixedJSONRowStatsUsesFleetMinimum(t *testing.T) {
	l := newTestLayer(newClock())
	l.Observe("new:9428", rel("v1.54.0"))
	if !l.Backend().On(MixedJSONRowStats) {
		t.Fatal("known supported member must enable the gate")
	}
	digest := l.Backend().Digest()
	l.Observe("old:9428", rel("v1.48.9"))
	s := l.Backend()
	if s.On(MixedJSONRowStats) || s.Release().String() != "v1.48.9" || s.Digest() == digest {
		t.Fatalf("unsupported fleet minimum must disable the gate and fence keys: %s", s.HeaderValue())
	}
	floor := newTestLayer(newClock())
	floor.Observe("new:9428", rel("v1.54.0"))
	floor.Observe("floor:9428", rel("v1.49.0"))
	if !floor.Backend().On(MixedJSONRowStats) {
		t.Fatal("validated floor in a mixed fleet must remain enabled")
	}
}
