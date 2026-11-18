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
	l.ObserveProbe(StatsLimitAfterStats, VerdictOff)
	l.ObserveProbe(StreamMetadataEndpoints, VerdictOn)
	s := l.Backend()
	if s.Release().Known || !s.On(PipeCoalesce) || !s.Decided(PipeCoalesce) || s.Why(PipeCoalesce) != ReasonProbe ||
		s.On(StatsLimitAfterStats) || !s.Decided(StatsLimitAfterStats) {
		t.Fatalf("probes decide: %s", s.HeaderValue())
	}
	// A coalesce answered means v1.51 or later: the capabilities of v1.50 follow (implied), the v1.52 ones do not.
	// A Safety gate and an undecided Behaviour gate stay as they were.
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
	// A known release is never overridden by an implication.
	l4 := newTestLayer(newClock())
	l4.Observe("a", rel("v1.49.0"))
	if l4.Backend().On(DensePatternWindowing) {
		t.Fatal("v1.49.0 has no dense windowing")
	}
	// A probe cannot decide a Safety gate or a gate without a probe.
	l.ObserveProbe(MathQuotedConstantClone, VerdictOn)
	l.ObserveProbe(ExtraFiltersLen, VerdictOn)
	if l.Backend().On(MathQuotedConstantClone) || l.Backend().On(ExtraFiltersLen) {
		t.Fatal("a probe decided a gate that has none")
	}
}

// A release that says the gate is on and a probe that observes it refused: the observed behaviour wins, the
// mismatch is reported and counted once.
//
// conformance: vl-capability-layer
func TestVersionAndProbeDisagreeObservedBehaviourWins(t *testing.T) {
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
	// The probe that agrees gives the reason "both", with no change of digest or epoch.
	l.ObserveProbe(PipeCoalesce, VerdictOn)
	epoch := l.Backend().Epoch()
	l.ObserveProbe(StatsLimitAfterStats, VerdictOn)
	if got := l.Backend(); got.Why(StatsLimitAfterStats) != ReasonBoth || got.Epoch() != epoch || got.Why(PipeCoalesce) != ReasonBoth {
		t.Fatalf("agreeing probes keep the digest: %s", got.HeaderValue())
	}
	// A Behaviour gate observed present by the probe on an older release flips too.
	l2 := newTestLayer(newClock())
	l2.Observe("vl:9428", rel("v1.52.0"))
	l2.ObserveProbe(RangeTimeWritersRefused, VerdictOn)
	if !l2.Backend().On(RangeTimeWritersRefused) || l2.Backend().Why(RangeTimeWritersRefused) != ReasonMismatch {
		t.Fatal("a refused shape the release says is accepted")
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
	if l.window != time.Hour {
		t.Fatal("the window never shrinks")
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
	if l.Status().Releases[0].InWindow {
		t.Fatal("a release past the window is marked")
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
