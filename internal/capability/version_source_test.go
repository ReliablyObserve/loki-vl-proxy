package capability

import (
	"strings"
	"testing"
)

var testReleases = []string{"v1.49.0", "v1.50.0", "v1.51.0", "v1.52.0", "v1.53.0", "v1.54.0"}

// probedGates are the gates with a feature probe, in table order.
func probedGates() (out []GateID) {
	for _, g := range Gates() {
		if g.Probe != nil && g.Kind != Safety {
			out = append(out, g.ID)
		}
	}
	return out
}

// answerProbes tells the layer what a backend of release actual answers the probes of mask.
func answerProbes(l *Layer, actual string, mask int) {
	var results []ProbeResult
	for i, id := range probedGates() {
		if mask&(1<<i) == 0 {
			continue
		}
		v := VerdictOff
		if gates[id].In(rel(actual)) {
			v = VerdictOn
		}
		results = append(results, ProbeResult{Gate: id, Verdict: v})
	}
	l.ObserveProbes(results)
}

func fromURL(s string) Release {
	r := rel(s)
	r.Source = SourceVersionURL
	return r
}

// Whatever release the operator names (declared or read from the version URL, or both), whatever the backend really
// runs and whichever probes answered: it never turns on a gate the backend lacks, crash guards included.
//
// conformance: vl-backend-version-source, backend-version-source
func TestNamedReleaseNeverEnablesWhatTheBackendLacks(t *testing.T) {
	all := 1<<len(probedGates()) - 1
	for _, claim := range testReleases {
		for _, actual := range testReleases {
			for mask := 0; mask <= all; mask++ {
				declared := newTestLayer(newClock())
				declared.Declare(rel(claim))
				url := newTestLayer(newClock())
				url.Observe("vl", fromURL(claim))
				both := newTestLayer(newClock())
				both.Declare(rel("v1.54.0"))
				both.Observe("vl", fromURL(claim))
				for name, l := range map[string]*Layer{"declared": declared, "version URL": url, "both": both} {
					answerProbes(l, actual, mask)
					for _, g := range Gates() {
						if s := l.Backend(); !g.In(rel(actual)) && !g.Default && s.On(g.ID) {
							t.Fatalf("%s %s on %s, probes %b: %s is on", name, claim, actual, mask, g.Name)
						}
					}
				}
			}
		}
	}
}

// conformance: vl-backend-version-source, backend-version-source
func TestNamedReleaseGatesWhenTheProbesAgree(t *testing.T) {
	all := 1<<len(probedGates()) - 1
	type want struct{ math, extra, mixed, coalesce, known bool }
	cases := []struct {
		name           string
		declared, url  string
		header, actual string
		mask           int
		release        string
		want           want
	}{
		{name: "version URL, v1.54.0", url: "v1.54.0", actual: "v1.54.0", mask: all, release: "v1.54.0", want: want{true, false, true, true, true}},
		{name: "version URL, no probe answered: nothing is enabled", url: "v1.54.0", actual: "v1.54.0", release: "v1.54.0", want: want{false, false, false, false, true}},
		{name: "version URL v1.54.0 on v1.53.0: the version-only gate no probe reaches stays off", url: "v1.54.0", actual: "v1.53.0", mask: all, release: "v1.54.0", want: want{true, false, true, true, true}},
		{name: "version URL newer than the backend: not used", url: "v1.54.0", actual: "v1.52.0", mask: all, release: "unknown", want: want{false, false, false, true, false}},
		{name: "declared v1.54.0: a gate newer than every probe stays off", declared: "v1.54.0", actual: "v1.54.0", mask: all, release: "v1.54.0", want: want{true, false, true, true, true}},
		{name: "declared v1.53.0", declared: "v1.53.0", actual: "v1.53.0", mask: all, release: "v1.53.0", want: want{true, false, true, true, true}},
		{name: "declared v1.52.0", declared: "v1.52.0", actual: "v1.52.0", mask: all, release: "v1.52.0", want: want{false, false, true, true, true}},
		{name: "declared v1.50.0: nothing probed proves it", declared: "v1.50.0", actual: "v1.50.0", mask: all, release: "v1.50.0", want: want{false, false, false, false, true}},
		{name: "declared, no probe answered", declared: "v1.54.0", actual: "v1.54.0", release: "v1.54.0", want: want{false, false, false, false, true}},
		{name: "declared newer than the backend: not used", declared: "v1.54.0", actual: "v1.52.0", mask: all, release: "unknown", want: want{false, false, false, true, false}},
		{name: "declared older than the backend: the lower wins", declared: "v1.50.0", actual: "v1.54.0", mask: all, release: "v1.50.0", want: want{false, false, true, false, true}},
		{name: "declared above the release read: the read one wins", declared: "v1.54.0", header: "v1.52.0", actual: "v1.52.0", mask: all, release: "v1.52.0", want: want{false, false, true, true, true}},
		{name: "declared below the release read: the declared one wins", declared: "v1.52.0", header: "v1.54.0", actual: "v1.54.0", mask: all, release: "v1.52.0", want: want{false, false, true, true, true}},
		{name: "version URL and a header: the lower wins", url: "v1.54.0", header: "v1.52.0", actual: "v1.54.0", mask: all, release: "v1.52.0", want: want{false, false, true, true, true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newTestLayer(newClock())
			if tc.declared != "" {
				l.Declare(rel(tc.declared))
			}
			if tc.url != "" {
				l.Observe("vl", fromURL(tc.url))
			}
			if tc.header != "" {
				r := rel(tc.header)
				r.Source = SourceHeader
				l.Observe("vl", r)
			}
			answerProbes(l, tc.actual, tc.mask)
			s := l.Backend()
			got := want{s.On(MathQuotedConstantClone), s.On(ExtraFiltersLen), s.On(MixedJSONRowStats), s.On(PipeCoalesce), s.Release().Known}
			if got != tc.want || s.Release().String() != tc.release {
				t.Fatalf("got %+v release %s, want %+v release %s (%s)", got, s.Release(), tc.want, tc.release, s.HeaderValue())
			}
			if s.Observed() != (s.Release().Known && (tc.url != "" || tc.header != "")) && tc.release != "unknown" {
				t.Fatalf("observed %v", s.Observed())
			}
		})
	}
}

// A mixed cluster read through one version URL: the lowest release in the window decides; one a probe contradicts is
// left out and counted, and the status shows it.
//
// conformance: vl-backend-version-source, backend-version-source
func TestVersionURLMixedAndContradicted(t *testing.T) {
	c := newClock()
	l := newTestLayer(c)
	for _, v := range []string{"v1.54.0", "v1.52.0", "v1.54.0"} {
		l.Observe("vl", fromURL(v))
	}
	answerProbes(l, "v1.52.0", 1<<len(probedGates())-1)
	s := l.Backend()
	if s.Release().String() != "v1.52.0" || s.On(MathQuotedConstantClone) || s.On(ExtraFiltersLen) || !s.On(MixedJSONRowStats) {
		t.Fatalf("lowest wins: %s", s.HeaderValue())
	}
	if s.Why(RangeTimeWritersRefused) != ReasonMismatch || l.Stats().Mismatches[RangeTimeWritersRefused] != 1 {
		t.Fatalf("the contradiction is counted once: %s", s.Why(RangeTimeWritersRefused))
	}
	rejected := 0
	for _, r := range l.Status().Releases {
		if r.Rejected {
			rejected++
			if r.InWindow || r.Release != "v1.54.0" || r.Source != "version_url" {
				t.Fatalf("%+v", r)
			}
		}
	}
	if rejected != 1 {
		t.Fatalf("%d rejected releases", rejected)
	}
	// A declared release never ages out and never ages a read one out.
	d := newTestLayer(c)
	d.Declare(rel("v1.54.0"))
	r := rel("v1.52.0")
	r.Source = SourceMetrics
	d.Observe("vl", r)
	c.Advance(10 * DefaultWindow)
	d.Prune()
	if got := d.Backend().Release().String(); got != "v1.52.0" || len(d.Status().Releases) != 2 {
		t.Fatalf("%s %+v", got, d.Status().Releases)
	}
}

// A release the backend reports itself decides every gate it carries; the same one from the version URL does not
// take that away.
//
// conformance: vl-backend-version-source, backend-version-source
func TestBackendReadingWinsOverTheSameVersionURLRelease(t *testing.T) {
	l := newTestLayer(newClock())
	l.Observe("vl", fromURL("v1.54.0"))
	if l.Backend().On(MathQuotedConstantClone) {
		t.Fatal("an unprobed version URL release lifted the crash guard")
	}
	r := rel("v1.54.0")
	r.Source = SourceHeader
	l.Observe("vl", r)
	if s := l.Backend(); !s.On(MathQuotedConstantClone) || s.Release().Source != SourceHeader {
		t.Fatalf("%s %s", s.HeaderValue(), s.Release().Source)
	}
}

// conformance: vl-backend-version-source, backend-version-source
func TestDescribeMetricsNamesTheOtherProgram(t *testing.T) {
	for text, want := range map[string]string{
		`vm_app_version{version="vmauth-20260929-120000-tags-v1.144.0-0-gabc",short_version="v1.144.0"} 1`: "found vmauth-20260929-120000-tags-v1.144.0-0-gabc",
		`vm_app_version{short_version="v1.144.0"} 1`:                                                       "found v1.144.0",
		"vm_app_version{version=\"a\x1b[31m b\"} 1":                                                        "found a31mb",
		`go_goroutines 1`: "no vm_app_version or victorialogs_build_info series names VictoriaLogs",
		"":                "no vm_app_version or victorialogs_build_info series names VictoriaLogs",
	} {
		if got := DescribeMetrics(text); got != want {
			t.Errorf("%q: %q, want %q", text, got, want)
		}
	}
	long := `vm_app_version{version="` + strings.Repeat("x", 4096) + `"} 1`
	if got := DescribeMetrics(long); len(got) != len("found ")+maxForeign {
		t.Fatalf("%d bytes", len(got))
	}
}

// A declared release older than what the probes show is reported; one the probes do not outrun is not.
//
// conformance: vl-backend-version-source, backend-version-source
func TestNamedReleaseBehindTheProbes(t *testing.T) {
	for declared, want := range map[string]string{"v1.50.0": "v1.50.0", "v1.52.0": "v1.52.0", "v1.53.0": "", "v1.54.0": ""} {
		l := newTestLayer(newClock())
		l.Declare(rel(declared))
		answerProbes(l, "v1.54.0", 1<<len(probedGates())-1)
		if got := l.Status().NamedBehind; got != want {
			t.Errorf("declared %s: behind %q, want %q", declared, got, want)
		}
	}
}

// confirmed reads a probed feature as proof of every release up to its own. A gate back-ported to an older line, or
// carried by some builds only, would break that proof of the crash guard, so it never counts; today no probed gate is
// one.
//
// conformance: vl-backend-version-source, backend-version-source
func TestProbedGatesProveTheirRelease(t *testing.T) {
	for _, id := range probedGates() {
		if g := gates[id]; len(g.Lines) != 0 || g.Flavours != 0 {
			t.Errorf("%s: a probed gate with release lines or flavours no longer confirms a named release", g.Name)
		}
	}
}
