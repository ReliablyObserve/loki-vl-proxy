package capability

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql/probes"
)

// gateSet is the gates a release turns on (decided ones listed by name), as sorted text.
func gateSet(s Set) string {
	var on []string
	for _, g := range Gates() {
		if s.On(g.ID) {
			on = append(on, strings.TrimPrefix(g.Name, "vl."))
		}
	}
	return strings.Join(on, " ")
}

// Every supported release and flavour maps to its gate set by the one table. An unknown release holds the defaults:
// every post-floor gate is off, the floor's own stream metadata is on, and the crash guard guards.
//
// conformance: vl-capability-layer
func TestGateSetByRelease(t *testing.T) {
	const (
		floor = "stream_metadata_endpoints"
		v150  = "dense_pattern_windowing column_field_values stream_metadata_endpoints"
	)
	for _, c := range []struct {
		release string
		want    string
		decided bool
	}{
		{"v1.49.0", floor, true},
		{"v1.50.0", v150, true},
		{"v1.51.1", "dense_pattern_windowing pipe_coalesce column_field_values stream_metadata_endpoints stats_limit_after_stats", true},
		{"v1.52.0", "dense_pattern_windowing pipe_coalesce column_field_values stream_metadata_endpoints stats_limit_after_stats pipe_json_array_concat", true},
		// The v1.52 LTS patch is enterprise-only and carries no gate the table does not list for the line.
		{"v1.52.2-enterprise", "dense_pattern_windowing pipe_coalesce column_field_values stream_metadata_endpoints stats_limit_after_stats pipe_json_array_concat", true},
		{"v1.53.0", "dense_pattern_windowing pipe_coalesce column_field_values stream_metadata_endpoints stats_limit_after_stats pipe_json_array_concat range_time_writers_refused math_quoted_constant_clone", true},
		{"v1.54.0", "dense_pattern_windowing pipe_coalesce column_field_values stream_metadata_endpoints stats_limit_after_stats pipe_json_array_concat range_time_writers_refused math_quoted_constant_clone extra_filters_len", true},
		{"v1.54.0-enterprise-fips", "dense_pattern_windowing pipe_coalesce column_field_values stream_metadata_endpoints stats_limit_after_stats pipe_json_array_concat range_time_writers_refused math_quoted_constant_clone extra_filters_len", true},
		{"", floor, false},
	} {
		s := ForRelease(ParseRelease(c.release))
		if got := gateSet(s); got != c.want {
			t.Errorf("%q: gates on %q, want %q", c.release, got, c.want)
		}
		for _, g := range Gates() {
			decided := s.Decided(g.ID)
			switch {
			case !c.decided && decided:
				t.Errorf("%q: %s decided with no release", c.release, g.Name)
			case c.decided && !decided:
				t.Errorf("%q: %s undecided on a known release", c.release, g.Name)
			}
		}
	}
	// The v1.52 line stays guarded by the Safety gate whatever its flavour, and so does an unknown release.
	for _, rel := range []string{"", "v1.49.0", "v1.52.0", "v1.52.1", "v1.52.2-enterprise", "v1.52.2"} {
		if ForRelease(ParseRelease(rel)).On(MathQuotedConstantClone) {
			t.Errorf("%q must stay guarded", rel)
		}
	}
	if !ForRelease(ParseRelease("v1.53.0-enterprise")).On(MathQuotedConstantClone) {
		t.Error("v1.53.0 carries the fix")
	}
}

func TestGateInByLineAndFlavour(t *testing.T) {
	lts := Gate{From: [3]int{1, 53, 0}, Lines: map[[2]int]Patch{{1, 52}: {Patch: 1}}}
	for rel, want := range map[string]bool{"v1.52.0": false, "v1.52.1": true, "v1.52.2": true, "v1.51.9": false, "v1.53.0": true, "": false, "v1.52.1-enterprise": true} {
		if got := lts.In(ParseRelease(rel)); got != want {
			t.Errorf("lts %q: %v, want %v", rel, got, want)
		}
	}
	entOnly := Gate{From: [3]int{1, 53, 0}, Lines: map[[2]int]Patch{{1, 52}: {Patch: 2, Flavours: Flavours(FlavourEnterprise, FlavourEnterpriseFIPS)}}}
	for rel, want := range map[string]bool{"v1.52.2": false, "v1.52.2-enterprise": true, "v1.52.2-enterprise-fips": true, "v1.52.1-enterprise": false, "v1.53.0": true} {
		if got := entOnly.In(ParseRelease(rel)); got != want {
			t.Errorf("entOnly %q: %v, want %v", rel, got, want)
		}
	}
	ossOnly := Gate{From: [3]int{1, 50, 0}, Flavours: Flavours(FlavourOSS)}
	if !ossOnly.In(ParseRelease("v1.50.0")) || ossOnly.In(ParseRelease("v1.50.0-enterprise")) || ossOnly.In(Release{Major: 1, Minor: 54, Known: true, Flavour: FlavourUnknown}) {
		t.Error("a flavour list never counts a build it does not name, an unrecognised one included")
	}
}

func TestGateTableIsConsistent(t *testing.T) {
	seen := map[string]bool{}
	for i, g := range Gates() {
		if int(g.ID) != i || !strings.HasPrefix(g.Name, "vl.") || seen[g.Name] || g.Registry == "" || g.Citation == "" || g.Reads == "" {
			t.Errorf("gate %d: %+v", i, g)
		}
		seen[g.Name] = true
		if g.Kind == Safety && g.Probe != nil {
			t.Errorf("%s: a Safety gate has no probe (a crash cannot be probed)", g.Name)
		}
		if p := g.Probe; p != nil && (p.Accepted == p.Refused || p.Accepted == VerdictNone || p.Refused == VerdictNone || len(p.RefusalStatus) == 0 || len(p.RefusalText) == 0 || p.CrashSafe[0] == "" || p.CrashSafe[1] == "") {
			t.Errorf("%s: probe %+v", g.Name, p)
		}
		if g.Default && g.From[1] >= 49 {
			t.Errorf("%s: a post-floor gate defaults off", g.Name)
		}
	}
	if g, ok := Lookup("vl.pipe_coalesce"); !ok || g.ID != PipeCoalesce || GateID(200).Name() != "unknown" {
		t.Fatal("lookup")
	}
	kinds := map[string]Kind{"vl.pipe_coalesce": Capability, "vl.stats_limit_after_stats": Capability, "vl.range_time_writers_refused": Behaviour,
		"vl.extra_filters_len": Behaviour, "vl.math_quoted_constant_clone": Safety, "vl.pipe_json_array_concat": Capability}
	for name, kind := range kinds {
		if g, ok := Lookup(name); !ok || g.Kind != kind {
			t.Errorf("%s: %+v", name, g)
		}
	}
}

// Every probe reads the same fixed one-second window long before any retention (it must never scan stored rows), holds
// its query to that window in its own text where the endpoint takes the range from the query, and asks for one row.
func TestProbeValuesAndClassify(t *testing.T) {
	start, err1 := time.Parse(time.RFC3339, probes.WindowStart)
	end, err2 := time.Parse(time.RFC3339, probes.WindowEnd)
	if err1 != nil || err2 != nil || end.Sub(start) != time.Second || start.Year() != 2000 {
		t.Fatalf("the probe window: %v %v", probes.WindowStart, probes.WindowEnd)
	}
	for _, g := range Gates() {
		p := g.Probe
		if p == nil {
			continue
		}
		v := p.Values()
		if v.Get("query") != p.Query {
			t.Errorf("%s: %v", g.Name, v)
		}
		if p.Path == pathQuery && v.Get("limit") != "1" {
			t.Errorf("%s: a query probe reads one row at most: %v", g.Name, v)
		}
		if v.Get("start") != probes.WindowStart || v.Get("end") != probes.WindowEnd {
			t.Errorf("%s: window %v", g.Name, v)
		}
		if p.Path == pathStatsQuery && v.Get("time") != probes.WindowEnd {
			t.Errorf("%s: the instant is the window's end: %v", g.Name, v)
		}
		if p.Path != pathStreamFieldName && !strings.HasPrefix(p.Query, "_time:["+probes.WindowStart+", "+probes.WindowEnd+") ") {
			t.Errorf("%s: the query holds itself to the window: %q", g.Name, p.Query)
		}
		for _, arg := range []string{"start", "end", "time"} {
			if raw := v.Get(arg); raw != "" {
				if ts, err := time.Parse(time.RFC3339, raw); err != nil || ts.Before(start) || ts.After(end) {
					t.Errorf("%s: %s=%s is outside the window", g.Name, arg, raw)
				}
			}
		}
		if _, err := url.ParseQuery(v.Encode()); err != nil {
			t.Error(err)
		}
		// A success is the probe's accepted verdict; a listed refusal the opposite; everything else no verdict.
		if got := p.Classify(http.StatusOK, nil); got != p.Accepted {
			t.Errorf("%s: 200 -> %v", g.Name, got)
		}
		refusal := []byte("cannot parse: " + p.RefusalText[0])
		if got := p.Classify(p.RefusalStatus[0], refusal); got != p.Refused {
			t.Errorf("%s: refusal -> %v", g.Name, got)
		}
		for _, status := range []int{0, 401, 403, 429, 500, 502, 503, 504} {
			if got := p.Classify(status, refusal); got != VerdictNone {
				t.Errorf("%s: status %d -> %v (an outage is no verdict)", g.Name, status, got)
			}
		}
		if p.RefusalText[0] != "" {
			if got := p.Classify(p.RefusalStatus[0], []byte("some other parse error")); got != VerdictNone {
				t.Errorf("%s: an unrelated parse error is no verdict, got %v", g.Name, got)
			}
		}
	}
	if !containsFold([]byte("UNEXPECTED PIPE"), "unexpected pipe") || containsFold([]byte("x"), "xy") || !containsFold(nil, "") {
		t.Fatal("containsFold")
	}
}
