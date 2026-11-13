package querycorpus_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The Loki differential's own checks: its fixtures give each selector the streams it means, its samples are
// accepted by the regexps and patterns they are made for, and its comparison sees a wrong answer.

// Every fixture has a stream the selector selects (when one can be made), every selected stream is selected by
// every matcher and has a service_name, the extra stream is not selected, and the same plan gives the same data.
//
// conformance: architecture-syntax-package-layering
func TestLokiDifferentialFixtures(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	queries := diffQueries(t)
	empty := 0
	for _, dq := range queries {
		f := dq.fix
		if len(f.Entries) == 0 {
			empty++
			continue
		}
		if f.hash() != buildDiffFixture(dq.sel).hash() {
			t.Errorf("%s: the fixture is not the same from one build to the next", dq.q)
		}
		unselected := hasUnselected(dq.sel, f)
		for i, s := range f.Streams {
			if s["service_name"] == "" {
				t.Errorf("%s: stream %d has no service_name: %v", dq.q, i, s)
			}
			want := !unselected || i < len(f.Streams)-1
			if got := matchesAll(dq.sel.Matchers, s); got != want {
				t.Errorf("%s: stream %d %v selected %v, want %v", dq.q, i, s, got, want)
			}
		}
		for _, e := range f.Entries {
			for k := range e.Metadata {
				if vlReservedName(k) {
					t.Errorf("%s: structured metadata named %s", dq.q, k)
				}
				if _, ok := f.Streams[e.Stream][k]; ok {
					t.Errorf("%s: structured metadata %s is also a stream label", dq.q, k)
				}
			}
		}
	}
	if empty > len(queries)/50 {
		t.Errorf("%d of %d queries have no selected stream", empty, len(queries))
	}
}

// hasUnselected is whether the fixture's last stream is the one the selector does not select.
func hasUnselected(sel *queryplan.LogSelect, f diffFixture) bool {
	return len(f.Streams) > 0 && !matchesAll(sel.Matchers, f.Streams[len(f.Streams)-1])
}

// The samples are accepted by what they are made for: a regexp's samples by the regexp (anchored, as Loki's stream
// matchers are), a pattern's sample by the pattern's literal parts in order, an ip() operand's sample is an address
// inside it.
func TestLokiDifferentialSamples(t *testing.T) {
	for _, re := range []string{"5..", "error|warn", "(?i)err.*", "/api/.*", "a+b?c{2}", "[0-9]+", `\d{3}`, "(x|y)z", "", ".+", "[^a]b"} {
		samples := regexpSamples(re)
		if len(samples) == 0 {
			t.Errorf("%q: no sample", re)
		}
		anchored := regexp.MustCompile("^(?:" + re + ")$")
		for _, s := range samples {
			if !anchored.MatchString(s) {
				t.Errorf("%q: sample %q does not match", re, s)
			}
		}
	}
	for _, p := range []string{"<_> - - <_> \"<method> <uri> <_>\" <status> <size>", "<_> GET <_> 200", "plain", "<a><b"} {
		s := patternSample(p)
		rest := s
		for _, part := range regexp.MustCompile(`<[^>]*>`).Split(p, -1) {
			i := strings.Index(rest, part)
			if i < 0 {
				t.Errorf("%q: sample %q lacks %q", p, s, part)
				break
			}
			rest = rest[i+len(part):]
		}
	}
	for in, want := range map[string]string{"10.0.0.0/8": "10.0.0.0", "1.2.3.4": "1.2.3.4", "10.0.0.1-10.0.0.9": "10.0.0.1", "::1": "::1", "zzz": "zzz"} {
		if got := ipSample(in); got != want {
			t.Errorf("ipSample(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"a.b": `{"a":{"b":"x1"}}`, `a["b"]`: `{"a":{"b":"x1"}}`, "a[1]": `{"a":["x0","x1"]}`, "": ""} {
		if got := jsonPathLine(in); got != want {
			t.Errorf("jsonPathLine(%q) = %q, want %q", in, got, want)
		}
	}
}

// The comparison sees a wrong answer: for queries the plan answers as Loki does, a dropped line, a changed line,
// an extra label and a line Loki drops each show as their difference class.
//
// conformance: architecture-syntax-package-layering
func TestLokiDifferentialCatchesAWrongAnswer(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	answers := readDiffAnswers(t)
	prof := diffProfiles()[0]
	mutations := map[string]func(rows []map[string]string, loki []diffLokiEntry) ([]map[string]string, bool){
		"drops a line Loki keeps": func(rows []map[string]string, _ []diffLokiEntry) ([]map[string]string, bool) {
			return rows[1:], len(rows) > 0
		},
		"writes another line": func(rows []map[string]string, _ []diffLokiEntry) ([]map[string]string, bool) {
			if len(rows) == 0 {
				return nil, false
			}
			rows[0]["_msg"] += " changed"
			return rows, true
		},
		"adds a label Loki does not have": func(rows []map[string]string, _ []diffLokiEntry) ([]map[string]string, bool) {
			if len(rows) == 0 {
				return nil, false
			}
			rows[0]["zz_extra"] = "1"
			return rows, true
		},
	}
	tried := map[string]int{}
	for _, dq := range diffQueries(t) {
		rec := answers[dq.q]
		if rec == nil || rec.LokiErr != "" || dq.readsLevel || len(dq.fix.Entries) == 0 || len(dq.sel.Stages) != 0 {
			continue
		}
		// A bare selector: the handler sends it unchanged and every selected line is kept.
		run := runDiffHandler(t, prof, dq.q, nil, false)
		o, ok := findDiffOptions(dq, prof, diffSentText(run.sent))
		vl := rec.VL[prof.vlKey()]
		if !ok || vl == nil {
			continue
		}
		a, ok := vl.Answers[shortHash(o.plan)]
		if !ok || a.Error != "" {
			continue
		}
		if diffs := compareRun(dq, rec.Loki, runDiffHandler(t, prof, dq.q, vl.rows(a), false), false); len(diffs) != 0 {
			t.Fatalf("%s: the recorded answer differs from Loki before any change: %v", dq.q, diffs)
		}
		for class, mutate := range mutations {
			rows, ok := mutate(vl.rows(a), rec.Loki)
			if !ok {
				continue
			}
			tried[class]++
			if diffs := compareRun(dq, rec.Loki, runDiffHandler(t, prof, dq.q, rows, false), false); diffs[class] == "" {
				t.Errorf("%s: %s is not seen: %v", dq.q, class, diffs)
			}
		}
		// A line of the stream the selector does not select, answered as kept.
		for i, e := range rec.Loki {
			if !e.Selected {
				rows := append(vl.rows(a), vl.storedRows()[i])
				tried["keeps a line Loki drops"]++
				if diffs := compareRun(dq, rec.Loki, runDiffHandler(t, prof, dq.q, rows, false), false); diffs["keeps a line Loki drops"] == "" {
					t.Errorf("%s: a line of an unselected stream is not seen: %v", dq.q, diffs)
				}
				break
			}
		}
		if len(tried) == 4 && tried["drops a line Loki keeps"] >= 3 {
			break
		}
	}
	for _, class := range []string{"drops a line Loki keeps", "writes another line", "adds a label Loki does not have", "keeps a line Loki drops"} {
		if tried[class] == 0 {
			t.Errorf("no query to try %q on", class)
		}
	}
}
