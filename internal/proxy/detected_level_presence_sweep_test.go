package proxy

import (
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// Every position of the presence mask is exercised alone: a window holding
// only a stored field (16 of them) or only a body key (14), or only an escape
// sequence, must get exactly that field or key in its pruned chain, from the
// one probe and with no second request. A probe that under-reports one
// position (a wrong word, a wrong index, a mask forced to 0 there) makes the
// pruned chain miss it; the witness then flags the row and the request is
// asked again with the full chain, which is the right answer but a second
// chain query, so both the extra query and the changed chain fail the test.
// The expected answers are Loki v3.7.7's (a level key or field of the allowed
// list, any of its spellings, is read: "dbg" is debug), written out here, and
// the fake VictoriaLogs counts with its own copies of the probe's tables
// (fakePresenceStored and friends).
// conformance: semantics/json-metric-detected-level-derivation
func TestLevelPresenceSweepOnePositionAtATime(t *testing.T) {
	type position struct {
		name string
		vl   map[string]string
		msg  string
		// stored, body: the pruned chain's fields and keys; answer: Loki's
		// detected_level ("" when the line is no Loki answer to check)
		stored, body []string
		answer       string
	}
	var cases []position
	for _, name := range []string{
		"detected_level", "level", "LEVEL", "Level", "log.level", "severity", "SEVERITY", "Severity", "SeverityText",
		"lvl", "LVL", "Lvl", "severity_text", "Severity_Text", "SEVERITY_TEXT", "severity_number",
	} {
		value := "dbg"
		if name == "severity_number" {
			value = "5" // OTLP DEBUG
		}
		cases = append(cases, position{name: "stored " + name, vl: map[string]string{name: value}, msg: `{"n":1}`, stored: []string{name}, answer: "debug"})
	}
	for _, key := range []string{
		"level", "LEVEL", "Level", "severity", "SEVERITY", "Severity", "SeverityText",
		"lvl", "LVL", "Lvl", "severity_text", "Severity_Text", "SEVERITY_TEXT",
	} {
		cases = append(cases, position{name: "body " + key, msg: `{"` + key + `":"dbg"}`, body: []string{key}, answer: "debug"})
	}
	// a nested key holds the words of both log and level
	cases = append(cases, position{name: "body log.level", msg: `{"log":{"level":"dbg"}}`, body: []string{"level", "log.level"}, answer: "debug"})
	// an escape can spell any key, so every key is read
	cases = append(cases, position{name: "escape", msg: `{"l\u0065vel":"dbg"}`,
		body: []string{"level", "LEVEL", "Level", "log.level", "severity", "SEVERITY", "Severity", "SeverityText", "lvl", "LVL", "Lvl", "severity_text", "Severity_Text", "SEVERITY_TEXT"}})
	if len(cases) != 16+14+1 {
		t.Fatalf("%d positions, want 31", len(cases))
	}
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(2 * time.Minute)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := pushdownRow{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, vl: tc.vl, msg: tc.msg}
			srv, fake := newPushdownFakeVL(t, []pushdownRow{row}, nil)
			fake.presence = true
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			code, got, body := detectedLevelQuery(t, p, `sum by (detected_level) (count_over_time({env="prod"}[5m]))`, end, end, time.Minute)
			if code != http.StatusOK {
				t.Fatalf("status %d %.200s", code, body)
			}
			if tc.answer != "" {
				want := map[string]map[int64]string{canonicalLabelsKey(map[string]string{"detected_level": tc.answer}): {end.Unix(): "1"}}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			}
			fake.mu.Lock()
			chain := chainStatsCalls(fake.stats)
			fake.mu.Unlock()
			if len(chain) != 1 {
				t.Fatalf("%d chain queries, want 1 (the probe left something out and the witness asked again)", len(chain))
			}
			present := chainPresence(chain[0])
			stored := slices.DeleteFunc(slices.Clone(present.Stored), func(s string) bool { return s == noSeverityNamePipes })
			if !sameSet(stored, tc.stored) || !sameSet(present.BodyKeys, tc.body) {
				t.Fatalf("pruned chain reads stored %v and body keys %v, want %v and %v", stored, present.BodyKeys, tc.stored, tc.body)
			}
			if strings.Contains(chain[0], levelChain) {
				t.Fatalf("the full chain was sent")
			}
		})
	}
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}
