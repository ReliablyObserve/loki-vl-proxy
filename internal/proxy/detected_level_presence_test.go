package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The presence probe is one stats query with a count per stored level field,
// per body key and for the escape flag, in mask order; the body counts look
// only at rows no definite stored level decides.
func TestLevelPresenceQuery(t *testing.T) {
	query := levelPresenceQuery(`env:="production"`)
	if n := strings.Count(query, "count() if ("); n != levelPresenceMaskLen() || n != 31 {
		t.Fatalf("%d counts, want 31 (16 stored fields, 14 body keys, 1 escape flag)", n)
	}
	for _, want := range []string{
		`env:="production" | format if (detected_level:* OR level:* OR LEVEL:* OR Level:* OR ` + "`log.level`" + `:* OR SEVERITY:* OR Severity:* OR SeverityText:* OR lvl:* OR LVL:* OR Lvl:* OR Severity_Text:* OR SEVERITY_TEXT:*) "1" as __p_h | stats `,
		`count() if (detected_level:*) as p0, count() if (level:*) as p1,`,
		`count() if (severity_number:*) as p15, count() if (-__p_h:* _msg:"level") as p16,`,
		`count() if (-__p_h:* _msg:"log" _msg:"level") as p19,`,
		`count() if (-__p_h:* _msg:"severity_text") as p27,`,
		`count() if (-__p_h:* _msg:~"\\\\u") as p30`,
	} {
		if !strings.Contains(query, want) {
			t.Errorf("probe lacks %s:\n%s", want, query)
		}
	}
	// severity, severity_text and severity_number do not decide a row by
	// themselves (a synthesised value does not), so they stay out of the filter
	// that restricts the body counts.
	for _, name := range []string{"severity:*", "severity_text:*", "severity_number:*"} {
		if strings.Contains(query[:strings.Index(query, " | stats ")], " "+name) || strings.Contains(query[:strings.Index(query, " | stats ")], "("+name) {
			t.Errorf("the deciding filter holds %s", name)
		}
	}
	if len(query) > 1500 {
		t.Errorf("probe is %d bytes", len(query))
	}
}

func TestLevelPresenceFromMask(t *testing.T) {
	mask := func(stored, body []string, escaped bool) string {
		m := []byte(strings.Repeat("0", levelPresenceMaskLen()))
		for _, name := range stored {
			m[indexOf(levelPresenceStored, name)] = '1'
		}
		for _, key := range body {
			m[len(levelPresenceStored)+indexOf(levelPresenceBody, key)] = '1'
		}
		if escaped {
			m[len(m)-1] = '1'
		}
		return string(m)
	}
	for _, tc := range []struct {
		name         string
		mask         string
		stored, body []string
	}{
		{"nothing", mask(nil, nil, false), nil, nil},
		{"level field and key", mask([]string{"level"}, []string{"level"}, false), []string{"level"}, []string{"level"}},
		{"priority order", mask([]string{"severity_number", "level", "detected_level"}, []string{"lvl", "severity"}, false),
			[]string{"detected_level", "level", "severity_number"}, []string{"severity", "lvl"}},
		// unpack_json decodes an escaped key, which no word test sees.
		{"escape makes every key present", mask([]string{"level"}, nil, true), []string{"level"}, translator.DetectedLevelBodyKeys()},
	} {
		got := levelPresenceFromMask(tc.mask)
		if !reflect.DeepEqual(got.Stored, tc.stored) || !reflect.DeepEqual(got.BodyKeys, tc.body) {
			t.Errorf("%s: got %v / %v, want %v / %v", tc.name, got.Stored, got.BodyKeys, tc.stored, tc.body)
		}
	}
	if got := orMasks("0101", "0011"); got != "0111" {
		t.Errorf("orMasks = %s", got)
	}
	if got := orMasks("", "0011"); got != "0011" {
		t.Errorf("orMasks with the empty mask = %s", got)
	}
}

func indexOf(list []string, name string) int {
	for i, item := range list {
		if item == name {
			return i
		}
	}
	panic(name)
}

// A key that occurs on a single late line of the window is still read: the
// pruned chain lists every key the window holds, so the answer is Loki's.
// conformance: semantics/json-metric-detected-level-derivation
func TestDetectedLevelPresenceKeepsAKeyOfOneLateLine(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 40; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(6*time.Minute + time.Duration(i)*time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"level":"info","n":` + strconv.Itoa(i) + `}`})
	}
	rows = append(rows, pushdownRow{ts: s0.Add(9*time.Minute + 30*time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"severity":"ERROR","n":41}`})
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.presence = true
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	end := s0.Add(10 * time.Minute)
	code, got, body := detectedLevelQuery(t, p, `sum by (detected_level) (count_over_time({env="prod"} | json | drop __error__ [5m]))`, end, end, time.Minute)
	want := map[string]map[int64]string{
		canonicalLabelsKey(map[string]string{"detected_level": "info"}):  {end.Unix(): "40"},
		canonicalLabelsKey(map[string]string{"detected_level": "error"}): {end.Unix(): "1"},
	}
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stats) != 1 || !strings.Contains(fake.stats[0], `fields (level, severity) result_prefix "__j_"`) {
		t.Fatalf("expected the chain to unpack exactly level and severity: %v", fake.stats)
	}
	if fake.raw != 0 {
		t.Fatalf("the raw-row evaluator answered (%d)", fake.raw)
	}
}

// A probe that fails, or answers with something other than its counts, leaves
// the full chain (correct, slower), and nothing is remembered.
// conformance: semantics/json-metric-detected-level-derivation
func TestDetectedLevelPresenceProbeFailureKeepsFullChain(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(2 * time.Minute)
	row := pushdownRow{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"msg":"x","severity":"error"}`}
	want := map[string]map[int64]string{canonicalLabelsKey(map[string]string{"detected_level": "error"}): {end.Unix(): "1"}}
	for _, tc := range []struct {
		name string
		set  func(f *pushdownFakeVL)
	}{
		{"backend error", func(f *pushdownFakeVL) { f.presence, f.presenceFail = true, true }},
		{"no counts", func(f *pushdownFakeVL) {}},
		{"short row", func(f *pushdownFakeVL) {
			f.presence, f.presenceLine = true, func(string) string { return `{"p0":"1"}` + "\n" }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, []pushdownRow{row}, nil)
			tc.set(fake)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			if code, got, body := detectedLevelQuery(t, p, `sum by (detected_level) (count_over_time({env="prod"} | json | drop __error__ [5m]))`, end, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
			}
			calls := func() (probes int, statsQueries []string) {
				fake.mu.Lock()
				defer fake.mu.Unlock()
				return len(fake.presenceCalls), append([]string(nil), fake.stats...)
			}
			probes, stats := calls()
			for _, q := range stats {
				if !strings.Contains(q, levelChain) {
					t.Fatalf("a stats query without the full chain:\n%s", q)
				}
			}
			if probes != 1 {
				t.Fatalf("%d presence probes, want 1", probes)
			}
			// Nothing was remembered: asking again probes again.
			fake.presenceFail = false
			p.detectedLevelPresence(context.Background(), presenceBase(levelPresenceQuery(`app:="api"`)), s0, end)
			if probes, _ = calls(); probes != 2 {
				t.Fatalf("%d presence probes after the second request, want 2", probes)
			}
		})
	}
}

// A mask that says a key is absent is only as good as the part of the window
// it was measured over: lines for recent timestamps may still arrive, so the
// part of a window younger than statsPushdownRiskSettle is probed on every
// request and never remembered. A line that appears there after an earlier
// request is seen by the next.
// conformance: semantics/json-metric-detected-level-derivation
func TestDetectedLevelPresenceNeverTrustsRecentAbsence(t *testing.T) {
	now := time.Now()
	var lines []jsonVolumeLine
	for i := 0; i < 10; i++ {
		lines = append(lines, jsonVolumeLine{ts: now.Add(-9*time.Minute + time.Duration(i)*time.Second), msg: `{"level":"info","n":` + strconv.Itoa(i) + `}`})
	}
	srv, fake := newJSONVolumeFakeVL(t, lines)
	fake.presence = true
	p := newSlidingTestProxy(t, srv.URL)
	start, end := now.Add(-10*time.Minute), now.Add(-time.Minute)
	ask := func() *translator.DetectedLevelPresence {
		t.Helper()
		got := p.detectedLevelPresence(context.Background(), `app:="api"`, start, end)
		if got == nil {
			t.Fatal("no presence")
		}
		return got
	}
	if got := ask(); !reflect.DeepEqual(got.BodyKeys, []string{"level"}) || len(got.Stored) != 0 {
		t.Fatalf("first request: %v / %v", got.Stored, got.BodyKeys)
	}
	// A line for a recent timestamp inside the window, with a key none had.
	fake.mu.Lock()
	fake.lines = append(fake.lines, jsonVolumeLine{ts: now.Add(-3 * time.Minute), msg: `{"severity":"error"}`})
	fake.mu.Unlock()
	if got := ask(); !reflect.DeepEqual(got.BodyKeys, []string{"level", "severity"}) {
		t.Fatalf("a key that first appeared in recent lines was missed: %v", got.BodyKeys)
	}
}

// The part of a window that was checked long ago must not look checked now:
// extending a remembered window leaves the entry's expiry where the first
// check set it, so a window kept alive by a refreshing dashboard is still
// checked again in full once the entry ages out.
func TestCoveredWindowProbeKeepsTheAgeOfTheCheck(t *testing.T) {
	old := statsPushdownRiskCacheTTL
	statsPushdownRiskCacheTTL = 400 * time.Millisecond
	defer func() { statsPushdownRiskCacheTTL = old }()
	srv, _ := newJSONVolumeFakeVL(t, nil)
	p := newSlidingTestProxy(t, srv.URL)
	s0 := time.Unix(1700000400, 0).UTC()
	var checked []time.Duration // the length of each window a check ran over
	probe := func(to time.Duration) {
		t.Helper()
		if _, _, err := p.coveredWindowProbe(context.Background(), "age-test", s0, s0.Add(to), func(from, end time.Time, acc string) (string, bool, error) {
			checked = append(checked, end.Sub(from))
			return acc, false, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	probe(30 * time.Minute)
	time.Sleep(250 * time.Millisecond)
	probe(40 * time.Minute) // extends: only the new ten minutes are checked
	time.Sleep(200 * time.Millisecond)
	probe(40 * time.Minute) // the entry is 450 ms old: checked again in full
	want := []time.Duration{30 * time.Minute, 10 * time.Minute, 40 * time.Minute}
	if !reflect.DeepEqual(checked, want) {
		t.Fatalf("checked windows %v, want %v", checked, want)
	}
}

// The native route (a plain selector grouped by detected_level, what Logs
// Drilldown sends) gets its chain from the translator in full; the request
// that carries it is narrowed to the window's fields and keys on its way to
// VictoriaLogs, and the answer does not change.
// conformance: semantics/detected-level-pipes-known-differences
func TestDetectedLevelPresenceNativeRoute(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	query := `sum by (detected_level) (count_over_time({app="api"}[1m]))`
	run := func(presence bool) (map[string]map[int64]string, []string, int) {
		srv, fake := newJSONVolumeFakeVL(t, jsonVolumeFixture(s0))
		fake.presence = presence
		p := newSlidingTestProxy(t, srv.URL)
		got := runJSONVolumeQueryRange(t, p, query, s0.Add(time.Minute), s0.Add(5*time.Minute), time.Minute)
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return got, append([]string(nil), fake.statsCalls...), len(fake.presenceCalls)
	}
	full, fullStats, _ := run(false)
	pruned, prunedStats, probes := run(true)
	if !reflect.DeepEqual(pruned, full) {
		t.Fatalf("pruned chain changed the answer\n got: %v\nwant: %v", pruned, full)
	}
	if probes != 1 || len(prunedStats) != 1 || len(fullStats) != 1 {
		t.Fatalf("presence probes %d, stats queries %d (full %d)", probes, len(prunedStats), len(fullStats))
	}
	if len(prunedStats[0])*3 > len(fullStats[0]) || !strings.Contains(prunedStats[0], `fields (level) result_prefix "__j_"`) {
		t.Fatalf("the chain was not pruned (%d bytes against %d):\n%s", len(prunedStats[0]), len(fullStats[0]), prunedStats[0])
	}
}

// A dashboard refreshing a relative range moves its window forward every few
// seconds: a refresh checks only the part younger than the settle horizon
// (plus what the clock moved), not the hour behind it.
func TestCoveredWindowProbeRefreshChecksOnlyTheRecentTail(t *testing.T) {
	srv, _ := newJSONVolumeFakeVL(t, nil)
	p := newSlidingTestProxy(t, srv.URL)
	now := time.Now()
	var checked []time.Duration
	probe := func(shift time.Duration) {
		t.Helper()
		if _, _, err := p.coveredWindowProbe(context.Background(), "refresh-test", now.Add(-time.Hour+shift), now.Add(shift), func(from, end time.Time, acc string) (string, bool, error) {
			checked = append(checked, end.Sub(from))
			return acc, false, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	probe(0)
	probe(5 * time.Second)
	probe(10 * time.Second)
	probe(15 * time.Second)
	if len(checked) != 4 || checked[0] != time.Hour {
		t.Fatalf("checked windows %v", checked)
	}
	for i, got := range checked[1:] {
		// the settle horizon, the seconds the window moved, and the step's slack
		if limit := statsPushdownRiskSettle + time.Duration(i+1)*5*time.Second + time.Second; got > limit {
			t.Errorf("refresh %d checked %v, more than the %v the horizon allows", i+1, got, limit)
		}
	}
}

// Two callers of one tenant who may see different data (forwarded
// credentials, routing) never share a remembered window: a mask or verdict
// measured for the narrower scope says nothing about the wider.
func TestCoveredWindowProbeKeepsScopesApart(t *testing.T) {
	srv, _ := newJSONVolumeFakeVL(t, nil)
	p := newSlidingTestProxy(t, srv.URL)
	p.forwardHeaders = []string{"Authorization"}
	scoped := func(token string) context.Context {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Scope-OrgID", "0")
		r.Header.Set("Authorization", token)
		return withOrgID(r).Context()
	}
	s0 := time.Unix(1700000400, 0).UTC()
	checks := 0
	probe := func(ctx context.Context) {
		t.Helper()
		if _, _, err := p.coveredWindowProbe(ctx, "scope-test", s0, s0.Add(30*time.Minute), func(_, _ time.Time, acc string) (string, bool, error) {
			checks++
			return acc, false, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	probe(scoped("Bearer narrow"))
	probe(scoped("Bearer wide"))
	probe(scoped("Bearer narrow"))
	if checks != 2 {
		t.Fatalf("%d checks, want 2: the second scope must not reuse the first's window, the third must reuse its own", checks)
	}
}

// A request holding the chain twice (two segments, perhaps on different
// bases) keeps both full: the mask of the first base says nothing about the
// second.
func TestPruneLevelChainLeavesTwoChainsAlone(t *testing.T) {
	srv, fake := newJSONVolumeFakeVL(t, nil)
	fake.presence = true
	p := newSlidingTestProxy(t, srv.URL)
	chain := fullLevelChain()
	for _, tc := range []struct {
		name, query string
		pruned      bool
	}{
		{"one chain", `app:="a" ` + chain + ` | stats by (detected_level) count()`, true},
		{"two chains", `app:="a" ` + chain + ` | stats by (detected_level) count() ||| app:="b" ` + chain + ` | stats by (detected_level) count()`, false},
	} {
		params := url.Values{"query": {tc.query}, "start": {"2023-11-14T22:00:00Z"}, "end": {"2023-11-14T22:30:00Z"}}
		got := p.pruneLevelChain(context.Background(), "/select/logsql/stats_query_range", params).Get("query")
		if (got != tc.query) != tc.pruned {
			t.Errorf("%s: pruned=%v, want %v", tc.name, got != tc.query, tc.pruned)
		}
	}
	// an instant query has no window to probe
	params := url.Values{"query": {`app:="a" ` + chain + ` | stats by (detected_level) count()`}, "time": {"2023-11-14T22:30:00Z"}}
	if got := p.pruneLevelChain(context.Background(), "/select/logsql/stats_query", params); got.Get("query") != params.Get("query") {
		t.Errorf("an instant query was changed")
	}
}
