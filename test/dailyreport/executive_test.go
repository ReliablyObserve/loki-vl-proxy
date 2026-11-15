package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// roundTrip puts a summary through its JSON, as the previous day's summary.json is read: counts become floats and
// maps become any, exactly as in a committed file.
func roundTrip(t testing.TB, s Summary) Summary {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var out Summary
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	out.detail = s.detail
	return out
}

func casesDoc(t testing.TB, verdicts map[string]map[string]string, flags map[string]map[string][]string, status map[string]string) *CasesFile {
	t.Helper()
	runs := map[string]any{}
	cases := map[string]map[string]any{}
	for v, byCase := range verdicts {
		runs[v] = map[string]any{"flags": flags[v]}
		for id, verdict := range byCase {
			if cases[id] == nil {
				cases[id] = map[string]any{"expected": "match", "gap_status": status[id]}
			}
			cases[id][v] = map[string]any{"verdict": verdict}
		}
	}
	return ParseCases(map[string]any{"runs": runs, "cases": cases})
}

// day builds one day's summary and its per-case file. today is the later of the two fixtures.
func day(t testing.TB, today bool) (Summary, *CasesFile) {
	t.Helper()
	s := Summary{GeneratedBy: "test", Date: "2026-11-14", Commit: "aaaaaaaaaaaaaaaa", Loki: "3.7.9", VictoriaLogs: []string{"v1.49.0", "v1.53.0"},
		Cases: map[string]any{}, Reasons: []string{
			"scheduled workflow parity-refresh.yaml: last scheduled run 2026-11-13 ended failure",
			"parity slice: 2 difference cluster(s) are claimed by no registry case",
			"perf smoke: 1 row(s) above 2x Loki",
			"b/old differs from Loki on VictoriaLogs v1.49.0, and the registry holds it fixed",
		}, Red: true}
	s.Registry = RegistryPart{Cases: 10, ByStatus: map[string]int{"fixed": 3, "no gap": 2, "open": 3, "documented": 1, "owner-kept": 0, "upstream": 1},
		OpenByImpact: map[string]int{"explore-visible": 2, "edge": 1},
		CaseStatus: map[string]string{"a/1": "open", "a/2": "open", "a/3": "open", "a/4": "fixed", "a/5": "fixed", "a/6": "fixed", "a/7": "no gap", "a/8": "no gap",
			"a/9": "documented", "a/10": "upstream"}}
	verdicts := map[string]map[string]string{
		"v1.49.0": {"a/1": "empty", "a/2": "differs", "a/4": "matches", "a/7": "matches", "a/9": "blocked"},
		"v1.53.0": {"a/1": "empty", "a/2": "differs", "a/4": "matches", "a/7": "matches", "a/9": "blocked"},
	}
	flags := map[string]map[string][]string{
		"v1.49.0": {"regression": {"b/old"}, "proposed_close": {"a/3"}, "behaviour_changed": {}},
		"v1.53.0": {"regression": {}, "proposed_close": {"a/3"}, "behaviour_changed": {}},
	}
	for v := range verdicts {
		counts := map[string]int{}
		for _, verdict := range verdicts[v] {
			counts[verdict]++
		}
		s.Cases[v] = map[string]any{"counts": counts, "regressions": len(flags[v]["regression"]), "proposed_close": len(flags[v]["proposed_close"]),
			"reuse": &CaseReuse{Counts: map[string]int{"reused": 4, "cache_not_reused": 1}, ColdVLCalls: 9, RepeatCalls: 2}}
	}
	s.Errors = &ErrorsPart{Live: &LiveErrors{Rows: 20, Changed: 1}}
	s.Errors.Recorded.ComparableRows, s.Errors.Recorded.MatchingRows, s.Errors.Recorded.KnownDifferences = 20, 18, 2
	replay := true
	s.Errors.ReplayOK = &replay
	s.Sweep = &SweepPart{Queries: 1000, LokiAnswers: map[string]int{"accepted": 700, "rejected": 300}, Differences: map[string]int{
		kindVLRejects: 20, kindTranslateFails: 5, "proxy accepts, Loki rejects": 7, "error text differs": 10, kindTreeFact + ": offsets": 3, kindTreeFact + ": binary operation": 2}}
	s.Parity = map[string]any{"clusters": 5, "requests": 40, "unclaimed": 2, "exit": 0, "unclaimed_ids": []string{"c1", "c2"},
		"verdicts": map[string]any{"same": 20, "differ": 10, "both_empty": 8, "blocked": 2}}
	s.PerfDaily = map[string]any{"date": "2026-11-14", "rows": 30, "comparable": 55, "ratchet_lost": []any{}, "reuse_lost": []any{},
		"reuse": map[string]any{"rows": 30, "cold_vl_calls": 50, "repeat_vl_calls": 40, "repeat_without_vl_calls": 3},
		"distribution": map[string]any{
			"cold": map[string]any{"rows": 20, "green": 17, "below": 8, "close": 9, "between": 2, "above": 1},
			"warm": map[string]any{"rows": 35, "green": 35, "below": 5, "close": 30, "between": 0, "above": 0}},
		"over_2x": []any{map[string]any{"set": "s1", "shape": "Q1", "range": "1h", "mode": "cold", "profile": "p", "loki_s": 0.1, "proxy_s": 0.5}}}
	s.PerfWeekly = &WeeklyPerf{Date: "2026-11-09", Loki: "3.7.9", Pairs: 100, Faster: 60, Par: 30, Slower: 10, Over2xRows: 3}
	s.Workflows = &WorkflowPart{CheckedAt: "2026-11-14T08:00:00Z", Scheduled: 10, Problems: []map[string]any{
		{"workflow": "parity-refresh.yaml", "problem": "last scheduled run 2026-11-13 ended failure"}}}
	// Coverage by state: the day before held fewer cases proven live and no case proven by a test.
	s.Coverage = map[string]*CoveragePart{}
	for v := range verdicts {
		s.Coverage[v] = &CoveragePart{Cases: 5, ProvenLive: 3, ProvenByTest: 1, NotProven: 1, NotProvenBy: map[string]int{"empty": 1},
			CI: &CIRun{Workflow: "ci.yaml", Conclusion: "success", Date: "2026-11-14", URL: "https://x/ci"}}
	}
	if today {
		s.Date, s.Commit = "2026-11-15", "bbbbbbbbbbbbbbbb"
		s.Reasons = []string{
			"scheduled workflow parity-refresh.yaml: last scheduled run 2026-11-14 ended failure",
			"scheduled workflow perf-daily.yaml: last scheduled run 2026-11-09 ended failure",
			"parity slice: 1 difference cluster(s) are claimed by no registry case",
			"perf smoke: 2 row(s) above 2x Loki",
			"a/4 differs from Loki on VictoriaLogs v1.53.0, and the registry holds it fixed",
		}
		s.Registry.ByStatus = map[string]int{"fixed": 4, "no gap": 2, "open": 2, "documented": 1, "upstream": 1}
		s.Registry.OpenByImpact = map[string]int{"explore-visible": 1, "edge": 1}
		s.Registry.CaseStatus["a/3"] = "fixed"
		s.Registry.CaseStatus["a/11"] = "open"
		s.Registry.Cases = 11
		s.Registry.ByStatus["open"]++
		s.Registry.OpenByImpact["edge"]++
		verdicts["v1.49.0"]["a/2"] = "matches"
		verdicts["v1.53.0"]["a/2"] = "matches"
		verdicts["v1.53.0"]["a/4"] = "differs"
		verdicts["v1.49.0"]["a/3"] = "matches"
		verdicts["v1.53.0"]["a/3"] = "matches"
		flags["v1.49.0"] = map[string][]string{"regression": {}, "proposed_close": {"a/2"}, "behaviour_changed": {}}
		flags["v1.53.0"] = map[string][]string{"regression": {"a/4"}, "proposed_close": {"a/2", "a/3"}, "behaviour_changed": {"a/9"}}
		for v := range verdicts {
			counts := map[string]int{}
			for _, verdict := range verdicts[v] {
				counts[verdict]++
			}
			s.Cases[v] = map[string]any{"counts": counts, "regressions": len(flags[v]["regression"]), "proposed_close": len(flags[v]["proposed_close"]),
				"reuse": &CaseReuse{Counts: map[string]int{"reused": 5, "cache_not_reused": 0, "not_measured": 1}, ColdVLCalls: 9, RepeatCalls: 0,
					Background: 3, CallsFromHeader: 5}}
		}
		for v := range verdicts {
			s.Coverage[v] = &CoveragePart{Cases: 6, ProvenLive: 4, ProvenByTest: 1, NotProven: 1, NotProvenBy: map[string]int{"empty": 1},
				CI: &CIRun{Workflow: "ci.yaml", Conclusion: "success", Date: "2026-11-15", URL: "https://x/ci"}}
		}
		s.Workflows.Pending = []map[string]any{{"workflow": "new-weekly.yaml", "problem": "added 2026-11-14, pending its first scheduled run"}}
		s.Errors.Recorded.MatchingRows, s.Errors.Recorded.KnownDifferences = 19, 1
		s.Errors.Live = &LiveErrors{Rows: 20, Changed: 0, OK: true}
		s.Sweep.Differences = map[string]int{kindVLRejects: 18, "proxy accepts, Loki rejects": 7, "error text differs": 10, "proxy rejects, Loki accepts": 4,
			kindTreeFact + ": offsets": 3}
		s.Parity["unclaimed"], s.Parity["unclaimed_ids"] = 1, []string{"c2", "c9"}
		s.Parity["verdicts"] = map[string]any{"same": 24, "differ": 6, "both_empty": 8, "blocked": 2}
		s.PerfDaily["over_2x"] = []any{
			map[string]any{"set": "s1", "shape": "Q1", "range": "1h", "mode": "cold", "profile": "p", "loki_s": 0.1, "proxy_s": 0.5},
			map[string]any{"set": "s2", "shape": "Q7", "range": "6h", "mode": "cold", "profile": "p", "loki_s": 0.2, "proxy_s": 0.6}}
		s.PerfDaily["distribution"].(map[string]any)["cold"] = map[string]any{"rows": 20, "green": 18, "below": 9, "close": 9, "between": 0, "above": 2}
		s.Workflows.Problems = []map[string]any{
			{"workflow": "parity-refresh.yaml", "problem": "last scheduled run 2026-11-14 ended failure"},
			{"workflow": "perf-daily.yaml", "problem": "last scheduled run 2026-11-09 ended failure"}}
		s.Workflows.Scheduled = 10
	}
	return s, casesDoc(t, verdicts, flags, s.Registry.CaseStatus)
}

func fixtures(t testing.TB) (cur Summary, prev *Previous) {
	t.Helper()
	cur, curCases := day(t, true)
	cur.detail = curCases
	p, pCases := day(t, false)
	p.detail = pCases
	return roundTrip(t, cur), &Previous{Summary: ptr(roundTrip(t, p)), Cases: pCases}
}

func ptr[T any](v T) *T { return &v }

func TestExecutiveGolden(t *testing.T) {
	cur, prev := fixtures(t)
	got := Executive(cur, prev)
	golden := filepath.Join("testdata", "executive.golden.md")
	if os.Getenv("LVP_GOLDEN_UPDATE") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("the executive summary differs from %s (LVP_GOLDEN_UPDATE=1 rewrites it):\n%s", golden, got)
	}
}

func TestPercentagesRound(t *testing.T) {
	for _, c := range []struct {
		good, total int
		want        string
	}{
		{0, 0, "n/a"}, {5, 0, "n/a"}, {0, 10, "0.0%"}, {10, 10, "100.0%"}, {1, 3, "33.3%"}, {2, 3, "66.7%"},
		{1, 16, "6.3%"}, {1, 8, "12.5%"}, {1, 3000, "<0.1%"}, {1, 2000, "0.1%"}, {1, 1000, "0.1%"}, {1995, 2000, "99.8%"}, {1994, 2000, "99.7%"},
	} {
		if got := pct(c.good, c.total); got != c.want {
			t.Errorf("pct(%d, %d) = %s, want %s", c.good, c.total, got, c.want)
		}
	}
	if got := signedTenths(8); got != "+0.8" {
		t.Errorf("signedTenths(8) = %s", got)
	}
	if got := signedTenths(-15); got != "-1.5" {
		t.Errorf("signedTenths(-15) = %s", got)
	}
}

func TestTheTableRowsAreComputedFromTheNumbers(t *testing.T) {
	cur, _ := fixtures(t)
	byKey := map[string]row{}
	for _, r := range rows(cur) {
		byKey[r.key] = r
	}
	check := func(key string, good, bad, total int, pct string) {
		t.Helper()
		r, ok := byKey[key]
		if !ok {
			t.Fatalf("no row %s", key)
		}
		if r.good != good || r.bad != bad || r.total != total || r.pct() != pct {
			t.Errorf("%s: good %d bad %d total %d (%s), want %d %d %d (%s)", key, r.good, r.bad, r.total, r.pct(), good, bad, total, pct)
		}
	}
	// fixed 4 + no gap 2 of 11 cases; open 3 (2 + the new one).
	check("registry", 6, 3, 11, "54.5%")
	// v1.49.0 proves a/2, a/3, a/4, a/7: 4 matches, no differs; v1.53.0 holds a/4 as differs.
	check("verdicts:v1.49.0", 4, 0, 4, "100.0%")
	check("verdicts:v1.53.0", 3, 1, 4, "75.0%")
	check("coverage:v1.49.0", 5, 1, 6, "83.3%")
	check("errors", 19, 1, 20, "95.0%")
	check("sweep", 958, 42, 1000, "95.8%")
	check("parity", 24, 6, 40, "60.0%")
	check("perf:cold", 18, 2, 20, "90.0%")
	check("perf:warm", 35, 0, 35, "100.0%")
	check("weekly", 90, 10, 100, "90.0%")
	check("cache:v1.49.0", 5, 0, 5, "100.0%")
	check("workflows", 8, 2, 10, "80.0%")
	if r := byKey["perf:cold"]; r.needed != "1 green row more; 2 above 2x to fix" {
		t.Errorf("perf:cold needed = %q (95%% of 20 is 19 green, 18 are)", r.needed)
	}
	if r := byKey["perf:warm"]; !r.met || r.needed != "none" {
		t.Errorf("perf:warm should meet the target: %+v", r)
	}
	if r := byKey["registry"]; r.needed != "3 open cases to close" || r.met {
		t.Errorf("registry: %+v", r)
	}
	if r := byKey["workflows"]; !strings.Contains(r.needed, "parity-refresh.yaml, perf-daily.yaml") {
		t.Errorf("workflows needed = %q", r.needed)
	}
}

func TestChangesSincePreviousReport(t *testing.T) {
	cur, prev := fixtures(t)
	out := Executive(cur, prev)
	for _, want := range []string{
		"#### What changed since 2026-11-14",
		"Main `bbbbbbbbbbbb` (was `aaaaaaaaaaaa`).",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// A number that did not move is not listed.
	if strings.Contains(out, "| Registry: documented |") || strings.Contains(out, "| Parity slice: requests |") {
		t.Errorf("a number that did not move is listed:\n%s", out)
	}
	// The numbers that moved, old to new.
	for _, want := range []string{"| Registry: cases | 10 | 11 |", "| Registry: fixed | 3 | 4 |", "| Error answers: matching | 18 | 19 |",
		"| Grammar sweep: proxy rejects, Loki accepts | 0 | 4 |", "| Grammar sweep: accepted, translation fails | 5 | 0 |",
		"| Parity slice: unclaimed clusters | 2 | 1 |", "| Daily perf smoke: above 2x | 1 | 2 |", "| Scheduled workflows: problems | 1 | 2 |",
		"| Cache reuse v1.49.0: VictoriaLogs calls on repeats | 2 | 0 |"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing moved number %q", want)
		}
	}
	for _, want := range []string{
		// red items: perf-daily is new, b/old resolved, the counts moved.
		"**New red items** (2)",
		"scheduled workflow perf-daily.yaml: last scheduled run 2026-11-09 ended failure",
		"a/4 differs from Loki on VictoriaLogs v1.53.0, and the registry holds it fixed",
		"**Red items resolved** (1)",
		"b/old differs from Loki on VictoriaLogs v1.49.0, and the registry holds it fixed",
		"**Red items that changed** (3)",
		"perf smoke: 2 row(s) above 2x Loki (was: perf smoke: 1 row(s) above 2x Loki)",
		// verdicts per version
		"**Cases whose verdict changed** (",
		"a/2: differs to matches (v1.49.0, v1.53.0)",
		"a/4: matches to differs (v1.53.0)",
		// flags
		"**New regressions** (1)", "- a/4 (v1.53.0)",
		"**Regressions resolved** (1)", "- b/old (v1.49.0)",
		"**New proposed closes** (",
		"- a/2 (v1.49.0, v1.53.0)", "**No longer proposed for close** (1)", "- a/3 (v1.49.0)",
		"**Accepted behaviour newly changed** (1)",
		// registry moves
		"**Registry status moves** (2)", "- a/11: new case, open", "- a/3: open to fixed",
		// perf rows
		"**Perf rows newly above 2x Loki** (1)", "s2 / Q7, 6h, cold, p: Loki 0.2 s, proxy 0.6 s, 3.0x",
		// clusters
		"**New unclaimed parity clusters** (1)", "- c9", "**Parity clusters now claimed or gone** (1)", "- c1",
		// workflows
		"**Scheduled workflows newly failing** (1)", "- perf-daily.yaml: last scheduled run 2026-11-09 ended failure",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Not compared") {
		t.Errorf("everything is comparable here, yet:\n%s", out)
	}
	// The table's change column.
	for _, want := range []string{"| Registry cases | 6 | 3 | 54.5% | 0 open | 3 open cases to close | good +1, total 10 to 11, +4.5 pt |"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing change cell %q", want)
		}
	}
}

func TestNoPreviousReport(t *testing.T) {
	cur, _ := fixtures(t)
	out := Executive(cur, nil)
	if !strings.Contains(out, "#### What changed\n\nNo earlier report to compare with.\n") {
		t.Errorf("the one-line answer is missing:\n%s", out)
	}
	if strings.Contains(out, "What changed since") || strings.Contains(out, "| new |") {
		t.Errorf("a comparison is shown without a previous report")
	}
	if !strings.Contains(out, "| Change since the previous report |") || !strings.Contains(out, "| - |") {
		t.Errorf("the change column should hold dashes:\n%s", out)
	}
}

func TestAnOldSummaryWithoutTheNewFieldsStillCompares(t *testing.T) {
	cur, _ := fixtures(t)
	// summary.json as committed before the executive summary: no case_status, no parity verdicts, no cluster ids.
	old := `{"date":"2026-11-14","commit":"abc","loki_version":"3.7.9","victorialogs_versions":["v1.49.0"],"red":true,"reasons":["perf smoke: 1 row(s) above 2x Loki"],
		"registry":{"cases":3,"by_status":{"open":1,"fixed":2},"open_by_impact":{"edge":1}},
		"cases":{"v1.49.0":{"counts":{"matches":2,"differs":1},"regressions":0,"proposed_close":0}},
		"parity":{"clusters":4,"requests":10,"unclaimed":1},
		"perf_daily":{"rows":3,"over_2x":[]}}`
	var prevSummary Summary
	if err := json.Unmarshal([]byte(old), &prevSummary); err != nil {
		t.Fatal(err)
	}
	out := Executive(cur, &Previous{Summary: &prevSummary})
	for _, want := range []string{
		"Not compared:", "per-case verdicts and flags", "registry status moves", "unclaimed parity clusters",
		"| Parity slice | 24 | 6 | 60.0% | 100% same, 0 unclaimed clusters | 6 differing requests to fix, 1 cluster to claim | total 10 to 40 |",
		"| Registry cases | 6 | 3 | 54.5% | 0 open | 3 open cases to close | good +4, not good +2, total 3 to 11, ",
		"| Error answers | 19 | 1 | 95.0% | 100% match | 1 answer to align, 0 live change(s) to review | new |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// The earlier day's perf rows were recorded: the new row above 2x is listed.
	if !strings.Contains(out, "**Perf rows newly above 2x Loki** (2)") {
		t.Errorf("an empty earlier list is a list:\n%s", out)
	}
}

func TestZeroDenominatorsAreNotAvailableNotPanics(t *testing.T) {
	s := Summary{Date: "2026-11-15", Commit: "c", Reasons: []string{}, Cases: map[string]any{}, VictoriaLogs: []string{"v1.49.0"},
		Sweep: &SweepPart{LokiAnswers: map[string]int{}, Differences: map[string]int{}}, Errors: &ErrorsPart{}, Parity: map[string]any{"requests": 0.0},
		Workflows: &WorkflowPart{}, PerfWeekly: &WeeklyPerf{Date: "2026-11-09"}}
	out := Executive(s, nil)
	for _, want := range []string{"**Green: 0 of 0 checks (areas) at target; 0 of 0 counted items ok (n/a); ", "| Grammar sweep | 0 | 0 | n/a |", "| Error answers | 0 | 0 | n/a |",
		"| Registry cases | n/a | n/a | n/a |", "Nothing is red."} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "NaN") || strings.Contains(out, "Inf") {
		t.Errorf("a division leaked:\n%s", out)
	}
	// An empty summary is not an error either.
	if got := Executive(Summary{}, nil); !strings.Contains(got, "### Executive summary") {
		t.Errorf("an empty summary renders nothing:\n%s", got)
	}
}

func TestEveryRedReasonIsGroupedWithItsCountAndWhatToDo(t *testing.T) {
	cur, _ := fixtures(t)
	out := wrongNow(cur)
	for _, want := range []string{
		"| Scheduled workflow not ok | 2 | parity-refresh.yaml; perf-daily.yaml |",
		"| Parity cluster claimed by no case | 1 | c2; c9 | add a registry case with a gap block that claims each cluster |",
		"| Perf smoke rows above 2x Loki | 2 | s1 / Q1, 1h, cold, p; s2 / Q7, 6h, cold, p | bring each row under 2x Loki",
		"| Fixed case differs from Loki | 1 | a/4 |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// A reason no rule knows is still listed.
	other := wrongNow(Summary{Reasons: []string{"something new broke"}})
	if !strings.Contains(other, "| Other red reason | 1 | - |") {
		t.Errorf("an unknown reason is dropped:\n%s", other)
	}
	// Many items are shortened.
	var many []string
	for i := 0; i < 9; i++ {
		many = append(many, "scheduled workflow w"+string(rune('a'+i))+".yaml: no scheduled run recorded")
	}
	if got := wrongNow(Summary{Reasons: many}); !strings.Contains(got, "; ... 4 more |") || !strings.Contains(got, "| 9 |") {
		t.Errorf("the which column should be capped at five:\n%s", got)
	}
}

func TestSidesSplitTheSweepBetweenLokiAndVictoriaLogs(t *testing.T) {
	cur, _ := fixtures(t)
	out := sides(cur)
	for _, want := range []string{
		"- Open registry cases: 3 of 11 (27.3%).",
		"- Live differs, VictoriaLogs v1.53.0: 1 of 4 proven (25.0%); proven is 4 of 6 cases.",
		"- Error answers differing from Loki: 1 of 20 recorded (5.0%).",
		"- Grammar sweep differences with Loki: 24 of 1000 queries (2.4%).",
		"  - proxy accepts, Loki rejects: 7 (0.7%)",
		"  - tree fact differs (1 kind): 3 (0.3%)",
		"- Grammar sweep, VictoriaLogs rejects the translator text: 18 of 1000 queries (1.8%).",
		"- Grammar sweep, accepted, translation fails: 0 of 1000 queries (0.0%).",
		"- Upstream-blocked cases: 1 of 11 (9.1%).",
		"- Cases whose live verdict differs between v1.49.0 and v1.53.0: 1 of 6 (16.7%).",
		"- Daily perf smoke rows above 2x Loki: 2 of 55 (3.6%).",
		"- Weekly perf pairs above 2x Loki: 3 on the list, of 100 pairs (3.0%).",
		"bring 5 perf rows (2 daily, 3 weekly) under 2x Loki",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// A sweep without a single VictoriaLogs row still shows both rows, with zero.
	clean := Summary{Sweep: &SweepPart{Queries: 10, Differences: map[string]int{"error text differs": 1}}}
	if got := sides(clean); !strings.Contains(got, "VictoriaLogs rejects the translator text: 0 of 10") {
		t.Errorf("a zero VictoriaLogs row is hidden:\n%s", got)
	}
}

func TestReasonKeysIgnoreNumbersAndDates(t *testing.T) {
	a := "perf smoke: 2 row(s) above 2x Loki"
	b := "perf smoke: 17 row(s) above 2x Loki"
	if reasonKey(a) != reasonKey(b) {
		t.Errorf("%q != %q", reasonKey(a), reasonKey(b))
	}
	added, gone, changed := diffReasons([]string{a, "x: gone"}, []string{b, "y: new"})
	if len(added) != 1 || len(gone) != 1 || len(changed) != 1 || !strings.Contains(changed[0], "was: "+a) {
		t.Errorf("added %v gone %v changed %v", added, gone, changed)
	}
}

func TestParseCasesReadsBuildAndJSONAlike(t *testing.T) {
	_, c := day(t, true)
	raw, err := json.Marshal(map[string]any{"runs": map[string]any{"v1.49.0": map[string]any{"flags": c.Flags["v1.49.0"]}},
		"cases": map[string]any{"a/2": map[string]any{"gap_status": "open", "v1.49.0": map[string]any{"verdict": "matches"}}, "a/7": map[string]any{"v1.49.0": map[string]any{}}}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	got := ParseCases(decoded)
	if got.Verdicts["v1.49.0"]["a/2"] != "matches" || got.GapStatus["a/2"] != "open" || got.GapStatus["a/7"] != "no gap" {
		t.Errorf("%+v", got)
	}
	if _, has := got.Verdicts["v1.49.0"]["a/7"]; has {
		t.Errorf("a case without a verdict has one")
	}
	if ParseCases(nil) != nil || ParseCases(make(chan int)) != nil {
		t.Errorf("nothing to read must be nil")
	}
}

func TestLoadPrevious(t *testing.T) {
	dir := t.TempDir()
	none, err := LoadPrevious(filepath.Join(dir, "no.json"), filepath.Join(dir, "no-cases.json"))
	if none != nil || err != nil {
		t.Fatalf("no summary is no previous report: %v %v", none, err)
	}
	sum := write(t, filepath.Join(dir, "summary.json"), `{"date":"2026-11-14","reasons":[],"registry":{"cases":1}}`)
	p, err := LoadPrevious(sum, filepath.Join(dir, "no-cases.json"))
	if err != nil || p == nil || p.Summary.Date != "2026-11-14" || p.Cases != nil {
		t.Fatalf("summary without cases: %+v %v", p, err)
	}
	cases := write(t, filepath.Join(dir, "cases.json"), `{"runs":{"v1":{"flags":{}}},"cases":{"a/1":{"gap_status":"open","v1":{"verdict":"empty"}}}}`)
	if p, err = LoadPrevious(sum, cases); err != nil || p.Cases.Verdicts["v1"]["a/1"] != "empty" {
		t.Fatalf("with cases: %+v %v", p, err)
	}
	broken := write(t, filepath.Join(dir, "broken.json"), `{`)
	if _, err = LoadPrevious(broken, cases); err == nil {
		t.Errorf("a corrupt summary must be an error")
	}
	if _, err = LoadPrevious(sum, broken); err == nil {
		t.Errorf("corrupt cases must be an error")
	}
}

// The report's body opens with the executive summary and keeps every other section as it was.
func TestReportKeepsTheRestOfTheBodyUnchanged(t *testing.T) {
	s, _, err := Build(fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	plain, report := Markdown(s), Report(s, nil)
	start := strings.Index(report, "### Executive summary")
	if start < 0 {
		t.Fatalf("no executive summary:\n%s", report)
	}
	end := start + 3 + strings.Index(report[start+3:], "\n### ") + 1
	if got := report[:start] + report[end:]; got != plain {
		t.Errorf("the body outside the summary changed:\n%s\n---\n%s", got, plain)
	}
	if !strings.HasPrefix(report[:start], "## Daily report") || strings.Count(report[:start], "\n") > 4 {
		t.Errorf("the summary must follow the title and status lines directly:\n%s", report[:start])
	}
	if strings.Contains(plain, "Executive summary") {
		t.Errorf("the report page (Markdown) must not carry the summary")
	}
}

// run reads the previous report from the files it is about to replace.
func TestRunComparesWithThePreviousFilesBeforeReplacingThem(t *testing.T) {
	in := fixture(t)
	dir := t.TempDir()
	out, casesOut, md := filepath.Join(dir, "summary.json"), filepath.Join(dir, "cases.json"), filepath.Join(dir, "body.md")
	cfg := config{in: in, out: out, casesOut: casesOut, md: md, prevSummary: out, prevCases: casesOut}
	var o, e strings.Builder
	if code := run(cfg, &o, &e); code != 0 {
		t.Fatalf("first run: %d %s", code, e.String())
	}
	first, _ := os.ReadFile(md)
	if !strings.Contains(string(first), "No earlier report to compare with.") {
		t.Errorf("the first run has nothing to compare with:\n%s", first)
	}
	// The second day replaces the files; its report names the first.
	cfg.in.Date = "2026-11-15"
	if code := run(cfg, &o, &e); code != 0 {
		t.Fatalf("second run: %d %s", code, e.String())
	}
	second, _ := os.ReadFile(md)
	if !strings.Contains(string(second), "What changed since 2026-11-14") || !strings.Contains(string(second), "No number moved.") {
		t.Errorf("the second run must compare with the first:\n%s", second)
	}
	// A previous file that cannot be read is a warning and the day is reported.
	if err := os.WriteFile(out, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.Reset()
	if code := run(cfg, &o, &e); code != 0 || !strings.Contains(e.String(), "no comparison with the previous report") {
		t.Errorf("a corrupt previous report: code %d, stderr %q", code, e.String())
	}
}

func TestBuildRecordsWhatTomorrowCompares(t *testing.T) {
	in := fixture(t)
	dir := t.TempDir()
	in.Parity = write(t, filepath.Join(dir, "parity.json"), `{"mode":"validate","exit":0,"clusters":4,"requests":10,"unclaimed":1,"unclaimed_ids":["c1"],
		"verdicts":{"same":6,"differ":2,"both_empty":2,"blocked":0}}`)
	s, merged, err := Build(in)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a/open1": "open", "a/open2": "open", "a/fixed": "fixed", "a/proof": "no gap"}
	for id, status := range want {
		if s.Registry.CaseStatus[id] != status {
			t.Errorf("case %s status %q, want %q", id, s.Registry.CaseStatus[id], status)
		}
	}
	if ids := strs(s.Parity["unclaimed_ids"]); len(ids) != 1 || ids[0] != "c1" {
		t.Errorf("unclaimed ids %v", s.Parity["unclaimed_ids"])
	}
	if r := parityRow(s); r.good != 6 || r.bad != 2 || r.total != 10 || r.pct() != "60.0%" {
		t.Errorf("parity row %+v", r)
	}
	if s.detail == nil || s.detail.Verdicts["v1.49.0"]["a/open1"] != "matches" || len(s.detail.Flags["v1.53.0"]["proposed_close"]) != 1 {
		t.Errorf("per-case detail %+v", s.detail)
	}
	if got := ParseCases(merged); got.Verdicts["v1.53.0"]["a/open2"] != "differs" {
		t.Errorf("merged cases %+v", got)
	}
	// A parity result without the verdict split (an older run) has a total and no good or not good.
	plain := parityRow(Summary{Parity: map[string]any{"requests": 12.0, "unclaimed": 1.0, "clusters": 3.0}})
	if g, b := plain.cells(); g != "n/a" || b != "n/a" || plain.pct() != "n/a" || plain.total != 12 {
		t.Errorf("parity row without a split: %+v", plain)
	}
	// A perf distribution without a green count falls back to below + close.
	rows := perfRows(Summary{PerfDaily: map[string]any{"distribution": map[string]any{"cold": map[string]any{"rows": 10, "below": 4, "close": 5, "between": 1, "above": 0}}}})
	if len(rows) != 1 || rows[0].good != 9 || rows[0].bad != 1 || rows[0].met || rows[0].needed != "1 green row more; 0 above 2x to fix" {
		t.Errorf("perf rows without green: %+v", rows)
	}
}

func TestTwoRegressionsAreTwoNewItemsNotOneChangedItem(t *testing.T) {
	was := []string{"a/1 differs from Loki on VictoriaLogs v1.49.0, and the registry holds it fixed"}
	now := append([]string{}, was...)
	now = append(now,
		"a/2 differs from Loki on VictoriaLogs v1.49.0, and the registry holds it fixed", // another case, a numeric id
		"a/1 differs from Loki on VictoriaLogs v1.53.0, and the registry holds it fixed", // the same case on the other version
		"perf smoke: 3 row(s) above 2x Loki")
	added, gone, changed := diffReasons(was, now)
	if len(added) != 3 || len(gone) != 0 || len(changed) != 0 {
		t.Errorf("added %v gone %v changed %v", added, gone, changed)
	}
	if reasonKey("a/4 differs on v1.53.0 (re-record exit 1)") != "a/4 differs on v1.53.0 (re-record exit #" {
		t.Errorf("%q", reasonKey("a/4 differs on v1.53.0 (re-record exit 1)"))
	}
	if reasonKey("last run 2026-11-09 failed") != "last run # failed" || reasonKey("12 recorded row(s)") != "# recorded row(s)" {
		t.Errorf("numbers and dates are not replaced: %q %q", reasonKey("last run 2026-11-09 failed"), reasonKey("12 recorded row(s)"))
	}
}

// Before the first summary that records every case's status, the earlier statuses come from cases.json, which lists only
// the cases the verifier ran: a case missing there is not a new case.
func TestRegistryMovesFromCasesJSONNeverReportNewOrRemovedCases(t *testing.T) {
	before := map[string]string{"a/1": "open", "a/2": "open"}
	after := map[string]string{"a/1": "fixed", "a/2": "open", "a/3": "open"}
	if got := statusMoves(before, after, false); len(got) != 1 || got[0] != "a/1: open to fixed" {
		t.Errorf("a fallback to cases.json reports %v", got)
	}
	got := statusMoves(before, after, true)
	if len(got) != 2 || got[1] != "a/3: new case, open" {
		t.Errorf("a complete earlier list reports %v", got)
	}
	if got := statusMoves(map[string]string{"a/1": "open", "a/9": "open"}, map[string]string{"a/1": "open"}, true); len(got) != 1 || got[0] != "a/9: removed (was open)" {
		t.Errorf("a removed case: %v", got)
	}
	// End to end: an earlier summary without case_status, with a cases.json that lacks a case.
	cur, prev := fixtures(t)
	prev.Summary.Registry.CaseStatus = nil
	prev.Cases.GapStatus["a/3"] = "open" // a case the verifier listed; a/11 and a/10 are not in that file
	out := Executive(cur, prev)
	if strings.Contains(out, "new case") || !strings.Contains(out, "- a/3: open to fixed") {
		t.Errorf("registry moves from the fallback:\n%s", out)
	}
}

func TestTheSiteCopyOmitsThePerCaseStatusList(t *testing.T) {
	cur, _ := fixtures(t)
	text, err := SiteText(&cur)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "case_status") || strings.Contains(text, "a/10") {
		t.Errorf("the website copy carries the per-case statuses:\n%s", text)
	}
	if !strings.Contains(text, `"cases": 11`) {
		t.Errorf("the registry counts are gone:\n%s", text)
	}
	if cur.Registry.CaseStatus["a/10"] == "" {
		t.Errorf("SiteText must not change the summary it renders")
	}
}
