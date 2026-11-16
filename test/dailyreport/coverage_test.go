package main

import (
	"path/filepath"
	"strings"
	"testing"
)

var greenCI = &CIRun{Workflow: "ci.yaml", Conclusion: "success", Date: "2026-11-14", URL: "https://x/ci"}

func TestCoverageOf(t *testing.T) {
	tests := []string{"internal/proxy/x_test.go"}
	cases := []struct {
		name  string
		o     caseOutcome
		tests []string
		ci    *CIRun
		state string
		class string
		why   string
	}{
		{"matches live", caseOutcome{verdict: "matches"}, nil, nil, coverageLive, "", ""},
		{"differs is live too", caseOutcome{verdict: "differs"}, tests, greenCI, coverageLive, "", ""},
		{"empty with a named test and a green CI", caseOutcome{verdict: "empty", reason: "both sides hold no line"}, tests, greenCI, coverageTest, "empty", ""},
		{"empty with no named test", caseOutcome{verdict: "empty", reason: "both sides hold no line"}, nil, greenCI, coverageNone, "empty", "no named test is wired to it"},
		{"blocked keeps its class", caseOutcome{verdict: "blocked", class: "fixture", reason: "needs data the stack does not hold"}, nil, greenCI, coverageNone,
			"blocked: fixture", "needs data the stack does not hold; no named test"},
		{"a red CI does not make a test pass", caseOutcome{verdict: "empty"}, tests, &CIRun{Workflow: "ci.yaml", Conclusion: "failure"}, coverageNone, "empty", "ended failure"},
		{"no CI run recorded", caseOutcome{verdict: "not_executable"}, tests, nil, coverageNone, "not_executable", "no run recorded"},
		{"a long reason is clipped", caseOutcome{verdict: "blocked", reason: strings.Repeat("x", 400)}, nil, greenCI, coverageNone, "blocked", "..."},
	}
	for _, c := range cases {
		state, why := coverageOf(c.o, c.tests, c.ci)
		if state != c.state || why.class != c.class || !strings.Contains(why.reason, c.why) {
			t.Errorf("%s: %s %+v", c.name, state, why)
		}
		if len(why.reason) > reasonClipped+len("; no named test is wired to it")+3 {
			t.Errorf("%s: reason not clipped (%d)", c.name, len(why.reason))
		}
	}
}

// The report counts three states per version, names the tests and the CI run behind "by a test", gives each case that
// is not proven its reason in cases.json, and a case that is not proven never turns the day red.
func TestBuildCoverage(t *testing.T) {
	in := fixture(t)
	run := `{"date":"2026-11-14T06:00:00Z","commit":"c0ffee","mode":"live","loki_version":"3.7.9","victorialogs_version":"v1.53.0",
		"proxy_flags":"-x","window_end":"2026-11-14T05:58:00Z","counts":{"matches":1,"empty":1,"blocked":1},
		"flags":{"regression":[],"proposed_close":[],"behaviour_changed":[]},"proxy_build":{"revision":"c0ffee0","verified":true},
		"cases":{"a/open1":{"verdict":"matches","expected":"differ","gap_status":"open","flag":"proposed_close"},
		"a/fixed":{"verdict":"empty","expected":"match","gap_status":"fixed","reason":"both sides hold no line"},
		"a/open2":{"verdict":"blocked","class":"fixture","expected":"differ","gap_status":"open","reason":"needs data the stack does not hold"}}}`
	write(t, filepath.Join(in.CasesDir, "cases-v1.53.0.json"), run)
	write(t, filepath.Join(in.CasesDir, "cases-v1.49.0.json"), strings.ReplaceAll(run, "v1.53.0", "v1.49.0"))
	in.Wiring = write(t, filepath.Join(t.TempDir(), "wiring.json"), `{"item_to_tests":{"a/fixed":["internal/proxy/fixed_test.go"]}}`)
	in.Runs = write(t, filepath.Join(t.TempDir(), "runs.json"), `{"fetched":"2026-11-14T07:00:00Z","workflows":{
		"query-corpus.yaml":{"main":{"conclusion":"success","date":"2026-11-14","url":"https://x/1"}},
		"ci.yaml":{"main":{"conclusion":"success","date":"2026-11-14","url":"https://x/ci"}}}}`)
	s, merged, err := Build(in)
	if err != nil {
		t.Fatal(err)
	}
	if s.Red {
		t.Fatalf("a case that is not proven is not red: %v", s.Reasons)
	}
	c := s.Coverage["v1.53.0"]
	if c == nil || c.Cases != 3 || c.ProvenLive != 1 || c.ProvenByTest != 1 || c.NotProven != 1 || c.NotProvenBy["blocked: fixture"] != 1 ||
		c.CI == nil || c.CI.URL != "https://x/ci" {
		t.Fatalf("coverage %+v", c)
	}
	byCase := merged["cases"].(map[string]map[string]any)
	proof := byCase["a/fixed"]["v1.53.0"].(map[string]any)["coverage"].(map[string]any)
	if proof["state"] != coverageTest || proof["ci_run"] != "https://x/ci" || proof["tests"].([]string)[0] != "internal/proxy/fixed_test.go" {
		t.Errorf("a case proven by a test names it: %+v", proof)
	}
	none := byCase["a/open2"]["v1.53.0"].(map[string]any)["coverage"].(map[string]any)
	if none["state"] != coverageNone || !strings.Contains(none["reason"].(string), "needs data the stack does not hold") {
		t.Errorf("a case not proven says why: %+v", none)
	}
	if _, has := byCase["a/open1"]["v1.53.0"].(map[string]any)["coverage"]; has {
		t.Error("a case proven live carries no coverage note")
	}
	md := Report(s, nil)
	for _, want := range []string{"| Coverage, VictoriaLogs v1.53.0 | 2 | 1 | 66.7% |",
		"Coverage, VictoriaLogs v1.53.0: 2 of 3 cases proven (live 1, by a named passing test 1); not proven 1 (blocked: fixture 1); tests passing: ci.yaml run of 2026-11-14 on main, https://x/ci."} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q:\n%s", want, md)
		}
	}

	// Without a green ci.yaml run the wired tests are not counted, and the report says their state is unknown.
	in.Runs = write(t, filepath.Join(t.TempDir(), "runs.json"), `{"fetched":"2026-11-14T07:00:00Z","workflows":{
		"query-corpus.yaml":{"main":{"conclusion":"success","date":"2026-11-14","url":"https://x/1"}}}}`)
	s, _, err = Build(in)
	if err != nil {
		t.Fatal(err)
	}
	if c := s.Coverage["v1.53.0"]; c.ProvenByTest != 0 || c.NotProven != 2 || !strings.Contains(coverageDetail("v1.53.0", c), "their state is unknown") {
		t.Errorf("no CI run: %+v", c)
	}
	// No wiring file named: coverage by a test is left out, nothing is missing.
	in.Wiring = ""
	if s, _, err = Build(in); err != nil || len(s.Missing) != 0 || s.Coverage["v1.53.0"].ProvenByTest != 0 {
		t.Errorf("no wiring: %v %v", err, s.Missing)
	}
	// A wiring file that is named and absent is a missing input, which turns the day red.
	in.Wiring = filepath.Join(t.TempDir(), "absent.json")
	if s, _, err = Build(in); err != nil || len(s.Missing) != 1 || !s.Red {
		t.Errorf("absent wiring: %v %v", err, s.Missing)
	}
}

// The perf smoke's two repeats are both reported: the repeat that moves the window and the exact repeat.
func TestExactRepeatText(t *testing.T) {
	if got := exactRepeatText(map[string]any{"rows": 3}); got != "" {
		t.Errorf("no exact repeat measured: %q", got)
	}
	got := exactRepeatText(map[string]any{"exact_rows": float64(30), "exact_without_vl_calls": float64(27), "exact_repeat_vl_calls": float64(4)})
	if want := " Exact repeats: 30 rows, 27 without a VictoriaLogs call, 4 calls in all."; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	in := fixture(t)
	in.Smoke = write(t, filepath.Join(t.TempDir(), "smoke.json"), `{"date":"2026-11-14","commit":"c0ffee","rows":120,"comparable":400,"loki_failed":2,
		"distribution":{"cold":{"rows":200}},"over_2x":[],"ratchet_lost":[],"ratchet_problem":"",
		"reuse":{"rows":110,"cold_vl_calls":300,"repeat_vl_calls":12,"repeat_without_vl_calls":100,"exact_rows":110,"exact_without_vl_calls":109,"exact_repeat_vl_calls":1},"reuse_lost":[]}`)
	s, _, err := Build(in)
	if err != nil {
		t.Fatal(err)
	}
	if md := Markdown(s); !strings.Contains(md, "0 rows lost reuse. Exact repeats: 110 rows, 109 without a VictoriaLogs call, 1 calls in all.") {
		t.Errorf("the markdown does not report the exact repeat:\n%s", md)
	}
}
