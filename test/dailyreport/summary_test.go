package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t testing.TB, path, text string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// fixture writes one green day's inputs and returns them.
func fixture(t testing.TB) Inputs {
	t.Helper()
	dir := t.TempDir()
	schema, err := os.ReadFile("../../conformance/registry/schema/case.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "reg/schema/case.schema.json"), string(schema))
	head := "title: t\nproves: [x]\nconsumer: api\nwhy: w\nrequest: {method: GET, path: /x}\ncompare: {mode: exact}\n"
	gap := func(status, impact string) string {
		return "gap: {status: " + status + ", impact: " + impact + ", effort: S, area: a, loki: l, proxy: p}\n"
	}
	write(t, filepath.Join(dir, "reg/cases/a/open1.yaml"), "id: a/open1\n"+head+gap("open", "explore-visible"))
	write(t, filepath.Join(dir, "reg/cases/a/open2.yaml"), "id: a/open2\n"+head+gap("open", "edge"))
	write(t, filepath.Join(dir, "reg/cases/a/fixed.yaml"), "id: a/fixed\n"+head+gap("fixed", "edge"))
	write(t, filepath.Join(dir, "reg/cases/a/proof.yaml"), "id: a/proof\n"+head)
	caseRun := func(version string) string {
		return `{"date":"2026-11-14T06:00:00Z","commit":"c0ffee","mode":"live","loki_version":"3.7.9","victorialogs_version":"` + version + `",
			"proxy_flags":"-x","window_end":"2026-11-14T05:58:00Z","counts":{"matches":3,"differs":1},
			"flags":{"regression":[],"proposed_close":["a/open1"],"behaviour_changed":[]},
			"proxy_build":{"revision":"c0ffee0","verified":true},
			"reuse":{"counts":{"reused":2,"cache_not_reused":1},"not_reused":["a/open2"],"cold_vl_calls":4,"repeat_vl_calls":1,"repeat_hit_ratio":{"loki:l1_memory":0.5}},
			"cases":{"a/open1":{"verdict":"matches","expected":"differ","gap_status":"open","flag":"proposed_close"},
			"a/open2":{"verdict":"differs","expected":"differ","gap_status":"open","differences":[{"facet":"values","loki":"` +
			strings.Repeat("x", 400) + `","proxy":"y"}]}}}`
	}
	write(t, filepath.Join(dir, "cases/cases-v1.49.0.json"), caseRun("v1.49.0"))
	write(t, filepath.Join(dir, "cases/cases-v1.53.0.json"), caseRun("v1.53.0"))
	return Inputs{
		Registry: filepath.Join(dir, "reg"), CasesDir: filepath.Join(dir, "cases"),
		Health: write(t, filepath.Join(dir, "health.json"), `{"checked_at":"2026-11-14T07:00:00Z","scheduled":8,"problems":[]}`),
		Runs: write(t, filepath.Join(dir, "runs.json"), `{"fetched":"2026-11-14T07:00:00Z","workflows":{"query-corpus.yaml":
			{"main":{"conclusion":"success","date":"2026-11-14","url":"https://x/1"}}}}`),
		SweepWorkflow: "query-corpus.yaml",
		ErrorCoverage: write(t, filepath.Join(dir, "errcov.json"), `{"summary":{"comparable_rows":231,"matching_rows":221},"known_differences":{"rows":10}}`),
		ErrorLeaks:    write(t, filepath.Join(dir, "leaks.json"), `{"leaks":{"cell":["a/x","a/y"]}}`),
		ErrorRerecord: write(t, filepath.Join(dir, "rerecord.txt"),
			"internal/lokierr/testdata/loki_error_answers.jsonl: 240 rows, 0 answers changed, 9 not re-recorded (not comparable)\n"+
				"internal/lokierr/testdata/loki_check_order.jsonl: 33 rows, 0 answers changed, 0 not re-recorded (not comparable)\n"),
		Sweep: write(t, filepath.Join(dir, "sweep.json"), `{"queries":25824,"loki_answers":{"accepted":16982,"rejected":8842},
			"differences_by_kind":{"proxy accepts, Loki rejects":547,"error text differs":212}}`),
		Parity: write(t, filepath.Join(dir, "parity.json"), `{"mode":"validate","exit":0,"clusters":40,"requests":900,"unclaimed":0,"problems":0,"conformance_ok":true}`),
		Smoke: write(t, filepath.Join(dir, "smoke.json"), `{"date":"2026-11-14","commit":"c0ffee","rows":120,"comparable":400,"loki_failed":2,
			"distribution":{"cold":{"rows":200}},"over_2x":[],"ratchet_lost":[],"ratchet_problem":"",
			"reuse":{"rows":110,"cold_vl_calls":300,"repeat_vl_calls":12,"repeat_without_vl_calls":100},"reuse_lost":[]}`),
		SiteData: write(t, filepath.Join(dir, "site.json"), `{"date":"2026-11-09","loki":"3.7.9","by_range":[
			{"pairs":40,"faster":25,"par":13,"slower":2},{"pairs":36,"faster":25,"par":11,"slower":0}],"distribution":[{"above":5}]}`),
		Over2x: write(t, filepath.Join(dir, "over.json"), `{"rows":[{},{},{}]}`),
		Date:   "2026-11-14", Commit: "c0ffee0123456789", Run: "https://x/run",
	}
}

func TestAGreenDay(t *testing.T) {
	s, merged, err := Build(fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if s.Red || len(s.Reasons) != 0 || len(s.Missing) != 0 {
		t.Fatalf("red: %v missing %v", s.Reasons, s.Missing)
	}
	if s.Registry.Cases != 4 || s.Registry.ByStatus["open"] != 2 || s.Registry.ByStatus["fixed"] != 1 || s.Registry.ByStatus["no gap"] != 1 ||
		s.Registry.OpenByImpact["explore-visible"] != 1 || s.Registry.OpenByImpact["edge"] != 1 {
		t.Errorf("registry %+v", s.Registry)
	}
	if strings.Join(s.VictoriaLogs, ",") != "v1.49.0,v1.53.0" || s.Loki != "3.7.9" {
		t.Errorf("versions %v %s", s.VictoriaLogs, s.Loki)
	}
	if e := s.Errors; e.Recorded.MatchingRows != 221 || e.Recorded.KnownDifferences != 10 || e.Recorded.KnownLeaks != 2 ||
		e.Live == nil || e.Live.Rows != 273 || e.Live.Kept != 9 || !e.Live.OK || e.ReplayOK == nil || !*e.ReplayOK {
		t.Errorf("errors %+v %+v", e, e.Live)
	}
	if s.Sweep.Differences["proxy accepts, Loki rejects"] != 547 || s.Sweep.LiveCheck["conclusion"] != "success" {
		t.Errorf("sweep %+v", s.Sweep)
	}
	if w := s.PerfWeekly; w.Pairs != 76 || w.Faster != 50 || w.Par != 24 || w.Slower != 2 || w.Over2xRows != 3 || w.Date != "2026-11-09" {
		t.Errorf("weekly %+v", w)
	}
	cases := merged["cases"].(map[string]map[string]any)
	open2 := cases["a/open2"]
	if open2["expected"] != "differ" || open2["gap_status"] != "open" {
		t.Errorf("merged entry %+v", open2)
	}
	v := open2["v1.53.0"].(map[string]any)
	diff := v["differences"].([]any)[0].(map[string]any)
	if len(diff["loki"].(string)) != clipped+3 || v["expected"] != nil {
		t.Errorf("merged verdict %+v", v)
	}
	md := Markdown(s)
	for _, want := range []string{"## Daily report 2026-11-14: green", "Main `c0ffee012345`, Loki 3.7.9, VictoriaLogs v1.49.0 and v1.53.0.",
		"| open | 2 |", "Open by impact: edge 1, explore-visible 1.", "| v1.53.0 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 |",
		"221 of 231 recorded Loki answers match, 10 known differences, 2 known leaks. Live re-record: 273 rows asked, 0 changed. Unit replay: passes.",
		"| proxy accepts, Loki rejects | 547 |", "`query-corpus.yaml` success on 2026-11-14", "40 clusters over 900 requests, 0 claimed by no case",
		"120 rows, 400 comparable timings, 0 above 2x Loki, 0 ratchet rows lost", "76 comparable shape x range pairs: 50 faster, 24 on par, 2 slower; 3 rows",
		"8 scheduled workflows checked",
		"- Registry cases, VictoriaLogs v1.53.0: 2 repeats reused, 1 not reused, 0 not measured; VictoriaLogs calls 4 cold, 1 on the repeats.",
		"- Perf smoke: 110 rows measured, VictoriaLogs calls 300 cold and 12 on the repeats, 100 rows repeat without calling VictoriaLogs, 0 rows lost reuse."} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q:\n%s", want, md)
		}
	}
}

func TestRedReasons(t *testing.T) {
	cases := []struct {
		name   string
		change func(t *testing.T, in *Inputs)
		want   string
	}{
		{"regression", func(t *testing.T, in *Inputs) {
			write(t, filepath.Join(in.CasesDir, "cases-v1.53.0.json"), `{"victorialogs_version":"v1.53.0","flags":{"regression":["a/fixed"]},"cases":{}}`)
		}, "a/fixed differs from Loki on VictoriaLogs v1.53.0, and the registry holds it fixed"},
		{"workflow red", func(t *testing.T, in *Inputs) {
			write(t, in.Health, `{"problems":[{"workflow":"security-heavy.yaml","problem":"last scheduled run 2026-11-14 ended failure"}]}`)
		}, "scheduled workflow security-heavy.yaml: last scheduled run 2026-11-14 ended failure"},
		{"error drift", func(t *testing.T, in *Inputs) {
			write(t, in.ErrorRerecord, "x: 10 rows, 2 answers changed, 0 not re-recorded\n")
			in.ErrorRerecordExit = 1
		}, "answers 2 recorded row(s) differently (re-record exit 1)"},
		{"error output unreadable", func(t *testing.T, in *Inputs) { write(t, in.ErrorRerecord, "errorrecord: connection refused\n") },
			"answers 0 recorded row(s) differently (re-record exit 0)"},
		{"replay", func(_ *testing.T, in *Inputs) { in.ErrorReplayExit = 1 }, "the unit replay against the recorded Loki answers failed"},
		{"sweep red", func(t *testing.T, in *Inputs) {
			write(t, in.Runs, `{"workflows":{"query-corpus.yaml":{"main":{"conclusion":"failure","date":"2026-11-14"}}}}`)
		}, "query-corpus.yaml: the live re-verification of the sweep and the differentials ended failure on 2026-11-14"},
		{"sweep not run", func(t *testing.T, in *Inputs) { write(t, in.Runs, `{"workflows":{}}`) }, "query-corpus.yaml: no run on main recorded"},
		{"parity unclaimed", func(t *testing.T, in *Inputs) { write(t, in.Parity, `{"exit":0,"unclaimed":12}`) }, "12 difference cluster(s) are claimed by no registry case"},
		{"parity failed", func(t *testing.T, in *Inputs) { write(t, in.Parity, `{"exit":2}`) }, "could not speak for its slice (exit 2)"},
		{"over 2x", func(t *testing.T, in *Inputs) { write(t, in.Smoke, `{"over_2x":[{}],"ratchet_lost":[]}`) }, "perf smoke: 1 row(s) above 2x Loki"},
		{"ratchet", func(t *testing.T, in *Inputs) { write(t, in.Smoke, `{"over_2x":[],"ratchet_lost":[{},{}]}`) }, "perf smoke: 2 row(s) of the weekly ratchet lost"},
		{"reuse lost", func(t *testing.T, in *Inputs) {
			write(t, in.Smoke, `{"over_2x":[],"ratchet_lost":[],"reuse_lost":[{}]}`)
		},
			"perf smoke: 1 row(s) reuse the proxy's cache less than the day before"},
		{"stale proxy", func(t *testing.T, in *Inputs) {
			write(t, filepath.Join(in.CasesDir, "cases-v1.53.0.json"), `{"victorialogs_version":"v1.53.0","proxy_build":{"revision":"old1234","stale":true},"cases":{}}`)
		}, "the VictoriaLogs v1.53.0 run measured a proxy built from old1234, not the commit"},
		{"no cases", func(_ *testing.T, in *Inputs) { in.CasesDir = "" }, "per-case verdicts: the run produced no result"},
		{"no smoke", func(_ *testing.T, in *Inputs) { in.Smoke = filepath.Join(in.CasesDir, "none.json") }, "perf smoke: the run produced no result"},
		{"no parity", func(_ *testing.T, in *Inputs) { in.Parity = "" }, "parity slice: the run produced no result"},
		{"no health", func(_ *testing.T, in *Inputs) { in.Health = "" }, "workflow health: the run produced no result"},
		{"no rerecord", func(_ *testing.T, in *Inputs) { in.ErrorRerecord = "" }, "error answers re-record: the run produced no result"},
		{"no coverage", func(_ *testing.T, in *Inputs) { in.ErrorCoverage = "" }, "error coverage: the run produced no result"},
		{"no leaks", func(_ *testing.T, in *Inputs) { in.ErrorLeaks = "" }, "error-layer known leaks: the run produced no result"},
		{"no sweep", func(_ *testing.T, in *Inputs) { in.Sweep = "" }, "grammar sweep: the run produced no result"},
		{"no runs", func(_ *testing.T, in *Inputs) { in.Runs = "" }, "workflow runs: the run produced no result"},
		{"no weekly", func(_ *testing.T, in *Inputs) { in.SiteData = "" }, "weekly perf figures: the run produced no result"},
		{"no over 2x", func(_ *testing.T, in *Inputs) { in.Over2x = "" }, "above-2x list: the run produced no result"},
	}
	for _, c := range cases {
		in := fixture(t)
		c.change(t, &in)
		s, _, err := Build(in)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !s.Red || !strings.Contains(strings.Join(s.Reasons, "\n"), c.want) {
			t.Errorf("%s: reasons %q, want %q", c.name, s.Reasons, c.want)
		}
		if md := Markdown(s); !strings.Contains(md, ": red") || !strings.Contains(md, "### Red") {
			t.Errorf("%s: markdown not red", c.name)
		}
	}
}

func TestSkippedErrorChecks(t *testing.T) {
	in := fixture(t)
	in.SkipRerecord, in.SkipReplay, in.ErrorReplayExit = true, true, 1
	s, _, err := Build(in)
	if err != nil || s.Red || s.Errors.Live != nil || s.Errors.ReplayOK != nil {
		t.Fatalf("skipped checks: %v %+v %v", err, s.Errors, s.Reasons)
	}
	if md := Markdown(s); !strings.Contains(md, "Live re-record: not run. Unit replay: not run.") {
		t.Errorf("markdown:\n%s", md)
	}
	failed := false
	if okText(failed) != "fails" {
		t.Error("okText")
	}
}

func TestBrokenInputsAreErrors(t *testing.T) {
	cases := map[string]func(t *testing.T, in *Inputs){
		"registry missing": func(_ *testing.T, in *Inputs) { in.Registry = filepath.Join(in.CasesDir, "none") },
		"registry problems": func(t *testing.T, in *Inputs) {
			write(t, filepath.Join(in.Registry, "cases/a/bad.yaml"), "id: a/other\n")
		},
		"cases not json":  func(t *testing.T, in *Inputs) { write(t, filepath.Join(in.CasesDir, "x.json"), "{") },
		"case not object": func(t *testing.T, in *Inputs) { write(t, filepath.Join(in.CasesDir, "x.json"), `{"cases":{"a":[1]}}`) },
		"cases bad glob":  func(_ *testing.T, in *Inputs) { in.CasesDir = "[" },
		"two runs of one version": func(t *testing.T, in *Inputs) {
			write(t, filepath.Join(in.CasesDir, "z.json"), `{"victorialogs_version":"v1.53.0","cases":{}}`)
		},
		"health not json":   func(t *testing.T, in *Inputs) { write(t, in.Health, "{") },
		"leaks not json":    func(t *testing.T, in *Inputs) { write(t, in.ErrorLeaks, "{") },
		"coverage not json": func(t *testing.T, in *Inputs) { write(t, in.ErrorCoverage, "[") },
		"rerecord is a dir": func(_ *testing.T, in *Inputs) { in.ErrorRerecord = in.CasesDir },
		"sweep not json":    func(t *testing.T, in *Inputs) { write(t, in.Sweep, "{") },
		"runs not json":     func(t *testing.T, in *Inputs) { write(t, in.Runs, "{") },
		"parity not json":   func(t *testing.T, in *Inputs) { write(t, in.Parity, "{") },
		"smoke not json":    func(t *testing.T, in *Inputs) { write(t, in.Smoke, "{") },
		"site not json":     func(t *testing.T, in *Inputs) { write(t, in.SiteData, "{") },
		"over not json":     func(t *testing.T, in *Inputs) { write(t, in.Over2x, "{") },
	}
	for name, change := range cases {
		in := fixture(t)
		change(t, &in)
		if _, _, err := Build(in); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestRunWritesTheFiles(t *testing.T) {
	in := fixture(t)
	dir := t.TempDir()
	cfg := config{in: in, out: filepath.Join(dir, "v/summary.json"), casesOut: filepath.Join(dir, "v/cases.json"), md: filepath.Join(dir, "body.md")}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 || !strings.Contains(out.String(), "green, 0 reason(s)") {
		t.Fatalf("run: %d %s %s", code, out.String(), errs.String())
	}
	raw, err := os.ReadFile(cfg.out)
	if err != nil {
		t.Fatal(err)
	}
	var s map[string]any
	if err := json.Unmarshal(raw, &s); err != nil || s["date"] != "2026-11-14" || s["commit"] != "c0ffee0123456789" || s["red"] != false {
		t.Errorf("summary.json: %v %v", err, s)
	}
	if strings.Contains(string(raw), `\u003c`) {
		t.Error("HTML characters are escaped")
	}
	if _, err := os.Stat(cfg.casesOut); err != nil {
		t.Error(err)
	}
	// -check fails a red day and lists why.
	cfg.check = true
	cfg.in.ErrorReplayExit = 1
	out.Reset()
	if code := run(cfg, &out, &errs); code != 1 || !strings.Contains(out.String(), "  error answers: the unit replay") {
		t.Errorf("check: %d %s", code, out.String())
	}
	cfg.md = filepath.Join(cfg.out, "body.md")
	if code := run(cfg, &out, &errs); code != 2 {
		t.Errorf("unwritable body: %d", code)
	}
	cfg.out = filepath.Join(cfg.casesOut, "s.json")
	if code := run(cfg, &out, &errs); code != 2 {
		t.Errorf("unwritable summary: %d", code)
	}
	if short("abc") != "abc" || lenOf(nil) != 0 {
		t.Error("helpers")
	}
}

func TestManyReasonsAreCapped(t *testing.T) {
	s := Summary{Date: "d", Red: true}
	for i := 0; i < 70; i++ {
		s.Reasons = append(s.Reasons, "r")
	}
	if md := Markdown(s); !strings.Contains(md, "- ... 10 more in summary.json") {
		t.Errorf("markdown:\n%s", md)
	}
}

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-cases", "d", "-error-rerecord-exit", "1", "-no-error-replay", "-check", "-date", "2026-11-14"})
	if err != nil || cfg.in.CasesDir != "d" || cfg.in.ErrorRerecordExit != 1 || !cfg.in.SkipReplay || !cfg.check || cfg.in.Date != "2026-11-14" ||
		cfg.in.SweepWorkflow != "query-corpus.yaml" || cfg.out != "conformance/registry/generated/verification/summary.json" {
		t.Errorf("flags: %v %+v", err, cfg)
	}
	if _, err := parseFlags([]string{"-nope"}); err == nil {
		t.Error("an unknown flag is an error")
	}
}

// BenchmarkBuild is one day's summary over a small registry and every input (the report job's cost).
func BenchmarkBuild(b *testing.B) {
	in := fixture(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := Build(in); err != nil {
			b.Fatal(err)
		}
	}
}
