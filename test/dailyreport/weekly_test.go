package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func weeklyFixture(t *testing.T) WeeklyInputs {
	t.Helper()
	in := fixture(t)
	dir := t.TempDir()
	s, _, err := Build(in)
	if err != nil {
		t.Fatal(err)
	}
	summary := filepath.Join(dir, "summary.json")
	if err := writeJSON(summary, s); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "parity/result-v1.49.0.json"), `{"clusters":41,"requests":9000,"unclaimed":2,"problems":2,"exit":0}`)
	write(t, filepath.Join(dir, "parity/result-v1.53.0.json"), `{"clusters":40,"requests":9000,"unclaimed":0,"problems":0,"exit":0}`)
	write(t, filepath.Join(dir, "sweep.jsonl"), strings.Join([]string{
		`{"class":"proxy accepts, Loki rejects | a | x","q":"1"}`,
		`{"class":"proxy accepts, Loki rejects | a | x","q":"2"}`,
		`{"class":"proxy accepts, Loki rejects | b | y","q":"3"}`,
		`{"class":"error text differs | c | z","q":"4"}`,
	}, "\n")+"\n")
	write(t, filepath.Join(dir, "floor.json"), `{"rows":100,"over_2x":[{}],"distribution":{"cold":{"below":60,"close":30,"between":9,"above":1}}}`)
	return WeeklyInputs{Summary: summary, CasesDir: in.CasesDir, ParityDir: filepath.Join(dir, "parity"), SweepDump: filepath.Join(dir, "sweep.jsonl"),
		FloorPerf: filepath.Join(dir, "floor.json"), SiteData: in.SiteData, Over2x: in.Over2x, ReleaseDry: "success", Date: "2026-11-16", Commit: "c1"}
}

func TestBuildWeekly(t *testing.T) {
	l, err := BuildWeekly(weeklyFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"registry.open": 2, "registry.open.edge": 1, "errors.matching": 221, "errors.known_leaks": 2,
		"sweep.default.proxy accepts, Loki rejects": 547, "sweep.full.proxy accepts, Loki rejects": 3,
		"sweep.full.error text differs": 1, "sweep.full.classes": 3,
		"cases.v1.49.0.matches": 3, "cases.v1.53.0.proposed_close": 1,
		"parity.v1.49.0.unclaimed": 2, "parity.v1.53.0.clusters": 40,
		"perf.floor.rows": 100, "perf.floor.above_2x": 1, "perf.floor.cold.between": 9,
		"perf.pin.pairs": 76, "perf.pin.over_2x_rows": 3,
	}
	for k, v := range want {
		if got, ok := l.Numbers[k]; !ok || got != v {
			t.Errorf("%s = %v (%v), want %v", k, got, ok, v)
		}
	}
	if l.Notes["release dry run"] != "success" || l.Notes["daily summary"] != "2026-11-14" || l.Notes["perf measured"] != "2026-11-09" {
		t.Errorf("notes %v", l.Notes)
	}
}

func TestBuildWeeklyNotesWhatWasNotMeasured(t *testing.T) {
	l, err := BuildWeekly(WeeklyInputs{Date: "2026-11-16"})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"daily summary", "per-case verdicts", "full parity corpus", "full sweep", "perf on the oldest VictoriaLogs", "perf on the pin"} {
		if l.Notes[k] != "not measured this week" {
			t.Errorf("%s: %q", k, l.Notes[k])
		}
	}
	if l.Notes["release dry run"] != "not run this week" || len(l.Numbers) != 0 {
		t.Errorf("empty week: %+v", l)
	}
	in := weeklyFixture(t)
	for name, change := range map[string]func(){
		"summary not json":  func() { write(t, in.Summary, "{") },
		"cases not json":    func() { write(t, filepath.Join(in.CasesDir, "z.json"), "{") },
		"parity not json":   func() { write(t, filepath.Join(in.ParityDir, "result-x.json"), "{") },
		"sweep not json":    func() { write(t, in.SweepDump, "{\n") },
		"sweep a directory": func() { in.SweepDump = in.ParityDir },
		"floor not json":    func() { write(t, in.FloorPerf, "{") },
		"site not json":     func() { write(t, in.SiteData, "{") },
	} {
		in = weeklyFixture(t)
		change()
		if _, err := BuildWeekly(in); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestWeeklyHistoryAndTrend(t *testing.T) {
	week := func(date string, open float64) WeeklyLine {
		return WeeklyLine{Date: date, Numbers: map[string]float64{"registry.open": open, "perf.ratio": 1.25}}
	}
	var lines []WeeklyLine
	for i, d := range []string{"2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19", "2026-10-26", "2026-11-02", "2026-11-09", "2026-11-16"} {
		lines = AppendWeekly(lines, week(d, float64(140-i)))
	}
	// A second run of a day replaces its line.
	lines = AppendWeekly(lines, WeeklyLine{Date: "2026-11-16", Numbers: map[string]float64{"registry.open": 130, "new.number": 4},
		Notes: map[string]string{"release dry run": "failure"}})
	if len(lines) != 9 || lines[8].Numbers["registry.open"] != 130 {
		t.Fatalf("history %+v", lines)
	}
	path := filepath.Join(t.TempDir(), "h", "weekly-history.jsonl")
	if err := WriteWeekly(path, lines); err != nil {
		t.Fatal(err)
	}
	again, err := ReadWeekly(path)
	if err != nil || len(again) != 9 || again[0].Date != "2026-09-21" {
		t.Fatalf("read back: %v %+v", err, again)
	}
	trend := RenderTrend(again)
	for _, want := range []string{"| Number | 2026-09-28 |", "| 2026-11-16 | Change |", "| registry.open | 139 | 138 | 137 | 136 | 135 | 134 | 133 | 130 | -3 |",
		"| new.number | - | - | - | - | - | - | - | 4 |  |", "| perf.ratio | 1.25 |", "- release dry run: failure"} {
		if !strings.Contains(trend, want) {
			t.Errorf("trend lacks %q:\n%s", want, trend)
		}
	}
	if strings.Contains(trend, "2026-09-21") {
		t.Error("the trend shows the last eight weeks only")
	}
	if got := RenderTrend(nil); !strings.Contains(got, "No weekly run recorded yet.") {
		t.Errorf("empty trend: %s", got)
	}
	if change(lines[:1], "registry.open") != "" || change([]WeeklyLine{week("a", 1), week("b", 3)}, "registry.open") != "+2" {
		t.Error("change")
	}
	if none, err := ReadWeekly(filepath.Join(t.TempDir(), "none")); err != nil || none != nil {
		t.Errorf("missing history: %v %v", none, err)
	}
	write(t, path, "{\n")
	if _, err := ReadWeekly(path); err == nil {
		t.Error("a broken history is an error")
	}
	if _, err := ReadWeekly(filepath.Dir(path)); err == nil {
		t.Error("an unreadable history is an error")
	}
	if err := WriteWeekly(filepath.Join(path, "x"), lines); err == nil {
		t.Error("an unwritable history is an error")
	}
}

func TestRunWeekly(t *testing.T) {
	in := weeklyFixture(t)
	dir := t.TempDir()
	cfg, err := parseFlags([]string{"-weekly", "-date", in.Date, "-commit", "c1", "-daily-summary", in.Summary, "-weekly-cases", in.CasesDir,
		"-weekly-parity", in.ParityDir, "-weekly-sweep", in.SweepDump, "-weekly-floor-perf", in.FloorPerf, "-weekly-perf", in.SiteData,
		"-over-2x", in.Over2x, "-release-dry-run", "success", "-weekly-history", filepath.Join(dir, "v/weekly-history.jsonl"),
		"-weekly-trend", filepath.Join(dir, "v/weekly-trend.md")})
	if err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 || !strings.Contains(out.String(), "weekly report 2026-11-16") {
		t.Fatalf("weekly: %d %s %s", code, out.String(), errs.String())
	}
	trend, _ := os.ReadFile(cfg.trend)
	if !strings.Contains(string(trend), "| parity.v1.49.0.unclaimed | 2 |") {
		t.Errorf("trend:\n%s", trend)
	}
	cfg.trend = filepath.Join(cfg.history, "t.md")
	if code := run(cfg, &out, &errs); code != 2 {
		t.Errorf("unwritable trend: %d", code)
	}
	write(t, in.Summary, "{")
	if code := run(cfg, &out, &errs); code != 2 {
		t.Errorf("broken summary: %d", code)
	}
}

func TestWeeklyReuseAndFleet(t *testing.T) {
	in := weeklyFixture(t)
	dir := t.TempDir()
	in.PinPerf = write(t, filepath.Join(dir, "pin.json"), `{"reuse":{"rows":40,"cold_vl_calls":90,"repeat_vl_calls":3,"repeat_without_vl_calls":37}}`)
	write(t, in.FloorPerf, `{"rows":100,"over_2x":[],"distribution":{},"reuse":{"rows":40,"cold_vl_calls":95,"repeat_vl_calls":4,"repeat_without_vl_calls":36}}`)
	in.FleetDir = filepath.Join(dir, "fleet")
	write(t, filepath.Join(in.FleetDir, "rf1.json"), `{"summary":[{"vl_calls":2,"peer_hits":5,"peer_errors":0},{"vl_calls":1,"peer_hits":3,"peer_errors":1}]}`)
	write(t, filepath.Join(in.FleetDir, "rf1.exit"), "0\n")
	write(t, filepath.Join(in.FleetDir, "rf2.json"), `{"summary":[]}`)
	write(t, filepath.Join(in.FleetDir, "rf2.exit"), "1\n")
	l, err := BuildWeekly(in)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]float64{"perf.pin.reuse.repeat_vl_calls": 3, "perf.floor.reuse.repeat_without_vl_calls": 36,
		"fleet.rf1.vl_calls": 3, "fleet.rf1.peer_hits": 8, "fleet.rf1.peer_errors": 1, "fleet.rf1.passed": 1, "fleet.rf2.passed": 0, "fleet.rf2.layers": 0} {
		if l.Numbers[k] != v {
			t.Errorf("%s = %v, want %v", k, l.Numbers[k], v)
		}
	}
	if len(l.Red) != 1 || !strings.Contains(l.Red[0], "fleet cache proof (rf2) did not pass") {
		t.Errorf("red %v", l.Red)
	}
	// Against a week whose repeats called VictoriaLogs less and more rows repeated without it.
	before := WeeklyLine{Numbers: map[string]float64{"perf.pin.reuse.repeat_vl_calls": 1, "perf.pin.reuse.repeat_without_vl_calls": 39,
		"perf.floor.reuse.repeat_vl_calls": 4}}
	judgeReuse(&l, &before)
	got := strings.Join(l.Red, "\n")
	for _, want := range []string{"pin: the repeats call VictoriaLogs 3 times, 1 the week before", "pin: 37 rows repeat without calling VictoriaLogs, 39 the week before"} {
		if !strings.Contains(got, want) {
			t.Errorf("red lacks %q: %v", want, l.Red)
		}
	}
	if strings.Contains(got, "floor: the repeats") {
		t.Error("an equal count is not a drop")
	}
	judgeReuse(&l, nil)
	if trend := RenderTrend([]WeeklyLine{l}); !strings.Contains(trend, "Red on 2026-11-16:") {
		t.Errorf("trend:\n%s", trend)
	}
	empty := WeeklyLine{Numbers: map[string]float64{}, Notes: map[string]string{}}
	if err := weeklyFleet(&empty, ""); err != nil || empty.Notes["fleet cache proof"] != "not run this week" {
		t.Errorf("no fleet: %v %v", err, empty.Notes)
	}
	write(t, filepath.Join(in.FleetDir, "rf3.json"), "{")
	if _, err := BuildWeekly(in); err == nil {
		t.Error("a broken fleet report is an error")
	}
}

func TestRunWeeklyCheckFailsARedWeek(t *testing.T) {
	in := weeklyFixture(t)
	dir := t.TempDir()
	write(t, filepath.Join(dir, "fleet/rf1.json"), `{"summary":[]}`)
	cfg, err := parseFlags([]string{"-weekly", "-check", "-date", in.Date, "-daily-summary", in.Summary, "-weekly-fleet", filepath.Join(dir, "fleet"),
		"-weekly-perf", in.SiteData, "-over-2x", in.Over2x, "-weekly-history", filepath.Join(dir, "h.jsonl"), "-weekly-trend", filepath.Join(dir, "t.md")})
	if err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 1 || !strings.Contains(out.String(), "red: the fleet cache proof (rf1) did not pass") {
		t.Errorf("red week: %d %s %s", code, out.String(), errs.String())
	}
	if _, err := os.Stat(cfg.trend); err != nil {
		t.Error("the trend is written before the week turns red")
	}
}
