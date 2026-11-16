package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func apiAnswer(runs ...string) string {
	return `{"total_count": 9, "workflow_runs": [` + strings.Join(runs, ",") + `]}`
}

func apiRun(status, conclusion, created string) string {
	return `{"status":"` + status + `","conclusion":"` + conclusion + `","event":"schedule","html_url":"https://x/` + created +
		`","created_at":"` + created + `"}`
}

var fetched = time.Date(2026, 11, 14, 7, 0, 0, 0, time.UTC)

func TestIngestTakesTheNewestCompletedRun(t *testing.T) {
	dir := t.TempDir()
	daily := workflow(t, "on:\n  schedule:\n    - cron: \"0 4 * * *\"\n  push:\n")
	daily.File = "daily.yaml"
	gate := workflow(t, "on: pull_request\n")
	gate.File = "gate.yaml"
	writeFile(t, filepath.Join(dir, "daily.yaml.main.json"), apiAnswer(apiRun("in_progress", "", "2026-11-14T06:00:00Z"),
		apiRun("completed", "failure", "2026-11-13T04:01:00Z")))
	writeFile(t, filepath.Join(dir, "daily.yaml.schedule.json"), apiAnswer(apiRun("completed", "success", "2026-11-14T04:02:00Z")))
	// A pull-request-only workflow's schedule answer is never read.
	writeFile(t, filepath.Join(dir, "gate.yaml.schedule.json"), "not json")
	runs, err := Ingest(dir, []Workflow{daily, gate}, fetched)
	if err != nil {
		t.Fatal(err)
	}
	d := runs.Workflows["daily.yaml"]
	if runs.Fetched != "2026-11-14T07:00:00Z" || d.Main == nil || d.Main.Conclusion != "failure" || d.Main.Date != "2026-11-13" ||
		d.Scheduled == nil || d.Scheduled.At != "2026-11-14T04:02:00Z" || d.Scheduled.URL != "https://x/2026-11-14T04:02:00Z" {
		t.Errorf("ingested %+v %+v %+v", runs, d.Main, d.Scheduled)
	}
	if _, ok := runs.Workflows["gate.yaml"]; ok {
		t.Error("a workflow with no recorded run has no entry")
	}
	for name, body := range map[string]string{"not json": "{", "bad time": apiAnswer(apiRun("completed", "success", "yesterday"))} {
		writeFile(t, filepath.Join(dir, "daily.yaml.main.json"), body)
		if _, err := Ingest(dir, []Workflow{daily}, fetched); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	writeFile(t, filepath.Join(dir, "daily.yaml.main.json"), apiAnswer())
	writeFile(t, filepath.Join(dir, "daily.yaml.schedule.json"), "{")
	if _, err := Ingest(dir, []Workflow{daily}, fetched); err == nil {
		t.Error("a broken schedule answer is an error")
	}
	if err := os.Remove(filepath.Join(dir, "daily.yaml.schedule.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "daily.yaml.schedule.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Ingest(dir, []Workflow{daily}, fetched); err == nil {
		t.Error("an unreadable answer is an error")
	}
}

func TestCheckHealth(t *testing.T) {
	mk := func(file, cron string) Workflow {
		w := workflow(t, "on:\n  schedule:\n    - cron: \""+cron+"\"\n")
		w.File = file
		return w
	}
	both := workflow(t, "on:\n  schedule:\n    - cron: \"0 6 * * 1\"\n    - cron: \"0 4 * * *\"\n")
	both.File = "both.yaml"
	gate := workflow(t, "on: pull_request\n")
	gate.File = "gate.yaml"
	workflows := []Workflow{mk("ok-daily.yaml", "0 4 * * *"), mk("late-daily.yaml", "0 4 * * *"), mk("red-weekly.yaml", "0 4 * * 1"),
		mk("ok-weekly.yaml", "0 4 * * 1"), mk("missing.yaml", "0 4 * * *"), mk("self.yaml", "0 7 * * *"), both, gate}
	run := func(conclusion, at string) *Run {
		return &Run{Conclusion: conclusion, Date: at[:10], At: at, URL: "u"}
	}
	runs := RunsFile{Fetched: "2026-11-14T07:00:00Z", Workflows: map[string]WorkflowRuns{
		"ok-daily.yaml":   {Scheduled: run("success", "2026-11-14T04:01:00Z")},
		"late-daily.yaml": {Scheduled: run("success", "2026-11-12T04:01:00Z")},
		"red-weekly.yaml": {Scheduled: run("failure", "2026-11-09T04:01:00Z")},
		"ok-weekly.yaml":  {Scheduled: run("success", "2026-11-09T04:01:00Z")},
		// The shortest period counts: a weekly and daily workflow last run four days ago is late.
		"both.yaml": {Scheduled: run("success", "2026-11-10T06:00:00Z")},
	}}
	h, err := CheckHealth(workflows, runs, map[string]bool{"self.yaml": true})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range h.Problems {
		got = append(got, p.Workflow+": "+p.Problem)
	}
	want := []string{
		"both.yaml: last scheduled run 2026-11-10 is older than its period (24h0m0s) and a day",
		"late-daily.yaml: last scheduled run 2026-11-12 is older than its period (24h0m0s) and a day",
		"missing.yaml: no scheduled run recorded",
		"red-weekly.yaml: last scheduled run 2026-11-09 ended failure",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") || h.Scheduled != 6 || h.CheckedAt != runs.Fetched {
		t.Errorf("health:\n%s\nscheduled %d", strings.Join(got, "\n"), h.Scheduled)
	}
	if _, err := CheckHealth(workflows, RunsFile{}, nil); err == nil {
		t.Error("a runs record without a fetch time is an error")
	}
	runs.Workflows["ok-daily.yaml"] = WorkflowRuns{Scheduled: &Run{Conclusion: "success", Date: "2026-11-14"}}
	if _, err := CheckHealth(workflows, runs, nil); err == nil {
		t.Error("a scheduled run without a time is an error")
	}
}

func TestRunIngestsAndWritesHealth(t *testing.T) {
	dir := t.TempDir()
	cfg := config{workflows: filepath.Join(dir, "wf"), meta: filepath.Join(dir, "meta.json"), runs: filepath.Join(dir, "gen", "runs.json"),
		out: filepath.Join(dir, "page.md"), ingest: filepath.Join(dir, "api"), health: filepath.Join(dir, "health.json"),
		skip: "report.yaml", now: func() time.Time { return fetched }}
	writeFile(t, filepath.Join(cfg.workflows, "nightly.yaml"), "name: N\non:\n  schedule:\n    - cron: \"0 4 * * *\"\n")
	writeFile(t, filepath.Join(cfg.workflows, "report.yaml"), "name: R\non:\n  schedule:\n    - cron: \"0 7 * * *\"\n")
	writeFile(t, cfg.meta, `{"workflows": {"nightly.yaml": {"group": "daily", "verifies": "n"}, "report.yaml": {"group": "daily", "verifies": "r"}}}`)
	writeFile(t, filepath.Join(cfg.ingest, "nightly.yaml.schedule.json"), apiAnswer(apiRun("completed", "failure", "2026-11-14T04:00:00Z")))
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("run: %d %s", code, errs.String())
	}
	raw, err := os.ReadFile(cfg.health)
	if err != nil {
		t.Fatal(err)
	}
	var h Health
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	if h.Scheduled != 1 || len(h.Problems) != 1 || h.Problems[0].Workflow != "nightly.yaml" {
		t.Errorf("health %+v", h)
	}
	page, _ := os.ReadFile(cfg.out)
	if !strings.Contains(string(page), "[failure 2026-11-14](https://x/2026-11-14T04:00:00Z)") || !strings.Contains(out.String(), "1 health problems") {
		t.Errorf("page:\n%s\n%s", page, out.String())
	}
	// The page is current for -check against the record just written.
	check := cfg
	check.ingest, check.health, check.check = "", "", true
	if code := run(check, &out, &errs); code != 0 {
		t.Errorf("check after ingest: %d %s", code, errs.String())
	}
	// Errors: a broken answer, an unwritable record, a record without a fetch time, an unwritable health file.
	bad := cfg
	writeFile(t, filepath.Join(cfg.ingest, "nightly.yaml.main.json"), "{")
	if code := run(bad, &out, &errs); code != 2 {
		t.Errorf("broken answer: %d", code)
	}
	if err := os.Remove(filepath.Join(cfg.ingest, "nightly.yaml.main.json")); err != nil {
		t.Fatal(err)
	}
	bad.runs = filepath.Join(cfg.health, "runs.json")
	if code := run(bad, &out, &errs); code != 2 {
		t.Errorf("unwritable record: %d", code)
	}
	noFetch := cfg
	noFetch.ingest, noFetch.runs = "", filepath.Join(dir, "none.json")
	if code := run(noFetch, &out, &errs); code != 2 {
		t.Errorf("health without a record: %d", code)
	}
	bad = cfg
	bad.health = filepath.Join(cfg.health, "h.json")
	if code := run(bad, &out, &errs); code != 2 {
		t.Errorf("unwritable health: %d", code)
	}
}

// A scheduled workflow added within its first period has had no chance to run: it is pending its first run, which is
// neither a problem nor counted among the scheduled workflows; one added long ago with no run is still missing.
func TestCheckHealthPendingFirstRun(t *testing.T) {
	mk := func(file, cron string, added string) Workflow {
		w := workflow(t, "on:\n  schedule:\n    - cron: \""+cron+"\"\n")
		w.File = file
		if added != "" {
			w.AddedAt, _ = time.Parse(time.RFC3339, added)
		}
		return w
	}
	workflows := []Workflow{
		mk("new-daily.yaml", "0 4 * * *", "2026-11-14T03:00:00Z"),  // added yesterday morning: first run due
		mk("new-weekly.yaml", "0 4 * * 1", "2026-11-10T03:00:00Z"), // added this week: not yet a week and a day
		mk("old-weekly.yaml", "0 4 * * 1", "2026-10-01T03:00:00Z"), // added long ago, never ran
		mk("unknown-age.yaml", "0 4 * * *", ""),                    // git does not know it
		mk("old-and-running.yaml", "0 4 * * *", "2026-10-01T03:00:00Z"),
	}
	runs := RunsFile{Fetched: "2026-11-14T07:00:00Z", Workflows: map[string]WorkflowRuns{
		"old-and-running.yaml": {Scheduled: &Run{Conclusion: "success", Date: "2026-11-14", At: "2026-11-14T04:01:00Z"}},
	}}
	h, err := CheckHealth(workflows, runs, nil)
	if err != nil {
		t.Fatal(err)
	}
	var pending, problems []string
	for _, p := range h.Pending {
		pending = append(pending, p.Workflow+": "+p.Problem)
	}
	for _, p := range h.Problems {
		problems = append(problems, p.Workflow+": "+p.Problem)
	}
	wantPending := "new-daily.yaml: added 2026-11-14, pending its first scheduled run\nnew-weekly.yaml: added 2026-11-10, pending its first scheduled run"
	wantProblems := "old-weekly.yaml: no scheduled run recorded\nunknown-age.yaml: no scheduled run recorded"
	if strings.Join(pending, "\n") != wantPending || strings.Join(problems, "\n") != wantProblems || h.Scheduled != 3 {
		t.Errorf("pending:\n%s\nproblems:\n%s\nscheduled %d", strings.Join(pending, "\n"), strings.Join(problems, "\n"), h.Scheduled)
	}
	// A pending workflow whose period has passed with no run is missing again.
	late := []Workflow{mk("late.yaml", "0 4 * * *", "2026-11-11T03:00:00Z")}
	if h, _ := CheckHealth(late, runs, nil); len(h.Pending) != 0 || len(h.Problems) != 1 {
		t.Errorf("a workflow past its first period: %+v", h)
	}
}

func TestAddedDates(t *testing.T) {
	var asked []string
	git := func(args ...string) ([]byte, error) {
		path := args[len(args)-1]
		asked = append(asked, path)
		switch path {
		case ".github/workflows/a.yaml":
			// newest first: the first add is the last line (a rename is followed)
			return []byte("2026-11-12T10:00:00+01:00\n2026-10-01T08:30:00Z\n"), nil
		case ".github/workflows/b.yaml":
			return []byte(""), nil
		}
		return nil, errors.New("not a repository")
	}
	ws := []Workflow{{File: "a.yaml"}, {File: "b.yaml"}, {File: "c.yaml"}}
	AddedDates(".github/workflows", ws, git)
	if want := time.Date(2026, 10, 1, 8, 30, 0, 0, time.UTC); !ws[0].AddedAt.Equal(want) {
		t.Errorf("a.yaml added %s, want %s", ws[0].AddedAt, want)
	}
	if !ws[1].AddedAt.IsZero() || !ws[2].AddedAt.IsZero() {
		t.Errorf("unknown files keep a zero time: %v %v", ws[1].AddedAt, ws[2].AddedAt)
	}
	if len(asked) != 3 {
		t.Errorf("asked %v", asked)
	}
}

// The jobs of a workflow's last run on main (<file>.main.jobs.json) are kept with it: a run ends failure when any one job
// does, and the daily report reads whether the test job passed.
func TestIngestKeepsTheJobsOfTheLastMainRun(t *testing.T) {
	dir := t.TempDir()
	ci := workflow(t, "on: push\n")
	ci.File = "ci.yaml"
	other := workflow(t, "on: push\n")
	other.File = "other.yaml"
	for _, f := range []string{"ci.yaml", "other.yaml"} {
		writeFile(t, filepath.Join(dir, f+".main.json"), apiAnswer(apiRun("completed", "failure", "2026-11-14T04:00:00Z")))
	}
	writeFile(t, filepath.Join(dir, "ci.yaml.main.jobs.json"),
		`{"total_count":3,"jobs":[{"name":"test","conclusion":"success"},{"name":"test-shard (1)","conclusion":"success"},{"name":"e2e-compat","conclusion":"failure"}]}`)
	got, err := Ingest(dir, []Workflow{ci, other}, fetched)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"test": "success", "test-shard (1)": "success", "e2e-compat": "failure"}
	if g := got.Workflows["ci.yaml"].Main.Jobs; len(g) != 3 || g["test"] != "success" || g["e2e-compat"] != "failure" {
		t.Errorf("jobs %v, want %v", g, want)
	}
	if got.Workflows["other.yaml"].Main.Jobs != nil {
		t.Errorf("a workflow whose jobs were not read has none: %v", got.Workflows["other.yaml"].Main.Jobs)
	}
	writeFile(t, filepath.Join(dir, "ci.yaml.main.jobs.json"), "{")
	if _, err := Ingest(dir, []Workflow{ci}, fetched); err == nil {
		t.Error("an unreadable jobs answer is an error")
	}
}
