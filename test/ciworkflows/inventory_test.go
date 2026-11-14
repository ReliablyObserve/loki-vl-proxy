package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseCron(t *testing.T) {
	cases := []struct {
		cron, cadence, human string
		period               time.Duration
		err                  bool
	}{
		{cron: "23 2 * * *", cadence: "daily", human: "daily 02:23 UTC", period: 24 * time.Hour},
		{cron: "17 3 * * 1", cadence: "weekly", human: "weekly, Monday 03:17 UTC", period: 7 * 24 * time.Hour},
		{cron: "43 13 * * 6", cadence: "weekly", human: "weekly, Saturday 13:43 UTC", period: 7 * 24 * time.Hour},
		{cron: "0 6 * * 0", cadence: "weekly", human: "weekly, Sunday 06:00 UTC", period: 7 * 24 * time.Hour},
		{cron: "5 1 15 * *", cadence: "monthly", human: "monthly, day 15 01:05 UTC", period: 31 * 24 * time.Hour},
		{cron: "7 * * * *", cadence: "hourly", human: "hourly at :07", period: time.Hour},
		{cron: "15 */4 * * *", cadence: "hourly", human: "every 4 hours at :15", period: 4 * time.Hour},
		// Forms the page does not name: a day of the week range, a list, a month, a step over minutes.
		{cron: "0 6 * * 1-5", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "0 6,18 * * *", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "0 6 1 1 *", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "*/5 * * * *", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "0 * 1 * *", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "0 */30 * * *", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "0 6 * * 7", cadence: "custom", human: "custom", period: 24 * time.Hour},
		{cron: "0 6 * * *  extra", err: true},
		{cron: "", err: true},
	}
	for _, c := range cases {
		got, err := ParseCron(c.cron)
		if c.err {
			if err == nil {
				t.Errorf("%q: want an error", c.cron)
			}
			continue
		}
		if err != nil || got.Cadence != c.cadence || got.Human != c.human || got.Period != c.period {
			t.Errorf("%q: got %+v, %v; want %s %q %s", c.cron, got, err, c.cadence, c.human, c.period)
		}
	}
}

func TestParseWorkflow(t *testing.T) {
	cases := []struct {
		name, src string
		events    []string
		triggers  []string
		schedules int
	}{
		{"string on", "name: A\non: push\n", []string{"push"}, []string{"push"}, 0},
		{"list on", "on: [push, pull_request]\n", []string{"pull_request", "push"}, []string{"pull_request", "push"}, 0},
		{"map on", `name: B
on:
  push:
    branches: [main]
    tags: ["v*"]
    paths-ignore: ["docs/**", "*.md"]
  pull_request:
    branches: [main]
    types: [opened, labeled]
    paths: [a, b, c]
  schedule:
    - cron: "17 3 * * 1"
    - cron: "0 4 * * *"
  workflow_dispatch:
    inputs:
      runs: {default: "7"}
      dry_run: {type: boolean}
  workflow_run:
    workflows: [CI, Release]
    types: [completed]
  workflow_call:
`, []string{"pull_request", "push", "schedule", "workflow_call", "workflow_dispatch", "workflow_run"}, []string{
			"pull_request (main; types opened, labeled; 3 paths)",
			"push (main; tags v*; 2 paths-ignore)",
			"schedule `17 3 * * 1` (weekly, Monday 03:17 UTC), `0 4 * * *` (daily 04:00 UTC)",
			"workflow_call",
			"workflow_dispatch (inputs: dry_run, runs)",
			"workflow_run (CI, Release; types completed)",
		}, 2},
		{"branches-ignore", "on:\n  push:\n    branches-ignore: [gh-pages]\n", []string{"push"}, []string{"push (branches-ignore gh-pages)"}, 0},
	}
	for _, c := range cases {
		w, err := ParseWorkflow("x.yaml", []byte(c.src))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !reflect.DeepEqual(w.Events, c.events) || !reflect.DeepEqual(w.Triggers, c.triggers) || len(w.Schedules) != c.schedules {
			t.Errorf("%s: got %q %q %d schedules", c.name, w.Events, w.Triggers, len(w.Schedules))
		}
	}
	if w, _ := ParseWorkflow("plain.yml", []byte("on: push\n")); w.Name != "plain" {
		t.Errorf("a workflow without a name is named after its file, got %q", w.Name)
	}
	for name, src := range map[string]string{
		"no on":                "name: x\njobs: {}\n",
		"on is a number":       "on: 3\n",
		"not yaml":             "on: [push\n",
		"schedule no cron":     "on:\n  schedule: []\n",
		"schedule bad cron":    "on:\n  schedule:\n    - cron: \"1 2 3\"\n",
		"schedule entry empty": "on:\n  schedule:\n    - {}\n",
	} {
		if _, err := ParseWorkflow("x.yaml", []byte(src)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func workflow(t *testing.T, src string) Workflow {
	t.Helper()
	w, err := ParseWorkflow("w.yaml", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestProblems(t *testing.T) {
	daily := workflow(t, "on:\n  schedule:\n    - cron: \"0 4 * * *\"\n")
	weekly := workflow(t, "on:\n  schedule:\n    - cron: \"0 4 * * 2\"\n")
	pr := workflow(t, "on: pull_request\n")
	push := workflow(t, "on: push\n")
	reusable := workflow(t, "on: workflow_call\n")
	follower := workflow(t, "on: workflow_run\n")
	cases := []struct {
		name string
		w    Workflow
		meta Meta
		want string
	}{
		{"daily ok", daily, Meta{Group: "daily", Verifies: "x"}, ""},
		{"daily without a daily cron", weekly, Meta{Group: "daily", Verifies: "x"}, "without a daily schedule"},
		{"weekly ok", weekly, Meta{Group: "weekly", Verifies: "x"}, ""},
		{"weekly without a weekly cron", daily, Meta{Group: "weekly", Verifies: "x"}, "without a weekly schedule"},
		{"gate ok", pr, Meta{Group: "pr-gates", Verifies: "x", Required: true}, ""},
		{"reusable gate", reusable, Meta{Group: "pr-gates", Verifies: "x"}, ""},
		{"follower gate", follower, Meta{Group: "pr-gates", Verifies: "x"}, ""},
		{"gate no pull request starts", push, Meta{Group: "pr-gates", Verifies: "x"}, "no pull request starts"},
		{"main push ok", push, Meta{Group: "main-push", Verifies: "x"}, ""},
		{"main push without push", pr, Meta{Group: "main-push", Verifies: "x"}, "without a push trigger"},
		{"required but no pull request", push, Meta{Group: "release", Verifies: "x", Required: true}, "a required check that no pull request starts"},
		{"unknown group", push, Meta{Group: "nightly", Verifies: "x"}, `unknown group "nightly"`},
		{"no verifies", push, Meta{Group: "maintenance", Verifies: " "}, "says nothing"},
	}
	for _, c := range cases {
		got := Problems([]Workflow{c.w}, MetaFile{Workflows: map[string]Meta{"w.yaml": c.meta}})
		if c.want == "" {
			if len(got) != 0 {
				t.Errorf("%s: got %q", c.name, got)
			}
			continue
		}
		if len(got) != 1 || !strings.Contains(got[0], c.want) {
			t.Errorf("%s: got %q, want one problem with %q", c.name, got, c.want)
		}
	}
	got := Problems([]Workflow{push}, MetaFile{Workflows: map[string]Meta{"gone.yaml": {Group: "maintenance", Verifies: "x"}}})
	if len(got) != 2 || !strings.Contains(got[0], "gone.yaml: an inventory row for a workflow that does not exist") ||
		!strings.Contains(got[1], "w.yaml: no row in the inventory") {
		t.Errorf("a renamed workflow is a missing row and a stale row, got %q", got)
	}
}

func TestRenderShowsTheRecordedRuns(t *testing.T) {
	sched := workflow(t, "name: Nightly | report\non:\n  schedule:\n    - cron: \"0 4 * * *\"\n  workflow_dispatch:\n")
	sched.File = "nightly.yaml"
	gate := workflow(t, "name: Gate\non: pull_request\n")
	gate.File = "gate.yaml"
	meta := MetaFile{Workflows: map[string]Meta{
		"nightly.yaml": {Group: "daily", Verifies: "line one\nline two"},
		"gate.yaml":    {Group: "pr-gates", Verifies: "gates", Required: true},
	}}
	empty := Render([]Workflow{gate, sched}, meta, RunsFile{})
	for _, want := range []string{"it has not recorded them yet", "| `gate.yaml` Gate | pull_request | gates | yes | pull requests only | not scheduled |",
		"| `nightly.yaml` Nightly \\| report |", "line one line two", "not recorded yet | not recorded yet |"} {
		if !strings.Contains(empty, want) {
			t.Errorf("page without runs lacks %q:\n%s", want, empty)
		}
	}
	if strings.Index(empty, "## Pull request gates") > strings.Index(empty, "## Daily") || strings.Contains(empty, "## Weekly") {
		t.Errorf("groups out of order or an empty group shown:\n%s", empty)
	}
	runs := RunsFile{Fetched: "2026-11-14T06:30:00Z", Workflows: map[string]WorkflowRuns{
		"nightly.yaml": {Main: &Run{Conclusion: "success", Date: "2026-11-14", URL: "https://x/1"},
			Scheduled: &Run{Conclusion: "failure", Date: "2026-11-13"}},
	}}
	page := Render([]Workflow{gate, sched}, meta, runs)
	for _, want := range []string{"recorded 2026-11-14T06:30:00Z", "[success 2026-11-14](https://x/1) | failure 2026-11-13 |"} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q:\n%s", want, page)
		}
	}
	other := workflow(t, "on:\n  schedule:\n    - cron: \"0 4 * * *\"\n")
	other.File = "other.yaml"
	meta.Workflows["other.yaml"] = Meta{Group: "daily", Verifies: "x"}
	if page := Render([]Workflow{other}, meta, runs); !strings.Contains(page, "| none | none |") {
		t.Errorf("a workflow with no recorded run shows none:\n%s", page)
	}
}

func writeFile(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRunWritesAndChecks(t *testing.T) {
	dir := t.TempDir()
	cfg := config{workflows: filepath.Join(dir, "wf"), meta: filepath.Join(dir, "meta.json"),
		runs: filepath.Join(dir, "runs.json"), out: filepath.Join(dir, "page.md")}
	writeFile(t, filepath.Join(cfg.workflows, "a.yaml"), "name: A\non: pull_request\n")
	writeFile(t, filepath.Join(cfg.workflows, "b.yml"), "name: B\non:\n  schedule:\n    - cron: \"0 4 * * 1\"\n")
	writeFile(t, cfg.meta, `{"workflows": {"a.yaml": {"group": "pr-gates", "verifies": "a", "required": true},
		"b.yml": {"group": "weekly", "verifies": "b"}}}`)
	var out, errs bytes.Buffer
	if code := run(config{workflows: cfg.workflows, meta: cfg.meta, runs: cfg.runs, out: cfg.out, check: true}, &out, &errs); code != 1 ||
		!strings.Contains(errs.String(), "is stale") {
		t.Fatalf("check before the page exists: %d %s", code, errs.String())
	}
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("write: %d %s", code, errs.String())
	}
	check := cfg
	check.check = true
	out.Reset()
	errs.Reset()
	if code := run(check, &out, &errs); code != 0 || !strings.Contains(out.String(), "2 workflows") {
		t.Fatalf("check after write: %d %s %s", code, out.String(), errs.String())
	}
	// A changed trigger makes the page stale.
	writeFile(t, filepath.Join(cfg.workflows, "b.yml"), "name: B\non:\n  schedule:\n    - cron: \"0 5 * * 1\"\n")
	errs.Reset()
	if code := run(check, &out, &errs); code != 1 || !strings.Contains(errs.String(), "is stale") {
		t.Fatalf("changed trigger: %d %s", code, errs.String())
	}
	// A new workflow without a row fails the check and the write.
	writeFile(t, filepath.Join(cfg.workflows, "c.yaml"), "on: push\n")
	errs.Reset()
	if code := run(cfg, &out, &errs); code != 1 || !strings.Contains(errs.String(), "c.yaml: no row in the inventory") {
		t.Fatalf("new workflow: %d %s", code, errs.String())
	}
	if code := run(check, &out, &errs); code != 1 {
		t.Fatalf("check with a new workflow: %d", code)
	}
	// Recorded runs reach the page.
	if err := os.Remove(filepath.Join(cfg.workflows, "c.yaml")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfg.runs, `{"fetched": "2026-11-14T06:00:00Z", "workflows": {"b.yml": {"scheduled": {"conclusion": "success", "date": "2026-11-09"}}}}`)
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("write with runs: %d %s", code, errs.String())
	}
	if page, _ := os.ReadFile(cfg.out); !strings.Contains(string(page), "success 2026-11-09") {
		t.Errorf("recorded run missing:\n%s", page)
	}
}

func TestRunReportsUnreadableInputs(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "wf")
	writeFile(t, filepath.Join(good, "a.yaml"), "on: push\n")
	meta := filepath.Join(dir, "meta.json")
	writeFile(t, meta, `{"workflows": {"a.yaml": {"group": "main-push", "verifies": "a"}}}`)
	bad := filepath.Join(dir, "bad")
	writeFile(t, filepath.Join(bad, "x.yaml"), "name: x\n")
	unknown := filepath.Join(dir, "unknown.json")
	writeFile(t, unknown, `{"workflows": {}, "extra": 1}`)
	badRuns := filepath.Join(dir, "runs.json")
	writeFile(t, badRuns, `{"fetched": 1}`)
	cases := map[string]config{
		"workflow without on": {workflows: bad, meta: meta, runs: badRuns, out: filepath.Join(dir, "p.md")},
		"missing meta":        {workflows: good, meta: filepath.Join(dir, "none.json"), out: filepath.Join(dir, "p.md")},
		"unknown meta key":    {workflows: good, meta: unknown, out: filepath.Join(dir, "p.md")},
		"bad runs":            {workflows: good, meta: meta, runs: badRuns, out: filepath.Join(dir, "p.md")},
		"runs is a directory": {workflows: good, meta: meta, runs: dir, out: filepath.Join(dir, "p.md")},
		"unwritable page":     {workflows: good, meta: meta, runs: filepath.Join(dir, "none.json"), out: filepath.Join(dir, "no", "such", "p.md")},
	}
	for name, cfg := range cases {
		var out, errs bytes.Buffer
		if code := run(cfg, &out, &errs); code != 2 {
			t.Errorf("%s: exit %d, want 2 (%s)", name, code, errs.String())
		}
	}
}

// The repository's own page is current and every workflow has a row: the same check the lint job runs.
func TestRepositoryInventoryIsCurrent(t *testing.T) {
	var out, errs bytes.Buffer
	cfg := config{workflows: "../../.github/workflows", meta: "inventory.json",
		runs: "../../conformance/registry/generated/verification/workflow-runs.json", out: "../../docs/ci-workflows.md", check: true}
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("docs/ci-workflows.md: %s\nrun go run ./test/ciworkflows from the repository root", errs.String())
	}
}

// Any workflow text parses or is refused without a panic, and a schedule it accepts has a positive period.
func FuzzParseWorkflow(f *testing.F) {
	for _, seed := range []string{"on: push\n", "on: [push, pull_request]\n", "on:\n  schedule:\n    - cron: \"17 3 * * 1\"\n",
		"on:\n  schedule:\n    - cron: \"15 */4 * * *\"\n  workflow_dispatch:\n    inputs:\n      a: {}\n", "on: 3\n", "name: x\n",
		"on:\n  schedule:\n    - cron: \"* * * * * *\"\n", "on:\n  workflow_run:\n    workflows: [CI]\n    types: [completed]\n"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, src string) {
		w, err := ParseWorkflow("f.yaml", []byte(src))
		if err != nil {
			return
		}
		if len(w.Triggers) != len(w.Events) {
			t.Fatalf("%d triggers for %d events", len(w.Triggers), len(w.Events))
		}
		for _, s := range w.Schedules {
			if s.Period <= 0 || s.Human == "" {
				t.Fatalf("schedule %+v", s)
			}
		}
	})
}

// BenchmarkRenderInventory is the generator's cost over the repository's workflows (the lint step's check).
func BenchmarkRenderInventory(b *testing.B) {
	workflows, err := ListWorkflows("../../.github/workflows")
	if err != nil {
		b.Fatal(err)
	}
	meta, err := LoadMeta("inventory.json")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = Problems(workflows, meta)
		_ = Render(workflows, meta, RunsFile{})
	}
}
