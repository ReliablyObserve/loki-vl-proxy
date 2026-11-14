package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type wfJob struct {
	If          string            `yaml:"if"`
	Needs       any               `yaml:"needs"`
	Permissions map[string]string `yaml:"permissions"`
	Env         map[string]string `yaml:"env"`
	Steps       []struct {
		Name string            `yaml:"name"`
		Uses string            `yaml:"uses"`
		If   string            `yaml:"if"`
		Run  string            `yaml:"run"`
		With map[string]any    `yaml:"with"`
		Env  map[string]string `yaml:"env"`
	} `yaml:"steps"`
}

func loadJobs(t *testing.T, file string) (map[string]wfJob, []byte) {
	t.Helper()
	raw, err := os.ReadFile("../../.github/workflows/" + file)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Jobs map[string]wfJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Jobs, raw
}

func (j wfJob) step(t *testing.T, name string) (int, string) {
	t.Helper()
	for i, s := range j.Steps {
		if s.Name == name {
			return i, s.Run
		}
	}
	t.Fatalf("no step %q", name)
	return 0, ""
}

func minuteOfWeek(t *testing.T, cron string, day int) int {
	t.Helper()
	f := strings.Fields(cron)
	var minute, hour int
	if _, err := ParseCron(cron); err != nil || len(f) != 5 {
		t.Fatalf("cron %q", cron)
	}
	minute, hour = atoi(t, f[0]), atoi(t, f[1])
	return day*24*60 + hour*60 + minute
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			t.Fatalf("not a number: %q", s)
		}
		n = n*10 + int(r-'0')
	}
	return n
}

// The daily report runs once a day after the query corpus (whose run it reads) and clear of the weekly perf
// run's window, and delivers through one rolling bot pull request that only holds bot-owned files.
func TestDailyReportWiring(t *testing.T) {
	jobs, raw := loadJobs(t, "daily-report.yaml")
	w, err := ParseWorkflow("daily-report.yaml", raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Schedules) != 1 || w.Schedules[0].Cadence != "daily" || !w.HasEvent("workflow_dispatch") || !w.HasEvent("pull_request") {
		t.Fatalf("triggers: %+v", w)
	}
	daily := w.Schedules[0].Cron
	corpus, _ := loadWorkflowFile(t, "query-corpus.yaml")
	perf, _ := loadWorkflowFile(t, "perf-daily.yaml")
	if minuteOfWeek(t, daily, 0)%(24*60) <= minuteOfWeek(t, corpus.Schedules[0].Cron, 0)%(24*60) {
		t.Errorf("the daily report (%s) runs before the query corpus (%s) of the day", daily, corpus.Schedules[0].Cron)
	}
	perfStart := minuteOfWeek(t, perf.Schedules[0].Cron, 0) % (24 * 60)
	if start := minuteOfWeek(t, daily, 0) % (24 * 60); start >= perfStart && start < perfStart+240 {
		t.Errorf("the daily report (%s) starts inside the weekly perf run's window", daily)
	}

	if v := jobs["validate"]; v.If != "github.event_name == 'pull_request'" || v.Permissions["contents"] != "read" {
		t.Errorf("validate: %+v", v)
	}
	for name, j := range jobs {
		if name == "report" {
			continue
		}
		for k, v := range j.Permissions {
			if v == "write" {
				t.Errorf("job %s holds %s: write; only the report job writes", name, k)
			}
		}
		if name != "validate" && j.If != "" && !strings.Contains(j.If, "!= 'pull_request'") {
			t.Errorf("job %s may run on a pull request: %q", name, j.If)
		}
	}
	if p := jobs["inventory"].Permissions; p["actions"] != "read" || len(p) != 2 {
		t.Errorf("inventory permissions %v", p)
	}
	_, parity := jobs["parity"].step(t, "Differential run (slice)")
	if !strings.Contains(parity, "--mode validate") || strings.Contains(parity, "--mode full") || strings.Contains(parity, "prune") {
		t.Errorf("the daily parity run is a slice that never prunes: %q", parity)
	}

	report := jobs["report"]
	if report.Permissions["contents"] != "write" || report.Permissions["pull-requests"] != "write" || report.Env["BRANCH"] != "bench/daily-report" {
		t.Errorf("report job: %+v", report)
	}
	for _, s := range report.Steps {
		if strings.HasPrefix(s.Uses, "actions/cache") {
			t.Error("the job holding the bot token restores no cache")
		}
		if strings.HasPrefix(s.Uses, "actions/checkout") && (s.With["ref"] != "main" || s.With["persist-credentials"] != false) {
			t.Errorf("checkout: %v", s.With)
		}
		if strings.HasPrefix(s.Uses, "actions/setup-go") && s.With["cache"] != false {
			t.Errorf("setup-go: %v", s.With)
		}
	}
	prAt, pr := report.step(t, "Open or update the daily report PR")
	files := regexp.MustCompile(`FILES=\(([^)]*)\)`).FindStringSubmatch(pr)
	if files == nil {
		t.Fatal("no FILES list")
	}
	for _, f := range strings.Fields(files[1]) {
		switch f {
		case "conformance/registry/generated/verification", "bench/ab/history/daily-smoke", "docs/ci-workflows.md":
		default:
			t.Errorf("the daily report commits %s, which it does not own", f)
		}
	}
	if strings.Contains(pr, "conformance/registry/cases") || regexp.MustCompile(`git add (-A|\.|--all)`).MatchString(pr) {
		t.Error("the daily report never stages registry cases or everything")
	}
	push, patch := strings.Index(pr, `push origin "${TMP_BRANCH}"`), strings.Index(pr, "-X PATCH")
	if push < 0 || patch < push || strings.Contains(pr, `push origin "$BRANCH"`) || regexp.MustCompile(`push [^\n]*(--force|-f )`).MatchString(pr) ||
		!strings.Contains(pr, "git/ref/heads/${BRANCH}") || !strings.Contains(pr, "-X DELETE") {
		t.Error("the report branch is moved only through the API, after a push to a new temporary ref")
	}
	failAt, _ := report.step(t, "Fail on a red day")
	if failAt < prAt {
		t.Error("the pull request is opened before the job turns red")
	}
	_, arm := report.step(t, "Arm auto-merge on the daily report PR")
	if !strings.Contains(arm, "RELEASE_PR_TOKEN is not configured") {
		t.Error("auto-merge keeps the warning path when the bot token is not configured")
	}
}

func loadWorkflowFile(t *testing.T, file string) (Workflow, []byte) {
	t.Helper()
	raw, err := os.ReadFile("../../.github/workflows/" + file)
	if err != nil {
		t.Fatal(err)
	}
	w, err := ParseWorkflow(file, raw)
	if err != nil {
		t.Fatal(err)
	}
	return w, raw
}

// The weekly full detailed run beside Perf weekly's A/B: still weekly, every check on both VictoriaLogs
// versions, nothing pruned or written by the extra jobs, the release dispatched as a dry run, and the trend
// delivered on the weekly pull request like the A/B report.
func TestWeeklyRunWiring(t *testing.T) {
	jobs, raw := loadJobs(t, "perf-daily.yaml")
	w, err := ParseWorkflow("perf-daily.yaml", raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Schedules) != 1 || w.Schedules[0].Cadence != "weekly" {
		t.Fatalf("Perf weekly stays weekly: %+v", w.Schedules)
	}
	for _, name := range []string{"weekly-plan", "weekly-sweep", "weekly-release-dry-run", "weekly-fleet"} {
		if jobs[name].If != "github.event_name != 'pull_request'" {
			t.Errorf("%s runs on a pull request: %q", name, jobs[name].If)
		}
	}
	for name, j := range jobs {
		if name == "daily" || name == "weekly-summary" {
			continue
		}
		for k, v := range j.Permissions {
			dispatches := name == "weekly-release-dry-run" && k == "actions"
			if v == "write" && !dispatches {
				t.Errorf("job %s holds %s: write", name, k)
			}
		}
	}
	_, sweep := jobs["weekly-sweep"].step(t, "Full-size grammar sweep with the live oracles")
	if !strings.Contains(sweep, "TestGrammarSweep") {
		t.Error("the weekly sweep runs TestGrammarSweep")
	}
	_, parity := jobs["weekly-verify"].step(t, "Differential run (whole corpus, no pruning)")
	if !strings.Contains(parity, "--mode full --no-prune") {
		t.Errorf("the weekly parity run is the whole corpus and never prunes: %q", parity)
	}
	_, floor := jobs["weekly-ab-floor"].step(t, "Full A/B on the oldest supported VictoriaLogs")
	if !strings.Contains(floor, "daily.py --out \"$OUT\" --no-write") || !strings.Contains(jobs["weekly-ab-floor"].Env["VICTORIALOGS_IMAGE"], "floor_vl") {
		t.Errorf("the floor A/B writes nothing and runs on the oldest version: %q", floor)
	}
	if _, fleet := jobs["weekly-fleet"].step(t, "Three-proxy cache proof for RF1 and RF2"); !strings.Contains(fleet, "TestFleetPeerCacheProof") ||
		!strings.Contains(fleet, "FLEET_PROOF_REPORT") {
		t.Errorf("the weekly run holds the fleet cache proof: %q", fleet)
	}
	_, dispatch := jobs["weekly-release-dry-run"].step(t, "Dispatch release.yaml with dry_run and wait for it")
	if !strings.Contains(dispatch, "gh workflow run release.yaml") || !strings.Contains(dispatch, "-f dry_run=true") {
		t.Errorf("the release is dispatched as a dry run: %q", dispatch)
	}
	summary := jobs["weekly-summary"]
	if summary.Env["BRANCH"] != "bench/daily-perf" || summary.Permissions["contents"] != "write" {
		t.Errorf("weekly summary: %+v", summary)
	}
	prAt, pr := summary.step(t, "Add the weekly trend to the weekly report PR")
	files := regexp.MustCompile(`FILES=\(([^)]*)\)`).FindStringSubmatch(pr)
	if files == nil {
		t.Fatal("no FILES list")
	}
	for _, f := range strings.Fields(files[1]) {
		if !strings.HasPrefix(f, "conformance/registry/generated/verification/weekly-") {
			t.Errorf("the weekly summary commits %s", f)
		}
	}
	push, patch := strings.Index(pr, `push origin "${TMP_BRANCH}"`), strings.Index(pr, "-X PATCH")
	if push < 0 || patch < push || strings.Contains(pr, `push origin "$BRANCH"`) || regexp.MustCompile(`push [^\n]*(--force|-f )`).MatchString(pr) ||
		!strings.Contains(pr, "git/ref/heads/${BRANCH}") {
		t.Error("the weekly report branch is moved only through the API")
	}
	buildAt, _ := summary.step(t, "Build the report tool from main")
	baseAt, base := summary.step(t, "Start from the weekly report branch")
	if buildAt > baseAt || !strings.Contains(base, "--state open") {
		t.Error("the tool is built from main first, and the branch tip is a base only while its pull request is open")
	}
	failAt, fail := summary.step(t, "Fail when a weekly check is red")
	if !strings.Contains(fail, `v["result"] != "success"`) {
		t.Error("a skipped weekly job is red")
	}
	if failAt < prAt {
		t.Error("the trend is delivered before the job turns red")
	}
	for _, s := range summary.Steps {
		if strings.HasPrefix(s.Uses, "actions/setup-go") && s.With["cache"] != false {
			t.Error("the job holding the bot token restores no cache")
		}
	}
}
