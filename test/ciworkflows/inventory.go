package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Groups, in the order the page shows them.
var groups = []struct{ id, title string }{
	{"pr-gates", "Pull request gates"},
	{"main-push", "Main push"},
	{"daily", "Daily"},
	{"weekly", "Weekly"},
	{"release", "Release"},
	{"maintenance", "Maintenance"},
}

// Meta is the hand-written row of one workflow (inventory.json): what the file cannot say about itself.
type Meta struct {
	Group    string `json:"group"`
	Verifies string `json:"verifies"`
	Required bool   `json:"required"`
}

// MetaFile is inventory.json.
type MetaFile struct {
	Comment   string          `json:"$comment,omitempty"`
	Workflows map[string]Meta `json:"workflows"`
}

// Run is one workflow run as the inventory shows it.
type Run struct {
	Conclusion string `json:"conclusion"`
	Date       string `json:"date"`
	At         string `json:"at,omitempty"`
	Event      string `json:"event,omitempty"`
	URL        string `json:"url,omitempty"`
}

// RunsFile is the bot-owned record of the last runs (written by the daily report, read here).
type RunsFile struct {
	Fetched   string                  `json:"fetched"`
	Workflows map[string]WorkflowRuns `json:"workflows"`
}

// WorkflowRuns is the last run on main and the last scheduled run of one workflow.
type WorkflowRuns struct {
	Main      *Run `json:"main,omitempty"`
	Scheduled *Run `json:"scheduled,omitempty"`
}

// Schedule is one cron entry with its cadence.
type Schedule struct {
	Cron    string
	Cadence string // hourly, daily, weekly, monthly, custom
	Period  time.Duration
	Human   string
}

// Workflow is what a workflow file declares.
type Workflow struct {
	File      string
	Name      string
	Events    []string // sorted event names
	Triggers  []string // rendered, one per event
	Schedules []Schedule
	// AddedAt is when the file first entered the repository's history (AddedDates); zero when unknown.
	AddedAt time.Time
}

// HasEvent reports whether the workflow runs on the event.
func (w Workflow) HasEvent(name string) bool {
	for _, e := range w.Events {
		if e == name {
			return true
		}
	}
	return false
}

func (w Workflow) startedByPullRequest() bool {
	return w.HasEvent("pull_request") || w.HasEvent("pull_request_target") || w.HasEvent("merge_group")
}

// runsOnMain reports whether anything but a pull request starts the workflow on main.
func (w Workflow) runsOnMain() bool {
	for _, e := range []string{"push", "schedule", "workflow_dispatch", "workflow_run", "release"} {
		if w.HasEvent(e) {
			return true
		}
	}
	return false
}

// ListWorkflows reads every *.yaml and *.yml file of dir.
func ListWorkflows(dir string) ([]Workflow, error) {
	var files []string
	for _, pattern := range []string{"*.yaml", "*.yml"} {
		found, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, err
		}
		files = append(files, found...)
	}
	sort.Strings(files)
	out := make([]Workflow, 0, len(files))
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		w, err := ParseWorkflow(filepath.Base(path), raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, w)
	}
	return out, nil
}

// ParseWorkflow reads the name and the triggers of one workflow file.
func ParseWorkflow(file string, raw []byte) (Workflow, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return Workflow{}, err
	}
	w := Workflow{File: file}
	w.Name, _ = doc["name"].(string)
	if w.Name == "" {
		w.Name = strings.TrimSuffix(strings.TrimSuffix(file, ".yaml"), ".yml")
	}
	on, ok := doc["on"]
	if !ok {
		return w, errors.New("no `on:` key")
	}
	events := map[string]any{}
	switch x := on.(type) {
	case string:
		events[x] = nil
	case []any:
		for _, e := range x {
			events[fmt.Sprint(e)] = nil
		}
	case map[string]any:
		events = x
	default:
		return w, fmt.Errorf("`on:` is a %T", on)
	}
	for name := range events {
		w.Events = append(w.Events, name)
	}
	sort.Strings(w.Events)
	for _, name := range w.Events {
		text, schedules, err := renderTrigger(name, events[name])
		if err != nil {
			return w, fmt.Errorf("%s: %w", name, err)
		}
		w.Triggers = append(w.Triggers, text)
		w.Schedules = append(w.Schedules, schedules...)
	}
	return w, nil
}

func listOf(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, e := range list {
		out = append(out, fmt.Sprint(e))
	}
	return out
}

func renderTrigger(name string, body any) (string, []Schedule, error) {
	m, _ := body.(map[string]any)
	var parts []string
	switch name {
	case "schedule":
		list, _ := body.([]any)
		var schedules []Schedule
		for _, e := range list {
			entry, _ := e.(map[string]any)
			cron, _ := entry["cron"].(string)
			s, err := ParseCron(cron)
			if err != nil {
				return "", nil, err
			}
			schedules = append(schedules, s)
			parts = append(parts, fmt.Sprintf("`%s` (%s)", cron, s.Human))
		}
		if len(schedules) == 0 {
			return "", nil, errors.New("a schedule without a cron")
		}
		return "schedule " + strings.Join(parts, ", "), schedules, nil
	case "workflow_dispatch", "workflow_call":
		inputs, _ := m["inputs"].(map[string]any)
		keys := make([]string, 0, len(inputs))
		for k := range inputs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > 0 {
			parts = append(parts, "inputs: "+strings.Join(keys, ", "))
		}
	case "workflow_run":
		parts = append(parts, strings.Join(listOf(m["workflows"]), ", "))
	}
	for _, key := range []string{"branches", "branches-ignore", "tags", "types"} {
		if values := listOf(m[key]); len(values) > 0 {
			label := key
			if key == "branches" {
				label = ""
			}
			parts = append(parts, strings.TrimSpace(label+" "+strings.Join(values, ", ")))
		}
	}
	for _, key := range []string{"paths", "paths-ignore"} {
		if values := listOf(m[key]); len(values) > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", len(values), key))
		}
	}
	if len(parts) == 0 {
		return name, nil, nil
	}
	return name + " (" + strings.Join(parts, "; ") + ")", nil, nil
}

var weekdays = []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

// ParseCron reads a five-field cron entry and names its cadence. Only the forms the workflows use are
// named (a fixed minute, a fixed or every hour, every day or one weekday or one day of the month); any
// other entry is "custom" with a period of a day, the shortest a scheduled report here may miss.
func ParseCron(cron string) (Schedule, error) {
	f := strings.Fields(cron)
	if len(f) != 5 {
		return Schedule{}, fmt.Errorf("cron %q does not have five fields", cron)
	}
	s := Schedule{Cron: cron, Cadence: "custom", Period: 24 * time.Hour, Human: "custom"}
	minute, hour, dom, month, dow := f[0], f[1], f[2], f[3], f[4]
	if !isNumber(minute) || month != "*" {
		return s, nil
	}
	at := ""
	switch {
	case hour == "*":
		if dom == "*" && dow == "*" {
			s.Cadence, s.Period, s.Human = "hourly", time.Hour, fmt.Sprintf("hourly at :%02s", minute)
		}
		return s, nil
	case strings.HasPrefix(hour, "*/") && isNumber(hour[2:]) && dom == "*" && dow == "*":
		n := 0
		_, _ = fmt.Sscan(hour[2:], &n)
		if n > 0 && n < 24 {
			s.Cadence, s.Period, s.Human = "hourly", time.Duration(n)*time.Hour, fmt.Sprintf("every %d hours at :%02s", n, minute)
		}
		return s, nil
	case isNumber(hour):
		at = fmt.Sprintf("%02s:%02s UTC", hour, minute)
	default:
		return s, nil
	}
	switch {
	case dom == "*" && dow == "*":
		s.Cadence, s.Period, s.Human = "daily", 24*time.Hour, "daily "+at
	case dom == "*" && isNumber(dow) && len(dow) == 1 && dow[0] <= '6':
		s.Cadence, s.Period, s.Human = "weekly", 7*24*time.Hour, "weekly, "+weekdays[dow[0]-'0']+" "+at
	case dow == "*" && isNumber(dom):
		s.Cadence, s.Period, s.Human = "monthly", 31*24*time.Hour, "monthly, day "+dom+" "+at
	}
	return s, nil
}

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Problems lists every disagreement between the workflow files and the hand-written rows.
func Problems(workflows []Workflow, meta MetaFile) []string {
	var out []string
	known := map[string]bool{}
	valid := map[string]bool{}
	for _, g := range groups {
		valid[g.id] = true
	}
	for _, w := range workflows {
		known[w.File] = true
		m, ok := meta.Workflows[w.File]
		if !ok {
			out = append(out, fmt.Sprintf("%s: no row in the inventory; add it with its group, what it verifies and whether it is a required check", w.File))
			continue
		}
		if !valid[m.Group] {
			out = append(out, fmt.Sprintf("%s: unknown group %q", w.File, m.Group))
		}
		if strings.TrimSpace(m.Verifies) == "" {
			out = append(out, fmt.Sprintf("%s: says nothing about what it verifies", w.File))
		}
		out = append(out, groupProblems(w, m)...)
	}
	for file := range meta.Workflows {
		if !known[file] {
			out = append(out, fmt.Sprintf("%s: an inventory row for a workflow that does not exist", file))
		}
	}
	sort.Strings(out)
	return out
}

func groupProblems(w Workflow, m Meta) []string {
	cadence := func(want string) bool {
		for _, s := range w.Schedules {
			if s.Cadence == want {
				return true
			}
		}
		return false
	}
	switch m.Group {
	case "daily", "weekly":
		if !cadence(m.Group) {
			return []string{fmt.Sprintf("%s: in the %s group without a %s schedule", w.File, m.Group, m.Group)}
		}
	case "pr-gates":
		// A reusable workflow the gates call, or one that follows their runs, belongs with them.
		if !w.startedByPullRequest() && !w.HasEvent("workflow_call") && !w.HasEvent("workflow_run") {
			return []string{fmt.Sprintf("%s: a pull request gate that no pull request starts", w.File)}
		}
	case "main-push":
		if !w.HasEvent("push") {
			return []string{fmt.Sprintf("%s: in the main push group without a push trigger", w.File)}
		}
	}
	if m.Required && !w.startedByPullRequest() {
		return []string{fmt.Sprintf("%s: a required check that no pull request starts", w.File)}
	}
	return nil
}

// LoadMeta reads inventory.json.
func LoadMeta(path string) (MetaFile, error) {
	var m MetaFile
	raw, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

// LoadRuns reads the bot-owned runs file; a missing file is an empty record (no daily run yet).
func LoadRuns(path string) (RunsFile, error) {
	var r RunsFile
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

func renderRun(r *Run, fetched string) string {
	if fetched == "" {
		return "not recorded yet"
	}
	if r == nil {
		return "none"
	}
	text := r.Conclusion + " " + r.Date
	if r.URL != "" {
		text = fmt.Sprintf("[%s](%s)", text, r.URL)
	}
	return text
}

func cell(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "|", `\|`), "\n", " ")
}

// Render writes the inventory page.
func Render(workflows []Workflow, meta MetaFile, runs RunsFile) string {
	var b strings.Builder
	// CommonMark, not MDX, on the docs site: the page holds an HTML comment and inline HTML.
	b.WriteString("---\nsidebar_label: CI workflows\ndescription: Every CI workflow, what starts it, what it verifies and its last runs.\n" +
		"mdx:\n  format: md\n---\n\n")
	b.WriteString("# CI workflows\n\n")
	b.WriteString("<!-- Generated by `go run ./test/ciworkflows` from .github/workflows, test/ciworkflows/inventory.json and " +
		"conformance/registry/generated/verification/workflow-runs.json. Do not edit by hand. -->\n\n")
	b.WriteString("Every workflow under `.github/workflows`, grouped by what starts it. The triggers and schedules are read " +
		"from the workflow files; the group, what each workflow verifies and whether its check is required for a merge " +
		"come from `test/ciworkflows/inventory.json`. A pull request that adds, removes or renames a workflow, or changes " +
		"its triggers, regenerates this page (`go run ./test/ciworkflows`); the lint job fails otherwise.\n\n")
	if runs.Fetched == "" {
		b.WriteString("The last-run columns are filled by the daily report from the GitHub API; it has not recorded them yet.\n")
	} else {
		fmt.Fprintf(&b, "The last-run columns are filled by the daily report from the GitHub API; recorded %s.\n", runs.Fetched)
	}
	byGroup := map[string][]Workflow{}
	for _, w := range workflows {
		g := meta.Workflows[w.File].Group
		byGroup[g] = append(byGroup[g], w)
	}
	for _, g := range groups {
		list := byGroup[g.id]
		if len(list) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n## %s\n\n", g.title)
		b.WriteString("| Workflow | Triggers | Verifies | Required | Last run on main | Last scheduled run |\n")
		b.WriteString("|---|---|---|---|---|---|\n")
		for _, w := range list {
			m := meta.Workflows[w.File]
			required := "no"
			if m.Required {
				required = "yes"
			}
			last := runs.Workflows[w.File]
			onMain, scheduled := "pull requests only", "not scheduled"
			if w.runsOnMain() {
				onMain = renderRun(last.Main, runs.Fetched)
			}
			if len(w.Schedules) > 0 {
				scheduled = renderRun(last.Scheduled, runs.Fetched)
			}
			fmt.Fprintf(&b, "| `%s` %s | %s | %s | %s | %s | %s |\n", w.File, cell(w.Name),
				cell(strings.Join(w.Triggers, "<br />")), cell(m.Verifies), required, onMain, scheduled)
		}
	}
	return b.String()
}
