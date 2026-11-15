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
)

// apiRuns is the part of GitHub's "list workflow runs" answer the inventory reads.
type apiRuns struct {
	WorkflowRuns []struct {
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		Event      string `json:"event"`
		HTMLURL    string `json:"html_url"`
		CreatedAt  string `json:"created_at"`
	} `json:"workflow_runs"`
}

// newestCompleted is the newest finished run of an answer (GitHub lists the newest first); nil when none.
func newestCompleted(path string) (*Run, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var answer apiRuns
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for _, r := range answer.WorkflowRuns {
		if r.Status != "completed" {
			continue
		}
		at, err := time.Parse(time.RFC3339, r.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("%s: created_at %q: %w", path, r.CreatedAt, err)
		}
		return &Run{Conclusion: r.Conclusion, Date: at.UTC().Format("2006-01-02"), At: at.UTC().Format(time.RFC3339),
			Event: r.Event, URL: r.HTMLURL}, nil
	}
	return nil, nil
}

// Ingest reads the API answers the daily report saved for every workflow (<file>.main.json: the runs on
// main; <file>.schedule.json: the scheduled runs) into a runs record. A missing answer is a workflow with
// no run of that kind.
func Ingest(dir string, workflows []Workflow, fetched time.Time) (RunsFile, error) {
	out := RunsFile{Fetched: fetched.UTC().Format(time.RFC3339), Workflows: map[string]WorkflowRuns{}}
	for _, w := range workflows {
		var runs WorkflowRuns
		var err error
		if runs.Main, err = newestCompleted(filepath.Join(dir, w.File+".main.json")); err != nil {
			return out, err
		}
		if len(w.Schedules) > 0 {
			if runs.Scheduled, err = newestCompleted(filepath.Join(dir, w.File+".schedule.json")); err != nil {
				return out, err
			}
		}
		if runs.Main != nil || runs.Scheduled != nil {
			out.Workflows[w.File] = runs
		}
	}
	return out, nil
}

// slack is how much later than its period a scheduled run may be: a run that queued, or a schedule GitHub
// delayed, is not missing.
const slack = 24 * time.Hour

// HealthProblem is a scheduled workflow whose last scheduled run is missing, late or not green.
type HealthProblem struct {
	Workflow string `json:"workflow"`
	Problem  string `json:"problem"`
	URL      string `json:"url,omitempty"`
}

// Health is the daily report's view of the scheduled workflows.
type Health struct {
	CheckedAt string          `json:"checked_at"`
	Scheduled int             `json:"scheduled"`
	Problems  []HealthProblem `json:"problems"`
	// Pending are the scheduled workflows added too recently to have had a scheduled run (their first is due within
	// their period and a day of being added): not a problem, and not counted in Scheduled.
	Pending []HealthProblem `json:"pending"`
}

// CheckHealth lists every scheduled workflow (but the ones in skip) whose last scheduled run is missing,
// older than its shortest period plus a day, or did not succeed.
func CheckHealth(workflows []Workflow, runs RunsFile, skip map[string]bool) (Health, error) {
	h := Health{CheckedAt: runs.Fetched, Problems: []HealthProblem{}, Pending: []HealthProblem{}}
	now, err := time.Parse(time.RFC3339, runs.Fetched)
	if err != nil {
		return h, fmt.Errorf("the runs record has no fetch time: %w", err)
	}
	for _, w := range workflows {
		if len(w.Schedules) == 0 || skip[w.File] {
			continue
		}
		period := w.Schedules[0].Period
		for _, s := range w.Schedules[1:] {
			period = min(period, s.Period)
		}
		last := runs.Workflows[w.File].Scheduled
		// A workflow added within its first period has had no chance to run on its schedule (GitHub does not even
		// list a workflow file until it is on the default branch): it is pending its first run, not missing.
		if last == nil && !w.AddedAt.IsZero() && now.Sub(w.AddedAt) <= period+slack {
			h.Pending = append(h.Pending, HealthProblem{Workflow: w.File,
				Problem: fmt.Sprintf("added %s, pending its first scheduled run", w.AddedAt.UTC().Format("2006-01-02"))})
			continue
		}
		h.Scheduled++
		if last == nil {
			h.Problems = append(h.Problems, HealthProblem{Workflow: w.File, Problem: "no scheduled run recorded"})
			continue
		}
		at, err := time.Parse(time.RFC3339, last.At)
		if err != nil {
			return h, fmt.Errorf("%s: the last scheduled run has no time: %w", w.File, err)
		}
		if age := now.Sub(at); age > period+slack {
			h.Problems = append(h.Problems, HealthProblem{Workflow: w.File, URL: last.URL,
				Problem: fmt.Sprintf("last scheduled run %s is older than its period (%s) and a day", last.Date, period)})
		}
		if last.Conclusion != "success" {
			h.Problems = append(h.Problems, HealthProblem{Workflow: w.File, URL: last.URL,
				Problem: fmt.Sprintf("last scheduled run %s ended %s", last.Date, last.Conclusion)})
		}
	}
	sort.SliceStable(h.Problems, func(i, j int) bool { return h.Problems[i].Workflow < h.Problems[j].Workflow })
	sort.SliceStable(h.Pending, func(i, j int) bool { return h.Pending[i].Workflow < h.Pending[j].Workflow })
	return h, nil
}

// AddedDates sets AddedAt of every workflow to the commit time that first added its file under dir, read with git
// (a function that returns the output of `git <args>`). A history cut short (a shallow checkout) dates every file at
// its oldest commit, so the daily report checks out the whole history; a file git does not know keeps a zero time.
func AddedDates(dir string, workflows []Workflow, git func(args ...string) ([]byte, error)) {
	for i, w := range workflows {
		out, err := git("log", "--diff-filter=A", "--follow", "--format=%cI", "--", filepath.ToSlash(filepath.Join(dir, w.File)))
		if err != nil {
			continue
		}
		lines := strings.Fields(string(out))
		if len(lines) == 0 {
			continue
		}
		if t, err := time.Parse(time.RFC3339, lines[len(lines)-1]); err == nil {
			workflows[i].AddedAt = t.UTC()
		}
	}
}
