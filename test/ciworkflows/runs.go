package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
}

// CheckHealth lists every scheduled workflow (but the ones in skip) whose last scheduled run is missing,
// older than its shortest period plus a day, or did not succeed.
func CheckHealth(workflows []Workflow, runs RunsFile, skip map[string]bool) (Health, error) {
	h := Health{CheckedAt: runs.Fetched, Problems: []HealthProblem{}}
	now, err := time.Parse(time.RFC3339, runs.Fetched)
	if err != nil {
		return h, fmt.Errorf("the runs record has no fetch time: %w", err)
	}
	for _, w := range workflows {
		if len(w.Schedules) == 0 || skip[w.File] {
			continue
		}
		h.Scheduled++
		period := w.Schedules[0].Period
		for _, s := range w.Schedules[1:] {
			period = min(period, s.Period)
		}
		last := runs.Workflows[w.File].Scheduled
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
	return h, nil
}
