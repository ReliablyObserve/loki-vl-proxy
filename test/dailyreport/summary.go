package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

// Summary is summary.json: every number the daily report re-verifies, with the run it comes from.
type Summary struct {
	GeneratedBy  string         `json:"generated_by"`
	Date         string         `json:"date"`
	Commit       string         `json:"commit"`
	Run          string         `json:"run,omitempty"`
	Loki         string         `json:"loki_version"`
	VictoriaLogs []string       `json:"victorialogs_versions"`
	Red          bool           `json:"red"`
	Reasons      []string       `json:"reasons"`
	Workflows    *WorkflowPart  `json:"workflows,omitempty"`
	Registry     RegistryPart   `json:"registry"`
	Cases        map[string]any `json:"cases"`
	Errors       *ErrorsPart    `json:"errors,omitempty"`
	Sweep        *SweepPart     `json:"sweep,omitempty"`
	Parity       map[string]any `json:"parity,omitempty"`
	PerfDaily    map[string]any `json:"perf_daily,omitempty"`
	PerfWeekly   *WeeklyPerf    `json:"perf_weekly,omitempty"`
	Missing      []string       `json:"missing,omitempty"`
}

// RegistryPart is the registry's case counts on the measured commit.
type RegistryPart struct {
	Cases        int            `json:"cases"`
	ByStatus     map[string]int `json:"by_status"`
	OpenByImpact map[string]int `json:"open_by_impact"`
}

// WorkflowPart is the health of the scheduled workflows (test/ciworkflows -health).
type WorkflowPart struct {
	CheckedAt string           `json:"checked_at"`
	Scheduled int              `json:"scheduled"`
	Problems  []map[string]any `json:"problems"`
}

// ErrorsPart is the error answers: the recorded figures, the live re-record against the stack's Loki and the
// unit replay against the recordings.
type ErrorsPart struct {
	Recorded struct {
		ComparableRows   int `json:"comparable_rows"`
		MatchingRows     int `json:"matching_rows"`
		KnownDifferences int `json:"known_differences"`
		KnownLeaks       int `json:"known_leaks"`
	} `json:"recorded"`
	// Live is nil when the run did not re-record, ReplayOK when it did not replay.
	Live     *LiveErrors `json:"live"`
	ReplayOK *bool       `json:"replay_ok"`
}

// LiveErrors is the re-record of the error answers against the stack's Loki.
type LiveErrors struct {
	Rows    int  `json:"rows"`
	Changed int  `json:"changed"`
	Kept    int  `json:"not_re_recorded"`
	OK      bool `json:"ok"`
}

// SweepPart is the grammar sweep's recorded classes, with the live re-verification of today's query-corpus run.
type SweepPart struct {
	Queries      int            `json:"queries"`
	LokiAnswers  map[string]int `json:"loki_answers"`
	Differences  map[string]int `json:"differences_by_kind"`
	LiveCheck    map[string]any `json:"live_check,omitempty"`
	RecordedFile string         `json:"recorded_file"`
}

// WeeklyPerf is the weekly full run's figures as committed (their own date: the weekly run measures them).
type WeeklyPerf struct {
	Date         string           `json:"date"`
	Loki         string           `json:"loki_version"`
	Pairs        int              `json:"pairs"`
	Faster       int              `json:"faster"`
	Par          int              `json:"par"`
	Slower       int              `json:"slower"`
	Distribution []map[string]any `json:"distribution"`
	Over2xRows   int              `json:"over_2x_rows"`
}

// Inputs names the files one daily run produced; an empty name is an input the run did not produce.
type Inputs struct {
	Registry, CasesDir, Health, Runs         string
	ErrorCoverage, ErrorLeaks, ErrorRerecord string
	ErrorRerecordExit, ErrorReplayExit       int
	Sweep, Parity, Smoke, SiteData, Over2x   string
	Date, Commit, Run, LokiVersion           string
	SweepWorkflow                            string
	SkipRerecord, SkipReplay                 bool
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// optional reads a file the run may not have produced: a missing file is listed and is not an error.
func (s *Summary) optional(label, path string, v any) (bool, error) {
	if path == "" {
		s.Missing = append(s.Missing, label)
		return false, nil
	}
	err := readJSON(path, v)
	if errors.Is(err, os.ErrNotExist) {
		s.Missing = append(s.Missing, label)
		return false, nil
	}
	return err == nil, err
}

func (s *Summary) red(format string, args ...any) {
	s.Red = true
	s.Reasons = append(s.Reasons, fmt.Sprintf(format, args...))
}

// CaseRun is the part of a test/caseverify file the report reads.
type CaseRun struct {
	Date         string                     `json:"date"`
	Commit       string                     `json:"commit"`
	Mode         string                     `json:"mode"`
	Loki         string                     `json:"loki_version"`
	VictoriaLogs string                     `json:"victorialogs_version"`
	ProxyFlags   string                     `json:"proxy_flags"`
	WindowEnd    string                     `json:"window_end"`
	Counts       map[string]int             `json:"counts"`
	Flags        map[string][]string        `json:"flags"`
	ProxyBuild   map[string]any             `json:"proxy_build"`
	Reuse        *CaseReuse                 `json:"reuse"`
	Cases        map[string]json.RawMessage `json:"cases"`
}

// CaseReuse is the cache reuse of a run's repeats (test/caseverify).
type CaseReuse struct {
	Counts      map[string]int     `json:"counts"`
	NotReused   []string           `json:"not_reused"`
	ColdVLCalls float64            `json:"cold_vl_calls"`
	RepeatCalls float64            `json:"repeat_vl_calls"`
	RepeatHits  map[string]float64 `json:"repeat_hit_ratio"`
}

// Build reads every input and judges the day.
func Build(in Inputs) (Summary, map[string]any, error) {
	s := Summary{GeneratedBy: "go run ./test/dailyreport", Date: in.Date, Commit: in.Commit, Run: in.Run, Loki: in.LokiVersion,
		Reasons: []string{}, Cases: map[string]any{}, VictoriaLogs: []string{}}
	if err := s.registry(in.Registry); err != nil {
		return s, nil, err
	}
	merged, err := s.cases(in.CasesDir)
	if err != nil {
		return s, nil, err
	}
	steps := []func(Inputs) error{s.workflows, s.errors, s.sweep, s.parity, s.perfDaily, s.perfWeekly}
	for _, step := range steps {
		if err := step(in); err != nil {
			return s, nil, err
		}
	}
	for _, m := range s.Missing {
		s.red("%s: the run produced no result, so it is not verified today", m)
	}
	sort.Strings(s.Missing)
	return s, merged, nil
}

func (s *Summary) registry(root string) error {
	cases, problems, err := registry.LoadAll(root)
	if err != nil {
		return err
	}
	if len(problems) > 0 {
		return fmt.Errorf("the registry does not load strictly (%d problems)", len(problems))
	}
	s.Registry = RegistryPart{Cases: len(cases), ByStatus: map[string]int{}, OpenByImpact: map[string]int{}}
	for _, c := range cases {
		gap, _ := c.Doc["gap"].(map[string]any)
		if gap == nil {
			s.Registry.ByStatus["no gap"]++
			continue
		}
		status, _ := gap["status"].(string)
		s.Registry.ByStatus[status]++
		if status == "open" {
			impact, _ := gap["impact"].(string)
			s.Registry.OpenByImpact[impact]++
		}
	}
	return nil
}

// clipped bounds a measured value in the committed cases.json; the full answers are in the run's artifact.
const clipped = 300

// cases reads every caseverify file of dir (one per VictoriaLogs version) and merges them: one entry per case
// with each version's verdict.
func (s *Summary) cases(dir string) (map[string]any, error) {
	var files []string
	if dir != "" {
		found, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil {
			return nil, err
		}
		files = found
	}
	sort.Strings(files)
	if len(files) == 0 {
		s.Missing = append(s.Missing, "per-case verdicts")
		return nil, nil
	}
	runs := map[string]any{}
	byCase := map[string]map[string]any{}
	for _, path := range files {
		var run CaseRun
		if err := readJSON(path, &run); err != nil {
			return nil, err
		}
		version := run.VictoriaLogs
		if _, dup := runs[version]; dup {
			return nil, fmt.Errorf("%s: a second run for VictoriaLogs %q; each version's verdicts are one file", path, version)
		}
		s.VictoriaLogs = append(s.VictoriaLogs, version)
		if s.Loki == "" {
			s.Loki = run.Loki
		}
		runs[version] = map[string]any{"date": run.Date, "commit": run.Commit, "mode": run.Mode, "loki_version": run.Loki,
			"proxy_flags": run.ProxyFlags, "window_end": run.WindowEnd, "counts": run.Counts, "flags": run.Flags,
			"proxy_build": run.ProxyBuild, "reuse": run.Reuse}
		s.Cases[version] = map[string]any{"counts": run.Counts, "regressions": len(run.Flags["regression"]),
			"proposed_close": len(run.Flags["proposed_close"]), "behaviour_changed": len(run.Flags["behaviour_changed"])}
		if run.Reuse != nil {
			s.Cases[version].(map[string]any)["reuse"] = run.Reuse
		}
		if stale, _ := run.ProxyBuild["stale"].(bool); stale {
			s.red("the VictoriaLogs %s run measured a proxy built from %v, not the commit", version, run.ProxyBuild["revision"])
		}
		for _, id := range run.Flags["regression"] {
			s.red("%s differs from Loki on VictoriaLogs %s, and the registry holds it fixed", id, version)
		}
		for id, raw := range run.Cases {
			var r map[string]any
			if err := json.Unmarshal(raw, &r); err != nil {
				return nil, fmt.Errorf("%s: %s: %w", path, id, err)
			}
			clipDifferences(r)
			entry := byCase[id]
			if entry == nil {
				entry = map[string]any{"expected": r["expected"], "gap_status": r["gap_status"]}
				byCase[id] = entry
			}
			delete(r, "expected")
			delete(r, "gap_status")
			entry[version] = r
		}
	}
	return map[string]any{"generated_by": "go run ./test/dailyreport (test/caseverify runs)", "date": s.Date, "commit": s.Commit,
		"runs": runs, "cases": byCase}, nil
}

func clipDifferences(r map[string]any) {
	list, _ := r["differences"].([]any)
	for _, d := range list {
		m, _ := d.(map[string]any)
		for _, side := range []string{"loki", "proxy"} {
			if v, ok := m[side].(string); ok && len(v) > clipped {
				m[side] = v[:clipped] + "..."
			}
		}
	}
}

func (s *Summary) workflows(in Inputs) error {
	var w WorkflowPart
	ok, err := s.optional("workflow health", in.Health, &w)
	if !ok {
		return err
	}
	s.Workflows = &w
	for _, p := range w.Problems {
		s.red("scheduled workflow %v: %v", p["workflow"], p["problem"])
	}
	return nil
}

// rerecordLine is test/errorrecord's per-file line: "<path>: N rows, M answers changed, K not re-recorded".
var rerecordLine = regexp.MustCompile(`(?m)^\S+: (\d+) rows, (\d+) answers changed, (\d+) not re-recorded`)

func (s *Summary) errors(in Inputs) error {
	var cov struct {
		Summary struct {
			ComparableRows int `json:"comparable_rows"`
			MatchingRows   int `json:"matching_rows"`
		} `json:"summary"`
		KnownDifferences struct {
			Rows int `json:"rows"`
		} `json:"known_differences"`
	}
	ok, err := s.optional("error coverage", in.ErrorCoverage, &cov)
	if !ok {
		return err
	}
	e := &ErrorsPart{}
	e.Recorded.ComparableRows, e.Recorded.MatchingRows = cov.Summary.ComparableRows, cov.Summary.MatchingRows
	e.Recorded.KnownDifferences = cov.KnownDifferences.Rows
	var leaks struct {
		Leaks map[string][]string `json:"leaks"`
	}
	if ok, err := s.optional("error-layer known leaks", in.ErrorLeaks, &leaks); err != nil {
		return err
	} else if ok {
		for _, ids := range leaks.Leaks {
			e.Recorded.KnownLeaks += len(ids)
		}
	}
	s.Errors = e
	if !in.SkipRerecord {
		text, err := os.ReadFile(in.ErrorRerecord)
		switch {
		case in.ErrorRerecord == "" || errors.Is(err, os.ErrNotExist):
			s.Missing = append(s.Missing, "error answers re-record")
		case err != nil:
			return err
		default:
			e.Live = &LiveErrors{}
			matches := rerecordLine.FindAllStringSubmatch(string(text), -1)
			for _, m := range matches {
				rows, _ := strconv.Atoi(m[1])
				changed, _ := strconv.Atoi(m[2])
				kept, _ := strconv.Atoi(m[3])
				e.Live.Rows += rows
				e.Live.Changed += changed
				e.Live.Kept += kept
			}
			e.Live.OK = len(matches) > 0 && in.ErrorRerecordExit == 0 && e.Live.Changed == 0
			if !e.Live.OK {
				s.red("error answers: the stack's Loki answers %d recorded row(s) differently (re-record exit %d)", e.Live.Changed, in.ErrorRerecordExit)
			}
		}
	}
	if !in.SkipReplay {
		passed := in.ErrorReplayExit == 0
		e.ReplayOK = &passed
		if !passed {
			s.red("error answers: the unit replay against the recorded Loki answers failed (exit %d)", in.ErrorReplayExit)
		}
	}
	return nil
}

func (s *Summary) sweep(in Inputs) error {
	var g struct {
		Queries     int            `json:"queries"`
		LokiAnswers map[string]int `json:"loki_answers"`
		Differences map[string]int `json:"differences_by_kind"`
	}
	ok, err := s.optional("grammar sweep", in.Sweep, &g)
	if !ok {
		return err
	}
	s.Sweep = &SweepPart{Queries: g.Queries, LokiAnswers: g.LokiAnswers, Differences: g.Differences, RecordedFile: in.Sweep}
	// The live re-verification of the sweep and the differentials is query-corpus's own run of the day.
	var runs struct {
		Workflows map[string]struct {
			Main      map[string]any `json:"main"`
			Scheduled map[string]any `json:"scheduled"`
		} `json:"workflows"`
	}
	if ok, err := s.optional("workflow runs", in.Runs, &runs); err != nil || !ok {
		return err
	}
	last := runs.Workflows[in.SweepWorkflow].Main
	if last == nil {
		s.red("%s: no run on main recorded, so the sweep and differential numbers are not re-verified", in.SweepWorkflow)
		return nil
	}
	s.Sweep.LiveCheck = map[string]any{"workflow": in.SweepWorkflow, "conclusion": last["conclusion"], "date": last["date"], "url": last["url"]}
	if last["conclusion"] != "success" {
		s.red("%s: the live re-verification of the sweep and the differentials ended %v on %v", in.SweepWorkflow, last["conclusion"], last["date"])
	}
	return nil
}

func (s *Summary) parity(in Inputs) error {
	var r map[string]any
	ok, err := s.optional("parity slice", in.Parity, &r)
	if !ok {
		return err
	}
	s.Parity = map[string]any{}
	for _, k := range []string{"mode", "exit", "clusters", "requests", "unclaimed", "problems", "conformance_ok", "not_reproducible", "error", "loki", "ref"} {
		if v, found := r[k]; found {
			s.Parity[k] = v
		}
	}
	if exit, _ := r["exit"].(float64); exit != 0 {
		s.red("parity slice: the differential run could not speak for its slice (exit %v)", r["exit"])
	} else if unclaimed, _ := r["unclaimed"].(float64); unclaimed > 0 {
		s.red("parity slice: %v difference cluster(s) are claimed by no registry case", r["unclaimed"])
	}
	return nil
}

func (s *Summary) perfDaily(in Inputs) error {
	var v map[string]any
	ok, err := s.optional("perf smoke", in.Smoke, &v)
	if !ok {
		return err
	}
	s.PerfDaily = map[string]any{}
	for _, k := range []string{"date", "commit", "rows", "comparable", "loki_failed", "distribution", "over_2x", "ratchet_lost", "ratchet_problem",
		"reuse", "reuse_lost"} {
		if x, found := v[k]; found {
			s.PerfDaily[k] = x
		}
	}
	if list, _ := v["over_2x"].([]any); len(list) > 0 {
		s.red("perf smoke: %d row(s) above 2x Loki", len(list))
	}
	if list, _ := v["ratchet_lost"].([]any); len(list) > 0 {
		s.red("perf smoke: %d row(s) of the weekly ratchet lost", len(list))
	}
	if list, _ := v["reuse_lost"].([]any); len(list) > 0 {
		s.red("perf smoke: %d row(s) reuse the proxy's cache less than the day before", len(list))
	}
	return nil
}

func (s *Summary) perfWeekly(in Inputs) error {
	var site struct {
		Date    string `json:"date"`
		Loki    string `json:"loki"`
		ByRange []struct {
			Pairs, Faster, Par, Slower int
		} `json:"by_range"`
		Distribution []map[string]any `json:"distribution"`
	}
	ok, err := s.optional("weekly perf figures", in.SiteData, &site)
	if !ok {
		return err
	}
	w := &WeeklyPerf{Date: site.Date, Loki: site.Loki, Distribution: site.Distribution}
	for _, r := range site.ByRange {
		w.Pairs += r.Pairs
		w.Faster += r.Faster
		w.Par += r.Par
		w.Slower += r.Slower
	}
	var over struct {
		Rows []any `json:"rows"`
	}
	if ok, err := s.optional("above-2x list", in.Over2x, &over); err != nil {
		return err
	} else if ok {
		w.Over2xRows = len(over.Rows)
	}
	s.PerfWeekly = w
	return nil
}

// Markdown is the daily report's pull request body: what is red first, then every number.
func Markdown(s Summary) string {
	var b strings.Builder
	state := "green"
	if s.Red {
		state = "red"
	}
	fmt.Fprintf(&b, "## Daily report %s: %s\n\n", s.Date, state)
	fmt.Fprintf(&b, "Main `%s`, Loki %s, VictoriaLogs %s.\n\n", short(s.Commit), s.Loki, strings.Join(s.VictoriaLogs, " and "))
	if len(s.Reasons) > 0 {
		b.WriteString("### Red\n\n")
		for i, r := range s.Reasons {
			if i == 60 {
				fmt.Fprintf(&b, "- ... %d more in summary.json\n", len(s.Reasons)-60)
				break
			}
			fmt.Fprintf(&b, "- %s\n", r)
		}
		b.WriteString("\n")
	}
	b.WriteString("### Registry\n\n| Status | Cases |\n|---|---|\n")
	for _, k := range sortedKeys(s.Registry.ByStatus) {
		fmt.Fprintf(&b, "| %s | %d |\n", k, s.Registry.ByStatus[k])
	}
	b.WriteString("\nOpen by impact: ")
	var parts []string
	for _, k := range sortedKeys(s.Registry.OpenByImpact) {
		parts = append(parts, fmt.Sprintf("%s %d", k, s.Registry.OpenByImpact[k]))
	}
	b.WriteString(strings.Join(parts, ", ") + ".\n\n")
	b.WriteString("### Per-case verdicts against Loki\n\nOnly differs is a difference of the proxy (its data was proven equal on both sides); " +
		"data differs, empty, Loki not ready, not comparable and unchecked prove nothing about it.\n\n" +
		"| VictoriaLogs | Matches | Differs | Data differs | Empty | Loki not ready | Not comparable | Unchecked | Unstable | Both fail | Blocked | Not executable | Regressions | Proposed closes | Behaviour changed |\n" +
		"|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, v := range s.VictoriaLogs {
		fmt.Fprintf(&b, "| %s |", v)
		for _, k := range []string{"matches", "differs", "data_differs", "empty", "loki_not_ready", "not_comparable", "data_unchecked",
			"unstable", "both_fail", "blocked", "not_executable", "regressions", "proposed_close", "behaviour_changed"} {
			fmt.Fprintf(&b, " %d |", caseCount(s, v, k))
		}
		b.WriteString("\n")
	}
	b.WriteString("\n### Cache reuse\n\n")
	for _, v := range s.VictoriaLogs {
		c, _ := s.Cases[v].(map[string]any)
		if r, ok := c["reuse"].(*CaseReuse); ok && r != nil {
			fmt.Fprintf(&b, "- Registry cases, VictoriaLogs %s: %d repeats reused, %d not reused, %d not measured; VictoriaLogs calls %g cold, %g on the repeats.\n",
				v, r.Counts["reused"], r.Counts["cache_not_reused"], r.Counts["not_measured"], r.ColdVLCalls, r.RepeatCalls)
		}
	}
	if p := s.PerfDaily; p != nil {
		if r, ok := p["reuse"].(map[string]any); ok {
			fmt.Fprintf(&b, "- Perf smoke: %v rows measured, VictoriaLogs calls %v cold and %v on the repeats, %v rows repeat without calling VictoriaLogs, %d rows lost reuse.\n",
				r["rows"], r["cold_vl_calls"], r["repeat_vl_calls"], r["repeat_without_vl_calls"], lenOf(p["reuse_lost"]))
		}
	}
	if e := s.Errors; e != nil {
		live, replay := "not run", "not run"
		if e.Live != nil {
			live = fmt.Sprintf("%d rows asked, %d changed", e.Live.Rows, e.Live.Changed)
		}
		if e.ReplayOK != nil {
			replay = okText(*e.ReplayOK)
		}
		fmt.Fprintf(&b, "\n### Error answers\n\n%d of %d recorded Loki answers match, %d known differences, %d known leaks. "+
			"Live re-record: %s. Unit replay: %s.\n", e.Recorded.MatchingRows, e.Recorded.ComparableRows,
			e.Recorded.KnownDifferences, e.Recorded.KnownLeaks, live, replay)
	}
	if sw := s.Sweep; sw != nil {
		fmt.Fprintf(&b, "\n### Grammar sweep\n\n%d queries; Loki accepts %d and rejects %d.\n\n| Difference | Queries |\n|---|---|\n",
			sw.Queries, sw.LokiAnswers["accepted"], sw.LokiAnswers["rejected"])
		for _, k := range sortedKeys(sw.Differences) {
			fmt.Fprintf(&b, "| %s | %d |\n", k, sw.Differences[k])
		}
		if sw.LiveCheck != nil {
			fmt.Fprintf(&b, "\nLive re-verification: `%v` %v on %v.\n", sw.LiveCheck["workflow"], sw.LiveCheck["conclusion"], sw.LiveCheck["date"])
		}
	}
	if p := s.Parity; p != nil {
		fmt.Fprintf(&b, "\n### Parity slice\n\n%v clusters over %v requests, %v claimed by no case (exit %v).\n", p["clusters"], p["requests"], p["unclaimed"], p["exit"])
	}
	if p := s.PerfDaily; p != nil {
		fmt.Fprintf(&b, "\n### Daily perf smoke\n\n%v rows, %v comparable timings, %d above 2x Loki, %d ratchet rows lost.\n", p["rows"], p["comparable"],
			lenOf(p["over_2x"]), lenOf(p["ratchet_lost"]))
	}
	if w := s.PerfWeekly; w != nil {
		fmt.Fprintf(&b, "\n### Weekly performance (measured %s, Loki %s)\n\n%d comparable shape x range pairs: %d faster, %d on par, %d slower; %d rows on the above-2x list.\n",
			w.Date, w.Loki, w.Pairs, w.Faster, w.Par, w.Slower, w.Over2xRows)
	}
	if w := s.Workflows; w != nil {
		fmt.Fprintf(&b, "\n### Scheduled workflows\n\n%d scheduled workflows checked at %s, %d problem(s).\n", w.Scheduled, w.CheckedAt, len(w.Problems))
	}
	return b.String()
}

func okText(ok bool) string {
	if ok {
		return "passes"
	}
	return "fails"
}

func lenOf(v any) int {
	list, _ := v.([]any)
	return len(list)
}

func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// caseCount reads one count of a version's per-case verdicts, built in this run or read back from summary.json:
// a verdict count (matches, differs, ...) or a flag count (regressions, proposed_close, behaviour_changed).
func caseCount(s Summary, version, key string) int {
	c, _ := s.Cases[version].(map[string]any)
	if v, ok := c[key]; ok {
		return toInt(v)
	}
	switch counts := c["counts"].(type) {
	case map[string]int:
		return counts[key]
	case map[string]any:
		return toInt(counts[key])
	}
	return 0
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case float64:
		return int(x)
	}
	return 0
}
