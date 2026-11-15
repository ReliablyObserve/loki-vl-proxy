// Command dailyreport writes the daily report's summary from what one daily run produced.
//
//	go run ./test/dailyreport -cases DIR -health FILE -runs FILE -error-rerecord FILE -error-rerecord-exit N \
//	    -error-replay-exit N -parity FILE -smoke FILE -commit SHA -date YYYY-MM-DD -loki-version V [-check]
//
// It reads the registry on the measured commit (case counts by status, open cases by impact), the per-case
// verdicts of test/caseverify (one file per VictoriaLogs version in -cases), the scheduled workflows' health
// (test/ciworkflows -health), the error answers (the recorded figures of error-coverage.json and the
// known-leak list, the live re-record of test/errorrecord -check and the unit replay's exit), the grammar
// sweep's recorded classes with the live re-verification of today's query-corpus run, the parity slice's
// result.json, the perf smoke's smoke.json (bench/ab/smoke.py) and the weekly run's committed figures, and
// writes:
//
//	conformance/registry/generated/verification/summary.json   every number with the date and commit it is
//	                                                            verified on, and why the day is red
//	conformance/registry/generated/verification/cases.json     the per-case verdicts of every version, merged
//	-md FILE                                                    the Markdown body of the rolling pull request
//
// The day is red when a fixed case differs from Loki, an error recording drifted or its replay failed, the
// sweep's live check is red, the parity slice holds a cluster no case claims, the perf smoke has a row above
// 2x Loki or lost a ratchet row, a scheduled workflow is missing or red, or an input is missing. -check exits
// 1 on a red day.
//
// The pull request body (-md) opens with an executive summary: what is ok and not ok per area in numbers, the Loki
// side and the VictoriaLogs side, what moved since the previous report (the summary.json and cases.json that -previous
// and -previous-cases name, the committed ones by default, read before -out and -cases-out replace them) and what is
// wrong now; the rest of the report follows unchanged.
//
// -weekly is the weekly full run (perf-daily.yaml): it appends the week's numbers (the newest daily summary,
// the weekly per-case verdicts and full parity corpus of both VictoriaLogs versions, the full-size grammar
// sweep, the full A/B on the oldest supported VictoriaLogs, the weekly A/B figures, the release dry run) to
// conformance/registry/generated/verification/weekly-history.jsonl and writes weekly-trend.md, every number
// over the last weeks.
//
// -docs renders the dated numbers from summary.json: the block between the verified-numbers markers of
// README.md, docs/KNOWN_ISSUES.md and docs/real-window-compatibility-gaps.md, conformance/reports/
// verification.md and website/src/data/verification.json. -docs -check writes nothing and fails on a stale
// page, and on numbers older than -max-daily-age (daily) or -max-weekly-age (weekly) when it runs on main's
// schedule (GITHUB_EVENT_NAME, GITHUB_REF: the daily report); elsewhere old numbers are a warning. The command is test
// support: it reads and writes files only.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type config struct {
	in                Inputs
	out, casesOut, md string
	check             bool
	weekly            bool
	week              WeeklyInputs
	history, trend    string
	docs              bool
	summary           string
	prevSummary       string
	prevCases         string
	ages              Ages
	now, event, ref   string
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}
	os.Exit(run(cfg, os.Stdout, os.Stderr))
}

func parseFlags(args []string) (config, error) {
	cfg := config{}
	in := &cfg.in
	fs := flag.NewFlagSet("dailyreport", flag.ContinueOnError)
	fs.StringVar(&in.Registry, "registry", "conformance/registry", "the registry root")
	fs.StringVar(&in.CasesDir, "cases", "", "directory of test/caseverify files, one per VictoriaLogs version")
	fs.StringVar(&in.Health, "health", "", "the scheduled workflows' health (test/ciworkflows -health)")
	fs.StringVar(&in.Runs, "runs", "conformance/registry/generated/verification/workflow-runs.json", "the last workflow runs")
	fs.StringVar(&in.SweepWorkflow, "sweep-workflow", "query-corpus.yaml", "the workflow whose run re-verifies the sweep and differentials live")
	fs.StringVar(&in.ErrorCoverage, "error-coverage", "conformance/registry/generated/error-coverage.json", "the error compatibility matrix")
	fs.StringVar(&in.ErrorLeaks, "error-leaks", "conformance/registry/error-layer-known-leaks.json", "the error-layer known-leak list")
	fs.StringVar(&in.ErrorRerecord, "error-rerecord", "", "the output of go run ./test/errorrecord -check")
	fs.IntVar(&in.ErrorRerecordExit, "error-rerecord-exit", 0, "its exit status")
	fs.IntVar(&in.ErrorReplayExit, "error-replay-exit", 0, "the exit status of TestLokiErrorAnswersReplay")
	fs.BoolVar(&in.SkipRerecord, "no-error-rerecord", false, "the run did not re-record the error answers")
	fs.BoolVar(&in.SkipReplay, "no-error-replay", false, "the run did not replay the error answers")
	fs.StringVar(&in.Sweep, "sweep", "conformance/registry/grammar-sweep.json", "the grammar sweep's recorded classes")
	fs.StringVar(&in.Parity, "parity", "", "the parity slice's result.json")
	fs.StringVar(&in.Smoke, "smoke", "", "the perf smoke's smoke.json")
	fs.StringVar(&in.SiteData, "weekly-perf", "website/src/data/quality-vs-loki.json", "the weekly run's committed figures")
	fs.StringVar(&in.Over2x, "over-2x", "bench/ab/history/over-2x-loki.json", "the weekly above-2x list")
	fs.StringVar(&in.Date, "date", time.Now().UTC().Format("2006-01-02"), "the day of the run")
	fs.StringVar(&in.Commit, "commit", os.Getenv("GITHUB_SHA"), "the commit measured")
	fs.StringVar(&in.Run, "run", "", "the workflow run's URL")
	fs.StringVar(&in.LokiVersion, "loki-version", "", "the stack's Loki version")
	fs.StringVar(&cfg.out, "out", "conformance/registry/generated/verification/summary.json", "the summary")
	fs.StringVar(&cfg.casesOut, "cases-out", "conformance/registry/generated/verification/cases.json", "the merged per-case verdicts")
	fs.StringVar(&cfg.md, "md", "", "also write the Markdown report here")
	fs.StringVar(&cfg.prevSummary, "previous", "conformance/registry/generated/verification/summary.json", "the previous report's summary, read before -out replaces it")
	fs.StringVar(&cfg.prevCases, "previous-cases", "conformance/registry/generated/verification/cases.json", "the previous report's per-case verdicts, read before -cases-out replaces it")
	fs.BoolVar(&cfg.check, "check", false, "exit 1 on a red day")
	fs.BoolVar(&cfg.weekly, "weekly", false, "the weekly full run: append the week's numbers and write the trend")
	fs.StringVar(&cfg.week.Summary, "daily-summary", "conformance/registry/generated/verification/summary.json", "weekly: the newest daily summary")
	fs.StringVar(&cfg.week.CasesDir, "weekly-cases", "", "weekly: directory of test/caseverify files")
	fs.StringVar(&cfg.week.ParityDir, "weekly-parity", "", "weekly: directory of parity result-<version>.json files")
	fs.StringVar(&cfg.week.SweepDump, "weekly-sweep", "", "weekly: the full-size sweep's dump")
	fs.StringVar(&cfg.week.FloorPerf, "weekly-floor-perf", "", "weekly: smoke.py judge of the A/B on the oldest supported VictoriaLogs")
	fs.StringVar(&cfg.week.PinPerf, "weekly-pin-perf", "", "weekly: smoke.py judge of the weekly A/B's results on the pin (reuse)")
	fs.StringVar(&cfg.week.FleetDir, "weekly-fleet", "", "weekly: directory of the fleet cache proof reports (rf<N>.json, rf<N>.exit)")
	fs.StringVar(&cfg.week.ReleaseDry, "release-dry-run", "", "weekly: the conclusion of the release.yaml dry run")
	fs.StringVar(&cfg.history, "weekly-history", "conformance/registry/generated/verification/weekly-history.jsonl", "weekly: the history")
	fs.BoolVar(&cfg.docs, "docs", false, "render the dated numbers of summary.json into the pages that state them")
	fs.StringVar(&cfg.summary, "summary", "conformance/registry/generated/verification/summary.json", "docs: the daily summary")
	fs.DurationVar(&cfg.ages.Daily, "max-daily-age", 48*time.Hour, "docs -check: the oldest a daily number may be on main")
	fs.DurationVar(&cfg.ages.Weekly, "max-weekly-age", 8*24*time.Hour, "docs -check: the oldest a weekly number may be on main")
	fs.StringVar(&cfg.now, "now", "", "docs -check: the time to judge ages at (RFC 3339; default now)")
	fs.StringVar(&cfg.event, "event", os.Getenv("GITHUB_EVENT_NAME"), "docs -check: the CI event")
	fs.StringVar(&cfg.ref, "ref", os.Getenv("GITHUB_REF"), "docs -check: the CI ref")
	fs.StringVar(&cfg.trend, "weekly-trend", "conformance/registry/generated/verification/weekly-trend.md", "weekly: the trend page")
	err := fs.Parse(args)
	return cfg, err
}

func run(cfg config, stdout, stderr io.Writer) int {
	if cfg.weekly {
		return runWeekly(cfg, stdout, stderr)
	}
	if cfg.docs {
		return runDocs(cfg, stdout, stderr)
	}
	// The previous report is what the committed files hold now, read before this run writes them. One that cannot
	// be read is a warning: the day is still reported, without the changes.
	prev, perr := LoadPrevious(cfg.prevSummary, cfg.prevCases)
	if perr != nil {
		fmt.Fprintln(stderr, "dailyreport: no comparison with the previous report:", perr)
		prev = nil
	}
	s, merged, err := Build(cfg.in)
	if err == nil {
		err = writeJSON(cfg.out, s)
	}
	if err == nil && merged != nil {
		err = writeJSON(cfg.casesOut, merged)
	}
	if err == nil && cfg.md != "" {
		err = os.WriteFile(cfg.md, []byte(Report(s, prev)), 0o644)
	}
	if err != nil {
		fmt.Fprintln(stderr, "dailyreport:", err)
		return 2
	}
	state := "green"
	if s.Red {
		state = "red"
	}
	fmt.Fprintf(stdout, "daily report %s: %s, %d reason(s)\n", s.Date, state, len(s.Reasons))
	for _, r := range s.Reasons {
		fmt.Fprintln(stdout, "  "+r)
	}
	if cfg.check && s.Red {
		return 1
	}
	return 0
}

func writeJSON(path string, v any) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b.Bytes(), 0o644)
}

func runWeekly(cfg config, stdout, stderr io.Writer) int {
	w := cfg.week
	w.Date, w.Commit, w.SiteData, w.Over2x = cfg.in.Date, cfg.in.Commit, cfg.in.SiteData, cfg.in.Over2x
	line, err := BuildWeekly(w)
	var lines []WeeklyLine
	if err == nil {
		lines, err = ReadWeekly(cfg.history)
	}
	if err == nil {
		var before *WeeklyLine
		for i := range lines {
			if lines[i].Date < line.Date {
				before = &lines[i]
			}
		}
		judgeReuse(&line, before)
		lines = AppendWeekly(lines, line)
		err = WriteWeekly(cfg.history, lines)
	}
	if err == nil {
		err = os.WriteFile(cfg.trend, []byte(RenderTrend(lines)), 0o644)
	}
	if err != nil {
		fmt.Fprintln(stderr, "dailyreport:", err)
		return 2
	}
	fmt.Fprintf(stdout, "weekly report %s: %d numbers, %d weeks of history\n", line.Date, len(line.Numbers), len(lines))
	for _, r := range line.Red {
		fmt.Fprintln(stdout, "red: "+r)
	}
	if cfg.check && len(line.Red) > 0 {
		return 1
	}
	return 0
}

func runDocs(cfg config, stdout, stderr io.Writer) int {
	fail := func(err error) int {
		fmt.Fprintln(stderr, "dailyreport:", err)
		return 2
	}
	s, err := LoadSummary(cfg.summary)
	if err != nil {
		return fail(err)
	}
	pages, err := Pages(s, os.ReadFile)
	if err != nil {
		return fail(err)
	}
	paths := make([]string, 0, len(pages))
	for p := range pages {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	if !cfg.check {
		for _, p := range paths {
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return fail(err)
			}
			if err := os.WriteFile(p, []byte(pages[p]), 0o644); err != nil {
				return fail(err)
			}
		}
		fmt.Fprintf(stdout, "verified numbers: wrote %d pages\n", len(paths))
		return 0
	}
	code := 0
	for _, p := range paths {
		if current, err := os.ReadFile(p); err != nil || string(current) != pages[p] {
			fmt.Fprintf(stdout, "verified numbers: %s is stale; run go run ./test/dailyreport -docs\n", p)
			code = 1
		}
	}
	now := time.Now()
	if cfg.now != "" {
		if now, err = time.Parse(time.RFC3339, cfg.now); err != nil {
			return fail(err)
		}
	}
	if s == nil {
		fmt.Fprintln(stdout, "verified numbers: no daily summary on this branch yet; the pages say so")
	}
	strict := Strict(cfg.event, cfg.ref)
	for _, problem := range Stale(s, now, cfg.ages) {
		if strict {
			fmt.Fprintln(stdout, "verified numbers: "+problem)
			code = 1
		} else {
			fmt.Fprintln(stdout, "verified numbers: warning: "+problem)
		}
	}
	if code == 0 {
		fmt.Fprintf(stdout, "verified numbers: %d pages current\n", len(paths))
	}
	return code
}
