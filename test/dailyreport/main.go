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
// 1 on a red day. The command is test support: it reads and writes files only.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type config struct {
	in                Inputs
	out, casesOut, md string
	check             bool
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
	fs.BoolVar(&cfg.check, "check", false, "exit 1 on a red day")
	err := fs.Parse(args)
	return cfg, err
}

func run(cfg config, stdout, stderr io.Writer) int {
	s, merged, err := Build(cfg.in)
	if err == nil {
		err = writeJSON(cfg.out, s)
	}
	if err == nil && merged != nil {
		err = writeJSON(cfg.casesOut, merged)
	}
	if err == nil && cfg.md != "" {
		err = os.WriteFile(cfg.md, []byte(Markdown(s)), 0o644)
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
