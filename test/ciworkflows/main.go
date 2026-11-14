// Command ciworkflows keeps docs/ci-workflows.md, the inventory of the CI workflows, true to
// .github/workflows.
//
//	go run ./test/ciworkflows          write docs/ci-workflows.md
//	go run ./test/ciworkflows -check   write nothing; exit 1 when a workflow has no inventory row (or a row
//	                                   names no workflow), a row breaks its group, or the page is stale
//	go run ./test/ciworkflows -ingest DIR [-health FILE] [-skip a.yaml,b.yaml]
//	                                   the daily report: read the GitHub API answers saved in DIR into the
//	                                   runs record, write the page, and write the health of the scheduled
//	                                   workflows (missing, late or red last scheduled run) to FILE
//
// The triggers and schedules come from the workflow files. What each workflow verifies, its group and
// whether its check is required come from test/ciworkflows/inventory.json, written by hand. The last
// runs come from conformance/registry/generated/verification/workflow-runs.json, which only the daily
// report writes; a pull request never changes those columns.
//
// The command is test support: it reads and writes files under the repository and nothing else.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type config struct {
	workflows, meta, runs, out string
	ingest, health, skip       string
	check                      bool
	now                        func() time.Time
}

func main() {
	var cfg config
	flag.StringVar(&cfg.workflows, "workflows", ".github/workflows", "the workflow directory")
	flag.StringVar(&cfg.meta, "meta", "test/ciworkflows/inventory.json", "the hand-written inventory rows")
	flag.StringVar(&cfg.runs, "runs", "conformance/registry/generated/verification/workflow-runs.json", "the last runs, written by the daily report")
	flag.StringVar(&cfg.out, "out", "docs/ci-workflows.md", "the inventory page")
	flag.BoolVar(&cfg.check, "check", false, "write nothing; fail on a missing row or a stale page")
	flag.StringVar(&cfg.ingest, "ingest", "", "daily report: directory of saved GitHub API answers (<file>.main.json, <file>.schedule.json)")
	flag.StringVar(&cfg.health, "health", "", "daily report: write the scheduled workflows' health here")
	flag.StringVar(&cfg.skip, "skip", "", "daily report: comma-separated workflows the health check leaves out (the report itself)")
	flag.Parse()
	cfg.now = time.Now
	os.Exit(run(cfg, os.Stdout, os.Stderr))
}

func run(cfg config, stdout, stderr io.Writer) int {
	workflows, err := ListWorkflows(cfg.workflows)
	if err != nil {
		fmt.Fprintln(stderr, "ciworkflows:", err)
		return 2
	}
	meta, err := LoadMeta(cfg.meta)
	if err != nil {
		fmt.Fprintln(stderr, "ciworkflows:", err)
		return 2
	}
	runs, err := LoadRuns(cfg.runs)
	if err != nil {
		fmt.Fprintln(stderr, "ciworkflows:", err)
		return 2
	}
	if cfg.ingest != "" {
		if runs, err = Ingest(cfg.ingest, workflows, cfg.now()); err == nil {
			err = writeJSON(cfg.runs, runs)
		}
		if err != nil {
			fmt.Fprintln(stderr, "ciworkflows:", err)
			return 2
		}
	}
	if cfg.health != "" {
		skip := map[string]bool{}
		for _, f := range strings.Split(cfg.skip, ",") {
			skip[strings.TrimSpace(f)] = true
		}
		h, err := CheckHealth(workflows, runs, skip)
		if err == nil {
			err = writeJSON(cfg.health, h)
		}
		if err != nil {
			fmt.Fprintln(stderr, "ciworkflows:", err)
			return 2
		}
		fmt.Fprintf(stdout, "ci workflows: %d scheduled, %d health problems\n", h.Scheduled, len(h.Problems))
	}
	problems := Problems(workflows, meta)
	for _, p := range problems {
		fmt.Fprintln(stderr, p)
	}
	page := Render(workflows, meta, runs)
	if cfg.check {
		current, err := os.ReadFile(cfg.out)
		if err != nil || string(current) != page {
			problems = append(problems, cfg.out+" is stale")
			fmt.Fprintf(stderr, "%s is stale; run go run ./test/ciworkflows\n", cfg.out)
		}
		if len(problems) > 0 {
			return 1
		}
		fmt.Fprintf(stdout, "ci workflows: %d workflows, every one has an inventory row and %s is current\n", len(workflows), cfg.out)
		return 0
	}
	if len(problems) > 0 {
		return 1
	}
	if err := os.WriteFile(cfg.out, []byte(page), 0o644); err != nil {
		fmt.Fprintln(stderr, "ciworkflows:", err)
		return 2
	}
	fmt.Fprintf(stdout, "ci workflows: wrote %s (%d workflows)\n", cfg.out, len(workflows))
	return 0
}

func writeJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}
