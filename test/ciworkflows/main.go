// Command ciworkflows keeps docs/ci-workflows.md, the inventory of the CI workflows, true to
// .github/workflows.
//
//	go run ./test/ciworkflows          write docs/ci-workflows.md
//	go run ./test/ciworkflows -check   write nothing; exit 1 when a workflow has no inventory row (or a row
//	                                   names no workflow), a row breaks its group, or the page is stale
//
// The triggers and schedules come from the workflow files. What each workflow verifies, its group and
// whether its check is required come from test/ciworkflows/inventory.json, written by hand. The last
// runs come from conformance/registry/generated/verification/workflow-runs.json, which only the daily
// report writes; a pull request never changes those columns.
//
// The command is test support: it reads and writes files under the repository and nothing else.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

type config struct {
	workflows, meta, runs, out string
	check                      bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.workflows, "workflows", ".github/workflows", "the workflow directory")
	flag.StringVar(&cfg.meta, "meta", "test/ciworkflows/inventory.json", "the hand-written inventory rows")
	flag.StringVar(&cfg.runs, "runs", "conformance/registry/generated/verification/workflow-runs.json", "the last runs, written by the daily report")
	flag.StringVar(&cfg.out, "out", "docs/ci-workflows.md", "the inventory page")
	flag.BoolVar(&cfg.check, "check", false, "write nothing; fail on a missing row or a stale page")
	flag.Parse()
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
