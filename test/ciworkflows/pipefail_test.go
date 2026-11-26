package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Without pipefail a pipeline exits with the status of its last command, so a step that pipes a test run into
// tee or tail stays green when the tests fail. GitHub runs `shell: bash` as `bash --noprofile --norc -eo
// pipefail {0}`; a step with no shell gets `bash -e {0}`, and a custom shell string gets no pipefail either.
// memory-leak-tests hid two failing tests this way from the day it was added. Only tee and tail are checked:
// they read all of their input, while a consumer that stops early (head, grep -q) makes the writer exit
// 141 under pipefail, so those pipelines keep the default shell.

// maskedPipeAllowlist names the steps that still hide failures behind such a pipe, keyed
// "<workflow>/<job>/<step>": each one fails on main today, so adding pipefail would turn it red until its
// failures are fixed. Remove an entry together with that fix.
var maskedPipeAllowlist = map[string]string{
	"ci.yaml/bench/Run benchmarks": "main's full benchmark run fails BenchmarkBareParserRangeMetric " +
		"(expected one series, got 0) and BenchmarkSuccessPathFlights (max entries limit 10001 > 10000)",
	"e2e-pipeline.yml/e2e-pipeline/Run pipeline e2e tests": "TestQueryRange_LogfmtUnwrap answers 400 " +
		"(maximum number of series (1000) reached) on main",
}

type pipeStep struct {
	Name  string `yaml:"name"`
	Run   string `yaml:"run"`
	Shell string `yaml:"shell"`
}

type runDefaults struct {
	Run struct {
		Shell string `yaml:"shell"`
	} `yaml:"run"`
}

type pipeWorkflow struct {
	Defaults runDefaults `yaml:"defaults"`
	Jobs     map[string]struct {
		Defaults runDefaults `yaml:"defaults"`
		Steps    []pipeStep  `yaml:"steps"`
	} `yaml:"jobs"`
}

var (
	setPipefail   = regexp.MustCompile(`(?m)^\s*set\s+(-\S+\s+)*-[a-zA-Z]*o\s+pipefail\b`)
	quotedText    = regexp.MustCompile(`'[^']*'|"(\\.|[^"\\])*"`)
	commandBreaks = regexp.MustCompile(`\|\||&&|;`)
)

// runsWithPipefail reports whether a step's script runs with pipefail set before its first line.
func runsWithPipefail(shell, script string) bool {
	return strings.TrimSpace(shell) == "bash" || strings.Contains(shell, "pipefail") || setPipefail.MatchString(script)
}

// maskedPipes returns the script lines whose pipeline ends in tee or tail.
func maskedPipes(script string) []string {
	var out []string
	for _, line := range strings.Split(strings.ReplaceAll(script, "\\\n", " "), "\n") {
		code := strings.TrimSpace(line)
		if strings.HasPrefix(code, "#") {
			continue
		}
		code = quotedText.ReplaceAllString(code, "''")
		for _, command := range commandBreaks.Split(code, -1) {
			stages := strings.Split(command, "|")
			if len(stages) < 2 {
				continue
			}
			if last := strings.Fields(stages[len(stages)-1]); len(last) > 0 && (last[0] == "tee" || last[0] == "tail") {
				out = append(out, strings.TrimSpace(line))
				break
			}
		}
	}
	return out
}

// maskedPipeSteps returns "<workflow>/<job>/<step>" for every step of the workflows in dir that pipes into tee
// or tail without pipefail.
func maskedPipeSteps(t *testing.T, dir string) map[string][]string {
	t.Helper()
	var files []string
	for _, pattern := range []string{"*.yaml", "*.yml"} {
		found, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, found...)
	}
	if len(files) == 0 {
		t.Fatalf("no workflows in %s", dir)
	}
	out := map[string][]string{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var w pipeWorkflow
		if err := yaml.Unmarshal(raw, &w); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for jobName, job := range w.Jobs {
			for _, step := range job.Steps {
				if step.Run == "" {
					continue
				}
				shell := step.Shell
				if shell == "" {
					shell = job.Defaults.Run.Shell
				}
				if shell == "" {
					shell = w.Defaults.Run.Shell
				}
				if runsWithPipefail(shell, step.Run) {
					continue
				}
				if lines := maskedPipes(step.Run); len(lines) > 0 {
					out[filepath.Base(path)+"/"+jobName+"/"+step.Name] = lines
				}
			}
		}
	}
	return out
}

func TestMaskedPipeDetection(t *testing.T) {
	for script, want := range map[string]bool{
		"go test ./... | tee out.txt":                         true,
		"go test ./internal/proxy/ -v \\\n  | tee out.txt\n":  true,
		"go test ./x -v | tail -n 40":                         true,
		"go test ./x | tee -a \"$GITHUB_STEP_SUMMARY\" && ls": true,
		"x=$(go tool cover -func=c.out | tail -1)":            true,
		"go test ./x > out.txt":                               false,
		"grep Benchmark out.txt | head -10":                   false,
		"echo \"$FILES\" | grep -q cmd/":                      false,
		"make test || tail -n 50 log.txt":                     false,
		"echo 'a | tee b'":                                    false,
		"# go test | tee out.txt":                             false,
		"go run ./x -check | tee r.txt || status=$?":          true,
	} {
		if got := len(maskedPipes(script)) > 0; got != want {
			t.Errorf("maskedPipes(%q) = %v, want %v", script, got, want)
		}
	}
	for _, c := range []struct {
		shell, script string
		want          bool
	}{
		{"bash", "go test | tee x", true},
		{"", "go test | tee x", false},
		{"bash -e {0}", "go test | tee x", false},
		{"bash -eo pipefail {0}", "go test | tee x", true},
		{"", "set -euo pipefail\ngo test | tee x", true},
		{"", "set -o pipefail\ngo test | tee x", true},
		{"", "echo set -o pipefail\ngo test | tee x", false},
	} {
		if got := runsWithPipefail(c.shell, c.script); got != c.want {
			t.Errorf("runsWithPipefail(%q, %q) = %v, want %v", c.shell, c.script, got, c.want)
		}
	}

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.yaml"), `jobs:
  plain:
    steps:
      - name: masked
        run: go test ./... | tee out.txt
      - name: checked
        shell: bash
        run: go test ./... | tee out.txt
  defaulted:
    defaults:
      run:
        shell: bash
    steps:
      - name: inherits bash
        run: go test ./... | tail -n 5
`)
	got := maskedPipeSteps(t, dir)
	if len(got) != 1 || got["a.yaml/plain/masked"] == nil {
		t.Errorf("masked steps of the fixture = %v, want only a.yaml/plain/masked", got)
	}
}

func TestWorkflowStepsDoNotMaskPipeFailures(t *testing.T) {
	found := maskedPipeSteps(t, "../../.github/workflows")
	var unexpected []string
	for step, lines := range found {
		if _, ok := maskedPipeAllowlist[step]; !ok {
			unexpected = append(unexpected, step+": "+strings.Join(lines, " ; "))
		}
	}
	sort.Strings(unexpected)
	for _, s := range unexpected {
		t.Errorf("%s\n\tpipes into tee or tail without pipefail, so the step passes when the command fails; add `shell: bash`", s)
	}
	for step, reason := range maskedPipeAllowlist {
		if _, ok := found[step]; !ok {
			t.Errorf("allowlisted step %s no longer masks a pipe (%s): remove it from maskedPipeAllowlist", step, reason)
		}
	}
}
