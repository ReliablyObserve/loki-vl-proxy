package testcache

import (
	"bufio"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	repoRoot  = "../.."
	policyTSV = "scripts/ci/test-cache-uncached.tsv"
	ciYAML    = ".github/workflows/ci.yaml"
)

// readUncached returns directory -> reason of the packages that always run uncached.
func readUncached(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(filepath.Join(repoRoot, policyTSV))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		dir, reason, ok := strings.Cut(line, "\t")
		if !ok || strings.TrimSpace(reason) == "" {
			t.Fatalf("%s: %q needs a tab and a reason", policyTSV, line)
		}
		out[dir] = reason
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Inputs the test cache cannot see: it records environment variables and the files and directories a
// test opens inside the module, and nothing else.
var (
	childProcess  = regexp.MustCompile(`"os/exec"`)
	hostIdentity  = regexp.MustCompile(`\bos\.(Executable|UserHomeDir|Environ|Hostname)\(`)
	hostFile      = regexp.MustCompile(`"/(proc|sys|etc|dev|run|var|usr|opt|root|home)(/|")`)
	networkLookup = regexp.MustCompile(`\bnet\.(Dial\w*|Lookup\w+)\(`)
	remoteRequest = regexp.MustCompile(`\bhttp\.(Get|Post|Head|NewRequest\w*)\([^)]*"(https?://[^"]*)"`)
)

// invisibleInputs lists why the Go code in src depends on state the cache cannot see.
func invisibleInputs(src string) []string {
	var reasons []string
	add := func(r string) {
		for _, have := range reasons {
			if have == r {
				return
			}
		}
		reasons = append(reasons, r)
	}
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		switch {
		case childProcess.MatchString(line):
			add("child process")
		case hostIdentity.MatchString(line):
			add("host identity")
		case hostFile.MatchString(line):
			add("host file outside the module")
		case networkLookup.MatchString(line):
			add("network lookup")
		}
		if m := remoteRequest.FindStringSubmatch(line); m != nil {
			url := m[2]
			local := strings.HasPrefix(url, "http://127.") || strings.HasPrefix(url, "https://127.") ||
				strings.Contains(url, "localhost") || strings.Contains(url, "[::1]") ||
				strings.ContainsAny(url, "%{")
			if !local {
				add("request to a literal remote host")
			}
		}
	}
	return reasons
}

// packageDirs returns the directories of the root module that hold Go files the unit tests build, the
// way `go test ./...` sees them: no other module, no testdata, no file behind a build tag.
func packageDirs(t *testing.T) []string {
	t.Helper()
	var dirs []string
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		rel, _ := filepath.Rel(repoRoot, path)
		if rel != "." && (strings.HasPrefix(name, ".") || name == "testdata" || name == "node_modules" || name == "vendor") {
			return filepath.SkipDir
		}
		if rel != "." {
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir // another module (bench/)
			}
		}
		if rel == filepath.Join("test", "testcache") || strings.HasPrefix(rel, filepath.Join("test", "testcache")+string(filepath.Separator)) {
			return filepath.SkipDir // this package holds the patterns above as text
		}
		entries, _ := os.ReadDir(path)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".go") && !isTagged(filepath.Join(path, e.Name())) {
				dirs = append(dirs, filepath.ToSlash(rel))
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(dirs)
	return dirs
}

func isTagged(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "package ") {
			return false
		}
		if strings.HasPrefix(line, "//go:build") {
			return true
		}
	}
	return false
}

func dirSource(t *testing.T, dir string) string {
	t.Helper()
	var sb strings.Builder
	entries, err := os.ReadDir(filepath.Join(repoRoot, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(repoRoot, dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// A package whose tests read state outside the module, run a child process or reach the network can
// pass from the cache after that state changed. Each such package must run uncached (and say why).
func TestEveryPackageReadingInvisibleStateRunsUncached(t *testing.T) {
	uncached := readUncached(t)
	dirs := packageDirs(t)
	have := map[string]bool{}
	for _, d := range dirs {
		have[d] = true
		if _, listed := uncached[d]; listed {
			continue
		}
		if reasons := invisibleInputs(dirSource(t, d)); len(reasons) > 0 {
			t.Errorf("%s reads state the test cache cannot see (%s) but is not listed in %s", d, strings.Join(reasons, ", "), policyTSV)
		}
	}
	for d, reason := range uncached {
		if !have[d] {
			t.Errorf("%s is listed in %s but is not a package of the root module", d, policyTSV)
		}
		if len(reason) < 20 {
			t.Errorf("%s: the reason %q is too short to be one", d, reason)
		}
	}
}

// The packages that make up almost all of the test time stay cached.
func TestTheHeavyPackagesStayCached(t *testing.T) {
	uncached := readUncached(t)
	for _, d := range []string{"internal/proxy", "internal/translator", "internal/cache", "internal/logql", "internal/logsql"} {
		if _, listed := uncached[d]; listed {
			t.Errorf("%s must use the test cache", d)
		}
	}
}

func TestInvisibleInputsAreFound(t *testing.T) {
	cases := map[string]string{
		"child process":                    `import "os/exec"`,
		"host identity":                    `h, _ := os.Hostname()`,
		"host file outside the module":     `b, _ := os.ReadFile("/etc/hosts")`,
		"network lookup":                   `c, _ := net.Dial("tcp", "db:5432")`,
		"request to a literal remote host": `r, _ := http.Get("https://api.github.com/x")`,
	}
	for want, src := range cases {
		got := invisibleInputs(src)
		if len(got) != 1 || got[0] != want {
			t.Errorf("%q: got %v, want [%s]", src, got, want)
		}
	}
	clean := strings.Join([]string{
		`// reads /etc/hosts in a comment`,
		`b, _ := os.ReadFile("../../conformance/registry/x.json")`,
		`v := os.Getenv("CI")`,
		`r, _ := http.Get(srv.URL)`,
		`r, _ := http.Get("http://127.0.0.1:" + port)`,
		`cfg := Config{BackendURL: "http://example.com", Origin: "https://grafana.example.com"}`,
		`msg := "see https://docs.victoriametrics.com/victorialogs/"`,
	}, "\n")
	if got := invisibleInputs(clean); len(got) != 0 {
		t.Errorf("clean source flagged: %v", got)
	}
}

// The runner lists the uncached packages from the policy file and gives Go -count=1 for those only.
func TestRunnerSplitsCachedAndUncachedPackages(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("no bash")
	}
	uncached := readUncached(t)
	for name, failing := range map[string]string{"ok": "", "cached fails": "cached", "uncached fails": "uncached"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "calls.log")
			stub := filepath.Join(dir, "bin", "go")
			if err := os.MkdirAll(filepath.Dir(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(stub, []byte(goStub), 0o755); err != nil {
				t.Fatal(err)
			}
			profile := filepath.Join(dir, "coverage.out")
			script, _ := filepath.Abs(filepath.Join(repoRoot, "scripts/ci/go_test_unit.sh"))
			cmd := exec.Command("bash", script)
			cmd.Env = append(os.Environ(), "PATH="+filepath.Dir(stub)+string(os.PathListSeparator)+os.Getenv("PATH"),
				"STUB_LOG="+log, "STUB_FAIL="+failing, "COVERPROFILE="+profile)
			out, err := cmd.Output()
			if (failing != "") != (err != nil) {
				t.Fatalf("failing=%q but err=%v", failing, err)
			}
			calls := readLines(t, log)
			var tests []string
			for _, c := range calls {
				if strings.HasPrefix(c, "go test") {
					tests = append(tests, c)
				}
			}
			if len(tests) != 2 {
				t.Fatalf("want one uncached and one cached go test, got %v", tests)
			}
			un, ca := tests[0], tests[1]
			if !strings.Contains(un, " -count=1 ") || strings.Contains(ca, " -count=1 ") {
				t.Fatalf("-count=1 must be on the uncached run only:\n%s\n%s", un, ca)
			}
			for d := range uncached {
				if !strings.Contains(un, "stub/mod/"+d+" ") || strings.Contains(ca, "stub/mod/"+d+" ") {
					t.Errorf("%s must be in the uncached run only", d)
				}
			}
			if !strings.Contains(ca, "stub/mod/internal/proxy") {
				t.Errorf("internal/proxy must be in the cached run: %s", ca)
			}
			for _, flag := range []string{"-race", "-timeout=50m", "-json", "-coverprofile="} {
				if !strings.Contains(un, flag) || !strings.Contains(ca, flag) {
					t.Errorf("both runs need %s", flag)
				}
			}
			if !strings.Contains(string(out), "T_cached") || !strings.Contains(string(out), "T_uncached") {
				t.Errorf("the events of both runs must reach stdout:\n%s", out)
			}
			merged := strings.Join(readLines(t, profile), "\n")
			if strings.Count(merged, "mode:") != 1 || !strings.HasPrefix(merged, "mode: atomic") ||
				!strings.Contains(merged, "cached.go:") || !strings.Contains(merged, "uncached.go:") {
				t.Errorf("coverage profile is not merged under one mode line:\n%s", merged)
			}
		})
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

// A go that records its calls, lists a fixed package set and writes a coverage profile.
const goStub = `#!/bin/bash
echo "go $*" >> "$STUB_LOG"
if [ "$1" = "list" ] && [ "$2" = "-m" ]; then echo stub/mod; exit 0; fi
if [ "$1" = "list" ]; then
  for p in cmd/proxy internal/cache internal/memlimit internal/metrics internal/proxy; do echo "stub/mod/$p"; done
  exit 0
fi
cover=""
for a in "$@"; do case "$a" in -coverprofile=*) cover="${a#-coverprofile=}";; esac; done
case " $* " in *" -count=1 "*) name=uncached;; *) name=cached;; esac
printf 'mode: atomic\n%s.go:1.1,2.2 1 1\n' "$name" > "$cover"
echo "{\"Action\":\"pass\",\"Package\":\"p\",\"Test\":\"T_$name\"}"
[ "${STUB_FAIL:-}" = "$name" ] && exit 1
exit 0
`

// The workflow restores and saves the Go cache with a key on the Go version, OS/arch, go.sum and the
// workflow file, normalises checkout times first and no longer forces every package to rerun.
func TestWorkflowWiring(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot, ciYAML))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	job := func(name string) string {
		re := regexp.MustCompile(`(?ms)^  ` + regexp.QuoteMeta(name) + `:\n(.*?)^  [a-z0-9-]+:\n`)
		m := re.FindStringSubmatch(text)
		if m == nil {
			t.Fatalf("no job %s", name)
		}
		return m[1]
	}
	test := job("test")
	if !strings.Contains(test, "scripts/ci/go_test_unit.sh") {
		t.Error("the test job must run the unit tests through scripts/ci/go_test_unit.sh")
	}
	if strings.Contains(test, "go test ./... -count=1") {
		t.Error("the test job must not force every package to rerun")
	}
	for _, name := range []string{"test", "race-stress-shard"} {
		j := job(name)
		for _, want := range []string{"actions/cache/restore@", "actions/cache/save@", "cache-primary-key", "restore-keys:", "cache: false",
			"cmd/normalize-mtimes"} {
			if !strings.Contains(j, want) {
				t.Errorf("job %s lacks %q", name, want)
			}
		}
		key := regexp.MustCompile(`(?m)^          key: (gotest-.*)$`).FindStringSubmatch(j)
		if key == nil {
			t.Errorf("job %s has no gotest key", name)
			continue
		}
		for _, part := range []string{"runner.os", "runner.arch", "setup-go.outputs.go-version", "hashFiles('**/go.sum')", "hashFiles('.github/workflows/ci.yaml')"} {
			if !strings.Contains(key[1], part) {
				t.Errorf("job %s: key lacks %s", name, part)
			}
		}
		if strings.Index(j, "cmd/normalize-mtimes") > strings.Index(j, "actions/cache/restore@") {
			t.Errorf("job %s: normalise the checkout times before the cache is used", name)
		}
	}
	// In the shards only the verbose re-run of the known racy tests keeps -count=1.
	var withCount []string
	for _, line := range strings.Split(job("race-stress-shard"), "\n") {
		if strings.Contains(line, "go test") && strings.Contains(line, "-count=1") {
			withCount = append(withCount, strings.TrimSpace(line))
		}
	}
	if len(withCount) != 1 || !strings.Contains(withCount[0], " -v ") {
		t.Errorf("only the verbose targeted re-run keeps -count=1 in the shards, got %v", withCount)
	}
}
