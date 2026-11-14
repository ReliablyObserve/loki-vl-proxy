package main

import (
	"bytes"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const deviation = "metric-identity/series-split"

var ownerKeptCases = map[string]string{
	deviation: "request: {method: GET, path: /loki/api/v1/labels, params: {start: now-1h, end: now}}\ncompare: {mode: exact}\n" +
		"gap: {status: owner-kept, impact: edge, effort: S, area: a, loki: l, proxy: p}\n",
	// Differs from Loki and names the deviation.
	"x/split": "request: {method: POST, path: /loki/api/v1/query_range, params: {query: 'sum(x)', start: now-1h, end: now, step: 60s}}\n" +
		"compare: {mode: numeric}\nowner_kept: [" + deviation + "]\ngap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n",
	// The same difference, naming nothing: a regression.
	"x/plain": "request: {method: POST, path: /loki/api/v1/query_range, params: {query: 'sum(x)', start: now-1h, end: now, step: 60s}}\n" +
		"compare: {mode: numeric}\ngap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n",
}

// A fixed case that names an owner-kept deviation and differs is reported with it, not as a regression; a
// case that names nothing stays a regression, and -check still fails on that one only.
func TestOwnerKeptDifferenceIsAnnotatedNeverPassed(t *testing.T) {
	root := writeRegistry(t, ownerKeptCases)
	loki, proxy, vl, _ := stack(t)
	dir := t.TempDir()
	cfg := config{registry: root, loki: loki.URL, proxy: proxy.URL, vl: vl.URL, out: filepath.Join(dir, "cases.json"),
		report: filepath.Join(dir, "report.md"), commit: "abc", tenant: "0", check: true, noReuse: true,
		settle: minSettle, timeout: 5 * time.Second, now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 1 {
		t.Fatalf("the unannotated difference still fails -check: %d %s", code, errs.String())
	}
	f := readFile(t, cfg.out)
	if r := f.Cases["x/split"]; r.Verdict != verdictDiffers || r.Flag != flagOwnerKept || !reflect.DeepEqual(r.OwnerKept, []string{deviation}) || len(r.Differences) == 0 {
		t.Errorf("annotated case: %+v", r)
	}
	if r := f.Cases["x/plain"]; r.Verdict != verdictDiffers || r.Flag != flagRegression {
		t.Errorf("plain case: %+v", r)
	}
	if !reflect.DeepEqual(f.Flags[flagOwnerKept], []string{"x/split"}) || !reflect.DeepEqual(f.Flags[flagRegression], []string{"x/plain"}) {
		t.Errorf("flags: %+v", f.Flags)
	}
	if !strings.Contains(errs.String(), "owner-kept: x/split differs from Loki, subject to "+deviation) || strings.Contains(errs.String(), "regression: x/split") {
		t.Errorf("stderr: %s", errs.String())
	}
	if !strings.Contains(out.String(), "1 regressions, 1 differ with an owner-kept deviation") {
		t.Errorf("stdout: %s", out.String())
	}
	report := readText(t, cfg.report)
	if !strings.Contains(report, "subject to the owner-kept deviation(s): "+deviation) || !strings.Contains(report, "`x/split`") {
		t.Errorf("the difference stays in the report:\n%s", report)
	}
}

// The annotation must point at an owner-kept case; otherwise the case is blocked, not excused.
func TestOwnerKeptMustNameAnOwnerKeptCase(t *testing.T) {
	root := writeRegistry(t, map[string]string{
		"x/open": "request: {method: GET, path: /loki/api/v1/labels}\ncompare: {mode: exact}\ngap: {status: open, impact: edge, effort: S, area: a, loki: l, proxy: p}\n",
		"x/a":    "request: {method: GET, path: /loki/api/v1/labels}\ncompare: {mode: exact}\nowner_kept: [x/open]\n",
		"x/b":    "request: {method: GET, path: /loki/api/v1/labels}\ncompare: {mode: exact}\nowner_kept: [x/missing]\n",
	})
	f := runReplayless(t, root)
	for _, id := range []string{"x/a", "x/b"} {
		if r := f.Cases[id]; r.Verdict != verdictBlocked || !strings.Contains(r.Reason, "is not an owner-kept case") {
			t.Errorf("%s: %+v", id, r)
		}
	}
}

// Only a case that is expected to match is excused: an open case that differs is already expected to.
func TestFlagOfOwnerKept(t *testing.T) {
	for _, c := range []struct {
		r    Result
		want string
	}{
		{Result{Expected: "match", Verdict: verdictDiffers, OwnerKept: []string{deviation}}, flagOwnerKept},
		{Result{Expected: "match", Verdict: verdictDiffers}, flagRegression},
		{Result{Expected: "match", Verdict: verdictMatches, OwnerKept: []string{deviation}}, ""},
		{Result{Expected: "differ", Verdict: verdictDiffers, OwnerKept: []string{deviation}}, ""},
	} {
		if got := flagOf(c.r); got != c.want {
			t.Errorf("%+v: %q, want %q", c.r, got, c.want)
		}
	}
}

func readText(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// runReplayless classifies a registry without a stack: every probe that is sent finds no answer.
func runReplayless(t *testing.T, root string) File {
	t.Helper()
	cases, _, err := registry.LoadAll(root)
	if err != nil {
		t.Fatal(err)
	}
	f := File{Cases: map[string]Result{}}
	for _, p := range Probes(cases) {
		f.Cases[p.ID] = Verify(p, fake{}, equalCounts, nil, testNow, "0")
	}
	return f
}
