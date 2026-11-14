package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A day's summary as summary.json holds it: written and read back.
func savedSummary(t *testing.T) (*Summary, string) {
	t.Helper()
	s, _, err := Build(fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "summary.json")
	if err := writeJSON(path, s); err != nil {
		t.Fatal(err)
	}
	back, err := LoadSummary(path)
	if err != nil || back == nil {
		t.Fatalf("read back: %v", err)
	}
	return back, path
}

func TestBlockStatesEveryNumberWithItsRun(t *testing.T) {
	s, _ := savedSummary(t)
	block := Block(s)
	for _, want := range []string{blockStart, blockEnd,
		"Verified 2026-11-14 on `c0ffee012345` (Loki 3.7.9, VictoriaLogs v1.49.0 and v1.53.0), every check green; see the [daily summary](" + summaryURL + ").",
		"- Registry: 2 open, 1 fixed, 0 documented, 0 owner-kept, 0 upstream; open by impact: 1 explore-visible, 0 drilldown-visible, 0 grafana-datasource, 0 api-only, 1 edge.",
		"v1.49.0: 3 match, 1 differ on equal data, 0 not comparable on that day's data, 0 blocked; v1.53.0: 3 match, 1 differ on equal data, 0 not comparable on that day's data, 0 blocked.",
		"- Errors: 221 of 231 recorded Loki answers match, 10 known differences, 2 known leaks; 273 of 273 rows answered as recorded by the live Loki.",
		"- Grammar sweep of 25824 queries, re-verified live by `query-corpus.yaml` on 2026-11-14 (success): 547 proxy accepts, Loki rejects, 212 error text differs.",
		"- Performance, weekly full run verified 2026-11-09 (Loki 3.7.9): 76 comparable shape x range pairs, 50 faster, 24 on par, 2 slower; 3 rows above 2x Loki.",
		"- Performance, daily smoke verified 2026-11-14: 120 rows, 0 above 2x Loki, 0 weekly ratchet rows lost.",
		"- Cache reuse, verified 2026-11-14: registry cases on v1.49.0: 2 repeats served from the cache, 1 not (1 VictoriaLogs calls on the repeats); " +
			"registry cases on v1.53.0: 2 repeats served from the cache, 1 not (1 VictoriaLogs calls on the repeats); perf smoke: 100 of 110 rows repeat without calling VictoriaLogs."} {
		if !strings.Contains(block, want) {
			t.Errorf("block lacks %q:\n%s", want, block)
		}
	}
	s.Red, s.Reasons, s.VictoriaLogs, s.Errors.Live, s.Sweep.LiveCheck = true, []string{"a", "b"}, nil, nil, nil
	red := Block(s)
	for _, want := range []string{"2 check(s) red", "VictoriaLogs not recorded", "not re-recorded", "not re-verified live"} {
		if !strings.Contains(red, want) {
			t.Errorf("red block lacks %q:\n%s", want, red)
		}
	}
	if none := Block(nil); !strings.Contains(none, "not verified by a live run yet") {
		t.Errorf("block without a summary: %s", none)
	}
}

func TestReplaceBlock(t *testing.T) {
	page := "# T\n\nintro\n\n" + blockStart + "\nold\n" + blockEnd + "\n\n## Next\n"
	got, err := ReplaceBlock(page, blockStart+"\nnew\n"+blockEnd)
	if err != nil || got != "# T\n\nintro\n\n"+blockStart+"\nnew\n"+blockEnd+"\n\n## Next\n" {
		t.Errorf("replace: %v %q", err, got)
	}
	for _, bad := range []string{"no markers", blockEnd + blockStart, blockStart + " only"} {
		if _, err := ReplaceBlock(bad, "x"); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestPagesAndSiteData(t *testing.T) {
	s, _ := savedSummary(t)
	read := func(path string) ([]byte, error) {
		return []byte("# " + path + "\n\n" + blockStart + blockEnd + "\n"), nil
	}
	pages, err := Pages(s, read)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != len(BlockPages)+2 || !strings.Contains(pages["README.md"], "Verified 2026-11-14") ||
		!strings.Contains(pages[ReportPage], "## Daily report of 2026-11-14: green") || !strings.Contains(pages[ReportPage], "| v1.53.0 | 3 | 1 |") {
		t.Errorf("pages: %v", pages[ReportPage])
	}
	var site map[string]any
	if err := json.Unmarshal([]byte(pages[SitePage]), &site); err != nil || site["verified"] != true || site["date"] != "2026-11-14" {
		t.Errorf("site data: %v %v", err, site)
	}
	empty, err := Pages(nil, read)
	if err != nil || !strings.Contains(empty[ReportPage], "has recorded no run") || !strings.Contains(empty[SitePage], `"verified": false`) {
		t.Errorf("pages without a summary: %v %v", err, empty)
	}
	if _, err := Pages(s, func(string) ([]byte, error) { return []byte("no markers"), nil }); err == nil {
		t.Error("a page without markers is an error")
	}
	if _, err := Pages(s, func(string) ([]byte, error) { return nil, errors.New("gone") }); err == nil {
		t.Error("an unreadable page is an error")
	}
}

func TestStaleness(t *testing.T) {
	s, _ := savedSummary(t)
	ages := Ages{Daily: 48 * time.Hour, Weekly: 8 * 24 * time.Hour}
	at := func(v string) time.Time {
		tm, err := time.Parse(time.RFC3339, v)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}
	if got := Stale(s, at("2026-11-16T23:00:00Z"), ages); len(got) != 0 {
		t.Errorf("47 hours after the day ended is fresh: %v", got)
	}
	got := Stale(s, at("2026-11-17T01:00:00Z"), ages)
	if len(got) != 1 || !strings.Contains(got[0], "the daily numbers were verified 2026-11-14, 49h0m0s ago") {
		t.Errorf("49 hours: %v", got)
	}
	got = Stale(s, at("2026-11-19T00:00:00Z"), ages)
	if len(got) != 2 || !strings.Contains(got[1], "the weekly performance numbers were verified 2026-11-09") {
		t.Errorf("a weekly number over eight days old: %v", got)
	}
	s.Date = "today"
	if got := Stale(s, at("2026-11-15T00:00:00Z"), ages); len(got) != 1 || !strings.Contains(got[0], "has no date") {
		t.Errorf("no date: %v", got)
	}
	if Stale(nil, time.Now(), ages) != nil {
		t.Error("no summary yet is not stale")
	}
	for _, c := range []struct {
		event, ref string
		strict     bool
	}{{"push", "refs/heads/main", false}, {"schedule", "refs/heads/main", true}, {"pull_request", "refs/pull/1/merge", false},
		{"schedule", "refs/heads/feature", false},
		{"push", "refs/heads/feature", false}, {"workflow_dispatch", "refs/heads/main", false}, {"", "", false}} {
		if Strict(c.event, c.ref) != c.strict {
			t.Errorf("%s %s: strict %v", c.event, c.ref, !c.strict)
		}
	}
}

func TestRunDocs(t *testing.T) {
	_, summary := savedSummary(t)
	dir := t.TempDir()
	t.Chdir(dir)
	for _, p := range BlockPages {
		write(t, p, "# page\n\n"+blockStart+"\n"+blockEnd+"\n\ntext\n")
	}
	base := []string{"-docs", "-summary", summary, "-now", "2026-11-15T12:00:00Z"}
	parse := func(extra ...string) config {
		cfg, err := parseFlags(append(append([]string{}, base...), extra...))
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	var out, errs bytes.Buffer
	if code := run(parse("-check", "-event", "pull_request"), &out, &errs); code != 1 || !strings.Contains(out.String(), "README.md is stale") {
		t.Errorf("check before writing: %d %s", code, out.String())
	}
	if code := run(parse(), &out, &errs); code != 0 {
		t.Fatalf("write: %d %s", code, errs.String())
	}
	out.Reset()
	if code := run(parse("-check", "-event", "push", "-ref", "refs/heads/main"), &out, &errs); code != 0 || !strings.Contains(out.String(), "5 pages current") {
		t.Errorf("check after writing: %d %s", code, out.String())
	}
	// Old numbers fail on main and warn on a pull request.
	old := []string{"-check", "-now", "2026-11-20T00:00:00Z"}
	out.Reset()
	if code := run(parse(append(old, "-event", "schedule", "-ref", "refs/heads/main")...), &out, &errs); code != 1 ||
		!strings.Contains(out.String(), "merge the newest daily report pull request") {
		t.Errorf("stale on main: %d %s", code, out.String())
	}
	out.Reset()
	if code := run(parse(append(old, "-event", "pull_request")...), &out, &errs); code != 0 || !strings.Contains(out.String(), "warning: the daily numbers") {
		t.Errorf("stale on a pull request: %d %s", code, out.String())
	}
	// No summary yet: the pages say so and nothing is stale.
	none := parse("-summary", filepath.Join(dir, "none.json"))
	if code := run(none, &out, &errs); code != 0 {
		t.Fatalf("write without a summary: %d", code)
	}
	none.check = true
	out.Reset()
	if code := run(none, &out, &errs); code != 0 || !strings.Contains(out.String(), "no daily summary on this branch yet") {
		t.Errorf("check without a summary: %d %s", code, out.String())
	}
	// Errors: a broken summary, a page without markers, a bad -now, an unwritable page.
	write(t, filepath.Join(dir, "broken.json"), "{")
	for name, cfg := range map[string]config{
		"broken summary": parse("-summary", filepath.Join(dir, "broken.json")),
		"bad now":        parse("-check", "-now", "yesterday"),
	} {
		if code := run(cfg, &out, &errs); code != 2 {
			t.Errorf("%s: %d", name, code)
		}
	}
	write(t, ReportPage, "")
	if err := os.Remove(ReportPage); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(ReportPage, 0o755); err != nil {
		t.Fatal(err)
	}
	if code := run(parse(), &out, &errs); code != 2 {
		t.Errorf("unwritable report: %d", code)
	}
	if err := os.WriteFile(BlockPages[0], []byte("no markers"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run(parse(), &out, &errs); code != 2 {
		t.Errorf("page without markers: %d", code)
	}
}

// The repository's pages are current against its summary: the check the conformance gate runs.
func TestRepositoryVerifiedNumbersAreCurrent(t *testing.T) {
	t.Chdir("../..")
	cfg, err := parseFlags([]string{"-docs", "-check", "-event", "pull_request"})
	if err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	if code := run(cfg, &out, &errs); code != 0 {
		t.Fatalf("%s%s\nrun go run ./test/dailyreport -docs", out.String(), errs.String())
	}
}

// BenchmarkPages renders the dated pages of one summary (the conformance gate's cost per run).
func BenchmarkPages(b *testing.B) {
	s, _, err := Build(fixture(b))
	if err != nil {
		b.Fatal(err)
	}
	page := []byte("# page\n\n" + blockStart + blockEnd + "\n")
	read := func(string) ([]byte, error) { return page, nil }
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Pages(&s, read); err != nil {
			b.Fatal(err)
		}
	}
}

func TestReuseLineFromABuiltSummary(t *testing.T) {
	s, _, err := Build(fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if line := reuseLine(&s); !strings.Contains(line, "registry cases on v1.53.0: 2 repeats served from the cache, 1 not") {
		t.Errorf("built summary: %q", line)
	}
	if line := reuseLine(&Summary{}); line != "" {
		t.Errorf("no reuse: %q", line)
	}
}
