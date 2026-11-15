package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// The dated block every page that states the verified numbers carries, between these markers.
const (
	blockStart = "<!-- verified-numbers:start -->"
	blockEnd   = "<!-- verified-numbers:end -->"
	summaryURL = "https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/registry/generated/verification/summary.json"
)

// Pages that carry the block, the report page and the website's copy of the figures.
var (
	BlockPages = []string{"README.md", "docs/KNOWN_ISSUES.md", "docs/real-window-compatibility-gaps.md"}
	ReportPage = "conformance/reports/verification.md"
	SitePage   = "website/src/data/verification.json"
)

var (
	statusOrder = []string{"open", "fixed", "documented", "owner-kept", "upstream"}
	impactOrder = []string{"explore-visible", "drilldown-visible", "grafana-datasource", "api-only", "edge"}
)

// LoadSummary reads summary.json; a missing file is nil (no daily run recorded yet).
func LoadSummary(path string) (*Summary, error) {
	var s Summary
	err := readJSON(path, &s)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func versions(s *Summary) string {
	if len(s.VictoriaLogs) == 0 {
		return "VictoriaLogs not recorded"
	}
	return "VictoriaLogs " + strings.Join(s.VictoriaLogs, " and ")
}

// Block is the dated block: every number with the run it was verified on. Its date and commit come from
// summary.json, never from the clock, so a pull request that regenerates it writes the same text.
func Block(s *Summary) string {
	var b strings.Builder
	b.WriteString(blockStart + "\n\n")
	if s == nil {
		b.WriteString("The numbers on this page are not verified by a live run yet: the daily report (`.github/workflows/daily-report.yaml`) has recorded no run on `main`.\n\n")
		b.WriteString(blockEnd)
		return b.String()
	}
	state := "every check green"
	if s.Red {
		state = fmt.Sprintf("%d check(s) red", len(s.Reasons))
	}
	fmt.Fprintf(&b, "Verified %s on `%s` (Loki %s, %s), %s; see the [daily summary](%s).\n\n",
		s.Date, short(s.Commit), s.Loki, versions(s), state, summaryURL)
	var parts []string
	for _, k := range statusOrder {
		parts = append(parts, fmt.Sprintf("%d %s", s.Registry.ByStatus[k], k))
	}
	var impact []string
	for _, k := range impactOrder {
		impact = append(impact, fmt.Sprintf("%d %s", s.Registry.OpenByImpact[k], k))
	}
	fmt.Fprintf(&b, "- Registry: %s; open by impact: %s.\n", strings.Join(parts, ", "), strings.Join(impact, ", "))
	if len(s.VictoriaLogs) > 0 {
		var runs []string
		for _, v := range s.VictoriaLogs {
			n := func(keys ...string) int {
				total := 0
				for _, k := range keys {
					total += caseCount(*s, v, k)
				}
				return total
			}
			runs = append(runs, fmt.Sprintf("%s: %d match, %d differ on equal data, %d not comparable on that day's data, %d blocked", v,
				n("matches"), n("differs"), n("data_differs", "empty", "loki_not_ready", "loki_cached_stale", "not_comparable", "data_unchecked"),
				n("blocked", "both_fail", "unstable")))
		}
		fmt.Fprintf(&b, "- Registry cases sent to Loki and the proxy, per VictoriaLogs version: %s.\n", strings.Join(runs, "; "))
	}
	if e := s.Errors; e != nil {
		live := "not re-recorded"
		if e.Live != nil {
			live = fmt.Sprintf("%d of %d rows answered as recorded by the live Loki", e.Live.Rows-e.Live.Changed, e.Live.Rows)
		}
		fmt.Fprintf(&b, "- Errors: %d of %d recorded Loki answers match, %d known differences, %d known leaks; %s.\n",
			e.Recorded.MatchingRows, e.Recorded.ComparableRows, e.Recorded.KnownDifferences, e.Recorded.KnownLeaks, live)
	}
	if sw := s.Sweep; sw != nil {
		kinds := make([]string, 0, len(sw.Differences))
		for k := range sw.Differences {
			kinds = append(kinds, k)
		}
		sort.Slice(kinds, func(i, j int) bool {
			if sw.Differences[kinds[i]] != sw.Differences[kinds[j]] {
				return sw.Differences[kinds[i]] > sw.Differences[kinds[j]]
			}
			return kinds[i] < kinds[j]
		})
		var list []string
		for _, k := range kinds {
			list = append(list, fmt.Sprintf("%d %s", sw.Differences[k], k))
		}
		live := "not re-verified live"
		if sw.LiveCheck != nil {
			live = fmt.Sprintf("re-verified live by `%v` on %v (%v)", sw.LiveCheck["workflow"], sw.LiveCheck["date"], sw.LiveCheck["conclusion"])
		}
		fmt.Fprintf(&b, "- Grammar sweep of %d queries, %s: %s.\n", sw.Queries, live, strings.Join(list, ", "))
	}
	if w := s.PerfWeekly; w != nil {
		fmt.Fprintf(&b, "- Performance, weekly full run verified %s (Loki %s): %d comparable shape x range pairs, %d faster, %d on par, %d slower; %d rows above 2x Loki.\n",
			w.Date, w.Loki, w.Pairs, w.Faster, w.Par, w.Slower, w.Over2xRows)
	}
	if p := s.PerfDaily; p != nil {
		fmt.Fprintf(&b, "- Performance, daily smoke verified %v: %v rows, %d above 2x Loki, %d weekly ratchet rows lost.\n",
			p["date"], p["rows"], lenOf(p["over_2x"]), lenOf(p["ratchet_lost"]))
	}
	if reuse := reuseLine(s); reuse != "" {
		b.WriteString(reuse)
	}
	b.WriteString("\n" + blockEnd)
	return b.String()
}

// reuseLine states the proxy's cache reuse of the day: the cases' repeats per version and the smoke's rows.
func reuseLine(s *Summary) string {
	var parts []string
	for _, v := range s.VictoriaLogs {
		c, _ := s.Cases[v].(map[string]any)
		var r map[string]any
		switch x := c["reuse"].(type) {
		case map[string]any:
			r = x
		case *CaseReuse:
			if x != nil {
				r = map[string]any{"counts": map[string]any{"reused": float64(x.Counts["reused"]), "cache_not_reused": float64(x.Counts["cache_not_reused"])},
					"repeat_vl_calls": x.RepeatCalls}
			}
		}
		if r == nil {
			continue
		}
		counts, _ := r["counts"].(map[string]any)
		parts = append(parts, fmt.Sprintf("registry cases on %s: %d repeats served from the cache, %d not (%s VictoriaLogs calls on the repeats)",
			v, toInt(counts["reused"]), toInt(counts["cache_not_reused"]), trimFloat(num(r["repeat_vl_calls"]))))
	}
	if p := s.PerfDaily; p != nil {
		if r, ok := p["reuse"].(map[string]any); ok {
			parts = append(parts, fmt.Sprintf("perf smoke: %d of %d rows repeat without calling VictoriaLogs", toInt(r["repeat_without_vl_calls"]), toInt(r["rows"])))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "- Cache reuse, verified " + s.Date + ": " + strings.Join(parts, "; ") + ".\n"
}

// ReplaceBlock puts the block between the page's markers; a page without them is an error.
func ReplaceBlock(text, block string) (string, error) {
	i := strings.Index(text, blockStart)
	j := strings.Index(text, blockEnd)
	if i < 0 || j < i {
		return "", fmt.Errorf("no %s ... %s markers", blockStart, blockEnd)
	}
	return text[:i] + block + text[j+len(blockEnd):], nil
}

// ReportText is conformance/reports/verification.md: the whole daily summary.
func ReportText(s *Summary) string {
	head := "# Verified numbers\n\n<!-- Generated by go run ./test/dailyreport -docs from conformance/registry/generated/verification/summary.json. Do not edit. -->\n\n"
	if s == nil {
		return head + "The daily report (`.github/workflows/daily-report.yaml`) has recorded no run on `main` yet.\n"
	}
	return head + strings.Replace(Markdown(*s), "## Daily report", "## Daily report of", 1)
}

// SiteText is the website's copy of the dated figures.
func SiteText(s *Summary) (string, error) {
	data := map[string]any{"verified": false}
	if s != nil {
		// The per-case status list is the next report's comparison, not a figure the site shows.
		registry := s.Registry
		registry.CaseStatus = nil
		data = map[string]any{"verified": true, "date": s.Date, "commit": short(s.Commit), "loki": s.Loki,
			"victorialogs": s.VictoriaLogs, "red": s.Red, "registry": registry, "errors": s.Errors, "sweep": s.Sweep,
			"perf_weekly": s.PerfWeekly}
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	err := enc.Encode(data)
	return b.String(), err
}

// Pages renders every page the summary feeds: path -> its full new text.
func Pages(s *Summary, read func(string) ([]byte, error)) (map[string]string, error) {
	out := map[string]string{}
	block := Block(s)
	for _, path := range BlockPages {
		raw, err := read(path)
		if err != nil {
			return nil, err
		}
		text, err := ReplaceBlock(string(raw), block)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out[path] = text
	}
	out[ReportPage] = ReportText(s)
	site, err := SiteText(s)
	if err != nil {
		return nil, err
	}
	out[SitePage] = site
	return out, nil
}

// Ages are the limits of the staleness gate: a daily number older than Daily, a weekly one older than Weekly.
type Ages struct {
	Daily, Weekly time.Duration
}

// Stale lists the numbers older than their limit at now; a summary that does not exist yet is not stale
// (no daily run has been merged), and is reported apart.
func Stale(s *Summary, now time.Time, ages Ages) []string {
	if s == nil {
		return nil
	}
	var out []string
	check := func(label, date string, limit time.Duration) {
		t, err := time.Parse("2006-01-02", date)
		if err != nil {
			out = append(out, fmt.Sprintf("%s has no date (%q)", label, date))
			return
		}
		// A date is the whole day: the age counts from its end.
		if age := now.Sub(t.Add(24 * time.Hour)); age > limit {
			out = append(out, fmt.Sprintf("%s were verified %s, %s ago (the limit is %s): merge the newest daily report pull request", label, date, age.Round(time.Hour), limit))
		}
	}
	check("the daily numbers", s.Date, ages.Daily)
	if s.PerfWeekly != nil {
		check("the weekly performance numbers", s.PerfWeekly.Date, ages.Weekly)
	}
	return out
}

// Strict reports whether the staleness gate fails rather than warns: on main's scheduled runs only (the daily
// report runs it), so neither a pull request nor a merge to main is blocked by a report not merged yet.
func Strict(event, ref string) bool {
	return event == "schedule" && ref == "refs/heads/main"
}
