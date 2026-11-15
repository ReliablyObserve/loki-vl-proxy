package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// The executive summary is the first thing of the daily report: what is ok and what is not, in numbers, per area,
// the Loki side and the VictoriaLogs side, and what moved since the previous report. Every number is computed from
// the Summary the rest of the report reads (and from the previous summary.json and cases.json for the changes); no
// figure is written by hand.

// Targets of the owner's bars.
const (
	greenTargetPercent = 95 // perf rows below or close to Loki
	listCap            = 25 // items of one list before "... N more"
)

// verdictKeys is every verdict of test/caseverify, in the order the report shows them.
var verdictKeys = []string{"matches", "differs", "data_differs", "empty", "loki_not_ready", "loki_cached_stale", "not_comparable",
	"data_unchecked", "unstable", "both_fail", "blocked", "not_executable"}

// Loki-side and VictoriaLogs-side kinds of the grammar sweep; every other kind is a difference with Loki.
const (
	kindVLRejects      = "VictoriaLogs rejects the translator text"
	kindTranslateFails = "accepted, translation fails"
	kindTreeFact       = "tree fact differs"
)

// CasesFile is the part of cases.json the executive summary reads: verdicts and flags per VictoriaLogs version, and
// every case's gap status.
type CasesFile struct {
	Versions  []string
	Verdicts  map[string]map[string]string   // version -> case -> verdict
	Flags     map[string]map[string][]string // version -> flag -> cases
	GapStatus map[string]string              // case -> gap status, "no gap" when it has none
}

// Previous is the earlier report the summary is compared with.
type Previous struct {
	Summary *Summary
	Cases   *CasesFile
}

// ParseCases reads a merged cases document (the one Build returns, or cases.json decoded): nil for nil.
func ParseCases(merged any) *CasesFile {
	if merged == nil {
		return nil
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return nil
	}
	var doc struct {
		Runs map[string]struct {
			Flags map[string][]string `json:"flags"`
		} `json:"runs"`
		Cases map[string]map[string]json.RawMessage `json:"cases"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return nil
	}
	c := &CasesFile{Verdicts: map[string]map[string]string{}, Flags: map[string]map[string][]string{}, GapStatus: map[string]string{}}
	for v, r := range doc.Runs {
		c.Versions = append(c.Versions, v)
		c.Verdicts[v] = map[string]string{}
		c.Flags[v] = r.Flags
	}
	sort.Strings(c.Versions)
	for id, entry := range doc.Cases {
		var status string
		_ = json.Unmarshal(entry["gap_status"], &status)
		if status == "" {
			status = "no gap"
		}
		c.GapStatus[id] = status
		for _, v := range c.Versions {
			var r struct {
				Verdict string `json:"verdict"`
			}
			if json.Unmarshal(entry[v], &r) == nil && r.Verdict != "" {
				c.Verdicts[v][id] = r.Verdict
			}
		}
	}
	return c
}

// LoadPrevious reads the previous report's summary.json and cases.json. A missing summary is no previous report (nil);
// a missing cases file leaves the per-case lists out.
func LoadPrevious(summaryPath, casesPath string) (*Previous, error) {
	s, err := LoadSummary(summaryPath)
	if err != nil || s == nil {
		return nil, err
	}
	p := &Previous{Summary: s}
	var cases any
	switch err := readJSON(casesPath, &cases); {
	case err == nil:
		p.Cases = ParseCases(cases)
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	return p, nil
}

// pct is good of total as a percentage with one decimal, rounded half up; a zero denominator is n/a, and a
// non-zero share that rounds to zero says so instead of showing 0.0%.
func pct(good, total int) string {
	if total <= 0 {
		return "n/a"
	}
	t := tenths(good, total)
	if t == 0 && good > 0 {
		return "<0.1%"
	}
	return fmt.Sprintf("%d.%d%%", t/10, t%10)
}

// tenths is 1000 * good / total, rounded half up.
func tenths(good, total int) int {
	if total <= 0 {
		return 0
	}
	return (good*2000 + total) / (2 * total)
}

func signedTenths(d int) string {
	sign := "+"
	if d < 0 {
		sign, d = "-", -d
	}
	return fmt.Sprintf("%s%d.%d", sign, d/10, d%10)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// row is one line of "Where we stand".
type row struct {
	key, area        string
	known            bool // the run produced the numbers
	split            bool // good and bad are known (a parity result without the verdict split has only a total)
	good, bad, total int
	skip             bool // not one of the counted checks (a view of numbers another row counts)
	met              bool
	target, needed   string
	detail           []string
}

func (r row) pct() string {
	if !r.known || !r.split {
		return "n/a"
	}
	return pct(r.good, r.total)
}

func (r row) cells() (good, bad string) {
	if !r.known || !r.split {
		return "n/a", "n/a"
	}
	return fmt.Sprint(r.good), fmt.Sprint(r.bad)
}

// rows computes every area of the table from the summary.
func rows(s Summary) []row {
	var out []row
	out = append(out, registryRow(s))
	for _, v := range s.VictoriaLogs {
		out = append(out, verdictRows(s, v)...)
	}
	out = append(out, errorsRow(s), sweepRow(s), parityRow(s))
	out = append(out, perfRows(s)...)
	for _, v := range s.VictoriaLogs {
		out = append(out, cacheRow(s, v))
	}
	out = append(out, workflowRow(s))
	return out
}

func registryRow(s Summary) row {
	r := s.Registry
	by := func(k string) int { return r.ByStatus[k] }
	x := row{key: "registry", area: "Registry cases", known: r.Cases > 0, split: true, total: r.Cases,
		good: by("fixed") + by("no gap"), bad: by("open"), target: "0 open"}
	accepted := by("documented") + by("owner-kept")
	x.met = x.bad == 0
	x.needed = "none"
	if x.bad > 0 {
		x.needed = plural(x.bad, "open case to close", "open cases to close")
	}
	x.detail = []string{fmt.Sprintf("Registry cases: fixed %d + no gap %d are good; accepted %d (documented %d, owner-kept %d) and upstream-blocked %d are shown apart; "+
		"good + accepted %d of %d (%s); open by impact: %s.", by("fixed"), by("no gap"), accepted, by("documented"), by("owner-kept"), by("upstream"),
		x.good+accepted, x.total, pct(x.good+accepted, x.total), impactLine(r.OpenByImpact))}
	return x
}

func impactLine(m map[string]int) string {
	var parts []string
	for _, k := range orderedKeys(m, impactOrder) {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// orderedKeys lists the known keys in their order, then the rest sorted.
func orderedKeys(m map[string]int, order []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, k := range order {
		if _, ok := m[k]; ok {
			out = append(out, k)
		}
		seen[k] = true
	}
	var rest []string
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

func verdictRows(s Summary, v string) []row {
	n := func(k string) int { return caseCount(s, v, k) }
	total := 0
	for _, k := range verdictKeys {
		total += n(k)
	}
	known := total > 0
	proven := n("matches") + n("differs")
	match := row{key: "verdicts:" + v, area: "Live verdicts, VictoriaLogs " + v, known: known, split: true, good: n("matches"), bad: n("differs"), total: proven,
		target: "100% of proven match", needed: "none", met: n("differs") == 0}
	if n("differs") > 0 {
		match.needed = fmt.Sprintf("%s to fix (%d of them regressions)", plural(n("differs"), "differing case", "differing cases"), caseCount(s, v, "regressions"))
	}
	match.detail = []string{fmt.Sprintf("Live verdicts, VictoriaLogs %s: proven = matches %d + differs %d = %d of %d cases; regressions %d; proposed closes %d; behaviour changed %d.",
		v, n("matches"), n("differs"), proven, total, caseCount(s, v, "regressions"), caseCount(s, v, "proposed_close"), caseCount(s, v, "behaviour_changed"))}
	cov := row{key: "coverage:" + v, area: "Live coverage, VictoriaLogs " + v, known: known, split: true, skip: true, good: proven, bad: total - proven, total: total,
		target: "100% proven", met: proven == total}
	cov.needed = "none"
	if total > proven {
		cov.needed = fmt.Sprintf("%d more cases to prove live", total-proven)
	}
	other := n("data_differs") + n("loki_not_ready") + n("loki_cached_stale") + n("not_comparable") + n("data_unchecked") + n("unstable") + n("both_fail")
	cov.detail = []string{fmt.Sprintf("Live coverage, VictoriaLogs %s: %d of %d cases proven; not proven: empty on both sides %d, blocked %d, not executable %d, other %d "+
		"(data differs, Loki not ready, Loki cached stale, not comparable, unchecked, unstable, both fail).", v, proven, total, n("empty"), n("blocked"), n("not_executable"), other)}
	// With the registry's wiring a case that is not proven live may be proven by a named passing test: three states.
	if c := s.Coverage[v]; c != nil && c.Cases > 0 {
		good := c.ProvenLive + c.ProvenByTest
		cov = row{key: "coverage:" + v, area: "Coverage, VictoriaLogs " + v, known: known, split: true, skip: true, good: good, bad: c.NotProven, total: c.Cases,
			target: "100% proven live or by a test", met: c.NotProven == 0, needed: "none"}
		if c.NotProven > 0 {
			cov.needed = plural(c.NotProven, "more case to prove", "more cases to prove")
		}
		cov.detail = []string{coverageDetail(v, c)}
	}
	return []row{match, cov}
}

func errorsRow(s Summary) row {
	e := s.Errors
	r := row{key: "errors", area: "Error answers", known: e != nil, split: true, target: "100% match"}
	if e == nil {
		return r
	}
	r.total, r.good = e.Recorded.ComparableRows, e.Recorded.MatchingRows
	r.bad = r.total - r.good
	live := "the live re-record did not run"
	changed := 0
	if e.Live != nil {
		live = fmt.Sprintf("live re-record: %d of %d rows changed", e.Live.Changed, e.Live.Rows)
		changed = e.Live.Changed
	}
	replay := "not run"
	if e.ReplayOK != nil {
		replay = okText(*e.ReplayOK)
	}
	r.met = r.bad == 0 && changed == 0
	r.needed = "none"
	if r.bad > 0 || changed > 0 {
		r.needed = fmt.Sprintf("%s to align, %d live change(s) to review", plural(r.bad, "answer", "answers"), changed)
	}
	r.detail = []string{fmt.Sprintf("Error answers: %d known differences, %d known leaks; %s; unit replay %s.", e.Recorded.KnownDifferences, e.Recorded.KnownLeaks, live, replay)}
	return r
}

func sweepDifferences(s Summary) (total int) {
	if s.Sweep == nil {
		return 0
	}
	for _, n := range s.Sweep.Differences {
		total += n
	}
	return total
}

func sweepRow(s Summary) row {
	sw := s.Sweep
	r := row{key: "sweep", area: "Grammar sweep", known: sw != nil, split: true, target: "0 differences"}
	if sw == nil {
		return r
	}
	diff := sweepDifferences(s)
	r.total, r.bad = sw.Queries, diff
	r.good = sw.Queries - diff
	if r.good < 0 {
		r.good = 0
	}
	r.met = diff == 0
	r.needed = "none"
	if diff > 0 {
		r.needed = plural(diff, "query to bring in line", "queries to bring in line")
	}
	r.detail = []string{fmt.Sprintf("Grammar sweep: %d queries agree out of %d (every difference row counted); Loki accepts %d and rejects %d.",
		r.good, r.total, sw.LokiAnswers["accepted"], sw.LokiAnswers["rejected"])}
	return r
}

func parityRow(s Summary) row {
	p := s.Parity
	r := row{key: "parity", area: "Parity slice", known: p != nil, target: "100% same, 0 unclaimed clusters"}
	if p == nil {
		return r
	}
	r.total = toInt(p["requests"])
	unclaimed := toInt(p["unclaimed"])
	v, hasSplit := p["verdicts"].(map[string]any)
	if hasSplit {
		r.split = true
		r.good, r.bad = toInt(v["same"]), toInt(v["differ"])
	}
	r.met = hasSplit && r.bad == 0 && unclaimed == 0
	switch {
	case !hasSplit:
		r.needed = fmt.Sprintf("%d unclaimed cluster(s) to claim; the same/differ split was not recorded", unclaimed)
	case r.bad == 0 && unclaimed == 0:
		r.needed = "none"
	default:
		r.needed = fmt.Sprintf("%s to fix, %s to claim", plural(r.bad, "differing request", "differing requests"), plural(unclaimed, "cluster", "clusters"))
	}
	split := "same/differ split not recorded"
	if hasSplit {
		split = fmt.Sprintf("same %d, differ %d, both empty %d, blocked %d", r.good, r.bad, toInt(v["both_empty"]), toInt(v["blocked"]))
	}
	r.detail = []string{fmt.Sprintf("Parity slice: %d requests (%s); %d clusters, %d claimed by no registry case.", r.total, split, toInt(p["clusters"]), unclaimed)}
	return r
}

func perfRows(s Summary) []row {
	var out []row
	if p := s.PerfDaily; p != nil {
		dist, _ := p["distribution"].(map[string]any)
		for _, mode := range []string{"cold", "warm"} {
			d, ok := dist[mode].(map[string]any)
			if !ok {
				continue
			}
			rows, green, above := toInt(d["rows"]), toInt(d["green"]), toInt(d["above"])
			if _, recorded := d["green"]; !recorded {
				green = toInt(d["below"]) + toInt(d["close"]) // green is below Loki or close to it
			}
			r := row{key: "perf:" + mode, area: "Daily perf smoke, " + mode, known: true, split: true, total: rows, good: green, bad: rows - green,
				target: fmt.Sprintf("%d%% green, 0 above 2x", greenTargetPercent)}
			more := (greenTargetPercent*rows+99)/100 - green
			if more < 0 {
				more = 0
			}
			r.met = more == 0 && above == 0
			r.needed = greenNeeded(more, above)
			r.detail = []string{fmt.Sprintf("Daily perf smoke, %s: %d rows, below Loki %d, close %d, 1-2x %d, above 2x %d (green = below + close).",
				mode, rows, toInt(d["below"]), toInt(d["close"]), toInt(d["between"]), above)}
			out = append(out, r)
		}
	}
	if w := s.PerfWeekly; w != nil {
		r := row{key: "weekly", area: "Weekly perf, " + w.Date, known: true, split: true, total: w.Pairs, good: w.Faster + w.Par, bad: w.Slower,
			target: fmt.Sprintf("%d%% faster or on par, 0 above 2x", greenTargetPercent)}
		more := (greenTargetPercent*w.Pairs+99)/100 - r.good
		if more < 0 {
			more = 0
		}
		r.met = more == 0 && w.Over2xRows == 0
		r.needed = greenNeeded(more, w.Over2xRows)
		r.detail = []string{fmt.Sprintf("Weekly perf (measured %s): %d shape x range pairs, faster %d, on par %d, slower %d; %d on the above-2x list.",
			w.Date, w.Pairs, w.Faster, w.Par, w.Slower, w.Over2xRows)}
		out = append(out, r)
	}
	return out
}

func greenNeeded(more, above int) string {
	switch {
	case more == 0 && above == 0:
		return "none"
	case more == 0:
		return fmt.Sprintf("target met; %d above 2x to fix", above)
	}
	return fmt.Sprintf("%s more; %d above 2x to fix", plural(more, "green row", "green rows"), above)
}

func reuseOf(s Summary, v string) *CaseReuse {
	c, _ := s.Cases[v].(map[string]any)
	switch r := c["reuse"].(type) {
	case nil:
		return nil
	case *CaseReuse:
		return r
	default:
		raw, err := json.Marshal(r)
		var out CaseReuse
		if err != nil || json.Unmarshal(raw, &out) != nil {
			return nil
		}
		return &out
	}
}

func cacheRow(s Summary, v string) row {
	u := reuseOf(s, v)
	r := row{key: "cache:" + v, area: "Cache reuse, VictoriaLogs " + v, known: u != nil, split: true, target: "100% reused, 0 VictoriaLogs calls on repeats"}
	if u == nil {
		return r
	}
	r.good, r.bad = u.Counts["reused"], u.Counts["cache_not_reused"]
	r.total = r.good + r.bad
	r.met = r.bad == 0 && u.RepeatCalls == 0
	r.needed = "none"
	var need []string
	if r.bad > 0 {
		need = append(need, plural(r.bad, "repeat", "repeats")+" to reuse")
	}
	if u.RepeatCalls > 0 {
		need = append(need, fmt.Sprintf("VictoriaLogs calls on repeats %g, target 0", u.RepeatCalls))
	}
	if len(need) > 0 {
		r.needed = strings.Join(need, "; ")
	}
	detail := fmt.Sprintf("Cache reuse, VictoriaLogs %s: %d repeats reused of %d measured, %d not measured; VictoriaLogs calls %g cold, %g on the repeats.",
		v, r.good, r.total, u.Counts["not_measured"], u.ColdVLCalls, u.RepeatCalls)
	// What the repeat count measures: per request (the proxy's own count) or by the process-wide metrics.
	switch {
	case u.CallsFromHeader > 0 || u.CallsFromMetrics > 0:
		detail += fmt.Sprintf(" Counted per request for %d repeats and by process-wide metrics for %d; the proxy's background calls around the repeats (%g) are not in the count.",
			u.CallsFromHeader, u.CallsFromMetrics, u.Background)
	default:
		detail += " Counted by process-wide metrics (no per-request count in this run)."
	}
	if p := s.PerfDaily; p != nil && v == s.VictoriaLogs[len(s.VictoriaLogs)-1] {
		if pr, ok := p["reuse"].(map[string]any); ok {
			detail += fmt.Sprintf(" Perf smoke: %v rows, VictoriaLogs calls %v cold and %v on the repeats, %d rows lost reuse.",
				pr["rows"], pr["cold_vl_calls"], pr["repeat_vl_calls"], lenOf(p["reuse_lost"]))
		}
	}
	r.detail = []string{detail}
	return r
}

func workflowRow(s Summary) row {
	w := s.Workflows
	r := row{key: "workflows", area: "Scheduled workflows", known: w != nil, split: true, target: "all ok"}
	if w == nil {
		return r
	}
	r.total, r.bad = w.Scheduled, len(w.Problems)
	r.good = r.total - r.bad
	if r.good < 0 {
		r.good = 0
	}
	r.met = r.bad == 0
	r.needed = "none"
	if r.bad > 0 {
		r.needed = plural(r.bad, "workflow to fix", "workflows to fix") + ": " + strings.Join(problemNames(w.Problems), ", ")
	}
	// A workflow added too recently to have run on its schedule is not a problem, and not one of the counted items.
	if len(w.Pending) > 0 {
		r.detail = []string{fmt.Sprintf("Scheduled workflows: %d pending their first scheduled run, not counted and not red: %s.", len(w.Pending), strings.Join(problemNames(w.Pending), ", "))}
	}
	return r
}

func problemNames(problems []map[string]any) []string {
	var names []string
	for _, p := range problems {
		names = append(names, fmt.Sprint(p["workflow"]))
	}
	sort.Strings(names)
	return names
}

// Executive is the executive summary's Markdown: the headline, where we stand, the Loki and VictoriaLogs sides, what
// changed since the previous report and what is wrong now. prev is nil when there is no earlier report.
func Executive(s Summary, prev *Previous) string {
	cur := rows(s)
	var prevRows map[string]row
	if prev != nil && prev.Summary != nil {
		prevRows = map[string]row{}
		for _, r := range rows(*prev.Summary) {
			prevRows[r.key] = r
		}
	}
	var b strings.Builder
	b.WriteString("### Executive summary\n\n")
	b.WriteString(headline(s, cur))
	b.WriteString("\n#### Where we stand\n\n| Area | Good | Not good | % good | Target | Still needed | Change since " + sinceLabel(prev) + " |\n|---|---|---|---|---|---|---|\n")
	for _, r := range cur {
		good, bad := r.cells()
		if !r.known {
			fmt.Fprintf(&b, "| %s | n/a | n/a | n/a | %s | not verified today | %s |\n", r.area, r.target, changeCell(r, prevRows, prev))
			continue
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", r.area, good, bad, r.pct(), r.target, r.needed, changeCell(r, prevRows, prev))
	}
	b.WriteString("\nA check is one area of this table at its target. Counted items are the units of a row: a registry case, a proven live verdict, a recorded error answer, a sweep query, a parity request, a perf row, a scheduled workflow. " +
		"Coverage rows show how much of the case set is proven live, or by a named passing test, and are not counted again; a case not proven is a number to work down, red only when a proven case regressed. Not good counts differences only; accepted and upstream-blocked cases are shown apart.\n\n")
	for _, r := range cur {
		if r.known {
			for _, d := range r.detail {
				fmt.Fprintf(&b, "- %s\n", d)
			}
		}
	}
	b.WriteString("\n")
	b.WriteString(sides(s))
	b.WriteString(changes(s, prev))
	b.WriteString(wrongNow(s))
	return b.String()
}

func sinceLabel(prev *Previous) string {
	if prev == nil || prev.Summary == nil {
		return "the previous report"
	}
	return prev.Summary.Date
}

func headline(s Summary, cur []row) string {
	state := "green"
	if s.Red {
		state = "red"
	}
	good, total, areas, met := 0, 0, 0, 0
	for _, r := range cur {
		if !r.known || !r.split || r.total == 0 {
			continue
		}
		areas++
		if r.met {
			met++
		}
		if !r.skip {
			good += r.good
			total += r.total
		}
	}
	return fmt.Sprintf("**%s: %d of %d checks (areas) at target; %d of %d counted items ok (%s); %s.**\n", strings.ToUpper(state[:1])+state[1:], met, areas, good, total,
		pct(good, total), plural(len(s.Reasons), "red reason", "red reasons"))
}

// changeCell is one row's change against the previous report.
func changeCell(r row, prevRows map[string]row, prev *Previous) string {
	if prev == nil || prev.Summary == nil {
		return "-"
	}
	p, ok := prevRows[r.key]
	if !ok || !p.known {
		if r.known {
			return "new"
		}
		return "-"
	}
	if !r.known {
		return "not verified today"
	}
	var parts []string
	if r.split && p.split {
		if r.good != p.good {
			parts = append(parts, fmt.Sprintf("good %+d", r.good-p.good))
		}
		if r.bad != p.bad {
			parts = append(parts, fmt.Sprintf("not good %+d", r.bad-p.bad))
		}
		if r.total != p.total {
			parts = append(parts, fmt.Sprintf("total %d to %d", p.total, r.total))
		}
		if d := tenths(r.good, r.total) - tenths(p.good, p.total); d != 0 && r.total > 0 && p.total > 0 {
			parts = append(parts, signedTenths(d)+" pt")
		}
	} else if r.total != p.total {
		parts = append(parts, fmt.Sprintf("total %d to %d", p.total, r.total))
	}
	if len(parts) == 0 {
		return "no change"
	}
	return strings.Join(parts, ", ")
}

// sideBlock is one side's lines and what it still needs to reach 100%.
type sideBlock struct {
	b    strings.Builder
	need []string
}

func (x *sideBlock) line(format string, args ...any) { fmt.Fprintf(&x.b, "- "+format+"\n", args...) }

func (x *sideBlock) sub(format string, args ...any) { fmt.Fprintf(&x.b, "  - "+format+"\n", args...) }

func (x *sideBlock) needs(n int, text string) {
	if n > 0 {
		x.need = append(x.need, text)
	}
}

func (x *sideBlock) String() string {
	return x.b.String() + "- To reach 100%: " + joinOrNone(x.need) + ".\n\n"
}

// sides states the Loki side and the VictoriaLogs side of the differences, each with what reaches 100%.
func sides(s Summary) string {
	lokiKinds, vlKinds := splitKinds(s)
	return "#### Loki side vs VictoriaLogs side\n\n**Loki (client-facing compatibility)**\n\n" + lokiSide(s, lokiKinds).String() +
		"**VictoriaLogs (backend side)**\n\n" + vlSide(s, vlKinds).String()
}

func lokiSide(s Summary, kinds []string) *sideBlock {
	x := &sideBlock{}
	if reg := registryRow(s); reg.known {
		x.line("Open registry cases: %d of %d (%s).", reg.bad, reg.total, pct(reg.bad, reg.total))
		x.needs(reg.bad, "close "+plural(reg.bad, "open case", "open cases"))
	}
	liveDiff := 0
	for _, v := range s.VictoriaLogs {
		if r := verdictRows(s, v)[0]; r.known {
			x.line("Live differs, VictoriaLogs %s: %d of %d proven (%s); proven is %d of %d cases.", v, r.bad, r.total, pct(r.bad, r.total), r.total, verdictTotal(s, v))
		}
		if d := caseCount(s, v, "differs"); d > liveDiff {
			liveDiff = d
		}
	}
	if len(s.VictoriaLogs) > 0 {
		first := s.VictoriaLogs[0]
		x.needs(liveDiff, "fix "+plural(liveDiff, "differing proven case", "differing proven cases"))
		if unproven := verdictTotal(s, first) - provenOf(s, first); verdictTotal(s, first) > 0 {
			x.needs(unproven, "prove "+plural(unproven, "more case", "more cases")+" live")
		}
	}
	if er := errorsRow(s); er.known {
		x.line("Error answers differing from Loki: %d of %d recorded (%s).", er.bad, er.total, pct(er.bad, er.total))
		x.needs(er.bad, "align "+plural(er.bad, "error answer", "error answers"))
	}
	if s.Sweep != nil {
		sweepLokiLines(x, s.Sweep, kinds)
	}
	return x
}

// sweepLokiLines lists the sweep's differences with Loki: each kind, and the tree facts as one line.
func sweepLokiLines(x *sideBlock, sw *SweepPart, kinds []string) {
	total, tree, treeKinds := 0, 0, 0
	for _, k := range kinds {
		total += sw.Differences[k]
	}
	x.line("Grammar sweep differences with Loki: %d of %d queries (%s).", total, sw.Queries, pct(total, sw.Queries))
	for _, k := range kinds {
		if strings.HasPrefix(k, kindTreeFact) {
			tree += sw.Differences[k]
			treeKinds++
			continue
		}
		x.sub("%s: %d (%s)", k, sw.Differences[k], pct(sw.Differences[k], sw.Queries))
	}
	if treeKinds > 0 {
		x.sub("%s (%s): %d (%s)", kindTreeFact, plural(treeKinds, "kind", "kinds"), tree, pct(tree, sw.Queries))
	}
	x.needs(total, "remove "+plural(total, "sweep difference", "sweep differences"))
}

func vlSide(s Summary, kinds []string) *sideBlock {
	x := &sideBlock{}
	if sw := s.Sweep; sw != nil {
		total := 0
		for _, k := range kinds {
			n := sw.Differences[k]
			total += n
			x.line("Grammar sweep, %s: %d of %d queries (%s).", k, n, sw.Queries, pct(n, sw.Queries))
		}
		x.needs(total, "fix "+plural(total, "sweep query", "sweep queries")+" whose text VictoriaLogs refuses or the translation fails")
	}
	if reg := registryRow(s); reg.known {
		up := s.Registry.ByStatus["upstream"]
		x.line("Upstream-blocked cases: %d of %d (%s).", up, reg.total, pct(up, reg.total))
		x.needs(up, plural(up, "case waits", "cases wait")+" on a VictoriaLogs fix")
	}
	if len(s.VictoriaLogs) >= 2 {
		cases, d := verdictTotal(s, s.VictoriaLogs[0]), versionDifferences(s)
		x.line("Cases whose live verdict differs between %s and %s: %d of %d (%s).", s.VictoriaLogs[0], s.VictoriaLogs[len(s.VictoriaLogs)-1], d, cases, pct(d, cases))
		x.needs(d, plural(d, "version-dependent verdict", "version-dependent verdicts")+" to explain")
	}
	perfSideLines(x, s)
	return x
}

func perfSideLines(x *sideBlock, s Summary) {
	daily, smokeRows, weekly := 0, 0, 0
	for _, r := range perfRows(s) {
		if r.key != "weekly" {
			smokeRows += r.total
		}
	}
	if p := s.PerfDaily; p != nil {
		daily = lenOf(p["over_2x"])
		x.line("Daily perf smoke rows above 2x Loki: %d of %d (%s).", daily, smokeRows, pct(daily, smokeRows))
	}
	if w := s.PerfWeekly; w != nil {
		weekly = w.Over2xRows
		x.line("Weekly perf pairs above 2x Loki: %d on the list, of %d pairs (%s).", weekly, w.Pairs, pct(weekly, w.Pairs))
	}
	x.needs(daily+weekly, fmt.Sprintf("bring %s (%d daily, %d weekly) under 2x Loki", plural(daily+weekly, "perf row", "perf rows"), daily, weekly))
}

func joinOrNone(list []string) string {
	if len(list) == 0 {
		return "nothing left in the numbers recorded today"
	}
	return strings.Join(list, "; ")
}

func verdictTotal(s Summary, v string) int {
	total := 0
	for _, k := range verdictKeys {
		total += caseCount(s, v, k)
	}
	return total
}

func provenOf(s Summary, v string) int {
	return caseCount(s, v, "matches") + caseCount(s, v, "differs")
}

// splitKinds sorts the sweep's difference kinds into the two sides: the VictoriaLogs rows are the ones where
// VictoriaLogs refuses the text the translator sends, every other row is a difference with Loki.
func splitKinds(s Summary) (loki, vl []string) {
	if s.Sweep == nil {
		return nil, nil
	}
	kinds := make([]string, 0, len(s.Sweep.Differences))
	for k := range s.Sweep.Differences {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		if k == kindVLRejects || k == kindTranslateFails {
			vl = append(vl, k)
		} else {
			loki = append(loki, k)
		}
	}
	// A VictoriaLogs row with no queries is still shown.
	for _, k := range []string{kindVLRejects, kindTranslateFails} {
		if _, ok := s.Sweep.Differences[k]; !ok {
			vl = append(vl, k)
		}
	}
	sort.Strings(vl)
	return loki, vl
}

// versionDifferences counts the cases whose verdict is not the same on the oldest and newest VictoriaLogs version
// of the run: from the per-case file the run wrote, else from the verdict counts.
func versionDifferences(s Summary) int {
	if c := s.detail; c != nil && len(c.Versions) >= 2 {
		first, last := c.Verdicts[c.Versions[0]], c.Verdicts[c.Versions[len(c.Versions)-1]]
		n := 0
		for id, v := range first {
			if last[id] != v {
				n++
			}
		}
		return n
	}
	// Without per-case verdicts the count of cases that moved is at least the difference of the verdict counts.
	first, last := s.VictoriaLogs[0], s.VictoriaLogs[len(s.VictoriaLogs)-1]
	moved := 0
	for _, k := range verdictKeys {
		if d := caseCount(s, first, k) - caseCount(s, last, k); d > 0 {
			moved += d
		}
	}
	return moved
}

// metric is one number of the report with the name the changes list shows.
type metric struct {
	name  string
	value float64
}

func numberText(v float64) string { return fmt.Sprintf("%g", v) }

// metricList collects the report's numbers in order.
type metricList []metric

func (m *metricList) add(name string, v float64) { *m = append(*m, metric{name, v}) }

func (m *metricList) addInt(name string, v int) { m.add(name, float64(v)) }

// metrics lists every number of the summary in the report's order. The kinds of the other day's registry statuses,
// impacts and sweep rows are listed with 0 when this day holds none: a row that went to zero is a move.
func metrics(s Summary, other *Summary) []metric {
	var m metricList
	m.registry(s, other)
	m.cases(s)
	m.errors(s)
	m.sweep(s, other)
	m.parity(s)
	m.perf(s)
	if w := s.Workflows; w != nil {
		m.addInt("Scheduled workflows: checked", w.Scheduled)
		m.addInt("Scheduled workflows: problems", len(w.Problems))
	}
	return m
}

// withKinds is the counts with every key of the other day's counts present, 0 where this day has none.
func withKinds(now, other map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range now {
		out[k] = v
	}
	for k := range other {
		out[k] += 0
	}
	return out
}

func (m *metricList) registry(s Summary, other *Summary) {
	if s.Registry.Cases == 0 {
		return
	}
	m.addInt("Registry: cases", s.Registry.Cases)
	var oStatus, oImpact map[string]int
	if other != nil && other.Registry.Cases > 0 {
		oStatus, oImpact = other.Registry.ByStatus, other.Registry.OpenByImpact
	}
	statuses, impacts := withKinds(s.Registry.ByStatus, oStatus), withKinds(s.Registry.OpenByImpact, oImpact)
	for _, k := range orderedKeys(statuses, statusOrder) {
		m.addInt("Registry: "+k, statuses[k])
	}
	for _, k := range orderedKeys(impacts, impactOrder) {
		m.addInt("Registry: open, "+k, impacts[k])
	}
}

func (m *metricList) cases(s Summary) {
	for _, v := range s.VictoriaLogs {
		if verdictTotal(s, v) > 0 {
			for _, k := range verdictKeys {
				m.addInt(fmt.Sprintf("Verdicts %s: %s", v, strings.ReplaceAll(k, "_", " ")), caseCount(s, v, k))
			}
			for _, k := range []string{"regressions", "proposed_close", "behaviour_changed"} {
				m.addInt(fmt.Sprintf("Verdicts %s: %s", v, strings.ReplaceAll(k, "_", " ")), caseCount(s, v, k))
			}
		}
		if u := reuseOf(s, v); u != nil {
			m.addInt("Cache reuse "+v+": reused", u.Counts["reused"])
			m.addInt("Cache reuse "+v+": not reused", u.Counts["cache_not_reused"])
			m.addInt("Cache reuse "+v+": not measured", u.Counts["not_measured"])
			m.add("Cache reuse "+v+": VictoriaLogs calls cold", u.ColdVLCalls)
			m.add("Cache reuse "+v+": VictoriaLogs calls on repeats", u.RepeatCalls)
		}
		if c := s.Coverage[v]; c != nil {
			m.addInt("Coverage "+v+": proven live", c.ProvenLive)
			m.addInt("Coverage "+v+": proven by a test", c.ProvenByTest)
			m.addInt("Coverage "+v+": not proven", c.NotProven)
		}
	}
}

func (m *metricList) errors(s Summary) {
	e := s.Errors
	if e == nil {
		return
	}
	m.addInt("Error answers: comparable", e.Recorded.ComparableRows)
	m.addInt("Error answers: matching", e.Recorded.MatchingRows)
	m.addInt("Error answers: known differences", e.Recorded.KnownDifferences)
	m.addInt("Error answers: known leaks", e.Recorded.KnownLeaks)
	if e.Live != nil {
		m.addInt("Error answers: live rows asked", e.Live.Rows)
		m.addInt("Error answers: live rows changed", e.Live.Changed)
	}
}

func (m *metricList) sweep(s Summary, other *Summary) {
	sw := s.Sweep
	if sw == nil {
		return
	}
	m.addInt("Grammar sweep: queries", sw.Queries)
	var o map[string]int
	if other != nil && other.Sweep != nil {
		o = other.Sweep.Differences
	}
	kinds := withKinds(sw.Differences, o)
	for _, k := range orderedKeys(kinds, nil) {
		m.addInt("Grammar sweep: "+k, kinds[k])
	}
}

func (m *metricList) parity(s Summary) {
	p := s.Parity
	if p == nil {
		return
	}
	m.addInt("Parity slice: requests", toInt(p["requests"]))
	m.addInt("Parity slice: clusters", toInt(p["clusters"]))
	m.addInt("Parity slice: unclaimed clusters", toInt(p["unclaimed"]))
	if v, ok := p["verdicts"].(map[string]any); ok {
		for _, k := range []string{"same", "differ", "both_empty", "blocked"} {
			m.addInt("Parity slice: "+strings.ReplaceAll(k, "_", " "), toInt(v[k]))
		}
	}
}

func (m *metricList) perf(s Summary) {
	if p := s.PerfDaily; p != nil {
		m.addInt("Daily perf smoke: rows", toInt(p["rows"]))
		m.addInt("Daily perf smoke: comparable timings", toInt(p["comparable"]))
		m.addInt("Daily perf smoke: above 2x", lenOf(p["over_2x"]))
		m.addInt("Daily perf smoke: ratchet rows lost", lenOf(p["ratchet_lost"]))
		dist, _ := p["distribution"].(map[string]any)
		for _, mode := range []string{"cold", "warm"} {
			if d, ok := dist[mode].(map[string]any); ok {
				for _, k := range []string{"rows", "below", "close", "between", "above", "green"} {
					m.addInt(fmt.Sprintf("Daily perf smoke %s: %s", mode, k), toInt(d[k]))
				}
			}
		}
	}
	if w := s.PerfWeekly; w != nil {
		m.addInt("Weekly perf: pairs", w.Pairs)
		m.addInt("Weekly perf: faster", w.Faster)
		m.addInt("Weekly perf: on par", w.Par)
		m.addInt("Weekly perf: slower", w.Slower)
		m.addInt("Weekly perf: above-2x list", w.Over2xRows)
	}
}

// itemList is one itemised list of "What changed".
type itemList struct {
	title string
	items []string
}

// changeSet collects the lists and the "not compared" notes.
type changeSet struct {
	lists []itemList
	notes []string
}

func (c *changeSet) add(title string, items []string) {
	if len(items) > 0 {
		c.lists = append(c.lists, itemList{title, items})
	}
}

func (c *changeSet) note(text string) { c.notes = append(c.notes, text) }

// changes is "What changed since <date>".
func changes(s Summary, prev *Previous) string {
	var b strings.Builder
	if prev == nil || prev.Summary == nil {
		b.WriteString("#### What changed\n\nNo earlier report to compare with.\n\n")
		return b.String()
	}
	p := prev.Summary
	fmt.Fprintf(&b, "#### What changed since %s\n\n", p.Date)
	if p.Commit != s.Commit {
		fmt.Fprintf(&b, "Main `%s` (was `%s`).\n\n", short(s.Commit), short(p.Commit))
	}
	var c changeSet
	b.WriteString(movedNumbers(s, p, &c))
	added, gone, changed := diffReasons(p.Reasons, s.Reasons)
	c.add("New red items", added)
	c.add("Red items resolved", gone)
	c.add("Red items that changed", changed)
	caseChanges(&c, s, prev)
	measurementChanges(&c, s, p)
	registryChanges(&c, s, prev)
	perfChanges(&c, s, p)
	parityChanges(&c, s, p)
	if p.Workflows != nil && s.Workflows != nil {
		n, g := workflowMoves(p.Workflows.Problems, s.Workflows.Problems)
		c.add("Scheduled workflows newly failing", n)
		c.add("Scheduled workflows ok again", g)
	}
	for _, l := range c.lists {
		fmt.Fprintf(&b, "**%s** (%d)\n\n", l.title, len(l.items))
		for i, item := range l.items {
			if i == listCap {
				fmt.Fprintf(&b, "- ... %d more\n", len(l.items)-listCap)
				break
			}
			fmt.Fprintf(&b, "- %s\n", item)
		}
		b.WriteString("\n")
	}
	if len(c.notes) > 0 {
		fmt.Fprintf(&b, "Not compared: %s.\n\n", strings.Join(c.notes, "; "))
	}
	return b.String()
}

// measurementChanges says when a number moved because it is measured another way: the repeats' VictoriaLogs calls were
// counted by process-wide metrics (background work included) and are counted per request from the day the runs carry
// the proxy's own count, so a drop in that row on that day is not a cache improvement; likewise coverage by a named
// test, new with the wiring.
func measurementChanges(c *changeSet, s Summary, p *Summary) {
	var items []string
	for _, v := range s.VictoriaLogs {
		now, before := reuseOf(s, v), reuseOf(*p, v)
		if now != nil && before != nil && now.CallsFromHeader > 0 && before.CallsFromHeader == 0 {
			items = append(items, fmt.Sprintf("Cache reuse %s: VictoriaLogs calls on repeats are counted per request from today (before: process-wide metrics, background calls included); a drop is not a cache improvement", v))
		}
		if s.Coverage[v] != nil && p.Coverage[v] == nil {
			items = append(items, fmt.Sprintf("Coverage %s: counts cases proven by a named passing test from today (before: live only)", v))
		}
	}
	c.add("Measured another way from today", items)
}

// movedNumbers is the table of every number that moved, old to new.
func movedNumbers(s Summary, p *Summary, c *changeSet) string {
	before := map[string]float64{}
	for _, m := range metrics(*p, &s) {
		before[m.name] = m.value
	}
	var moved [][3]string
	unseen := 0
	for _, m := range metrics(s, p) {
		old, ok := before[m.name]
		switch {
		case !ok:
			unseen++
		case old != m.value:
			moved = append(moved, [3]string{m.name, numberText(old), numberText(m.value)})
		}
	}
	if unseen > 0 {
		c.note(plural(unseen, "number", "numbers") + " the earlier summary did not record")
	}
	if len(moved) == 0 {
		return "No number moved.\n\n"
	}
	var b strings.Builder
	b.WriteString("| Number | Before | Now |\n|---|---|---|\n")
	for i, m := range moved {
		if i == 40 {
			fmt.Fprintf(&b, "| ... %d more moved numbers in summary.json | | |\n", len(moved)-40)
			break
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", m[0], m[1], m[2])
	}
	b.WriteString("\n")
	return b.String()
}

// caseChanges lists the per-case verdict moves and the flags that came or went, from the two cases.json files.
func caseChanges(c *changeSet, s Summary, prev *Previous) {
	if prev.Cases == nil || s.detail == nil {
		c.note("per-case verdicts and flags (no per-case file in the earlier report, or none today)")
		return
	}
	c.add("Cases whose verdict changed", verdictMoves(s.detail, prev.Cases))
	for _, f := range []struct{ flag, now, gone string }{
		{"regression", "New regressions", "Regressions resolved"},
		{"proposed_close", "New proposed closes", "No longer proposed for close"},
		{"behaviour_changed", "Accepted behaviour newly changed", "Accepted behaviour back as accepted"},
	} {
		n, g := flagMoves(s.detail, prev.Cases, f.flag)
		c.add(f.now, n)
		c.add(f.gone, g)
	}
}

func registryChanges(c *changeSet, s Summary, prev *Previous) {
	// The summary records every case's status; cases.json lists only the cases the verifier ran, so a status read from it
	// can move but cannot tell a new case from one it never listed.
	prevStatus, complete := prev.Summary.Registry.CaseStatus, true
	if len(prevStatus) == 0 && prev.Cases != nil {
		prevStatus, complete = prev.Cases.GapStatus, false
	}
	if len(prevStatus) == 0 || len(s.Registry.CaseStatus) == 0 {
		c.note("registry status moves (the earlier report recorded no per-case status)")
		return
	}
	c.add("Registry status moves", statusMoves(prevStatus, s.Registry.CaseStatus, complete))
}

func perfChanges(c *changeSet, s Summary, p *Summary) {
	if s.PerfDaily == nil {
		return
	}
	if _, ok := p.PerfDaily["over_2x"]; !ok {
		c.note("perf rows above 2x (the earlier report recorded none)")
		return
	}
	n, g := perfMoves(p.PerfDaily["over_2x"], s.PerfDaily["over_2x"])
	c.add("Perf rows newly above 2x Loki", n)
	c.add("Perf rows no longer above 2x Loki", g)
}

func parityChanges(c *changeSet, s Summary, p *Summary) {
	if s.Parity == nil {
		return
	}
	_, had := p.Parity["unclaimed_ids"]
	_, has := s.Parity["unclaimed_ids"]
	if !had || !has {
		c.note("unclaimed parity clusters (the earlier report recorded no cluster ids)")
		return
	}
	n, g := setMoves(strs(p.Parity["unclaimed_ids"]), strs(s.Parity["unclaimed_ids"]))
	c.add("New unclaimed parity clusters", n)
	c.add("Parity clusters now claimed or gone", g)
}

// reasonNumber is a whole token that is a count or a date: "2", "17)", "2026-11-14". A case id (a/4) or a version (v1.53.0)
// is not one, so two regressions are two items, not one that changed.
var reasonNumber = regexp.MustCompile(`^\(?[0-9]+[,.:;)]*$|^[0-9]{4}-[0-9]{2}-[0-9]{2}[,.:;)]*$`)

// reasonKey is a red reason with its counts and dates replaced, so a count that moved is one changed item, not a new
// one and a resolved one.
func reasonKey(r string) string {
	words := strings.Fields(r)
	for i, w := range words {
		if reasonNumber.MatchString(w) {
			words[i] = "#"
		}
	}
	return strings.Join(words, " ")
}

func diffReasons(before, after []string) (added, gone, changed []string) {
	old := map[string]string{}
	for _, r := range before {
		old[reasonKey(r)] = r
	}
	now := map[string]bool{}
	for _, r := range after {
		k := reasonKey(r)
		now[k] = true
		switch o, ok := old[k]; {
		case !ok:
			added = append(added, r)
		case o != r:
			changed = append(changed, fmt.Sprintf("%s (was: %s)", r, o))
		}
	}
	for _, r := range before {
		if !now[reasonKey(r)] {
			gone = append(gone, r)
		}
	}
	return added, gone, changed
}

// verdictMoves lists the cases whose verdict moved, one line per case and move with the versions it holds on.
func verdictMoves(cur, prev *CasesFile) []string {
	type move struct{ id, from, to string }
	versions := map[move][]string{}
	var order []move
	for _, v := range cur.Versions {
		before, ok := prev.Verdicts[v]
		if !ok {
			continue
		}
		for id, now := range cur.Verdicts[v] {
			if old, ok := before[id]; ok && old != now {
				m := move{id, old, now}
				if _, seen := versions[m]; !seen {
					order = append(order, m)
				}
				versions[m] = append(versions[m], v)
			}
		}
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].id != order[j].id {
			return order[i].id < order[j].id
		}
		return order[i].from+order[i].to < order[j].from+order[j].to
	})
	var out []string
	for _, m := range order {
		out = append(out, fmt.Sprintf("%s: %s to %s (%s)", m.id, m.from, m.to, strings.Join(versions[m], ", ")))
	}
	return out
}

// flagMoves lists the cases that carry a flag now and did not, and the ones that carried it and do not, with versions.
func flagMoves(cur, prev *CasesFile, flag string) (added, gone []string) {
	collect := func(f *CasesFile) map[string][]string {
		m := map[string][]string{}
		for _, v := range f.Versions {
			for _, id := range f.Flags[v][flag] {
				m[id] = append(m[id], v)
			}
		}
		return m
	}
	now, was := collect(cur), collect(prev)
	for id, vs := range now {
		if old := subtract(vs, was[id]); len(old) > 0 {
			added = append(added, fmt.Sprintf("%s (%s)", id, strings.Join(old, ", ")))
		}
	}
	for id, vs := range was {
		if left := subtract(vs, now[id]); len(left) > 0 {
			gone = append(gone, fmt.Sprintf("%s (%s)", id, strings.Join(left, ", ")))
		}
	}
	sort.Strings(added)
	sort.Strings(gone)
	return added, gone
}

func subtract(a, b []string) []string {
	skip := map[string]bool{}
	for _, x := range b {
		skip[x] = true
	}
	var out []string
	for _, x := range a {
		if !skip[x] {
			out = append(out, x)
		}
	}
	return out
}

func statusMoves(before, after map[string]string, complete bool) []string {
	var out []string
	ids := map[string]bool{}
	for id := range before {
		ids[id] = true
	}
	for id := range after {
		ids[id] = true
	}
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	for _, id := range sorted {
		old, hadOld := before[id]
		now, hasNow := after[id]
		switch {
		case hadOld && hasNow && old != now:
			out = append(out, fmt.Sprintf("%s: %s to %s", id, old, now))
		case !complete:
		case !hadOld:
			out = append(out, fmt.Sprintf("%s: new case, %s", id, now))
		case !hasNow:
			out = append(out, fmt.Sprintf("%s: removed (was %s)", id, old))
		}
	}
	return out
}

func strs(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, fmt.Sprint(e))
		}
		return out
	}
	return nil
}

func setMoves(before, after []string) (added, gone []string) {
	added, gone = subtract(after, before), subtract(before, after)
	sort.Strings(added)
	sort.Strings(gone)
	return added, gone
}

// perfRowKey names one perf smoke row; text describes it with its timings.
func perfRowKey(m map[string]any) string {
	return fmt.Sprintf("%v / %v, %v, %v, %v", m["set"], m["shape"], m["range"], m["mode"], m["profile"])
}

func perfRowText(m map[string]any) string {
	loki, _ := m["loki_s"].(float64)
	proxy, _ := m["proxy_s"].(float64)
	ratio := ""
	if loki > 0 {
		ratio = fmt.Sprintf(", %.1fx", proxy/loki)
	}
	return fmt.Sprintf("%s: Loki %g s, proxy %g s%s", perfRowKey(m), loki, proxy, ratio)
}

func perfMoves(before, after any) (added, gone []string) {
	index := func(v any) map[string]map[string]any {
		out := map[string]map[string]any{}
		list, _ := v.([]any)
		for _, e := range list {
			if m, ok := e.(map[string]any); ok {
				out[perfRowKey(m)] = m
			}
		}
		return out
	}
	old, now := index(before), index(after)
	for k, m := range now {
		if _, ok := old[k]; !ok {
			added = append(added, perfRowText(m))
		}
	}
	for k, m := range old {
		if _, ok := now[k]; !ok {
			gone = append(gone, perfRowText(m))
		}
	}
	sort.Strings(added)
	sort.Strings(gone)
	return added, gone
}

func workflowMoves(before, after []map[string]any) (added, gone []string) {
	old := map[string]string{}
	for _, p := range before {
		old[fmt.Sprint(p["workflow"])] = fmt.Sprint(p["problem"])
	}
	now := map[string]string{}
	for _, p := range after {
		now[fmt.Sprint(p["workflow"])] = fmt.Sprint(p["problem"])
	}
	for w, problem := range now {
		if _, ok := old[w]; !ok {
			added = append(added, fmt.Sprintf("%s: %s", w, problem))
		}
	}
	for w := range old {
		if _, ok := now[w]; !ok {
			gone = append(gone, w)
		}
	}
	sort.Strings(added)
	sort.Strings(gone)
	return added, gone
}

// wrongRules classify a red reason: the item it belongs to, whether its first number is a count, and what to do.
var wrongRules = []struct {
	contains, item, todo string
	counted              bool
}{
	{"scheduled workflow ", "Scheduled workflow not ok", "open the workflow's last scheduled run, fix the failure or the missing schedule, run it again", false},
	{"the unit replay against the recorded Loki answers failed", "Error answers replay fails", "run TestLokiErrorAnswersReplay, fix the proxy or the recording", false},
	{"error answers: the stack's Loki answers", "Error answers drifted from the recording", "run go run ./test/errorrecord -check, review the changed rows, re-record those Loki changed", true},
	{"the live re-verification of the sweep", "Sweep live check red", "open the query-corpus run, fix what it reports", false},
	{"no run on main recorded", "Sweep live check missing", "run query-corpus.yaml on main", false},
	{"claimed by no registry case", "Parity cluster claimed by no case", "add a registry case with a gap block that claims each cluster", true},
	{"the differential run could not speak", "Parity run unusable", "open the parity job's artifact, fix the run", false},
	{"above 2x Loki", "Perf smoke rows above 2x Loki", "bring each row under 2x Loki (closest to Loki first)", true},
	{"of the weekly ratchet lost", "Perf ratchet rows lost", "restore each row's speed or ratchet, never relax the ratchet", true},
	{"reuse the proxy's cache less", "Perf rows lost cache reuse", "restore the reuse of each row", true},
	{"differs from Loki on VictoriaLogs", "Fixed case differs from Loki", "fix the regression, or reopen the case with the owner's decision", false},
	{"proxy built from", "Stale proxy build measured", "re-run with the proxy built from the measured commit", false},
	{"the run produced no result", "Input missing today", "open the job that should have produced it, fix and re-run", false},
}

func firstInt(s string) int {
	n, in := 0, false
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n, in = n*10+int(c-'0'), true
		} else if in {
			break
		}
	}
	return n
}

// wrongNow is "What is wrong now": the red reasons, grouped by item, each with its count and what to do.
func wrongNow(s Summary) string {
	var b strings.Builder
	b.WriteString("#### What is wrong now\n\n")
	if len(s.Reasons) == 0 {
		b.WriteString("Nothing is red.\n\n")
		return b.String()
	}
	type item struct {
		name, todo string
		count      int
		which      []string
	}
	var items []*item
	byName := map[string]*item{}
	for _, reason := range s.Reasons {
		name, todo, counted := "Other red reason", "see the reason in the Red list below", false
		for _, rule := range wrongRules {
			if strings.Contains(reason, rule.contains) {
				name, todo, counted = rule.item, rule.todo, rule.counted
				break
			}
		}
		it := byName[name]
		if it == nil {
			it = &item{name: name, todo: todo}
			byName[name] = it
			items = append(items, it)
		}
		n := 1
		if counted {
			n = firstInt(reason)
		}
		it.count += n
		if w := whichOf(reason); w != "" {
			it.which = append(it.which, w)
		}
	}
	if ids := strs(s.Parity["unclaimed_ids"]); len(ids) > 0 {
		if it := byName["Parity cluster claimed by no case"]; it != nil {
			it.which = ids
		}
	}
	if p := s.PerfDaily; p != nil {
		if it := byName["Perf smoke rows above 2x Loki"]; it != nil {
			it.which = nil
			list, _ := p["over_2x"].([]any)
			for _, e := range list {
				if m, ok := e.(map[string]any); ok {
					it.which = append(it.which, perfRowKey(m))
				}
			}
		}
	}
	b.WriteString("| Item | Count | Which | What to do |\n|---|---|---|---|\n")
	for _, it := range items {
		fmt.Fprintf(&b, "| %s | %d | %s | %s |\n", it.name, it.count, whichText(it.which), it.todo)
	}
	b.WriteString("\n")
	return b.String()
}

// whichOf is the thing a reason names: the workflow of a "scheduled workflow X: ..." reason, the case of "<id> differs".
func whichOf(reason string) string {
	if rest, ok := strings.CutPrefix(reason, "scheduled workflow "); ok {
		name, _, _ := strings.Cut(rest, ":")
		return name
	}
	if id, _, ok := strings.Cut(reason, " differs from Loki on VictoriaLogs "); ok {
		return id
	}
	return ""
}

func whichText(list []string) string {
	if len(list) == 0 {
		return "-"
	}
	const shown = 5
	if len(list) > shown {
		return strings.Join(list[:shown], "; ") + fmt.Sprintf("; ... %d more", len(list)-shown)
	}
	return strings.Join(list, "; ")
}
