package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WeeklyInputs names what the weekly full run produced next to the daily summary.
type WeeklyInputs struct {
	Summary    string // the newest daily summary.json on main
	CasesDir   string // test/caseverify files of the weekly run, one per VictoriaLogs version
	ParityDir  string // parity result.json files of the weekly run, named result-<version>.json
	SweepDump  string // LVP_GRAMMAR_SWEEP_DUMP of the full-size sweep (one JSON object per query of a class)
	FloorPerf  string // smoke.py judge of the full A/B on the oldest supported VictoriaLogs
	PinPerf    string // smoke.py judge of the weekly A/B on the pin (its results), for the reuse
	FleetDir   string // the fleet cache proof: rf<N>.json reports and rf<N>.exit test statuses
	SiteData   string // the weekly run's figures (website/src/data/quality-vs-loki.json)
	Over2x     string // the weekly above-2x list
	ReleaseDry string // the conclusion of the release.yaml dry run
	Date       string
	Commit     string
}

// WeeklyLine is one week's numbers, flat: one key per number, so the trend shows each over the weeks.
type WeeklyLine struct {
	Date    string             `json:"date"`
	Commit  string             `json:"commit"`
	Numbers map[string]float64 `json:"numbers"`
	Notes   map[string]string  `json:"notes,omitempty"`
	// Red is why the week is red against the week before: reuse that dropped, a fleet proof that failed.
	Red []string `json:"red,omitempty"`
}

func (l *WeeklyLine) put(key string, v float64) { l.Numbers[key] = v }

// BuildWeekly gathers the week's numbers; a missing input is a note, not an error, so the trend keeps the week.
func BuildWeekly(in WeeklyInputs) (WeeklyLine, error) {
	l := WeeklyLine{Date: in.Date, Commit: in.Commit, Numbers: map[string]float64{}, Notes: map[string]string{}}
	missing := func(label, path string, err error) error {
		if path == "" || errors.Is(err, os.ErrNotExist) {
			l.Notes[label] = "not measured this week"
			return nil
		}
		return err
	}
	var daily Summary
	if err := readJSON(in.Summary, &daily); err != nil {
		if err := missing("daily summary", in.Summary, err); err != nil {
			return l, err
		}
	} else {
		for k, v := range daily.Registry.ByStatus {
			l.put("registry."+k, float64(v))
		}
		for k, v := range daily.Registry.OpenByImpact {
			l.put("registry.open."+k, float64(v))
		}
		if e := daily.Errors; e != nil {
			l.put("errors.matching", float64(e.Recorded.MatchingRows))
			l.put("errors.comparable", float64(e.Recorded.ComparableRows))
			l.put("errors.known_differences", float64(e.Recorded.KnownDifferences))
			l.put("errors.known_leaks", float64(e.Recorded.KnownLeaks))
		}
		if sw := daily.Sweep; sw != nil {
			for k, v := range sw.Differences {
				l.put("sweep.default."+k, float64(v))
			}
		}
		l.Notes["daily summary"] = daily.Date
	}
	if err := weeklyCases(&l, in.CasesDir); err != nil {
		return l, err
	}
	if err := weeklyParity(&l, in.ParityDir); err != nil {
		return l, err
	}
	if err := weeklySweep(&l, in.SweepDump); err != nil {
		if err := missing("full sweep", in.SweepDump, err); err != nil {
			return l, err
		}
	}
	for _, side := range []struct{ label, key, path string }{{"perf on the oldest VictoriaLogs", "floor", in.FloorPerf},
		{"reuse on the pin", "pin", in.PinPerf}} {
		var v map[string]any
		if err := readJSON(side.path, &v); err != nil {
			if err := missing(side.label, side.path, err); err != nil {
				return l, err
			}
			continue
		}
		weeklyPerf(&l, side.key, v)
	}
	if err := weeklyFleet(&l, in.FleetDir); err != nil {
		return l, err
	}
	s := Summary{}
	if err := s.perfWeekly(Inputs{SiteData: in.SiteData, Over2x: in.Over2x}); err != nil {
		return l, err
	}
	if w := s.PerfWeekly; w != nil {
		l.put("perf.pin.pairs", float64(w.Pairs))
		l.put("perf.pin.faster", float64(w.Faster))
		l.put("perf.pin.par", float64(w.Par))
		l.put("perf.pin.slower", float64(w.Slower))
		l.put("perf.pin.over_2x_rows", float64(w.Over2xRows))
		l.Notes["perf measured"] = w.Date
	} else {
		l.Notes["perf on the pin"] = "not measured this week"
	}
	if in.ReleaseDry == "" {
		l.Notes["release dry run"] = "not run this week"
	} else {
		l.Notes["release dry run"] = in.ReleaseDry
	}
	return l, nil
}

// weeklyPerf puts a smoke.py judge's numbers: rows, above 2x, the distribution and the cache reuse.
func weeklyPerf(l *WeeklyLine, key string, v map[string]any) {
	prefix := "perf." + key + "."
	if key == "floor" {
		l.put(prefix+"rows", num(v["rows"]))
		l.put(prefix+"above_2x", float64(lenOf(v["over_2x"])))
		dist, _ := v["distribution"].(map[string]any)
		for mode, d := range dist {
			m, _ := d.(map[string]any)
			for _, band := range []string{"below", "close", "between", "above"} {
				l.put(prefix+mode+"."+band, num(m[band]))
			}
		}
	}
	if r, ok := v["reuse"].(map[string]any); ok {
		for _, k := range []string{"rows", "cold_vl_calls", "repeat_vl_calls", "repeat_without_vl_calls"} {
			l.put(prefix+"reuse."+k, num(r[k]))
		}
	}
}

// weeklyFleet reads the fleet cache proof of each replication factor: whether its tests passed, and the
// VictoriaLogs calls, peer hits and errors its layers summed.
func weeklyFleet(l *WeeklyLine, dir string) error {
	files, _ := filepath.Glob(filepath.Join(dir, "rf*.json"))
	if dir == "" || len(files) == 0 {
		l.Notes["fleet cache proof"] = "not run this week"
		return nil
	}
	for _, path := range files {
		rf := strings.TrimSuffix(filepath.Base(path), ".json")
		var report struct {
			Summary []struct {
				VLCalls    float64 `json:"vl_calls"`
				PeerHits   float64 `json:"peer_hits"`
				PeerErrors float64 `json:"peer_errors"`
			} `json:"summary"`
		}
		if err := readJSON(path, &report); err != nil {
			return err
		}
		var calls, hits, errs float64
		for _, s := range report.Summary {
			calls, hits, errs = calls+s.VLCalls, hits+s.PeerHits, errs+s.PeerErrors
		}
		prefix := "fleet." + rf + "."
		l.put(prefix+"layers", float64(len(report.Summary)))
		l.put(prefix+"vl_calls", calls)
		l.put(prefix+"peer_hits", hits)
		l.put(prefix+"peer_errors", errs)
		exit, err := os.ReadFile(strings.TrimSuffix(path, ".json") + ".exit")
		passed := err == nil && strings.TrimSpace(string(exit)) == "0"
		if passed {
			l.put(prefix+"passed", 1)
		} else {
			l.put(prefix+"passed", 0)
			l.Red = append(l.Red, fmt.Sprintf("the fleet cache proof (%s) did not pass", rf))
		}
	}
	return nil
}

// judgeReuse adds the reuse that dropped against the week before: more VictoriaLogs calls on the repeats, or
// fewer rows that repeat without calling VictoriaLogs.
func judgeReuse(now *WeeklyLine, before *WeeklyLine) {
	if before == nil {
		return
	}
	for _, key := range []string{"floor", "pin"} {
		p := "perf." + key + ".reuse."
		if a, ok := before.Numbers[p+"repeat_vl_calls"]; ok {
			if b, ok := now.Numbers[p+"repeat_vl_calls"]; ok && b > a {
				now.Red = append(now.Red, fmt.Sprintf("%s: the repeats call VictoriaLogs %s times, %s the week before", key, trimFloat(b), trimFloat(a)))
			}
		}
		if a, ok := before.Numbers[p+"repeat_without_vl_calls"]; ok {
			if b, ok := now.Numbers[p+"repeat_without_vl_calls"]; ok && b < a {
				now.Red = append(now.Red, fmt.Sprintf("%s: %s rows repeat without calling VictoriaLogs, %s the week before", key, trimFloat(b), trimFloat(a)))
			}
		}
	}
}

func num(v any) float64 {
	f, _ := v.(float64)
	return f
}

func weeklyCases(l *WeeklyLine, dir string) error {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if dir == "" || len(files) == 0 {
		l.Notes["per-case verdicts"] = "not measured this week"
		return nil
	}
	for _, path := range files {
		var run CaseRun
		if err := readJSON(path, &run); err != nil {
			return err
		}
		for k, v := range run.Counts {
			l.put("cases."+run.VictoriaLogs+"."+k, float64(v))
		}
		for k, ids := range run.Flags {
			l.put("cases."+run.VictoriaLogs+"."+k, float64(len(ids)))
		}
	}
	return nil
}

func weeklyParity(l *WeeklyLine, dir string) error {
	files, _ := filepath.Glob(filepath.Join(dir, "result-*.json"))
	if dir == "" || len(files) == 0 {
		l.Notes["full parity corpus"] = "not measured this week"
		return nil
	}
	for _, path := range files {
		version := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "result-"), ".json")
		var r map[string]any
		if err := readJSON(path, &r); err != nil {
			return err
		}
		for _, k := range []string{"clusters", "requests", "unclaimed", "problems", "exit"} {
			l.put("parity."+version+"."+k, num(r[k]))
		}
	}
	return nil
}

// weeklySweep counts the full-size sweep's differing queries per kind (the class id's first field).
func weeklySweep(l *WeeklyLine, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	kinds, classes := map[string]float64{}, map[string]bool{}
	for sc.Scan() {
		var row struct {
			Class string `json:"class"`
		}
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		kind, _, _ := strings.Cut(row.Class, " | ")
		kinds[kind]++
		classes[row.Class] = true
	}
	if err := sc.Err(); err != nil {
		return err
	}
	for k, v := range kinds {
		l.put("sweep.full."+k, v)
	}
	l.put("sweep.full.classes", float64(len(classes)))
	return nil
}

// ReadWeekly reads the weekly history (one line per week); a missing file is no history yet.
func ReadWeekly(path string) ([]WeeklyLine, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []WeeklyLine
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var l WeeklyLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, i+1, err)
		}
		out = append(out, l)
	}
	return out, nil
}

// AppendWeekly adds the week's line; a second run of the same date replaces that date's line.
func AppendWeekly(lines []WeeklyLine, l WeeklyLine) []WeeklyLine {
	out := make([]WeeklyLine, 0, len(lines)+1)
	for _, old := range lines {
		if old.Date != l.Date {
			out = append(out, old)
		}
	}
	out = append(out, l)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Date < out[j].Date })
	return out
}

// WriteWeekly writes the history, one compact line per week.
func WriteWeekly(path string, lines []WeeklyLine) error {
	var b strings.Builder
	for _, l := range lines {
		raw, err := json.Marshal(l)
		if err != nil {
			return err
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// TrendWeeks is how many recent weeks the trend shows.
const TrendWeeks = 8

// RenderTrend is the weekly pull request's trend: every number over the recent weeks, newest last, with
// the move since the week before.
func RenderTrend(lines []WeeklyLine) string {
	if len(lines) > TrendWeeks {
		lines = lines[len(lines)-TrendWeeks:]
	}
	var b strings.Builder
	b.WriteString("## Weekly trend\n\n")
	if len(lines) == 0 {
		b.WriteString("No weekly run recorded yet.\n")
		return b.String()
	}
	keys := map[string]bool{}
	for _, l := range lines {
		for k := range l.Numbers {
			keys[k] = true
		}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	b.WriteString("| Number |")
	for _, l := range lines {
		fmt.Fprintf(&b, " %s |", l.Date)
	}
	b.WriteString(" Change |\n|---|")
	for range lines {
		b.WriteString("---|")
	}
	b.WriteString("---|\n")
	for _, k := range sorted {
		fmt.Fprintf(&b, "| %s |", k)
		for _, l := range lines {
			if v, ok := l.Numbers[k]; ok {
				fmt.Fprintf(&b, " %s |", trimFloat(v))
			} else {
				b.WriteString(" - |")
			}
		}
		b.WriteString(" " + change(lines, k) + " |\n")
	}
	last := lines[len(lines)-1]
	if len(last.Red) > 0 {
		fmt.Fprintf(&b, "\nRed on %s:\n\n", last.Date)
		for _, r := range last.Red {
			b.WriteString("- " + r + "\n")
		}
	}
	if len(last.Notes) > 0 {
		fmt.Fprintf(&b, "\nNotes for %s:\n\n", last.Date)
		notes := make([]string, 0, len(last.Notes))
		for k, v := range last.Notes {
			notes = append(notes, fmt.Sprintf("- %s: %s", k, v))
		}
		sort.Strings(notes)
		b.WriteString(strings.Join(notes, "\n") + "\n")
	}
	return b.String()
}

func change(lines []WeeklyLine, key string) string {
	if len(lines) < 2 {
		return ""
	}
	now, ok1 := lines[len(lines)-1].Numbers[key]
	before, ok2 := lines[len(lines)-2].Numbers[key]
	if !ok1 || !ok2 || now == before {
		return ""
	}
	if now > before {
		return "+" + trimFloat(now-before)
	}
	return "-" + trimFloat(before-now)
}

func trimFloat(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%.3g", v)
}
