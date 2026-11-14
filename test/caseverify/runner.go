package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Request is one probe's request with its times resolved.
type Request struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   string            `json:"query"`
	Headers map[string]string `json:"headers,omitempty"`
	// Variant is the proxy variant the case runs against ("" is the default proxy).
	Variant string `json:"variant,omitempty"`
}

// Source answers a request on one side ("loki" or "proxy"). attempt counts from 1; a difference is asked
// a second time before it is reported.
type Source interface {
	Ask(side, caseID string, attempt int, req Request) Answer
}

// maxBody bounds what is read of one answer; a longer answer is truncated and the probe blocked.
const maxBody = 32 << 20

// Live sends the requests over HTTP and, when rec is set, records every pair of answers.
type Live struct {
	Bases  map[string]string // side -> base URL
	Client *http.Client
	rec    *Recorder
}

// Ask sends the request to the side's base URL (a proxy variant's own URL when the case names one).
func (l *Live) Ask(side, caseID string, attempt int, req Request) Answer {
	base := l.Bases[side]
	if side == "proxy" && req.Variant != "" {
		base = l.Bases["proxy@"+req.Variant]
	}
	a := l.send(base, req)
	if l.rec != nil {
		l.rec.add(side, caseID, attempt, req, a)
	}
	return a
}

func (l *Live) send(base string, req Request) Answer {
	target := strings.TrimRight(base, "/") + req.Path
	var body io.Reader
	if req.Method == http.MethodPost {
		body = strings.NewReader(req.Query)
	} else if req.Query != "" {
		target += "?" + req.Query
	}
	r, err := http.NewRequest(req.Method, target, body)
	if err != nil {
		return Answer{Err: err.Error()}
	}
	if req.Method == http.MethodPost {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, v := range req.Headers {
		r.Header.Set(k, v)
	}
	resp, err := l.Client.Do(r)
	if err != nil {
		return Answer{Err: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return Answer{Status: resp.StatusCode, Err: err.Error()}
	}
	a := Answer{Status: resp.StatusCode, Body: string(raw), Stale: resp.Header.Get(staleHeader) != "",
		Partial: resp.Header.Get(partialHeader) == "true"}
	if len(raw) > maxBody {
		a.Body, a.Truncated = string(raw[:maxBody]), true
	}
	return a
}

// Recorded is one line of the recorded answers file: both sides' answers to one attempt of one case.
type Recorded struct {
	Case    string  `json:"case,omitempty"`
	Attempt int     `json:"attempt,omitempty"`
	Request Request `json:"request"`
	Loki    *Answer `json:"loki,omitempty"`
	Proxy   *Answer `json:"proxy,omitempty"`
	// The run's header (the first line): the settled end every window was resolved against, and the proxy build.
	Header *RunHeader `json:"header,omitempty"`
	// A metrics line instead: the proxy's counters at one phase of a case (cold_before, cold_after, ...).
	Phase   string   `json:"metrics_phase,omitempty"`
	Metrics Snapshot `json:"metrics,omitempty"`
	// A count line of the data precheck instead: the key and each side's count.
	Count *CountKey    `json:"count,omitempty"`
	LokiN *CountAnswer `json:"loki_count,omitempty"`
	VLN   *CountAnswer `json:"vl_count,omitempty"`
}

// RunHeader is what a replay needs to resolve every window as the live run did.
type RunHeader struct {
	WindowEnd int64      `json:"window_end"`
	Proxy     ProxyBuild `json:"proxy"`
}

// ProxyBuild is the build of the proxy under test, as the run was told it.
type ProxyBuild struct {
	Revision string `json:"revision,omitempty"`
	Image    string `json:"image,omitempty"`
	// Stale is set when the revision is not the commit the run measures: its verdicts are not the commit's.
	Stale bool `json:"stale,omitempty"`
	// Verified is set when the revision is known and equals the commit.
	Verified bool `json:"verified"`
}

// Recorder collects the answers of a live run in the order they were asked.
type Recorder struct {
	header *RunHeader
	lines  []*Recorded
	index  map[string]*Recorded
}

func (r *Recorder) add(side, caseID string, attempt int, req Request, a Answer) {
	if r.index == nil {
		r.index = map[string]*Recorded{}
	}
	key := fmt.Sprintf("%s\x00%d", caseID, attempt)
	line, ok := r.index[key]
	if !ok {
		line = &Recorded{Case: caseID, Attempt: attempt, Request: req}
		r.index[key] = line
		r.lines = append(r.lines, line)
	}
	if side == "loki" {
		line.Loki = &a
	} else {
		line.Proxy = &a
	}
}

func (r *Recorder) addCount(side string, key CountKey, a CountAnswer) {
	if r.index == nil {
		r.index = map[string]*Recorded{}
	}
	k := fmt.Sprintf("count\x00%s\x00%d\x00%d", key.Selector, key.Start, key.End)
	line, ok := r.index[k]
	if !ok {
		kk := key
		line = &Recorded{Count: &kk}
		r.index[k] = line
		r.lines = append(r.lines, line)
	}
	if side == "loki" {
		line.LokiN = &a
	} else {
		line.VLN = &a
	}
}

func (r *Recorder) addMetrics(caseID, phase string, s Snapshot) {
	r.lines = append(r.lines, &Recorded{Case: caseID, Phase: phase, Metrics: s})
}

// Write writes one JSON line per case attempt.
func (r *Recorder) Write(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if r.header != nil {
		if err := enc.Encode(Recorded{Header: r.header}); err != nil {
			return err
		}
	}
	for _, line := range r.lines {
		if err := enc.Encode(line); err != nil {
			return err
		}
	}
	return nil
}

// Replay answers from a recorded answers file and sends nothing.
type Replay struct {
	lines   map[string]Recorded
	counts  map[CountKey]Recorded
	metrics map[string]Snapshot
	header  *RunHeader
}

// Measure returns the recorded counters of a case's phase.
func (r *Replay) Measure(caseID, _, phase string) (Snapshot, bool) {
	s, ok := r.metrics[caseID+"\x00"+phase]
	return s, ok
}

// WindowEnd is the settled end the recorded run resolved its windows against: the header's, or, for a
// recording made before headers, the end most of its every-stream counts share (the label endpoints count at
// the settled end itself; a case with its own end counts elsewhere), else the latest count's end. Zero when
// the recording holds none of them.
func (r *Replay) WindowEnd() int64 {
	if r.header != nil {
		return r.header.WindowEnd
	}
	ends := map[int64]int{}
	var latest, best int64
	for key := range r.counts {
		latest = max(latest, key.End)
		if key.Selector == stackWide {
			ends[key.End]++
		}
	}
	for end, n := range ends {
		if n > ends[best] || (n == ends[best] && end > best) {
			best = end
		}
	}
	if best != 0 {
		return best
	}
	return latest
}

// LoadReplay reads a recorded answers file.
func LoadReplay(r io.Reader) (*Replay, error) {
	rp := &Replay{lines: map[string]Recorded{}, counts: map[CountKey]Recorded{}, metrics: map[string]Snapshot{}}
	// A line holds both sides' answers, each up to maxBody and JSON-escaped: no line length is capped.
	br := bufio.NewReaderSize(r, 1<<20)
	n := 0
	for {
		raw, readErr := br.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		if len(raw) == 0 && readErr == io.EOF {
			break
		}
		n++
		if len(bytes.TrimSpace(raw)) == 0 {
			if readErr == io.EOF {
				break
			}
			continue
		}
		var line Recorded
		if err := json.Unmarshal(raw, &line); err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		if line.Header != nil {
			rp.header = line.Header
			continue
		}
		if line.Count != nil {
			rp.counts[*line.Count] = line
			continue
		}
		if line.Phase != "" {
			rp.metrics[line.Case+"\x00"+line.Phase] = line.Metrics
			continue
		}
		if line.Case == "" || line.Attempt < 1 {
			return nil, fmt.Errorf("line %d: a case id and an attempt from 1 are required", n)
		}
		rp.lines[fmt.Sprintf("%s\x00%d", line.Case, line.Attempt)] = line
		if readErr == io.EOF {
			break
		}
	}
	return rp, nil
}

// Count returns a recorded count; false when the recording holds none (a recording made before the precheck).
func (r *Replay) Count(side string, key CountKey) (CountAnswer, bool) {
	line, ok := r.counts[key]
	a := line.LokiN
	if side != "loki" {
		a = line.VLN
	}
	if !ok || a == nil {
		return CountAnswer{}, false
	}
	return *a, true
}

// Ask returns the recorded answer; a missing one is an error answer, which blocks the probe.
func (r *Replay) Ask(side, caseID string, attempt int, _ Request) Answer {
	line, ok := r.lines[fmt.Sprintf("%s\x00%d", caseID, attempt)]
	a := line.Loki
	if side != "loki" {
		a = line.Proxy
	}
	if !ok || a == nil {
		return Answer{Err: "no recorded answer"}
	}
	return *a
}

// Verdicts.
const (
	verdictMatches       = "matches"
	verdictDiffers       = "differs"
	verdictBothFail      = "both_fail"
	verdictUnstable      = "unstable"
	verdictBlocked       = "blocked"
	verdictNotExecutable = "not_executable"
	// The data precheck's verdicts: nothing about the proxy is proven, so none of them is ever a regression.
	verdictDataDiffers   = "data_differs"   // Loki and VictoriaLogs count other lines for the case's selectors and window
	verdictEmpty         = "empty"          // both count no line
	verdictLokiNotReady  = "loki_not_ready" // Loki's metric path answers nothing while VictoriaLogs holds the lines
	verdictNotComparable = "not_comparable" // Loki answered with warnings, or the proxy with a stale or partial answer
	verdictDataUnchecked = "data_unchecked" // no count to prove the data equal (no VictoriaLogs URL, or a recording without counts)
)

// verdicts in the order the report lists them.
var verdicts = []string{verdictMatches, verdictDiffers, verdictUnstable, verdictBothFail, verdictDataDiffers, verdictEmpty,
	verdictLokiNotReady, verdictNotComparable, verdictDataUnchecked, verdictBlocked, verdictNotExecutable}

// staleHeader marks a proxy answer served stale (internal/proxy staleResponseHeader).
const staleHeader = "X-Proxy-Stale-Response"

// partialHeader marks a proxy answer it could not complete (internal/proxy query_range windowing).
const partialHeader = "X-Loki-VL-Partial-Response"

// Flags raised against the registry's expectation. The runner never changes a case: these are reported.
const (
	flagRegression       = "regression"
	flagProposedClose    = "proposed_close"
	flagBehaviourChanged = "behaviour_changed"
	// flagStaleRegression holds the cases that differ on a proxy built from another commit than the one measured.
	flagStaleRegression = "differs_on_stale_proxy"
)

// Result is one case's verdict.
type Result struct {
	Verdict       string       `json:"verdict"`
	GapStatus     string       `json:"gap_status,omitempty"`
	Expected      string       `json:"expected"`
	Flag          string       `json:"flag,omitempty"`
	Reason        string       `json:"reason,omitempty"`
	Facets        []string     `json:"facets,omitempty"`
	UnknownFields []string     `json:"unknown_fields,omitempty"`
	LokiStatus    int          `json:"loki_status,omitempty"`
	ProxyStatus   int          `json:"proxy_status,omitempty"`
	Variant       string       `json:"variant,omitempty"`
	Data          []DataCheck  `json:"data,omitempty"`
	Reuse         *Reuse       `json:"reuse,omitempty"`
	Differences   []Difference `json:"differences,omitempty"`
}

// DataCheck is one selector's count on both sides over the case's window.
type DataCheck struct {
	CountKey
	Loki *CountAnswer `json:"loki"`
	VL   *CountAnswer `json:"victorialogs"`
}

// statusIntent reports whether the case compares the status or the error: then a failing Loki is the answer
// being compared, not an unhealthy side.
func statusIntent(p Probe) bool {
	if p.Mode == "status" {
		return true
	}
	for _, f := range p.Fields {
		if facet := fieldFacet[f]; facet == facetStatus || facet == facetError || facet == facetErrorType {
			return true
		}
	}
	return false
}

func tolerance(p Probe) float64 {
	if t, ok := p.Values["tolerance"].(float64); ok && t >= 0 {
		return t
	}
	if p.Mode == "numeric" || p.Values["mode"] == "numeric" {
		return 1e-6
	}
	return 0
}

// Verify runs one probe.
func Verify(p Probe, src Source, cnt Counter, meter Meter, now time.Time, tenant string) (r Result) {
	r = Result{GapStatus: p.Status, Expected: p.Expectation(), Variant: p.Variant}
	switch {
	case p.NotExecutable != "":
		r.Verdict, r.Reason = verdictNotExecutable, p.NotExecutable
		return r
	case p.Blocked != "":
		r.Verdict, r.Reason = verdictBlocked, p.Blocked
		return r
	}
	q, why := p.Query(now)
	if why != "" {
		r.Verdict, r.Reason = verdictBlocked, why
		return r
	}
	headers := map[string]string{}
	for k, v := range p.Headers {
		headers[k] = v
	}
	if _, ok := headers["X-Scope-OrgID"]; !ok && tenant != "" {
		headers["X-Scope-OrgID"] = tenant
	}
	req := Request{Method: p.Method, Path: p.Path, Query: q.Encode(), Headers: headers, Variant: p.Variant}
	r.Facets, r.UnknownFields = p.Facets()
	opts := OptionsOf(p, q)
	checked := false
	var first Answer
	var firstCost Snapshot
	// Whatever the verdict on the answers, a proxy answer the proxy served is asked again for its reuse; an
	// answer that is not one (an error, a partial or stale answer, a side that did not answer) is not.
	defer func() {
		switch r.Verdict {
		case verdictBlocked, verdictNotComparable:
			return
		}
		if first.OK() && meter != nil {
			r.Reuse = measureReuse(p, req, first, firstCost, src, meter, r.Facets, opts)
		}
	}()
	for attempt := 1; attempt <= 2; attempt++ {
		loki := src.Ask("loki", p.ID, attempt, req)
		var before Snapshot
		var measured bool
		if attempt == 1 && meter != nil {
			before, measured = meter.Measure(p.ID, p.Variant, "cold_before")
		}
		proxy := src.Ask("proxy", p.ID, attempt, req)
		if attempt == 1 {
			first = proxy
			if after, ok := meterAfter(meter, p, measured); ok {
				firstCost = Delta(before, after)
			}
		}
		r.LokiStatus, r.ProxyStatus = loki.Status, proxy.Status
		if blocked := unusable("Loki", loki) + unusable("the proxy", proxy); blocked != "" {
			r.Verdict, r.Reason, r.Differences = verdictBlocked, strings.TrimSuffix(blocked, "; "), nil
			return r
		}
		if !statusIntent(p) && loki.Status >= 500 {
			r.Verdict, r.Reason = verdictBlocked, fmt.Sprintf("Loki answered %d", loki.Status)
			if proxy.Status >= 500 {
				r.Verdict = verdictBothFail
				r.Reason += fmt.Sprintf(" and the proxy %d", proxy.Status)
			}
			return r
		}
		if why := notComparable(loki, proxy); why != "" {
			r.Verdict, r.Reason, r.Differences = verdictNotComparable, why, nil
			return r
		}
		// The data precheck, once: an answer is compared only on equal, non-empty data.
		if !checked && needsData(r.Facets, loki) {
			checked = true
			if verdict, why := precheck(&r, p, q, cnt, now, headers["X-Scope-OrgID"], tenant); verdict != "" {
				r.Verdict, r.Reason = verdict, why
				return r
			}
		}
		r.Differences = CompareWith(r.Facets, loki, proxy, opts)
		if len(r.Differences) == 0 {
			if Vacuous(r.Facets, loki, proxy) {
				r.Verdict, r.Reason = verdictBlocked, "both answers are empty: nothing is proven"
				return r
			}
			r.Verdict = verdictMatches
			if attempt == 2 {
				r.Verdict, r.Reason = verdictUnstable, "the first answers differed, the second matched"
			}
			break
		}
		r.Verdict = verdictDiffers
	}
	r.Flag = flagOf(r)
	return r
}

func meterAfter(meter Meter, p Probe, measured bool) (Snapshot, bool) {
	if !measured {
		return nil, false
	}
	return meter.Measure(p.ID, p.Variant, "cold_after")
}

// notComparable is why two answers cannot be compared at all: Loki's own answer carries warnings (a partial or
// limited answer), or the proxy's is stale.
func notComparable(loki, proxy Answer) string {
	var doc struct {
		Warnings []any `json:"warnings"`
	}
	if loki.OK() && json.Unmarshal([]byte(loki.Body), &doc) == nil && len(doc.Warnings) > 0 {
		return fmt.Sprintf("Loki answered with %d warning(s)", len(doc.Warnings))
	}
	if proxy.Stale {
		return "the proxy answered stale"
	}
	if proxy.Partial {
		return "the proxy answered partial"
	}
	return ""
}

// needsData reports whether the comparison reads data (not only the status and the error): then the data must
// be proven equal first. A Loki error is compared as it is.
func needsData(facets []string, loki Answer) bool {
	if !loki.OK() {
		return false
	}
	for _, f := range facets {
		switch f {
		case facetStatus, facetError, facetErrorType, facetWarnings:
		default:
			return true
		}
	}
	return false
}

// window is the span of data a request reads: start to end, a lookback before an instant query's time, or
// Loki's default last hour.
func window(q url.Values, now time.Time) (int64, int64) {
	parse := func(v string) (int64, bool) {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			if f < 1e12 { // seconds
				return int64(f * 1e9), true
			}
			return int64(f), true
		}
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UnixNano(), true
		}
		return 0, false
	}
	if s, ok := parse(q.Get("start")); ok {
		if e, ok := parse(q.Get("end")); ok {
			return s, e
		}
		return s, now.UnixNano()
	}
	if t, ok := parse(q.Get("time")); ok {
		return t - int64(Lookback(q.Get("query"))), t
	}
	return now.Add(-time.Hour).UnixNano(), now.UnixNano()
}

// precheck counts the case's selectors on both sides; a verdict when the data does not allow a comparison.
func precheck(r *Result, p Probe, q url.Values, cnt Counter, now time.Time, caseTenant, tenant string) (string, string) {
	if caseTenant != "" && caseTenant != tenant {
		return verdictDataUnchecked, fmt.Sprintf("the data precheck counts tenant %q only, the case asks %q", tenant, caseTenant)
	}
	start, end := window(q, now)
	selectors := Selectors(q.Get("query"))
	if len(selectors) == 0 {
		selectors = []string{stackWide}
	}
	empty := true
	for _, sel := range selectors {
		key := CountKey{Selector: sel, Start: start, End: end}
		l, lok := cnt.Count("loki", key)
		v, vok := cnt.Count("vl", key)
		if !lok || !vok {
			return verdictDataUnchecked, "no count of the data on both sides (a VictoriaLogs URL or a recording with counts is needed)"
		}
		r.Data = append(r.Data, DataCheck{CountKey: key, Loki: &l, VL: &v})
		label := sel
		if label == stackWide {
			label = "every stream"
		}
		switch {
		case l.Err != "" || v.Err != "":
			return verdictBlocked, "the data precheck failed: " + strings.TrimSpace(l.Err+" "+v.Err)
		case l.Empty && v.Count > 0:
			return verdictLokiNotReady, fmt.Sprintf("Loki's metric path answers nothing for %s while VictoriaLogs holds %.0f lines", label, v.Count)
		case l.Count != v.Count:
			return verdictDataDiffers, fmt.Sprintf("%s: Loki holds %.0f lines, VictoriaLogs %.0f", label, l.Count, v.Count)
		}
		if l.Count > 0 {
			empty = false
		}
	}
	if empty {
		return verdictEmpty, "both sides hold no line for the case's selectors and window"
	}
	return "", ""
}

func unusable(side string, a Answer) string {
	switch {
	case a.Err != "":
		return fmt.Sprintf("%s did not answer: %s; ", side, a.Err)
	case a.Truncated:
		return fmt.Sprintf("%s answered more than %d bytes; ", side, maxBody)
	}
	return ""
}

func flagOf(r Result) string {
	switch {
	case r.Expected == "match" && r.Verdict == verdictDiffers:
		return flagRegression
	case r.Expected == "differ" && r.Verdict == verdictMatches:
		return flagProposedClose
	case r.Expected == "accepted" && r.Verdict == verdictMatches:
		return flagBehaviourChanged
	}
	return ""
}

// File is cases.json: one run's verdicts.
type File struct {
	GeneratedBy  string              `json:"generated_by"`
	Date         string              `json:"date"`
	Commit       string              `json:"commit"`
	Mode         string              `json:"mode"`
	Loki         string              `json:"loki_version"`
	VictoriaLogs string              `json:"victorialogs_version"`
	ProxyFlags   string              `json:"proxy_flags"`
	ProxyBuild   ProxyBuild          `json:"proxy_build"`
	WindowEnd    string              `json:"window_end"`
	Counts       map[string]int      `json:"counts"`
	Flags        map[string][]string `json:"flags"`
	Reuse        ReuseSummary        `json:"reuse"`
	Cases        map[string]Result   `json:"cases"`
}

// Summarize counts the verdicts and lists the flagged cases.
func Summarize(f *File) {
	f.Reuse = SummarizeReuse(f.Cases)
	f.Counts = map[string]int{}
	f.Flags = map[string][]string{flagRegression: {}, flagProposedClose: {}, flagBehaviourChanged: {}}
	for id, r := range f.Cases {
		f.Counts[r.Verdict]++
		if r.Flag != "" {
			f.Flags[r.Flag] = append(f.Flags[r.Flag], id)
		}
	}
	for k := range f.Flags {
		sort.Strings(f.Flags[k])
	}
}

// Report is the Markdown summary of a run: the counts and every flagged case with its measured values.
func Report(f File) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### Registry cases against Loki (%s, %s)\n\n", f.Date, f.Mode)
	fmt.Fprintf(&b, "Commit `%s`, Loki %s, VictoriaLogs %s, proxy build `%s`.\n\n", short(f.Commit), f.Loki, f.VictoriaLogs, short(f.ProxyBuild.Revision))
	if f.ProxyBuild.Stale {
		fmt.Fprintf(&b, "**The proxy under test was built from `%s`, not the commit measured: no difference is a regression of this commit "+
			"(%d listed apart as %s).**\n\n", short(f.ProxyBuild.Revision), len(f.Flags[flagStaleRegression]), flagStaleRegression)
	} else if !f.ProxyBuild.Verified {
		b.WriteString("The proxy's build is not known (no -proxy-revision): its verdicts are not tied to the commit.\n\n")
	}
	b.WriteString("Only `differs` is a difference of the proxy: its data was proven equal and non-empty on both sides. " +
		"`data_differs`, `empty`, `loki_not_ready`, `not_comparable` and `data_unchecked` prove nothing about the proxy.\n\n")
	b.WriteString("| Verdict | Cases |\n|---|---|\n")
	for _, v := range verdicts {
		fmt.Fprintf(&b, "| %s | %d |\n", v, f.Counts[v])
	}
	ru := f.Reuse
	fmt.Fprintf(&b, "\n#### Cache reuse of a repeat: %d reused, %d not reused, %d not measured\n\n", ru.Counts[reuseReused],
		ru.Counts[reuseNotReused], ru.Counts[reuseNotMeasured])
	fmt.Fprintf(&b, "VictoriaLogs calls: %.0f cold, %.0f on the repeats (%.0f of them on endpoints the first ask did not use: "+
		"background work the process-wide counters count); bytes %.0f cold, %.0f on the repeats.", ru.ColdVLCalls,
		ru.RepeatCalls, ru.Background, ru.ColdBytes, ru.RepeatBytes)
	if len(ru.RepeatHits) > 0 {
		keys := make([]string, 0, len(ru.RepeatHits))
		for k := range ru.RepeatHits {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s %.2f", k, ru.RepeatHits[k]))
		}
		b.WriteString(" Hit ratio of the repeats by layer and tier: " + strings.Join(parts, ", ") + ".")
	}
	b.WriteString("\n")
	for i, id := range ru.NotReused {
		if i == 50 {
			fmt.Fprintf(&b, "- ... %d more in cases.json\n", len(ru.NotReused)-50)
			break
		}
		fmt.Fprintf(&b, "- `%s`: %s\n", id, f.Cases[id].Reuse.Reason)
	}
	sections := []struct{ flag, title string }{
		{flagRegression, "Fixed cases (or cases without a gap) that differ from Loki on equal data"},
		{flagProposedClose, "Open cases that now match Loki (proposed close; the case file is not changed)"},
		{flagBehaviourChanged, "Documented, owner-kept or upstream cases that now match Loki (behaviour changed)"},
	}
	for _, s := range sections {
		ids := f.Flags[s.flag]
		fmt.Fprintf(&b, "\n#### %s: %d\n\n", s.title, len(ids))
		for i, id := range ids {
			if i == 50 {
				fmt.Fprintf(&b, "- ... %d more in cases.json\n", len(ids)-50)
				break
			}
			r := f.Cases[id]
			fmt.Fprintf(&b, "- `%s` (Loki %d, proxy %d, compared: %s)\n", id, r.LokiStatus, r.ProxyStatus, strings.Join(r.Facets, ", "))
			for _, d := range r.Differences {
				fmt.Fprintf(&b, "  - %s: Loki `%s`, proxy `%s`\n", d.Facet, oneLine(d.Loki, 200), oneLine(d.Proxy, 200))
			}
		}
	}
	return b.String()
}

func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

func oneLine(s string, n int) string {
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "`", "'")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// readVersion reads a short text from url and extracts a version with find; "" when it cannot.
func readVersion(client *http.Client, target string, find func(string) string) string {
	if target == "" {
		return ""
	}
	resp, err := client.Get(target)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	return find(string(raw))
}

// lokiVersion is the version a Loki reports on /loki/api/v1/status/buildinfo.
func lokiVersion(text string) string {
	var info struct {
		Version string `json:"version"`
	}
	if json.Unmarshal([]byte(text), &info) != nil {
		return ""
	}
	return info.Version
}

// vlVersion is the short_version label of VictoriaLogs' vm_app_version metric.
func vlVersion(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "vm_app_version{") {
			continue
		}
		_, rest, ok := strings.Cut(line, `short_version="`)
		if !ok {
			continue
		}
		if v, _, ok := strings.Cut(rest, `"`); ok {
			return v
		}
	}
	return ""
}

func baseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an http(s) base URL", raw)
	}
	return nil
}

func openReplay(path string) (*Replay, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	rp, err := LoadReplay(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return rp, nil
}

var errUsage = errors.New("give -loki and -proxy for a live run, or -replay for a recorded one")
