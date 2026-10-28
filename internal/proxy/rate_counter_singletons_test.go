package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// rateCounterEmuHandler serves vlEmulator plus the two reads of the
// rate_counter one-sample proof on /select/logsql/query, as VictoriaLogs
// v1.49-v1.53 answer them: `| limit N | unpack_logfmt from _msg result_prefix
// "__lvpk." | field_names` (every field of the first N lines carrying the
// unwrapped field, stored and parsed, with hits; pipe_field_names.go) and
// `[| unpack_logfmt from _msg fields (...) result_prefix "__lvpk."] | stats
// count() as n, count_uniq_hash(<fields>) as u` (stats_count_uniq_hash.go: a
// tuple of only empty values is not counted). A row's stored fields are its
// stream labels and emuRow.labels; its parsed keys come from its line.
func rateCounterEmuHandler(e *vlEmulator) http.Handler {
	inner := e.handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.FormValue("query")
		if r.URL.Path != "/select/logsql/query" || !strings.Contains(query, ` | filter `+e.field+`:*`) {
			inner.ServeHTTP(w, r)
			return
		}
		e.record(r.URL.Path, query)
		rateCounterEmuTenants.Store(r.Header.Get("AccountID")+"/"+r.Header.Get("ProjectID"), true)
		selected := e.selectRows(r)
		sort.SliceStable(selected, func(i, j int) bool { return selected[i].ts < selected[j].ts })
		var rows []map[string]string
		for _, row := range selected {
			if row.text() == "" {
				continue
			}
			f := map[string]string{"_time": strconv.FormatInt(row.ts, 10), "_msg": e.line(row), "_stream": row.stream, "_stream_id": row.stream, e.field: row.text()}
			for _, m := range emuStreamLabelRE.FindAllStringSubmatch(row.stream, -1) {
				f[m[1]] = m[2]
			}
			for k, v := range row.labels {
				if v != "" {
					f[k] = v
				}
			}
			rows = append(rows, f)
		}
		unpack := func(f map[string]string, only map[string]bool) {
			for _, kv := range emuLogfmt(f["_msg"]) {
				if (only == nil || only[kv[0]]) && kv[1] != "" {
					f["__lvpk."+kv[0]] = kv[1]
				}
			}
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		if m := rateCounterEmuNamesRE.FindStringSubmatch(query); m != nil {
			limit, _ := strconv.Atoi(m[1])
			if len(rows) > limit {
				rows = rows[:limit]
			}
			hits := map[string]int{}
			for _, row := range rows {
				unpack(row, nil)
				for k := range row {
					hits[k]++
				}
			}
			for k, n := range hits {
				line, _ := json.Marshal(map[string]string{"name": k, "hits": strconv.Itoa(n)})
				_, _ = w.Write(append(line, '\n'))
			}
			return
		}
		m := rateCounterEmuUniqRE.FindStringSubmatch(query)
		if m == nil {
			e.t.Fatalf("emulator: unknown proof query %s", query)
		}
		if m[1] != "" {
			only := map[string]bool{}
			for _, k := range strings.Split(m[1], ",") {
				only[strings.Trim(strings.TrimSpace(k), "`")] = true
			}
			for _, row := range rows {
				unpack(row, only)
			}
		}
		var fields []string
		for _, f := range strings.Split(m[2], ",") {
			fields = append(fields, strings.Trim(strings.TrimSpace(f), "`"))
		}
		seen := map[string]bool{}
		for _, row := range rows {
			var key strings.Builder
			empty := true
			for _, f := range fields {
				key.WriteString(strconv.Quote(row[f]))
				empty = empty && row[f] == ""
			}
			if !empty {
				seen[key.String()] = true
			}
		}
		line, _ := json.Marshal(map[string]string{"n": strconv.Itoa(len(rows)), "u": strconv.Itoa(len(seen))})
		_, _ = w.Write(append(line, '\n'))
	})
}

// emuLogfmt splits a test line of space-separated key=value pairs.
func emuLogfmt(line string) [][2]string {
	var out [][2]string
	for _, pair := range strings.Fields(line) {
		k, v, _ := strings.Cut(pair, "=")
		out = append(out, [2]string{k, v})
	}
	return out
}

var (
	rateCounterEmuNamesRE = regexp.MustCompile(`\| limit (\d+) \| unpack_logfmt from _msg result_prefix "__lvpk\." \| field_names$`)
	rateCounterEmuUniqRE  = regexp.MustCompile(`(?: \| unpack_logfmt from _msg fields \((.*)\) result_prefix "__lvpk\.")? \| stats count\(\) as n, count_uniq_hash\((.*)\) as u$`)
)

// rateCounterEmuTenants holds the tenant headers the proof reads carried.
var rateCounterEmuTenants sync.Map

// lokiRateCounter is Loki's rate_counter of one series' samples in a window,
// written apart from the proxy from pkg/logql/range_vector.go:274-350
// (rateCounter, extrapolatedRate with isCounter and isRate).
func lokiRateCounter(ts []int64, vs []float64, window int64) float64 {
	if len(ts) < 2 {
		return 0
	}
	rangeStart := ts[0] - window
	rangeEnd := ts[len(ts)-1]
	result := vs[len(vs)-1] - vs[0]
	var last float64
	for _, v := range vs {
		if v < last {
			result += last
		}
		last = v
	}
	toStart := float64(ts[0]-rangeStart) / 1e9
	toEnd := float64(rangeEnd-ts[len(ts)-1]) / 1e9
	sampled := float64(ts[len(ts)-1]-ts[0]) / 1e9
	avg := sampled / float64(len(ts)-1)
	if result > 0 && vs[0] >= 0 {
		if toZero := sampled * (vs[0] / result); toZero < toStart {
			toStart = toZero
		}
	}
	threshold := avg * 1.1
	interval := sampled
	if toStart < threshold {
		interval += toStart
	} else {
		interval += avg / 2
	}
	if toEnd < threshold {
		interval += toEnd
	} else {
		interval += avg / 2
	}
	return result * (interval / sampled) / (float64(window) / 1e9)
}

// lokiRateCounterSum is Loki's `sum [by (group)] (rate_counter(...))`: per
// evaluation time, the series (stream plus parsed labels, the unwrapped one
// left out) with a sample in (t-window, t], their rate_counter, summed per
// group value (pkg/logql/evaluator.go VectorAggEvaluator.Next). A group with
// no sample at t has no point.
func lokiRateCounterSum(rows []emuRow, window int64, times []int64, group string) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	sorted := append([]emuRow(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ts < sorted[j].ts })
	for _, t := range times {
		type series struct {
			group string
			ts    []int64
			vs    []float64
		}
		perSeries := map[string]*series{}
		for _, row := range sorted {
			if row.ts <= t-window || row.ts > t {
				continue
			}
			parsed := lokiRateCounterLabels(row.msg)
			labels := make([]string, 0, len(parsed))
			for k, v := range parsed {
				labels = append(labels, k+"="+v)
			}
			sort.Strings(labels)
			key := row.stream + "|" + strings.Join(labels, ",")
			s := perSeries[key]
			if s == nil {
				s = &series{}
				if group != "" {
					s.group = parsed[group]
				}
				perSeries[key] = s
			}
			s.ts = append(s.ts, row.ts)
			s.vs = append(s.vs, row.value)
		}
		for _, s := range perSeries {
			if out[s.group] == nil {
				out[s.group] = map[int64]float64{}
			}
			out[s.group][t] += lokiRateCounter(s.ts, s.vs, window)
		}
	}
	return out
}

// lokiRateCounterLabels are the labels Loki's `| logfmt` extracts from a
// test line, the unwrapped one (amount) left out: the key sanitized
// (pkg/logql/log/util.go:22-39), the first value of a label kept, empty values
// dropped.
func lokiRateCounterLabels(line string) map[string]string {
	out := map[string]string{}
	for _, kv := range emuLogfmt(line) {
		name := lokiLogfmtLabel(kv[0])
		if _, done := out[name]; done || kv[1] == "" {
			continue
		}
		out[name] = kv[1]
	}
	delete(out, "amount")
	return out
}

type rcScenario struct {
	window, step time.Duration
	start, end   int64
	rows         []emuRow
	group        string // "" for a bare sum
	parser       string
	shared       bool // some lines share every label
}

func (s rcScenario) query() string {
	agg := "sum"
	if s.group != "" {
		agg = "sum by (" + s.group + ")"
	}
	return fmt.Sprintf(`%s(rate_counter({app="rc"} | %s | unwrap amount [%s]))`, agg, s.parser, uwWindowText(s.window))
}

func (s rcScenario) times() []int64 {
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

// generateRateCounterScenario draws lines of three streams, each with its own
// transaction id unless shared is set (then a few ids repeat inside one
// stream), a parsed level, values among which NaN, zero and drops (counter
// resets), lines exactly on window edges and 1 ns off them, and a window
// shorter than, equal to, longer than or not a multiple of the step.
func generateRateCounterScenario(rnd *rand.Rand, parser string, shared bool) rcScenario {
	steps := []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute, 150 * time.Second}
	s := rcScenario{step: steps[rnd.Intn(len(steps))], window: windows[rnd.Intn(len(windows))], parser: parser, shared: shared}
	if rnd.Intn(4) == 0 {
		s.window = s.step
	}
	if rnd.Intn(3) == 0 {
		s.group = "level"
	}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	s.end = s.start + int64(rnd.Intn(12))*int64(s.step)
	streams := []string{`{app="rc",pod="a"}`, `{app="rc",pod="b"}`, `{app="rc",pod="c"}`}
	levels := []string{"info", "error", ""}
	values := []float64{0, 1, 2.5, 7, 100, 3, math.NaN()}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 1 + rnd.Intn(50)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 {
			edge := s.start + int64(rnd.Intn(12))*int64(s.step)
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		id := strconv.Itoa(i)
		if shared && rnd.Intn(3) == 0 {
			id = "dup"
		}
		level := levels[rnd.Intn(len(levels))]
		row := emuRow{ts: ts, stream: streams[rnd.Intn(len(streams))], value: values[rnd.Intn(len(values))], labels: map[string]string{"txn": id, "level": level}}
		if shared && id == "dup" {
			row.stream, row.labels["level"] = streams[0], "info"
		}
		row.msg = rateCounterLine(parser, row)
		s.rows = append(s.rows, row)
	}
	return s
}

func rateCounterLine(parser string, row emuRow) string {
	if parser == "json" {
		fields := map[string]any{"amount": row.value, "txn": row.labels["txn"]}
		if math.IsNaN(row.value) {
			fields["amount"] = "NaN"
		}
		if row.labels["level"] != "" {
			fields["level"] = row.labels["level"]
		}
		line, _ := json.Marshal(fields)
		return string(line)
	}
	line := "amount=" + row.text() + " txn=" + row.labels["txn"]
	if row.labels["level"] != "" {
		line += " level=" + row.labels["level"]
	}
	return line
}

// runRateCounterScenario sends the scenario through the real handler and
// returns group value -> time -> value, and whether rows were read.
func runRateCounterScenario(t testing.TB, version string, s rcScenario, tenant string) (map[string]map[int64]float64, bool) {
	t.Helper()
	emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
	backend := httptest.NewServer(rateCounterEmuHandler(emu))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
		TenantMap: map[string]TenantMapping{"team-a": {AccountID: "7", ProjectID: "3"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {s.query()}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)}, "step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": tenant})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", s.query(), res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		g := series.Metric[s.group]
		if s.group == "" && len(series.Metric) != 0 {
			t.Fatalf("%s: the series carries labels %v", s.query(), series.Metric)
		}
		if got[g] == nil {
			got[g] = map[int64]float64{}
		}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[g][int64(point[0].(float64))*1e9] = v
		}
	}
	readRows := false
	for _, q := range emu.endpointCalls("/select/logsql/query") {
		if !strings.Contains(q, "| filter amount:*") {
			readRows = true
		}
	}
	return got, readRows
}

func compareRateCounter(t testing.TB, label string, s rcScenario, got, want map[string]map[int64]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d series, Loki %d\nquery %s step=%v window=%v rows=%d", label, len(got), len(want), s.query(), s.step, s.window, len(s.rows))
	}
	for g, points := range want {
		if len(got[g]) != len(points) {
			t.Fatalf("%s: group %q has %d points, Loki %d\nquery %s step=%v window=%v", label, g, len(got[g]), len(points), s.query(), s.step, s.window)
		}
		for ts, w := range points {
			v, ok := got[g][ts]
			if !ok || math.Float64bits(v) != math.Float64bits(w) {
				t.Fatalf("%s: group %q at %d got %v (present %v), Loki %v\nquery %s step=%v window=%v", label, g, ts/1e9, v, ok, w, s.query(), s.step, s.window)
			}
		}
	}
}

// TestRateCounterOneSampleMatchesLoki: over generated lines whose parsed
// transaction id makes every line a series of its own, `sum` and `sum by
// (level)` of rate_counter over `| logfmt` answer Loki's points bit for bit
// (0 at every step whose window holds a sample, no point elsewhere), from
// VictoriaLogs stats alone (no row read), on VictoriaLogs v1.49.0, v1.53.0 and
// an unknown version, under a mapped tenant and the default one. `| json`
// keeps the per-row evaluator.
//
// conformance: semantics/unwrap-rate-counter-one-sample-series, semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestRateCounterOneSampleMatchesLoki(t *testing.T) {
	for _, version := range []string{"v1.49.0", "v1.53.0", ""} {
		for _, parser := range []string{"logfmt", "json"} {
			t.Run(version+"/"+parser, func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(version)*31 + len(parser))))
				for i := 0; i < 60; i++ {
					s := generateRateCounterScenario(rnd, parser, false)
					tenant := "0"
					if i%2 == 1 {
						tenant = "team-a"
					}
					got, readRows := runRateCounterScenario(t, version, s, tenant)
					if parser == "json" {
						if !readRows {
							t.Fatalf("#%d %s: | json took the stats-only answer", i, s.query())
						}
						continue
					}
					if readRows {
						t.Fatalf("#%d %s: rows were read, want stats only", i, s.query())
					}
					compareRateCounter(t, fmt.Sprintf("#%d", i), s, got, lokiRateCounterSum(s.rows, int64(s.window), s.times(), s.group))
				}
			})
		}
	}
	if _, ok := rateCounterEmuTenants.Load("7/3"); !ok {
		t.Fatal("the proof reads never carried the mapped tenant's AccountID/ProjectID")
	}
}

// TestRateCounterOneSampleProofRefusesSharedSeries: when two lines of one
// stream share every parsed label (the same transaction id), the proof fails
// and the per-row evaluator answers, as before: the zero answer is never given
// for a series holding two samples.
//
// conformance: semantics/unwrap-rate-counter-one-sample-series
func TestRateCounterOneSampleProofRefusesSharedSeries(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	refused := 0
	for i := 0; i < 80; i++ {
		s := generateRateCounterScenario(rnd, "logfmt", true)
		dups := 0
		for _, row := range s.rows {
			if row.labels["txn"] == "dup" && row.ts > s.start-int64(s.window) && row.ts <= s.end {
				dups++
			}
		}
		_, readRows := runRateCounterScenario(t, "v1.53.0", s, "0")
		if dups >= 2 && !readRows {
			t.Fatalf("#%d %s: %d lines share a series, the stats-only answer was given", i, s.query(), dups)
		}
		if dups >= 2 {
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("no scenario had a shared series")
	}
}

// TestRateCounterOneSampleProofEdges: lines that are one Loki series under a
// tuple that could look finer never take the stats-only answer: a stored
// per-line field Loki's `| logfmt` does not extract, two keys Loki names alike
// (`a.b` and `a_b`, `0id` and `_0id`: one value kept); lines that differ by a
// parsed key do, also with a stored field all of
// them share, and so does a window with no line (no point).
//
// conformance: semantics/unwrap-rate-counter-one-sample-series
func TestRateCounterOneSampleProofEdges(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	at := func(i int) int64 { return base - int64(4*time.Minute) + int64(i)*int64(7*time.Second) }
	for _, tc := range []struct {
		name   string
		row    func(i int) emuRow
		zeroOK bool
	}{
		{"unique parsed id", func(i int) emuRow {
			return emuRow{ts: at(i), stream: `{app="rc"}`, value: float64(i), msg: fmt.Sprintf("amount=%d txn=t%d", i, i), labels: map[string]string{"txn": fmt.Sprintf("t%d", i)}}
		}, true},
		{"unique parsed id, shared stored field", func(i int) emuRow {
			return emuRow{ts: at(i), stream: `{app="rc"}`, value: float64(i), msg: fmt.Sprintf("amount=%d txn=t%d", i, i), labels: map[string]string{"txn": fmt.Sprintf("t%d", i), "region": "eu"}}
		}, true},
		{"unique stored field only", func(i int) emuRow {
			return emuRow{ts: at(i), stream: `{app="rc"}`, value: float64(i), msg: fmt.Sprintf("amount=%d txn=same", i), labels: map[string]string{"txn": "same", "rid": fmt.Sprintf("r%d", i)}}
		}, false},
		{"keys Loki names alike", func(i int) emuRow {
			return emuRow{ts: at(i), stream: `{app="rc"}`, value: float64(i), msg: fmt.Sprintf("amount=%d a_b=same a.b=u%d", i, i)}
		}, false},
		{"a digit-leading key and its sanitized twin", func(i int) emuRow {
			return emuRow{ts: at(i), stream: `{app="rc"}`, value: float64(i), msg: fmt.Sprintf("amount=%d _0id=same 0id=u%d", i, i)}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := rcScenario{window: 5 * time.Minute, step: 30 * time.Second, start: base, end: base + int64(5*time.Minute), parser: "logfmt"}
			for i := 0; i < 30; i++ {
				s.rows = append(s.rows, tc.row(i))
			}
			got, readRows := runRateCounterScenario(t, "v1.53.0", s, "0")
			if readRows == tc.zeroOK {
				t.Fatalf("rows read %v, want %v", readRows, !tc.zeroOK)
			}
			if !readRows {
				compareRateCounter(t, tc.name, s, got, lokiRateCounterSum(s.rows, int64(s.window), s.times(), s.group))
			}
		})
	}
	t.Run("no line", func(t *testing.T) {
		s := rcScenario{window: 5 * time.Minute, step: 30 * time.Second, start: base, end: base + int64(5*time.Minute), parser: "logfmt"}
		got, readRows := runRateCounterScenario(t, "v1.53.0", s, "0")
		if readRows || len(got) != 0 {
			t.Fatalf("rows read %v, %d series; want none", readRows, len(got))
		}
	})
}

// TestRateCounterUnderSumScope: only a sum (bare or by) directly over
// rate_counter of a log query with a parser, and no stage that drops, keeps or
// renames labels, takes the proof.
//
// conformance: semantics/unwrap-rate-counter-one-sample-series
func TestRateCounterUnderSumScope(t *testing.T) {
	cases := []struct {
		query         string
		ok, ungrouped bool
	}{
		{`sum(rate_counter({app="a"} | logfmt | unwrap amount [5m]))`, true, true},
		{`sum by () (rate_counter({app="a"} | logfmt | unwrap amount [5m]))`, true, true},
		{`sum by (level) (rate_counter({app="a"} | logfmt | unwrap amount [5m]))`, true, false},
		{`sum by (level) (rate_counter({app="a"} | json | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt --strict | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt amount, txn | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt | line_format "{{.txn}}" | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt | logfmt | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | regexp "(?P<amount>\\d+)" | unwrap amount [5m]))`, false, false},
		{`label_replace(sum(rate_counter({app="a"} | logfmt | unwrap amount [5m])), "x", "y", "", "")`, true, true},
		{`sum(rate_counter({app="a"} |= "pay" | logfmt | level="error" | unwrap amount | __error__="" [5m]))`, true, true},
		{`sum(rate_counter({app="a"} | logfmt | unwrap duration(ttl) [5m]))`, true, true},
		{`sum(rate_counter({app="a"} | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt | drop txn | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt | keep amount | unwrap amount [5m]))`, false, false},
		{`sum(rate_counter({app="a"} | logfmt | label_format txn="x" | unwrap amount [5m]))`, false, false},
		{`sum without (pod) (rate_counter({app="a"} | logfmt | unwrap amount [5m]))`, false, false},
		{`max(rate_counter({app="a"} | logfmt | unwrap amount [5m]))`, false, false},
		{`sum(rate({app="a"} | logfmt | unwrap amount [5m]))`, false, false},
		{`rate_counter({app="a"} | logfmt | unwrap amount [5m])`, false, false},
		{`sum(rate_counter({app="a"} | logfmt | unwrap amount [5m] offset 1h))`, false, false},
		{`sum(sum(rate_counter({app="a"} | logfmt | unwrap amount [5m])))`, false, false},
		{`not a query`, false, false},
	}
	for _, c := range cases {
		_, ungrouped, ok := rateCounterUnderSum(c.query)
		if ok != c.ok || (ok && ungrouped != c.ungrouped) {
			t.Errorf("%s: ok=%v ungrouped=%v, want %v %v", c.query, ok, ungrouped, c.ok, c.ungrouped)
		}
	}
}

// FuzzRateCounterOneSample: whatever the lines (ids drawn so series repeat or
// not), windows and steps, an answer given without reading rows is Loki's.
func FuzzRateCounterOneSample(f *testing.F) {
	f.Add(int64(1), false, "logfmt")
	f.Add(int64(2), true, "logfmt")
	f.Add(int64(3), false, "json")
	f.Fuzz(func(t *testing.T, seed int64, shared bool, parser string) {
		if parser != "json" {
			parser = "logfmt"
		}
		s := generateRateCounterScenario(rand.New(rand.NewSource(seed)), parser, shared)
		got, readRows := runRateCounterScenario(t, "v1.53.0", s, "0")
		if !readRows {
			compareRateCounter(t, "fuzz", s, got, lokiRateCounterSum(s.rows, int64(s.window), s.times(), s.group))
		}
	})
}
