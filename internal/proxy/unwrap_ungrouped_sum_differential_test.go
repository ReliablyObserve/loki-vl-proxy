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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins the answer of an ungrouped sum over a range function with
// unwrap to Loki's, over generated rows and generated windows, with a
// reference evaluator written apart from the proxy: Loki v3.7.7
// pkg/logql/evaluator.go VectorAggEvaluator.Next (a grouping of no labels folds
// every series of its input), pkg/logql/range_vector.go (rate over an unwrapped
// label is the sum of the values per second, sum_over_time their sum) over the
// window (t-range, t], and pkg/logql/log/metrics_extraction.go (a series is the
// stream plus the labels a parser extracted, less the unwrapped one).
//
// The proxy under test is the real handler. Only VictoriaLogs is simulated, by
// vlEmulator: the stats_query_range and stats_query endpoints over the three
// stats pipes the translation uses (stats, math <field>/<number>, stats), with
// VictoriaLogs' bucket semantics, and the raw /select/logsql/query rows.

type emuRow struct {
	ts     int64 // ns
	stream string
	labels map[string]string // the labels a parser extracts
	value  float64
	msg    string // the line; empty: "<field>=<value>"
	raw    string // the unwrapped text when it is not value's (a word, nan)
}

// text is the unwrapped label's text.
func (r emuRow) text() string {
	if r.raw != "" {
		return r.raw
	}
	return strconv.FormatFloat(r.value, 'f', -1, 64)
}

func (e *vlEmulator) line(row emuRow) string {
	if row.msg != "" {
		return row.msg
	}
	return fmt.Sprintf("%s=%s", e.field, row.text())
}

type emuCall struct{ path, query string }

type vlEmulator struct {
	t       testing.TB
	rows    []emuRow
	field   string // the unwrapped label
	version string // "" answers no version
	// deriveLevel is Loki's detected_level of a row (its stored level field and
	// line), applied where the translator's detected_level pipe chain is.
	deriveLevel func(level, msg string) string

	mu    sync.Mutex
	calls []emuCall
	// widest is the most series one stats response held for a single stats
	// function (the unwrap counter's group aside): VictoriaLogs' work and the
	// proxy's series budget scale with it, and an ungrouped sum needs one.
	widest int
}

func (e *vlEmulator) record(path, query string) {
	e.mu.Lock()
	e.calls = append(e.calls, emuCall{path, query})
	e.mu.Unlock()
}

// endpointCalls returns the queries the emulator received on path.
func (e *vlEmulator) endpointCalls(path string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	for _, c := range e.calls {
		if c.path == path {
			out = append(out, c.query)
		}
	}
	return out
}

func (e *vlEmulator) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
			if e.version != "" {
				_, _ = fmt.Fprintf(w, "vm_app_version{short_version=%q,version=%q} 1\n", e.version, e.version)
			}
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			e.record(r.URL.Path, r.FormValue("query"))
			e.serveStats(w, r)
		case "/select/logsql/query":
			e.record(r.URL.Path, r.FormValue("query"))
			e.serveRaw(w, r)
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
}

func vlTimeNs(e *vlEmulator, raw string) int64 {
	if raw == "" {
		return math.MinInt64
	}
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts.UnixNano()
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n < 1e11 { // whole seconds
			return n * 1e9
		}
		return n
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		e.t.Fatalf("emulator: unparsable time %q", raw)
	}
	return int64(f * 1e9)
}

func (e *vlEmulator) selectRows(r *http.Request) []emuRow {
	start := vlTimeNs(e, r.FormValue("start"))
	end := int64(math.MaxInt64)
	if raw := r.FormValue("end"); raw != "" {
		end = vlTimeNs(e, raw)
	}
	var out []emuRow
	for _, row := range e.rows {
		if row.ts >= start && row.ts < end {
			out = append(out, row)
		}
	}
	return out
}

func (e *vlEmulator) serveRaw(w http.ResponseWriter, r *http.Request) {
	rows := e.selectRows(r)
	sort.Slice(rows, func(i, j int) bool { return rows[i].ts < rows[j].ts })
	if strings.Contains(r.FormValue("query"), `| filter __lvp_bad:="1"`) {
		// The conversion check's lookup: the rows the counting gate flags 1.
		kept := rows[:0:0]
		for _, row := range rows {
			if _, err := strconv.ParseFloat(row.raw, 64); row.raw != "" && err != nil && !strings.EqualFold(row.raw, "nan") {
				kept = append(kept, row)
			}
		}
		rows = kept
	}
	if id, ok := strings.CutPrefix(r.FormValue("query"), "_stream_id:"); ok {
		// The stored-row read of the conversion check: the stream's rows.
		kept := rows[:0:0]
		for _, row := range rows {
			if strings.HasPrefix(id, strconv.Quote(row.stream)) {
				kept = append(kept, row)
			}
		}
		rows = kept
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, row := range rows {
		fields := map[string]string{
			"_time":      time.Unix(0, row.ts).UTC().Format(time.RFC3339Nano),
			"_stream":    row.stream,
			"_stream_id": row.stream,
			"_msg":       e.line(row),
			e.field:      row.text(),
		}
		for k, v := range row.labels {
			fields[k] = v
		}
		line, _ := json.Marshal(fields)
		_, _ = w.Write(append(line, '\n'))
	}
}

type emuRec struct {
	t int64
	f map[string]string
}

// emuPresentRE is a presence filter on a computed field: the translator adds it
// behind a copy of the value.
var emuPresentRE = regexp.MustCompile(`^filter (\w+):\*$`)

var (
	emuStatsRE = regexp.MustCompile(`^stats (?:by \(([^)]*)\) )?(.+)$`)
	emuMathRE  = regexp.MustCompile(`^math (\w+)/([0-9.]+) as (\w+)$`)
	// math <field> as <field>: the number the field holds, written to the result field.
	emuMathCopyRE = regexp.MustCompile(`^math (\w+) as (\w+)$`)
	// unpack_json from <field> fields (<name>): the named key of the JSON in the field.
	emuUnpackJSONRE = regexp.MustCompile(`^unpack_json from (\w+) fields \((\w+)\)$`)
	emuFuncRE       = regexp.MustCompile(`^(\w+)\(([^)]*)\)(?: as (\w+))?$`)
	// replace_regexp(`<expression>`, "<replacement>") at <field>: Go regexp, as VictoriaLogs runs it.
	emuReplaceRegexpRE = regexp.MustCompile("^replace_regexp\\(`(.*)`, \"(.*)\"\\) at (\\w+)$")

	emuStreamLabelRE = regexp.MustCompile(`(\w+)="([^"]*)"`)
	emuPhaseMathRE   = regexp.MustCompile(`^math \(\(_time - (\d+)\) % (\d+)\) as __lvp_window_phase$`)
	emuPhaseFilterRE = regexp.MustCompile(`^filter __lvp_window_phase:>0 __lvp_window_phase:<=(\d+)$`)
)

// serveStats runs the stats pipes of the query over the rows the request
// selects, the way VictoriaLogs does: on stats_query_range every stats pipe
// also groups by the time bucket [edge, edge+step), edges at k*step-offset.
func (e *vlEmulator) serveStats(w http.ResponseWriter, r *http.Request) {
	ranged := r.URL.Path == "/select/logsql/stats_query_range"
	var step, offset int64
	if ranged {
		d, err := time.ParseDuration(r.FormValue("step"))
		if err != nil {
			secs, perr := strconv.ParseFloat(r.FormValue("step"), 64)
			if perr != nil {
				e.t.Fatalf("emulator: step %q", r.FormValue("step"))
			}
			d = time.Duration(secs * float64(time.Second))
		}
		step = int64(d)
		if raw := r.FormValue("offset"); raw != "" {
			od, err := time.ParseDuration(raw)
			if err != nil {
				e.t.Fatalf("emulator: offset %q", raw)
			}
			offset = int64(od)
		}
	}
	pipes := strings.Split(r.FormValue("query"), " | ")
	first := -1
	for i, pipe := range pipes {
		if strings.HasPrefix(pipe, "stats ") {
			first = i
			break
		}
	}
	if first < 0 {
		e.t.Fatalf("emulator: no stats pipe in %s", r.FormValue("query"))
	}
	// The pipes before the first stats pipe select rows: the unwrap gate and
	// parsers keep every generated row (all values are numbers), and the window
	// phase filter of a window shorter than the step keeps the lines inside one.
	var phaseAnchor, phaseStep, phaseWindow int64
	derivedLevel := false
	for _, pipe := range pipes[:first] {
		// The translator's detected_level chain ends by writing the field: the
		// emulator applies Loki's rule to the row instead of running the pipes.
		if pipe == `format "<__dl>" as detected_level` {
			derivedLevel = true
		}
		if m := emuPhaseMathRE.FindStringSubmatch(pipe); m != nil {
			phaseAnchor, _ = strconv.ParseInt(m[1], 10, 64)
			phaseStep, _ = strconv.ParseInt(m[2], 10, 64)
		}
		if m := emuPhaseFilterRE.FindStringSubmatch(pipe); m != nil {
			phaseWindow, _ = strconv.ParseInt(m[1], 10, 64)
		}
	}
	var recs []emuRec
	for _, row := range e.selectRows(r) {
		if phaseStep > 0 {
			phase := (row.ts - phaseAnchor) % phaseStep
			if phase < 0 {
				phase += phaseStep
			}
			if phase <= 0 || phase > phaseWindow {
				continue
			}
		}
		f := map[string]string{"_stream": row.stream, "_msg": e.line(row), "__lvp_v": row.text(), "__lvp_bad": "0"}
		if row.raw != "" {
			// The counting gate (unwrapCountedGateMode): a nan is flagged 2 and a
			// value the conversion rejects 1, each with the value 0, when the
			// query counts them; the plain gate drops both.
			flag := "1"
			if _, err := strconv.ParseFloat(row.raw, 64); err == nil {
				flag = "0"
			} else if strings.EqualFold(row.raw, "nan") {
				flag = "2"
			}
			if flag != "0" {
				if !strings.Contains(r.FormValue("query"), `"`+flag+`" as __lvp_bad`) {
					continue
				}
				f["__lvp_bad"], f["__lvp_v"] = flag, "0"
			}
		}
		for _, m := range emuStreamLabelRE.FindAllStringSubmatch(row.stream, -1) { // stream fields are fields
			f[m[1]] = m[2]
		}
		for k, v := range row.labels {
			f[k] = v
		}
		if derivedLevel {
			f["detected_level"] = e.deriveLevel(f["level"], f["_msg"])
		}
		recs = append(recs, emuRec{t: row.ts, f: f})
	}
	var by, names []string
	for _, pipe := range pipes[first:] {
		if m := emuMathRE.FindStringSubmatch(pipe); m != nil {
			div, _ := strconv.ParseFloat(m[2], 64)
			for _, rec := range recs {
				v, _ := strconv.ParseFloat(rec.f[m[1]], 64)
				rec.f[m[3]] = strconv.FormatFloat(v/div, 'f', -1, 64)
			}
			// A math pipe after the last stats pipe adds a metric to the answer
			// (VictoriaLogs' stats_query accepts it, "since it adds additional
			// metrics"): one row per numeric field, the stats results included.
			if len(names) > 0 {
				names = append(names, m[3])
			}
			continue
		}
		if m := emuMathCopyRE.FindStringSubmatch(pipe); m != nil {
			for _, rec := range recs {
				v, err := strconv.ParseFloat(rec.f[m[1]], 64)
				if err != nil {
					e.t.Fatalf("emulator: math reads %q of %s, not a number (a row_* result is JSON)", rec.f[m[1]], m[1])
				}
				rec.f[m[2]] = strconv.FormatFloat(v, 'f', -1, 64)
			}
			continue
		}
		if m := emuUnpackJSONRE.FindStringSubmatch(pipe); m != nil {
			for _, rec := range recs {
				var obj map[string]string
				if err := json.Unmarshal([]byte(rec.f[m[1]]), &obj); err != nil {
					e.t.Fatalf("emulator: unpack_json of %q: %v", rec.f[m[1]], err)
				}
				rec.f[m[2]] = obj[m[2]]
			}
			continue
		}
		if m := emuReplaceRegexpRE.FindStringSubmatch(pipe); m != nil {
			re, err := regexp.Compile(m[1])
			if err != nil {
				e.t.Fatalf("emulator: replace_regexp %q: %v", m[1], err)
			}
			for _, rec := range recs {
				rec.f[m[3]] = re.ReplaceAllString(rec.f[m[3]], m[2])
			}
			continue
		}
		if m := emuPresentRE.FindStringSubmatch(pipe); m != nil {
			kept := recs[:0]
			for _, rec := range recs {
				if rec.f[m[1]] != "" {
					kept = append(kept, rec)
				}
			}
			recs = kept
			continue
		}
		m := emuStatsRE.FindStringSubmatch(pipe)
		if m == nil {
			e.t.Fatalf("emulator: unknown pipe %q in %s", pipe, r.FormValue("query"))
		}
		by = by[:0]
		for _, field := range strings.Split(m[1], ",") {
			if field = strings.TrimSpace(field); field != "" {
				by = append(by, field)
			}
		}
		type group struct {
			t    int64
			key  map[string]string
			recs []emuRec
		}
		groups := map[string]*group{}
		var order []string
		for _, rec := range recs {
			bucket := int64(0)
			if ranged {
				bucket = floorDiv(rec.t+offset, step)*step - offset
			}
			key := map[string]string{}
			id := strconv.FormatInt(bucket, 10)
			for _, field := range by {
				key[field] = rec.f[field]
				id += "\x00" + rec.f[field]
			}
			g := groups[id]
			if g == nil {
				g = &group{t: bucket, key: key}
				groups[id] = g
				order = append(order, id)
			}
			g.recs = append(g.recs, rec)
		}
		type fn struct{ kind, arg, alias string }
		var fns []fn
		for _, expr := range splitStatsFuncs(m[2]) {
			fm := emuFuncRE.FindStringSubmatch(strings.TrimSpace(expr))
			if fm == nil {
				e.t.Fatalf("emulator: unknown stats function %q in %s", expr, r.FormValue("query"))
			}
			alias := fm[3]
			if alias == "" {
				alias = fm[1] + "(" + fm[2] + ")"
			}
			fns = append(fns, fn{fm[1], fm[2], alias})
		}
		names = names[:0]
		onlyRows := true
		for _, f := range fns {
			names = append(names, f.alias)
			onlyRows = onlyRows && (f.kind == "row_min" || f.kind == "row_max")
		}
		if onlyRows && pipe == lastStatsPipe(pipes) {
			// VictoriaLogs reads a row_* result as a label (GetStatsLabelsAddGroupingByTime).
			http.Error(w, "missing metric fields in the results of query ["+r.FormValue("query")+"]", http.StatusBadRequest)
			return
		}
		var next []emuRec
		for _, id := range order {
			g := groups[id]
			f := map[string]string{}
			for k, v := range g.key {
				f[k] = v
			}
			for _, fx := range fns {
				if fx.kind == "row_min" || fx.kind == "row_max" {
					// row_min(_time, f) is the row of the earliest sample (row_max the
					// latest; the first row met wins a tie) as a JSON object of f.
					src, only, _ := strings.Cut(fx.arg, ", ")
					if src != "_time" || only == "" {
						e.t.Fatalf("emulator: %s(%s)", fx.kind, fx.arg)
					}
					pick := 0
					for i, rec := range g.recs {
						if (fx.kind == "row_min" && rec.t < g.recs[pick].t) || (fx.kind == "row_max" && rec.t > g.recs[pick].t) {
							pick = i
						}
					}
					enc, _ := json.Marshal(map[string]string{only: g.recs[pick].f[only]})
					f[fx.alias] = string(enc)
					continue
				}
				var acc float64
				for i, rec := range g.recs {
					v, _ := strconv.ParseFloat(rec.f[fx.arg], 64)
					switch fx.kind {
					case "sum":
						acc += v
					case "count":
						acc++
					case "sum_len":
						acc += float64(len(rec.f[fx.arg]))
					case "avg":
						acc += (v - acc) / float64(i+1)
					case "max":
						if i == 0 || v > acc {
							acc = v
						}
					case "min":
						if i == 0 || v < acc {
							acc = v
						}
					default:
						e.t.Fatalf("emulator: stats function %s", fx.kind)
					}
				}
				f[fx.alias] = strconv.FormatFloat(acc, 'f', -1, 64)
			}
			next = append(next, emuRec{t: g.t, f: f})
		}
		recs = next
	}

	type series struct {
		metric map[string]string
		values [][]any
	}
	bySeries := map[string]*series{}
	var seriesOrder []string
	for _, rec := range recs {
		for _, name := range names {
			metric := map[string]string{"__name__": name}
			id := name
			for _, field := range by {
				metric[field] = rec.f[field]
				id += "\x00" + rec.f[field]
			}
			s := bySeries[id]
			if s == nil {
				s = &series{metric: metric}
				bySeries[id] = s
				seriesOrder = append(seriesOrder, id)
			}
			if ranged {
				s.values = append(s.values, []any{float64(rec.t) / 1e9, rec.f[name]})
			} else {
				s.values = [][]any{{float64(vlTimeNs(e, r.FormValue("time"))) / 1e9, rec.f[name]}}
			}
		}
	}
	perName := map[string]map[string]bool{}
	for _, id := range seriesOrder {
		s := bySeries[id]
		labels := map[string]string{}
		for k, v := range s.metric {
			if k != "__lvp_bad" {
				labels[k] = v
			}
		}
		key, _ := json.Marshal(labels)
		if perName[s.metric["__name__"]] == nil {
			perName[s.metric["__name__"]] = map[string]bool{}
		}
		perName[s.metric["__name__"]][string(key)] = true
	}
	e.mu.Lock()
	for _, set := range perName {
		e.widest = max(e.widest, len(set))
	}
	e.mu.Unlock()
	result := make([]map[string]any, 0, len(seriesOrder))
	for _, id := range seriesOrder {
		s := bySeries[id]
		sort.Slice(s.values, func(i, j int) bool { return s.values[i][0].(float64) < s.values[j][0].(float64) })
		entry := map[string]any{"metric": s.metric}
		if ranged {
			entry["values"] = s.values
		} else {
			entry["value"] = s.values[0]
		}
		result = append(result, entry)
	}
	resultType := "vector"
	if ranged {
		resultType = "matrix"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
}

// splitStatsFuncs splits the functions of a stats pipe at the commas outside
// parentheses (row_min(_time, f) is one function).
func splitStatsFuncs(funcs string) []string {
	var out []string
	depth, from := 0, 0
	for i, c := range funcs {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(funcs[from:i]))
				from = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(funcs[from:]))
}

// lastStatsPipe is the last stats pipe of a query, the one VictoriaLogs reads the
// labels and the metrics of.
func lastStatsPipe(pipes []string) string {
	for i := len(pipes) - 1; i >= 0; i-- {
		if strings.HasPrefix(pipes[i], "stats ") {
			return pipes[i]
		}
	}
	return ""
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// lokiUnwrapSum is the reference: for every evaluation time t, the windows
// (t-window, t] of the series (stream plus parsed labels), the range function
// over each series' values, and the ungrouped sum over the series that have a
// sample. A time with no sample in any series has no point.
func lokiUnwrapSum(rows []emuRow, fn string, window int64, times []int64) map[int64]float64 {
	out := map[int64]float64{}
	for _, t := range times {
		perSeries := map[string][]float64{}
		for _, row := range rows {
			if row.ts <= t-window || row.ts > t {
				continue
			}
			labels := make([]string, 0, len(row.labels))
			for k, v := range row.labels {
				labels = append(labels, k+"="+v)
			}
			sort.Strings(labels)
			key := row.stream + "|" + strings.Join(labels, ",")
			perSeries[key] = append(perSeries[key], row.value)
		}
		if len(perSeries) == 0 {
			continue
		}
		var total float64
		for _, values := range perSeries {
			var sum float64
			for _, v := range values {
				sum += v
			}
			switch fn {
			case "rate":
				total += sum / (float64(window) / 1e9)
			case "sum_over_time":
				total += sum
			case "avg_over_time":
				total += sum / float64(len(values))
			case "max_over_time":
				total += slices.Max(values)
			case "min_over_time":
				total += slices.Min(values)
			default:
				panic("reference: " + fn)
			}
		}
		out[t] = total
	}
	return out
}

type uwScenario struct {
	fn, parser string
	window     time.Duration
	step       time.Duration // 0: instant
	start, end int64         // ns, step-aligned evaluation times
	rows       []emuRow
	wrap       func(string) string // wraps the query, e.g. in label_replace
	seriesCap  int                 // -max-stats-query-series; 0: the default
}

func (s uwScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

func (s uwScenario) query() string {
	pipeline := ""
	if s.parser != "" {
		pipeline = " | " + s.parser
	}
	q := fmt.Sprintf(`sum(%s({app="uw"}%s | unwrap amount [%s]))`, s.fn, pipeline, uwWindowText(s.window))
	if s.wrap != nil {
		q = s.wrap(q)
	}
	return q
}

func uwWindowText(d time.Duration) string {
	if d%time.Second != 0 {
		return strconv.Itoa(int(d/time.Millisecond)) + "ms"
	}
	if d%time.Minute == 0 {
		return strconv.Itoa(int(d/time.Minute)) + "m"
	}
	return strconv.Itoa(int(d/time.Second)) + "s"
}

// forceRawWindow makes the window one millisecond longer than the step, so the
// gcd(step, window) bucket grid is 1 ms wide and holds more than
// heavyQueryBucketThreshold buckets: the raw-sample evaluator answers, whatever
// the backend version. Instant scenarios have no grid and are left alone.
func (s uwScenario) forceRawWindow() uwScenario {
	if s.step > 0 {
		s.window = s.step + time.Millisecond
	}
	return s
}

func generateUnwrapScenario(rnd *rand.Rand, fn, parser string, instant bool) uwScenario {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute, 150 * time.Second}
	s := uwScenario{fn: fn, parser: parser, window: windows[rnd.Intn(len(windows))]}
	base := int64(1_800_000_000) * 1e9
	if !instant {
		s.step = steps[rnd.Intn(len(steps))]
		if rnd.Intn(3) == 0 {
			s.window = s.step // a tumbling window
		}
	}
	// Evaluation times on whole seconds, as Grafana sends them.
	s.start = base + int64(rnd.Intn(600))*1e9
	if instant {
		s.end = s.start
	} else {
		s.end = s.start + int64(rnd.Intn(8))*int64(s.step)
	}
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`, `{app="uw",pod="c"}`}
	labelSets := []map[string]string{nil, {"route": "/x"}, {"route": "/y"}}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 5 + rnd.Intn(60)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(8))*int64(max(s.step, time.Second))
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		row := emuRow{ts: ts, stream: streams[rnd.Intn(len(streams))], value: values[rnd.Intn(len(values))]}
		if s.parser != "" {
			row.labels = labelSets[rnd.Intn(len(labelSets))]
		}
		s.rows = append(s.rows, row)
	}
	return s
}

// runUnwrapScenario sends the scenario through the real handler against the
// emulated VictoriaLogs and returns timestamp -> value of the one series.
func runUnwrapScenario(t *testing.T, emu *vlEmulator, s uwScenario) (map[int64]float64, int) {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: s.seriesCap})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	query := s.query()
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if s.step == 0 {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(s.end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(s.start/1e9, 10))
		params.Set("end", strconv.FormatInt(s.end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(s.step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := map[int64]float64{}
	for _, series := range resp.Data.Result {
		if len(series.Metric) != 0 {
			t.Fatalf("%s: the series carries labels %v", query, series.Metric)
		}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[int64(point[0].(float64))*1e9] = v
		}
	}
	return got, len(resp.Data.Result)
}

func compareUnwrapSum(t *testing.T, label string, s uwScenario, got, want map[int64]float64, series int) {
	t.Helper()
	if series > 1 {
		t.Fatalf("%s: %d series, want one\nquery %s", label, series, s.query())
	}
	for ts, w := range want {
		g, ok := got[ts]
		if !ok {
			t.Fatalf("%s: no point at %d, Loki has %g\nquery %s step=%v window=%v rows=%d", label, ts/1e9, w, s.query(), s.step, s.window, len(s.rows))
		}
		if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
			t.Fatalf("%s: at %d got %g, Loki has %g\nquery %s step=%v window=%v rows=%d", label, ts/1e9, g, w, s.query(), s.step, s.window, len(s.rows))
		}
	}
	for ts, g := range got {
		if _, ok := want[ts]; !ok {
			t.Fatalf("%s: extra point at %d = %g, Loki has none\nquery %s step=%v window=%v rows=%d", label, ts/1e9, g, s.query(), s.step, s.window, len(s.rows))
		}
	}
}

// TestUngroupedSumUnwrapDifferential: over generated rows (several streams,
// parsed labels, zero values, lines on window edges) and generated windows
// (tumbling, sliding, shorter and longer than the step, a range that is not a
// multiple of the step, instant), the proxy's answer to an ungrouped
// `sum(rate(... | unwrap amount [r]))` is the one series Loki's vector aggregation
// makes of the per-series values, at every step, on every VictoriaLogs line the
// proxy supports (v1.49.0 and v1.52.0) and on an unknown version, which behaves
// as the floor. The answer comes from stats on buckets anchored with the offset
// argument; no raw rows are read. With raw=true a 1 ms bucket grid (see
// forceRawWindow) sends a query_range through the raw-sample evaluator, which
// must read the same (t-range, t] window (isLogRangeWindowFunc covers
// unwrap_rate) and call /select/logsql/query.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity, window-bounds-and-step-alignment
func TestUngroupedSumUnwrapDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.49.0", "v1.52.0"} {
		for _, parser := range []string{"", "logfmt", "json"} {
			for _, instant := range []bool{false, true} {
				for _, raw := range []bool{false, true} {
					if raw && instant {
						continue
					}
					name := fmt.Sprintf("version=%s/parser=%s/instant=%v/raw=%v", version, parser, instant, raw)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + 17))
						for i := 0; i < 25; i++ {
							s := generateUnwrapScenario(rnd, "rate", parser, instant)
							if raw {
								s = s.forceRawWindow()
							}
							emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
							got, series := runUnwrapScenario(t, emu, s)
							want := lokiUnwrapSum(s.rows, "rate", int64(s.window), s.times())
							compareUnwrapSum(t, fmt.Sprintf("%s #%d", name, i), s, got, want, series)
							rawRows := emu.endpointCalls("/select/logsql/query")
							switch {
							case raw && len(rawRows) == 0:
								t.Fatalf("%s #%d: expected the raw-sample evaluator, got stats calls only", name, i)
							case raw:
							case len(rawRows) > 0:
								t.Fatalf("%s #%d: the metric read raw rows: %s", name, i, rawRows[0])
							case emu.widest > 1:
								// One series from VictoriaLogs, not one per stream for the proxy to add up.
								t.Fatalf("%s #%d: VictoriaLogs answered %d series for an ungrouped sum", name, i, emu.widest)
							}
						}
					})
				}
			}
		}
	}
}

// TestUngroupedSumUnwrapPerSeriesFunctions: a function that takes a value per
// series (avg, max, min over time) sums those values over the series, at an
// instant, and the answer is the one series Loki's vector aggregation makes of
// them, whichever route answers. Every line is a series of its own here (a
// unique parsed label), which the stats route reads from `_msg` and matches
// Loki's values. The raw-row route (`| json`) keeps a series per stream, not per
// stream and parsed labels, so only its series count is pinned; the values are
// registered apart as semantics/unwrap-per-series-identity-raw-rows.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumUnwrapPerSeriesFunctions(t *testing.T) {
	for _, parser := range []string{"logfmt", "json"} {
		for _, fn := range []string{"avg_over_time", "max_over_time", "min_over_time"} {
			t.Run(parser+"/"+fn, func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(parser+fn)) * 104729))
				for i := 0; i < 20; i++ {
					s := generateUnwrapScenario(rnd, fn, parser, true)
					for j := range s.rows {
						s.rows[j].labels = map[string]string{"id": strconv.Itoa(j)}
						s.rows[j].msg = fmt.Sprintf("amount=%v id=%d", s.rows[j].value, j)
					}
					emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
					got, series := runUnwrapScenario(t, emu, s)
					want := lokiUnwrapSum(s.rows, fn, int64(s.window), s.times())
					if parser == "json" {
						if series > 1 || (len(want) == 1 && len(got) != 1) {
							t.Fatalf("%s/%s #%d: %d series with %d points, want one series with Loki's %d point(s)", parser, fn, i, series, len(got), len(want))
						}
						continue
					}
					compareUnwrapSum(t, fmt.Sprintf("%s/%s #%d", parser, fn, i), s, got, want, series)
				}
			})
		}
	}
}

// A `label_replace` around the sum, the form Grafana's transformations and
// dashboards add, changes nothing about the fold: one series, Loki's values.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumUnwrapUnderLabelReplace(t *testing.T) {
	wrap := func(q string) string { return `label_replace(` + q + `, "dst", "$1", "src", "(.*)")` }
	for _, parser := range []string{"logfmt", "json"} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", parser, instant), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(parser)) * 31337))
				for i := 0; i < 15; i++ {
					s := generateUnwrapScenario(rnd, "rate", parser, instant)
					s.wrap = wrap
					emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
					got, series := runUnwrapScenario(t, emu, s)
					compareUnwrapSum(t, fmt.Sprintf("label_replace %s #%d", parser, i), s, got, lokiUnwrapSum(s.rows, "rate", int64(s.window), s.times()), series)
				}
			})
		}
	}
}

// Loki counts max_query_series on the answer, so a sum over more streams than
// the limit is still one series when the function is taken per series and the
// proxy adds the streams up itself (rate_counter, or avg over a JSON stream).
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumUnwrapPerSeriesFoldIgnoresSeriesLimit(t *testing.T) {
	for _, tc := range []struct {
		fn      string
		instant bool
	}{{"rate_counter", false}, {"rate_counter", true}, {"avg_over_time", true}} {
		t.Run(fmt.Sprintf("%s/instant=%v", tc.fn, tc.instant), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(99))
			s := generateUnwrapScenario(rnd, tc.fn, "json", tc.instant)
			s.seriesCap = 2
			s.rows = s.rows[:0]
			base := s.end - int64(s.window)/2
			for i := 0; i < 12; i++ { // twelve streams, six times the limit
				s.rows = append(s.rows, emuRow{ts: base - int64(i)*1e9, stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i), value: float64(i + 1)})
				s.rows = append(s.rows, emuRow{ts: base - int64(i)*1e9 + 1e8, stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i), value: float64(i + 2)})
			}
			emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
			got, series := runUnwrapScenario(t, emu, s)
			if series != 1 || len(got) == 0 {
				t.Fatalf("%d series, %d points; want one series (Loki folds the streams before its limit)", series, len(got))
			}
		})
	}
}

// A grouped sum of a rate over an unwrapped label reads Loki's window
// (t-range, t]: a line on the lower edge belongs to the previous window, the
// line at t to this one. The raw-sample evaluator answers (a 1 ms bucket grid
// is over heavyQueryBucketThreshold, whatever the backend version) and must
// have read /select/logsql/query.
//
// conformance: semantics/unwrap-range-last-point, window-bounds-and-step-alignment
func TestGroupedSumUnwrapRateRawWindow(t *testing.T) {
	rnd := rand.New(rand.NewSource(424242))
	for i := 0; i < 30; i++ {
		s := generateUnwrapScenario(rnd, "rate", "", false).forceRawWindow()
		emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: "v1.49.0"}
		backend := httptest.NewServer(emu.handler())
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		query := fmt.Sprintf(`sum by (pod) (rate({app="uw"} | unwrap amount [%s]))`, uwWindowText(s.window))
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)}, "step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		backend.Close()
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, res.Code, res.Body)
		}
		if len(emu.endpointCalls("/select/logsql/query")) == 0 {
			t.Fatalf("%s: expected the raw-sample evaluator, got stats calls only", query)
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
			pts := map[int64]float64{}
			for _, point := range series.Values {
				v, _ := strconv.ParseFloat(point[1].(string), 64)
				pts[int64(point[0].(float64))*1e9] = v
			}
			got[series.Metric["pod"]] = pts
		}
		for _, pod := range []string{"a", "b", "c"} {
			var rows []emuRow
			for _, row := range s.rows {
				if row.stream == fmt.Sprintf(`{app="uw",pod=%q}`, pod) {
					rows = append(rows, row)
				}
			}
			want := lokiUnwrapSum(rows, "rate", int64(s.window), s.times())
			if len(want) == 0 && len(got[pod]) == 0 {
				continue
			}
			compareUnwrapSum(t, fmt.Sprintf("grouped #%d pod=%s", i, pod), s, got[pod], want, 1)
		}
	}
}
