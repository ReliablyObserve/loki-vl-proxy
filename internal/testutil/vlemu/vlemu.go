// Package vlemu is an in-memory VictoriaLogs for tests: an http.Handler that
// answers the endpoints the proxy reads (stats_query_range, stats_query, the
// raw /select/logsql/query rows, /health, /metrics) over generated rows, with
// VictoriaLogs' bucket semantics. It is imported only from _test.go files, so
// the same emulated backend can drive answer grids outside internal/proxy.
package vlemu

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Row is one generated log line.
type Row struct {
	TS     int64 // ns
	Stream string
	Labels map[string]string // the labels a parser extracts
	Value  float64
	Msg    string // the line; empty: "<field>=<value>"
	Raw    string // the unwrapped text when it is not value's (a word, nan)
}

// Text is the unwrapped label's text.
func (r Row) Text() string {
	if r.Raw != "" {
		return r.Raw
	}
	return strconv.FormatFloat(r.Value, 'f', -1, 64)
}

// Line is the row's log line: Msg, or "<field>=<value>" when Msg is empty.
func (e *Emulator) Line(row Row) string {
	if row.Msg != "" {
		return row.Msg
	}
	return fmt.Sprintf("%s=%s", e.Field, row.Text())
}

// Call is one request the emulator received.
type Call struct{ Path, Query string }

// Emulator is the in-memory VictoriaLogs; build it with a struct literal and
// serve Handler from an httptest server.
type Emulator struct {
	T       testing.TB
	Rows    []Row
	Field   string // the unwrapped label
	Version string // "" answers no version
	// FloatPhase evaluates the window filter in float64 as VictoriaLogs does
	// (WindowPhaseKeeps); otherwise as the ideal integer arithmetic.
	FloatPhase bool
	// DeriveLevel is Loki's detected_level of a row (its stored level field and
	// line), applied where the translator's detected_level pipe chain is.
	DeriveLevel func(level, msg string) string

	Mu    sync.Mutex
	Calls []Call
	// Widest is the most series one stats response held for a single stats
	// function (the unwrap counter's group aside): VictoriaLogs' work and the
	// proxy's series budget scale with it, and an ungrouped sum needs one.
	Widest int
}

func (e *Emulator) LevelOf(level, msg string) string {
	if e.DeriveLevel != nil {
		return e.DeriveLevel(level, msg)
	}
	if level != "" {
		return level
	}
	return "unknown"
}

func (e *Emulator) Record(path, query string) {
	e.Mu.Lock()
	e.Calls = append(e.Calls, Call{path, query})
	e.Mu.Unlock()
}

// EndpointCalls returns the queries the emulator received on path.
func (e *Emulator) EndpointCalls(path string) []string {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	var out []string
	for _, c := range e.Calls {
		if c.Path == path {
			// /query also carries a bounded stats-only field-presence probe.
			// Callers asking for raw-row reads mean the queries that return rows,
			// not this aggregate probe.
			if path == "/select/logsql/query" && strings.Contains(c.Query, "| stats ") {
				continue
			}
			out = append(out, c.Query)
		}
	}
	return out
}

func (e *Emulator) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
			if e.Version != "" {
				_, _ = fmt.Fprintf(w, "vm_app_version{short_version=%q,version=%q} 1\n", e.Version, e.Version)
			}
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			e.Record(r.URL.Path, r.FormValue("query"))
			e.ServeStats(w, r)
		case "/select/logsql/query":
			e.Record(r.URL.Path, r.FormValue("query"))
			e.serveRaw(w, r)
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
}

func vlTimeNs(e *Emulator, raw string) int64 {
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
		e.T.Fatalf("emulator: unparsable time %q", raw)
	}
	return int64(f * 1e9)
}

func (e *Emulator) SelectRows(r *http.Request) []Row {
	start := vlTimeNs(e, r.FormValue("start"))
	end := int64(math.MaxInt64)
	if raw := r.FormValue("end"); raw != "" {
		end = vlTimeNs(e, raw)
	}
	var out []Row
	for _, row := range e.Rows {
		if row.TS >= start && row.TS < end {
			out = append(out, row)
		}
	}
	return out
}

func (e *Emulator) serveRaw(w http.ResponseWriter, r *http.Request) {
	rows := e.SelectRows(r)
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	if strings.Contains(r.FormValue("query"), `| filter __lvp_bad:="1"`) {
		// The conversion check's lookup: the rows the counting gate flags 1.
		kept := rows[:0:0]
		for _, row := range rows {
			if _, err := strconv.ParseFloat(row.Raw, 64); row.Raw != "" && err != nil && !strings.EqualFold(row.Raw, "nan") {
				kept = append(kept, row)
			}
		}
		rows = kept
	}
	if id, ok := strings.CutPrefix(r.FormValue("query"), "_stream_id:"); ok {
		// The stored-row read of the conversion check: the stream's rows.
		kept := rows[:0:0]
		for _, row := range rows {
			if strings.HasPrefix(id, strconv.Quote(row.Stream)) {
				kept = append(kept, row)
			}
		}
		rows = kept
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, row := range rows {
		fields := map[string]string{
			"_time":      time.Unix(0, row.TS).UTC().Format(time.RFC3339Nano),
			"_stream":    row.Stream,
			"_stream_id": row.Stream,
			"_msg":       e.Line(row),
			e.Field:      row.Text(),
		}
		for k, v := range row.Labels {
			fields[k] = v
		}
		if strings.Contains(r.FormValue("query"), `format "<__dl>" as detected_level`) {
			fields["detected_level"] = e.LevelOf(fields["level"], fields["_msg"])
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

	// StreamLabelRE reads the name="value" pairs of a stream.
	StreamLabelRE = regexp.MustCompile(`(\w+)="([^"]*)"`)
	// PhaseMathRE and PhaseFilterRE read the window-phase pipes the proxy writes
	// for a window shorter than the step (see WindowPhaseKeeps).
	PhaseMathRE   = regexp.MustCompile(`^math \(\(_time - (\d+)\) % (\d+)\) as __lvp_window_phase$`)
	PhaseFilterRE = regexp.MustCompile(`^filter __lvp_window_phase:>(\d+) __lvp_window_phase:<=(\d+)$`)
)

// ServeStats runs the stats pipes of the query over the rows the request
// selects, the way VictoriaLogs does: on stats_query_range every stats pipe
// also groups by the time bucket [edge, edge+step), edges at k*step-offset.
func (e *Emulator) ServeStats(w http.ResponseWriter, r *http.Request) {
	ranged := r.URL.Path == "/select/logsql/stats_query_range"
	var step, offset int64
	if ranged {
		d, err := time.ParseDuration(r.FormValue("step"))
		if err != nil {
			secs, perr := strconv.ParseFloat(r.FormValue("step"), 64)
			if perr != nil {
				e.T.Fatalf("emulator: step %q", r.FormValue("step"))
			}
			d = time.Duration(secs * float64(time.Second))
		}
		step = int64(d)
		if raw := r.FormValue("offset"); raw != "" {
			od, err := time.ParseDuration(raw)
			if err != nil {
				e.T.Fatalf("emulator: offset %q", raw)
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
		e.T.Fatalf("emulator: no stats pipe in %s", r.FormValue("query"))
	}
	// The pipes before the first stats pipe select rows: the unwrap gate and
	// parsers keep every generated row (all values are numbers), and the window
	// phase filter of a window shorter than the step keeps the lines inside one.
	var phaseAnchor, phaseStep, phaseLower, phaseUpper int64
	// fieldOps are the pipes before the first stats pipe that change fields, in
	// order: the translator's detected_level chain ends by writing the field (the
	// emulator applies Loki's rule to the row instead of running its pipes), and
	// a delete pipe removes the fields it names (`| drop detected_level`).
	var fieldOps []func(f map[string]string)
	for _, pipe := range pipes[:first] {
		if pipe == `format "<__dl>" as detected_level` {
			fieldOps = append(fieldOps, func(f map[string]string) { f["detected_level"] = e.LevelOf(f["level"], f["_msg"]) })
		}
		if names, ok := strings.CutPrefix(pipe, "delete "); ok {
			fieldOps = append(fieldOps, func(f map[string]string) {
				for _, name := range strings.Split(names, ", ") {
					if !strings.HasSuffix(name, "*") {
						delete(f, name)
					}
				}
			})
		}
		if m := PhaseMathRE.FindStringSubmatch(pipe); m != nil {
			phaseAnchor, _ = strconv.ParseInt(m[1], 10, 64)
			phaseStep, _ = strconv.ParseInt(m[2], 10, 64)
		}
		if m := PhaseFilterRE.FindStringSubmatch(pipe); m != nil {
			phaseLower, _ = strconv.ParseInt(m[1], 10, 64)
			phaseUpper, _ = strconv.ParseInt(m[2], 10, 64)
		}
	}
	var recs []emuRec
	for _, row := range e.SelectRows(r) {
		if phaseStep > 0 {
			if !WindowPhaseKeeps(row.TS, phaseAnchor, phaseStep, phaseLower, phaseUpper, e.FloatPhase) {
				continue
			}
		}
		f := map[string]string{"_stream": row.Stream, "_msg": e.Line(row), "__lvp_v": row.Text(), "__lvp_bad": "0"}
		if row.Raw != "" {
			// The counting gate (unwrapCountedGateMode): a nan is flagged 2 and a
			// value the conversion rejects 1, each with the value 0, when the
			// query counts them; the plain gate drops both.
			flag := "1"
			if _, err := strconv.ParseFloat(row.Raw, 64); err == nil {
				flag = "0"
			} else if strings.EqualFold(row.Raw, "nan") {
				flag = "2"
			}
			if flag != "0" {
				if !strings.Contains(r.FormValue("query"), `"`+flag+`" as __lvp_bad`) {
					continue
				}
				f["__lvp_bad"], f["__lvp_v"] = flag, "0"
			}
		}
		for _, m := range StreamLabelRE.FindAllStringSubmatch(row.Stream, -1) { // stream fields are fields
			f[m[1]] = m[2]
		}
		for k, v := range row.Labels {
			f[k] = v
		}
		for _, op := range fieldOps {
			op(f)
		}
		recs = append(recs, emuRec{t: row.TS, f: f})
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
					e.T.Fatalf("emulator: math reads %q of %s, not a number (a row_* result is JSON)", rec.f[m[1]], m[1])
				}
				rec.f[m[2]] = strconv.FormatFloat(v, 'f', -1, 64)
			}
			continue
		}
		if m := emuUnpackJSONRE.FindStringSubmatch(pipe); m != nil {
			for _, rec := range recs {
				var obj map[string]string
				if err := json.Unmarshal([]byte(rec.f[m[1]]), &obj); err != nil {
					e.T.Fatalf("emulator: unpack_json of %q: %v", rec.f[m[1]], err)
				}
				rec.f[m[2]] = obj[m[2]]
			}
			continue
		}
		if m := emuReplaceRegexpRE.FindStringSubmatch(pipe); m != nil {
			re, err := regexp.Compile(m[1])
			if err != nil {
				e.T.Fatalf("emulator: replace_regexp %q: %v", m[1], err)
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
			e.T.Fatalf("emulator: unknown pipe %q in %s", pipe, r.FormValue("query"))
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
				bucket = FloorDiv(rec.t+offset, step)*step - offset
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
				e.T.Fatalf("emulator: unknown stats function %q in %s", expr, r.FormValue("query"))
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
						e.T.Fatalf("emulator: %s(%s)", fx.kind, fx.arg)
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
						e.T.Fatalf("emulator: stats function %s", fx.kind)
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
	e.Mu.Lock()
	for _, set := range perName {
		e.Widest = max(e.Widest, len(set))
	}
	e.Mu.Unlock()
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

func FloorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// WindowPhaseKeeps evaluates the filter windowPhaseFilter writes for a line at
// ts. With float false it is the ideal arithmetic the filter stands for: the
// phase past the window start, exact in integers, kept when 0 < phase <= window
// (the anchor's multiple-of-256 remainder and the 128 ns of tolerance are taken
// out again). With float true it is what VictoriaLogs runs: math in float64,
// which reads a time near 1.8e18 ns as the nearest multiple of 256 and takes
// the remainder with math.Mod.
func WindowPhaseKeeps(ts, anchor, step, lower, upper int64, float bool) bool {
	if float {
		phase := math.Mod(float64(ts)-float64(anchor), float64(step))
		return phase > float64(lower) && phase <= float64(upper)
	}
	rest, window := lower-128, upper-lower
	phase := (ts - anchor - rest) % step
	if phase < 0 {
		phase += step
	}
	return phase > 0 && phase <= window
}
