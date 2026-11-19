package vlemu

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The fold of a parser's series that lie in one bucket (the proxy's lower.SeriesFold), one scan of
// /select/logsql/query: the bucket stats by the bucket on the read's grid, the read's grouping and the series hash;
// each row's value in its bucket; the series' buckets; a series in one bucket whose value is finite (and not a NaN
// line's) folded into a row per bucket and group with the outer aggregation's merge of the folded values and their
// count, every other series' bucket rows passed through as they are. The emulator runs the fold's meaning, not its
// pipes, and answers rows as /select/logsql/query does (one JSON object per line, empty fields left out).

// seriesFoldRowsField is the field the series stats of a folded read collect each series' bucket rows into.
const seriesFoldRowsField = "__lvp_rows"

var (
	// stats by (_time:<step> offset <offset>, <grouping>) <functions>: the fold's bucket stats.
	seriesFoldBucketRE = regexp.MustCompile(`^stats by \(_time:(\S+) offset (\S+), ([^)]*)\) (.+)$`)
	// math <a> [/ or * <b>] as __lvp_fold: a row's value in its bucket.
	seriesFoldValueRE = regexp.MustCompile(`^math (\w+)(?: ([/*]) (\w+))? as __lvp_fold$`)
	// stats by (_time, <grouping>) <functions>: the fold's last stats pipe.
	seriesFoldLastRE = regexp.MustCompile(`^stats by \(_time, ([^)]*)\) (.+)$`)
	// <outer>(__lvp_fold) as __lvp_fold: the merge of the folded values.
	seriesFoldOuterRE = regexp.MustCompile(`\b(sum|max|min)\(__lvp_fold\) as __lvp_fold\b`)
	// any(<column>) as <column>: a column the last stats pipe passes through.
	seriesFoldAnyRE = regexp.MustCompile(`\bany\((\w+)\) as (\w+)`)
)

// isSeriesFold reports a folded read.
func isSeriesFold(query string) bool {
	return strings.Contains(query, ") as "+seriesFoldRowsField)
}

// serveSeriesFold answers a folded read.
func (e *Emulator) serveSeriesFold(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("query")
	pipes := strings.Split(query, " | ")
	first := firstStatsPipe(pipes)
	if first < 0 {
		e.T.Fatalf("emulator: folded read without its bucket stats: %s", query)
	}
	bm := seriesFoldBucketRE.FindStringSubmatch(pipes[first])
	vm, lm, om := []string(nil), seriesFoldLastRE.FindStringSubmatch(pipes[len(pipes)-1]), seriesFoldOuterRE.FindStringSubmatch(pipes[len(pipes)-1])
	for _, pipe := range pipes[first+1:] {
		if m := seriesFoldValueRE.FindStringSubmatch(pipe); m != nil {
			vm = m
			break
		}
	}
	if bm == nil || vm == nil || lm == nil || om == nil {
		e.T.Fatalf("emulator: folded read %s", query)
	}
	step, err1 := time.ParseDuration(bm[1])
	offset, err2 := time.ParseDuration(bm[2])
	if err1 != nil || err2 != nil || step <= 0 {
		e.T.Fatalf("emulator: folded read grid %q %q", bm[1], bm[2])
	}
	by, labels := splitFields(bm[3]), splitFields(lm[1])
	var columns []string
	for _, m := range seriesFoldAnyRE.FindAllStringSubmatch(lm[2], -1) {
		columns = append(columns, m[2])
	}
	s, off := int64(step), int64(offset)
	rows := e.statsGroups(query, by, bm[4], e.statsRecs(r, pipes[:first]), func(t int64) int64 { return FloorDiv(t+off, s)*s - off })
	e.Mu.Lock()
	defer e.Mu.Unlock()
	e.Widest = max(e.Widest, writeSeriesFold(w, rows, by, labels, columns, vm, om[1]))
}

// writeSeriesFold answers the fold of rows (the bucket stats' rows: their bucket time and fields) as
// /select/logsql/query does, and returns the number of groups it answered (less the flag); vm is the value's math pipe
// (seriesFoldValueRE), outer the merge of the folded values.
func writeSeriesFold(w http.ResponseWriter, rows []emuRec, by, labels, columns []string, vm []string, outer string) int {

	// Each row's value, whether it folds, and its series' buckets.
	value := func(f map[string]string) float64 {
		a, _ := strconv.ParseFloat(f[vm[1]], 64)
		if vm[2] == "" {
			return a
		}
		b, _ := strconv.ParseFloat(f[vm[3]], 64)
		if vm[2] == "/" {
			return a / b
		}
		return a * b
	}
	seriesKey := func(f map[string]string) string {
		var b strings.Builder
		for _, field := range by {
			if field != "__lvp_bad" {
				b.WriteString(f[field] + "\x00")
			}
		}
		return b.String()
	}
	buckets := map[string]int{}
	for _, rec := range rows {
		buckets[seriesKey(rec.f)]++
	}

	type outRow struct {
		t      int64
		labels map[string]string
		cols   map[string]string
		fold   float64
		n      int
	}
	out := map[string]*outRow{}
	var order []string
	for _, rec := range rows {
		v := value(rec.f)
		folds := buckets[seriesKey(rec.f)] == 1 && rec.f["__lvp_bad"] != "2" && !math.IsNaN(v) && !math.IsInf(v, 0)
		lv := map[string]string{}
		id := strconv.FormatInt(rec.t, 10)
		for _, l := range labels {
			lv[l] = rec.f[l]
			if folds && l == seriesHashField {
				lv[l] = ""
			}
			id += "\x00" + lv[l]
		}
		o := out[id]
		if o == nil {
			o = &outRow{t: rec.t, labels: lv, cols: map[string]string{}}
			out[id] = o
			order = append(order, id)
		}
		o.n++
		if !folds {
			for _, c := range columns {
				o.cols[c] = rec.f[c]
			}
			o.fold = math.NaN()
			continue
		}
		switch {
		case o.n == 1:
			o.fold = v
		case outer == "max":
			o.fold = math.Max(o.fold, v)
		case outer == "min":
			o.fold = math.Min(o.fold, v)
		default:
			o.fold += v
		}
	}
	sort.Strings(order)
	widest := map[string]bool{}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, id := range order {
		o := out[id]
		fields := map[string]string{"_time": time.Unix(0, o.t).UTC().Format(time.RFC3339Nano)}
		var key []string
		for _, l := range labels {
			if o.labels[l] != "" {
				fields[l] = o.labels[l]
			}
			if l != "__lvp_bad" {
				key = append(key, o.labels[l])
			}
		}
		widest[strings.Join(key, "\x00")] = true
		for c, v := range o.cols {
			if v != "" {
				fields[c] = v
			}
		}
		fields["__lvp_fold"] = strconv.FormatFloat(o.fold, 'f', -1, 64)
		fields["__lvp_fc"] = strconv.Itoa(o.n)
		line, _ := json.Marshal(fields)
		_, _ = w.Write(append(line, '\n'))
	}
	return len(widest)
}

// splitFields splits a grouping list ("a, b") into its fields.
func splitFields(list string) []string {
	var out []string
	for _, f := range strings.Split(list, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// SeriesFoldCalls are the folded reads the emulator received (EndpointCalls leaves /select/logsql/query's stats reads
// out).
func (e *Emulator) SeriesFoldCalls() []string {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	var out []string
	for _, c := range e.Calls {
		if c.Path == "/select/logsql/query" && isSeriesFold(c.Query) {
			out = append(out, c.Query)
		}
	}
	return out
}

// FoldOverStats answers the folded reads sent to a fake VictoriaLogs that answers stats_query_range only (a test's
// own handler): each folded read is sent to inner as the stats_query_range read of every series (the base query
// and its bucket stats, on the read's grid), and its answer folded as the emulator folds rows. Every other request
// goes to inner as it is.
func FoldOverStats(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		if r.URL.Path != "/select/logsql/query" || !isSeriesFold(query) {
			inner.ServeHTTP(w, r)
			return
		}
		pipes := strings.Split(query, " | ")
		first := firstStatsPipe(pipes)
		var bm, vm []string
		if first >= 0 {
			bm = seriesFoldBucketRE.FindStringSubmatch(pipes[first])
			for _, pipe := range pipes[first+1:] {
				if m := seriesFoldValueRE.FindStringSubmatch(pipe); m != nil {
					vm = m
					break
				}
			}
		}
		lm, om := seriesFoldLastRE.FindStringSubmatch(pipes[len(pipes)-1]), seriesFoldOuterRE.FindStringSubmatch(pipes[len(pipes)-1])
		if bm == nil || vm == nil || lm == nil || om == nil {
			http.Error(w, "fake: folded read "+query, http.StatusBadRequest)
			return
		}
		form := url.Values{}
		for k, v := range r.Form {
			form[k] = v
		}
		form.Set("query", strings.Join(pipes[:first], " | ")+" | stats by ("+bm[3]+") "+bm[4])
		form.Set("step", bm[1])
		form.Set("offset", bm[2])
		req := httptest.NewRequest(http.MethodPost, "/select/logsql/stats_query_range", strings.NewReader(form.Encode()))
		req.Header = r.Header.Clone()
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		var answer struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][2]any          `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil {
			http.Error(w, "fake: "+err.Error(), http.StatusInternalServerError)
			return
		}
		by := splitFields(bm[3])
		rowAt := map[string]*emuRec{}
		var rows []emuRec
		var order []string
		for _, s := range answer.Data.Result {
			for _, p := range s.Values {
				secs, _ := p[0].(float64)
				v, _ := p[1].(string)
				t := int64(math.Round(secs * 1e9))
				id := strconv.FormatInt(t, 10)
				for _, f := range by {
					id += "\x00" + s.Metric[f]
				}
				rec := rowAt[id]
				if rec == nil {
					rec = &emuRec{t: t, f: map[string]string{}}
					for _, f := range by {
						rec.f[f] = s.Metric[f]
					}
					if rec.f[seriesHashField] == "" {
						// A fake that does not group by the series hash answers a series per group: its hash is the
						// group's labels (less the flag).
						sid := "fake"
						for _, f := range by {
							if f != "__lvp_bad" && f != seriesHashField {
								sid += "\x00" + s.Metric[f]
							}
						}
						rec.f[seriesHashField] = sid
					}
					rowAt[id] = rec
					order = append(order, id)
				}
				rec.f[s.Metric["__name__"]] = v
			}
		}
		for _, id := range order {
			rows = append(rows, *rowAt[id])
		}
		var columns []string
		for _, m := range seriesFoldAnyRE.FindAllStringSubmatch(lm[2], -1) {
			columns = append(columns, m[2])
		}
		writeSeriesFold(w, rows, by, splitFields(lm[1]), columns, vm, om[1])
	})
}
