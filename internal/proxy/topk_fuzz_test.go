package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// topKFuzzValues are the sample values a fuzz cell picks from (index 0 is "no
// sample at this step"). Ties, NaN and +Inf are over-represented on purpose.
var topKFuzzValues = []float64{math.NaN(), 1, 2, 2, 3, 5, 8, 0.5, math.Inf(1), math.NaN()}

// topKFuzzCase is one decoded fuzz input: a grid of samples by series and step,
// the ranking and the grouping clause.
type topKFuzzCase struct {
	series, steps int
	cells         [][]float64 // [series][step]
	present       [][]bool
	k             int
	descending    bool
	group         *logqlpkg.Grouping
	shuffleSeed   int64
}

func (c topKFuzzCase) app(i int) string { return "a" + strconv.Itoa(i/3) }
func (c topKFuzzCase) env(i int) string { return "e" + strconv.Itoa(i%3) }

// decodeTopKFuzz reads data as: series count, step count, k, mode, then one byte
// per cell (0: no sample, otherwise an index into topKFuzzValues).
func decodeTopKFuzz(data []byte) topKFuzzCase {
	at := func(i int) byte {
		if i < len(data) {
			return data[i]
		}
		return 0
	}
	c := topKFuzzCase{series: 1 + int(at(0))%9, steps: 1 + int(at(1))%5, k: int(at(2)) % 7, descending: at(3)&1 == 0, shuffleSeed: int64(at(3))}
	switch (at(3) >> 1) % 6 {
	case 1:
		c.group = &logqlpkg.Grouping{Labels: []string{"env"}}
	case 2:
		c.group = &logqlpkg.Grouping{Without: true, Labels: []string{"app"}}
	case 3:
		c.group = &logqlpkg.Grouping{Labels: []string{"app", "env"}}
	case 4:
		c.group = &logqlpkg.Grouping{Without: true}
	case 5:
		c.group = &logqlpkg.Grouping{Labels: []string{"zone"}} // a label no series has: one group
	}
	c.cells = make([][]float64, c.series)
	c.present = make([][]bool, c.series)
	for i := range c.cells {
		c.cells[i] = make([]float64, c.steps)
		c.present[i] = make([]bool, c.steps)
		for j := range c.cells[i] {
			if cell := int(at(4+i*c.steps+j)) % (len(topKFuzzValues) + 1); cell > 0 {
				c.cells[i][j], c.present[i][j] = topKFuzzValues[cell-1], true
			}
		}
	}
	return c
}

// refGroup is the group key of a series, written apart from topKGroupKey: the
// by() labels' values, or every label but the without() ones.
func (c topKFuzzCase) refGroup(s topKSeries) string {
	labels := map[string]string{"app": s.app, "env": s.env}
	if c.group == nil {
		return ""
	}
	var parts []string
	switch {
	case c.group.Without:
		excluded := map[string]bool{}
		for _, name := range c.group.Labels {
			excluded[name] = true
		}
		for _, name := range []string{"app", "env"} {
			if !excluded[name] {
				parts = append(parts, name+"="+labels[name])
			}
		}
	default:
		for _, name := range c.group.Labels {
			parts = append(parts, name+"="+labels[name])
		}
	}
	return strings.Join(parts, ",")
}

// refLess orders values for the reference tie check: NaN is last.
func refLess(a, b float64, descending bool) bool {
	switch {
	case math.IsNaN(a):
		return false
	case math.IsNaN(b):
		return true
	case descending:
		return a > b
	}
	return a < b
}

// topKFuzzStep is the samples of the answer at one step: series name -> value.
type topKFuzzStep map[string]float64

// checkStep compares what an implementation kept at one step with Loki's heap
// (lokiTopK) over the same vector, group by group: the kept values must be
// Loki's, and where no tie sits across the k-th place the kept series too.
func (c topKFuzzCase) checkStep(t *testing.T, impl string, step int, got topKFuzzStep) {
	t.Helper()
	var vec []topKSeries
	for i := 0; i < c.series; i++ {
		if c.present[i][step] {
			s := topKSeries{app: c.app(i), env: c.env(i), value: c.cells[i][step]}
			vec = append(vec, s)
		}
	}
	rng := rand.New(rand.NewSource(c.shuffleSeed + int64(step)))
	shuffled := append([]topKSeries(nil), vec...)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	var picked []topKSeries
	if c.k >= 1 {
		picked = lokiTopK(shuffled, c.k, c.descending, c.refGroup)
	}
	format := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	byGroup := func(values func(group string) []string) map[string]string {
		out := map[string]string{}
		for _, s := range vec {
			out[c.refGroup(s)] = ""
		}
		for g := range out {
			v := values(g)
			sort.Strings(v)
			out[g] = strings.Join(v, " ")
		}
		return out
	}
	want := byGroup(func(g string) []string {
		var v []string
		for _, s := range picked {
			if c.refGroup(s) == g {
				v = append(v, format(s.value))
			}
		}
		return v
	})
	have := byGroup(func(g string) []string {
		var v []string
		for _, s := range vec {
			if value, ok := got[s.app+"/"+s.env]; ok && c.refGroup(s) == g {
				v = append(v, format(value))
			}
		}
		return v
	})
	for kept := range got {
		found := false
		for _, s := range vec {
			if s.app+"/"+s.env == kept && format(s.value) == format(got[kept]) {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s step %d: kept %s=%v, which is no input sample (%+v)", impl, step, kept, got[kept], vec)
		}
	}
	if fmt.Sprint(want) != fmt.Sprint(have) {
		t.Fatalf("%s step %d k=%d desc=%v group=%+v: kept values %v, Loki's heap %v\ninput %+v", impl, step, c.k, c.descending, c.group, have, want, vec)
	}
	// A group with no tie across its k-th place keeps the same series as Loki.
	for g := range want {
		var members []topKSeries
		for _, s := range vec {
			if c.refGroup(s) == g {
				members = append(members, s)
			}
		}
		sort.SliceStable(members, func(i, j int) bool { return refLess(members[i].value, members[j].value, c.descending) })
		if c.k < 1 || len(members) <= c.k || format(members[c.k-1].value) == format(members[c.k].value) {
			continue
		}
		for _, s := range picked {
			if c.refGroup(s) != g {
				continue
			}
			if _, ok := got[s.app+"/"+s.env]; !ok {
				t.Fatalf("%s step %d k=%d group %q: Loki keeps %s/%s, the implementation dropped it (kept %v)", impl, step, c.k, g, s.app, s.env, got)
			}
		}
	}
}

// matrixBody renders the grid as a Loki matrix (one point per present cell).
func (c topKFuzzCase) matrixBody(start time.Time, step time.Duration) []byte {
	var result []map[string]any
	for i := 0; i < c.series; i++ {
		var values [][]any
		for j := 0; j < c.steps; j++ {
			if c.present[i][j] {
				values = append(values, []any{float64(start.Add(time.Duration(j) * step).Unix()), strconv.FormatFloat(c.cells[i][j], 'f', -1, 64)})
			}
		}
		if len(values) > 0 {
			result = append(result, map[string]any{"metric": map[string]string{"app": c.app(i), "env": c.env(i)}, "values": values})
		}
	}
	body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
	return body
}

// decodeMatrix reads a matrix answer as step -> "app/env" -> value.
func decodeTopKMatrix(t *testing.T, body []byte, start time.Time, step time.Duration) map[int]topKFuzzStep {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string
				Values [][]any
			}
		}
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	out := map[int]topKFuzzStep{}
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			at := int((int64(v[0].(float64)) - start.Unix()) / int64(step/time.Second))
			value, err := strconv.ParseFloat(v[1].(string), 64)
			if err != nil {
				t.Fatalf("value %v: %v", v[1], err)
			}
			if out[at] == nil {
				out[at] = topKFuzzStep{}
			}
			out[at][s.Metric["app"]+"/"+s.Metric["env"]] = value
		}
	}
	return out
}

// FuzzTopKPerStep: topk and bottomk rank every step on its own, inside each
// group of the by()/without() labels, with NaN last and ties kept to k
// (pkg/logql/evaluator.go VectorAggEvaluator.Next, pkg/logql/vector.go). The
// four places the proxy ranks (the bounded heap over the VictoriaLogs window
// stats, the filter over a finished matrix, the filter over an instant vector
// and the instant sort) must keep what Loki's heap keeps over the same vector, for any grid
// of samples (absent, tied, NaN, +Inf), any k from 0 to 6 (the heap is only
// reached with k above 0: Loki's parser rejects a smaller one), topk and
// bottomk, and every kind of clause.
//
// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, semantics/topk-bottomk-without-strips-label
func FuzzTopKPerStep(f *testing.F) {
	f.Add([]byte{5, 3, 2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6})
	f.Add([]byte{8, 4, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3})
	f.Add([]byte{6, 2, 3, 5, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10})
	f.Add([]byte{7, 5, 0, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2, 9, 2})
	f.Add([]byte{3, 1, 6, 11, 9, 9, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		c := decodeTopKFuzz(data)
		start, step := time.Unix(1700000000, 0), time.Minute

		// The filter over a finished matrix.
		matrix := decodeTopKMatrix(t, applyTopKToMatrix(c.matrixBody(start, step), c.k, c.descending, c.group), start, step)
		for j := 0; j < c.steps; j++ {
			c.checkStep(t, "applyTopKToMatrix", j, matrix[j])
		}

		// The bounded heap over the window stats (reached with k above 0 only).
		if c.k >= 1 {
			stats := &slidingStats{windowGrid: true, ids: map[string]int32{}, lokiNames: []string{"app", "env"}}
			for i := 0; i < c.series; i++ {
				stats.keys = append(stats.keys, c.app(i)+"\xff"+c.env(i))
				for j := 0; j < c.steps; j++ {
					if c.present[i][j] {
						stats.records = append(stats.records, slidingRecord{series: int32(i), grid: slidingGridA, index: int32(j), count: c.cells[i][j]})
					}
				}
			}
			body, err := stats.encodeTopK(context.Background(), false, 1, c.steps-1, 0, rangeTopK{k: c.k, descending: c.descending, group: c.group}, start, step, 1<<24)
			if err != nil {
				t.Fatalf("encodeTopK: %v", err)
			}
			heapped := decodeTopKMatrix(t, body, start, step)
			for j := 0; j < c.steps; j++ {
				c.checkStep(t, "encodeTopK", j, heapped[j])
			}
		}

		// The filters over an instant vector (the first step): the vector filter
		// and the instant sort with its per-group keep.
		var items []string
		for i := 0; i < c.series; i++ {
			if c.present[i][0] {
				items = append(items, fmt.Sprintf(`{"metric":{"app":%q,"env":%q},"value":[1,%q]}`, c.app(i), c.env(i), strconv.FormatFloat(c.cells[i][0], 'f', -1, 64)))
			}
		}
		body := []byte(`{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(items, ",") + `]}}`)
		name := "bottomk"
		if c.descending {
			name = "topk"
		}
		for impl, vector := range map[string][]byte{
			"applyTopKToVector":       applyTopKToVector(body, c.k, c.descending, c.group),
			"applyInstantSortTopkAgg": applyInstantSortTopkAgg(body, instantMetricPostAgg{name: name, k: c.k, group: c.group}),
		} {
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string
						Value  []any
					}
				}
			}
			if err := json.Unmarshal(vector, &resp); err != nil {
				t.Fatalf("%s: %v: %s", impl, err, vector)
			}
			got := topKFuzzStep{}
			for _, s := range resp.Data.Result {
				value, err := strconv.ParseFloat(s.Value[1].(string), 64)
				if err != nil {
					t.Fatal(err)
				}
				got[s.Metric["app"]+"/"+s.Metric["env"]] = value
			}
			c.checkStep(t, impl, 0, got)
		}
	})
}
