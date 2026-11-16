package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The reference vector aggregation (aggLayer.eval, which FuzzFoldBinaryAggregation and the over-aggregation
// differentials use) gives Loki's answer over the fuzz target's domain: label values holding `,`, `=` and quotes,
// absent labels, by() a label no series has, without() every label, every operator, negative and zero values, NaN
// and the infinities.
// Loki's answers are recorded with its engine (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/aggregation-over-binary-dropped
func TestVectorFoldReferenceMatchesRecordedLoki(t *testing.T) {
	const at = 1700000100
	ops := []string{"sum", "avg", "min", "max", "count"}
	names := []string{"job", "pod", "x"}
	values := []string{"", "a", "b", "a,b=c", `q"1`, "b,a"}
	layers := []aggLayer{{}, {hasBy: true, by: []string{"job"}}, {hasBy: true, by: []string{"job", "x"}}, {hasWith: true, without: []string{"pod"}},
		{hasWith: true, without: []string{"job", "pod", "x"}}, {hasBy: true, by: []string{"absent"}}}
	clause := func(l aggLayer) string {
		switch {
		case l.hasBy:
			return " by (" + strings.Join(l.by, ", ") + ")"
		case l.hasWith:
			return " without (" + strings.Join(l.without, ", ") + ")"
		}
		return ""
	}
	rng := rand.New(rand.NewSource(7))
	type tc struct {
		layer aggLayer
		vec   []topKFoldRefSample
	}
	var cases []tc
	var sets []oraclerun.EngineDataSet
	for i := 0; i < 300; i++ {
		layer := layers[rng.Intn(len(layers))]
		layer.op = ops[rng.Intn(len(ops))]
		seen := map[string]bool{}
		set := oraclerun.EngineDataSet{}
		var vec []topKFoldRefSample
		for j, n := 0, 1+rng.Intn(10); j < n; j++ {
			labels := map[string]string{}
			for _, name := range names {
				if v := values[rng.Intn(len(values))]; v != "" {
					labels[name] = v
				}
			}
			if seen[canonicalLabelText(labels)] {
				continue
			}
			seen[canonicalLabelText(labels)] = true
			v := float64(rng.Intn(41) - 20)
			if i%4 == 3 {
				v = []float64{math.NaN(), math.Inf(1), math.Inf(-1), v, v}[rng.Intn(5)]
			}
			vec = append(vec, topKFoldRefSample{labels, v})
			stream := map[string]string{"i": strconv.Itoa(j)}
			for k, lv := range labels {
				stream[k] = lv
			}
			set.Streams = append(set.Streams, oraclerun.EngineStream{Labels: stream, Entries: []oraclerun.EngineEntry{{TS: (at - 30) * 1e9, Line: "v=" + strconv.FormatFloat(v, 'f', -1, 64)}}})
		}
		query := layer.op + clause(layer) + ` (sum without (i) (last_over_time({i=~".+"} | logfmt | unwrap v [1m])))`
		set.Requests = []oraclerun.EngineRequest{{ID: strconv.Itoa(i), Instant: true, Form: url.Values{"query": {query}, "time": {strconv.Itoa(at)}}.Encode()}}
		cases = append(cases, tc{layer, vec})
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, "vector_fold_reference", sets)
	for i, c := range cases {
		id := strconv.Itoa(i)
		points, labels := lokiMetricPoints(t, id, answers[id])
		loki := map[string]float64{}
		for key, series := range points {
			for _, v := range series {
				loki[binaryLabelKey(labels[key])] = v
			}
		}
		ref, _ := c.layer.eval(c.vec)
		desc := fmt.Sprintf("case %d %s%s over %v", i, c.layer.op, clause(c.layer), c.vec)
		if len(ref) != len(loki) {
			t.Fatalf("%s: reference %v, Loki %v", desc, ref, loki)
		}
		for _, s := range ref {
			v, ok := loki[binaryLabelKey(s.labels)]
			if !ok || !(math.IsNaN(v) && math.IsNaN(s.value) || v == s.value || math.Abs(v-s.value) <= 1e-9*math.Max(1, math.Abs(v))) {
				t.Fatalf("%s: reference %v = %v, Loki %v (present %v)", desc, s.labels, s.value, v, ok)
			}
		}
	}
}
