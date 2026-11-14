package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/metricparity"
)

// facetRanked is the ranked_values facet of a topk or bottomk answer. Loki ranks a tied cutoff in Go map
// order, so two correct answers may hold different series at the k-th place: the values at every step must
// still be the same, and so must every series that ranks strictly above the cutoff.
const facetRanked = "ranked_values"

// RankSpec is the outermost topk or bottomk of a query.
type RankSpec struct {
	K          int
	Descending bool // topk: the best are the largest
	Grouping   *logql.Grouping
}

// RankOf reads the ranking a query ends in; nil when its outermost operation is not topk or bottomk.
func RankOf(query string) *RankSpec {
	expr, err := logql.Parse(query)
	if err != nil {
		return nil
	}
	agg, ok := expr.(*logql.VectorAggregation)
	if !ok || (agg.Op != logql.VectorTopK && agg.Op != logql.VectorBottomK) || !agg.HasParam || agg.Param < 1 {
		return nil
	}
	return &RankSpec{K: int(agg.Param), Descending: agg.Op == logql.VectorTopK, Grouping: agg.Grouping}
}

// group is the grouping key of a series: the values of the by() labels, or the labels left by without().
func (s *RankSpec) group(labels map[string]string) string {
	if s.Grouping == nil {
		return ""
	}
	names := map[string]bool{}
	for _, l := range s.Grouping.Labels {
		names[l] = true
	}
	out := map[string]string{}
	for k, v := range labels {
		if names[k] != s.Grouping.Without && k != "__name__" {
			out[k] = v
		}
	}
	return canonical(out)
}

// decodeRanked reads a metric answer's samples, grouped as the ranking groups them: group -> series -> time -> value.
func decodeRanked(a Answer, spec *RankSpec) (map[string]map[string]map[string]string, error) {
	dec := json.NewDecoder(strings.NewReader(a.Body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("not a JSON object: %w", err)
	}
	samples, err := metricparity.DecodeSamples(doc)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]map[string]string{}
	for key, points := range samples {
		var labels map[string]string
		if err := json.Unmarshal([]byte(key), &labels); err != nil {
			return nil, err
		}
		g := spec.group(labels)
		if out[g] == nil {
			out[g] = map[string]map[string]string{}
		}
		out[g][key] = points
	}
	return out, nil
}

// better reports whether a ranks strictly above b.
func (s *RankSpec) better(a, b float64) bool {
	if s.Descending {
		return a > b && !sameSample(a, b)
	}
	return a < b && !sameSample(a, b)
}

func sameSample(a, b float64) bool {
	if a == b || (math.IsNaN(a) && math.IsNaN(b)) {
		return true
	}
	if math.IsNaN(a) || math.IsNaN(b) || math.IsInf(a, 0) || math.IsInf(b, 0) {
		return false
	}
	return math.Abs(a-b) <= math.Max(1e-9, 1e-6*math.Max(math.Abs(a), math.Abs(b)))
}

// RankedDifference compares two topk or bottomk answers within each group of the ranking: the ranked sample
// values of every step equal (metricparity.RankedValuesDifference, per group), and every series that ranks
// strictly above the cutoff of a full step is in both answers with the same value. Only the series tied at
// the cutoff may differ. A step with fewer than k series has no cutoff: its series must be the same. The empty
// string is no difference.
func RankedDifference(loki, proxy Answer, spec *RankSpec) string {
	lg, err := decodeRanked(loki, spec)
	if err != nil {
		return "Loki's answer is not a metric answer: " + err.Error()
	}
	pg, err := decodeRanked(proxy, spec)
	if err != nil {
		return "the proxy's answer is not a metric answer: " + err.Error()
	}
	groups := map[string]bool{}
	for g := range lg {
		groups[g] = true
	}
	for g := range pg {
		groups[g] = true
	}
	names := make([]string, 0, len(groups))
	for g := range groups {
		names = append(names, g)
	}
	sort.Strings(names)
	for _, g := range names {
		l, p := lg[g], pg[g]
		if l == nil || p == nil {
			return fmt.Sprintf("group %s: ranked by %s only", g, map[bool]string{true: "the proxy", false: "Loki"}[l == nil])
		}
		if err := metricparity.RankedValuesDifference(l, p, false); err != nil {
			return fmt.Sprintf("group %s: %v", g, err)
		}
		if msg := rankedSeriesDifference(l, p, spec); msg != "" {
			return fmt.Sprintf("group %s: %s", g, msg)
		}
	}
	return ""
}

// rankedSeriesDifference checks, step by step, the series that rank strictly above the cutoff.
func rankedSeriesDifference(loki, proxy map[string]map[string]string, spec *RankSpec) string {
	atStep := func(series map[string]map[string]string) map[string]map[string]float64 {
		steps := map[string]map[string]float64{}
		for key, points := range series {
			for ts, raw := range points {
				v, _ := strconv.ParseFloat(raw, 64)
				if steps[ts] == nil {
					steps[ts] = map[string]float64{}
				}
				steps[ts][key] = v
			}
		}
		return steps
	}
	l, p := atStep(loki), atStep(proxy)
	times := make([]string, 0, len(l))
	for ts := range l {
		times = append(times, ts)
	}
	sort.Strings(times)
	for _, ts := range times {
		members := l[ts]
		cutoff, full := 0.0, len(members) >= spec.K
		first := true
		for _, v := range members {
			if first || spec.better(cutoff, v) {
				cutoff, first = v, false
			}
		}
		for _, side := range []struct {
			name         string
			this, other  map[string]float64
			otherInLabel string
		}{{"Loki", members, p[ts], "the proxy"}, {"the proxy", p[ts], members, "Loki"}} {
			for key, v := range side.this {
				if full && !spec.better(v, cutoff) {
					continue // tied at the cutoff: either answer may hold it
				}
				if w, ok := side.other[key]; !ok || !sameSample(v, w) {
					return fmt.Sprintf("t=%s: %s ranks %s at %v, %s does not hold it there", ts, side.name, key, v, side.otherInLabel)
				}
			}
		}
	}
	return ""
}
