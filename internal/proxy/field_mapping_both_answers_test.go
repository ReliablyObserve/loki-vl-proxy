package proxy

import (
	"encoding/json"
	"maps"
	"net/url"
	"slices"
	"sort"
	"testing"
)

// bothAnswerSeries returns the label sets and values of a metric answer.
func bothAnswerSeries(t *testing.T, p *Proxy, path, query string) []map[string]string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {"1767225600"}, "step": {"60"}, "start": {"1767225300"}, "end": {"1767225600"}}
	var out struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(mappingGet(t, p, path, params), &out); err != nil {
		t.Fatal(err)
	}
	var series []map[string]string
	for _, r := range out.Data.Result {
		m := maps.Clone(r.Metric)
		if m == nil {
			m = map[string]string{}
		}
		v, _ := json.Marshal([]any{r.Value, r.Values})
		m["__value__"] = string(v)
		series = append(series, m)
	}
	sort.Slice(series, func(i, j int) bool { return canonicalLabelsKey(series[i]) < canonicalLabelsKey(series[j]) })
	return series
}

// In the both mode, metric answers are the alias mode's answers with the other
// names of a mapped field added beside its answer name, with its value, on
// series the query does not group: the same series, values, count and
// matching. A grouping, a matching, a without or a label_replace that names a
// field leaves its series as the alias mode answers them.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes, loki_api_v1_query, loki_api_v1_query_range
func TestFieldMappingLabelMode_BothSeriesMatchAlias(t *testing.T) {
	ungrouped := `count_over_time({pod="api-1"}[5m])`
	for _, tc := range []struct {
		query string
		// gains: the answer gains a name; never: names it must not carry.
		gains bool
		never []string
	}{
		{query: ungrouped, gains: true},
		{query: `sum by (pod) (` + ungrouped + `)`},
		{query: `sum by (k8s_pod_name) (` + ungrouped + `)`},
		{query: `sum by (pod, nodename) (` + ungrouped + `)`},
		{query: `sum without (service_name) (` + ungrouped + `)`, gains: true},
		{query: `sum without (nodename) (` + ungrouped + `)`, gains: true, never: []string{"nodename", "node", "k8s_node_name"}},
		// The alias mode keeps nodename (the answer name): so does a Loki that
		// stores both names; k8s_node_name is never added back.
		{query: `sum without (k8s_node_name) (` + ungrouped + `)`, gains: true, never: []string{"k8s_node_name"}},
		{query: `topk(1, ` + ungrouped + `)`, gains: true},
		{query: `sort_desc(` + ungrouped + `)`, gains: true},
		{query: `label_replace(` + ungrouped + `, "k8s_pod_name", "x", "", "")`, gains: true},
		{query: `label_replace(` + ungrouped + `, "dst", "$1", "pod", "(.*)")`, gains: true},
		{query: ungrouped + ` / ` + ungrouped, gains: true},
		{query: ungrouped + ` / ignoring(pod) ` + ungrouped, gains: true, never: []string{"pod", "k8s_pod_name"}},
		{query: ungrouped + ` / on(pod) ` + ungrouped},
		{query: `sum by (pod) (` + ungrouped + `) * on(pod) group_left(nodename) ` + ungrouped},
		{query: ungrouped + ` > 1`, gains: true},
	} {
		for _, path := range []string{"/loki/api/v1/query", "/loki/api/v1/query_range"} {
			vl := nameMatrixVL()
			backend := vl.server(t).URL
			proxyFor := func(mode FieldMappingLabelMode) *Proxy {
				return newMappingProxy(t, backend, nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
					FieldMappingLabelMode: mode, FieldMappings: nameMatrixMappings()})
			}
			alias := bothAnswerSeries(t, proxyFor(FieldMappingLabelModeAlias), path, tc.query)
			both := bothAnswerSeries(t, proxyFor(FieldMappingLabelModeBoth), path, tc.query)
			if len(alias) == 0 {
				t.Fatalf("%s %s: alias mode answered no series", path, tc.query)
			}
			if len(both) != len(alias) {
				t.Fatalf("%s %s: %d series, alias mode %d", path, tc.query, len(both), len(alias))
			}
			gained := false
			for i := range alias {
				stripped := maps.Clone(both[i])
				for _, extra := range []struct{ answer, name string }{
					{"pod", "k8s_pod_name"}, {"nodename", "node"}, {"nodename", "k8s_node_name"},
				} {
					if _, inAlias := alias[i][extra.name]; inAlias {
						continue
					}
					if v, ok := stripped[extra.name]; ok {
						if v != stripped[extra.answer] {
							t.Errorf("%s %s: %s=%q beside %s=%q", path, tc.query, extra.name, v, extra.answer, stripped[extra.answer])
						}
						delete(stripped, extra.name)
						gained = true
					}
				}
				if !maps.Equal(stripped, alias[i]) {
					t.Errorf("%s %s:\n both %v\nalias %v", path, tc.query, both[i], alias[i])
				}
				for _, name := range tc.never {
					if _, ok := both[i][name]; ok {
						t.Errorf("%s %s: carries %s: %v", path, tc.query, name, both[i])
					}
				}
			}
			if gained != tc.gains {
				t.Errorf("%s %s: gained names %v, want %v (%v)", path, tc.query, gained, tc.gains, both)
			}
		}
	}
}

// Log lines carry every listed name of a mapped stream field in the both
// mode, on the plain answer and on a cached repeat, with no VictoriaLogs call
// added; the other modes keep one name.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes, loki_api_v1_query_range, loki_api_v1_series
func TestFieldMappingLabelMode_BothStreamLabels(t *testing.T) {
	vl := nameMatrixVL()
	backend := vl.server(t).URL
	var aliasCalls map[string]int
	for _, tc := range []struct {
		mode FieldMappingLabelMode
		want []string
	}{
		{FieldMappingLabelModeAlias, []string{"nodename", "pod", "service_name"}},
		{FieldMappingLabelModeBoth, []string{"k8s_node_name", "k8s_pod_name", "node", "nodename", "pod", "service_name"}},
	} {
		mode, want := tc.mode, tc.want
		p := newMappingProxy(t, backend, nil, Config{LabelStyle: LabelStyleUnderscores, FieldMappingLabelMode: mode, FieldMappings: nameMatrixMappings()})
		var calls []map[string]int
		for run := 0; run < 2; run++ {
			vl.snapshot()
			if got := mappingAnswerLabels(t, p, "/loki/api/v1/query_range"); !slices.Equal(got, want) {
				t.Errorf("%s run %d: stream labels %v, want %v", mode, run, got, want)
			}
			if got := mappingAnswerLabels(t, p, "/loki/api/v1/series"); !slices.Equal(got, want) {
				t.Errorf("%s run %d: series labels %v, want %v", mode, run, got, want)
			}
			calls = append(calls, vl.snapshot())
		}
		if mode == FieldMappingLabelModeBoth {
			if !maps.Equal(calls[0], aliasCalls) {
				t.Errorf("both mode VictoriaLogs calls %v, alias mode %v", calls[0], aliasCalls)
			}
		} else {
			aliasCalls = calls[0]
		}
		if n := calls[1]["/select/logsql/query"]; n != 0 {
			t.Errorf("%s: the repeat queried VictoriaLogs %d times", mode, n)
		}
		// The tail labels.
		entry, ok := p.indexLabelTailEntry(map[string]any{"_time": "2026-01-01T00:00:00Z", "_msg": "x",
			"_stream": `{k8s.node.name="node-a",k8s.pod.name="api-1",service.name="api"}`}, nil)
		if !ok || !slices.Equal(slices.Sorted(maps.Keys(entry.stream)), want) {
			t.Errorf("%s: tail labels %v, want %v", mode, entry.stream, want)
		}
	}
}
