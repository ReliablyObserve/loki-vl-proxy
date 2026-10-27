package proxy

import (
	"encoding/json"
	"testing"
)

func vectorBody(series ...map[string]string) []byte {
	result := make([]map[string]any, 0, len(series))
	for _, s := range series {
		metric := map[string]string{}
		value := "0"
		for k, v := range s {
			if k == "value" {
				value = v
			} else {
				metric[k] = v
			}
		}
		result = append(result, map[string]any{"metric": metric, "value": []any{1.0, value}})
	}
	out, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	return out
}

func foldedVector(t *testing.T, body []byte) map[string]string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range resp.Data.Result {
		out[nvLabelsKey(r.Metric)] = r.Value[1].(string)
	}
	return out
}

// conformance: semantics/nested-aggregation-outer-by-native-routes
func TestApplyWithoutChainFoldsEveryLevel(t *testing.T) {
	rows := vectorBody(
		map[string]string{"pod": "a", "zone": "x", "value": "1"},
		map[string]string{"pod": "b", "zone": "x", "value": "3"},
		map[string]string{"pod": "c", "zone": "y", "value": "10"},
	)
	for _, tc := range []struct {
		name, logql string
		want        map[string]string
	}{
		{
			"innermost without, then by",
			`min by (zone) (avg without (pod) (count_over_time({a="b"}[1m])))`,
			map[string]string{"zone=x,": "2", "zone=y,": "10"},
		},
		{
			"by names a label the inner level dropped",
			`max by (pod) (sum without (pod) (count_over_time({a="b"}[1m])))`,
			map[string]string{"": "10"},
		},
		{
			"by () folds everything",
			`count by () (max without (pod) (count_over_time({a="b"}[1m])))`,
			map[string]string{"": "2"},
		},
		{
			"without over without",
			`sum without (zone) (max without (pod) (count_over_time({a="b"}[1m])))`,
			map[string]string{"": "13"},
		},
		{
			"trailing grouping spelling",
			`sum(max(count_over_time({a="b"}[1m])) without (pod)) by (zone)`,
			map[string]string{"zone=x,": "3", "zone=y,": "10"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := foldedVector(t, applyWithoutChain(rows, []string{"pod"}, tc.logql, withoutAggregationOp(tc.logql)))
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// conformance: semantics/nested-aggregation-outer-by-native-routes
func TestApplyWithoutChainKeepsTheSingleFold(t *testing.T) {
	rows := vectorBody(
		map[string]string{"pod": "a", "zone": "x", "value": "1"},
		map[string]string{"pod": "b", "zone": "x", "value": "3"},
	)
	for _, logql := range []string{
		`avg without (pod) (count_over_time({a="b"}[1m]))`,
		// a by level innermost: the translator answered it natively, no fold chain
		`min without (pod) (avg by (pod, zone) (count_over_time({a="b"}[1m])))`,
		// an operator the chain does not model
		`min by (zone) (stddev without (pod) (count_over_time({a="b"}[1m])))`,
	} {
		if chain, ok := withoutAggregationChain(logql); ok {
			t.Errorf("%s: folded as a chain of %d levels", logql, len(chain))
		}
		want := applyWithoutGrouping(rows, []string{"pod"}, withoutAggregationOp(logql))
		if got := applyWithoutChain(rows, []string{"pod"}, logql, withoutAggregationOp(logql)); string(got) != string(want) {
			t.Errorf("%s: %s, want the single fold %s", logql, got, want)
		}
	}
}

// A nested vector aggregation is read for its range function the way a single
// one is: the original range metric spec (function, window, base query, unwrap
// field and conversion) of `sum by (a) (sum by (a, b) (<range>))` and of
// `sum(<range>)` are the same, for pipelines the re-serialised syntax tree could
// alter (line filters, parsers, label filters, drop, unwrap conversions, offset).
//
// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
func TestNestedVectorRangeSpecMatchesTheSingleLevelSpec(t *testing.T) {
	for _, rng := range []string{
		`count_over_time({app="uw"}[5m])`,
		`rate({app="uw"} |= "GET" != "health" [1m])`,
		`rate({app="uw", ns=~"x|y"} |~ "a.+b" !~ "c[0-9]" [30s])`,
		`count_over_time({app="uw"} | logfmt | level="error" [5m])`,
		`count_over_time({app="uw"} | json a="b", c="d" | drop __error__ [5m])`,
		`count_over_time({app="uw"} | logfmt | drop __error__, __error_details__ [5m])`,
		`sum_over_time({app="uw"} | logfmt | unwrap amount [5m])`,
		`avg_over_time({app="uw"} | logfmt | unwrap duration(took) [5m])`,
		`max_over_time({app="uw"} | json | unwrap bytes(size) [5m])`,
		`rate({app="uw"} | logfmt | unwrap amount [5m])`,
		`count_over_time({app="uw"} | logfmt | line_format "{{.msg}}" [5m])`,
		`count_over_time({app="uw"} | logfmt | label_format x=y | x != "" [5m])`,
		`count_over_time({app="uw"} | pattern "<ip> <_>" | ip != "" [5m])`,
		`count_over_time({app="uw"} [5m] offset 1h)`,
		`bytes_over_time({app="uw"} |= "x" [90s])`,
		`rate({app="uw"} | regexp "(?P<code>[0-9]+)" [1m])`,
	} {
		single, singleOK := parseOriginalRangeMetricSpec("sum(" + rng + ")")
		for _, query := range []string{
			`sum by (zone) (sum by (pod, zone) (` + rng + `))`,
			`max(avg by (pod, zone) (` + rng + `)) by (zone)`,
			`sum(sum(` + rng + `) by (pod, zone)) by (zone)`,
			`min by (zone) (avg without (pod) (` + rng + `))`,
			`count by (zone) (sum without (svc) (max by (pod, zone, svc) (` + rng + `)))`,
		} {
			nested, nestedOK := parseOriginalRangeMetricSpec(query)
			if nestedOK != singleOK || nested != single {
				t.Errorf("%s\n nested %#v (%v)\n single %#v (%v)", query, nested, nestedOK, single, singleOK)
			}
		}
	}
}
