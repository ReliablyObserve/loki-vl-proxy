package translator

import "testing"

// conformance: metric-series-identity
// count/min/max/avg of a range aggregation read one value per Loki series, so
// the inner stats pipe keeps the series identity and the outer pipe
// aggregates those values. sum stays a single pipe: summing the values of the
// series of a group is the same as counting their rows.
func TestSeriesLevelOuterAggregationsKeepSeriesIdentity(t *testing.T) {
	for _, tc := range []struct{ logql, want string }{
		{
			`count by (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (pod, _stream, level) count() as __lvp_inner | stats by (pod) count()`,
		},
		{
			`max by (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (pod, _stream, level) count() as __lvp_inner | stats by (pod) max(__lvp_inner)`,
		},
		{
			`avg by (pod) (bytes_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (pod, _stream, level) sum_len(_msg) as __lvp_inner | stats by (pod) avg(__lvp_inner)`,
		},
		{
			`min(count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner | stats min(__lvp_inner)`,
		},
		{
			`count(count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner | stats count()`,
		},
		{
			// With a parser stage the identity is the stream: the keys of
			// `| json` and `| logfmt` are known only once a line is read, so
			// Loki's per-parsed-label series are a documented deviation.
			`count by (pod) (count_over_time({app="x"} | json [1m]))`,
			`app:="x" | unpack_json | stats by (pod, _stream) count() as __lvp_inner | stats by (pod) count()`,
		},
		{
			// Counting series does not read the per-second value.
			`count by (pod) (rate({app="x"}[1m]))`,
			`app:="x" | stats by (pod, _stream, level) count() as __lvp_inner | stats by (pod) count()`,
		},
		{
			`max by (pod) (rate({app="x"}[1m]))`,
			`app:="x" | stats by (pod, _stream, level) count() as __lvp_inner | math __lvp_inner/60 as __lvp_rate | stats by (pod) max(__lvp_rate)`,
		},
		{
			// `without` keeps the identity in the outer pipe: the proxy drops
			// the excluded labels and aggregates the rows that remain.
			`count without (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner | stats by (_stream, level) count()` + WithoutMarkerSuffix + `pod`,
		},
		{
			`max without (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner | stats by (_stream, level) max(__lvp_inner)` + WithoutMarkerSuffix + `pod`,
		},
		{
			// sum without keeps the single pipe it always had.
			`sum without (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats count()` + WithoutMarkerSuffix + `pod`,
		},
		{
			`max by (pod) (max_over_time({app="x"} | unwrap d [1m]))`,
			`app:="x" | stats by (pod, _stream, level) max(d) as __lvp_inner | stats by (pod) max(__lvp_inner)`,
		},
		{
			// sum aggregates rows, so it keeps the single stats pipe.
			`sum by (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (pod) count()`,
		},
	} {
		got, err := TranslateLogQL(tc.logql)
		if err != nil || got != tc.want {
			t.Errorf("%s:\n got %s (%v)\nwant %s", tc.logql, got, err, tc.want)
		}
	}
}

func TestJoinByLabels(t *testing.T) {
	for _, tc := range []struct{ by, identity, want string }{
		{"pod", "_stream, level", "pod, _stream, level"},
		{"", "_stream", "_stream"},
		{"pod, _stream", "_stream, level", "pod, _stream, level"},
		{emptyByGrouping, "_stream, level", emptyByGrouping},
	} {
		if got := joinByLabels(tc.by, tc.identity); got != tc.want {
			t.Errorf("joinByLabels(%q, %q) = %q, want %q", tc.by, tc.identity, got, tc.want)
		}
	}
}
