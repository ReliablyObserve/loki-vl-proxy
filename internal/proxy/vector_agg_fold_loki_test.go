package proxy

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// Every fold edge case's expected answer is Loki's: its input series become Loki streams whose line, 30 s before
// each point, holds the point's value (read back with last_over_time over an unwrap; a series' detected_level is
// structured metadata, as Loki stores it), and the case's aggregation over them answers the case's want on
// query_range (steps 60, 120, 180) and at the 180 instant (test/oracle/loki engine; LVP_RUN_ORACLES=1 records
// again).
//
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity
func TestFoldByGroupingCasesMatchRecordedLoki(t *testing.T) {
	cases := foldByGroupingCases()
	var sets []oraclerun.EngineDataSet
	for i, tc := range cases {
		set := oraclerun.EngineDataSet{}
		for j, s := range tc.series {
			labels := map[string]string{"i": strconv.Itoa(j)}
			var metadata map[string]string
			for k, v := range s.metric {
				if k == "detected_level" {
					metadata = map[string]string{k: v}
					continue
				}
				labels[k] = v
			}
			st := oraclerun.EngineStream{Labels: labels}
			for _, p := range s.points {
				st.Entries = append(st.Entries, oraclerun.EngineEntry{TS: (int64(p[0]) - 30) * 1e9, Line: "v=" + strconv.FormatFloat(p[1], 'f', -1, 64), Metadata: metadata})
			}
			set.Streams = append(set.Streams, st)
		}
		grouping := ""
		if tc.by != nil {
			grouping = " by (" + strings.Join(tc.by, ", ") + ")"
		}
		query := tc.op + grouping + ` (sum without (i) (last_over_time({i=~".+"} | logfmt | unwrap v [1m])))`
		set.Requests = []oraclerun.EngineRequest{
			{ID: fmt.Sprintf("%d/range", i), Unaligned: true, Form: url.Values{"query": {query}, "start": {"60"}, "end": {"180"}, "step": {"60"}}.Encode()},
			{ID: fmt.Sprintf("%d/instant", i), Instant: true, Form: url.Values{"query": {query}, "time": {"180"}}.Encode()},
		}
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, "fold_by_grouping_cases", sets)
	for i, tc := range cases {
		for _, instant := range []bool{false, true} {
			id := fmt.Sprintf("%d/range", i)
			want := tc.want
			if instant {
				id, want = fmt.Sprintf("%d/instant", i), instantOf(want, 180)
			}
			a := answers[id]
			if a.Status != 200 {
				t.Fatalf("%s %s: Loki answered %d %s", tc.name, id, a.Status, a.Error)
			}
			if got, w := fmt.Sprint(foldResult(t, a.Body)), fmt.Sprint(foldResult(t, foldBody(want, instant))); got != w {
				t.Errorf("%s (instant=%v): the case wants %s, Loki answers %s", tc.name, instant, w, got)
			}
		}
	}
}
