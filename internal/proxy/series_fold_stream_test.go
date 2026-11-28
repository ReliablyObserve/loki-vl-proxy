package proxy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// When the stream identity's series fold applies: with -structured-metadata-series-identity, to count_over_time and
// rate under vector aggregations by labels the read groups by, and to nothing else; what it reads and how it names
// the series.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestStreamSeriesFold(t *testing.T) {
	on := (&Proxy{structuredMetadataSeries: true}).withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`)
	spec := statsCompatSpec{GroupBy: []string{"service_name", "_stream", "level", "detected_level"}}
	by := func(merge withoutMerge, labels ...string) *matrixFold {
		set := map[string]bool{}
		for _, l := range labels {
			set[l] = true
		}
		return &matrixFold{levels: []foldLevel{{merge: merge, labels: set, keep: true}}}
	}
	const query = `avg by (service_name) (rate({env="production"}[5m]))`
	for _, c := range []struct {
		name       string
		ctx        context.Context
		query      string
		spec       statsCompatSpec
		manualFunc string
		fold       *matrixFold
		outer      logql.VectorOp
		value      string
	}{
		{"count", on, query, spec, "count_over_time", by(withoutMerge{count: true}, "service_name"), logql.VectorCount, "c"},
		{"avg rate", on, query, spec, "rate", by(withoutMerge{avg: true}, "service_name"), logql.VectorAvg, "c / 300"},
		{"max", on, query, spec, "count_over_time", by(withoutMerge{max: true}, "service_name"), logql.VectorMax, "c"},
		{"min", on, query, spec, "count_over_time", by(withoutMerge{min: true}), logql.VectorMin, "c"},
		{"sum", on, query, spec, "count_over_time", by(withoutMerge{sum: true}, "service_name"), logql.VectorSum, "c"},
		{"flag off", context.Background(), query, spec, "count_over_time", by(withoutMerge{count: true}, "service_name"), "", ""},
		{"no stream", on, query, statsCompatSpec{GroupBy: []string{"service_name"}}, "count_over_time", by(withoutMerge{count: true}, "service_name"), "", ""},
		{"bytes", on, query, spec, "bytes_over_time", by(withoutMerge{count: true}, "service_name"), "", ""},
		{"label not read", on, query, spec, "count_over_time", by(withoutMerge{count: true}, "pod"), "", ""},
		{"without", on, query, spec, "count_over_time", &matrixFold{levels: []foldLevel{{merge: withoutMerge{sum: true}, labels: map[string]bool{"pod": true}}}}, "", ""},
		{"no fold", on, query, spec, "count_over_time", nil, "", ""},
		{"binary", on, `sum by (service_name) (rate({a="b"}[5m]) / count_over_time({a="b"}[5m]))`, spec, "rate", by(withoutMerge{sum: true}, "service_name"), "", ""},
		{"bare", on, `count_over_time({a="b"}[5m])`, spec, "count_over_time", by(withoutMerge{count: true}), "", ""},
	} {
		got := streamSeriesFold(c.ctx, c.query, c.spec, c.manualFunc, c.fold, 5*time.Minute)
		if c.outer == "" {
			if got != nil {
				t.Errorf("%s: folded", c.name)
			}
			continue
		}
		if got == nil || got.outer != c.outer || got.value != c.value || !got.stream || !got.added || got.countColumn != lower.SeriesFoldCount {
			t.Errorf("%s: %+v", c.name, got)
			continue
		}
		folded, ok := got.query(`env:="production"`, spec.GroupBy, "count() as c", time.Unix(1700000000, 0), 20*time.Second)
		if !ok || !strings.HasPrefix(folded, `env:="production" `+streamSeriesCopy+` | stats by (_time:20s offset -1ns, service_name, __lvp_sid, level, detected_level) count() as c`) ||
			!strings.HasSuffix(folded, streamSeriesRename) || !strings.Contains(folded, `| math `+c.value+` as __lvp_fold`) {
			t.Errorf("%s: %v %s", c.name, ok, folded)
		}
		if got.seriesLabel() != "_stream" {
			t.Errorf("%s: series label %q", c.name, got.seriesLabel())
		}
	}
	if (&seriesFoldRead{}).seriesLabel() != dynamicSeriesHashLabel {
		t.Error("parser identity's series label")
	}
}

// Which queries the split can change: every other one reads, and is keyed, as without the flag.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestMetadataSplitChanges(t *testing.T) {
	for q, want := range map[string]bool{
		`{a="b"}`:        false,
		`{a="b"} | json`: false,
		`sum by (level_extracted) (count_over_time({a="b"} | json [5m]))`: false,
		`sum(rate({a="b"}[5m]))`:                                              false,
		`sum by (app) (bytes_over_time({a="b"}[5m]))`:                         false,
		`sum by (app) (sum_over_time({a="b"} | unwrap x [5m]))`:               false,
		`max by (app) (max_over_time({a="b"} | unwrap x [5m]))`:               false,
		`min(min_over_time({a="b"} | unwrap x [5m]))`:                         false,
		`absent_over_time({a="b"}[5m])`:                                       false,
		`topk(3, sum by (app) (rate({a="b"}[5m])))`:                           false,
		`count by (x) (sum by (x, y) (count_over_time({a="b"}[5m])))`:         false,
		`sum by (app) (rate({a="b"}[5m])) / sum by (app) (rate({c="d"}[5m]))`: false,
		`count_over_time({a="b"}[5m])`:                                        true,
		`count by (app) (count_over_time({a="b"}[5m]))`:                       true,
		`avg by (app) (rate({a="b"}[5m]))`:                                    true,
		`sum without (pod) (count_over_time({a="b"}[5m]))`:                    true,
		`sum by (app) (max_over_time({a="b"} | unwrap x [5m]))`:               true,
		`max by (app) (count_over_time({a="b"}[5m]))`:                         true,
		`topk(3, count_over_time({a="b"}[5m]))`:                               true,
		`sum by (s) (rate({a="b"}[5m]) / count_over_time({a="b"}[5m]))`:       true,
		`sum by (app) (sum by (app, pod) (rate({a="b"}[5m])) > 1)`:            false,
	} {
		if got := metadataSplitChanges(q); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}
