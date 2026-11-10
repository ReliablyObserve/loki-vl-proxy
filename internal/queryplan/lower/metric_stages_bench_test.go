package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// metricStageQueries are the plan corpus's range and vector aggregations over a log query with a drop, a keep,
// decolorize, line_format, label_format, regexp or pattern stage that the plan lowers, and shapes of the grid for
// the stages the corpus holds few of.
func metricStageQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	qs = append(qs,
		`rate({app="a"} | drop level, x [5m])`, `sum by (app) (count_over_time({app="a"} | json | drop a | keep b, c [5m]))`,
		`count_over_time({app="a"} | decolorize | line_format "{{.a}} x" [5m])`, `bytes_over_time({app="a"} | label_format b=a | drop a [5m])`,
		"sum(rate({app=\"a\"} | regexp `(?P<a>\\d+)` | a=\"1\" [5m]))", `sum by (a) (count_over_time({app="a"} | pattern "<a> <b>" | drop b [5m]))`,
		`avg(bytes_over_time({app="a"} | keep detected_level, a [5m]))`, `sum_over_time({app="a"} | json | drop a | unwrap n [5m])`,
	)
	var out []string
	for _, q := range qs {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		ra, ok := p.Root.(*queryplan.RangeAgg)
		if va, isVec := p.Root.(*queryplan.VectorAgg); isVec {
			ra, ok = va.In.(*queryplan.RangeAgg)
		}
		if !ok || !hasMetricStage(ra.In) {
			continue
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no stage aggregation in the corpus")
	}
	b.Logf("%d metric stage queries", len(out))
	return out
}

// hasMetricStage reports whether the log query has a drop, keep, formatting stage or a regexp or pattern parser.
func hasMetricStage(sel *queryplan.LogSelect) bool {
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Drop, *queryplan.Keep, *queryplan.Decolorize, *queryplan.LineFormat, *queryplan.LabelFormat:
			return true
		case *queryplan.Parse:
			if st.Type == logql.ParserRegexp || st.Type == logql.ParserPattern {
				return true
			}
		}
	}
	return false
}

// The string translator on the lowered stage aggregations: text to LogsQL text.
func BenchmarkMetricStageQueriesTranslator(b *testing.B) {
	qs := metricStageQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkMetricStageQueriesLowered(b *testing.B) {
	qs := metricStageQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan alone on the same queries, built once: the lowering's own cost.
func BenchmarkMetricStageQueriesPlanOnly(b *testing.B) {
	qs := metricStageQueries(b)
	plans := make([]*queryplan.Plan, len(qs))
	for i, q := range qs {
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		if plans[i], err = queryplan.Build(expr, queryplan.Inputs{}); err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(plans[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Plan(plans[i%len(plans)], Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The string translator on the lowered stage aggregations under the tenant's stream labels.
func BenchmarkMetricStageQueriesStreamLabelsTranslator(b *testing.B) {
	qs := metricStageQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries under the tenant's stream labels.
func BenchmarkMetricStageQueriesStreamLabelsLowered(b *testing.B) {
	qs := metricStageQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{Label: benchStreamLabels}); err != nil {
			b.Fatal(err)
		}
	}
}
