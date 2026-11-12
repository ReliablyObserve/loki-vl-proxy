package lower

import (
	"errors"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// benchNumericEqualityQueries are label filters after a json or logfmt parser with a numeric, duration or bytes
// equality, among the keys a label can be stored under and the operators around it.
var benchNumericEqualityQueries = []string{
	`{app="a"} | json | status == 200`,
	`{app="a"} | json | status != 200.5`,
	`{app="a"} | logfmt | http_status = 200`,
	`{app="a"} | json | duration != 1m30s`,
	`{app="a"} | json | size == 1.5MiB`,
	`{app="a"} | json | status == 200 and (latency != 5s or size = 1KB)`,
	`{app="a"} | logfmt | level="error" | status == 500 |= "x"`,
	`{app="a"} | json | pod_extracted == 2`,
}

// benchParsedLabelFilterOtherQueries are label filters after a parser with no numeric equality: the path the
// numeric equality shares its leaves with.
var benchParsedLabelFilterOtherQueries = []string{
	`{app="a"} | json | status >= 500`,
	`{app="a"} | logfmt | level="error"`,
	`{app="a"} | json | http_status="5"`,
	`{app="a"} | json | a=~"x|y" or b!="z"`,
	`{app="a"} | json | d>5s and b<=1KB`,
	`{app="a"} | json | status>=500 | status<600`,
	`{app="a"} | logfmt | level="error" | status >= 500 |= "x"`,
	`{app="a"} | json | pod_extracted="1"`,
}

func benchLoweredQueries(b *testing.B, qs []string) {
	b.Helper()
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
		if _, err := Plan(p, Profile{}); err != nil {
			if errors.Is(err, ErrNotLowered) {
				b.Skip("not lowered by this build")
			}
			b.Fatal(err)
		}
	}
}

func benchTranslatedQueries(b *testing.B, qs []string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the numeric equality queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkNumericEqualityQueriesLowered(b *testing.B) {
	benchLoweredQueries(b, benchNumericEqualityQueries)
}

// The string translator on the same queries (its text is the exact-match form).
func BenchmarkNumericEqualityQueriesTranslator(b *testing.B) {
	benchTranslatedQueries(b, benchNumericEqualityQueries)
}

// The plan alone on the numeric equality queries, built once: the lowering's own cost.
func BenchmarkNumericEqualityQueriesPlanOnly(b *testing.B) {
	plans := make([]*queryplan.Plan, len(benchNumericEqualityQueries))
	for i, q := range benchNumericEqualityQueries {
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		if plans[i], err = queryplan.Build(expr, queryplan.Inputs{}); err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(plans[i])
	}
	if _, err := Plan(plans[0], Profile{}); errors.Is(err, ErrNotLowered) {
		b.Skip("not lowered by this build")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(plans)
		if _, err := Plan(plans[j], Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on parsed label filters without a numeric equality: unchanged by the numeric equality lowering.
func BenchmarkParsedLabelFilterOtherQueriesLowered(b *testing.B) {
	benchLoweredQueries(b, benchParsedLabelFilterOtherQueries)
}

// The string translator on the same queries.
func BenchmarkParsedLabelFilterOtherQueriesTranslator(b *testing.B) {
	benchTranslatedQueries(b, benchParsedLabelFilterOtherQueries)
}
