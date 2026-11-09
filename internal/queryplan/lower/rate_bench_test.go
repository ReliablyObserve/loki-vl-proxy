package lower

import (
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// rateQueries are the plan corpus's rate and bytes_rate aggregations over a log query with line, pattern and
// label filters only (no parser, no unwrap, no grouping): the shapes whose stats group by the stream identity
// and so carry the detected_level chain.
func rateQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	var out []string
	for _, q := range qs {
		expr, err := logql.Parse(q)
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		ra, ok := p.Root.(*queryplan.RangeAgg)
		if !ok || (ra.Op != logql.RangeRate && ra.Op != logql.RangeBytesRate) || ra.By != nil || rangeInnerConstruct(ra.In) != "" {
			continue
		}
		queryplan.Resolve(p)
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no rate aggregation in the corpus")
	}
	return out
}

// The string translator on the corpus's rate and bytes_rate aggregations: text to LogsQL text.
func BenchmarkRateQueriesTranslator(b *testing.B) {
	qs := rateQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The string translator on the same queries under the tenant's stream labels.
func BenchmarkRateQueriesStreamLabelsTranslator(b *testing.B) {
	qs := rateQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The detected_level chain of a stats query that groups by the stream identity, from the memo the repeated
// translations of a query share.
func BenchmarkDetectedLevelChainMemo(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = translator.BuildDetectedLevelChain(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil)
	}
}

// The chain built each time (a query that unpacks keys no earlier query did, once the memo is full).
func BenchmarkDetectedLevelChainBuild(b *testing.B) {
	for i := 0; i < 5000; i++ {
		translator.BuildDetectedLevelChainShared(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil,
			&translator.SharedUnpack{Keys: []string{"fill" + strconv.Itoa(i)}})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = translator.BuildDetectedLevelChainShared(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil,
			&translator.SharedUnpack{Keys: []string{"app", "k" + strconv.Itoa(i)}})
	}
}
