package proxy

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkTranslateExtractionListMiss is the translate-on-miss path of an aggregation over a range function after an
// extraction list (lower.DynamicUnwrapIdentity's extraction-list shapes), the shapes it turns away and a parser of
// every key: each iteration translates a selector the translation cache has not seen, through every step a request
// takes (screens, plan, lowering or the string translator).
func BenchmarkTranslateExtractionListMiss(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 1000), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	for _, q := range []string{
		`sum(max_over_time({app="APP"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="APP"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg by (ns) (avg_over_time({app="APP"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg(stddev_over_time({app="APP"} | json amount, route | unwrap amount [5m]))`,
		`sum(stdvar_over_time({app="APP"} | logfmt amount, route | unwrap amount [5m]))`,
		// Turned away: an alias in the list, a count over the series.
		`sum(max_over_time({app="APP"} | json a="amount", route | unwrap a [5m]))`,
		`count by (ns) (max_over_time({app="APP"} | logfmt amount, route | unwrap amount [5m]))`,
		// A parser of every key (the series hash), unchanged.
		`sum by (ns) (max_over_time({app="APP"} | logfmt | unwrap amount [5m]))`,
		// The maximum of maxima and the minimum of minima: one stats pipe over the rows.
		`max by (ns) (max_over_time({app="APP"} | logfmt amount, route | unwrap amount [5m]))`,
		`min(min_over_time({app="APP"} | json amount, route | unwrap amount [5m]))`,
		`max by (ns) (max_over_time({app="APP"} | logfmt | unwrap amount [5m]))`,
	} {
		b.Run(strings.ReplaceAll(q, `{app="APP"} `, ""), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := p.translateQueryOpts(context.Background(), strings.Replace(q, "APP", "u"+strconv.Itoa(i), 1), false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
