package proxy

import (
	"fmt"
	"testing"
)

// The plan reads that replaced the text predicates, on the query shapes the handlers ask about: a hit (the plan is
// memoised) and a miss (a text never seen, so the query is parsed, built and resolved).
var planPredicateQueries = []string{
	`sum(count_over_time({app="api"}[5m]))`,
	`sum by (app) (rate({app="api"} | json | drop __error__ [5m]))`,
	`sum(rate({app="api"}[5m] | json | drop __error__)) by (app)`,
	`topk(5, sum by (app) (rate({env="prod"}[5m])))`,
	`label_replace(sum(rate({app="api"} | logfmt | drop __error__, __error_details__ [1m])), "b", "$1", "app", "(.*)")`,
}

func BenchmarkPlanPredicates(b *testing.B) {
	for _, mode := range []string{"hit", "miss"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				q := planPredicateQueries[i%len(planPredicateQueries)]
				if mode == "miss" {
					q = fmt.Sprintf("%s or vector(%d)", q, i)
				}
				_ = queryOuterUngrouped(q)
				_ = queryDropsOnlyErrorAfterParser(q)
				_, _, _, _ = parseTopKWrapper(q)
			}
		})
	}
}
