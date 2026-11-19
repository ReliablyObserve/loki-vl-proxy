package proxy

import (
	"context"
	"strings"
	"testing"
	"time"
)

// BenchmarkValidateUnderPlanSegment is a validation miss (validateQueryPhasesWithin, nothing cached) of a dashboard
// query and a deep one, without a budget and under a request's real planning segment (its clock and lock), which the
// validation asks every 256 steps once it is 1,024 steps in.
func BenchmarkValidateUnderPlanSegment(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", LogLevel: "error", ExecutionLimits: ExecutionLimitsConfig{MaxQueryPlanTime: time.Hour}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	for name, q := range map[string]string{
		"dashboard": `sum by (namespace, pod) (rate({cluster="prod", namespace=~"team-.+"} |= "level=error" != "healthcheck" | logfmt | status >= 500 [5m]))`,
		"deep":      strings.Repeat("sum(", 800) + `rate({a="b"}[1m])` + strings.Repeat(")", 800),
	} {
		b.Run(name+"/none", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _, _ = validateQueryPhasesWithin(q, nil)
			}
		})
		b.Run(name+"/segment", func(b *testing.B) {
			ctx, _ := requestContext()
			seg := p.beginPlan(ctx, p.limits())
			if seg == nil {
				b.Fatal("no planning segment")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _, _ = validateQueryPhasesWithin(q, seg)
			}
		})
	}
}
