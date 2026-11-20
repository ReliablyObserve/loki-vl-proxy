package proxy

import (
	"strings"
	"testing"
)

// absentSpecShapes are queries every range request reads for an absent_over_time route
// (parseAbsentOverTimeCompatSpec): a label_replace nest, a small one over absent_over_time, and a plain metric.
var absentSpecShapes = []struct{ name, q string }{
	{"label_replace_nest_2500", strings.Repeat("label_replace(", 2500) + `rate({a="b"}[1m])` + strings.Repeat(`, "d", "$1", "s", "(.*)")`, 2500)},
	{"label_replace_absent_2", `label_replace(label_replace(absent_over_time({app="a"}[5m]), "x", "$1", "app", "(.*)"), "y", "$1", "x", "(.*)")`},
	{"plain_rate", `sum by (app) (rate({app="a"}[5m]))`},
}

func BenchmarkAbsentSpecShapes(b *testing.B) {
	for _, s := range absentSpecShapes {
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = parseAbsentOverTimeCompatSpec(s.q)
			}
		})
	}
}
