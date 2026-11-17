package proxy

import (
	"strconv"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The unwrap code on the request path, measured on its own: the conversion the raw evaluators run per row, the
// logfmt reading of the patterns miner and the conversion-error check, the counting rewrite and the answer strip of
// every gated stats read, the deviation merge per window, and the label derivation of a looked-up line.

func BenchmarkUnwrapConvert(b *testing.B) {
	for _, c := range []struct{ conv, value string }{{"", "1234.5"}, {"", "abc"}, {"duration", "1h30m15s"}, {"bytes", "1.5 KiB"}} {
		b.Run(c.conv+"/"+c.value, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = convertUnwrapValue(c.value, c.conv)
			}
		})
	}
}

func BenchmarkUnwrapLogfmtPairs(b *testing.B) {
	for name, line := range map[string]string{
		"plain":  `ts=2026-11-16T10:00:00Z level=info service=api duration_ms=12.5 status=200 path=/v1/items`,
		"quoted": `level=warn msg="slow request \"items\"" duration="1.5s" user=é`,
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for i := 0; i < b.N; i++ {
				lokiLogfmtPairs(line, func(string, string) { n++ })
			}
		})
	}
}

func BenchmarkUnwrapCountingRewrite(b *testing.B) {
	query := `app:="a" | unpack_logfmt` + translator.UnwrapGateFor("v", "duration") + ` | stats by (app) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (app) sum(__lvp_rate)`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _, _ = unwrapCountingRewrite(query, func(string, string) (bool, bool) { return true, true })
	}
}

func BenchmarkUnwrapStripFlags(b *testing.B) {
	values := ""
	for i := 0; i < 240; i++ {
		if i > 0 {
			values += ","
		}
		values += "[" + strconv.Itoa(1_700_000_000+60*i) + `,"` + strconv.Itoa(i) + `"]`
	}
	clean := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","app":"a","__lvp_bad":"0"},"values":[` + values + `]},{"metric":{"__name__":"c","app":"b","__lvp_bad":"0"},"values":[` + values + `]}]}}`)
	flagged := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","app":"a","__lvp_bad":"0"},"values":[` + values + `]},{"metric":{"__name__":"c","app":"a","__lvp_bad":"2"},"values":[[1700000060,"0"]]},{"metric":{"__name__":"c","app":"a","__lvp_bad":"1"},"values":[[1700000120,"0"]]}]}}`)
	nan := unwrapNaNFor(`app:="a" | stats by (app, __lvp_bad) sum(__lvp_v) as c`)
	for name, body := range map[string][]byte{"clean": clean, "flagged": flagged} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _ = stripUnwrapFlags(body, nan)
			}
		})
	}
}

func BenchmarkUnwrapMergeDeviation(b *testing.B) {
	var means, counts, devs []rangeMetricSample
	for i := 0; i < 10; i++ {
		ts := int64(i) * int64(time.Minute)
		means = append(means, rangeMetricSample{ts: ts, value: float64(i) + 0.5})
		counts = append(counts, rangeMetricSample{ts: ts, value: float64(i%3 + 1)})
		devs = append(devs, rangeMetricSample{ts: ts, value: float64(i%4) / 2})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mergeUnwrapBuckets("stddev", means, counts, devs)
	}
}

func BenchmarkUnwrapLokiLabels(b *testing.B) {
	for name, c := range map[string]struct{ query, line string }{
		"logfmt": {`sum by (app) (sum_over_time({app="x"} | logfmt | unwrap v [1m]))`, `n=1 v=x7 w=abc level=info path=/a`},
		"json":   {`sum by (app) (sum_over_time({app="x"} | json | unwrap v [1m]))`, `{"n":"1","v":"x7","w":"abc","o":{"k":"2"}}`},
	} {
		expr, err := logqlpkg.Parse(c.query)
		if err != nil {
			b.Fatal(err)
		}
		probe := unwrapErrorProbes(expr)[0]
		base := map[string]string{"app": "x", "service_name": "x", "detected_level": "info"}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = unwrapLokiError(probe, base, c.line, c.line)
			}
		})
	}
}
