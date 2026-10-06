package proxy

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Unwrap sample validity (Loki v3.7.7 pkg/logql/log/metrics_extraction.go,
// streamLabelSampleExtractor.Process): a line whose unwrapped label is absent
// or empty makes no sample, and a value the conversion rejects is not a sample
// either (it carries __error__="SampleExtractionErr", which `| __error__=""`
// drops). VictoriaLogs' stats functions parse leniently instead (a unit such as
// "86282s" or "1KiB" becomes a number, max and min compare strings, a group
// without a value answers "" or NaN), so a stats pipe over an unwrapped label
// reads only the rows the translator selected and converted in Loki's syntax
// (translator.UnwrapGateFor, plain, duration() and bytes()). The raw evaluators
// (quantile, rate_counter, first, last, the bare-parser rows) convert with Go's
// own parsing (convertUnwrap).

// withoutUnwrapGate returns spec with the unwrap gate off its base query: the
// raw evaluators read the rows whole and convert the value themselves.
func withoutUnwrapGate(spec statsCompatSpec) statsCompatSpec {
	if base, _, ok := translator.SplitUnwrapGate(spec.BaseQuery); ok {
		spec.BaseQuery = base
	}
	return spec
}

// unwrapManualSpecs splits the spec of a manual range metric into the one the
// stats buckets read and the one the raw evaluator reads. The raw evaluator
// reads rows whole and converts the value itself, so it never sees the unwrap
// gate. The stats of an unwrap rate keep it: rate over an unwrapped label is its
// sum per second, answered from buckets of the sum with the sample count as
// presence. Every other metric reads the ungated spec on both routes.
func unwrapManualSpecs(spec statsCompatSpec, manualFunc string) (stats, raw statsCompatSpec, statsAggFunc string) {
	raw = withoutUnwrapGate(spec)
	if manualFunc == "unwrap_rate" && spec.BaseQuery != raw.BaseQuery {
		return spec, raw, "sum(" + translator.UnwrapValueAlias + ") as c, count() as __sample_count"
	}
	return raw, raw, ""
}

// queryWithoutUnwrapGate returns a translated query without the unwrap gate
// that ends the pipes before its first stats pipe.
func queryWithoutUnwrapGate(query string) string {
	idx := strings.Index(query, " | stats ")
	if idx < 0 {
		return query
	}
	if base, _, ok := translator.SplitUnwrapGate(query[:idx]); ok {
		return base + query[idx:]
	}
	return query
}
