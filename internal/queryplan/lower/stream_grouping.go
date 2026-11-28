package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// A sum grouped by labels the stream carries reads no parsed key (semantics/stream-grouped-sum-skips-parsers).
//
// `sum by (level) (count_over_time({…} | json | logfmt | drop __error__, __error_details__ [r]))` groups by a
// label every line already holds before its parsers run, a stream label or a structured metadata field. Loki's json
// and logfmt parsers never overwrite such a label: a key of the same name is written `level_extracted` (the
// documented collision rule, recorded against the Loki engine for both parser orders by
// internal/querycorpus TestStreamGroupedParsersMatchLoki). Neither parser drops a line, the drop clears the error
// labels, so every selected line counts once, in the group of its own label values: the answer of the query with no
// stage from the first parser on.
//
// Whether every counted line holds every grouping label is a fact of the data, not of the query. The proxy reads
// it from that answer itself: a line without one of the labels lands in a series that lacks it, and only an answer
// with no such series is served (internal/proxy streamGroupedParsers). Every other answer goes on to the route
// that evaluates both parsers.
//
// The shape: a sum by one or more labels (not without) of count_over_time, rate, bytes_over_time or bytes_rate,
// whose log query holds only line filters before its first parser; from it on bare json and logfmt parsers (no
// extraction list, no flag), at least one of each, and drops of whole labels (no matcher) that name no grouping
// label, the last parser followed by a drop of __error__. A grouping label is a plain name: none that starts with an
// underscore (the parser error labels, and VictoriaLogs' own fields `_msg`, `_time`, `_stream` in any case), not an
// `_extracted` name (the parsers write it), not detected_level or service_name (labels the proxy derives on the
// read path). Without the drop Loki answers 400 on any line its JSON parser rejects, which the cut
// query cannot see, so that shape keeps today's route.

// StreamGroupedParsersAt is the index of the first parser of the sum's log query when p's root is a sum of the
// shape above, or a ranking (topk, bottomk, sort, sort_desc) of one, and -1 otherwise.
func StreamGroupedParsersAt(p *queryplan.Plan) int {
	if p == nil {
		return -1
	}
	root := p.Root
	if rank, ok := root.(*queryplan.Rank); ok {
		root = rank.In
	}
	va, ok := root.(*queryplan.VectorAgg)
	if !ok || va.Op != logql.VectorSum || va.By == nil || va.By.Without || len(va.By.Labels) == 0 {
		return -1
	}
	for _, label := range va.By.Labels {
		if !streamGroupingLabel(label) {
			return -1
		}
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	if !ok || ra.In == nil || ra.By != nil {
		return -1
	}
	switch ra.Op {
	case logql.RangeCountOverTime, logql.RangeRate, logql.RangeBytesOverTime, logql.RangeBytesRate:
	default:
		return -1
	}
	return streamGroupedParsersStages(ra.In.Stages, va.By.Labels)
}

// streamGroupedParsersStages is the index of the first parser of stages when they have the shape, and -1 otherwise.
func streamGroupedParsersStages(stages []queryplan.Stage, grouping []string) int {
	at, json, logfmt, cleared := -1, false, false, false
	for i, st := range stages {
		switch s := st.(type) {
		case *queryplan.LineFilter:
			if at >= 0 {
				return -1
			}
		case *queryplan.Parse:
			if s.Type != logql.ParserJSON && s.Type != logql.ParserLogfmt || s.Param != "" || s.Strict || s.KeepEmpty || s.Fields != nil {
				return -1
			}
			if at < 0 {
				at = i
			}
			json = json || s.Type == logql.ParserJSON
			logfmt = logfmt || s.Type == logql.ParserLogfmt
			cleared = false
		case *queryplan.Drop:
			if at < 0 || len(s.Matchers) != 0 {
				return -1
			}
			for _, name := range s.Labels {
				for _, label := range grouping {
					if name == label {
						return -1
					}
				}
				cleared = cleared || name == "__error__"
			}
		default:
			return -1
		}
	}
	if !json || !logfmt || !cleared {
		return -1
	}
	return at
}

// streamGroupingLabel reports a grouping label the parsers can only collide with, never write.
func streamGroupingLabel(label string) bool {
	return label != "" && !strings.HasPrefix(label, "_") && !strings.HasSuffix(label, "_extracted") &&
		label != "detected_level" && label != "service_name"
}
