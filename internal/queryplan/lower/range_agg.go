package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// rangeAgg lowers a range aggregation over a log query that lowers natively and has no parser and no unwrapped
// value: count_over_time, bytes_over_time and absent_over_time, and rate and bytes_rate. The string translator
// writes the log query with the metric profile (translateMetricInnerQuery: no capabilities, no stream fields, no
// kept line, only the label names), then the stats pipes, so VictoriaLogs counts or sums per step bucket and the
// proxy never buffers rows. The offset and the range stay out of the text, except in the division of a rate by
// the range's seconds: the proxy aligns the request window by them.
//
// A rate and a bytes_rate group by the stream, its stored level and the detected_level that Loki attaches to
// every entry: the chain of pipes that derives it (translate.RateDetectedLevelChain) goes after the filters of the
// log query and before its first pipe, then the stats count (or sum of line bytes) per series, the division by the
// range's seconds, and the sum per series (translate.RateStatsPipes).
//
// Everything else is left to the string translator under a construct name:
//   - the functions that read an unwrapped value, and the pipelines with a parser or any stage the log query
//     lowering does not write;
//   - a grouping, which Loki accepts only on the unwrap functions;
//   - the profile that keeps the line aside, which is for log responses: a metric row is not translated under it.
//
// A label function that names stream labels adds the pipes that restore their stream values before the first pipe
// of the log query (streamTagRestore, printed once per set of stream labels).
func rangeAgg(ra *queryplan.RangeAgg, prof Profile) (physical.Op, error) {
	rate := ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate
	var stats logsql.PipeStats
	var ratePipes [3]logsql.Pipe
	if rate {
		// The translator reads the range's seconds from its text and writes none for a range of no length.
		seconds := ra.Range.Seconds()
		if seconds <= 0 {
			return nil, &NotLoweredError{Construct: "RangeAgg range"}
		}
		ratePipes, _ = translate.RateStatsPipes(ra.Op, seconds)
	} else {
		var ok bool
		if stats, ok = translate.RangeStatsPipe(ra.Op); !ok {
			return nil, &NotLoweredError{Construct: "RangeAgg unwrap"}
		}
	}
	if ra.By != nil {
		return nil, &NotLoweredError{Construct: "RangeAgg grouping"}
	}
	if c := rangeInnerConstruct(ra.In); c != "" {
		return nil, &NotLoweredError{Construct: c}
	}
	if prof.KeepLine {
		return nil, &NotLoweredError{Construct: "RangeAgg keepLine"}
	}
	restore, ok := streamTagRestore(prof.Label)
	if !ok {
		return nil, &NotLoweredError{Construct: "RangeAgg stream label name"}
	}
	q, err := logSelectQuery(ra.In, Profile{Label: prof.Label})
	if err != nil {
		if nl, ok := err.(*NotLoweredError); ok {
			return nil, &NotLoweredError{Construct: "RangeAgg " + nl.Construct}
		}
		return nil, err
	}
	// The derivation of detected_level goes after the filters of the query, then the restore of the stream labels,
	// the rest of the query's pipes and the stats: the query as logsql.Query prints it, with the printed chain and
	// restore inserted.
	var b strings.Builder
	head := logsql.Query{Filter: q.Filter}.String()
	if rate {
		// One allocation for the text: the chain is most of it.
		chain := translate.RateDetectedLevelChain()
		b.Grow(len(head) + len(chain) + len(restore) + 512)
		b.WriteString(head)
		b.WriteByte(' ')
		b.WriteString(chain)
	} else {
		b.WriteString(head)
	}
	if restore != "" {
		b.WriteByte(' ')
		b.WriteString(restore)
	}
	for _, p := range q.Pipes {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	if rate {
		for _, p := range ratePipes {
			b.WriteByte(' ')
			b.WriteString(p.String())
		}
	} else {
		b.WriteByte(' ')
		b.WriteString(stats.String())
	}
	return physical.VLQuery{Text: b.String()}, nil
}

// rangeInnerConstruct names the first part of the log query of a range aggregation that its lowering does not
// write, or "" when it writes all: an unwrap or a parser (the metric rows group by what they extract) first, any
// drop or keep (written unlike a log query's) and any other stage the log query lowering leaves out is named by
// that lowering.
func rangeInnerConstruct(sel *queryplan.LogSelect) string {
	parser, dropKeep := false, false
	for _, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.Unwrap:
			return "RangeAgg unwrap"
		case *queryplan.Parse:
			parser = true
		case *queryplan.Drop, *queryplan.Keep:
			dropKeep = true
		}
	}
	if parser {
		return "RangeAgg parser"
	}
	if dropKeep {
		// The string translator writes a metric row's drop and keep unlike a log query's: a drop deletes the
		// level fields detected_level is derived from, and a keep of detected_level deletes level after its fields.
		return "RangeAgg drop or keep"
	}
	return ""
}
