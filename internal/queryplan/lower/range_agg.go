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
	log, err := metricLogQuery(ra.In, prof, "RangeAgg")
	if err != nil {
		return nil, err
	}
	if rate {
		// One allocation for the text: the chain is most of it.
		return physical.VLQuery{Text: log.text(translate.RateDetectedLevelChain(), "", ratePipes[:]...)}, nil
	}
	var b strings.Builder
	log.write(&b, "", "", 80)
	b.WriteByte(' ')
	b.WriteString(stats.String())
	return physical.VLQuery{Text: b.String()}, nil
}

// metricLog is the log query of a metric row, split where a range or vector aggregation inserts its derivations:
// the filters (head), the restore of the stream labels, and the log query's pipes.
type metricLog struct {
	head    string
	restore string
	// pipes is the log query's pipes printed, each after a space.
	pipes string
}

// metricLogQuery lowers the log query of a range aggregation the way the string translator writes a metric row's
// (translateMetricInnerQuery: no capabilities, no stream fields, no kept line, only the label names). A part it
// does not write is named under owner, the construct the aggregation delegates as.
func metricLogQuery(in *queryplan.LogSelect, prof Profile, owner string) (metricLog, error) {
	if c := rangeInnerConstruct(in); c != "" {
		return metricLog{}, &NotLoweredError{Construct: owner + strings.TrimPrefix(c, "RangeAgg")}
	}
	if prof.KeepLine {
		return metricLog{}, &NotLoweredError{Construct: owner + " keepLine"}
	}
	restore, ok := streamTagRestore(prof.Label)
	if !ok {
		return metricLog{}, &NotLoweredError{Construct: owner + " stream label name"}
	}
	q, err := logSelectQuery(in, Profile{Label: prof.Label})
	if err != nil {
		if nl, ok := err.(*NotLoweredError); ok {
			return metricLog{}, &NotLoweredError{Construct: owner + " " + nl.Construct}
		}
		return metricLog{}, err
	}
	// The string translator strips what its regexp reads as a without clause out of the query text, and removes a
	// " bool " modifier, quoted strings included: the lowering leaves such a query to it. The printed query may not
	// hold the client's value (a value that is not UTF-8 is written as a filter that matches nothing), so the
	// selector and the line filters are read as well as the printed filter and pipes.
	quoted := quotedClauseError(owner)
	if selectionHas(in, "without") || selectionHas(in, "bool") || selectionHas(in, "|||") {
		return metricLog{}, quoted
	}
	head := logsql.Query{Filter: q.Filter}.String()
	if readsAsClause(head) {
		return metricLog{}, quoted
	}
	var pipes strings.Builder
	for _, p := range q.Pipes {
		pipes.WriteByte(' ')
		start := pipes.Len()
		pipes.WriteString(p.String())
		if readsAsClause(pipes.String()[start:]) {
			return metricLog{}, quoted
		}
	}
	return metricLog{head: head, restore: restore, pipes: pipes.String()}, nil
}

// write puts the metric row's query, before its stats, into b, with room for extra more bytes: the filters, then the
// derivation of detected_level (chain) and of service_name (service), each when it is not empty, the restore of the
// stream labels and the log query's pipes. The string translator inserts the derivations before the first pipe of
// the log query, the chain after the service name's, in front of it.
func (m metricLog) write(b *strings.Builder, chain, service string, extra int) {
	b.Grow(len(m.head) + len(chain) + len(service) + len(m.restore) + len(m.pipes) + extra)
	b.WriteString(m.head)
	for _, part := range [3]string{chain, service, m.restore} {
		if part != "" {
			b.WriteByte(' ')
			b.WriteString(part)
		}
	}
	b.WriteString(m.pipes)
}

// text is the metric row's query with the stats pipes after it (write).
func (m metricLog) text(chain, service string, stats ...logsql.Pipe) string {
	var b strings.Builder
	m.write(&b, chain, service, 80*len(stats))
	for _, p := range stats {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	return b.String()
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

// errQuotedClause and errVectorQuotedClause name a log query holding what a regexp of the string translator reads
// out of the whole LogQL text, quoted strings included: a `without (...)` clause (stripped from the query, and a
// fold marker appended), a ` bool ` modifier (removed) and the `|||` that separates the operands of a binary
// expression (the translator splits its output there to insert the derived-label pipes).
var (
	errQuotedClause       = &NotLoweredError{Construct: "RangeAgg quoted clause"}
	errVectorQuotedClause = &NotLoweredError{Construct: "VectorAgg quoted clause"}
)

func quotedClauseError(owner string) *NotLoweredError {
	if owner == "VectorAgg" {
		return errVectorQuotedClause
	}
	return errQuotedClause
}

// readsAsClause reports whether printed LogsQL holds a word the string translator reads as a without clause or a
// bool modifier.
func readsAsClause(s string) bool {
	return strings.Contains(s, "without") || strings.Contains(s, "bool") || strings.Contains(s, "|||")
}
