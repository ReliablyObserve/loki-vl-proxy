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
	if unwrapStage(ra.In) != nil {
		if ra.Op != logql.RangeAbsentOverTime {
			return unwrapRange(ra, prof)
		}
		var err error
		if ra, err = withoutUnwrap(ra, "RangeAgg"); err != nil {
			return nil, err
		}
	}
	rate := ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate
	var stats logsql.PipeStats
	if rate {
		// The translator reads the range's seconds from its text and writes none for a range of no length.
		seconds := ra.Range.Seconds()
		if seconds <= 0 {
			return nil, &NotLoweredError{Construct: "RangeAgg range"}
		}
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
	// A rate reads the unwrapped label out of the text (a quoted `| unwrap x` makes it a sum) and its series
	// identity from the parsers it finds there (a quoted `| json` drops the level from it).
	if rate && readsUnwrapOrParser(ra.In, log, true, !log.parser) {
		return nil, errQuotedClause
	}
	// The series of a rate behind a trailing keep are the labels it leaves.
	var ident []string
	if rate {
		if ident, err = seriesIdent(ra.In, log.parser, false, "RangeAgg", prof); err != nil {
			return nil, err
		}
	}
	if rate && (log.parser || ident != nil) {
		// The stream is the series of a rate over a parser: the detected_level chain is not derived.
		keys, by := parserIdentity, parserBy
		if ident != nil {
			keys, by = groupKeys(ident), ident
		}
		ratePipes := translate.RateStatsPipesBy(rateFunc(ra.Op), ra.Range.Seconds(), keys)
		resolved, err := log.keyPipes(by, rateFunc(ra.Op), statArg(rateFunc(ra.Op)), "RangeAgg", true)
		if err != nil {
			return nil, err
		}
		// The final stats is built through the path that resolves the labels again, which finds a copy pipe.
		pipes := ratePipes[:]
		if (log.holdsCopy() || resolved.copies) && log.unpacked() {
			pipes = []logsql.Pipe{ratePipes[0], ratePipes[1], logsql.PipeFilter{Expr: logsql.FieldFilter{Field: rateFilterField, Op: logsql.FieldOpAny}}, ratePipes[2]}
		}
		chain, service := "", ""
		first := ratePipes[0].(logsql.PipeStats)
		if groupsBy(first, translate.DetectedLevelLabel) {
			chain = translate.RateDetectedLevelChain()
		}
		if groupsBy(first, "service_name") {
			service = translate.ServiceNameGroupingPipes(prof.Caps)
		}
		return physical.VLQuery{Text: log.text(chain, service, resolved.text, pipes...)}, nil
	}
	if rate {
		// One allocation for the text: the chain is most of it.
		ratePipes, _ := translate.RateStatsPipes(ra.Op, ra.Range.Seconds())
		return physical.VLQuery{Text: log.text(translate.RateDetectedLevelChain(), "", "", ratePipes[:]...)}, nil
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
	// parser says the log query has a parser stage: the translator then takes the series of a metric to be the
	// stream (and, for an unwrapped label, each line), where it takes the stream and its level without one.
	parser bool
	// copies says a copy pipe is among the pipes (a stream label's stored value, a json expression).
	copies bool
	// unpack says the pipes hold a json or logfmt unpack. metricLogQuery leaves a query whose text holds one without
	// a typed unpack to the translator, so it also says whether the translator's text of the query holds one.
	unpack bool
	// bareLogfmt is where the last pipe in pipes starts when it is the query's only unpack and a bare
	// `| unpack_logfmt` (an unwrap names the fields to read then), and -1 otherwise.
	bareLogfmt int
	// unpacks counts the json and logfmt unpack pipes (at most 8 are told apart), logfmtMask says which of them are
	// logfmt (bit i for the i-th), and bareMask which are the plain pipe with no field list.
	unpacks, logfmtMask, bareMask uint8
	// afterParser says a label filter or a drop stands after a parser stage.
	afterParser bool
	// levelChain says the pipes derive detected_level for a filter on it (detectedLevelStages): a grouping by it
	// reads that value, the translator derives it once.
	levelChain bool
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
	// The metric row's log query keeps the client's spelling of a regexp line filter as the log query does
	// (keepsSpelling; it has no stream fields).
	if keepsSpelling(in, Profile{Label: prof.Label}) {
		return metricLog{}, &NotLoweredError{Construct: owner + " " + escapedSpellingConstruct}
	}
	q, err := logSelectQuery(in, Profile{Label: prof.Label, metric: true})
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
	if anyClientString(in, clientReadsAsClause) {
		return metricLog{}, quoted
	}
	head := logsql.Query{Filter: q.Filter}.String()
	if readsAsClause(head) {
		return metricLog{}, quoted
	}
	// The pipes are printed once, into one text, each after a space (a word the translator reads as a clause never
	// spans the space between two pipes, so the whole text is read at once).
	var pipesText string
	if len(q.Pipes) > 0 {
		pipesText = logsql.Query{Pipes: q.Pipes}.String()[1:]
	}
	if readsAsClause(pipesText) {
		return metricLog{}, quoted
	}
	bare, unpacks, copies, levelChain := -1, 0, false, holdsLevelStage(in)
	var mask, bareMask uint8
	for i, p := range q.Pipes {
		switch u := p.(type) {
		case logsql.PipeCopy:
			copies = true
		case logsql.PipeUnpackJSON:
			if unpacks < 8 && isBareUnpackJSON(u) {
				bareMask |= 1 << unpacks
			}
			unpacks++
		case logsql.PipeUnpackLogfmt:
			if unpacks < 8 {
				mask |= 1 << unpacks
				if isBareUnpackLogfmt(u) {
					bareMask |= 1 << unpacks
				}
			}
			unpacks++
			if i == len(q.Pipes)-1 && isBareUnpackLogfmt(u) {
				bare = len(pipesText) - len(bareUnpackLogfmt) - 1
			}
		}
	}
	m := metricLog{head: head, restore: restore, pipes: pipesText, parser: hasParser(in), copies: copies, bareLogfmt: bare,
		unpacks: uint8(min(unpacks, 255)), logfmtMask: mask, bareMask: bareMask, afterParser: filteredAfterParser(in), levelChain: levelChain}
	// The translator narrows the unpack when its text of the query holds exactly one unpack pipe, quoted text included.
	if unpacks != 1 || strings.Count(head, "| unpack_")+strings.Count(restore, "| unpack_")+strings.Count(m.pipes, "| unpack_") != 1 {
		m.bareLogfmt = -1
	}
	// The translator resolves the labels a stats reads (and looks for a copy pipe) when its text of the query holds an
	// unpack pipe: quoted text that holds one takes a query with no parser there. The pipes that derive
	// detected_level for a filter on it hold two, which the translator reads the same way.
	if unpacks == 0 && !levelChain && m.holdsUnpack() {
		return metricLog{}, quoted
	}
	m.unpack = unpacks > 0 || levelChain
	return m, nil
}

// write puts the metric row's query, before its stats, into b, with room for extra more bytes: the filters, then the
// derivation of detected_level (chain) and of service_name (service), each when it is not empty, the restore of the
// stream labels and the log query's pipes. The string translator inserts the derivations before the first pipe of
// the log query, the chain after the service name's, in front of it.
func (m metricLog) write(b *strings.Builder, chain, service string, extra int) {
	if m.levelChain {
		chain = ""
	}
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
func (m metricLog) text(chain, service, keys string, stats ...logsql.Pipe) string {
	var b strings.Builder
	m.write(&b, chain, service, len(keys)+80*len(stats))
	b.WriteString(keys)
	for _, p := range stats {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	return b.String()
}

// rangeInnerConstruct names the first part of the log query of a range aggregation that its lowering does not
// write, or "" when it writes all: an unwrap (the unwrap aggregations strip it first). Any other stage the log query
// lowering leaves out is named by that lowering. A metric row writes the stages a log query does, except that a drop
// and a keep differ (metricDropPipes, metricKeepPipes).
func rangeInnerConstruct(sel *queryplan.LogSelect) string {
	for _, st := range sel.Stages {
		if _, ok := st.(*queryplan.Unwrap); ok {
			return "RangeAgg unwrap"
		}
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

// clientReadsAsClause is readsAsClause for a short value the client wrote, in one pass over its bytes: the three words
// are looked for at their first letters, where three searches of the whole value cost more than they find.
func clientReadsAsClause(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 'w':
			if strings.HasPrefix(s[i:], "without") {
				return true
			}
		case 'b':
			if strings.HasPrefix(s[i:], "bool") {
				return true
			}
		case '|':
			if strings.HasPrefix(s[i:], "|||") {
				return true
			}
		}
	}
	return false
}

// isBareUnpackJSON reports an unpack_json pipe that prints as bareUnpackJSON: no condition, source, list, prefix or
// flag.
func isBareUnpackJSON(u logsql.PipeUnpackJSON) bool {
	return u.If == nil && u.From == "" && len(u.Fields) == 0 && len(u.PreserveKeys) == 0 && u.ResultPrefix == "" && !u.KeepOriginalFields && !u.SkipEmptyResults
}

// isBareUnpackLogfmt reports an unpack_logfmt pipe that prints as bareUnpackLogfmt.
func isBareUnpackLogfmt(u logsql.PipeUnpackLogfmt) bool {
	return u.If == nil && u.From == "" && len(u.Fields) == 0 && u.ResultPrefix == "" && !u.KeepOriginalFields && !u.SkipEmptyResults
}

// bareUnpackJSON is the printed `| unpack_json` of a json stage.
var bareUnpackJSON = logsql.PipeUnpackJSON{}.String()

// bareUnpackLogfmt is the printed `| unpack_logfmt` of a logfmt stage with no flags and no extraction list.
var bareUnpackLogfmt = logsql.PipeUnpackLogfmt{}.String()

// parserIdentity is the series of a rate over a log query with a parser.
var parserIdentity = []logsql.GroupKey{{Field: "_stream"}}

// parserBy is parserIdentity's fields.
var parserBy = []string{"_stream"}

// rateFunc is the stats function of the rows a rate or a bytes_rate reads.
func rateFunc(op logql.RangeOp) logsql.StatsFunc {
	if op == logql.RangeBytesRate {
		return logsql.SumLen{Field: "_msg"}
	}
	return logsql.Count{}
}

// has reports whether the printed log query holds s, as the translator's searches of its text do.
func (m metricLog) has(s string) bool {
	return strings.Contains(m.head, s) || strings.Contains(m.restore, s) || strings.Contains(m.pipes, s)
}

// unpacked reports whether the translator's text of the log query holds a json or logfmt unpack, which is what makes
// it resolve the labels its stats read (resolveStatsKeys): a query whose parsers are regexp and pattern holds none.
func (m metricLog) unpacked() bool { return m.unpack }

// holdsUnpack reports whether the printed log query holds a json or logfmt unpack pipe, typed or in quoted text.
func (m metricLog) holdsUnpack() bool { return m.has("| unpack_json") || m.has("| unpack_logfmt") }
