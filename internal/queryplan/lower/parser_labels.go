package lower

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The labels a metric query reads out of its parsers. The string translator takes three things from the text of the
// query that the plan has as typed data:
//
//   - the labels a stats pipe groups by or aggregates that a json or logfmt parser produced under their sanitized
//     name (http.method and http-method are both http_method): the pipes ahead of the stats give the label the
//     value of each spelling (keyPipes, translate.ParsedKeyPipesText);
//   - the series of a `without` fold over a parser: the stream plus the labels the parsers name (parsedSeriesLabels);
//   - the series of a rate, and of the one value per series of count, min, max and avg, behind a trailing keep: the
//     labels the keep leaves (trailingKeepLabels).
//
// What the translator decides by searching the printed query for the label (` as <label>` or `<label>` in the text:
// a label the query defines itself) is read from the printed log query the same way (metricLog.has).

// Constructs delegated by this file. The collision rename of a label named under `_extracted` reuses the scratch
// fields a label filter on the same label already computed (the translator finds them by position in its text), and
// a resolved label that a label filter checks for existence has that filter moved behind the key pipes: where the
// first such text is not in the printed pipes (the client's quoted text, or the stream-label restore), the
// translator edits text the lowering does not hold, and the query is left to it. A label filter or a drop after the
// parser, under a label the lowering resolves, costs more allocations to write than the translator spends on the
// same text (the stage's own lowering does, before this step too), so the query stays with the translator.
const (
	extractedConstruct = " parser extracted label"
	existsConstruct    = " parser label filter"
	costConstruct      = " parser label cost"
)

// keyed is the pipes keyPipes writes: the text, each pipe after a blank, and whether one of them is a copy pipe.
type keyed struct {
	text   string
	copies bool
}

// keyPipes are the pipes the string translator writes between the log query and a stats pipe that groups by `by`
// and aggregates arg (the unwrapped label, or the field the stats function reads; "" for none): for every grouping
// label that may be a sanitized parser key, one pipe per other spelling of the key; for a label named under
// `_extracted` the collision-rename pipes; the existence checks on the resolved labels moved behind them; then the
// filter that drops the lines without the aggregated label when it is resolved or a copy pipe defines it. owner is
// the aggregation, for the name of a delegation; again says a second stats pipe that resolves its labels follows
// (the final sum of a rate). The log query is returned without the existence checks the pipes carry on. It reads
// nothing when the log query has no json or logfmt unpack.
func (m metricLog) keyPipes(by []string, fn logsql.StatsFunc, arg, owner string, again bool) (keyed, error) {
	if !m.unpacked() {
		return keyed{}, nil
	}
	// A count of one label behind its existence check keeps the exact key the field breakdown of Grafana Drilldown
	// relies on. A rate sums that count by the same label next, which then resolves it in the middle of the query;
	// the existence check is a label filter after the parser, which costs more to write than the translator spends
	// (costConstruct) for a label that may be a sanitized key.
	if _, count := fn.(logsql.Count); count && len(by) == 1 && m.breakdownShape(by[0]) {
		switch {
		case again:
			return keyed{}, &NotLoweredError{Construct: owner + existsConstruct}
		case translate.ParsedLabelCandidate(by[0]):
			return keyed{}, &NotLoweredError{Construct: owner + costConstruct}
		}
		return keyed{}, nil
	}
	var labelBuf [6]string
	labels := labelBuf[:0]
	for i := 0; i <= len(by); i++ {
		l := arg
		if i < len(by) {
			l = by[i]
		}
		if l != "" && !slices.Contains(labels, l) && m.resolvable(l) {
			labels = append(labels, l)
		}
	}
	// The aggregated label of a stats function is dropped when empty if a pipe resolved it or a copy defined it.
	filterArg := arg != "" && (slices.Contains(labels, arg) || m.holdsCopy() && m.hasJoined(asPipe, arg, ""))
	if len(labels) == 0 && !filterArg {
		return keyed{}, nil
	}
	var b strings.Builder
	b.Grow(len(labels)*192 + 64)
	copies := false
	var bases, extracted []string
	for _, l := range labels {
		translate.ParsedKeyPipesText(&b, l)
		if base, ok := strings.CutSuffix(l, translate.ExtractedSuffix); ok && base != "" {
			bases, extracted = append(bases, base), append(extracted, l)
		}
	}
	if len(bases) > 0 {
		var err error
		if copies, err = m.extractedPipes(&b, bases, extracted, owner); err != nil {
			return keyed{}, err
		}
	}
	if len(labels) > 0 && m.afterParser {
		return keyed{}, &NotLoweredError{Construct: owner + costConstruct}
	}
	// An existence check on a resolved label, anywhere in the printed query (a label filter after the parser is
	// delegated above; quoted text can spell one), is moved by the translator behind the key pipes.
	for _, l := range labels {
		if m.hasJoined(existsHead, l, existsTail) {
			return keyed{}, &NotLoweredError{Construct: owner + existsConstruct}
		}
	}
	if filterArg {
		b.WriteString(existsHead)
		b.WriteString(arg)
		b.WriteString(anyTail)
	}
	return keyed{text: b.String(), copies: copies}, nil
}

// asPipe is how a pipe names its result in the printed query.
const asPipe = " as "

// extractedPipes writes the collision-rename pipes of the labels named under `_extracted` (bases are the names
// before the suffix) and reports whether one is a copy pipe. The scratch fields of a filter on the label, and
// quoted text that spells an unpack pipe or more unpacks than are told apart, leave the query to the translator.
func (m metricLog) extractedPipes(b *strings.Builder, bases, labels []string, owner string) (copies bool, err error) {
	for _, base := range bases {
		if m.hasJoined(asPipe, translate.ExtractedScratch+"_"+base, "") {
			return false, &NotLoweredError{Construct: owner + extractedConstruct}
		}
	}
	if int(m.unpacks) != m.unpackTexts() || m.unpacks > 8 {
		return false, &NotLoweredError{Construct: owner + extractedConstruct}
	}
	// The unpack pipes the translator finds in its text are the plain ones.
	var logfmt []bool
	for i := uint8(0); i < m.unpacks; i++ {
		if m.bareMask&(1<<i) != 0 {
			logfmt = append(logfmt, m.logfmtMask&(1<<i) != 0)
		}
	}
	return translate.ExtractedStatsText(b, logfmt, bases, labels), nil
}

// The texts keyPipes writes, taken from the typed pipes: the head of a filter pipe (` | filter `) and the tails of
// an existence check (`:!""`) and of a field that has any value (`:*`).
var (
	existsHead = " " + strings.TrimSuffix(logsql.PipeFilter{Expr: logsql.FieldFilter{Field: "x", Op: logsql.FieldOpAny}}.String(), "x:*")
	existsTail = strings.TrimPrefix(logsql.FieldFilter{Field: "x", Op: logsql.FieldOpNotExact}.String(), "x")
	anyTail    = strings.TrimPrefix(logsql.FieldFilter{Field: "x", Op: logsql.FieldOpAny}.String(), "x")
)

// hasJoined reports whether the printed log query holds prefix+mid+suffix, without building it.
func (m metricLog) hasJoined(prefix, mid, suffix string) bool {
	return containsJoined(m.head, prefix, mid, suffix) || containsJoined(m.restore, prefix, mid, suffix) || containsJoined(m.pipes, prefix, mid, suffix)
}

// containsJoined reports whether s holds prefix+mid+suffix (prefix is not empty).
func containsJoined(s, prefix, mid, suffix string) bool {
	for from := 0; ; {
		i := strings.Index(s[from:], prefix)
		if i < 0 {
			return false
		}
		from += i + len(prefix)
		if rest := s[from:]; strings.HasPrefix(rest, mid) && strings.HasPrefix(rest[len(mid):], suffix) {
			return true
		}
	}
}

// deleteErrors are the pipes that delete the error labels, as the translator's Drilldown rewrite reads them.
var deleteErrors = [...]string{
	"",
	" " + logsql.PipeDelete{Labels: []string{"__error__"}}.String(),
	" " + logsql.PipeDelete{Labels: []string{"__error__", "__error_details__"}}.String(),
	" " + logsql.PipeDelete{Labels: []string{"__error__"}}.String() + ",__error_details__",
}

// breakdownShape reports whether the printed log query ends in a json or logfmt unpack (and the delete of the error
// labels), then an existence check on the label, the shape of the field breakdown of Grafana Drilldown, which the
// translator leaves as it is.
func (m metricLog) breakdownShape(label string) bool {
	if label == "" || (label[0] < 'a' || label[0] > 'z') && (label[0] < 'A' || label[0] > 'Z') {
		return false
	}
	rest, ok := strings.CutSuffix(m.pipes, existsHead+label+existsTail)
	if !ok {
		return false
	}
	for _, d := range deleteErrors {
		if r, ok := strings.CutSuffix(rest, d); ok && (strings.HasSuffix(r, bareUnpackJSON) || strings.HasSuffix(r, bareUnpackLogfmt)) {
			return true
		}
	}
	return false
}

// unpackPrefix is how a json or logfmt unpack pipe starts in the printed query ("| unpack_"), taken from the typed pipe.
var unpackPrefix = strings.TrimSuffix(logsql.PipeUnpackJSON{}.String(), "json")

// unpackTexts counts the unpack pipes in the printed log query, quoted text included.
func (m metricLog) unpackTexts() int {
	return strings.Count(m.head, unpackPrefix) + strings.Count(m.restore, unpackPrefix) + strings.Count(m.pipes, unpackPrefix)
}

// copyPipe is how a copy pipe starts in the printed query ("| copy "), taken from the typed pipe.
var copyPipe = strings.TrimSuffix(logsql.PipeCopy{Pairs: [][2]string{{"a", "b"}}}.String(), "a as b")

// holdsCopy reports whether the printed log query holds a copy pipe (a stored stream value, a json expression).
func (m metricLog) holdsCopy() bool { return m.has(copyPipe) }

// resolvable reports whether the translator looks for the label under the other spellings of a parser key: a
// candidate (translate.ParsedLabelCandidate) the printed query does not define itself.
func (m metricLog) resolvable(label string) bool {
	return translate.ParsedLabelCandidate(label) && !m.hasJoined(asPipe, label, "") && !m.hasJoined("<", label, ">")
}

// statArg is the field a stats function reads, the way the translator reads it out of the function's text
// (`sum_len(_msg)`); "" for a function that reads none.
func statArg(fn logsql.StatsFunc) string {
	if f, ok := fn.(logsql.SumLen); ok {
		return f.Field
	}
	return ""
}

// firstBy appends the grouping fields of the first stats pipe of pipes to dst.
func firstBy(dst []string, pipes []logsql.Pipe) []string {
	for _, p := range pipes {
		if st, ok := p.(logsql.PipeStats); ok {
			for _, k := range st.By {
				dst = append(dst, k.Field)
			}
			return dst
		}
	}
	return dst
}

// errorLabels are the labels a keep never removes.
var errorLabels = [...]string{"__error__", "__error_details__", "__preserve_error__"}

// trailingKeepLabels are the labels the last keep stage of the log query leaves, when the translator takes that
// keep to decide the series of every sample (trailingKeepGrouping): the keep has no matcher and only plain label
// names, and no parser, label_format, drop or unwrap follows it. Loki deletes every label a keep does not name, so
// the series that keep the same labels are one. ok is false when the keep decides nothing: the series are then the
// stream (and its level). A keep that lists only error labels leaves none (kept is empty, ok is true).
func trailingKeepLabels(sel *queryplan.LogSelect) (kept []string, ok bool) {
	found := false
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Keep:
			if len(st.Matchers) > 0 {
				return nil, false
			}
			for _, l := range st.Labels {
				if !bareIdentifier(l) {
					return nil, false
				}
			}
			// The labels are the stage's own unless it names an error label, which a keep never removes.
			kept, found = st.Labels, true
			for i, l := range st.Labels {
				if slices.Contains(errorLabels[:], l) {
					kept = append(slices.Clone(st.Labels[:i]), keptAfter(st.Labels[i+1:])...)
					break
				}
			}
		case *queryplan.Parse, *queryplan.LabelFormat, *queryplan.Drop, *queryplan.Unwrap:
			found = false
		}
	}
	return kept, found
}

// filteredAfterParser reports whether a label filter or a drop stage stands after a parser stage of the log query.
func filteredAfterParser(sel *queryplan.LogSelect) bool {
	parsed := false
	for _, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.Parse:
			parsed = true
		case *queryplan.LabelFilter, *queryplan.Drop:
			if parsed {
				return true
			}
		}
	}
	return false
}

// keptAfter are the labels of rest that are not error labels.
func keptAfter(rest []string) []string {
	var kept []string
	for _, l := range rest {
		if !slices.Contains(errorLabels[:], l) {
			kept = append(kept, l)
		}
	}
	return kept
}

// bareIdentifier reports whether s is a label name made of letters, digits and underscores, as the translator reads
// the names of a keep.
func bareIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

var (
	jsonPlainPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	patternName   = regexp.MustCompile(`<([A-Za-z][A-Za-z0-9_]*)>`)
)

// parsedSeriesLabels are the labels the parser stages of the log query add to a Loki series, less the unwrapped
// one, and whether that set is known (translator parsedSeriesLabels). A Loki series is the stream labels plus the
// labels its parsers extracted, so when every parser names the keys it extracts (`| logfmt amount, route`, a
// regexp with named groups, a pattern's captures) the series of a stream is told apart by those labels alone. A
// bare json or logfmt extracts every key of every line, a stage that creates, renames or removes labels
// (label_format, drop, keep, unpack) changes the identity in ways this does not follow, and a name that collides
// with a label the proxy derives (level, detected_level, service_name) or with a label of the stream selector
// (which Loki renames `<key>_extracted`) is read differently: for those the set is not known. The stages after an
// unwrap are not read.
func parsedSeriesLabels(sel *queryplan.LogSelect, unwrap string) (labels []string, ok bool) {
	stages := sel.Stages
	for i, st := range stages {
		var names []string
		switch st := st.(type) {
		case *queryplan.LineFilter:
			// The translator's reading of the text glues a negative line filter to the stage before it.
			if i > 0 && (st.Op == logql.LineFilterExcludes || st.Op == logql.LineFilterExcludeRe || st.Op == logql.LineFilterExcludePat) {
				if _, isParse := stages[i-1].(*queryplan.Parse); isParse {
					return nil, false
				}
			}
			if st.Op == logql.LineFilterExcludePat {
				return nil, false
			}
			continue
		case *queryplan.LineFormat, *queryplan.Decolorize, *queryplan.LabelFilter:
			continue
		case *queryplan.Unwrap:
			return labels, true
		case *queryplan.Parse:
			var known bool
			if names, known = parserLabels(st); !known {
				return nil, false
			}
		default:
			return nil, false
		}
		for _, name := range names {
			switch {
			case name == "" || name == unwrap:
			case name == "level" || name == translate.DetectedLevelLabel || name == "service_name":
				return nil, false
			case selectorHas(sel, name):
				// A parsed key that is also a stream label becomes `<key>_extracted` on Loki: the name the
				// grouping would give the series is not the one Loki gives it.
				return nil, false
			case !slices.Contains(labels, name):
				labels = append(labels, name)
			}
		}
	}
	return labels, true
}

func selectorHas(sel *queryplan.LogSelect, name string) bool {
	for _, m := range sel.Matchers {
		if m.Name == name {
			return true
		}
	}
	return false
}

// parserLabels are the labels a parser stage names; known is false for a bare json or logfmt, which extracts every
// key, for a flag, an alias the translator does not follow (a logfmt alias, a json path with an index or bracket)
// and for an unpack.
func parserLabels(st *queryplan.Parse) (names []string, known bool) {
	switch st.Type {
	case logql.ParserJSON, logql.ParserLogfmt:
		if len(st.Fields) == 0 || st.Strict || st.KeepEmpty {
			return nil, false
		}
		logfmt := st.Type == logql.ParserLogfmt
		for _, f := range st.Fields {
			if !plainIdent(f.Name) {
				return nil, false
			}
			if logfmt && f.Expression != f.Name || logfmt && strings.Contains(st.Param, "=") {
				return nil, false
			}
			if !logfmt && f.Expression != f.Name && !jsonPlainPath.MatchString(f.Expression) {
				return nil, false
			}
			names = append(names, f.Name)
		}
		return names, true
	case logql.ParserRegexp:
		re, err := regexp.Compile(st.Param)
		if err != nil {
			return nil, false
		}
		return re.SubexpNames(), true
	case logql.ParserPattern:
		for _, m := range patternName.FindAllStringSubmatch(st.Param, -1) {
			names = append(names, m[1])
		}
		return names, true
	}
	return nil, false
}

// seriesIdent are the grouping fields that tell the series of a rate, or the one value per series of count, min,
// max and avg, apart (the translator's seriesIdentityGrouping): the labels a trailing keep leaves, else for a
// `without` fold (fold) over a parser the stream and the labels it names, else the stream and its level (the
// stream alone behind a parser). owner names the delegation of a keep
// that leaves no label (an explicit `by ()`). It returns nil for the default identity (seriesIdentity).
func seriesIdent(sel *queryplan.LogSelect, parser, fold bool, owner string, prof Profile) ([]string, error) {
	if kept, ok := trailingKeepLabels(sel); ok {
		if filteredAfterParser(sel) {
			return nil, &NotLoweredError{Construct: owner + costConstruct}
		}
		by := translate.NormalizeByLabels(kept, prof.Label)
		if len(by) == 0 {
			return nil, &NotLoweredError{Construct: owner + " keep identity"}
		}
		return by, nil
	}
	if parser && fold {
		unwrap := ""
		if u := unwrapStage(sel); u != nil {
			unwrap = u.Label
		}
		if labels, ok := parsedSeriesLabels(sel, unwrap); ok {
			return translate.JoinByLabels([]string{"_stream"}, labels), nil
		}
	}
	return nil, nil
}

// groupKeys are the grouping keys of fields.
func groupKeys(fields []string) []logsql.GroupKey {
	keys := make([]logsql.GroupKey, len(fields))
	for i, f := range fields {
		keys[i] = logsql.GroupKey{Field: f}
	}
	return keys
}

// quoteField is a field name as the string translator writes it in a fields list: quoted when it holds a dot or a
// hyphen.
func quoteField(name string) string {
	if strings.ContainsAny(name, ".-") {
		return strconv.Quote(name)
	}
	return name
}
