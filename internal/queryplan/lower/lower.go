// Package lower lowers a logical plan (internal/queryplan) to the physical
// operators the proxy runs (internal/queryplan/physical) under a Profile,
// building the VictoriaLogs filters with the builders it shares with the
// string translator (internal/translate). It sits above them and below the
// translator and the proxy, which it never imports. A construct is lowered natively once its lowering lands, held to
// the string translator's operators by the shadow harness; until then Plan
// answers ErrNotLowered and the proxy uses the string translator.
package lower

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// Profile is what changes the VictoriaLogs query a plan lowers to: the
// capabilities of the VictoriaLogs version, the client-to-VictoriaLogs label
// names, the stream fields, and whether a log query keeps its stored line
// aside for a later line_format. The physical inputs never enter the logical
// plan or its key.
type Profile struct {
	Caps         logsql.Capabilities
	Label        func(string) string
	StreamFields map[string]bool
	KeepLine     bool
	// metric says the log query is the one of a metric row (metricLogQuery).
	metric bool
}

// ErrNotLowered is the error of a plan whose construct has no native lowering
// yet: the proxy answers it with the string translator.
var ErrNotLowered = errors.New("lower: not lowered")

// NotLoweredError names the construct that is not lowered; it is ErrNotLowered.
type NotLoweredError struct {
	Construct string
}

func (e *NotLoweredError) Error() string {
	return fmt.Sprintf("lower: %s is not lowered", e.Construct)
}

// Is reports ErrNotLowered.
func (e *NotLoweredError) Is(target error) bool { return target == ErrNotLowered }

// Plan lowers a resolved logical plan to physical operators under prof. Each
// construct is lowered natively once its lowering lands, and the shadow
// harness holds it to the string translator's operators on the whole corpus
// under every profile (internal/querycorpus TestPlanShadow); until then Plan
// answers ErrNotLowered naming the construct.
func Plan(p *queryplan.Plan, prof Profile) (physical.Op, error) {
	switch n := p.Root.(type) {
	case *queryplan.LogSelect:
		if keepsSpelling(n, prof) {
			return nil, &NotLoweredError{Construct: escapedSpellingConstruct}
		}
		return logSelect(n, prof)
	case *queryplan.RangeAgg:
		return rangeAgg(n, prof)
	case *queryplan.VectorAgg:
		return vectorAgg(n, prof)
	case *queryplan.BinOp:
		return binOp(n, prof)
	case *queryplan.LabelReplace:
		return labelReplace(n, prof)
	case *queryplan.Rank:
		return rank(n, prof)
	}
	return nil, &NotLoweredError{Construct: Construct(p.Root)}
}

// logSelect lowers a log query: its stream filter (translate.StreamSelectorFilter, the filter the string
// translator writes; keeping the line aside adds nothing without a line_format), its line filters
// (translate.LineFilter, and translate.PatternLineFilter for |> and !>), its label filters without a parser
// (labelFilter) and its json and logfmt parsers, drops and keeps (stagePipes), in order. A pattern filter that
// needs the extract pipe adds pipes, and so does a parser, a drop of names and a keep; the filters after a pipe
// go into one filter pipe up to the next pipe, as the string translator writes them, except that a label filter
// that is a field filter and directly follows a pipe or a label filter in a filter pipe opens a filter pipe of
// its own (the translator wraps it as it translates it, where a line filter is wrapped once the query is
// assembled). Any other stage, and a label filter after a parser, is not lowered yet.
func logSelect(sel *queryplan.LogSelect, prof Profile) (physical.Op, error) {
	q, err := logSelectQuery(sel, prof)
	if err != nil {
		return nil, err
	}
	return physical.VLQuery{Text: q.String()}, nil
}

// logSelectQuery is the typed query logSelect prints: a range aggregation appends its stats pipe to it.
func logSelectQuery(sel *queryplan.LogSelect, prof Profile) (logsql.Query, error) {
	defs := definitions(sel)
	staged, read, err := stagePipes(sel, prof, defs)
	if err != nil {
		return logsql.Query{}, err
	}
	// The label filters of a metric row read the field the stored value was put back into, not the copy.
	filterRead := read
	if prof.metric {
		filterRead = nil
	}
	lf, err := labelFilters(sel, prof, filterRead, defs)
	if err != nil {
		return logsql.Query{}, err
	}
	built := lf.filters
	if len(lf.bases) > 0 {
		if staged, err = extractedUnpacks(sel, staged, lf.bases); err != nil {
			return logsql.Query{}, err
		}
	}
	var filters, after logsql.AllOf
	pipes := pipeRoom(len(sel.Stages), staged, lf.pre)
	flush := func() {
		switch len(after) {
		case 0:
		case 1:
			pipes = append(pipes, logsql.PipeFilter{Expr: after[0]})
		default:
			pipes = append(pipes, logsql.PipeFilter{Expr: after})
		}
		after = nil
	}
	add := func(fs ...logsql.FilterExpr) {
		if len(pipes) == 0 {
			filters = append(filters, fs...)
		} else {
			after = append(after, fs...)
		}
	}
	// seq numbers the pattern alternatives that extract, across the query.
	seq := 0
	// lastPipe is whether the last part the string translator holds starts with a pipe: a pipe, or a label
	// filter it wrapped in a filter pipe.
	lastPipe := false
	// afterParser is whether a json or logfmt parser came before: the string translator then gives every label
	// filter that is a field filter a filter pipe of its own, wherever it stands.
	afterParser := false
	for i, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.Parse, *queryplan.Drop, *queryplan.Keep, *queryplan.Decolorize, *queryplan.LineFormat, *queryplan.LabelFormat:
			// A parser, a drop, a keep and decolorize are pipes (stagePipes); a drop of matchers and a
			// wildcard pattern write none.
			if len(staged) > 0 && len(staged[i]) > 0 {
				if _, ok := st.(*queryplan.Parse); ok {
					afterParser = true
				}
				flush()
				pipes = append(pipes, staged[i]...)
				lastPipe = true
			}
			continue
		}
		if _, ok := st.(*queryplan.LabelFilter); ok {
			f := built[i]
			if len(lf.pre) > 0 && len(lf.pre[i]) > 0 {
				flush()
				pipes = append(pipes, lf.pre[i]...)
				lastPipe = true
			}
			if (lastPipe || afterParser) && translate.IsFieldFilter(f.String()) {
				flush()
				after = logsql.AllOf{f}
			} else {
				add(f)
				lastPipe = false
			}
			continue
		}
		lf, ok := st.(*queryplan.LineFilter)
		if !ok {
			return logsql.Query{}, &NotLoweredError{Construct: "LogSelect pipeline"}
		}
		if lf.Op == logql.LineFilterContainsPat || lf.Op == logql.LineFilterExcludePat {
			values := []string{lf.Value}
			for _, alt := range lf.Or {
				values = append(values, alt.Value)
			}
			fs, ps := translate.PatternLineFilter(values, lf.Op == logql.LineFilterExcludePat, &seq)
			add(fs...)
			if len(ps) > 0 {
				flush()
				pipes = append(pipes, ps...)
			}
			lastPipe = len(ps) > 0
			continue
		}
		f, ok := translate.LineFilter(lf)
		if !ok {
			return logsql.Query{}, &NotLoweredError{Construct: "LogSelect line filter"}
		}
		if f != nil {
			add(f)
			lastPipe = false
		}
	}
	flush()
	if len(read) > 0 {
		pipes = append(pipes, translate.StreamValueDelete())
	}
	if len(lf.bases) > 0 {
		pipes = append(pipes, translate.ExtractedDelete())
	}
	f, err := translate.StreamSelectorFilter(sel.Matchers, prof.Label, prof.StreamFields)
	if err != nil {
		return logsql.Query{}, err
	}
	return selectQuery(f, filters, pipes), nil
}

// pipeRoom is the empty list the pipes of a query go into, with room for the staged pipes, the pipes ahead of a
// label filter, a filter pipe for each stage and the two deletes; nil when nothing is staged (no pipe is likely).
func pipeRoom(stages int, staged, pre [][]logsql.Pipe) []logsql.Pipe {
	if len(staged) == 0 {
		return nil
	}
	room := stages + 2
	for _, ps := range staged {
		room += len(ps)
	}
	for _, ps := range pre {
		room += len(ps)
	}
	return make([]logsql.Pipe, 0, room)
}

// selectQuery is the query of a stream selector's filter f, the filters written in front of the first pipe and
// the pipes.
func selectQuery(f logsql.FilterExpr, filters logsql.AllOf, pipes []logsql.Pipe) logsql.Query {
	q := logsql.Query{Filter: f, Pipes: pipes}
	if len(filters) > 0 {
		// A selector that keeps every row (`{detected_level!="info"}`) is no filter next to the others, as the
		// string translator writes it.
		if _, all := f.(logsql.Wildcard); all {
			q.Filter = filters
		} else {
			q.Filter = append(logsql.AllOf{f}, filters...)
		}
	}
	return q
}

// labelFilters builds the filter of each label filter stage of the query, by stage index (nil elsewhere), or
// answers NotLoweredError for a query whose label filters the lowering does not write: a filter on
// detected_level or service_name (the string translator derives them), a value that is not a valid literal, and a
// label that translates to several parsed keys or to no field.
func labelFilters(sel *queryplan.LogSelect, prof Profile, sv []string, defs stageDefs) (filterStages, error) {
	var res filterStages
	lb := leafBuilder{label: prof.Label, caps: prof.Caps, sv: sv}
	for i, st := range sel.Stages {
		lb.names.note(defs, i)
		if p, ok := st.(*queryplan.Parse); ok {
			lb.parsed = true
			if p.Type == logql.ParserJSON || p.Type == logql.ParserLogfmt {
				lb.keyParsed = true
				lb.keyParsers++
				lb.addJSONAliases(p)
			}
			continue
		}
		lbl, ok := st.(*queryplan.LabelFilter)
		if !ok {
			continue
		}
		if lbl.Expr == nil {
			return filterStages{}, &NotLoweredError{Construct: "LogSelect label filter"}
		}
		// The labels a regexp captured are read as they are, not translated.
		stageProf := prof
		stageProf.Label = lb.names.label(prof.Label)
		lb.label = stageProf.Label
		if c := checkLabelFilterExpr(lbl.Expr, stageProf); c != "" {
			return filterStages{}, &NotLoweredError{Construct: c}
		}
		if lb.keyParsed {
			if c := checkParsedLabelFilterExpr(lbl.Expr, sel); c != "" {
				return filterStages{}, &NotLoweredError{Construct: c}
			}
		}
		if res.filters == nil {
			res.filters = make([]logsql.FilterExpr, len(sel.Stages))
		}
		res.filters[i] = labelFilter(lbl.Expr, &lb)
		if lb.bad != "" {
			return filterStages{}, &NotLoweredError{Construct: lb.bad}
		}
		if len(lb.stageBases) > 0 {
			if res.pre == nil {
				res.pre = make([][]logsql.Pipe, len(sel.Stages))
			}
			res.pre[i] = translate.ExtractedCoalesce(lb.stageBases, lb.keyParsers, &lb.streamCopied)
			lb.stageBases = lb.stageBases[:0]
		}
	}
	res.bases = lb.bases
	return res, nil
}

// filterStages are the label filters of a log query, by stage index.
type filterStages struct {
	// filters are the filter of each label filter stage (nil elsewhere).
	filters []logsql.FilterExpr
	// pre are the pipes ahead of a stage that filters on name_extracted labels: the value of each from the first
	// parser that extracted it.
	pre [][]logsql.Pipe
	// bases are the labels whose name_extracted rename the filters read; each json or logfmt parser unpacks their
	// keys into scratch fields.
	bases []string
}

// checkLabelFilterExpr names the construct of the first leaf the lowering does not write, "" when it writes all.
func checkLabelFilterExpr(e *logql.LabelFilterExpr, prof Profile) string {
	if !e.IsLeaf() {
		if c := checkLabelFilterExpr(e.Left, prof); c != "" {
			return c
		}
		return checkLabelFilterExpr(e.Right, prof)
	}
	if e.Kind == logql.LabelFilterValueNone {
		return "LogSelect label filter value"
	}
	label := translate.SanitizeFieldIdentifier(e.Name)
	switch label {
	case "":
		return "LogSelect label filter name"
	case "detected_level", "service_name":
		return "LogSelect label filter " + label
	case "extract":
		// The string translator reads `extract ="x"` as its own pass-through extract pipe (for queries it
		// translated before), not as a filter.
		return "LogSelect label filter extract name"
	}
	if prof.Label != nil {
		translated := prof.Label(label)
		if strings.Contains(translated, translate.ParsedKeySep) {
			return "LogSelect label filter parsed keys"
		}
		if translate.SanitizeFieldIdentifier(translated) == "" {
			return "LogSelect label filter name"
		}
	}
	if e.Kind == logql.LabelFilterValueIP && !strings.HasSuffix(e.Leaf, `ip("`+e.Value+`")`) {
		return "LogSelect label filter ip form"
	}
	if !translate.KnownLabelFilterCmp(e.Cmp) {
		return "LogSelect label filter operator"
	}
	return ""
}

// leafBuilder builds the filter of a label filter leaf: the label translation and capabilities of the profile,
// and whether a json or logfmt parser came before the stage (the leaf then names a sanitized parsed key). bad is
// the construct of the first leaf it cannot write, "" while it writes all.
type leafBuilder struct {
	label func(string) string
	caps  logsql.Capabilities
	// parsed is whether a parser of any kind came before the stage; keyParsed whether a json or logfmt parser
	// did (only those name the keys a sanitized label can be stored under).
	parsed, keyParsed bool
	// names are the labels the regexp and pattern stages before the stage define.
	names definedNames
	// aliases are the labels the json stages before the label filter define.
	aliases []jsonAlias
	// sv are the stream labels the query reads after a parser, which the filters read from their scratch copies.
	sv []string
	// keyParsers are the json and logfmt stages before the stage; bases the labels whose name_extracted rename the
	// filters read, stageBases those of the stage; streamCopied is whether _stream was copied aside for them.
	keyParsers   int
	bases        []string
	stageBases   []string
	streamCopied bool
	bad          string
}

// labelFilter is the filter of a label filter stage that checkLabelFilters accepted: the leaf, or the and/or
// tree of leaves in parentheses, each translated like a stand-alone stage (translateLabelFilterExpr).
func labelFilter(e *logql.LabelFilterExpr, lb *leafBuilder) logsql.FilterExpr {
	if e.IsLeaf() {
		return lb.leaf(e)
	}
	var operands []logsql.FilterExpr
	collectLabelFilter(e, e.Op, &operands, lb)
	var tree logsql.FilterExpr
	for i, o := range operands {
		switch {
		case i == 0:
			tree = o
		case e.Op == logql.LabelFilterOr:
			tree = logsql.OrExpr{Left: tree, Right: o, Lower: true}
		default:
			tree = logsql.AndExpr{Left: tree, Right: o, Lower: true}
		}
	}
	return logsql.Group{Expr: tree}
}

// collectLabelFilter flattens a chain of one operator, parenthesizing a leaf that holds a top-level space and a
// nested chain of the other operator (LabelFilterExpr.Render).
func collectLabelFilter(e *logql.LabelFilterExpr, op logql.LabelFilterOp, out *[]logsql.FilterExpr, lb *leafBuilder) {
	switch {
	case e.IsLeaf():
		f := lb.leaf(e)
		if translate.HasTopLevelSpace(f.String()) {
			f = logsql.Group{Expr: f}
		}
		*out = append(*out, f)
	case e.Op != op:
		*out = append(*out, labelFilter(e, lb))
	default:
		collectLabelFilter(e.Left, op, out, lb)
		collectLabelFilter(e.Right, op, out, lb)
	}
}

// leaf is the filter of one leaf: the one translateSingleLabelFilter writes, which names the sanitized keys a
// parser can have produced when a parser came before the stage (parsedLabelFilterLeaf).
func (lb *leafBuilder) leaf(e *logql.LabelFilterExpr) logsql.FilterExpr {
	if lb.parsed {
		f, c := lb.parsedLeaf(e)
		if c != "" && lb.bad == "" {
			lb.bad = c
		}
		return f
	}
	return labelFilterLeaf(e, lb.label, lb.caps)
}

// labelFilterLeaf is translateSingleLabelFilter's generic path for one leaf: the label named like the selector
// names it, quoted where LogsQL needs it, and the filter translate.LabelFilterLeaf builds for its value.
func labelFilterLeaf(e *logql.LabelFilterExpr, labelFn func(string) string, caps logsql.Capabilities) logsql.FilterExpr {
	label := translate.SanitizeFieldIdentifier(e.Name)
	if labelFn != nil {
		label = translate.SanitizeFieldIdentifier(labelFn(label))
	}
	if strings.ContainsAny(label, ".-") {
		label = `"` + label + `"`
	}
	f, _ := translate.LabelFilterLeaf(label, e.Cmp, e.Value, e.Kind == logql.LabelFilterValueIP, caps) // the operator is known
	return f
}

// Construct is the kind of the node at the top of a plan, the unit the shadow
// harness counts delegations by: LogSelect, RangeAgg, VectorAgg, Rank, BinOp,
// LabelReplace, VectorLit, Scalar or Opaque.
func Construct(n queryplan.Node) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", n), "*queryplan.")
}
