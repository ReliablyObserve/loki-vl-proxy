package lower

import (
	"slices"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// stagePipes builds the pipes of each json, logfmt, drop and keep stage of a log query, by stage index (nil for a
// stage that writes none), the way the string translator writes them for a log query (translate.JSONUnpack and
// the builders next to it). It answers NotLoweredError for a pipeline the lowering does not write:
//   - a stage that is none of those and not a line or label filter;
//   - a parser and a stream label the string translator copies aside because the pipeline reads it;
//   - a stage whose text the string translator reads differently from the typed stage (translate.JSONAliasCopies,
//     a keep that mixes names and matchers, a matcher value holding the characters its splitting trips on).
func stagePipes(sel *queryplan.LogSelect, prof Profile, defs stageDefs) (built [][]logsql.Pipe, read []string, err error) {
	parsed := false
	// keepLine is whether the first line_format still has to keep the stored line aside.
	keepLine := prof.KeepLine
	var names definedNames
	// A stream label the pipeline reads after a parser: the string translator copies its stored value aside.
	// The query stays with it unless the label filters alone read it (streamValues); the construct is answered
	// once the stages themselves are known to lower, so a query reports its first cause.
	var streamErr error
	if names := translate.StreamLabelNames(prof.Label); len(names) > 0 && hasParser(sel) {
		if prof.metric {
			// A metric query puts the stored value of every stream label back behind each parser (and so reads
			// the label from its own field): the labels the pipeline reads make no difference.
			read = names
		} else if readsStreamLabel(sel, names) {
			streamErr = &NotLoweredError{Construct: "LogSelect stream label after parser"}
		} else if r, c := streamValues(sel, names); c != "" {
			streamErr = &NotLoweredError{Construct: c}
		} else {
			read = r
		}
	}
	if hasParser(sel) {
		read = aliasStreamReads(sel, prof, read)
	}
	for i, st := range sel.Stages {
		var ps []logsql.Pipe
		names.note(defs, i)
		if stageNamesReservedField(st) {
			return nil, nil, &NotLoweredError{Construct: reservedStageConstruct}
		}
		switch st := st.(type) {
		case *queryplan.LineFilter:
			err = lineFilterAfter(sel, i)
		case *queryplan.LabelFilter:
		case *queryplan.Parse:
			ps, err = parserPipes(st)
			if err != nil && st.Type == logql.ParserUnpack && !prof.metric {
				err = &NotLoweredError{Construct: unpackLineConstruct}
			}
			if err == nil && len(ps) == 0 && len(read) > 0 {
				// A wildcard pattern writes no pipe, and the string translator then copies the stream labels
				// aside without the fill that follows a parser's pipe.
				err = &NotLoweredError{Construct: "LogSelect no-op pattern with stream label"}
			}
			ps = streamValuePipes(ps, read, !parsed, prof.metric)
			parsed = true
		case *queryplan.Drop:
			if prof.metric {
				ps, err = metricDropPipes(st)
			} else {
				ps, err = dropPipes(st)
			}
		case *queryplan.Keep:
			if prof.metric {
				ps, err = metricKeepPipes(st, names.label(prof.Label), prof)
			} else {
				ps, err = keepPipes(st, names.label(prof.Label), prof)
			}
		case *queryplan.Decolorize:
			ps = []logsql.Pipe{translate.Decolorize()}
		case *queryplan.LineFormat:
			var reason string
			if ps, reason = translate.LineFormat(st.Template, keepLine); reason == "" {
				keepLine = false
			} else {
				err = &NotLoweredError{Construct: "LogSelect line format " + reason}
			}
		case *queryplan.LabelFormat:
			var reason string
			if ps, reason = translate.LabelFormats(st.Formats); reason != "" {
				err = &NotLoweredError{Construct: "LogSelect label format " + reason}
			}
		default:
			err = &NotLoweredError{Construct: "LogSelect pipeline"}
		}
		if err != nil {
			return nil, nil, err
		}
		if len(ps) > 0 {
			if built == nil {
				built = make([][]logsql.Pipe, len(sel.Stages))
			}
			built[i] = ps
		}
	}
	if streamErr != nil {
		return nil, nil, streamErr
	}
	return built, read, nil
}

// hasParser reports whether the query has a json or logfmt parser stage.
func hasParser(sel *queryplan.LogSelect) bool {
	for _, st := range sel.Stages {
		if _, ok := st.(*queryplan.Parse); ok {
			return true
		}
	}
	return false
}

// lineFilterAfter answers NotLoweredError for a negative line filter (!=, !~, !>) right after a stage whose
// text the string translator reads as holding the operator: a parser with flags, an extraction list or a quoted argument (regexp, pattern), a drop or
// a keep of matchers. The translator takes the operator for part of the stage's arguments and writes no filter.
func lineFilterAfter(sel *queryplan.LogSelect, i int) error {
	lf := sel.Stages[i].(*queryplan.LineFilter)
	if i == 0 || (lf.Op != logql.LineFilterExcludes && lf.Op != logql.LineFilterExcludeRe && lf.Op != logql.LineFilterExcludePat) {
		return nil
	}
	arguments := false
	switch prev := sel.Stages[i-1].(type) {
	case *queryplan.Parse:
		// A regexp or pattern has its quoted argument, which the translator also reads the operator into.
		arguments = len(prev.Fields) > 0 || prev.Strict || prev.KeepEmpty || prev.Type == logql.ParserRegexp || prev.Type == logql.ParserPattern
	case *queryplan.Drop:
		arguments = len(prev.Matchers) > 0
	case *queryplan.Keep:
		arguments = len(prev.Matchers) > 0
	case *queryplan.LineFormat:
		arguments = true // the template is a quoted argument
	case *queryplan.LabelFormat:
		arguments = true // the assignments are arguments
	}
	if arguments {
		return &NotLoweredError{Construct: "LogSelect line filter after parser arguments"}
	}
	return nil
}

// The pipes of a json and a logfmt stage with no extraction list. The slices are shared and never written to: they
// have no spare room (a caller that appends copies them) and the pipes are values.
var (
	jsonUnpackPipes   = []logsql.Pipe{translate.JSONUnpack()}[0:1:1]
	logfmtUnpackPipes = []logsql.Pipe{translate.LogfmtUnpack()}[0:1:1]
)

// parserPipes are the pipes of a json, logfmt, regexp or pattern stage.
func parserPipes(st *queryplan.Parse) ([]logsql.Pipe, error) {
	switch st.Type {
	case logql.ParserJSON:
		if len(st.Fields) == 0 {
			return jsonUnpackPipes, nil
		}
		copies, ok := translate.JSONAliasCopies(st.Fields, st.Param)
		if !ok {
			return nil, &NotLoweredError{Construct: "LogSelect json extraction"}
		}
		ps := make([]logsql.Pipe, 0, 1+len(copies))
		return append(append(ps, translate.JSONUnpack()), copies...), nil
	case logql.ParserLogfmt:
		return logfmtUnpackPipes, nil
	case logql.ParserRegexp:
		return []logsql.Pipe{translate.ExtractRegexp(st.Param)}, nil
	case logql.ParserPattern:
		if p, ok := translate.ExtractPattern(st.Param); ok {
			return []logsql.Pipe{p}, nil
		}
		return nil, nil // a wildcard-only pattern writes no pipe
	}
	return nil, &NotLoweredError{Construct: unpackParserConstruct}
}

// dropPipes are the pipes of a drop stage: none for a drop of matchers, whose values the string translator's
// splitting reads wrongly when they hold a quote.
func dropPipes(st *queryplan.Drop) ([]logsql.Pipe, error) {
	for _, m := range st.Matchers {
		if hasQuote(m.Value) {
			return nil, &NotLoweredError{Construct: "LogSelect drop matcher value"}
		}
	}
	if p, ok := translate.DropDelete(st.Labels); ok {
		return []logsql.Pipe{p}, nil
	}
	return nil, nil
}

// keepPipes are the pipes of a keep stage over its names or its matchers' names. A keep that holds both loses the
// order they were written in, and a matcher value with a quote or an operator character is read wrongly by the
// string translator.
func keepPipes(st *queryplan.Keep, label func(string) string, prof Profile) ([]logsql.Pipe, error) {
	if len(st.Labels) > 0 && len(st.Matchers) > 0 {
		return nil, &NotLoweredError{Construct: "LogSelect keep order"}
	}
	names := append([]string(nil), st.Labels...)
	for _, m := range st.Matchers {
		if hasQuote(m.Value) || hasOperatorByte(m.Value) {
			return nil, &NotLoweredError{Construct: "LogSelect keep matcher value"}
		}
		names = append(names, m.Name)
	}
	return []logsql.Pipe{translate.KeepFields(names, label)}, nil
}

// hasQuote reports a quote character, which the string translator's comma splitting of a drop or keep list
// reads as the end of a value.
func hasQuote(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '"' || s[i] == '`' {
			return true
		}
	}
	return false
}

// hasOperatorByte reports a character of a matcher operator, which the string translator's reading of a keep
// matcher takes for the operator.
func hasOperatorByte(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '=', '~', '!':
			return true
		}
	}
	return false
}

// readsStreamLabel reports whether a stage names one of the stream labels in a place the string translator
// copies its stored value aside for: a drop or keep matcher's name, a json or logfmt extraction name (the label
// filters that read one are streamValues'). It is wider than the translator's reading, never narrower: a query
// it reports stays with the translator.
func readsStreamLabel(sel *queryplan.LogSelect, names []string) bool {
	if len(names) == 0 {
		return false
	}
	named := func(n string) bool {
		s := translate.SanitizeFieldIdentifier(n)
		for _, name := range names {
			if n == name || s == name {
				return true
			}
		}
		return false
	}
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Drop:
			for _, m := range st.Matchers {
				if named(m.Name) {
					return true
				}
			}
		case *queryplan.Keep:
			for _, m := range st.Matchers {
				if named(m.Name) {
					return true
				}
			}
		case *queryplan.Parse:
			for _, f := range st.Fields {
				if named(f.Name) {
					return true
				}
			}
		case *queryplan.LabelFormat:
			// The translator reads a label_format's destination and its rename source as names the pipeline reads.
			for _, f := range st.Formats {
				if named(f.Name) || f.Rename && named(f.Value) {
					return true
				}
			}
		}
	}
	return false
}

// stageDefs are the labels the stages of a query that define labels by name define, in stage order, read from the
// regexp, pattern and label_format stages once for the query: a regexp's are captured, the others defined.
type stageDefs []stageDef

// stageDef is the labels one stage defines.
type stageDef struct {
	stage    int
	captured bool
	names    []string
}

// definitions reads the labels the regexp, pattern and label_format stages of the query define; nil when there is
// no such stage, and when no label filter and no keep follows to read them (a regexp is parsed to find its groups).
func definitions(sel *queryplan.LogSelect) stageDefs {
	if !readsDefinitions(sel) {
		return nil
	}
	var defs stageDefs
	for i, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Parse:
			switch st.Type {
			case logql.ParserRegexp:
				defs = appendDef(defs, sel, stageDef{stage: i, captured: true, names: translate.SharedRegexpCaptureNames(st.Param)})
			case logql.ParserPattern:
				defs = appendDef(defs, sel, stageDef{stage: i, names: translate.PatternCaptureNames(st.Param)})
			}
		case *queryplan.LabelFormat:
			def := stageDef{stage: i, names: make([]string, 0, len(st.Formats))}
			for _, f := range st.Formats {
				def.names = append(def.names, f.Name)
			}
			defs = appendDef(defs, sel, def)
		}
	}
	return defs
}

// appendDef appends a stage's definitions, in a list with room for every stage that can define labels.
func appendDef(defs stageDefs, sel *queryplan.LogSelect, def stageDef) stageDefs {
	if defs == nil {
		n := 0
		for _, st := range sel.Stages[def.stage:] {
			switch st.(type) {
			case *queryplan.Parse, *queryplan.LabelFormat:
				n++
			}
		}
		defs = make(stageDefs, 0, n)
	}
	return append(defs, def)
}

// readsDefinitions reports whether a stage reads the labels the regexp, pattern and label_format stages define: a
// label filter (it names its label as defined, not as a parser key) and a keep (it translates its names with them).
func readsDefinitions(sel *queryplan.LogSelect) bool {
	for _, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.LabelFilter, *queryplan.Keep:
			return true
		}
	}
	return false
}

// definedNames are the labels the stages before a stage define by name: the named groups of a regexp and the
// placeholders of a pattern. The string translator keeps them in two sets, read by the stages that follow; here
// they are the definitions of the stages noted so far (defs[:n]), read in place.
type definedNames struct {
	defs stageDefs
	n    int
}

// note adds the labels stage i defines; it is called for the stages in order.
func (d *definedNames) note(defs stageDefs, i int) {
	d.defs = defs
	for d.n < len(defs) && defs[d.n].stage <= i {
		d.n++
	}
}

// label is the label translation the stages after a regexp read: a name the regexp captured is its own field,
// not translated.
func (d *definedNames) label(base func(string) string) func(string) string {
	noted := d.defs[:d.n]
	if !anyCaptured(noted) {
		return base
	}
	return func(l string) string {
		if capturedIn(noted, l) || base == nil {
			return l
		}
		return base(l)
	}
}

// exact reports whether the label is one a regexp or pattern defines, which a parser's keys do not stand for.
func (d *definedNames) exact(label string) bool {
	for _, def := range d.defs[:d.n] {
		if slices.Contains(def.names, label) {
			return true
		}
	}
	return false
}

// anyCaptured reports a regexp group among the definitions.
func anyCaptured(defs stageDefs) bool {
	for _, def := range defs {
		if def.captured && len(def.names) > 0 {
			return true
		}
	}
	return false
}

// capturedIn reports a regexp group named label among the definitions.
func capturedIn(defs stageDefs, label string) bool {
	for _, def := range defs {
		if def.captured && slices.Contains(def.names, label) {
			return true
		}
	}
	return false
}
