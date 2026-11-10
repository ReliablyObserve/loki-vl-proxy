package lower

import (
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
func stagePipes(sel *queryplan.LogSelect, prof Profile) (built [][]logsql.Pipe, read []string, err error) {
	parsed := false
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
	for i, st := range sel.Stages {
		var ps []logsql.Pipe
		switch st := st.(type) {
		case *queryplan.LineFilter:
			err = lineFilterAfter(sel, i)
		case *queryplan.LabelFilter:
		case *queryplan.Parse:
			ps, err = parserPipes(st)
			ps = streamValuePipes(ps, read, !parsed, prof.metric)
			parsed = true
		case *queryplan.Drop:
			ps, err = dropPipes(st)
		case *queryplan.Keep:
			ps, err = keepPipes(st, prof)
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
// text the string translator reads as holding the operator: a parser with flags or an extraction list, a drop or
// a keep of matchers. The translator takes the operator for part of the stage's arguments and writes no filter.
func lineFilterAfter(sel *queryplan.LogSelect, i int) error {
	lf := sel.Stages[i].(*queryplan.LineFilter)
	if i == 0 || (lf.Op != logql.LineFilterExcludes && lf.Op != logql.LineFilterExcludeRe && lf.Op != logql.LineFilterExcludePat) {
		return nil
	}
	arguments := false
	switch prev := sel.Stages[i-1].(type) {
	case *queryplan.Parse:
		arguments = len(prev.Fields) > 0 || prev.Strict || prev.KeepEmpty
	case *queryplan.Drop:
		arguments = len(prev.Matchers) > 0
	case *queryplan.Keep:
		arguments = len(prev.Matchers) > 0
	}
	if arguments {
		return &NotLoweredError{Construct: "LogSelect line filter after parser arguments"}
	}
	return nil
}

// parserPipes are the pipes of a json or logfmt stage.
func parserPipes(st *queryplan.Parse) ([]logsql.Pipe, error) {
	switch st.Type {
	case logql.ParserJSON:
		ps := []logsql.Pipe{translate.JSONUnpack()}
		if len(st.Fields) > 0 {
			copies, ok := translate.JSONAliasCopies(st.Fields, st.Param)
			if !ok {
				return nil, &NotLoweredError{Construct: "LogSelect json extraction"}
			}
			ps = append(ps, copies...)
		}
		return ps, nil
	case logql.ParserLogfmt:
		return []logsql.Pipe{translate.LogfmtUnpack()}, nil
	case logql.ParserRegexp:
		return nil, &NotLoweredError{Construct: "LogSelect regexp parser"}
	case logql.ParserPattern:
		return nil, &NotLoweredError{Construct: "LogSelect pattern parser"}
	}
	return nil, &NotLoweredError{Construct: "LogSelect unpack parser"}
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
func keepPipes(st *queryplan.Keep, prof Profile) ([]logsql.Pipe, error) {
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
	return []logsql.Pipe{translate.KeepFields(names, prof.Label)}, nil
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
		}
	}
	return false
}
