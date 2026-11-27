package logql

import (
	"slices"
	"strings"
)

// ParserStages is a query read once for the proxy's parser probe (ReadParserStages): where its json and logfmt
// stages stand, the arguments of its calls kept as text, and the log query its first range aggregation reads. The
// probe reads that log query, and the stage the probe does not keep is then removed (Drop), from one read of the
// text; nothing is written but the removal.
type ParserStages struct {
	query string
	// read is false for a query the parser stopped on: Drop then returns it as it is.
	read bool
	// json and logfmt are the stages' spans: from the end of the token before the stage's `|` to the end of its
	// last token (its flags and field list included).
	json, logfmt [][2]int
	// args are the spans of the arguments of the calls kept as text (label_replace, approx_topk): the parser reads
	// over them, so each one is read on its own.
	args [][2]int
	// logQuery is the span of the first range aggregation's log query, the zero span when it is not one span of
	// the text (stages after the range) or the query holds no range aggregation the parser reads.
	logQuery  [2]int
	rangeSeen bool
}

func (s *ParserStages) noteStage(t ParserType, from, to int) {
	switch t {
	case ParserJSON:
		s.json = append(s.json, [2]int{from, to})
	case ParserLogfmt:
		s.logfmt = append(s.logfmt, [2]int{from, to})
	}
}

// ReadParserStages reads a query Parse accepts (a handler's query, validated before it is read) once, without
// Parse's own validation: its answer for a query Parse refuses is unspecified (DropParserStages checks first).
func ReadParserStages(query string) ParserStages {
	s := ParserStages{query: query}
	p := &parser{sc: newScanner(query), stages: &s}
	p.advance()
	if _, err := p.parseExpr(); err != nil || p.cur.Typ != TokEOF || p.plainUnwrapAt > 0 {
		return ParserStages{query: query}
	}
	s.read = true
	return s
}

// LogQuery is the text of the log query the first range aggregation reads, as written (`{a="b"} | json | logfmt`
// of `sum(count_over_time({a="b"} | json | logfmt [5m]))`), or "" when it is not one span of the text: a log query
// (no range aggregation), stages after the range, or a range aggregation inside a call kept as text.
func (s ParserStages) LogQuery() string {
	if !s.read || s.logQuery[1] <= s.logQuery[0] {
		return ""
	}
	return s.query[s.logQuery[0]:s.logQuery[1]]
}

// Drop removes every `| json` (ParserJSON) or `| logfmt` (ParserLogfmt) stage, with its flags and field list, where
// the parser read it: the stage's `|`, the blank before it and its words go, the rest of the text stays as written.
// A stage that is the last one before a range (`{a="b"} | json | logfmt [5m]`) leaves the range and everything
// after it in place. The arguments of a call kept as text are read the same way, each on its own. A query the
// parser stopped on, or that holds no such stage, is returned as it is.
func (s ParserStages) Drop(drop ParserType) string {
	var spans [][2]int
	switch drop {
	case ParserJSON:
		spans = s.json
	case ParserLogfmt:
		spans = s.logfmt
	default:
		return s.query
	}
	if !s.read || len(spans) == 0 && len(s.args) == 0 {
		return s.query
	}
	// The edits, left to right: a stage removed, or an argument of a call kept as text read on its own. The parser
	// reads over a call's arguments, so no stage span falls inside one.
	type edit struct {
		start, end int
		text       string
	}
	edits := make([]edit, 0, len(spans)+len(s.args))
	for _, sp := range spans {
		edits = append(edits, edit{sp[0], sp[1], ""})
	}
	for _, a := range s.args {
		if arg := s.query[a[0]:a[1]]; strings.Contains(arg, "|") {
			if dropped := ReadParserStages(arg).Drop(drop); dropped != arg {
				edits = append(edits, edit{a[0], a[1], dropped})
			}
		}
	}
	if len(edits) == 0 {
		return s.query
	}
	if len(s.args) > 0 {
		slices.SortFunc(edits, func(a, b edit) int { return a.start - b.start })
	}
	var b strings.Builder
	b.Grow(len(s.query))
	last := 0
	for _, e := range edits {
		if e.start < last || e.end < e.start || e.end > len(s.query) {
			return s.query // overlapping edits: never splice a text the parser did not read once, left to right
		}
		b.WriteString(s.query[last:e.start])
		b.WriteString(e.text)
		last = e.end
	}
	b.WriteString(s.query[last:])
	return b.String()
}

// DropParserStages is ReadParserStages(query).Drop(drop) for any text: a query Parse refuses is returned as it is
// (validation answers Loki's error for it).
func DropParserStages(query string, drop ParserType) string {
	if drop != ParserJSON && drop != ParserLogfmt {
		return query
	}
	if _, err := Parse(query); err != nil {
		return query
	}
	return ReadParserStages(query).Drop(drop)
}
