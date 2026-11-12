package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The `| unpack` parser. Loki's unpack reads a line a packing agent wrote (a JSON object holding an `_entry` key
// with a string): only then does it promote the object's string keys to labels, and it replaces the line with the
// value of `_entry`. A line without a string `_entry`, one that is not an object, or one that is empty goes on
// unchanged and gets no label (a line that is not JSON object also gets __error__).
//
// VictoriaLogs has no pipe with that rule. `unpack_json` promotes every key of every JSON line (numbers, booleans
// and nested values too, as text), never touches the line, and an empty value is a missing one. The string
// translator writes `| unpack` as `| unpack_json`. That text keeps the lines Loki keeps wherever no later stage reads
// what the unpack produced: the number of lines a count, a rate or an absence check sees per stream is the same
// whatever the unpack promotes, and a drop or a keep after it changes labels, not lines. It is wrong where a later
// stage reads the promoted labels (a label filter, an unwrapped value, a label_format) or the line (a line filter, a
// parser, a line_format, decolorize, the bytes of the line), and for a log query, which returns the line.
//
// What the text does not carry is the split of the count into one series per set of promoted labels, which Loki
// makes and which unpack_json promotes too many keys for: it is the series approximation the json lowering ships,
// handled where the proxy reads the answer (it reads an unpack as a json parser there).
//
// So only a plain range aggregation (count_over_time, rate, absent_over_time) over a query whose only parser is the
// unpack and whose stages after it are drops and keeps is lowered, written as the json parser the translator writes
// for it (the same pipe, so the text is equal to the translator's); every other shape is delegated under the name
// of what it reads (semantics/translator-writes-unpack-as-unpack-json).

// The constructs this file delegates. The metric ones carry the prefix of the aggregation that owns the query.
const (
	// unpackLineConstruct is a log query holding an unpack: it returns the line, which Loki replaces by `_entry`.
	unpackLineConstruct = "LogSelect unpack line"
	// unpackLineReadConstruct is a stage or a range function that reads the line the unpack replaced.
	unpackLineReadConstruct = "unpack line read"
	// unpackLabelReadConstruct is a stage or a range function that reads the labels the unpack promoted.
	unpackLabelReadConstruct = "unpack label read"
	// unpackOrderConstruct is an unpack among other parsers, which read the line it replaced or the labels it set.
	unpackOrderConstruct = "unpack parser order"
	// unpackParserConstruct is the unpack in a query the lowering does not read the stages of (a vector
	// aggregation's operand, a nest).
	unpackParserConstruct = "LogSelect unpack parser"
)

// unpackStage is the index of the unpack stage of the log query, or -1.
func unpackStage(sel *queryplan.LogSelect) int {
	for i, st := range sel.Stages {
		if p, ok := st.(*queryplan.Parse); ok && p.Type == logql.ParserUnpack {
			return i
		}
	}
	return -1
}

// unpackRange is the range aggregation to lower for one over an unpack: ra itself when it has none, a copy whose
// unpack stage is the json stage the translator writes the same pipe for when its result reads neither the line nor
// the labels the unpack makes, and a NotLoweredError naming what it reads otherwise.
func unpackRange(ra *queryplan.RangeAgg) (*queryplan.RangeAgg, error) {
	at := unpackStage(ra.In)
	if at < 0 {
		return ra, nil
	}
	reads := func(what string) error { return &NotLoweredError{Construct: "RangeAgg LogSelect " + what} }
	parsers := 0
	for _, st := range ra.In.Stages {
		if _, ok := st.(*queryplan.Parse); ok {
			parsers++
		}
	}
	if parsers != 1 {
		return nil, reads(unpackOrderConstruct)
	}
	// An unwrapped value is a promoted label, whichever function reads it.
	if unwrapStage(ra.In) != nil {
		return nil, reads(unpackLabelReadConstruct)
	}
	switch ra.Op {
	case logql.RangeCountOverTime, logql.RangeRate, logql.RangeAbsentOverTime:
	case logql.RangeBytesOverTime, logql.RangeBytesRate:
		return nil, reads(unpackLineReadConstruct)
	default:
		return nil, reads(unpackLabelReadConstruct)
	}
	for _, st := range ra.In.Stages[at+1:] {
		switch st.(type) {
		case *queryplan.Drop, *queryplan.Keep:
		case *queryplan.LabelFilter, *queryplan.LabelFormat:
			return nil, reads(unpackLabelReadConstruct)
		default:
			return nil, reads(unpackLineReadConstruct)
		}
	}
	in := *ra.In
	in.Stages = append([]queryplan.Stage(nil), ra.In.Stages...)
	in.Stages[at] = &queryplan.Parse{Type: logql.ParserJSON}
	cp := *ra
	cp.In = &in
	return &cp, nil
}
