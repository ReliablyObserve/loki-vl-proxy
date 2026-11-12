package physical

import (
	"encoding/base64"
	"strings"
)

// The markers of the legacy text encoding: the post-processing the string translator carries inside the LogsQL it
// writes. Encode writes them; translator.DecodePhysical reads them (the reader lives with the translator's other
// text readers), and the translator's own constants are these.
const (
	// WithoutMarkerSuffix opens the marker of a query that needs `without` post-processing:
	// `...query__without__:pod,node` folds the rows with the aggregation of the whole query (the proxy reads it
	// from the LogQL), `...query__without__:<op>:pod,node` says the query is a `<op> without (...)` the proxy folds
	// itself (op is sum, min, max, avg or count).
	WithoutMarkerSuffix = "__without__:"
	// LabelReplacePrefix opens a label_replace marker: the prefix, the base64 of the four arguments, then `__`.
	LabelReplacePrefix = "__lvp_lr:"
	// BinaryPrefix opens a binary expression: `__binary__:<op>:<left>|||<right>` and the modifier text.
	BinaryPrefix = "__binary__:"
	// MatchingPrefix opens each vector matching modifier of a binary expression's modifier text, after the right
	// operand: `@@@on:app, pod@@@group_left:x`, the keyword and its labels as the query spells them.
	MatchingPrefix = "@@@"
)

// AppendWithoutMarker appends the `without` marker of labels (and the folding op, "" when the query's own
// aggregation folds) to query. A query with no labels is returned as it is.
func AppendWithoutMarker(query, op string, labels []string) string {
	if len(labels) == 0 {
		return query
	}
	if op != "" {
		op += ":"
	}
	return query + WithoutMarkerSuffix + op + strings.Join(labels, ",")
}

// AppendLabelReplaceMarker appends the label_replace marker of spec to dst.
func AppendLabelReplaceMarker(dst []byte, spec LabelReplace) []byte {
	var buf [96]byte
	payload := append(buf[:0], spec.DstLabel...)
	payload = append(append(payload, 0), spec.Replacement...)
	payload = append(append(payload, 0), spec.SrcLabel...)
	payload = append(append(payload, 0), spec.Regex...)
	dst = append(dst, LabelReplacePrefix...)
	dst = base64.StdEncoding.AppendEncode(dst, payload)
	return append(dst, "__"...)
}

// Encode writes the operators as the text the string translator writes: the query with its markers. It is what the
// proxy sends on, and translator.EncodePhysical(translator.DecodePhysical(s)) == s for every s. A RankStep and a
// SeriesAgg are levels the proxy evaluates over the answer of their operand and have no text: Encode writes the
// operand, the request the proxy sends. DecodePhysical never produces them.
func Encode(op Op) string {
	switch v := op.(type) {
	case VLQuery:
		return v.Text
	case ScalarOp:
		return v.Text
	case VectorOp:
		return v.Text
	case Verbatim:
		return v.Text
	case RankStep:
		return Encode(v.In)
	case SeriesAgg:
		return Encode(v.In)
	case Binary:
		if v.Text != "" {
			return v.Text
		}
		return BinaryPrefix + v.Op + ":" + Encode(v.L) + "|||" + Encode(v.R) + v.Tail
	case Fold:
		s := Encode(v.In)
		for _, level := range v.Levels {
			s = AppendWithoutMarker(s, level.Op, level.Labels)
		}
		return s
	case LabelMap:
		b := append([]byte(nil), Encode(v.In)...)
		for _, spec := range v.Replace {
			b = AppendLabelReplaceMarker(b, spec)
		}
		return string(b)
	}
	return ""
}
