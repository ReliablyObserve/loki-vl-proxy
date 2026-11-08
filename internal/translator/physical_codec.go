package translator

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"

// DecodePhysical reads a translated query into its physical operators: the post-processing markers become Fold and
// LabelMap around the query, and a binary expression or a number becomes Binary or ScalarOp. It is built from the
// readers the proxy used on the text and in the order it used them: the `without` marker first (it leaves a trailing
// label_replace marker in the query), then the label_replace markers. physical.Split of the result gives, for any
// text, the clean query and the values ParseWithoutMarker and ParseAllLabelReplaceMarkers return.
//
// The error is always nil for now: the text reader has no failure the proxy ever acted on. It is in the signature
// for the typed query, which can fail to parse.
//
// The codec lives in the translator because it reads the translator's text; it goes with the translator.
func DecodePhysical(s string) (physical.Op, error) {
	clean, op, labels := ParseWithoutMarker(s)
	clean, specs := ParseAllLabelReplaceMarkers(clean)
	out := decodeCore(clean)
	if op != "" || len(labels) > 0 {
		out = physical.Fold{Levels: []physical.FoldLevel{{Op: op, Labels: labels, Without: true}}, In: out}
	}
	if len(specs) > 0 {
		out = physical.LabelMap{Replace: specs, In: out}
	}
	// A marker the encoder would write elsewhere or differently (not one the translator wrote) keeps the text.
	if len(clean) != len(s) && !encodesTo(out, s) {
		out = physical.Verbatim{Text: s, In: out}
	}
	return out, nil
}

// encodesTo is EncodePhysical(op) == s without building the text: the markers are matched from the end of s.
func encodesTo(op physical.Op, s string) bool {
	switch v := op.(type) {
	case physical.VLQuery:
		return s == v.Text
	case physical.LabelMap:
		for i := len(v.Replace) - 1; i >= 0; i-- {
			var buf [160]byte
			m := appendLabelReplaceMarker(buf[:0], v.Replace[i])
			if len(s) < len(m) || s[len(s)-len(m):] != string(m) {
				return false
			}
			s = s[:len(s)-len(m)]
		}
		return encodesTo(v.In, s)
	case physical.Fold:
		for i := len(v.Levels) - 1; i >= 0; i-- {
			var ok bool
			if s, ok = trimWithoutMarker(s, v.Levels[i].Op, v.Levels[i].Labels); !ok {
				return false
			}
		}
		return encodesTo(v.In, s)
	}
	return EncodePhysical(op) == s
}

// trimWithoutMarker removes what appendWithoutMarker(_, op, labels) appends from the end of s, and reports whether
// s ended with it.
func trimWithoutMarker(s, op string, labels []string) (string, bool) {
	if len(labels) == 0 {
		return s, true
	}
	n := len(WithoutMarkerSuffix) + len(labels) - 1
	if op != "" {
		n += len(op) + 1
	}
	for _, l := range labels {
		n += len(l)
	}
	if len(s) < n {
		return s, false
	}
	rest, at := s[:len(s)-n], len(s)-n
	eat := func(part string) bool {
		if s[at:at+len(part)] != part {
			return false
		}
		at += len(part)
		return true
	}
	if !eat(WithoutMarkerSuffix) || (op != "" && (!eat(op) || !eat(":"))) {
		return s, false
	}
	for i, l := range labels {
		if (i > 0 && !eat(",")) || !eat(l) {
			return s, false
		}
	}
	return rest, true
}

// decodeCore reads a query without post-processing markers.
func decodeCore(s string) physical.Op {
	if op, left, right, vm, ok := ParseBinaryMetricExprFull(s); ok {
		// The operands are read as plain queries: the markers of the whole text are already out of it, and the
		// translator writes none inside an operand.
		b := physical.Binary{Op: op, L: decodeCore(left), R: decodeCore(right), Text: s}
		// What follows the right operand is the modifier text (`@@@on:app`), if any.
		if tail := s[len(BinaryMetricPrefix)+len(op)+1+len(left)+len("|||")+len(right):]; tail != "" {
			b.Tail = tail
			b.Match = &physical.Matching{MatchOn: vm.MatchOn, GroupSide: vm.GroupSide, On: vm.On, Ignoring: vm.Ignoring, GroupLeft: vm.GroupLeft, GroupRight: vm.GroupRight}
		}
		return b
	}
	if v, ok := scalarValue(s); ok {
		return physical.ScalarOp{V: v, Text: s}
	}
	return physical.VLQuery{Text: s}
}

// EncodePhysical writes the operators DecodePhysical returns back as the text the translator wrote:
// EncodePhysical(DecodePhysical(s)) == s for every s.
func EncodePhysical(op physical.Op) string {
	switch v := op.(type) {
	case physical.VLQuery:
		return v.Text
	case physical.ScalarOp:
		return v.Text
	case physical.Verbatim:
		return v.Text
	case physical.Binary:
		if v.Text != "" {
			return v.Text
		}
		return BinaryMetricPrefix + v.Op + ":" + EncodePhysical(v.L) + "|||" + EncodePhysical(v.R) + v.Tail
	case physical.Fold:
		s := EncodePhysical(v.In)
		for _, level := range v.Levels {
			s = appendWithoutMarker(s, level.Op, level.Labels)
		}
		return s
	case physical.LabelMap:
		s := EncodePhysical(v.In)
		for _, spec := range v.Replace {
			s += labelReplaceMarker(spec)
		}
		return s
	}
	return ""
}
