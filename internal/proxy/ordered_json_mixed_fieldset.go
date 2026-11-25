package proxy

import (
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// A window's stored field names bound the keys of every row in it. VictoriaLogs lists them from its column index
// (a call of a fraction of a millisecond over a day of a million rows), so the conditions the class checks on every
// row's packed envelope (the key grammar, aliases, the label itself, the scratch names, the packed length) can be
// settled once per request from that list plus a few per-field guards, and the packed envelope, the costliest step
// of the native count, is not built for the count at all. The exact complement still packs it.
//
// Everything below only ever makes a row less likely to be counted natively: a row a guard rejects goes to the
// exact complement, which reads it with the original evaluator, so a guard stricter than the one it replaces is
// always sound and a looser one never is.

const (
	// The built-in fields of every row.
	mixedCertTimeValueMax     = 48 // RFC 3339 with nanoseconds is 35 bytes.
	mixedCertStreamIDValueMax = 64 // The stream id is 48 hex digits.
	mixedCertMsgMax           = 1200
	mixedCertStreamMax        = 300
	mixedCertOtherValueMax    = 48
	mixedCertOtherFieldsMax   = 6
	mixedCertOtherNameMax     = 32
	// With measured lengths (see mixedClassLengths) the field count is bounded by the byte budget alone, and by
	// this ceiling on the names listed, which keeps the guards and the measuring query short.
	mixedCertMeasuredFieldsMax = 32
	// A measured maximum is rounded up to this step, so a window whose longest value grows a little between the
	// measurement and the scan keeps its rows on the native count.
	mixedCertLengthStep = 16
)

var mixedCertBuiltin = map[string]struct{}{"_time": {}, "_stream_id": {}, "_stream": {}, "_msg": {}}

// mixedClassFieldNamesClean reports whether every stored field name of a window is acceptable to the mixed-parser
// row class for the extracted label: a plain identifier of at most 128 characters that is neither the label itself
// (Loki would rename the parsed label to <label>_extracted) nor an alias of another name (two underscores in a row or
// a trailing underscore collapse under the underscore label style) nor a scratch name of the class. These are
// exactly the conditions the envelope guards check on the packed row text, per row: for the whole set they hold for
// every key of every row.
func mixedClassFieldNamesClean(label string, names []string) bool {
	for _, name := range names {
		if !mixedClassFieldNameClean(label, name) {
			return false
		}
	}
	return true
}

func mixedClassFieldNameClean(label, name string) bool {
	if name == "" || len(name) > 128 || name == label {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return !strings.Contains(name, "__") && !strings.HasSuffix(name, "_")
}

// mixedClassCertifiedWorstPacked is the longest packed envelope (pack_json of all fields) a row can have under the
// guards of a certified class, for the given non-built-in field names. VictoriaLogs escapes ' and < as six-byte
// \u00XX sequences, control characters likewise, and " and \ as two bytes. The certified guards admit neither ' nor <
// nor a control character in the message, the stream or another field, so every character takes at most two bytes
// (the message guard also excludes \). Each field costs its name, a colon, two commas' share and four quotes.
func mixedClassCertifiedWorstPacked(otherNames []string) int {
	field := func(name string, value int) int { return len(name) + 6 + value }
	total := 2 // braces
	total += field("_time", mixedCertTimeValueMax)
	total += field("_stream_id", mixedCertStreamIDValueMax)
	total += field("_stream", 2*mixedCertStreamMax)
	total += field("_msg", 2*mixedCertMsgMax)
	for _, name := range otherNames {
		total += field(name, 2*mixedCertOtherValueMax)
	}
	return total
}

// mixedClassLengths holds, for a window, the longest value in bytes of each stored field the certified guards
// bound: "_msg", "_stream" and every non-built-in field. VictoriaLogs answers them with one stats call over the
// window (the same call that lists the field names, widened the same way).
type mixedClassLengths map[string]int

// mixedCertBounds are the per-field value bounds a certified class enforces, one guard each.
type mixedCertBounds struct {
	msg, stream int
	other       map[string]int
}

func mixedCertRound(n int) int {
	if n < 1 {
		n = 1
	}
	return (n + mixedCertLengthStep - 1) / mixedCertLengthStep * mixedCertLengthStep
}

// mixedCertBoundsFor derives the bounds from measured lengths: each is the measured maximum rounded up. It reports
// false when a length is missing or negative.
func mixedCertBoundsFor(others []string, lengths mixedClassLengths) (mixedCertBounds, bool) {
	get := func(name string) (int, bool) {
		n, ok := lengths[name]
		return n, ok && n >= 0
	}
	msg, ok1 := get("_msg")
	stream, ok2 := get("_stream")
	if !ok1 || !ok2 {
		return mixedCertBounds{}, false
	}
	b := mixedCertBounds{msg: mixedCertRound(msg), stream: mixedCertRound(stream), other: make(map[string]int, len(others))}
	for _, name := range others {
		n, ok := get(name)
		if !ok {
			return mixedCertBounds{}, false
		}
		b.other[name] = mixedCertRound(n)
	}
	return b, true
}

// mixedClassCertifiedWorstPackedBounds is mixedClassCertifiedWorstPacked for explicit per-field bounds: the same
// two bytes per character, the same per-field overhead.
func mixedClassCertifiedWorstPackedBounds(otherNames []string, b mixedCertBounds) int {
	field := func(name string, value int) int { return len(name) + 6 + value }
	total := 2
	total += field("_time", mixedCertTimeValueMax)
	total += field("_stream_id", mixedCertStreamIDValueMax)
	total += field("_stream", 2*b.stream)
	total += field("_msg", 2*b.msg)
	for _, name := range otherNames {
		total += field(name, 2*b.other[name])
	}
	return total
}

// certified derives the class for a window whose stored field names are names, with the fixed bounds of the
// certificate (see certifiedFor for the measured form).
func (c *mixedJSONRowClass) certified(names []string) (*mixedJSONRowClass, bool) {
	return c.certifiedFor(names, nil)
}

// certifiedFor derives the class for a window whose stored field names are names. With lengths nil the value
// bounds are the fixed ones (a message of 1200, a stream of 300, another field of 48 bytes, at most six other
// fields). With the window's measured maximum lengths each bound is that maximum rounded up, and the number of
// other fields is limited by the packed-envelope budget alone. It reports false, leaving the caller on the class
// itself, when a name is not clean, a length is missing, or the packed envelope under the bounds could exceed the
// class's own bound. A row longer than its bound simply fails its guard and goes to the exact complement, as every
// row a guard rejects does, so the bounds only decide how much is counted natively, never what the answer is.
func (c *mixedJSONRowClass) certifiedFor(names []string, lengths mixedClassLengths) (*mixedJSONRowClass, bool) {
	if !mixedClassFieldNamesClean(c.label, names) {
		return nil, false
	}
	var others []string
	for _, name := range names {
		if _, ok := mixedCertBuiltin[name]; ok {
			continue
		}
		if len(name) > mixedCertOtherNameMax {
			return nil, false
		}
		others = append(others, name)
	}
	bound, err := strconv.Atoi(mixedClassMaxBytes)
	if err != nil || len(c.base) < 3 {
		return nil, false
	}
	var bounds mixedCertBounds
	if lengths == nil {
		if len(others) > mixedCertOtherFieldsMax {
			return nil, false
		}
		bounds = mixedCertBounds{msg: mixedCertMsgMax, stream: mixedCertStreamMax, other: make(map[string]int, len(others))}
		for _, name := range others {
			bounds.other[name] = mixedCertOtherValueMax
		}
	} else {
		if len(others) > mixedCertMeasuredFieldsMax {
			return nil, false
		}
		var ok bool
		if bounds, ok = mixedCertBoundsFor(others, lengths); !ok {
			return nil, false
		}
	}
	if mixedClassCertifiedWorstPackedBounds(others, bounds) > bound {
		return nil, false
	}
	regex := func(field, pattern string, negate bool) logsql.FilterExpr {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: pattern, Negate: negate}
	}
	// The message: printable ASCII without ' < = \ (the class's own grammar and its second guard, which keeps = and \
	// out, with ' and < added), bounded in length.
	guards := logsql.AllOf{
		regex("_msg", `^\{[\x20-\x26\x28-\x3b\x3e-\x5b\x5d-\x7e]*\}$`, false),
		logsql.LenRange{Field: "_msg", Min: "0", Max: strconv.Itoa(bounds.msg)},
		logsql.LenRange{Field: mixedClassStreamField, Min: "0", Max: strconv.Itoa(bounds.stream)},
		regex(mixedClassStreamField, `['<]`, true),
	}
	for _, name := range others {
		guards = append(guards,
			logsql.LenRange{Field: name, Min: "0", Max: strconv.Itoa(bounds.other[name])},
			regex(name, `^[\x20-\x26\x28-\x3b\x3d-\x7e]*$`, false))
	}
	// The class's remaining guards, after its first three (the message grammar, the second message guard and the
	// message length, replaced above), without the five that read the packed envelope and without the three that read
	// the stream descriptor's label names (mixedClassStreamNameGuards): a stream label is a stored field, and the names
	// of the window's stored fields are checked clean before the scan and again after it (mixedClassFieldNamesClean:
	// an identifier, not the label, no doubled or trailing underscore), which is exactly what those guards test per row.
	guards = append(guards, c.base[3])
	guards = append(guards, c.base[mixedClassStreamNameGuardsAt+mixedClassStreamNameGuards:]...)
	nativePrepare := make([]logsql.Pipe, 0, len(c.prepare)-1)
	for _, pipe := range c.prepare {
		if _, packed := pipe.(logsql.PipePackJSON); !packed {
			nativePrepare = append(nativePrepare, pipe)
		}
	}
	derived := *c
	derived.nativePrepare = nativePrepare
	derived.safe = guards
	return &derived, true
}
