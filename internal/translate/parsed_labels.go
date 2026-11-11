package translate

import (
	"strconv"
	"strings"
)

// The labels of a stats pipe that a json or logfmt parser produced under their sanitized name. Loki's parsers turn
// every extracted key into a label name (http.method and http-method both become http_method) where VictoriaLogs'
// unpack_json and unpack_logfmt keep the key as it is written, so a stats pipe that groups by, or aggregates, such
// a label first gives it the value of each spelling a line may hold. The string translator and the lowering write
// the same pipes from these builders.

// ParsedLabelCandidate reports whether label may be a sanitized parser key that a stats pipe has to resolve: a
// plain identifier with an underscore that is not its first character, and not one of the labels the proxy
// derives (service_name, detected_level). Whether the query defines the label itself is the caller's to say.
func ParsedLabelCandidate(label string) bool {
	if label == "service_name" || label == DetectedLevelLabel {
		return false
	}
	underscore := false
	for i := 0; i < len(label); i++ {
		c := label[i]
		switch {
		case c == '_':
			if i == 0 {
				return false
			}
			underscore = true
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return underscore
}

// ParsedKeyPipesText writes the pipes that give label the value of each spelling of its key other than its own (the
// dotted and the hyphenated ones, ParsedKeyVariants), keeping the value a line already stores under the label,
// each pipe after a blank: `| format if ("a.b":*) "<a.b>" as a_b keep_original_fields`. A key holds letters,
// digits, underscores, dots and hyphens only, so quoting it is wrapping it in double quotes.
func ParsedKeyPipesText(b *strings.Builder, label string) {
	keys := ParsedKeyVariants(label)[1:]
	b.Grow(len(keys) * (2*len(label) + 80))
	for _, key := range keys {
		b.WriteString(" | format if (\"")
		b.WriteString(key)
		b.WriteString("\":*) \"<")
		b.WriteString(key)
		b.WriteString(">\" as ")
		b.WriteString(label)
		b.WriteString(" keep_original_fields")
	}
}

// ExtractedStatsText writes the pipes that give each label name_extracted of a stats pipe its Loki value, ahead of
// a stats pipe over a log query whose json or logfmt parsers extract every key (logfmt[i] says plain parser i is a
// logfmt one), each pipe after a blank: the keys of the bases (the part of each label before the suffix) unpacked
// again into scratch fields behind the parsers, the first parser's value picked into one field per base, the
// label set to it where the entry has the stream label of that name (and left as the unpacked key name_extracted
// where it has not), and the scratch fields deleted. labels and bases are in step. It reports whether it wrote a
// copy pipe.
func ExtractedStatsText(b *strings.Builder, logfmt []bool, bases, labels []string) (copies bool) {
	for i, lf := range logfmt {
		b.WriteString(" | unpack_")
		if lf {
			b.WriteString("logfmt")
		} else {
			b.WriteString("json")
		}
		b.WriteString(" from _msg fields (")
		for k, base := range bases {
			if k > 0 {
				b.WriteString(", ")
			}
			b.WriteString(LogsQLFieldName(base))
		}
		b.WriteString(") result_prefix \"")
		b.WriteString(ExtractedScratch)
		b.WriteString(strconv.Itoa(i))
		b.WriteString("_\"")
	}
	for _, base := range bases {
		if base != "service_name" {
			b.WriteString(" | copy _stream as ")
			b.WriteString(ExtractedScratch)
			b.WriteString("_stream")
			copies = true
			break
		}
	}
	for _, base := range bases {
		for j := len(logfmt) - 1; j >= 0; j-- {
			field := ExtractedScratchField(j, base)
			if j == len(logfmt)-1 {
				b.WriteString(" | copy ")
				b.WriteString(field)
				copies = true
			} else {
				b.WriteString(" | format if (")
				b.WriteString(field)
				b.WriteString(":*) \"<")
				b.WriteString(unquoted(field))
				b.WriteString(">\"")
			}
			b.WriteString(" as ")
			b.WriteString(ExtractedScratch)
			b.WriteByte('_')
			b.WriteString(base)
		}
	}
	for i, label := range labels {
		base := bases[i]
		b.WriteString(" | format if (")
		if base != "service_name" {
			b.WriteString(ExtractedScratch)
			b.WriteString("_stream:~")
			b.WriteString(strconv.Quote("[{,]" + dotOrUnderscore(base) + "=\""))
			b.WriteByte(' ')
		}
		b.WriteString(ExtractedScratch)
		b.WriteByte('_')
		b.WriteString(base)
		b.WriteString(":*) \"<")
		b.WriteString(ExtractedScratch)
		b.WriteByte('_')
		b.WriteString(base)
		b.WriteString(">\" as ")
		b.WriteString(label)
	}
	b.WriteString(" | delete ")
	b.WriteString(ExtractedScratch)
	b.WriteByte('*')
	return copies
}
