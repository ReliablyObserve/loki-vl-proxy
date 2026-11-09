package translate

import (
	"net"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// OrAlternative is one `or` alternative of a line filter: its unquoted text and
// whether it was written ip("...").
// OrAlternative is one alternative of a line filter's `or` chain: its text, and whether it is an ip() pattern.
type OrAlternative struct {
	Text string
	IP   bool
}

// OrLineFilter renders a line filter with `or` alternatives as one regexp, which
// VictoriaLogs evaluates in a single pass over the line. Loki matches a line
// that holds any alternative for |=, |~ and |> and none of them for !=, !~ and
// !> (a negated chain is a conjunction), so negate flips the one regexp.
// A positive chain follows Loki's quirks: every alternative is plain text, even
// ip(...) (LineFilterExpr.newOrFilter ignores its Op), an ip(...) alternative
// ends its orFilter so that the next `or` drops everything attached before
// (newOrLineFilterExpr: left.Or = right), and an alternative that matches every
// line is dropped (log.newOrFilter: true or x is x). A negated != keeps ip
// semantics. alts[0] is the filter's own operand.
func OrLineFilterPattern(negate, regex bool, alts []OrAlternative) string {
	if !negate {
		alts = positiveOrChain(alts)
	}
	var frags []string
	wrap := regex
	for _, alt := range alts {
		switch {
		case !negate && LokiMatchesEverything(alt.Text, regex):
			continue
		case regex:
			frags = append(frags, alt.Text)
		case alt.IP && negate:
			frags, wrap = append(frags, IPLineFilterRegex(alt.Text)), true
		default:
			frags = append(frags, regexp.QuoteMeta(alt.Text))
		}
	}
	if wrap && len(frags) > 1 {
		for i, frag := range frags {
			frags[i] = "(?:" + frag + ")"
		}
	}
	return strings.Join(frags, "|")
}

// positiveOrChain keeps the head and the alternatives Loki keeps: those of the
// last orFilter, which an ip(...) alternative ends.
func positiveOrChain(alts []OrAlternative) []OrAlternative {
	rest := alts[1:]
	start := 0
	for i, alt := range rest {
		if alt.IP && i+1 < len(rest) {
			start = i + 1
		}
	}
	return append([]OrAlternative{alts[0]}, rest[start:]...)
}

// LokiMatchesEverything reports whether Loki turns the filter into its
// match-all filter: an empty string, or a regexp that simplifies to `.*` or the
// empty match (RegexSimplifier.Simplify).
func LokiMatchesEverything(text string, regex bool) bool {
	if !regex {
		return text == ""
	}
	re, err := syntax.Parse(text, syntax.Perl)
	if err != nil {
		return false
	}
	re = re.Simplify()
	for re.Op == syntax.OpCapture {
		re = re.Sub[0]
	}
	return re.Op == syntax.OpEmptyMatch || re.Op == syntax.OpStar && re.Sub[0].Op == syntax.OpAnyCharNotNL
}

// IPLineFilterRegex converts an ip() filter argument to a VL-compatible regex.
// This is a best-effort approximation: VL has no native IP semantics.
// IPv4: precise prefix-based regex. IPv6: normalized address literal match.
func IPLineFilterRegex(arg string) string {
	if boxes, ok := ipv4Boxes(arg); ok {
		return ipv4LineRegex(boxes)
	}
	if slashIdx := strings.IndexByte(arg, '/'); slashIdx >= 0 {
		ipStr := arg[:slashIdx]
		parsedIP := net.ParseIP(ipStr)
		// IPv6 CIDR: match the network prefix as a hex string approximation
		if parsedIP != nil && parsedIP.To4() == nil {
			return buildIPv6CIDRRegex(parsedIP, arg[slashIdx+1:])
		}
		// IPv4 CIDR
		bits, err := strconv.Atoi(arg[slashIdx+1:])
		if err != nil {
			return regexp.QuoteMeta(ipStr)
		}
		return buildCIDRRegex(ipStr, bits)
	}
	if dashIdx := strings.IndexByte(arg, '-'); dashIdx >= 0 && strings.Count(arg[:dashIdx], ".") == 3 {
		return buildIPRangeRegex(arg[:dashIdx], arg[dashIdx+1:])
	}
	// Plain IP — normalize and escape
	if parsedIP := net.ParseIP(arg); parsedIP != nil {
		return regexp.QuoteMeta(parsedIP.String())
	}
	return regexp.QuoteMeta(arg)
}

// buildIPv6CIDRRegex generates a regex approximation for an IPv6 CIDR.
// Uses the normalized IPv6 prefix up to the full groups, then wildcards the rest.
func buildIPv6CIDRRegex(ip net.IP, prefixBitsStr string) string {
	prefixBits, err := strconv.Atoi(prefixBitsStr)
	if err != nil {
		return regexp.QuoteMeta(ip.String())
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return regexp.QuoteMeta(ip.String())
	}
	// Number of full bytes fixed by the prefix
	fullBytes := prefixBits / 8
	if fullBytes > 16 {
		fullBytes = 16
	}
	// Use the normalized string prefix of full groups (2 bytes each)
	fullGroups := fullBytes / 2
	normalized := ip.String()
	// For simplicity, match any hex:colon pattern that starts with the fixed prefix text
	if fullGroups == 0 {
		return `[0-9a-fA-F:]+`
	}
	// Split the normalized IPv6 into groups and use the first fullGroups as literal prefix
	groups := strings.Split(normalized, ":")
	if len(groups) < fullGroups {
		return regexp.QuoteMeta(normalized)
	}
	prefix := strings.Join(groups[:fullGroups], ":")
	return regexp.QuoteMeta(prefix) + `[0-9a-fA-F:]*`
}

func buildCIDRRegex(ip string, bits int) string {
	octets := strings.Split(ip, ".")
	if len(octets) != 4 {
		return regexp.QuoteMeta(ip)
	}
	fullOctets := bits / 8
	var sb strings.Builder
	for i := 0; i < 4; i++ {
		if i > 0 {
			sb.WriteString(`\.`)
		}
		if i < fullOctets {
			sb.WriteString(regexp.QuoteMeta(octets[i]))
		} else {
			sb.WriteString(`\d{1,3}`)
		}
	}
	return sb.String()
}

func buildIPRangeRegex(startIP, endIP string) string {
	startOctets := strings.Split(startIP, ".")
	endOctets := strings.Split(endIP, ".")
	if len(startOctets) != 4 || len(endOctets) != 4 {
		return regexp.QuoteMeta(startIP)
	}
	commonOctets := 0
	for i := 0; i < 4; i++ {
		if startOctets[i] == endOctets[i] {
			commonOctets = i + 1
		} else {
			break
		}
	}
	var sb strings.Builder
	for i := 0; i < 4; i++ {
		if i > 0 {
			sb.WriteString(`\.`)
		}
		if i < commonOctets {
			sb.WriteString(regexp.QuoteMeta(startOctets[i]))
		} else {
			sb.WriteString(`\d{1,3}`)
		}
	}
	return sb.String()
}

// OrLineFilter is the LogsQL text of a line filter with `or` alternatives: the regexp OrLineFilterPattern builds,
// negated for != and !~.
func OrLineFilter(negate, regex bool, alts []OrAlternative) string {
	filter := "~" + strconv.Quote(OrLineFilterPattern(negate, regex, alts))
	if negate {
		return "NOT " + filter
	}
	return filter
}

// LineFilter is the VictoriaLogs filter of a line filter stage, built as nodes: the filter the string translator
// writes for it, nil when it keeps every line (a != of a text no line holds). Loki's |= is a substring match (a
// quoted literal regexp), |~ a regexp, ip() a regexp of the addresses it accepts, and an `or` chain one regexp of its alternatives (OrLineFilterPattern). ok is false for the
// pattern filters (|> and !>), which the translator writes with extract pipes.
func LineFilter(st *logql.LineFilterStage) (f logsql.FilterExpr, ok bool) {
	var negate, regex bool
	switch st.Op {
	case logql.LineFilterContains:
	case logql.LineFilterExcludes:
		negate = true
	case logql.LineFilterMatchRe:
		regex = true
	case logql.LineFilterExcludeRe:
		negate, regex = true, true
	default:
		return nil, false
	}
	alts := make([]OrAlternative, len(st.Or))
	for i, a := range st.Or {
		alts[i] = OrAlternative{Text: a.Value, IP: a.IP}
	}
	if !regex && (!textHeld(OrAlternative{Text: st.Value, IP: st.IP}) || !TextsHeld(alts)) {
		return TextLineFilter(negate, append([]OrAlternative{{Text: st.Value, IP: st.IP}}, alts...)), true
	}
	var pattern string
	switch {
	case st.IP && len(alts) == 0:
		return IPLineFilter(st.Value, negate), true
	case st.IP && negate:
		// != ip(...) reads the head as an address in a chain too.
		pattern = OrLineFilterPattern(true, false, append([]OrAlternative{{Text: st.Value, IP: true}}, alts...))
	case len(alts) > 0:
		// Loki builds the chain from the text of each alternative: a positive ip() head matches as text.
		pattern = OrLineFilterPattern(negate, regex, append([]OrAlternative{{Text: st.Value}}, alts...))
	case regex:
		pattern = st.Value
	default:
		pattern = regexp.QuoteMeta(st.Value)
	}
	f = logsql.Regexp{Pattern: pattern}
	if negate {
		f = logsql.NotExpr{Expr: f}
	}
	return f, true
}

// IPLineFilter is the VictoriaLogs filter of a line filter `|= ip(pattern)` (negate: `!= ip(pattern)`): the regexp
// IPLineFilterRegex builds, which matches as Loki's ip() filter reads a line, after a substring every address of an
// IPv4 pattern holds (its fixed octets), which VictoriaLogs checks first and looks up in its bloom filters, so the
// regexp runs on the lines that may hold an address only.
func IPLineFilter(pattern string, negate bool) logsql.FilterExpr {
	var f logsql.FilterExpr = logsql.Regexp{Pattern: IPLineFilterRegex(pattern)}
	if boxes, ok := ipv4Boxes(pattern); ok {
		if lit := ipv4Literal(boxes); lit != "" {
			f = logsql.AllOf{logsql.Regexp{Pattern: regexp.QuoteMeta(lit)}, f}
		}
	}
	if negate {
		return logsql.NotExpr{Expr: f}
	}
	return f
}

// matchNoLine is a LogsQL filter no row passes: an empty in() list.
var matchNoLine = logsql.FieldFilter{Field: "_stream_id", Op: logsql.FieldOpIn}

// textHeld reports whether a stored line can hold an alternative of a substring line filter (|= or !=, alone or in
// an `or` chain). A text that is not UTF-8 is in no line: every ingestion API carries UTF-8 strings (Loki's push API,
// OTLP, VictoriaLogs' JSON lines), so Loki's byte match of it never matches, and VictoriaLogs could not compile its
// regexp. An ip() alternative is held.
func textHeld(a OrAlternative) bool {
	return a.IP || utf8.ValidString(a.Text)
}

// TextsHeld reports whether a stored line can hold every alternative (textHeld).
func TextsHeld(alts []OrAlternative) bool {
	for _, a := range alts {
		if !textHeld(a) {
			return false
		}
	}
	return true
}

// TextLineFilter is the filter of a substring line filter (|= when negate is false, != when true) with alternatives
// a line cannot hold (TextsHeld is false): those never match, so the filter is the regexp of the others, and with
// none left `|=` keeps no line and `!=` every line (nil: no filter).
func TextLineFilter(negate bool, alts []OrAlternative) logsql.FilterExpr {
	kept := alts[:0:0]
	for _, a := range alts {
		if textHeld(a) {
			kept = append(kept, a)
		}
	}
	switch {
	case len(kept) > 0 && negate:
		return logsql.NotExpr{Expr: logsql.Regexp{Pattern: OrLineFilterPattern(true, false, kept)}}
	case len(kept) > 0:
		return logsql.Regexp{Pattern: OrLineFilterPattern(false, false, kept)}
	case negate:
		return nil
	}
	return matchNoLine
}
