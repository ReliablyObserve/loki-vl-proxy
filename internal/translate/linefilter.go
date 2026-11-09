package translate

import (
	"net"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
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
func OrLineFilter(negate, regex bool, alts []OrAlternative) string {
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
	filter := "~" + strconv.Quote(strings.Join(frags, "|"))
	if negate {
		return "NOT " + filter
	}
	return filter
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
