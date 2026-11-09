package translator

import (
	"net/netip"
	"regexp"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// ipLabelFilter is translate.IPLabelFilter as LogsQL text.
func ipLabelFilter(field, pattern string, negate bool) (string, bool) {
	f, ok := translate.IPLabelFilter(field, pattern, negate)
	if !ok {
		return "", false
	}
	return f.String(), true
}

// ipCondition is the Go evaluation of ipLabelFilter on one field value, for
// the rows a metric plan reads itself: it answers what VictoriaLogs answers
// for that filter, so the rows, the stats pushdown and the log query agree. The
// forms the filter spells as regexps are written out as plain checks, and an
// IPv4 pattern is the regexp the filter is.
type ipCondition struct {
	raw     string // the pattern as written in the query
	pattern translate.IPPattern
	// re is the regexp of an IPv4 pattern (translate.IPLineFilterRegex) and nil
	// for an IPv6 one.
	re *regexp.Regexp
}

// newIPCondition reads a Loki ip("...") pattern; ok is false for one Loki rejects.
func newIPCondition(pattern string) (*ipCondition, bool) {
	p, ok := translate.ParseLokiIPPattern(pattern)
	if !ok {
		return nil, false
	}
	c := &ipCondition{raw: pattern, pattern: p}
	if !p.Empty && p.Lo.Is4() {
		re, err := regexp.Compile(translate.IPLineFilterRegex(pattern))
		if err != nil {
			return nil, false
		}
		c.re = re
	}
	return c, true
}

// holdsText is `value` matching textOf unanchored, for an IPv6 pattern.
func (c *ipCondition) holdsText(value string) bool {
	last, clean := -1, true
	for i := 0; i < len(value); i++ {
		switch {
		case value[i] == ':':
			if last >= 0 && clean {
				return true
			}
			last, clean = i, true
		case last >= 0 && value[i] != '.' && !isHexDigit(value[i]):
			clean = false
		}
	}
	return false
}

// matches mirrors ipLabelFilter(field, pattern, negate) on a field whose value
// is value; an absent field is the empty value, which no form keeps.
func (c *ipCondition) matches(value string, negate bool) bool {
	present := value != ""
	if c.pattern.Empty {
		return negate && present
	}
	if c.re != nil {
		in := c.re.MatchString(value)
		if negate {
			return present && !in
		}
		return in
	}
	in := c.inPattern(value)
	if !negate {
		return in
	}
	// A value holding text of the pattern's form that is not one whole address
	// is dropped, as the filter does.
	_, whole4 := parseVLIPv4(value)
	_, whole6 := parseVLIPv6(value)
	wholeAddr := whole4 && onlyIPv4Chars(value) || whole6 && indexByte(value, '%') < 0
	return present && !in && (wholeAddr || !c.holdsText(value))
}

func (c *ipCondition) inPattern(value string) bool {
	p := c.pattern
	a, ok := parseVLIPv6(value)
	lo, hi := p.Lo.As16(), p.Hi.As16()
	return ok && string(a[:]) >= string(lo[:]) && string(a[:]) <= string(hi[:]) && startsIPv6Group(value) && !onlyIPv4Chars(value)
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// onlyIPv4Chars is `^[0-9.]+$`.
func onlyIPv4Chars(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '.' && (s[i] < '0' || s[i] > '9') {
			return false
		}
	}
	return s != ""
}

// startsIPv6Group is `^(::[0-9a-fA-F]|[0-9a-fA-F]{1,4}:)` together with the
// five characters `^.{5,}` asks for; the whole-address parse leaves no newline
// for a dot to meet.
func startsIPv6Group(s string) bool {
	if utf8.RuneCountInString(s) < 5 {
		return false
	}
	if s[0] == ':' {
		return s[1] == ':' && isHexDigit(s[2])
	}
	n := 0
	for n < len(s) && isHexDigit(s[n]) {
		n++
	}
	return n >= 1 && n <= 4 && n < len(s) && s[n] == ':'
}

// parseVLIPv4 reads an address the way VictoriaLogs' ipv4_range does
// (lib/logstorage values_encoder.go tryParseIPv4): leading zeros are accepted
// ("1.2.3.0004" is 1.2.3.4), and an octet of two characters is read without
// checking its second one ("1.2.3.4a" is 1.2.3.89). The first three octets
// have at most three characters, the last up to nine.
func parseVLIPv4(s string) (uint32, bool) {
	if len(s) < 7 || len(s) > 15 {
		return 0, false
	}
	var out uint32
	for octet, start := 0, 0; octet < 4; octet++ {
		end := len(s)
		if octet < 3 {
			if end = start + indexByte(s[start:], '.'); end < start {
				return 0, false
			}
		}
		part := s[start:end]
		if part == "" || len(part) > 9 || octet < 3 && len(part) > 3 || indexByte(part, '.') >= 0 {
			return 0, false
		}
		var n uint32
		if len(part) == 2 && part[0] >= '0' && part[0] <= '9' {
			n = 10*uint32(part[0]-'0') + uint32(part[1]-'0')
		} else {
			for i := 0; i < len(part); i++ {
				if part[i] < '0' || part[i] > '9' {
					return 0, false
				}
				if n = n*10 + uint32(part[i]-'0'); n > 255 {
					return 0, false
				}
			}
		}
		if n > 255 {
			return 0, false
		}
		out = out<<8 | n
		start = end + 1
	}
	return out, true
}

// parseVLIPv6 reads an address the way VictoriaLogs' ipv6_range does: any text
// netip.ParseAddr reads, of two to 45 characters (IPv4 text included).
func parseVLIPv6(s string) ([16]byte, bool) {
	if len(s) < 2 || len(s) > 45 {
		return [16]byte{}, false
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return [16]byte{}, false
	}
	return a.As16(), true
}
