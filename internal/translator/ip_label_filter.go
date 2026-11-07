package translator

import (
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// lokiIPPattern is the address set of a Loki ip("...") pattern, read the way
// Loki reads it (pkg/logql/log/ip.go:240-261 getMatcher): a single address
// (netip.ParseAddr), then a CIDR (netip.ParsePrefix), then a range of two
// addresses of one family joined by a hyphen (go4.org/netipx ParseIPRange).
type lokiIPPattern struct {
	lo, hi netip.Addr
	// empty: no address Loki scans out of a value is in the pattern. Loki
	// compares addresses with their zone (netip.Addr.Compare) and a scanned
	// address has none (the scan stops at '%'), so a zoned single address
	// matches nothing and a zoned range only the addresses after its first.
	empty bool
}

func parseLokiIPPattern(pattern string) (lokiIPPattern, bool) {
	if a, err := netip.ParseAddr(pattern); err == nil {
		return lokiIPPattern{lo: a.WithZone(""), hi: a.WithZone(""), empty: a.Zone() != ""}, true
	}
	if p, err := netip.ParsePrefix(pattern); err == nil {
		p = p.Masked()
		return lokiIPPattern{lo: p.Addr(), hi: lastAddr(p)}, true
	}
	from, to, ok := strings.Cut(pattern, "-")
	if !ok {
		return lokiIPPattern{}, false
	}
	lo, err1 := netip.ParseAddr(from)
	hi, err2 := netip.ParseAddr(to)
	if err1 != nil || err2 != nil || lo.BitLen() != hi.BitLen() || lo.Zone() != hi.Zone() || hi.Less(lo) {
		return lokiIPPattern{}, false
	}
	out := lokiIPPattern{lo: lo.WithZone(""), hi: hi.WithZone("")}
	if lo.Zone() != "" {
		out.lo = out.lo.Next()
		out.empty = !out.lo.IsValid() || out.hi.Less(out.lo)
	}
	return out, true
}

// lastAddr is the last address of a masked prefix.
func lastAddr(p netip.Prefix) netip.Addr {
	b := p.Addr().AsSlice()
	for i := p.Bits(); i < len(b)*8; i++ {
		b[i/8] |= 1 << (7 - uint(i%8))
	}
	a, _ := netip.AddrFromSlice(b)
	return a
}

// textOf is a regexp every address of the pattern written out matches: the
// whole octets the bounds of an IPv4 pattern share followed by the remaining
// octets (the full address for one address), and two colons with address
// characters between them for IPv6.
func (p lokiIPPattern) textOf() string {
	if !p.lo.Is4() {
		return `:[0-9a-fA-F.]*:`
	}
	if p.lo == p.hi {
		return regexp.QuoteMeta(p.lo.String())
	}
	lo, hi := strings.Split(p.lo.String(), "."), strings.Split(p.hi.String(), ".")
	parts := make([]string, 0, 4)
	shared := true
	for i := 0; i < 4; i++ {
		if shared = shared && lo[i] == hi[i]; shared {
			parts = append(parts, lo[i])
			continue
		}
		parts = append(parts, `[0-9]{1,3}`)
	}
	return strings.Join(parts, `\.`)
}

func ipRangeFilter(field string, lo, hi netip.Addr) string {
	if lo.Is4() {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv4Range, Value: lo.String() + ", " + hi.String()}.String()
	}
	return logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv6Range, Value: strconv.Quote(lo.String()) + ", " + strconv.Quote(hi.String())}.String()
}

// ipLabelFilter translates Loki's `label = ip("pattern")` (negate: `!=`)
// label filter (pkg/logql/log/ip.go:123-145 IPLabelFilter.filterTy). Loki
// drops a line whose label is absent under both operators; otherwise `=`
// keeps it when an address it scans out of the value (ip.go:183-226) is in
// the pattern and `!=` when none is. The scan starts an address at a digit
// followed by a dot within three characters, or at the start of an IPv6
// group, reads the longest run of address characters and parses it with
// netip.ParseAddr: no leading zero in an IPv4 octet, an address of the other
// family never matches, and an IPv6 value shorter than five characters is
// never read.
//
// VictoriaLogs' ipv4_range and ipv6_range match a value that is one whole
// address (lib/logstorage filter_ipv4_range.go matchIPv4Range,
// filter_ipv6_range.go matchIPv6Range). Its IPv4 parser accepts leading
// zeros and reads a two-character octet without checking the second
// character (values_encoder.go tryParseIPv4, tryParseDateUint64), and its
// IPv6 parser reads IPv4 text too; the guards below keep only the values
// Loki reads as that same whole address, so a whole-address value answers
// exactly as Loki. A value with an address inside other text ("1.2.3.4:443")
// cannot be scanned in LogsQL: `=` leaves it out, and `!=` leaves out every
// such value holding text of the pattern's form (textOf). Neither keeps a line
// Loki drops (semantics/ip-label-filter-embedded-address). ok is false for a
// pattern Loki rejects.
func ipLabelFilter(field, pattern string, negate bool) (string, bool) {
	p, ok := parseLokiIPPattern(pattern)
	if !ok {
		return "", false
	}
	re := func(expr string) string {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: expr}.String()
	}
	ipv4Text := re(`^[0-9.]+$`)
	anyIPv4 := "(" + logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv4Range, Value: "0.0.0.0, 255.255.255.255"}.String() + " " + ipv4Text + ")"
	// A zone ("::%0::00") can hold an address Loki scans, so a zoned value is
	// no whole address here.
	anyIPv6 := "(" + logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv6Range, Value: `"::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"`}.String() + " -" + re(`%`) + ")"
	present := logsql.FieldFilter{Field: field, Op: logsql.FieldOpAny}.String()
	if p.empty {
		if negate {
			return present, true
		}
		return "(" + present + " -" + present + ")", true // matches nothing
	}
	ranged := ipRangeFilter(field, p.lo, p.hi)
	// Loki reads an IPv6 value whole only where its scan starts one at the
	// first character: an IPv6 group or "::" and a hex digit there, and at
	// least five characters in the value (ip.go:216-223 ipv6Hint).
	inPattern := "(" + ranged + " " + re(`^.{5,}`) + " " + re(`^(::[0-9a-fA-F]|[0-9a-fA-F]{1,4}:)`) + " -" + ipv4Text + ")"
	if p.lo.Is4() {
		inPattern = "(" + ranged + " " + ipv4Text + " -" + re(`(^|\.)0[0-9]`) + ")"
	}
	if !negate {
		return inPattern, true
	}
	holdsText := re(p.textOf())
	return "(" + present + " -" + inPattern + " -(" + holdsText + " -" + anyIPv4 + " -" + anyIPv6 + "))", true
}

// ipCondition is the Go evaluation of ipLabelFilter on one field value, for
// the rows a metric plan reads itself: it answers what VictoriaLogs answers
// for that filter, so the rows, the stats pushdown and the log query agree. The
// forms the filter spells as regexps are written out as plain checks.
type ipCondition struct {
	raw     string // the pattern as written in the query
	pattern lokiIPPattern
	// The text form (lokiIPPattern.textOf) of an IPv4 pattern: textLead, then
	// textRest groups of one to three digits joined by dots. An IPv6 pattern's
	// form is two colons with address characters between them.
	textLead string
	textRest int
}

// newIPCondition reads a Loki ip("...") pattern; ok is false for one Loki rejects.
func newIPCondition(pattern string) (*ipCondition, bool) {
	p, ok := parseLokiIPPattern(pattern)
	if !ok {
		return nil, false
	}
	c := &ipCondition{raw: pattern, pattern: p}
	if p.empty || !p.lo.Is4() {
		return c, true
	}
	lo, hi := p.lo.As4(), p.hi.As4()
	shared := 0
	for shared < 4 && lo[shared] == hi[shared] {
		shared++
	}
	for i := 0; i < shared; i++ {
		if i > 0 {
			c.textLead += "."
		}
		c.textLead += strconv.Itoa(int(lo[i]))
	}
	if c.textRest = 4 - shared; c.textRest > 0 && shared > 0 {
		c.textLead += "."
	}
	return c, true
}

// holdsText is `value` matching textOf unanchored.
func (c *ipCondition) holdsText(value string) bool {
	if !c.pattern.lo.Is4() {
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
	for start := 0; start+len(c.textLead) <= len(value); start++ {
		if value[start:start+len(c.textLead)] == c.textLead && digitGroups(value[start+len(c.textLead):], c.textRest) {
			return true
		}
	}
	return false
}

// digitGroups reports whether s starts with n groups of one to three digits
// joined by dots. A group followed by a dot is the whole digit run before it;
// the last one may stop inside a longer run.
func digitGroups(s string, n int) bool {
	for ; n > 0; n-- {
		run := 0
		for run < len(s) && s[run] >= '0' && s[run] <= '9' {
			run++
		}
		if n == 1 {
			return run >= 1
		}
		if run < 1 || run > 3 || run == len(s) || s[run] != '.' {
			return false
		}
		s = s[run+1:]
	}
	return true
}

// matches mirrors ipLabelFilter(field, pattern, negate) on a field whose value
// is value; an absent field is the empty value, which no form keeps.
func (c *ipCondition) matches(value string, negate bool) bool {
	present := value != ""
	if c.pattern.empty {
		return negate && present
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
	if p.lo.Is4() {
		n, ok := parseVLIPv4(value)
		return ok && onlyIPv4Chars(value) && !hasLeadingZeroOctet(value) && n >= ipv4Number(p.lo) && n <= ipv4Number(p.hi)
	}
	a, ok := parseVLIPv6(value)
	lo, hi := p.lo.As16(), p.hi.As16()
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

// hasLeadingZeroOctet is `(^|\.)0[0-9]`.
func hasLeadingZeroOctet(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '0' && (i == 0 || s[i-1] == '.') && s[i+1] >= '0' && s[i+1] <= '9' {
			return true
		}
	}
	return false
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

func ipv4Number(a netip.Addr) uint32 {
	b := a.As4()
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
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
