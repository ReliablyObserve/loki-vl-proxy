package translator

import (
	"net/netip"
	"regexp"
	"strconv"
	"strings"

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
