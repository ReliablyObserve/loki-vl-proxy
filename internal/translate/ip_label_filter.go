package translate

import (
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// IPPattern is the address set of a Loki ip("...") pattern, read the way
// Loki reads it (pkg/logql/log/ip.go:240-261 getMatcher): a single address
// (netip.ParseAddr), then a CIDR (netip.ParsePrefix), then a range of two
// addresses of one family joined by a hyphen (go4.org/netipx ParseIPRange).
type IPPattern struct {
	Lo, Hi netip.Addr
	// Empty: no address Loki scans out of a value is in the pattern. Loki
	// compares addresses with their zone (netip.Addr.Compare) and a scanned
	// address has none (the scan stops at '%'), so a zoned single address
	// matches nothing and a zoned range only the addresses after its first.
	Empty bool
}

func ParseLokiIPPattern(pattern string) (IPPattern, bool) {
	if a, err := netip.ParseAddr(pattern); err == nil {
		return IPPattern{Lo: a.WithZone(""), Hi: a.WithZone(""), Empty: a.Zone() != ""}, true
	}
	if p, err := netip.ParsePrefix(pattern); err == nil {
		p = p.Masked()
		return IPPattern{Lo: p.Addr(), Hi: lastAddr(p)}, true
	}
	from, to, ok := strings.Cut(pattern, "-")
	if !ok {
		return IPPattern{}, false
	}
	lo, err1 := netip.ParseAddr(from)
	hi, err2 := netip.ParseAddr(to)
	if err1 != nil || err2 != nil || lo.BitLen() != hi.BitLen() || lo.Zone() != hi.Zone() || hi.Less(lo) {
		return IPPattern{}, false
	}
	out := IPPattern{Lo: lo.WithZone(""), Hi: hi.WithZone("")}
	if lo.Zone() != "" {
		out.Lo = out.Lo.Next()
		out.Empty = !out.Lo.IsValid() || out.Hi.Less(out.Lo)
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

// TextOf is a regexp every address of the pattern written out matches: the
// whole octets the bounds of an IPv4 pattern share followed by the remaining
// octets (the full address for one address), and two colons with address
// characters between them for IPv6.
func (p IPPattern) TextOf() string {
	if !p.Lo.Is4() {
		return `:[0-9a-fA-F.]*:`
	}
	if p.Lo == p.Hi {
		return regexp.QuoteMeta(p.Lo.String())
	}
	lo, hi := strings.Split(p.Lo.String(), "."), strings.Split(p.Hi.String(), ".")
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

func ipRangeFilter(field string, lo, hi netip.Addr) logsql.FilterExpr {
	if lo.Is4() {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv4Range, Value: lo.String() + ", " + hi.String()}
	}
	return logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv6Range, Value: strconv.Quote(lo.String()) + ", " + strconv.Quote(hi.String())}
}

// ipv4ValuePattern is the exact regexp of an IPv4 pattern read on a label value (IPLineFilterRegex) and the
// regexp of the substring every address of the pattern holds (ipv4Prefilter).
type ipv4ValuePattern struct{ regex, prefilter string }

// ipv4PrefilterLimit bounds the alternatives of a prefilter: a pattern whose boxes fix more alternatives than this
// falls back to the dot every address holds.
const ipv4PrefilterLimit = 8

// ipv4Prefilter is the regexp of a substring every address of the pattern holds, which VictoriaLogs checks before the
// exact regexp and looks up in its bloom filters: the octets all the boxes fix (ipv4Literal); else the alternatives
// each box fixes, or its first octets one by one when it spans a few ("10.|11." for 10.0.0.0/7); else, for a pattern
// that spans too many (0.0.0.0/0), the dot of an address, which still skips the values without one.
func ipv4Prefilter(boxes []ipv4Box) string {
	if lit := ipv4Literal(boxes); lit != "" {
		return regexp.QuoteMeta(lit)
	}
	var alts []string
	for _, b := range boxes {
		if b[0][0] == b[0][1] {
			alts = append(alts, ipv4Literal([]ipv4Box{b}))
			continue
		}
		if b[0][1]-b[0][0]+1 > ipv4PrefilterLimit {
			return `\.`
		}
		for v := b[0][0]; v <= b[0][1]; v++ {
			alts = append(alts, strconv.Itoa(v)+".")
		}
	}
	// An alternative that holds another one adds nothing: a value with it holds the other too.
	kept := alts[:0:0]
	for i, a := range alts {
		redundant := false
		for j, o := range alts {
			if i != j && strings.Contains(a, o) && (len(a) > len(o) || j < i) {
				redundant = true
				break
			}
		}
		if !redundant {
			kept = append(kept, regexp.QuoteMeta(a))
		}
	}
	if len(kept) > ipv4PrefilterLimit {
		return `\.`
	}
	return strings.Join(kept, "|")
}

// ipv4ValuePatterns memoizes ipv4ValueFilter's patterns: building the octet boxes into a regexp costs more than
// the rest of the translation, and the patterns a deployment filters on are few. It holds at most
// ipv4ValuePatternLimit patterns and starts over when full.
var (
	ipv4ValuePatternsMu   sync.Mutex
	ipv4ValuePatterns     = map[string]ipv4ValuePattern{}
	ipv4ValuePatternLimit = 512
)

func ipv4ValuePatternOf(pattern string) ipv4ValuePattern {
	ipv4ValuePatternsMu.Lock()
	defer ipv4ValuePatternsMu.Unlock()
	if p, ok := ipv4ValuePatterns[pattern]; ok {
		return p
	}
	if len(ipv4ValuePatterns) >= ipv4ValuePatternLimit {
		clear(ipv4ValuePatterns)
	}
	p := ipv4ValuePattern{regex: IPLineFilterRegex(pattern), prefilter: `\.`}
	if boxes, ok := ipv4Boxes(pattern); ok {
		p.prefilter = ipv4Prefilter(boxes)
	}
	ipv4ValuePatterns[pattern] = p
	return p
}

// ipv4ValueFilter is the filter of the values an IPv4 pattern keeps: the exact regexp, after the substring every
// address of the pattern holds (ipv4Prefilter), which VictoriaLogs checks first and looks up in its bloom filters,
// so the regexp runs on the values that may hold an address only (as IPLineFilter does on a line).
func ipv4ValueFilter(field, pattern string) logsql.FilterExpr {
	p := ipv4ValuePatternOf(pattern)
	f := logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: p.regex}
	return logsql.AllOf{logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: p.prefilter}, f}
}

// ipv4LabelFilterNodes is the filter of an IPv4 pattern read on a label.
func ipv4LabelFilterNodes(field, pattern string, negate bool, p IPPattern) logsql.FilterExpr {
	// VictoriaLogs stores a block whose values are all IPv4 addresses as 32-bit numbers, where ipv4_range is a
	// compare of numbers and the value is the canonical address; any other block holds text, which only the
	// regexp reads as Loki's scan does.
	typed := logsql.FieldFilter{Field: field, Op: logsql.FieldOpValueType, Value: "ipv4"}
	var in logsql.FilterExpr = logsql.Group{Expr: logsql.AnyOf{
		logsql.Group{Expr: logsql.AllOf{typed, ipRangeFilter(field, p.Lo, p.Hi)}},
		logsql.Group{Expr: append(logsql.AllOf{logsql.Exclude{Expr: typed}}, ipv4ValueFilter(field, pattern).(logsql.AllOf)...)},
	}}
	if negate {
		present := logsql.FieldFilter{Field: field, Op: logsql.FieldOpAny}
		in = logsql.Group{Expr: logsql.AllOf{present, logsql.Exclude{Expr: in}}}
	}
	return in
}

// ipLabelFilters memoizes the text of an ip() label filter by field, pattern and operator: the text is long (the
// exact regexp, the IPv6 guards), and rendering it costs more than the rest of the translation. It holds at most
// ipv4ValuePatternLimit filters and starts over when full.
var (
	ipLabelFiltersMu sync.Mutex
	ipLabelFilters   = map[string]logsql.FilterExpr{}
)

// IPLabelFilter translates Loki's `label = ip("pattern")` (negate: `!=`)
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
// An IPv4 pattern is the line filter's regexp (IPLineFilterRegex) run on the
// value: it reads a value as the scan does, an address alone or inside other
// text ("1.2.3.4:443", "client 1.2.3.4"), and agrees with Loki on every value
// of the recorded grid (semantics/ip-label-filter-embedded-address).
//
// VictoriaLogs' ipv4_range and ipv6_range match a value that is one whole
// address (lib/logstorage filter_ipv4_range.go matchIPv4Range,
// filter_ipv6_range.go matchIPv6Range). Its IPv4 parser accepts leading
// zeros and reads a two-character octet without checking the second
// character (values_encoder.go tryParseIPv4, tryParseDateUint64), and its
// IPv6 parser reads IPv4 text too; the guards below keep only the values
// Loki reads as that same whole address, so an IPv6 pattern answers a
// whole-address value exactly as Loki. A value with an IPv6 address inside
// other text cannot be scanned in LogsQL: `=` leaves it out, and `!=` leaves
// out every such value holding text of the pattern's form (TextOf). Neither
// keeps a line Loki drops. ok is false for a pattern Loki rejects.
func IPLabelFilter(field, pattern string, negate bool) (logsql.FilterExpr, bool) {
	key := field + "\x00" + pattern
	if negate {
		key += "\x00!"
	}
	ipLabelFiltersMu.Lock()
	f, ok := ipLabelFilters[key]
	ipLabelFiltersMu.Unlock()
	if ok {
		return f, true
	}
	built, ok := buildIPLabelFilter(field, pattern, negate)
	if !ok {
		return nil, false
	}
	f = logsql.DeferredExpr{Raw: built.String()}
	ipLabelFiltersMu.Lock()
	if len(ipLabelFilters) >= ipv4ValuePatternLimit {
		clear(ipLabelFilters)
	}
	ipLabelFilters[key] = f
	ipLabelFiltersMu.Unlock()
	return f, true
}

// buildIPLabelFilter is IPLabelFilter as a tree of nodes, not memoized.
func buildIPLabelFilter(field, pattern string, negate bool) (logsql.FilterExpr, bool) {
	p, ok := ParseLokiIPPattern(pattern)
	if !ok {
		return nil, false
	}
	re := func(expr string) logsql.FilterExpr {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: expr}
	}
	present := logsql.FieldFilter{Field: field, Op: logsql.FieldOpAny}
	if !p.Empty && p.Lo.Is4() {
		return ipv4LabelFilterNodes(field, pattern, negate, p), true
	}
	ipv4Text := re(`^[0-9.]+$`)
	anyIPv4 := logsql.Group{Expr: logsql.AllOf{logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv4Range, Value: "0.0.0.0, 255.255.255.255"}, ipv4Text}}
	// A zone ("::%0::00") can hold an address Loki scans, so a zoned value is
	// no whole address here.
	anyIPv6 := logsql.Group{Expr: logsql.AllOf{logsql.FieldFilter{Field: field, Op: logsql.FieldOpIPv6Range, Value: `"::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"`}, logsql.Exclude{Expr: re(`%`)}}}
	if p.Empty {
		if negate {
			return present, true
		}
		return logsql.Group{Expr: logsql.AllOf{present, logsql.Exclude{Expr: present}}}, true // matches nothing
	}
	ranged := ipRangeFilter(field, p.Lo, p.Hi)
	// Loki reads an IPv6 value whole only where its scan starts one at the
	// first character: an IPv6 group or "::" and a hex digit there, and at
	// least five characters in the value (ip.go:216-223 ipv6Hint).
	inPattern := logsql.Group{Expr: logsql.AllOf{ranged, re(`^.{5,}`), re(`^(::[0-9a-fA-F]|[0-9a-fA-F]{1,4}:)`), logsql.Exclude{Expr: ipv4Text}}}
	if p.Lo.Is4() {
		inPattern = logsql.Group{Expr: logsql.AllOf{ranged, ipv4Text, logsql.Exclude{Expr: re(`(^|\.)0[0-9]`)}}}
	}
	if !negate {
		return inPattern, true
	}
	holdsText := re(p.TextOf())
	return logsql.Group{Expr: logsql.AllOf{present, logsql.Exclude{Expr: inPattern}, logsql.Exclude{Expr: logsql.Group{Expr: logsql.AllOf{holdsText, logsql.Exclude{Expr: anyIPv4}, logsql.Exclude{Expr: anyIPv6}}}}}}, true
}
