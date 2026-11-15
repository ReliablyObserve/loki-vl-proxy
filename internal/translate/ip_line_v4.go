package translate

import (
	"net/netip"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The IPv4 ip() line filter as Loki's filter reads a line (recorded with the Loki oracle, test/oracle/loki
// ipfilter and process): the filter walks the line from its start. At a digit with a dot one, two or three
// characters on, it reads the run of digits and dots there as a candidate address; else, at the start of an IPv6
// group (one to four hex digits and a colon, or two colons and a hex digit), it reads the run of hex digits, colons
// and dots there; else it moves one character on. After a run it skips the run and the character that ends it. A
// candidate is an address of four octets of at most 255 without leading zeros, so a digit or a dot right after it
// makes it invalid. So where a candidate can start depends on the line before it: in `X0:1.2.3.4` the run `0` is
// read first and 1.2.3.4 is found, in `a0:1.2.3.4` the IPv6 run `a0:1.2.3.4` takes it, in `1:a1.2.3.4` the IPv6 run
// `1:a1.2.3.4` does, and in `1.0:1.2.3.4` the run `1.0` and its colon are skipped and 1.2.3.4 is found.

// ipv4Box is a set of addresses: a range of values per octet.
type ipv4Box [4][2]int

// ipv4Boxes is the address set of an IPv4 ip() pattern (an address, a CIDR or a range) as boxes; ok is false for a
// pattern that is not IPv4.
func ipv4Boxes(pattern string) ([]ipv4Box, bool) {
	if prefix, err := netip.ParsePrefix(pattern); err == nil {
		if !prefix.Addr().Is4() {
			return nil, false
		}
		prefix = prefix.Masked()
		lo := prefix.Addr().As4()
		hi := lo
		bits := prefix.Bits()
		for i := 0; i < 4; i++ {
			keep := min(max(bits-8*i, 0), 8)
			mask := byte(0xff << (8 - keep))
			hi[i] = lo[i] | ^mask
		}
		return ipv4RangeBoxes(lo, hi), true
	}
	if from, to, ok := strings.Cut(pattern, "-"); ok {
		a, err1 := netip.ParseAddr(from)
		b, err2 := netip.ParseAddr(to)
		if err1 != nil || err2 != nil || !a.Is4() || !b.Is4() || b.Less(a) {
			return nil, false
		}
		return ipv4RangeBoxes(a.As4(), b.As4()), true
	}
	if a, err := netip.ParseAddr(pattern); err == nil && a.Is4() {
		return ipv4RangeBoxes(a.As4(), a.As4()), true
	}
	return nil, false
}

// ipv4RangeBoxes splits the addresses lo..hi into boxes, octet by octet.
func ipv4RangeBoxes(lo, hi [4]byte) []ipv4Box {
	var out []ipv4Box
	var walk func(d int, lo, hi [4]byte, prefix ipv4Box)
	walk = func(d int, lo, hi [4]byte, prefix ipv4Box) {
		if d == 4 {
			out = append(out, prefix)
			return
		}
		full := func(from int) bool { // lo[from:] all 0 and hi[from:] all 255
			for i := from; i < 4; i++ {
				if lo[i] != 0 || hi[i] != 255 {
					return false
				}
			}
			return true
		}
		if lo[d] == hi[d] {
			prefix[d] = [2]int{int(lo[d]), int(lo[d])}
			walk(d+1, lo, hi, prefix)
			return
		}
		if full(d + 1) {
			prefix[d] = [2]int{int(lo[d]), int(hi[d])}
			for i := d + 1; i < 4; i++ {
				prefix[i] = [2]int{0, 255}
			}
			out = append(out, prefix)
			return
		}
		top, bottom := [4]byte{}, [4]byte{}
		for i := range 4 {
			top[i], bottom[i] = hi[i], lo[i]
		}
		// The first value of octet d with the rest from lo to 255...
		for i := d + 1; i < 4; i++ {
			top[i] = 255
		}
		top[d] = lo[d]
		walk(d, lo, top, prefix)
		// ...the values between, each with every rest...
		if int(hi[d])-int(lo[d]) > 1 {
			mid := prefix
			mid[d] = [2]int{int(lo[d]) + 1, int(hi[d]) - 1}
			for i := d + 1; i < 4; i++ {
				mid[i] = [2]int{0, 255}
			}
			out = append(out, mid)
		}
		// ...and the last value with the rest from 0 to hi.
		for i := d + 1; i < 4; i++ {
			bottom[i] = 0
		}
		bottom[d] = hi[d]
		walk(d, bottom, hi, prefix)
	}
	walk(0, lo, hi, ipv4Box{})
	return out
}

// decimalRange is a regexp of the numbers a..b (0 <= a <= b <= 255) written without leading zeros.
func decimalRange(a, b int) string {
	var alts []string
	add := func(lo, hi int) {
		if lo > hi {
			return
		}
		alts = append(alts, digitsRange(strconv.Itoa(lo), strconv.Itoa(hi)))
	}
	for _, span := range [][2]int{{0, 9}, {10, 99}, {100, 999}} {
		add(max(a, span[0]), min(b, span[1]))
	}
	if len(alts) == 1 {
		return alts[0]
	}
	return "(?:" + strings.Join(alts, "|") + ")"
}

// digitsRange is a regexp of the numbers lo..hi of the same digit count.
func digitsRange(lo, hi string) string {
	if lo == hi {
		return lo
	}
	if len(lo) == 1 {
		return "[" + lo + "-" + hi + "]"
	}
	if lo[0] == hi[0] {
		return lo[:1] + digitsRange(lo[1:], hi[1:])
	}
	rest := len(lo) - 1
	var alts []string
	// The first lead digit with the rest from lo up, the leads between with every rest, the last lead up to hi.
	alts = append(alts, lo[:1]+digitsRange(lo[1:], strings.Repeat("9", rest)))
	if hi[0]-lo[0] > 1 {
		alts = append(alts, "["+string(lo[0]+1)+"-"+string(hi[0]-1)+"]"+strings.Repeat("[0-9]", rest))
	}
	alts = append(alts, hi[:1]+digitsRange(strings.Repeat("0", rest), hi[1:]))
	return "(?:" + strings.Join(alts, "|") + ")"
}

// The edges of the IPv4 regexp. A negated class such as [^0-9.:] holds the letters and the rest of Unicode, and
// VictoriaLogs prints it while it simplifies a regexp; Go's regexp/syntax then checks its case folding rune by rune,
// about a millisecond per class on every query. Each edge is therefore written as a class without ASCII letters and
// the letters in a class of their own, with the anchor between them so the parser does not merge the two back into
// one class: the same characters match, and the regexp parses in microseconds.
const (
	// ipv4Other is the start of the line or a character no run holds and no run starts at (^ or [^0-9A-Fa-f.:]).
	// The walk is at a character right after one in every line, so the regexp starts there.
	ipv4Other = `(?:[^0-9.:A-Za-z]|^|[G-Zg-z])`
	ipv4Trail = `(?:[^0-9.A-Za-z]|$|[A-Za-z])` // [^0-9.] or $
	ipv4Hex   = `[0-9a-fA-F]`
	// ipv4RunEnd is the rest of a run of digits and dots and the hex letter or colon that ends it, which the walk
	// skips.
	ipv4RunEnd = `[0-9.]*[a-fA-F:]`
	// ipv4Step is a stretch of hex digits, colons and dots the walk crosses from where it starts afresh to where it
	// starts afresh again, without an IPv6 run (which only a character outside the three ends): dots and colons; hex
	// digits up to a digit and a dot, then that run and its end; hex digits up to a letter, then a dot (no run
	// starts there); a digit and a colon, which a dot two characters on makes a run of the digit, then a dot, a run
	// from a digit, or a letter or colon and a dot; two hex digits from a digit, a colon and a dot.
	ipv4Step = `(?::*\.|:?(?:` + ipv4Hex + `*[0-9]\.` + ipv4RunEnd + `|` + ipv4Hex + `*[a-fA-F]\.|[0-9]:(?:\.|[0-9]\.` +
		ipv4RunEnd + `|[a-fA-F:]\.)|[0-9]` + ipv4Hex + `:\.))`
	// ipv4LeadShort is what may come right before a first octet of one or two digits where the walk starts afresh:
	// nothing, a colon, or hex digits ending in a letter (no run starts at them, and the octet's dot is too close
	// for an IPv6 group). ipv4LeadOne adds a digit and a colon before a one-digit octet: the dot three characters
	// on makes the digit a run of its own, and the walk skips it and the colon. ipv4LeadLong is what may come
	// before a three-digit first octet: any hex digits, which start nothing since the octet holds no colon and
	// its dot is four characters on.
	ipv4LeadShort = `:?(?:` + ipv4Hex + `*[a-fA-F])?`
	ipv4LeadOne   = `:?[0-9]:`
	ipv4LeadLong  = `:?` + ipv4Hex + `*`
)

// ipv4Regexps are the regexps of an IPv4 pattern read on a line or a label value. exact keeps a line exactly when
// Loki's filter finds an address of the pattern in it: from the start of the line or a character no run holds
// (ipv4Other), the stretches the walk crosses (ipv4Step), the characters before the first octet (ipv4LeadShort,
// ipv4LeadOne, ipv4LeadLong, by the octet's digits), the address, and a character that ends its run.
//
// exact crosses every stretch of hex digits, colons and dots on the line, which costs VictoriaLogs more than a
// plain search for the address, so the filters run it on the few lines the two cheap ones leave: direct is an
// address right after the start of the line or a character no run holds, where the walk always starts afresh, so
// every line it keeps is kept by exact; guard is an address after a hex digit, a colon or a dot, which every other
// line exact keeps holds, and which a line with a near miss (10.0.0.12 for 10.0.0.1) does not, so exact runs on few
// lines. A line is kept by exact exactly when direct keeps it, or guard and exact both do (ipv4Split).
type ipv4Regexps struct{ exact, direct, guard string }

func ipv4LineRegexps(boxes []ipv4Box) ipv4Regexps {
	var one, two, three []string
	for _, b := range boxes {
		rest := `\.` + decimalRange(b[1][0], b[1][1]) + `\.` + decimalRange(b[2][0], b[2][1]) + `\.` + decimalRange(b[3][0], b[3][1])
		for _, d := range []struct {
			lo, hi int
			out    *[]string
		}{{0, 9, &one}, {10, 99, &two}, {100, 255, &three}} {
			if lo, hi := max(b[0][0], d.lo), min(b[0][1], d.hi); lo <= hi {
				*d.out = append(*d.out, decimalRange(lo, hi)+rest)
			}
		}
	}
	group := func(alts []string) string { return "(?:" + strings.Join(alts, "|") + ")" }
	short := append(one[:len(one):len(one)], two...)
	var alts []string
	if len(short) > 0 {
		alts = append(alts, ipv4LeadShort+group(short))
	}
	if len(one) > 0 {
		alts = append(alts, ipv4LeadOne+group(one))
	}
	if len(three) > 0 {
		alts = append(alts, ipv4LeadLong+group(three))
	}
	addr := group(append(short, three...))
	return ipv4Regexps{
		exact:  ipv4Other + ipv4Step + "*" + group(alts) + ipv4Trail,
		direct: ipv4Other + addr + ipv4Trail,
		guard:  `[0-9a-fA-F:.]` + addr + ipv4Trail,
	}
}

// ipv4Split is the filter of the regexps on a field ("" for the line): direct, or guard and exact; exact alone when
// there is no direct one.
func ipv4Split(field string, r ipv4Regexps) logsql.FilterExpr {
	re := func(pattern string) logsql.FilterExpr {
		if field == "" {
			return logsql.Regexp{Pattern: pattern}
		}
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: pattern}
	}
	if r.direct == "" {
		return re(r.exact)
	}
	return logsql.AnyOf{re(r.direct), logsql.AllOf{re(r.guard), re(r.exact)}}
}

// ipv4Literal is the text every address of boxes starts with (the octets they all fix, with the dot after a fixed
// octet that another octet follows), "" when they share none: a substring VictoriaLogs can search, and look up in
// its bloom filters, before it runs the exact regexp.
func ipv4Literal(boxes []ipv4Box) string {
	var lit string
	for i, b := range boxes {
		var s strings.Builder
		for k := 0; k < 4 && b[k][0] == b[k][1]; k++ {
			s.WriteString(strconv.Itoa(b[k][0]))
			if k < 3 {
				s.WriteByte('.')
			}
		}
		if i == 0 {
			lit = s.String()
			continue
		}
		t, n := s.String(), 0
		for n < len(lit) && n < len(t) && lit[n] == t[n] {
			n++
		}
		lit = lit[:n]
	}
	return lit
}
