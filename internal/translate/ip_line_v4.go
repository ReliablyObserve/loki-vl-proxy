package translate

import (
	"net/netip"
	"strconv"
	"strings"
)

// The IPv4 ip() line filter as Loki's filter reads a line (recorded with the Loki oracle, test/oracle/loki
// ipfilter): a candidate address is a run of digits and dots that starts at a digit; its first octet is the last
// three digits before the first dot (so digits in front of a three-digit first octet are ignored, and any digit in
// front of a shorter one makes another octet); the other octets are read as they are; the candidate is four octets
// of at most 255 without leading zeros, so a digit or a dot right after it makes it invalid. A single colon in
// front is the IPv6 scanner's unless no hex digit precedes it, and a dot in front is part of the run unless no digit
// precedes it.

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

// ipv4LineRegex is the regexp of a line that holds an address of boxes as Loki's filter finds it.
func ipv4LineRegex(boxes []ipv4Box) string {
	// What may come right before the first octet: the start of the line, a character outside [0-9.:], a colon
	// no hex digit or colon precedes, or dots no digit precedes.
	const before = `(?:^|[^0-9.:]|(?:^|[^0-9A-Fa-f:]):|(?:^|[^0-9])\.+)`
	var short, long []string
	for _, b := range boxes {
		rest := `\.` + decimalRange(b[1][0], b[1][1]) + `\.` + decimalRange(b[2][0], b[2][1]) + `\.` + decimalRange(b[3][0], b[3][1])
		// A first octet of three digits may follow any digits: they are not read.
		if lo, hi := max(b[0][0], 100), b[0][1]; lo <= hi {
			long = append(long, decimalRange(lo, hi)+rest)
		}
		if lo, hi := b[0][0], min(b[0][1], 99); lo <= hi {
			short = append(short, decimalRange(lo, hi)+rest)
		}
	}
	var alts []string
	if len(short) > 0 {
		alts = append(alts, before+"(?:"+strings.Join(short, "|")+")")
	}
	if len(long) > 0 {
		alts = append(alts, before+"[0-9]*(?:"+strings.Join(long, "|")+")")
	}
	return "(?:" + strings.Join(alts, "|") + `)(?:[^0-9.]|$)`
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
