package translator

import (
	"fmt"
	"math/rand"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// lokiIPLabelFilter is the reference: Loki's ip() label filter on a label
// value (pkg/logql/log/ip.go:123-145 and the scan at :183-226), written from
// its rules. present is false for a line without the label.
func lokiIPLabelFilter(pattern, value string, present, negate bool) (keep, ok bool) {
	var contains func(netip.Addr) bool
	if a, err := netip.ParseAddr(pattern); err == nil {
		contains = func(ip netip.Addr) bool { return a.Compare(ip) == 0 }
	} else if p, err := netip.ParsePrefix(pattern); err == nil {
		contains = p.Contains
	} else if from, to, cut := strings.Cut(pattern, "-"); cut {
		lo, err1 := netip.ParseAddr(from)
		hi, err2 := netip.ParseAddr(to)
		if err1 != nil || err2 != nil || lo.BitLen() != hi.BitLen() || lo.Zone() != hi.Zone() || hi.Less(lo) {
			return false, false
		}
		contains = func(ip netip.Addr) bool { return lo.Compare(ip) <= 0 && hi.Compare(ip) >= 0 }
	} else {
		return false, false
	}
	if !present {
		return false, true
	}
	found := lokiScanIP(value, contains)
	return found != negate, true
}

func lokiScanIP(line string, contains func(netip.Addr) bool) bool {
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }
	isHex := func(c byte) bool { return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }
	span := func(s, charset string) int {
		for i := 0; i < len(s); i++ {
			if !strings.ContainsRune(charset, rune(s[i])) {
				return i
			}
		}
		return len(s)
	}
	try := func(i int, charset string) (bool, int) {
		n := span(line[i:], charset)
		if ip, err := netip.ParseAddr(line[i : i+n]); err == nil && contains(ip) {
			return true, 0
		}
		return false, n
	}
	n := len(line)
	for i := 0; i < n; i++ {
		if i+3 < n && isDigit(line[i]) && (line[i+1] == '.' || line[i+2] == '.' || line[i+3] == '.') {
			ok, skip := try(i, "0123456789.")
			if ok {
				return true
			}
			i += skip
			continue
		}
		if i+4 < n {
			p := line[i : i+5]
			hint := p[0] == ':' && p[1] == ':' && isHex(p[2]) ||
				isHex(p[0]) && p[1] == ':' ||
				isHex(p[0]) && isHex(p[1]) && p[2] == ':' ||
				isHex(p[0]) && isHex(p[1]) && isHex(p[2]) && p[3] == ':' ||
				isHex(p[0]) && isHex(p[1]) && isHex(p[2]) && isHex(p[3]) && p[4] == ':'
			if hint {
				ok, skip := try(i, "0123456789abcdefABCDEF:.")
				if ok {
					return true
				}
				i += skip
			}
		}
	}
	return false
}

// vlFilter evaluates the LogsQL filter ipLabelFilter writes on one field
// value with VictoriaLogs' semantics: `f:*` a non-empty value (an absent
// field is empty), ipv4_range and ipv6_range one whole address
// (lib/logstorage values_encoder.go tryParseIPv4 with its leading zeros and
// unchecked second character of a two-character octet, parser.go
// tryParseIPv6 = netip.ParseAddr of 2 to 45 characters), `f:~"re"` an
// unanchored regexp, space AND, OR, `-` NOT and parentheses.
type vlFilter struct {
	src string
	pos int
	val string
}

func evalVLFilter(t testing.TB, expr, value string) bool {
	t.Helper()
	f := &vlFilter{src: expr, val: value}
	got := f.or(t)
	if f.skip(); f.pos != len(f.src) {
		t.Fatalf("filter %q: unread %q", expr, f.src[f.pos:])
	}
	return got
}

func (f *vlFilter) skip() {
	for f.pos < len(f.src) && f.src[f.pos] == ' ' {
		f.pos++
	}
}

func (f *vlFilter) or(t testing.TB) bool {
	v := f.and(t)
	for f.skip(); strings.HasPrefix(f.src[f.pos:], "OR "); f.skip() {
		f.pos += 3
		w := f.and(t)
		v = v || w
	}
	return v
}

func (f *vlFilter) and(t testing.TB) bool {
	v := true
	for {
		f.skip()
		if f.pos >= len(f.src) || f.src[f.pos] == ')' || strings.HasPrefix(f.src[f.pos:], "OR ") {
			return v
		}
		w := f.unary(t)
		v = v && w
	}
}

func (f *vlFilter) unary(t testing.TB) bool {
	f.skip()
	switch f.src[f.pos] {
	case '-':
		f.pos++
		return !f.unary(t)
	case '(':
		f.pos++
		v := f.or(t)
		f.skip()
		if f.src[f.pos] != ')' {
			t.Fatalf("filter %q: want ) at %d", f.src, f.pos)
		}
		f.pos++
		return v
	}
	// field:op
	colon := strings.IndexByte(f.src[f.pos:], ':')
	if f.src[f.pos] == '"' {
		end := strings.IndexByte(f.src[f.pos+1:], '"')
		colon = end + 2
	}
	f.pos += colon + 1
	rest := f.src[f.pos:]
	switch {
	case strings.HasPrefix(rest, "*"):
		f.pos++
		return f.val != ""
	case strings.HasPrefix(rest, "~"):
		f.pos++
		q, err := strconv.QuotedPrefix(f.src[f.pos:])
		if err != nil {
			t.Fatalf("filter %q: regexp at %d: %v", f.src, f.pos, err)
		}
		f.pos += len(q)
		pattern, _ := strconv.Unquote(q)
		return regexp.MustCompile(pattern).MatchString(f.val)
	case strings.HasPrefix(rest, "ipv4_range("), strings.HasPrefix(rest, "ipv6_range("):
		v6 := strings.HasPrefix(rest, "ipv6")
		end := strings.IndexByte(rest, ')')
		args := strings.Split(rest[len("ipv4_range("):end], ", ")
		f.pos += end + 1
		unq := func(s string) string {
			if u, err := strconv.Unquote(s); err == nil {
				return u
			}
			return s
		}
		if !v6 {
			lo, _ := vlParseIPv4(unq(args[0]))
			hi, _ := vlParseIPv4(unq(args[1]))
			n, ok := vlParseIPv4(f.val)
			return ok && n >= lo && n <= hi
		}
		lo := netip.MustParseAddr(unq(args[0])).As16()
		hi := netip.MustParseAddr(unq(args[1])).As16()
		if len(f.val) < 2 || len(f.val) > 45 {
			return false
		}
		a, err := netip.ParseAddr(f.val)
		if err != nil {
			return false
		}
		b := a.As16()
		return string(b[:]) >= string(lo[:]) && string(b[:]) <= string(hi[:])
	}
	t.Fatalf("filter %q: unknown filter at %d", f.src, f.pos)
	return false
}

// vlParseIPv4 mirrors VictoriaLogs' tryParseIPv4.
func vlParseIPv4(s string) (uint32, bool) {
	if len(s) < 7 || len(s) > 15 || strings.Count(s, ".") != 3 {
		return 0, false
	}
	// The first three octets are at most three characters (strings.IndexByte
	// at most 3); the last goes to tryParseDateUint64 whole (up to nine).
	octet := func(p string, last bool) (uint32, bool) {
		if len(p) == 0 || len(p) > 9 || !last && len(p) > 3 {
			return 0, false
		}
		if len(p) == 2 {
			if p[0] < '0' || p[0] > '9' {
				return 0, false
			}
			return 10*uint32(p[0]-'0') + uint32(p[1]-'0'), true
		}
		n := uint64(0)
		for i := 0; i < len(p); i++ {
			if p[i] < '0' || p[i] > '9' {
				return 0, false
			}
			n = n*10 + uint64(p[i]-'0')
		}
		if n > 255 {
			return 0, false
		}
		return uint32(n), true
	}
	parts := strings.Split(s, ".")
	var out uint32
	for i, p := range parts {
		v, ok := octet(p, i == 3)
		if !ok || v > 255 {
			return 0, false
		}
		out = out<<8 | v
	}
	return out, true
}

// exactFor reports the values on which the translation must equal Loki: one
// whole address as VictoriaLogs reads it, or a value with no text of the
// pattern's form (lokiIPPattern.textOf), which no address of the pattern is
// scanned out of either. Any other value holds an address inside other text
// (semantics/ip-label-filter-embedded-address), where the translation keeps a
// subset of Loki's lines.
func exactFor(pattern, value string) bool {
	if _, ok := vlParseIPv4(value); ok && strings.Trim(value, "0123456789.") == "" {
		return true // VictoriaLogs also reads "0.0.0.0A" whole, as 0.0.0.17
	}
	if len(value) >= 2 && len(value) <= 45 && !strings.Contains(value, "%") {
		if _, err := netip.ParseAddr(value); err == nil {
			return true // a zone can hold an address Loki scans ("::%0::00")
		}
	}
	p, ok := parseLokiIPPattern(pattern)
	return !ok || p.empty || !regexp.MustCompile(p.textOf()).MatchString(value)
}

// Loki's ip() label filter on whole-address values, other families, leading
// zeros, absent labels and plain text, against the LogsQL it is translated to.
// conformance: semantics/ip-label-filter-missing-label
func TestIPLabelFilter_EdgeTable(t *testing.T) {
	const absent = "\x00absent"
	for _, tc := range []struct {
		pattern, value string
		eq, ne         bool // Loki: kept under = and under !=
	}{
		// An absent label: dropped under both operators (ip.go:128-131).
		{"1.2.3.4", absent, false, false},
		{"1.2.3.0/24", absent, false, false},
		{"2001:db8::/32", absent, false, false},
		// Whole IPv4 values.
		{"1.2.3.4", "1.2.3.4", true, false},
		{"1.2.3.4", "1.2.3.5", false, true},
		{"1.2.3.0/24", "1.2.3.255", true, false},
		{"1.2.3.0/24", "1.2.4.0", false, true},
		{"1.2.3.7/24", "1.2.3.200", true, false}, // host bits set in the pattern
		{"0.0.0.0/0", "255.255.255.255", true, false},
		{"1.2.3.4/32", "1.2.3.4", true, false},
		{"1.2.3.4-1.2.4.9", "1.2.4.9", true, false},
		{"1.2.3.4-1.2.4.9", "1.2.3.3", false, true},
		// Leading zeros: Loki's netip parser reads no address.
		{"1.2.3.4", "01.2.3.4", false, true},
		{"1.2.3.0/24", "1.2.3.04", false, true},
		// VictoriaLogs reads "1.2.3.4a" as 1.2.3.89 and "1.2.3.0004" as
		// 1.2.3.4; Loki scans 1.2.3.4 out of the first and nothing out of the
		// second.
		{"1.2.3.89", "1.2.3.4a", false, true},
		{"1.2.3.4", "1.2.3.0004", false, true},
		// The other family never matches.
		{"::ffff:1.2.3.4", "1.2.3.4", false, true},
		{"1.2.3.0/24", "::ffff:1.2.3.4", false, true},
		// IPv6.
		{"2001:db8::1", "2001:db8::1", true, false},
		{"2001:db8::/32", "2001:db8:0:1::5", true, false},
		{"2001:db8::/32", "2001:db9::1", false, true},
		{"::1", "::1", false, true}, // shorter than five characters: never scanned
		{"fe80::1", "fe80::1%eth0", true, false},
		{"fe80::1%eth0", "fe80::1", false, true}, // no scanned address has a zone
		{"::", "::%00000", false, true},          // the scan starts no address at "::%"
		// Plain text and the empty value.
		{"1.2.3.4", "abc", false, true},
		{"1.2.3.4", "", false, true},
	} {
		present := tc.value != absent
		value := tc.value
		if !present {
			value = ""
		}
		for _, negate := range []bool{false, true} {
			want, ok := lokiIPLabelFilter(tc.pattern, value, present, negate)
			if !ok {
				t.Fatalf("%s: Loki rejects the pattern", tc.pattern)
			}
			if expect := map[bool]bool{false: tc.eq, true: tc.ne}[negate]; want != expect {
				t.Fatalf("reference: ip(%q) on %q negate=%v = %v, table says %v", tc.pattern, tc.value, negate, want, expect)
			}
			filter, ok := ipLabelFilter("addr", tc.pattern, negate)
			if !ok {
				t.Fatalf("%s: not translated", tc.pattern)
			}
			// An empty value is absent in VictoriaLogs; Loki's json parser
			// keeps an empty label, which `!=` keeps (a documented edge).
			if tc.value == "" && negate {
				want = false
			}
			// A zoned value: `!=` may drop it where Loki keeps it (a zone can
			// hold an address Loki scans, "::%0::00").
			subset := strings.Contains(value, "%")
			if got := evalVLFilter(t, filter, value); got != want && (!subset || got) {
				t.Errorf("ip(%q) negate=%v on %q: translated %v, Loki %v\n%s", tc.pattern, negate, tc.value, got, want, filter)
			}
		}
	}
	// Loki rejects these patterns; the translation does not read them.
	for _, bad := range []string{"999.1.1.1", "1.2.3.4/33", "1.2.3.4-1.2.3.1", "1.2.3.4 - 1.2.3.9", "abc", "1.2.3.4-::1", ""} {
		if _, ok := ipLabelFilter("addr", bad, false); ok {
			t.Errorf("%q translated; Loki rejects it", bad)
		}
		if _, ok := lokiIPLabelFilter(bad, "1.2.3.4", true, false); ok {
			t.Errorf("%q: the reference accepts it", bad)
		}
	}
}

// A differential run: random patterns of both families (single addresses,
// CIDRs from /0 to the full length, ranges) and random values (absent,
// inside and outside the pattern, the other family, leading zeros, plain
// text, an address inside other text). The translated filter never keeps a
// line Loki drops, and equals Loki on every value that is one whole address
// or holds no text of the pattern's form (exactFor).
// conformance: semantics/ip-label-filter-missing-label, semantics/ip-label-filter-embedded-address
func TestIPLabelFilter_Differential(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	checked, embedded := 0, 0
	for i := 0; i < 4000; i++ {
		pattern := randomIPPattern(rng)
		value, present := randomIPValue(rng)
		for _, negate := range []bool{false, true} {
			want, ok := lokiIPLabelFilter(pattern, value, present, negate)
			if !ok {
				t.Fatalf("pattern %q", pattern)
			}
			filter, ok := ipLabelFilter("addr", pattern, negate)
			if !ok {
				t.Fatalf("pattern %q not translated", pattern)
			}
			got := evalVLFilter(t, filter, value)
			if got && !want {
				t.Errorf("ip(%q) negate=%v keeps %q, which Loki drops\n%s", pattern, negate, value, filter)
			}
			if value == "" && present && negate {
				continue // the empty label: absent in VictoriaLogs
			}
			if exactFor(pattern, value) && got != want {
				t.Errorf("ip(%q) negate=%v on %q: %v, Loki %v\n%s", pattern, negate, value, got, want, filter)
			}
			if !exactFor(pattern, value) {
				embedded++
			}
			checked++
		}
	}
	t.Logf("checked %d, of which %d embedded (subset only)", checked, embedded)
}

func randomIPPattern(rng *rand.Rand) string {
	v4 := func() netip.Addr {
		return netip.AddrFrom4([4]byte{byte(rng.Intn(3) + 1), byte(rng.Intn(3)), byte(rng.Intn(256)), byte(rng.Intn(256))})
	}
	v6 := func() netip.Addr {
		var b [16]byte
		b[0], b[1], b[15] = 0x20, 0x01, byte(rng.Intn(256))
		b[3] = byte(rng.Intn(3))
		return netip.AddrFrom16(b)
	}
	switch rng.Intn(6) {
	case 0:
		return v4().String()
	case 1:
		return netip.PrefixFrom(v4(), rng.Intn(33)).String()
	case 2:
		a, b := v4(), v4()
		if b.Less(a) {
			a, b = b, a
		}
		return a.String() + "-" + b.String()
	case 3:
		return v6().String()
	case 4:
		return netip.PrefixFrom(v6(), rng.Intn(129)).String()
	}
	a, b := v6(), v6()
	if b.Less(a) {
		a, b = b, a
	}
	return a.String() + "-" + b.String()
}

func randomIPValue(rng *rand.Rand) (string, bool) {
	v4 := fmt.Sprintf("%d.%d.%d.%d", rng.Intn(3)+1, rng.Intn(3), rng.Intn(256), rng.Intn(256))
	v6 := fmt.Sprintf("2001:%x::%x", rng.Intn(3), rng.Intn(256))
	switch rng.Intn(12) {
	case 0:
		return "", false
	case 1, 2, 3:
		return v4, true
	case 4, 5:
		return v6, true
	case 6:
		return "0" + v4, true // a leading zero
	case 7:
		return "::ffff:" + v4, true
	case 8:
		return []string{"abc", "", "-", "n/a", "1.2", "::"}[rng.Intn(6)], true
	case 9:
		return v4 + ":" + strconv.Itoa(rng.Intn(65536)), true // embedded
	case 10:
		return "client " + v4, true
	}
	return "[" + v6 + "]:443", true
}

// FuzzIPLabelFilterNeverKeepsWhatLokiDrops: for any pattern Loki accepts and
// any value, the translated filter keeps a line only when Loki does, and
// equals Loki where exactFor says it must.
func FuzzIPLabelFilterNeverKeepsWhatLokiDrops(f *testing.F) {
	for _, seed := range [][2]string{
		{"1.2.3.4", "1.2.3.4"}, {"1.2.3.0/24", "1.2.3.4:443"}, {"1.2.3.89", "1.2.3.4a"},
		{"2001:db8::/32", "2001:db8::1"}, {"::ffff:1.2.3.4", "1.2.3.4"}, {"1.2.3.4-1.2.4.9", "01.2.3.4"},
		{"fe80::1", "fe80::1%eth0"}, {"0.0.0.0/0", "x 9.9.9.9 y"}, {"f00::", "0X::0"}, {"::", "::%00000"}, {"0.0.0.0", "0.0.0.0A"}, {"::", "::%0::00"},
	} {
		f.Add(seed[0], seed[1], false)
		f.Add(seed[0], seed[1], true)
	}
	f.Fuzz(func(t *testing.T, pattern, value string, negate bool) {
		want, ok := lokiIPLabelFilter(pattern, value, true, negate)
		filter, tok := ipLabelFilter("addr", pattern, negate)
		if ok != tok {
			t.Fatalf("pattern %q: Loki accepts=%v, translated=%v", pattern, ok, tok)
		}
		if !ok || strings.ContainsAny(value, "\x00") {
			return
		}
		got := evalVLFilter(t, filter, value)
		if got && !want {
			t.Fatalf("ip(%q) negate=%v keeps %q, which Loki drops\n%s", pattern, negate, value, filter)
		}
		if exactFor(pattern, value) && got != want && value != "" {
			t.Fatalf("ip(%q) negate=%v on %q: %v, Loki %v\n%s", pattern, negate, value, got, want, filter)
		}
	})
}
