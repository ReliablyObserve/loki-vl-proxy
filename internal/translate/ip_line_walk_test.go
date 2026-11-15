package translate

import (
	"math/rand"
	"net/netip"
	"os"
	"regexp"
	"strings"
	"testing"
)

// ipv4Walk is the reference of Loki's ip() filter on a line for an IPv4 pattern, written from the behaviour the Loki
// oracle records (the walk ip_line_v4.go describes): from the start of the line, at a digit with a dot one to three
// characters on it reads the run of digits and dots there; else at the start of an IPv6 group (one to four hex
// digits and a colon, or two colons and a hex digit, with five characters left) the run of hex digits, colons and
// dots; else it moves one character on. After a run it skips the run and the character that ends it. A run of
// digits and dots that is an address of the pattern is a match. TestIPv4WalkMatchesLoki holds it to Loki's recorded
// answers.
func ipv4Walk(line string, p IPPattern) bool {
	digit := func(c byte) bool { return '0' <= c && c <= '9' }
	hex := func(c byte) bool { return digit(c) || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F' }
	run := func(i int, set string) int {
		j := i
		for j < len(line) && strings.IndexByte(set, line[j]) >= 0 {
			j++
		}
		return j - i
	}
	group := func(i int) bool { // an IPv6 group starts at i
		if line[i] == ':' {
			return line[i+1] == ':' && hex(line[i+2])
		}
		for k := i; k < i+4 && hex(line[k]); k++ {
			if line[k+1] == ':' {
				return true
			}
		}
		return false
	}
	n := len(line)
	for i := 0; i < n; i++ {
		if i+3 < n && digit(line[i]) && strings.IndexByte(line[i+1:i+4], '.') >= 0 {
			k := run(i, "0123456789.")
			if a, err := netip.ParseAddr(line[i : i+k]); err == nil && a.Is4() && !p.Empty && !a.Less(p.Lo) && !p.Hi.Less(a) {
				return true
			}
			i += k
			continue
		}
		if i+4 < n && group(i) {
			i += run(i, "0123456789abcdefABCDEF:.")
		}
	}
	return false
}

// The reference walk answers like Loki on every IPv4 line of the recorded grid, so the fuzz target and the random
// run below compare the regexp with Loki's answers.
// conformance: semantics/ip-filter-ipv4-after-hex-colon
func TestIPv4WalkMatchesLoki(t *testing.T) {
	cases := ipLineCases()
	b, err := os.ReadFile(ipLineLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	answers := strings.TrimSpace(string(b))
	if len(answers) != len(cases) {
		t.Fatalf("%d recorded answers for %d cases: record them again with LVP_RUN_ORACLES=1", len(answers), len(cases))
	}
	checked := 0
	for i, c := range cases {
		if strings.Contains(c.pattern, ":") {
			continue
		}
		p, ok := ParseLokiIPPattern(c.pattern)
		if !ok {
			t.Fatalf("pattern %q", c.pattern)
		}
		if got, loki := ipv4Walk(c.line, p), answers[i] == '1'; got != loki {
			t.Errorf("ip(%q) on %q: walk %v, Loki %v", c.pattern, c.line, got, loki)
		}
		checked++
	}
	if checked < 40000 {
		t.Fatalf("only %d IPv4 lines checked", checked)
	}
}

// ipv4WalkPieces are the bits random lines are made of: every character class the walk tells apart (digits, hex
// letters, other letters, colons, dots, other characters) and the short stretches that change where it starts.
var ipv4WalkPieces = []string{"0", "1", "9", "a", "f", "A", "g", "Z", "x", ":", ".", "-", " ", "[", "00", "1.0", ":1.",
	"::", "1:a", "a0:", "X0:", "0:", "1:1.", "11:", "1a:", "111:", "a1:"}

// randomIPv4Line is a line of random pieces with, most of the time, one of addrs among them.
func randomIPv4Line(rng *rand.Rand, addrs []string) string {
	var b strings.Builder
	for k := rng.Intn(5); k > 0; k-- {
		b.WriteString(ipv4WalkPieces[rng.Intn(len(ipv4WalkPieces))])
	}
	if rng.Intn(10) > 0 {
		b.WriteString(addrs[rng.Intn(len(addrs))])
	}
	for k := rng.Intn(4); k > 0; k-- {
		b.WriteString(ipv4WalkPieces[rng.Intn(len(ipv4WalkPieces))])
	}
	return b.String()
}

var ipv4WalkPatterns = []string{"1.2.3.0/27", "1.2.3.4", "0.0.0.0/0", "9.0.0.0-100.255.255.255", "12.2.3.0/24", "123.0.0.1",
	"0.0.0.0", "255.255.255.255", "1.0.0.0/8", "99.1.1.1-101.1.1.1", "10.0.0.0/24"}

var ipv4WalkAddrs = []string{"1.2.3.0", "1.2.3.4", "1.2.3.31", "1.2.3.32", "12.2.3.4", "123.0.0.1", "0.0.0.0",
	"255.255.255.255", "9.0.0.0", "100.255.255.255", "101.1.1.1", "99.1.1.1", "01.2.3.4", "1.2.3.04", "10.0.0.7"}

// The IPv4 regexp keeps a line exactly when the walk finds an address of the pattern in it, on random lines built
// from the pieces that change where the walk starts.
// conformance: semantics/ip-filter-ipv4-after-hex-colon, semantics/ip-line-filter-ipv4-like-loki
func TestIPv4LineRegexMatchesWalk(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for _, pattern := range ipv4WalkPatterns {
		p, _ := ParseLokiIPPattern(pattern)
		re := regexp.MustCompile(IPLineFilterRegex(pattern))
		for i := 0; i < 20000; i++ {
			line := randomIPv4Line(rng, ipv4WalkAddrs)
			if got, want := re.MatchString(line), ipv4Walk(line, p); got != want {
				t.Fatalf("ip(%q) on %q: regexp %v, Loki %v", pattern, line, got, want)
			}
		}
	}
}

// The line filter keeps a line exactly when the exact regexp does: through the direct regexp, or the guard and the
// exact one (ipv4Split), on random lines and on every IPv4 line of the recorded grid, so the cheap regexps lose no
// line and keep none Loki drops.
// conformance: semantics/ip-filter-ipv4-after-hex-colon
func TestIPv4SplitMatchesExact(t *testing.T) {
	check := func(pattern, line string) {
		boxes, _ := ipv4Boxes(pattern)
		r := ipv4LineRegexps(boxes)
		exact := regexp.MustCompile(r.exact).MatchString(line)
		direct, guard := regexp.MustCompile(r.direct).MatchString(line), regexp.MustCompile(r.guard).MatchString(line)
		if direct && !exact {
			t.Fatalf("ip(%q) on %q: direct keeps a line exact drops", pattern, line)
		}
		if exact && !direct && !guard {
			t.Fatalf("ip(%q) on %q: exact keeps a line neither direct nor guard keeps", pattern, line)
		}
		if got := lineKept(t, IPLineFilter(pattern, false), line); got != exact {
			t.Fatalf("ip(%q) on %q: line filter %v, exact %v", pattern, line, got, exact)
		}
	}
	rng := rand.New(rand.NewSource(9))
	for _, pattern := range ipv4WalkPatterns {
		for i := 0; i < 3000; i++ {
			check(pattern, randomIPv4Line(rng, ipv4WalkAddrs))
		}
	}
	for _, c := range ipLineCases() {
		if !strings.Contains(c.pattern, ":") {
			check(c.pattern, c.line)
		}
	}
}

// Edge cases of the walk, with Loki's answers (the Loki oracle, ipfilter, recorded 2026-11-15): the line filter's
// regexp and the label filter's text branch are the same regexp.
// conformance: semantics/ip-filter-ipv4-after-hex-colon
func TestIPv4LineRegexWalkEdges(t *testing.T) {
	for _, c := range []struct {
		line string
		loki bool
	}{
		{"X0:1.2.3.0", true},  // the run "0" is read first (a dot three characters on), the colon skipped
		{"0:1.2.3.0", true},   // the same at the start of the line
		{"a0:1.2.3.0", false}, // an IPv6 run starts at "a0:" and takes the address
		{"00:1.2.3.0", false}, // no dot within three characters of the first digit: an IPv6 run
		{"1:a1.2.3.0", false}, // an IPv6 run from "1:" takes the letter and the address
		{"1.0:1.2.3.0", true}, // the run "1.0" and its colon are skipped
		{"1.2.3.0", true},
		{"x 1.2.3.0:443", true},
	} {
		re := regexp.MustCompile(IPLineFilterRegex("1.2.3.0/27"))
		if got := re.MatchString(c.line); got != c.loki {
			t.Errorf("ip(\"1.2.3.0/27\") on %q: %v, Loki %v", c.line, got, c.loki)
		}
	}
}

// FuzzIPv4LineRegexLikeLoki: for any IPv4 pattern and any line, the regexp the line and label filters run, and the
// line filter with its cheap regexps first, keep the line exactly when Loki's walk finds an address of the pattern
// in it.
func FuzzIPv4LineRegexLikeLoki(f *testing.F) {
	for _, seed := range [][2]string{
		{"1.2.3.0/27", "X0:1.2.3.0"}, {"1.2.3.0/27", "a0:1.2.3.0"}, {"1.2.3.0/27", "1:a1.2.3.0"}, {"1.2.3.0/27", "1.0:1.2.3.0"},
		{"10.0.0.1", "x 0..10.0.0.1 y"}, {"0.0.0.0/0", "1.2a345.6.7.8"}, {"123.0.0.1", "aaaa123.0.0.1"}, {"9.0.0.0-100.255.255.255", "1:1.9.0.0.1"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pattern, line string) {
		if _, ok := ipv4Boxes(pattern); !ok {
			return
		}
		p, ok := ParseLokiIPPattern(pattern)
		if !ok {
			t.Fatalf("pattern %q: boxes but no Loki pattern", pattern)
		}
		re := regexp.MustCompile(IPLineFilterRegex(pattern))
		want := ipv4Walk(line, p)
		if got := re.MatchString(line); got != want {
			t.Fatalf("ip(%q) on %q: regexp %v, Loki %v", pattern, line, got, want)
		}
		if got := lineKept(t, IPLineFilter(pattern, false), line); got != want {
			t.Fatalf("ip(%q) on %q: line filter %v, Loki %v", pattern, line, got, want)
		}
	})
}
