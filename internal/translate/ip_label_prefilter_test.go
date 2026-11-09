package translate

import (
	"regexp"
	"strconv"
	"testing"
)

// The prefilter of an IPv4 pattern is a substring every address of the pattern holds, so VictoriaLogs may skip the
// values without it before running the exact regexp: the fixed octets all boxes share, else an alternative per box
// or per first octet, else the dot of an address. Every address of the pattern, alone or inside other text, matches
// both the prefilter and the exact regexp.
//
// conformance: semantics/ip-label-filter-embedded-address
func TestIPv4Prefilter(t *testing.T) {
	for _, tc := range []struct {
		pattern, want string
		in, out       []string
	}{
		{"10.1.0.0/16", `10\.1\.`, []string{"10.1.0.0", "10.1.255.255"}, []string{"10.2.0.1"}},
		{"10.1.2.3", `10\.1\.2\.3`, []string{"10.1.2.3"}, []string{"10.1.2.4"}},
		{"10.0.0.0/7", `10\.|11\.`, []string{"10.0.0.0", "11.255.255.255"}, []string{"12.0.0.0", "9.255.255.255"}},
		{"10.0.0.0-12.255.255.255", `10\.|11\.|12\.`, []string{"10.0.0.0", "12.255.255.255"}, []string{"13.0.0.0"}},
		{"9.255.255.250-11.0.0.5", "", nil, nil},
		{"0.0.0.0/0", `\.`, []string{"0.0.0.0", "255.255.255.255"}, nil},
		{"10.0.0.0-100.255.255.255", `\.`, []string{"10.0.0.0", "100.255.255.255", "55.1.1.1"}, []string{"101.0.0.0"}},
	} {
		boxes, ok := ipv4Boxes(tc.pattern)
		if !ok {
			t.Fatalf("%s: not IPv4", tc.pattern)
		}
		got := ipv4Prefilter(boxes)
		if tc.in != nil && got != tc.want {
			t.Errorf("%s: prefilter %q, want %q", tc.pattern, got, tc.want)
		}
		pre, exact := regexp.MustCompile(got), regexp.MustCompile(IPLineFilterRegex(tc.pattern))
		for _, a := range tc.in {
			for _, v := range []string{a, a + ":443", "client " + a, "[" + a + "]:80"} {
				if !pre.MatchString(v) || !exact.MatchString(v) {
					t.Errorf("%s: %q is out (prefilter %v, exact %v)", tc.pattern, v, pre.MatchString(v), exact.MatchString(v))
				}
			}
		}
		for _, a := range tc.out {
			if exact.MatchString(a) {
				t.Errorf("%s: %q is in", tc.pattern, a)
			}
		}
	}
}

// Whatever the pattern, a value the exact regexp keeps holds the prefilter: the prefilter never drops a value of the
// pattern, on a sweep of first octets and tails around the boundaries of every pattern.
//
// conformance: semantics/ip-label-filter-embedded-address
func TestIPv4PrefilterNeverDropsAnExactMatch(t *testing.T) {
	patterns := []string{"0.0.0.0/0", "10.0.0.0/7", "10.0.0.0/4", "8.0.0.0/5", "9.255.255.250-11.0.0.5", "1.2.3.4-5.6.7.8",
		"10.0.0.5-10.2.0.0", "10.1.0.0/16", "250.0.0.0/5", "10.0.0.0-30.255.255.255", "0.0.0.0-0.0.0.9", "100.64.0.0/10"}
	tails := []string{".0.0.0", ".0.0.5", ".1.2.3", ".5.6.7", ".255.255.255", ".64.0.1", ".2.0.0", ".255.255.250", ".0.0.9"}
	for _, p := range patterns {
		boxes, ok := ipv4Boxes(p)
		if !ok {
			t.Fatal(p)
		}
		pre, exact := regexp.MustCompile(ipv4Prefilter(boxes)), regexp.MustCompile(IPLineFilterRegex(p))
		kept := 0
		for a := 0; a < 256; a++ {
			for _, tail := range tails {
				for _, v := range []string{strconv.Itoa(a) + tail, "x " + strconv.Itoa(a) + tail + ":80"} {
					if exact.MatchString(v) {
						kept++
						if !pre.MatchString(v) {
							t.Errorf("%s: prefilter %q drops %q, which the exact regexp keeps", p, pre, v)
						}
					}
				}
			}
		}
		if kept == 0 {
			t.Errorf("%s: the sweep keeps no value", p)
		}
	}
}
