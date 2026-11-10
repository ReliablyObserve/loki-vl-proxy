package translator

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// The reference scan (lokiScanIP, written from the rules of Loki's ip() scan) answers exactly what Loki answers on
// every case of the recorded label grid, for = and !=: it is the specification an exact IPv6 filter is built to.
//
// conformance: semantics/ip-filter-precision, semantics/ip-label-filter-embedded-address
func TestLokiScanReferenceMatchesLokiOnTheLabelGrid(t *testing.T) {
	cases := ipLabelCases()
	b, _ := os.ReadFile(ipLabelLokiAnswers)
	ans := strings.TrimSpace(string(b))
	diff := map[string]int{}
	ex := map[string][]string{}
	for i, c := range cases {
		for j, neg := range []bool{false, true} {
			want, ok := lokiIPLabelFilter(c.pattern, c.value, true, neg)
			if !ok {
				t.Fatal(c.pattern)
			}
			if want != (ans[2*i+j] == '1') {
				k := fmt.Sprintf("neg=%v ref=%v", neg, want)
				diff[k]++
				if len(ex[k]) < 8 {
					ex[k] = append(ex[k], fmt.Sprintf("ip(%q) on %q", c.pattern, c.value))
				}
			}
		}
	}
	t.Logf("%d cases", len(cases))
	if len(diff) != 0 {
		t.Errorf("the reference differs from Loki: %v, e.g. %v", diff, ex)
	}
}

// lineCase is one ip() line filter pattern and one line of the grid in internal/translate (ipLineCases).
type lineCase struct{ pattern, line string }

func ipLineGridCases() []lineCase {
	v4Patterns := []string{"10.0.0.1", "10.0.0.0/24", "10.0.0.1-10.0.0.5", "192.168.1.1", "10.0.0.0/8"}
	v6Patterns := []string{"::1", "2001:db8::1", "2001:db8::/32", "fe80::1", "::ffff:10.0.0.1"}
	v4 := []string{"10.0.0.1", "10.0.0.12", "110.0.0.12", "10.0.0.255", "10.0.1.1", "10.0.0.3", "192.168.1.1",
		"0010.0.0.1", "10.00.0.1", "10.0.0.01", "256.0.0.1"}
	v6 := []string{"::1", "2001:db8::1", "2001:DB8::1", "2001:db8:0::1", "2001:db8::12", "fe80::1", "::ffff:10.0.0.1",
		"::ffff:a00:1", "1::1", "2001:0db8:0000:0000:0000:0000:0000:0001", "10.0.0.1"}
	before := []string{"", " ", "a", "f", "g", "0", "1", ".", ":", "-", "[", "=", "1.", "::"}
	after := []string{"", " ", "a", "f", "g", "0", "1", ".", ":", "]", "/24", ",", "%eth0", ":80"}
	var out []lineCase
	grid := func(patterns, addrs, before []string) {
		for _, p := range patterns {
			for _, a := range addrs {
				for _, b := range before {
					for _, e := range after {
						out = append(out, lineCase{p, "x " + b + a + e + " y"})
					}
				}
			}
		}
	}
	grid(v4Patterns, v4, before)
	grid(v6Patterns, v6, before)
	grid(v4Patterns, v4, []string{"..", "...", "0..", "1..", "a..", "f..", ":..", "1.2..", "1. ."})
	grid(v4Patterns, v4, []string{"0:", "1:", "a:", "f:", "g:", "z:", "0::", "a::", "1:2:"})
	return out
}

// The same reference answers what Loki's ip() line filter answers on every line of the recorded line grid.
//
// conformance: semantics/ip-filter-precision
func TestLokiScanReferenceMatchesLokiOnTheLineGrid(t *testing.T) {
	cases := ipLineGridCases()
	b, _ := os.ReadFile("../translate/testdata/ip-line-filter-loki.txt")
	ans := strings.TrimSpace(string(b))
	if len(ans) != len(cases) {
		t.Fatal(len(ans), len(cases))
	}
	diff := map[string]int{}
	ex := map[string][]string{}
	for i, c := range cases {
		want, _ := lokiIPLabelFilter(c.pattern, c.line, true, false)
		if want != (ans[i] == '1') {
			k := fmt.Sprintf("ref=%v", want)
			diff[k]++
			if len(ex[k]) < 10 {
				ex[k] = append(ex[k], fmt.Sprintf("ip(%q) on %q", c.pattern, c.line))
			}
		}
	}
	t.Logf("%d cases", len(cases))
	if len(diff) != 0 {
		t.Errorf("the reference differs from Loki: %v, e.g. %v", diff, ex)
	}
}
