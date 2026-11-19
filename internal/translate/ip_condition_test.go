package translate

import (
	"strings"
	"testing"
)

// parseVLIPv4 and the test emulation of VictoriaLogs' IPv4 reader are two
// writings of the same rules (the emulation is internal/translator's test
// copy), kept apart so this test checks one against the other.
// conformance: semantics/ip-filter-json-stats-pushdown
func TestParseVLIPv4_MatchesEmulation(t *testing.T) {
	for _, s := range []string{"1.2.3.4", "1.2.3.4a", "1.2.3.0004", "01.2.3.4", "1.2.3.04", "255.255.255.255", "256.1.1.1", "1.2.3.", "1..2.3", "a.b.c.d", "1.2.3.4.5", "1234.1.1.1", "1.2.3.123456789", "1.2.3.1234567890", "1.2.3.:", "1.2.3.0.", "1.2.3.0/", "1.2.3.9 ", "9.9.9.9 ", ""} {
		want, wantOK := vlParseIPv4(s)
		got, gotOK := parseVLIPv4(s)
		if got != want || gotOK != wantOK {
			t.Errorf("%q: %d, %v; emulation %d, %v", s, got, gotOK, want, wantOK)
		}
	}
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
