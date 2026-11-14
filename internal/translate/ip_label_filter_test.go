package translate

import (
	"net/netip"
	"strconv"
	"strings"
	"testing"
)

// Loki reads an ip() pattern as a single address, then a CIDR, then a range of two addresses of one family joined by a
// hyphen. A zoned address holds nothing a scan can produce, so the pattern is empty.
// conformance: semantics/ip-label-filter-invalid-pattern
func TestParseLokiIPPattern(t *testing.T) {
	for _, c := range []struct {
		pattern, lo, hi string
		empty, ok       bool
		text            string
	}{
		{"10.0.0.1", "10.0.0.1", "10.0.0.1", false, true, `10\.0\.0\.1`},
		{"1.2.3.4-1.2.3.4", "1.2.3.4", "1.2.3.4", false, true, `1\.2\.3\.4`},
		{"10.0.0.5/24", "10.0.0.0", "10.0.0.255", false, true, `10\.0\.0\.[0-9]{1,3}`},
		{"10.0.0.0/8", "10.0.0.0", "10.255.255.255", false, true, `10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}`},
		{"10.0.0.1-10.0.0.5", "10.0.0.1", "10.0.0.5", false, true, `10\.0\.0\.[0-9]{1,3}`},
		{"10.0.0.1-10.0.2.5", "10.0.0.1", "10.0.2.5", false, true, `10\.0\.[0-9]{1,3}\.[0-9]{1,3}`},
		{"2001:db8::5/32", "2001:db8::", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff", false, true, `:[0-9a-fA-F.]*:`},
		{"::1-::5", "::1", "::5", false, true, `:[0-9a-fA-F.]*:`},
		{"fe80::1%eth0", "fe80::1", "fe80::1", true, true, `:[0-9a-fA-F.]*:`},
		// Rejected: a range that runs backwards or mixes families, a prefix length out of range, and text.
		{"10.0.0.5-10.0.0.1", "", "", false, false, ""},
		{"10.0.0.1-::1", "", "", false, false, ""},
		{"10.0.0.1-", "", "", false, false, ""},
		{"-10.0.0.1", "", "", false, false, ""},
		{"10.0.0.0/33", "", "", false, false, ""},
		{"garbage", "", "", false, false, ""},
		{"", "", "", false, false, ""},
	} {
		p, ok := ParseLokiIPPattern(c.pattern)
		if ok != c.ok {
			t.Errorf("%q: ok = %v, want %v", c.pattern, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if p.Lo != netip.MustParseAddr(c.lo) || p.Hi != netip.MustParseAddr(c.hi) || p.Empty != c.empty {
			t.Errorf("%q: %v-%v empty=%v, want %s-%s empty=%v", c.pattern, p.Lo, p.Hi, p.Empty, c.lo, c.hi, c.empty)
		}
		if got := p.TextOf(); got != c.text {
			t.Errorf("%q: TextOf = %q, want %q", c.pattern, got, c.text)
		}
	}
}

// An ip() label filter drops a line whose label is absent under both operators; = keeps a line whose label holds an
// address of the pattern, != one that holds none. A pattern Loki rejects is refused.
// conformance: semantics/ip-label-filter-missing-label, semantics/ip-label-filter-invalid-pattern
func TestIPLabelFilter(t *testing.T) {
	for _, c := range []struct {
		name, pattern string
		negate        bool
		want          string // the whole text when short, else a prefix
		contains      []string
		absent        []string
	}{
		{"zoned address matches nothing", "fe80::1%eth0", false, "(addr:* -addr:*)", nil, nil},
		{"zoned address under != keeps every line with the label", "fe80::1%eth0", true, "addr:*", nil, nil},
		{"IPv6 address", "::1", false, `(addr:ipv6_range("::1", "::1") addr:~"^.{5,}" addr:~"^(::[0-9a-fA-F]|[0-9a-fA-F]{1,4}:)" -addr:~"^[0-9.]+$")`, nil, nil},
		{"IPv6 CIDR under != requires the label", "2001:db8::/32", true, "", []string{`(addr:* -(addr:ipv6_range("2001:db8::", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff")`, `addr:~":[0-9a-fA-F.]*:"`}, nil},
		{"IPv4 range", "10.0.0.0/8", false, "", []string{"addr:value_type(ipv4)", "addr:ipv4_range(10.0.0.0, 10.255.255.255)", `addr:~"10\\."`}, []string{"addr:*"}},
		{"IPv4 address under != requires the label", "10.0.0.1", true, "", []string{"(addr:* -((addr:value_type(ipv4) addr:ipv4_range(10.0.0.1, 10.0.0.1))"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, ok := IPLabelFilter("addr", c.pattern, c.negate)
			if !ok {
				t.Fatal("refused")
			}
			got := f.String()
			if c.want != "" && got != c.want {
				t.Errorf("filter = %s, want %s", got, c.want)
			}
			for _, s := range c.contains {
				if !strings.Contains(got, s) {
					t.Errorf("filter %s lacks %s", got, s)
				}
			}
			for _, s := range c.absent {
				if strings.Contains(got, s) {
					t.Errorf("filter %s must not hold %s", got, s)
				}
			}
			mustParseLogsQL(t, "* "+got)
			// The memoized answer is the same text, and keyed by field and operator.
			again, ok := IPLabelFilter("addr", c.pattern, c.negate)
			if !ok || again.String() != got {
				t.Errorf("second call: %v, %v", again, ok)
			}
			other, _ := IPLabelFilter("other", c.pattern, c.negate)
			if strings.Contains(other.String(), "addr") {
				t.Errorf("the memoized filter of another field leaked: %s", other)
			}
		})
	}
	for _, pattern := range []string{"bad", "10.0.0.5-10.0.0.1", "10.0.0.0/33", ""} {
		if f, ok := IPLabelFilter("addr", pattern, false); ok || f != nil {
			t.Errorf("ip(%q) must be refused, got %v, %v", pattern, f, ok)
		}
		if f, ok := IPLabelFilter("addr", pattern, true); ok || f != nil {
			t.Errorf("!= ip(%q) must be refused, got %v, %v", pattern, f, ok)
		}
	}
}

// The memo starts over when it is full, and answers the same text after it.
// conformance: semantics/ip-label-filter-invalid-pattern
func TestIPLabelFilterMemoBound(t *testing.T) {
	first, ok := IPLabelFilter("memo", "10.9.9.9", false)
	if !ok {
		t.Fatal("refused")
	}
	for i := 0; i <= ipv4ValuePatternLimit+1; i++ {
		if _, ok := IPLabelFilter("memo", "10.1."+strconv.Itoa(i%250)+"."+strconv.Itoa(i/250), false); !ok {
			t.Fatal("refused")
		}
	}
	again, ok := IPLabelFilter("memo", "10.9.9.9", false)
	if !ok || again.String() != first.String() {
		t.Errorf("after the memo restarted: %v, %v", again, ok)
	}
	ipLabelFiltersMu.Lock()
	n := len(ipLabelFilters)
	ipLabelFiltersMu.Unlock()
	if n > ipv4ValuePatternLimit {
		t.Errorf("the memo holds %d filters, over its bound %d", n, ipv4ValuePatternLimit)
	}
}
