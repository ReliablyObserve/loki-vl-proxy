package translate

import (
	"net"
	"regexp"
	"strings"
	"testing"
)

func TestBuildCIDRRegex(t *testing.T) {
	tests := []struct {
		ip   string
		bits int
		want string
	}{
		{"10.0.0.0", 8, `10\.\d{1,3}\.\d{1,3}\.\d{1,3}`},
		{"10.0.0.0", 16, `10\.0\.\d{1,3}\.\d{1,3}`},
		{"10.0.0.0", 24, `10\.0\.0\.\d{1,3}`},
		{"10.0.0.5", 32, `10\.0\.0\.5`},
		{"not.an.ip", 24, `not\.an\.ip`}, // fallback: not 4 octets — QuoteMeta
	}
	for _, tc := range tests {
		t.Run(tc.ip, func(t *testing.T) {
			got := buildCIDRRegex(tc.ip, tc.bits)
			if got != tc.want {
				t.Errorf("buildCIDRRegex(%q, %d) = %q, want %q", tc.ip, tc.bits, got, tc.want)
			}
		})
	}
}

func TestBuildIPRangeRegex(t *testing.T) {
	tests := []struct {
		start, end string
		want       string
	}{
		{"10.0.0.1", "10.0.0.250", `10\.0\.0\.\d{1,3}`},
		{"10.0.0.1", "10.0.5.250", `10\.0\.\d{1,3}\.\d{1,3}`},
		{"10.0.0.1", "11.0.0.1", `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`},
		{"bad.ip", "10.0.0.1", `bad\.ip`}, // fallback when start isn't 4 octets
	}
	for _, tc := range tests {
		t.Run(tc.start+"-"+tc.end, func(t *testing.T) {
			got := buildIPRangeRegex(tc.start, tc.end)
			if got != tc.want {
				t.Errorf("buildIPRangeRegex(%q, %q) = %q, want %q", tc.start, tc.end, got, tc.want)
			}
		})
	}
}

func TestBuildIPv6CIDRRegex(t *testing.T) {
	parse := func(s string) net.IP {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("ParseIP(%q) returned nil", s)
		}
		return ip
	}

	t.Run("prefix_zero_returns_any_hex_colon", func(t *testing.T) {
		got := buildIPv6CIDRRegex(parse("::"), "0")
		if got != `[0-9a-fA-F:]+` {
			t.Errorf("zero-prefix want %q, got %q", `[0-9a-fA-F:]+`, got)
		}
	})

	t.Run("invalid_prefix_returns_quoted_literal", func(t *testing.T) {
		ip := parse("2001:db8::")
		got := buildIPv6CIDRRegex(ip, "not-a-number")
		want := `2001:db8::`
		// QuoteMeta on the normalized form
		if !strings.Contains(got, "2001") {
			t.Errorf("invalid-prefix fallback should contain literal IP %q, got %q", want, got)
		}
	})

	t.Run("partial_prefix_returns_prefix_plus_hex_colon", func(t *testing.T) {
		ip := parse("2001:db8:abcd:ef::")
		got := buildIPv6CIDRRegex(ip, "32") // /32 → 4 bytes = 2 full groups
		if !strings.HasSuffix(got, `[0-9a-fA-F:]*`) {
			t.Errorf("partial-prefix should end with hex/colon wildcard, got %q", got)
		}
	})
}

func TestIPLineFilterToRegex(t *testing.T) {
	tests := []struct {
		name, arg string
		// lines the regexp matches and lines it does not (Loki's answers, as recorded in ip-line-filter-loki.txt)
		match, miss []string
	}{
		{"plain_ipv4", "10.0.0.1", []string{"x 10.0.0.1 y", "a10.0.0.1b", "10.0.0.1:80"}, []string{"10.0.0.12", "110.0.0.1", "10.0.0.1."}},
		{"ipv4_cidr_8", "10.0.0.0/8", []string{"x 10.200.3.4 y"}, []string{"x 11.0.0.1 y", "x 10.0.0.256 y"}},
		{"ipv4_cidr_invalid_bits", "10.0.0.0/notanumber", []string{"10.0.0.0/notanumber"}, nil},
		{"ipv4_dash_range", "10.0.0.1-10.0.0.250", []string{"x 10.0.0.250 y"}, []string{"x 10.0.0.251 y"}},
		{"plain_ipv6", "::1", []string{"x ::1 y"}, nil},
		{"ipv6_cidr", "2001:db8::/32", []string{"x 2001:db8::1 y"}, nil},
		{"unparseable", "not-an-ip-at-all", []string{"not-an-ip-at-all"}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			re := regexp.MustCompile(IPLineFilterRegex(tc.arg))
			for _, l := range tc.match {
				if !re.MatchString(l) {
					t.Errorf("IPLineFilterRegex(%q) does not match %q", tc.arg, l)
				}
			}
			for _, l := range tc.miss {
				if re.MatchString(l) {
					t.Errorf("IPLineFilterRegex(%q) matches %q", tc.arg, l)
				}
			}
		})
	}
}
