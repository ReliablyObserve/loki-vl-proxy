package translator

import (
	"net"
	"strings"
	"testing"
)

func TestExtractIPFilterArg(t *testing.T) {
	tests := []struct {
		in      string
		wantArg string
		wantOK  bool
	}{
		{`ip("10.0.0.0/8")`, "10.0.0.0/8", true},
		{`ip("192.168.1.1")`, "192.168.1.1", true},
		{`ip("2001:db8::/32")`, "2001:db8::/32", true},
		{`  ip( "10.0.0.0" )`, "10.0.0.0", true},
		{`notip("10.0.0.0")`, "", false},
		{`ip(10.0.0.0)`, "", false}, // missing quotes
		{`ip("unclosed`, "", false}, // missing closing quote
		{`ip("")`, "", true},        // empty arg is valid
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			arg, _, ok := extractIPFilterArg(tc.in)
			if ok != tc.wantOK {
				t.Errorf("extractIPFilterArg(%q): ok=%v, want %v", tc.in, ok, tc.wantOK)
			}
			if ok && arg != tc.wantArg {
				t.Errorf("extractIPFilterArg(%q): arg=%q, want %q", tc.in, arg, tc.wantArg)
			}
		})
	}
}

func TestExtractIPFilterArg_RestIsParsed(t *testing.T) {
	// After ip("X") VL queries often continue. Verify the rest is returned.
	_, rest, ok := extractIPFilterArg(`ip("10.0.0.1") |~ ".*"`)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !strings.Contains(rest, `|~`) {
		t.Fatalf("expected rest to contain remaining query, got %q", rest)
	}
}

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
		name string
		arg  string
		// substring expected in the output regex (full-regex is fragile to
		// IPv6-normalization quirks)
		contains string
	}{
		{"plain_ipv4", "10.0.0.1", `10\.0\.0\.1`},
		{"ipv4_cidr_8", "10.0.0.0/8", `10\.\d{1,3}`},
		{"ipv4_cidr_invalid_bits", "10.0.0.0/notanumber", `10\.0\.0\.0`},
		{"ipv4_dash_range", "10.0.0.1-10.0.0.250", `\d{1,3}`},
		{"plain_ipv6", "::1", `:`},
		{"ipv6_cidr", "2001:db8::/32", `[0-9a-fA-F:]`},
		{"unparseable", "not-an-ip-at-all", `not-an-ip-at-all`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ipLineFilterToRegex(tc.arg)
			if !strings.Contains(got, tc.contains) {
				t.Errorf("ipLineFilterToRegex(%q): %q does not contain %q", tc.arg, got, tc.contains)
			}
		})
	}
}
