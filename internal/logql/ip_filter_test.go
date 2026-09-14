package logql

import (
	"fmt"
	"strings"
	"testing"
)

func TestIPFilterPatternValidation(t *testing.T) {
	valid := []string{
		"192.168.1.1", "::1", "::ffff:192.168.1.1", "fe80::1%eth0",
		"0.0.0.0/0", "192.168.1.33/27", "2001:db8::1/127", "::/0",
		"192.168.1.1-192.168.1.1", "192.168.1.1-192.168.2.255",
		"2001:db8::1-2001:db8::ffff", "fe80::1%eth0-fe80::2%eth1",
	}
	invalid := []string{
		"", "not-an-ip", "999.999.999.999", "::gggg", "1.2.3.4/33", "::/129",
		"01.2.3.4", " 1.2.3.4", "1.2.3.4 ", "1.2.3.4/abc",
		"1.2.3.4-1.2.3.3", "::2-::1", "1.2.3.4-::1", "::1-1.2.3.4",
		"1.2.3.4-", "1.2.3.4 - 1.2.3.5", "1.2.3.4-1.2.3.5-1.2.3.6",
	}
	for _, pattern := range valid {
		for _, op := range []string{"|=", "!="} {
			q := fmt.Sprintf(`{app="a"} %s ip(%q)`, op, pattern)
			t.Run(q, func(t *testing.T) {
				if err := ValidateLogQL(q); err != "" {
					t.Fatal(err)
				}
			})
		}
	}
	for _, pattern := range invalid {
		for _, op := range []string{"|=", "!="} {
			q := fmt.Sprintf(`{app="a"} %s ip(%q)`, op, pattern)
			t.Run(q, func(t *testing.T) {
				if err := ValidateLogQL(q); !strings.Contains(err, "ip: invalid pattern:") {
					t.Fatalf("expected IP pattern error, got %q", err)
				}
			})
		}
	}
}

func TestIPFilterValidationPreservesStringLiterals(t *testing.T) {
	for _, q := range []string{
		`{app="a"} |= "ip(bad)"`,
		`{app="a"} |= "ip(\"bad\")"`,
		"{app=\"a\"} |= `ip(\"bad\")`",
		`{app="a"} |~ "ip(bad)"`,
		`{app="a"} | json | message="ip(bad)"`,
		`{app="a"} |= ip("1.2.3.4") |= "ip(bad)"`,
		"{app=\"a\"} |= ip(`2001:db8::/32`)",
	} {
		t.Run(q, func(t *testing.T) {
			if err := ValidateLogQL(q); err != "" {
				t.Fatal(err)
			}
		})
	}
}

func TestIPFilterRejectsUnsupportedLineOperators(t *testing.T) {
	for _, op := range []string{"|~", "!~", "|>", "!>"} {
		q := fmt.Sprintf(`{app="a"} %s ip("1.2.3.4")`, op)
		t.Run(q, func(t *testing.T) {
			if err := ValidateLogQL(q); !strings.Contains(err, "ip: invalid operation") {
				t.Fatalf("expected IP operation error, got %q", err)
			}
		})
	}
}

func TestIPFilterValidationInsideMetrics(t *testing.T) {
	for _, q := range []string{
		`rate({app="a"} |= ip("bad")[5m])`,
		`sum by(app)(count_over_time({app="a"} != ip("::gggg")[5m]))`,
		`sum(rate({app="a"}[5m])) + sum(rate({app="b"} |= ip("bad")[5m]))`,
	} {
		t.Run(q, func(t *testing.T) {
			if err := ValidateLogQL(q); err == "" {
				t.Fatal("expected validation error")
			}
		})
	}
}
