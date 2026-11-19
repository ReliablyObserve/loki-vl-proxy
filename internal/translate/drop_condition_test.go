package translate

import (
	"slices"
	"testing"
)

// dropConditionValues are the values the condition tests evaluate every
// condition on.
var dropConditionValues = []string{"", "debug", "info", "Debug", "debugx", "a|b", "x"}

// A drop or keep matcher's condition (NewDropCondition) selects what Loki's
// drop and keep matchers select: = and != compare the whole value, =~ and !~
// match the whole value against ^(?:re)$; a regexp that does not compile is an
// error and its condition selects nothing.
//
// conformance: architecture-syntax-package-layering
func TestNewDropConditionMatches(t *testing.T) {
	for _, tc := range []struct {
		op, value string
		keeps     []string
	}{
		{"=", "debug", []string{"debug"}},
		{"!=", "debug", []string{"", "info", "Debug", "debugx", "a|b", "x"}},
		{"=~", "debug|info", []string{"debug", "info"}},
		{"=~", "de.*", []string{"debug", "debugx"}},
		{"!~", "debug|info", []string{"", "Debug", "debugx", "a|b", "x"}},
		{"=~", "", []string{""}},
	} {
		c, err := NewDropCondition("level", tc.op, tc.value)
		if err != nil {
			t.Fatalf("%s %q: %v", tc.op, tc.value, err)
		}
		if c.Operator() != tc.op || c.Operand() != tc.value || c.Field != "level" {
			t.Errorf("%s %q: condition %+v", tc.op, tc.value, c)
		}
		var kept []string
		for _, v := range dropConditionValues {
			if c.Matches(v) {
				kept = append(kept, v)
			}
		}
		if !slices.Equal(kept, tc.keeps) {
			t.Errorf("level%s%q keeps %q, want %q", tc.op, tc.value, kept, tc.keeps)
		}
		if _, ok := c.IPFilter("level"); ok {
			t.Errorf("%s %q: a string condition has an ip filter", tc.op, tc.value)
		}
	}
	for _, op := range []string{"=~", "!~"} {
		c, err := NewDropCondition("level", op, "(")
		if err == nil {
			t.Fatalf("%s %q: no error for a regexp that does not compile", op, "(")
		}
		if c.Matches("(") || c.Matches("") {
			t.Errorf("%s %q: a condition without its regexp selects a value", op, "(")
		}
	}
	if c, _ := NewDropCondition("level", "<", "1"); c.Matches("1") || c.Matches("0") {
		t.Error("an operator no matcher has selects a value")
	}
}

// A label filter's condition (NewLabelFilterCondition) matches the whole value
// as Loki's label filter does; a regexp that does not compile is an error.
//
// conformance: profiles/label-filter-regexp-anchoring
func TestNewLabelFilterConditionMatches(t *testing.T) {
	for _, tc := range []struct {
		op, value string
		keeps     []string
	}{
		{"=~", "de.*", []string{"debug", "debugx"}},
		{"!~", "de.*", []string{"", "info", "Debug", "a|b", "x"}},
		{"=~", "(?i)debug", []string{"debug", "Debug"}},
		{"=", "x", []string{"x"}},
		{"!=", "x", []string{"", "debug", "info", "Debug", "debugx", "a|b"}},
	} {
		c, err := NewLabelFilterCondition("level", tc.op, tc.value)
		if err != nil {
			t.Fatalf("%s %q: %v", tc.op, tc.value, err)
		}
		var kept []string
		for _, v := range dropConditionValues {
			if c.Matches(v) {
				kept = append(kept, v)
			}
		}
		if !slices.Equal(kept, tc.keeps) {
			t.Errorf("level%s%q keeps %q, want %q", tc.op, tc.value, kept, tc.keeps)
		}
	}
	if _, err := NewLabelFilterCondition("level", "=~", "("); err == nil {
		t.Error("no error for a regexp that does not compile")
	}
}

// ipConditionValues are the values the ip() condition table is evaluated on:
// whole IPv4 and IPv6 addresses, VictoriaLogs' lenient IPv4 reading, an address
// inside other text, zones, and colon text with and without address characters.
var ipConditionValues = []string{"", "1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3",
	"11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b",
	"[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}

// An ip() label filter's condition (NewIPLabelFilterCondition) answers what
// the LogsQL of IPLabelFilter answers on VictoriaLogs for each value (the table
// is that answer; internal/translator's TestIPLabelFilterCondition_TextForm
// holds the condition to its VictoriaLogs filter emulation on these values),
// and its IPFilter is that LogsQL. A pattern Loki rejects is an error.
//
// conformance: semantics/ip-filter-json-stats-pushdown
func TestNewIPLabelFilterConditionMatches(t *testing.T) {
	for _, tc := range []struct {
		pattern            string
		keptEq, keptNegate []string
	}{
		{"1.2.3.4", []string{"1.2.3.4", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443"}, []string{"1.2.3.5", "1.2.3.0004", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b", "[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}},
		{"1.2.3.0/24", []string{"1.2.3.4", "1.2.3.5", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443"}, []string{"1.2.3.0004", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b", "[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}},
		{"10.0.0.0/8", []string{"10.1.2.3"}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b", "[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}},
		{"1.2.3.4-1.2.4.9", []string{"1.2.3.4", "1.2.3.5", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443"}, []string{"1.2.3.0004", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b", "[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}},
		{"0.0.0.0/0", []string{"1.2.3.4", "1.2.3.5", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4"}, []string{"1.2.3.0004", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b", "[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}},
		{"2001:db8::/32", []string{"2001:db8::1", "2001:db8:1::1", "2001:0db8::1"}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4", "::1", "fe80::1", "::ffff:1.2.3.4", "x:y:z", "a::b", "1:2", "::"}},
		{"::1", []string{}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "::ffff:1.2.3.4", "x:y:z", "a::b", "1:2", "::"}},
		{"fe80::1", []string{"fe80::1", "fe80::1%eth0"}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "::ffff:1.2.3.4", "x:y:z", "a::b", "1:2", "::"}},
		{"::ffff:1.2.3.4", []string{"::ffff:1.2.3.4"}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "x:y:z", "a::b", "1:2", "::"}},
		{"2001:db8::-2001:db8::ff", []string{"2001:db8::1", "2001:0db8::1"}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4", "2001:db8:1::1", "::1", "fe80::1", "::ffff:1.2.3.4", "x:y:z", "a::b", "1:2", "::"}},
		{"fe80::1%eth0", []string{}, []string{"1.2.3.4", "1.2.3.5", "1.2.3.0004", "1.2.3.4a", "x 1.2.3.4 y", "1.2.3.4:443", "10.1.2.3", "11.2.3.4", "2001:db8::1", "2001:db8:1::1", "2001:0db8::1", "::1", "fe80::1", "fe80::1%eth0", "::ffff:1.2.3.4", "x:y:z", "a::b", "[2001:db8::1]:443", "ab:cd:ef", "1:2", "::", "a:.:b"}},
	} {
		for _, negate := range []bool{false, true} {
			c, err := NewIPLabelFilterCondition("addr", tc.pattern, negate)
			if err != nil {
				t.Fatalf("ip(%q): %v", tc.pattern, err)
			}
			want, op := tc.keptEq, "="
			if negate {
				want, op = tc.keptNegate, "!="
			}
			if c.Field != "addr" || c.Operator() != op || c.Operand() != "ip(\""+tc.pattern+"\")" {
				t.Errorf("ip(%q) negate=%v: condition %+v", tc.pattern, negate, c)
			}
			var kept []string
			for _, v := range ipConditionValues {
				if c.Matches(v) {
					kept = append(kept, v)
				}
			}
			if !slices.Equal(kept, want) && len(kept)+len(want) > 0 {
				t.Errorf("addr %s ip(%q) keeps %q, want %q", op, tc.pattern, kept, want)
			}
			filter, ok := IPLabelFilter("addr", tc.pattern, negate)
			got, gotOK := c.IPFilter("addr")
			if !ok || !gotOK || got != filter.String() {
				t.Errorf("ip(%q) negate=%v: IPFilter %q, %v; want %q", tc.pattern, negate, got, gotOK, filter.String())
			}
		}
	}
	for _, bad := range []string{"999.1.1.1", "1.2.3.4/33", "1.2.3.9-1.2.3.4", "abc", ""} {
		if _, err := NewIPLabelFilterCondition("addr", bad, false); err == nil {
			t.Errorf("ip(%q): no error for a pattern Loki rejects", bad)
		}
	}
}
