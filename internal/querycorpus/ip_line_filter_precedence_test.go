package querycorpus_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// An IPv4 ip() line filter of a pattern without a fixed octet is an OR of regexps. Written beside the stream filter and
// the other line filters of a query it must stay one AND operand: `sel a OR b x` reads as `(sel AND a) OR (b AND x)`,
// which keeps rows of other streams. The string translator joins the filters of a query with spaces.
// conformance: semantics/ip-line-filter-ipv4-like-loki
func TestIPLineFilterWithoutLiteralIsOneOperand(t *testing.T) {
	for _, q := range []string{
		`{app="a"} |= ip("0.0.0.0/0") |= "x"`,
		`{app="a"} |= "x" |= ip("0.0.0.0/0")`,
		`{app="a"} |= ip("0.0.0.0/0")`,
		`{app="a"} != ip("0.0.0.0/0") |= "x"`,
		`{app="a"} |= ip("9.0.0.0-100.255.255.255") |= "x"`,
		`{app="a"} |= ip("10.0.0.0/24") |= "x"`,
	} {
		text, err := translator.TranslateLogQL(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		parsed, err := logsql.Parse(text)
		if err != nil {
			t.Fatalf("%s: %s does not parse: %v", q, text, err)
		}
		switch parsed.Filter.(type) {
		case logsql.OrExpr, logsql.AnyOf, logsql.AnyOfLower:
			t.Errorf("%s: the stream filter is one operand of an OR: %s", q, text)
		}
	}
}
