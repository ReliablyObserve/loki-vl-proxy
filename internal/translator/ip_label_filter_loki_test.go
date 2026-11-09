package translator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// ipLabelCase is one ip() label filter pattern and one label value.
type ipLabelCase struct{ pattern, value string }

// ipLabelCases is the grid the ip() label filter is held to Loki on: every pattern against every address of its
// family (and the v4/v6 crossings), each between every boundary the value can put around it, and the value
// without a boundary (the address alone is the whole value).
func ipLabelCases() []ipLabelCase {
	v4Patterns := []string{"10.0.0.1", "10.0.0.0/24", "10.0.0.1-10.0.0.5", "192.168.1.1", "10.0.0.0/8",
		"10.0.0.0/7", "9.255.255.250-11.0.0.5", "8.0.0.0-13.255.255.255", "0.0.0.0/0"}
	v6Patterns := []string{"::1", "2001:db8::1", "2001:db8::/32", "fe80::1", "::ffff:10.0.0.1"}
	v4 := []string{"10.0.0.1", "10.0.0.12", "110.0.0.12", "10.0.0.255", "10.0.1.1", "10.0.0.3", "192.168.1.1",
		"0010.0.0.1", "10.00.0.1", "10.0.0.01", "256.0.0.1",
		"11.255.255.255", "9.255.255.251", "12.0.0.1", "9.0.0.1", "13.255.255.255", "14.0.0.0"}
	v6 := []string{"::1", "2001:db8::1", "2001:DB8::1", "2001:db8:0::1", "2001:db8::12", "fe80::1", "::ffff:10.0.0.1",
		"::ffff:a00:1", "1::1", "2001:0db8:0000:0000:0000:0000:0000:0001", "10.0.0.1"}
	before := []string{"", " ", "a", "f", "g", "0", "1", ".", ":", "-", "[", "=", "1.", "::", "client ", "ip="}
	after := []string{"", " ", "a", "f", "g", "0", "1", ".", ":", "]", "/24", ",", "%eth0", ":80", ":443", " rest"}
	var out []ipLabelCase
	grid := func(patterns, addrs []string) {
		for _, p := range patterns {
			for _, a := range addrs {
				for _, b := range before {
					for _, e := range after {
						out = append(out, ipLabelCase{pattern: p, value: b + a + e})
					}
				}
			}
		}
	}
	grid(v4Patterns, v4)
	grid(v6Patterns, v6)
	grid(v4Patterns, v6[:4])
	grid(v6Patterns, v4[:4])
	return out
}

const ipLabelLokiAnswers = "testdata/ip-label-filter-loki.txt"

// recordIPLabelAnswers runs the Loki oracle (test/oracle/loki process) over the grid and writes two characters per
// case: whether Loki keeps a line whose label f holds the value under `| f=ip(pattern)` and under `| f!=ip(pattern)`.
func recordIPLabelAnswers(t *testing.T, cases []ipLabelCase) {
	t.Helper()
	var pc []oraclerun.ProcessCase
	for _, c := range cases {
		for _, op := range []string{"=", "!="} {
			pc = append(pc, oraclerun.ProcessCase{Query: fmt.Sprintf(`{app="a"} | f%sip(%q)`, op, c.pattern),
				Labels: map[string]string{"app": "a", "f": c.value}, Line: "x"})
		}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for i, a := range answers {
		if a.Error != "" {
			t.Fatalf("Loki rejects %s: %s", pc[i].Query, a.Error)
		}
		if a.Keep {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ipLabelLokiAnswers, []byte(b.String()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ipv6LabelKnownDifferences is the number of IPv6 grid cases (a pattern, a value and an operator) where the ip()
// label filter keeps fewer lines than Loki: LogsQL has no filter that scans a value for an IPv6 address, so an
// address inside other text, or one the translation cannot tell from text of its form, is left out (`=`) or leaves
// the value out (`!=`). It never keeps a line Loki drops. See semantics/ip-label-filter-embedded-address. The count
// may only fall.
const ipv6LabelKnownDifferences = 12414

// The ip() label filter keeps a line exactly when Loki's ip() label filter keeps it, for every IPv4 pattern of the
// grid and every value: an address alone, an address inside other text ("1.2.3.4:443", "client 1.2.3.4"), leading
// zeros, the other family. An IPv6 pattern keeps a subset of Loki's lines (ipv6LabelKnownDifferences) and never a
// line Loki drops. Both the LogsQL the translator writes and the Go evaluation of it (DropCondition.Matches) are
// held to Loki's answers, recorded with the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/ip-label-filter-embedded-address, semantics/ip-label-filter-missing-label
func TestIPLabelFilterMatchesLoki(t *testing.T) {
	cases := ipLabelCases()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordIPLabelAnswers(t, cases)
	}
	b, err := os.ReadFile(ipLabelLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	answers := strings.TrimSpace(string(b))
	if len(answers) != 2*len(cases) {
		t.Fatalf("%d recorded answers for %d cases: record them again with LVP_RUN_ORACLES=1", len(answers), 2*len(cases))
	}
	filters := map[string]string{}
	v4Diffs, v6Diffs, kept := 0, 0, 0
	var examples []string
	for i, c := range cases {
		for j, neg := range []bool{false, true} {
			key := fmt.Sprint(c.pattern, neg)
			filter, ok := filters[key]
			if !ok {
				if filter, ok = ipLabelFilter("f", c.pattern, neg); !ok {
					t.Fatalf("ip(%q) not translated", c.pattern)
				}
				filters[key] = filter
			}
			dc, err := NewIPLabelFilterCondition("f", c.pattern, neg)
			if err != nil {
				t.Fatal(err)
			}
			loki := answers[2*i+j] == '1'
			if loki {
				kept++
			}
			logsQL, goEval := evalVLFilter(t, filter, c.value), dc.Matches(c.value)
			if logsQL != goEval {
				t.Fatalf("ip(%q) negate=%v on %q: LogsQL %v, Go evaluation %v\n%s", c.pattern, neg, c.value, logsQL, goEval, filter)
			}
			if logsQL && !loki {
				t.Errorf("ip(%q) negate=%v keeps %q, which Loki drops\n%s", c.pattern, neg, c.value, filter)
			}
			if logsQL != loki {
				if strings.Contains(c.pattern, ":") {
					v6Diffs++
				} else {
					v4Diffs++
					if len(examples) < 8 {
						examples = append(examples, fmt.Sprintf("ip(%q) negate=%v on %q (Loki %v)", c.pattern, neg, c.value, loki))
					}
				}
			}
		}
	}
	t.Logf("%d cases x 2 operators: %d IPv4 and %d IPv6 differ from Loki; Loki keeps %d", len(cases), v4Diffs, v6Diffs, kept)
	if v4Diffs != 0 {
		t.Errorf("%d IPv4 cases differ from Loki, e.g. %v", v4Diffs, examples)
	}
	switch {
	case v6Diffs > ipv6LabelKnownDifferences:
		t.Errorf("IPv6 cases that differ from Loki grew from %d to %d", ipv6LabelKnownDifferences, v6Diffs)
	case v6Diffs < ipv6LabelKnownDifferences:
		t.Errorf("IPv6 cases that differ from Loki fell from %d to %d: lower ipv6LabelKnownDifferences", ipv6LabelKnownDifferences, v6Diffs)
	}
}
