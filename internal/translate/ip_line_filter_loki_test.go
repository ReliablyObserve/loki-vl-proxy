package translate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// ipLineCase is one ip() line filter and one line.
type ipLineCase struct {
	pattern, line string
}

// ipLineCases is the grid the ip() line filter is held to Loki on: every pattern against every address of its
// family (and the v4/v6 crossings), each between every boundary the line can put around it.
func ipLineCases() []ipLineCase {
	v4Patterns := []string{"10.0.0.1", "10.0.0.0/24", "10.0.0.1-10.0.0.5", "192.168.1.1", "10.0.0.0/8"}
	v6Patterns := []string{"::1", "2001:db8::1", "2001:db8::/32", "fe80::1", "::ffff:10.0.0.1"}
	v4 := []string{"10.0.0.1", "10.0.0.12", "110.0.0.12", "10.0.0.255", "10.0.1.1", "10.0.0.3", "192.168.1.1",
		"0010.0.0.1", "10.00.0.1", "10.0.0.01", "256.0.0.1"}
	v6 := []string{"::1", "2001:db8::1", "2001:DB8::1", "2001:db8:0::1", "2001:db8::12", "fe80::1", "::ffff:10.0.0.1",
		"::ffff:a00:1", "1::1", "2001:0db8:0000:0000:0000:0000:0000:0001", "10.0.0.1"}
	before := []string{"", " ", "a", "f", "g", "0", "1", ".", ":", "-", "[", "=", "1.", "::"}
	after := []string{"", " ", "a", "f", "g", "0", "1", ".", ":", "]", "/24", ",", "%eth0", ":80"}
	var out []ipLineCase
	grid := func(patterns, addrs []string) {
		for _, p := range patterns {
			for _, a := range addrs {
				for _, b := range before {
					for _, e := range after {
						out = append(out, ipLineCase{pattern: p, line: "x " + b + a + e + " y"})
					}
				}
			}
		}
	}
	grid(v4Patterns, v4)
	grid(v6Patterns, v6)
	return out
}

const ipLineLokiAnswers = "testdata/ip-line-filter-loki.txt"

// recordIPLineAnswers runs the Loki oracle (test/oracle/loki ipfilter: Loki's exported log.NewIPLineFilter) over
// the grid and writes one character per case: 1 when Loki keeps the line under `|= ip(pattern)`, 0 when not.
func recordIPLineAnswers(t *testing.T, cases []ipLineCase) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(filepath.Join("..", "..", "test", "oracle", "loki"), "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for i, c := range cases {
		if err := enc.Encode(map[string]any{"id": fmt.Sprint(i), "pattern": c.pattern, "line": c.line}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "ipfilter", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	r, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var b strings.Builder
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var v struct {
			Keep  bool   `json:"keep"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		if v.Error != "" {
			t.Fatalf("Loki rejects a pattern of the grid: %s", v.Error)
		}
		if v.Keep {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ipLineLokiAnswers, []byte(b.String()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The ip() line filter regexp (IPLineFilterRegex, which VictoriaLogs runs on the line) keeps a line exactly when
// Loki's ip() line filter keeps it, on the whole grid: Loki's answers are recorded with the Loki oracle
// (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/ip-filter-precision
func TestIPLineFilterMatchesLoki(t *testing.T) {
	cases := ipLineCases()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordIPLineAnswers(t, cases)
	}
	b, err := os.ReadFile(ipLineLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	answers := strings.TrimSpace(string(b))
	if len(answers) != len(cases) {
		t.Fatalf("%d recorded answers for %d cases: record them again with LVP_RUN_ORACLES=1", len(answers), len(cases))
	}
	res := map[string]*regexp.Regexp{}
	diffs := map[string][]string{}
	n := 0
	for i, c := range cases {
		re := res[c.pattern]
		if re == nil {
			re = regexp.MustCompile(IPLineFilterRegex(c.pattern))
			res[c.pattern] = re
		}
		loki := answers[i] == '1'
		if got := re.MatchString(c.line); got != loki {
			n++
			diffs[c.pattern] = append(diffs[c.pattern], fmt.Sprintf("%q (Loki %v)", c.line, loki))
		}
	}
	if n > 0 {
		patterns := make([]string, 0, len(diffs))
		for p := range diffs {
			patterns = append(patterns, p)
		}
		sort.Strings(patterns)
		var msg strings.Builder
		for _, p := range patterns {
			d := diffs[p]
			fmt.Fprintf(&msg, "ip(%q): %d differences, e.g. %s\n", p, len(d), strings.Join(d[:min(4, len(d))], ", "))
		}
		t.Fatalf("%d of %d lines differ from Loki:\n%s", n, len(cases), msg.String())
	}
}
