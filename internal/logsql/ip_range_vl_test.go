package logsql

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// ipRangeVerdicts are VictoriaLogs' own verdicts (its parser, lib/logstorage ParseQuery, on v1.49.0 and v1.53.0) on
// the IP address spellings an ipv4_range or ipv6_range filter is written with: compressed and expanded IPv6, upper
// case, IPv4-mapped and IPv4-embedded forms (::ffff:a.b.c.d, ::a.b.c.d, 64:ff9b::a.b.c.d), zone ids, IPv4 written
// to ipv6_range and IPv6 written to ipv4_range, leading zeros and CIDR masks, each as the one argument (an address
// or a CIDR) and as the two bounds of the filter, quoted and bare. Recorded with the VictoriaLogs oracle
// (test/oracle/vl check): LVP_RUN_ORACLES=1 go test ./internal/logsql -run TestRecordIPRangeVerdicts records them again.
const ipRangeVerdicts = "testdata/ip-range-vl.jsonl"

type ipRangeVerdict struct {
	Query string `json:"q"`
	V149  bool   `json:"v1.49.0"`
	V153  bool   `json:"v1.53.0"`
}

// ipRangeAddresses are the spellings of the grid.
var ipRangeAddresses = []string{
	"::1", "::", "1::", "0::", "0::/80", "::/0", "::/128", "::/129", "::/-1", "::/+1", "::/", "/64", "",
	"::ffff:1.2.3.4", "::FFFF:1.2.3.4", "::ffff:255.255.255.255", "::ffff:0.0.0.0", "::ffff:1.2.3.4/112", "::ffff:1.2.3.4/129",
	"::ffff:1.2.3.4/0", "::ffff:1.2.3", "::ffff:1.2.3.4.5", "::ffff:01.2.3.4", "::ffff:1.2.3.256", "::ffff:1.2.3.4%eth0",
	"::1.2.3.4", "::1.2.3.4/96", "64:ff9b::1.2.3.4", "64:ff9b::1.2.3.4/96", "64:ff9b::102:304", "1:2:3:4:5:6:1.2.3.4",
	"1:2:3:4:5:6:7:1.2.3.4", "0000:0000:0000:0000:0000:ffff:255.255.255.255", "0000:0000:0000:0000:0000:ffff:255.255.255.255/128",
	"fe80::1%eth0", "fe80::1%", "fe80::1%0", "fe80::1%eth0/64", "ABCD::EF", "AbCd::eF", "2001:db8::", "2001:DB8::1",
	"2001:db8:0:0:0:0:0:1", "2001:0db8:0000:0000:0000:0000:0000:0001", "2001:db8::1/64", "2001:db8::1/064",
	"2001:db8::1/6_4", "2001:db8::1/_", "2001:db8::1/64/8", "2001:db8::1/ 64", "2001:db8::1/128", "2001:db8::1/1_28",
	"1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8:9", "1::2::3", ":::", "::g", "1:2", "1:", ":1", "12345::", "[::1]", " ::1", "::1 ",
	"1.2.3.4", "1.2.3.4/32", "1.2.3.4/33", "1.2.3.4/0", "1.2.3.4/032", "1.2.3.4/3_2", "0.0.0.0/0", "255.255.255.255",
	"01.2.3.4", "001.2.3.4", "1.2.3.04", "0001.2.3.4", "1.2.3", "1.2.3.4.5", "256.1.1.1", "1.2.3.", ".1.2.3", "1..2.3",
	"+1.2.3.4", "1.2.3.-4", "1.2.3.1a", "1.2.1a.4", "1a.2.3.4", "1.2.3.1:", "1.2.3.1 ", "1.2.3.1\x00", "1.2.3.0a", "1.2.3.9z", "1.2.3.4%eth0", "1.1.1.1/", "٣.٣.٣.٣", "::٣",
}

// ipRangeContexts are the places an address is written; %s is an address spelling, %q the same quoted.
var ipRangeContexts = []string{
	`x:ipv6_range(%q)`, `x:ipv6_range(%q, %q)`, `x:ipv6_range(%s)`, `x:ipv6_range(%s, %s)`,
	`x:ipv4_range(%q)`, `x:ipv4_range(%q, %q)`, `x:ipv4_range(%s)`, `x:ipv4_range(%s, %s)`,
}

func ipRangeQueries() []string {
	var qs []string
	seen := map[string]bool{}
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			qs = append(qs, q)
		}
	}
	for _, c := range ipRangeContexts {
		for _, a := range ipRangeAddresses {
			if a == "" && strings.Contains(c, "(%s") {
				// a bare empty argument is a trailing comma or an empty list, which VictoriaLogs reads and
				// the proxy's parser does not (every function filter, not the IP ones): outside this grid
				continue
			}
			if strings.Count(c, "%") == 1 {
				add(fmt.Sprintf(c, a))
				continue
			}
			add(fmt.Sprintf(c, a, a))
			// a bound that is a CIDR or the other family
			add(fmt.Sprintf(c, "::", a))
			add(fmt.Sprintf(c, a, "::ffff:255.255.255.255"))
			add(fmt.Sprintf(c, "1.2.3.4", a))
		}
	}
	return qs
}

func readIPRangeVerdicts(t *testing.T) []ipRangeVerdict {
	t.Helper()
	f, err := os.Open(ipRangeVerdicts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var rows []ipRangeVerdict
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r ipRangeVerdict
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// The proxy's LogsQL parser reads the address of an ipv4_range or ipv6_range filter the way VictoriaLogs does: it
// accepts every spelling VictoriaLogs accepts (an IPv4-mapped ::ffff:255.255.255.255, which the lowering of
// ip("0::/80") writes, an IPv4 address given to ipv6_range, a zone id, a leading zero in an IPv4 octet, an
// underscore in a mask) and rejects every spelling it rejects.
//
// conformance: architecture-syntax-package-layering
func TestParseIPRangeMatchesVictoriaLogs(t *testing.T) {
	rows := readIPRangeVerdicts(t)
	want := ipRangeQueries()
	if len(rows) != len(want) {
		t.Fatalf("%d recorded verdicts, the grid has %d queries: record them again", len(rows), len(want))
	}
	accepted := 0
	for i, r := range rows {
		if r.Query != want[i] {
			t.Fatalf("verdict %d is for %s, the grid has %s: record them again", i, r.Query, want[i])
		}
		if r.V149 != r.V153 {
			t.Errorf("%s: VictoriaLogs v1.49.0 accepts=%v, v1.53.0 accepts=%v: the grid is for one reading", r.Query, r.V149, r.V153)
		}
		if r.V153 {
			accepted++
		}
		if _, err := Parse(r.Query); (err == nil) != r.V153 {
			t.Errorf("%s: VictoriaLogs accepts=%v, the proxy's parser: %v", r.Query, r.V153, err)
		}
	}
	if accepted < 200 || accepted > len(rows)-200 {
		t.Errorf("%d of %d queries accepted: the grid no longer holds both verdicts", accepted, len(rows))
	}
}

// TestRecordIPRangeVerdicts records VictoriaLogs' verdicts of the grid when LVP_RUN_ORACLES=1.
func TestRecordIPRangeVerdicts(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	dir, err := filepath.Abs("../../test/oracle/vl")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	queries := ipRangeQueries()
	inputs := filepath.Join(work, "inputs.jsonl")
	var b strings.Builder
	for i, q := range queries {
		line, _ := json.Marshal(map[string]any{"id": fmt.Sprint(i), "kind": "ip", "q": q})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(inputs, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	accept := map[string][]bool{}
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		modfile := "go." + v + ".mod"
		if v == "v1.53.0" {
			modfile = ""
		}
		bin, err := oraclerun.Build(dir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, "verdicts-"+v+".jsonl")
		if err := oraclerun.Run(bin, "check", "-in", inputs, "-out", out); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var vd struct {
				Accept bool `json:"accept"`
			}
			if err := json.Unmarshal([]byte(line), &vd); err != nil {
				t.Fatal(err)
			}
			accept[v] = append(accept[v], vd.Accept)
		}
		if len(accept[v]) != len(queries) {
			t.Fatalf("%s: %d verdicts for %d queries", v, len(accept[v]), len(queries))
		}
	}
	var out strings.Builder
	for i, q := range queries {
		line, _ := json.Marshal(ipRangeVerdict{Query: q, V149: accept["v1.49.0"][i], V153: accept["v1.53.0"][i]})
		out.Write(line)
		out.WriteByte('\n')
	}
	if err := os.WriteFile(ipRangeVerdicts, []byte(out.String()), 0o644); err != nil { // #nosec G306 -- a testdata file
		t.Fatal(err)
	}
}
