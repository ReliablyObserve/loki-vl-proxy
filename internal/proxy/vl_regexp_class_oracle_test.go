package proxy

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// VictoriaLogs itself (test/oracle/vl store, v1.49.0 and v1.53.0) selects the same lines with each probe regexp
// written with vlNotInRun as with the [^set]* spelling it replaces, and parses both.
//
// conformance: quality/probe-regexp-parse-cost
func TestVLNotInVictoriaLogsSelectsTheSameLines(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs VictoriaLogs' own storage and query code: LVP_RUN_ORACLES=1")
	}
	rows := make([]oraclerun.StoreRow, len(vlNotInLines))
	for i, line := range vlNotInLines {
		rows[i] = oraclerun.StoreRow{Stream: [][2]string{{"app", "notin"}, {"row", strconv.Itoa(i)}}, Fields: [][2]string{{"_msg", line}}}
	}
	var queries []string
	for _, p := range vlNotInPatterns {
		for _, re := range []string{p.before, p.after} {
			queries = append(queries, `{app="notin"} | filter _msg:~`+strconv.Quote(re)+` | fields row`)
		}
	}
	// The detected_level probe's form: the escaped-value regexp behind the backslash filter (detectedLevelKeysQuery).
	queries = append(queries, `{app="notin"} | filter _msg:"\\" _msg:~`+strconv.Quote(detectedLevelEscapedValue)+` | fields row`)
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.53.0", ""}} {
		got, err := oraclerun.VLStore(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, rows, queries)
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range vlNotInPatterns {
			before, after := got[2*i], got[2*i+1]
			if before.Error != "" || after.Error != "" {
				t.Fatalf("%s %s: VictoriaLogs rejects a spelling: %q / %q", v.version, p.name, before.Error, after.Error)
			}
			if b, a := selectedRows(before), selectedRows(after); !slices.Equal(b, a) {
				t.Errorf("%s %s: [^set]* selects rows %v, vlNotInRun %v", v.version, p.name, b, a)
			}
		}
		gated := got[len(got)-1]
		if b, a := selectedRows(got[2]), selectedRows(gated); gated.Error != "" || !slices.Equal(b, a) || len(b) == 0 {
			t.Errorf("%s: the gated escaped-value probe selects rows %v (%q), [^set]* %v", v.version, a, gated.Error, b)
		}
	}
}

func selectedRows(a oraclerun.StoreAnswer) []string {
	var out []string
	for _, r := range a.Rows {
		for _, f := range r {
			if v, ok := strings.CutPrefix(f, "row="); ok {
				out = append(out, v)
			}
		}
	}
	slices.Sort(out)
	return out
}
