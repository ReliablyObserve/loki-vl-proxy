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

// reservedFieldVerdicts are VictoriaLogs' own verdicts (lib/logstorage ParseQuery, v1.49.0 and v1.53.0) on the
// field names it keeps for itself, _time, _stream, _stream_id and _msg, in lower case and in other cases, bare and
// quoted, at the places a filter, a stream filter and a pipe name a field (reservedFieldGrid). They are the evidence
// of ReservedFieldName and of the other-case names in KeywordFieldName: in lower case _time, _stream and _stream_id
// are VictoriaLogs' own filters bare and quoted, in another case only bare; _msg is the line; every name is a tag in
// a stream filter and a field to the pipes. LVP_RUN_ORACLES=1 records them again (the oracle downloads VictoriaLogs;
// set GOMODCACHE to a fresh directory when the shared cache holds other copies).
const reservedFieldVerdicts = "testdata/reserved-field-names-vl.jsonl"

// reservedFieldGrid is every position with N for the name, for each spelling of each name, bare and quoted.
func reservedFieldGrid() []string {
	positions := []string{
		`N:="x"`, `app:="a" N:="x"`, `app:="a" -N:="x"`, `app:="a" N:~"x"`, `app:="a" N:>5`, `app:="a" N:>=5s`,
		`app:="a" N:*`, `app:="a" (N:="x" or b:="y")`, `app:="a" | filter N:="x"`, `app:="a" | format if (N:*) "<a>" as b`,
		`{N="x"}`, `app:="a" _stream:{N!="x"}`, `app:="a" {N=~"x.*"}`, `app:="a" | fields N`, `app:="a" | delete N`,
		`app:="a" | stats by (N) count()`, `app:="a" | copy a as N`, `app:="a" | extract_regexp "(?P<N>.+)" from a`,
	}
	var grid []string
	for _, name := range []string{"_time", "_TIME", "_Time", "_stream", "_STREAM", "_Stream", "_stream_id", "_STREAM_ID", "_Stream_Id", "_msg", "_MSG"} {
		for _, spelling := range []string{name, `"` + name + `"`} {
			for _, p := range positions {
				if strings.HasPrefix(spelling, `"`) && (strings.Contains(p, "(?P<N>") || strings.Contains(p, "{N")) {
					continue // a quoted name is no group name, nor a tag name in a stream filter
				}
				grid = append(grid, strings.ReplaceAll(p, "N", spelling))
			}
		}
	}
	return grid
}

type reservedFieldVerdict struct {
	Query string `json:"q"`
	V149  bool   `json:"v1.49.0"`
	V153  bool   `json:"v1.53.0"`
}

// looserThanVLReserved are the queries the proxy's LogsQL parser reads where VictoriaLogs rejects them: a field filter
// on _time, _stream or _stream_id (any case bare, lower case quoted), which the parser reads as a field filter.
// The proxy never writes one (the lowering filters the stream tag or quotes the name: ReservedFieldName,
// KeywordFieldName), and the string translator's text with one is VictoriaLogs' parse error, so the difference is
// only in what the parser reads back.
func looserThanVLReserved(q string) bool {
	for _, n := range []string{"_time", "_stream_id", "_stream"} {
		for _, w := range []string{n, strings.ToUpper(n), strings.ToUpper(n[:2]) + n[2:]} {
			if strings.Contains(q, w+":") || strings.Contains(q, `"`+n+`":`) {
				return true
			}
		}
	}
	return strings.Contains(q, "_Stream_Id:")
}

// The proxy's LogsQL parser reads the reserved field names the way VictoriaLogs does, but for the registered
// looseness, and the recorded verdicts are the rules ReservedFieldName and KeywordFieldName follow.
//
// conformance: semantics/translator-writes-reserved-field-names
func TestParseReservedFieldNamesMatchVictoriaLogs(t *testing.T) {
	grid := reservedFieldGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordReservedFieldVerdicts(t, grid)
	}
	f, err := os.Open(reservedFieldVerdicts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var rows []reservedFieldVerdict
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r reservedFieldVerdict
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if len(rows) != len(grid) {
		t.Fatalf("%d recorded verdicts for %d queries: record them again with LVP_RUN_ORACLES=1", len(rows), len(grid))
	}
	accepted := map[string]bool{}
	for i, r := range rows {
		if r.Query != grid[i] {
			t.Fatalf("verdict %d is for %s, not %s: record them again with LVP_RUN_ORACLES=1", i, r.Query, grid[i])
		}
		if r.V149 != r.V153 {
			t.Errorf("%s: VictoriaLogs v1.49.0 accepts=%v, v1.53.0 accepts=%v", r.Query, r.V149, r.V153)
		}
		accepted[r.Query] = r.V153
		_, err := Parse(r.Query)
		switch {
		case r.V153 && err != nil:
			t.Errorf("%s: VictoriaLogs accepts it, the proxy's parser rejects it: %v", r.Query, err)
		case !r.V153 && err == nil && !looserThanVLReserved(r.Query):
			t.Errorf("%s: VictoriaLogs rejects it, the proxy's parser accepts it", r.Query)
		}
	}
	// The rules the lowering follows, as VictoriaLogs reads them: a field filter on the lower-case name is rejected
	// bare and quoted, on another case of _time, _stream and _stream_id bare only, and _msg in any case is a field;
	// a stream filter and the pipes read every name.
	for _, name := range []string{"_time", "_stream", "_stream_id", "_TIME", "_Stream", "_STREAM_ID", "_msg", "_MSG"} {
		filter := fmt.Sprintf(`app:="a" %s:="x"`, name)
		quoted := fmt.Sprintf(`app:="a" "%s":="x"`, name)
		special := name != "_msg" && name != "_MSG"
		if accepted[filter] == special {
			t.Errorf("%s: VictoriaLogs accepts=%v", filter, accepted[filter])
		}
		if want := !special || !ReservedFieldName(name); accepted[quoted] != want {
			t.Errorf("%s: VictoriaLogs accepts=%v, want %v", quoted, accepted[quoted], want)
		}
		if KeywordFieldName(name) != (special && !ReservedFieldName(name)) {
			t.Errorf("KeywordFieldName(%s) = %v", name, KeywordFieldName(name))
		}
		for _, q := range []string{fmt.Sprintf(`{%s="x"}`, name), fmt.Sprintf(`app:="a" | fields %s`, name), fmt.Sprintf(`app:="a" | stats by (%s) count()`, name)} {
			if !accepted[q] {
				t.Errorf("%s: VictoriaLogs rejects it", q)
			}
		}
	}
}

func recordReservedFieldVerdicts(t *testing.T, grid []string) {
	t.Helper()
	dir := filepath.Join("..", "..", "test", "oracle", "vl")
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var b strings.Builder
	for i, q := range grid {
		line, _ := json.Marshal(map[string]string{"id": fmt.Sprint(i), "kind": "reserved", "q": q})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	verdicts := make([]reservedFieldVerdict, len(grid))
	for i, q := range grid {
		verdicts[i].Query = q
	}
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		modfile := ""
		if v == "v1.49.0" {
			modfile = "go.v1.49.0.mod"
		}
		bin, err := oraclerun.Build(dir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, v+".jsonl")
		if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		r, err := os.Open(out)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			var x struct {
				ID     string `json:"id"`
				Accept bool   `json:"accept"`
			}
			if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
				t.Fatal(err)
			}
			var i int
			_, _ = fmt.Sscan(x.ID, &i)
			if v == "v1.49.0" {
				verdicts[i].V149 = x.Accept
			} else {
				verdicts[i].V153 = x.Accept
			}
		}
		_ = r.Close()
	}
	var w strings.Builder
	for _, v := range verdicts {
		line, _ := json.Marshal(v)
		w.Write(line)
		w.WriteByte('\n')
	}
	if err := os.WriteFile(reservedFieldVerdicts, []byte(w.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
