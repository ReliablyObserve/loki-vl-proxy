package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// exoticNameVerdicts is the file of VictoriaLogs' verdicts on the names its lexer does not join into a word, bare
// and quoted, at the places a filter or a pipe names a field (internal/logsql reads it:
// TestParseFieldNamesMatchVictoriaLogs).
const exoticNameVerdicts = "../logsql/testdata/exotic-field-names-vl.jsonl"

// TestRecordExoticFieldNameVerdicts records the VictoriaLogs oracle's verdicts (both matrix versions) on every place
// x name x spelling, with LVP_RUN_ORACLES=1: a bare exotic name is rejected wherever a filter or a pipe names it,
// the quoted one is read as the field, and a name made of letters, digits and underscores is read either way.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestRecordExoticFieldNameVerdicts(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	names := []string{"、", ";", "@", "³", "Ⅰ", "𝛁", "😀", "ा", "\u200b", "·", "☃", "»", "゠", "⃝", "a.b", "a-b", "é", "日本", "x1", "not"}
	places := []string{
		`a:="b" N:="z"`, `a:="b" (x:="y" and N:="z")`, `a:="b" -N:*`, `a:="b" | filter N:~"x"`, `a:="b" | filter N:>5`,
		`* | copy N as x`, `* | copy x as N`, `* | fields N`, `* | delete N`, `* | unpack_json fields (N)`, `* | stats by (N) count()`,
		`* | format if (N:*) "<x>" as y`, `* | sort by (N)`,
	}
	var queries []string
	for _, p := range places {
		for _, n := range names {
			queries = append(queries, strings.ReplaceAll(p, "N", n), strings.ReplaceAll(p, "N", logsql.QuoteFieldName(n)))
		}
	}
	versions := goldenVersions(t)
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for i, q := range queries {
		if err := enc.Encode(oracleInput{ID: strconv.Itoa(i), Kind: "exotic", Query: q}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	accepted := map[string]map[string]bool{}
	for _, v := range versions {
		file := oracleModfile(v, versions[0])
		modfile := ""
		if file != "go.mod" {
			modfile = file
		}
		bin, err := oraclerun.Build(oracleDir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, "out-"+v+".jsonl")
		if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		for id, vd := range readVerdictFile(t, out) {
			i, _ := strconv.Atoi(id)
			if accepted[queries[i]] == nil {
				accepted[queries[i]] = map[string]bool{}
			}
			accepted[queries[i]][v] = vd.Accept
		}
	}
	o, err := os.Create(exoticNameVerdicts)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(o)
	oenc := json.NewEncoder(w)
	oenc.SetEscapeHTML(false)
	for _, q := range queries {
		row := map[string]any{"q": q}
		for _, v := range versions {
			row[v] = accepted[q][v]
		}
		if err := oenc.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
}

// readVerdictFile reads the oracle's verdicts by input id.
func readVerdictFile(t *testing.T, path string) map[string]oracleVerdict {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	out := map[string]oracleVerdict{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var v oracleVerdict
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		out[v.ID] = v
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
