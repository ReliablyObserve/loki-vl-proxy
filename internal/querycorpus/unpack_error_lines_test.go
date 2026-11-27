package querycorpus_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// unpackErrorLinesPath holds Loki's verdict on lines an unpack reads, which the proxy's reading of them is held to
// (internal/proxy TestUnpackLineErrorMatchesRecordedLoki).
const unpackErrorLinesPath = "../proxy/testdata/unpack_error_lines.json"

// TestRecordUnpackErrorLines records the verdicts with the Loki oracle (LVP_RUN_ORACLES=1).
//
// conformance: semantics/unpack-parser-error-not-raised
func TestRecordUnpackErrorLines(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("records with the Loki oracle: LVP_RUN_ORACLES=1")
	}
	lines := []string{``, `{}`, `{ }`, `not json`, `{"a":"1","_entry":"k"`, `["k"]`, `"s"`, `5`, `null`, `[`, `{`, ` `, ` {"a":"1"}`,
		"\t{}", `x{"a":1}`, `{"a":"1"} x`, `{"_entry":"k",}`, `{"a":}`, `{"a":NaN}`, `{"a":"1","b":{"c":}}`,
		`{"a":"1","_entry":"\q"}`, `{"a":"1","_entry":"say \"hi\""}`, `{"a":"é"}`, `{"a" "1"}`, `{"a":"1" "b":"2"}`}
	cases := make([]oraclerun.ProcessCase, len(lines))
	for i, l := range lines {
		cases[i] = oraclerun.ProcessCase{Query: `{app="s"} | unpack`, Labels: map[string]string{"app": "s"}, Line: l}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), cases)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Line    string `json:"line"`
		Error   string `json:"error"`
		Details string `json:"details"`
	}
	rows := make([]row, len(lines))
	for i, a := range answers {
		rows[i] = row{Line: lines[i], Error: a.Labels["__error__"], Details: a.Labels["__error_details__"]}
	}
	b, err := json.MarshalIndent(rows, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unpackErrorLinesPath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
