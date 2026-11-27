package proxy

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The proxy reads a line an unpack parses as Loki does: the same verdict and, for an error, Loki's details, on the
// recorded lines.
//
// conformance: semantics/unpack-parser-error-not-raised
func TestUnpackLineErrorMatchesRecordedLoki(t *testing.T) {
	b, err := os.ReadFile("testdata/unpack_error_lines.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ Line, Error, Details string }
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) < 20 {
		t.Fatalf("%d recorded lines", len(rows))
	}
	for _, row := range rows {
		got := unpackLineError(row.Line)
		switch {
		case (got != nil) != (row.Error != ""):
			t.Errorf("%q: error %v, Loki %q %q", row.Line, got, row.Error, row.Details)
		case got == nil:
		case strings.Contains(row.Line, `\q`):
			// An invalid escape: both fail the line, with other details. VictoriaLogs reads the line as JSON, so no
			// metric reaches this reading for it (semantics/json-parser-error-hazard-lines-not-marked).
		case got.Error() != row.Details:
			t.Errorf("%q: details %q, Loki %q", row.Line, got, row.Details)
		}
	}
}

// FuzzUnpackLineError holds the reading to the contract on any line: an empty line and a valid JSON object set no
// error, a line that opens with anything but a brace sets Loki's not-an-object error.
//
// conformance: semantics/unpack-parser-error-not-raised
func FuzzUnpackLineError(f *testing.F) {
	for _, s := range []string{``, `{}`, `{"a":"1","_entry":"k"}`, `{"a":"1","_entry":"k"`, `[1]`, `x`, `{"a":"é"}`, `{"a":{"b":[1,"\n"]}}`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		err := unpackLineError(line)
		switch {
		case line == "":
			if err != nil {
				t.Fatalf("empty line: %v", err)
			}
		case line[0] != '{':
			if !errors.Is(err, errUnpackNotObject) {
				t.Fatalf("%q: %v", line, err)
			}
		case json.Valid([]byte(line)) && !strings.Contains(line, `\`):
			if err != nil {
				t.Fatalf("%q is a JSON object: %v", line, err)
			}
		}
	})
}

// The unwrap check reads from a line an unpack parses the labels Loki reads, and its error, on the recorded lines of
// the lowering's grid (internal/queryplan/lower/testdata/unpack-oracles.json, the plain unpack).
//
// conformance: semantics/unwrap-conversion-check-reads-unpack-unlike-loki
func TestUnwrapPackedLabelsMatchRecordedLoki(t *testing.T) {
	b, err := os.ReadFile("../queryplan/lower/testdata/unpack-oracles.json")
	if err != nil {
		t.Fatal(err)
	}
	var families []struct {
		Query string `json:"q"`
		Loki  []struct {
			Labels map[string]string `json:"labels"`
		} `json:"loki"`
	}
	if err := json.Unmarshal(b, &families); err != nil {
		t.Fatal(err)
	}
	// The grid's lines (internal/queryplan/lower unpack_test.go unpackLines).
	lines := []string{`{"a":"1","b":"x","_entry":"hello world"}`, `{"a":"1","b":"x"}`, `{"a":"1","_entry":""}`,
		`{"a":1,"b":true,"c":null,"d":{"e":"f"},"_entry":"L"}`, `not json`, `{"a":"1","_entry":5}`, `{"a":"1","_entry":{"x":"y"}}`,
		`{"_entry":"only"}`, `{"a":"1","n":{"_entry":"z"}}`, `{}`, ``, `{"a":"1","_entry":"say \"hi\" {x}"}`,
		`{"a":"2","_entry":"hello again"}`, `{"a":"hello","_entry":"bye"}`, `{"a":"1","_entry":"{\"z\":\"2\"}"}`}
	if len(families) == 0 || families[0].Query != `{app="s"} | unpack` || len(families[0].Loki) != len(lines) {
		t.Fatalf("the recorded grid changed: %d families", len(families))
	}
	for i, line := range lines {
		labels := map[string]string{"app": "s"}
		if !unwrapParseLine(&logqlpkg.ParserStage{Type: logqlpkg.ParserUnpack}, line, labels, map[string]bool{}, false, false, nil) {
			t.Fatalf("%q: not read", line)
		}
		want := families[0].Loki[i].Labels
		if len(labels) != len(want) {
			t.Errorf("%q: labels %v, Loki %v", line, labels, want)
			continue
		}
		for name, value := range want {
			if labels[name] != value {
				t.Errorf("%q: %s=%q, Loki %q", line, name, labels[name], value)
			}
		}
	}
}
