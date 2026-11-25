package proxy

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
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

// The probe of a count over an unpack exists exactly where Loki can fail the query.
//
// conformance: semantics/unpack-parser-error-not-raised
func TestUnpackErrorProbes(t *testing.T) {
	for q, want := range map[string]int{
		`count_over_time({a="b"} | unpack [1m])`:                                     1,
		`rate({a="b"} |= "k" | unpack | drop x [1m] offset 5m)`:                      1,
		`bytes_over_time({a="b"} | x="1" | unpack [1m])`:                             1,
		`bytes_rate({a="b"} | unpack |= "k" [1m])`:                                   1,
		`sum by (x) (count_over_time({a="b"} | unpack [1m]))`:                        1,
		`max(count_over_time({a="b"} | unpack [1m]))`:                                1,
		`count_over_time({a="b"} | unpack [1m]) + rate({a="b"} | unpack [1m])`:       2,
		`label_replace(count_over_time({a="b"} | unpack [1m]), "d", "x", "d", ".*")`: 1,
		`sum(count_over_time({a="b"} | unpack [1m]))`:                                0,
		`sum(rate({a="b"} | unpack | keep x [1m]))`:                                  0,
		`count_over_time({a="b"} | unpack | drop __error__, __error_details__ [1m])`: 0,
		`count_over_time({a="b"} | unpack | __error__="" [1m])`:                      0,
		`absent_over_time({a="b"} | unpack [1m])`:                                    0,
		// Another parser beside the unpack: not covered yet (semantics/translator-writes-unpack-as-unpack-json).
		`count_over_time({a="b"} | unpack | json [1m])`:                              0,
		`count_over_time({a="b"}[1m])`:                                               0,
		`sum_over_time({a="b"} | unpack | unwrap x [1m])`:                            0,
	} {
		expr, ok := sharedLogQLExpr(RequestQueryText(q))
		if !ok {
			t.Fatalf("%s does not parse", q)
		}
		if got := len(jsonErrorProbes(expr)); got != want {
			t.Errorf("%s: %d probes, want %d", q, got, want)
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
