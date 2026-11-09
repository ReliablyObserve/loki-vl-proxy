package logql

import "testing"

// The validation of a log query with pattern line filters: Parse and the semantic checks.
func BenchmarkParseAndValidatePatternFilter(b *testing.B) {
	q := `{app="a"} |> "<_> GET <_> 200 <_>" !> "<_> healthz <_>" or "<_> ready <_>" |= "x"`
	b.ReportAllocs()
	for b.Loop() {
		if err := ParseAndValidate(q); err != nil {
			b.Fatal(err)
		}
	}
}

// The validation of a log query whose line filters Loki builds as runs: several filters of every kind, across
// parsers and a label filter.
func BenchmarkParseAndValidateLineFilterRun(b *testing.B) {
	q := `{app="a"} |= "x" != "y" or "z" |~ "a.*b" | json |> "<_> GET <_>" != ip("10.0.0.0/8") | logfmt | level="error" |~ "(?i)timeout" !~ "debug" or "trace"`
	b.ReportAllocs()
	for b.Loop() {
		if err := ParseAndValidate(q); err != nil {
			b.Fatal(err)
		}
	}
}

// The rejection of a log query with an invalid filter in a run, where the error quotes the run.
func BenchmarkParseAndValidateLineFilterRunError(b *testing.B) {
	q := `{app="a"} |= "x" != "y" or "z" | json |~ "(" | logfmt |= "k" | level="error" |~ "a"`
	b.ReportAllocs()
	for b.Loop() {
		if err := ParseAndValidate(q); err == nil {
			b.Fatal("accepted")
		}
	}
}
