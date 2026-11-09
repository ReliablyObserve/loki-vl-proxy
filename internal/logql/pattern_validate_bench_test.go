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
