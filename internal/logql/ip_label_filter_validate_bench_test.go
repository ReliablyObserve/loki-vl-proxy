package logql

import "testing"

// The validation of a log query with an ip() label filter and a plain one.
func BenchmarkParseAndValidateIPLabelFilter(b *testing.B) {
	q := `{app="a"} | addr=ip("10.0.0.0/8") | status="500" |= "x"`
	b.ReportAllocs()
	for b.Loop() {
		if err := ParseAndValidate(q); err != nil {
			b.Fatal(err)
		}
	}
}
