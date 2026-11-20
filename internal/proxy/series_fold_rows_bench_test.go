package proxy

import (
	"fmt"
	"testing"
)

// BenchmarkFoldReadFields reads one VictoriaLogs stats row as the series fold does, for the cost of the row check.
func BenchmarkFoldReadFields(b *testing.B) {
	for _, n := range []int{2, 8} {
		row := `{"_time":"2026-11-20T10:00:00Z"`
		for i := 0; i < n; i++ {
			row += fmt.Sprintf(`,"label_%d":"value-%d-abcdefgh"`, i, i)
		}
		row += `,"__lvp_sid":"7","v":"12.5"}`
		line := []byte(row)
		b.Run(fmt.Sprintf("fields=%d", n+3), func(b *testing.B) {
			d := &foldDecoder{}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := d.readFields(line); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
