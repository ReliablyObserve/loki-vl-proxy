package proxy

import (
	"testing"
	"time"
)

func TestNanosToVLTimestamp(t *testing.T) {
	cases := []struct {
		name  string
		nanos int64
		want  string
	}{
		{
			name:  "typical unix nanoseconds (April 2026)",
			nanos: 1745904000 * int64(time.Second),
			want:  "1745904000",
		},
		{
			name:  "five minutes before epoch anchor",
			nanos: 1745903700 * int64(time.Second),
			want:  "1745903700",
		},
		{
			name:  "zero",
			nanos: 0,
			want:  "0",
		},
		{
			name:  "one second in nanoseconds",
			nanos: int64(time.Second),
			want:  "1",
		},
		{
			name:  "sub-second nanoseconds truncate to seconds",
			nanos: 500 * int64(time.Millisecond),
			want:  "0",
		},
		{
			name:  "large modern timestamp stays in valid seconds range",
			nanos: 2_000_000_000 * int64(time.Second),
			want:  "2000000000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nanosToVLTimestamp(tc.nanos)
			if got != tc.want {
				t.Errorf("nanosToVLTimestamp(%d) = %q, want %q", tc.nanos, got, tc.want)
			}
		})
	}
}

// TestNanosToVLTimestampNotNanoseconds verifies that the output is never in
// nanosecond magnitude — VL stats_query and stats_query_range expect seconds.
func TestNanosToVLTimestampNotNanoseconds(t *testing.T) {
	typicalNow := time.Date(2026, 4, 29, 0, 0, 0, 0, time.UTC).UnixNano()
	got := nanosToVLTimestamp(typicalNow)
	// A nanosecond timestamp would be 19 digits; a second timestamp is 10 digits.
	if len(got) > 12 {
		t.Errorf("nanosToVLTimestamp returned %q — looks like nanoseconds, expected seconds (<=12 digits)", got)
	}
}
