package proxy

import (
	"strconv"
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

// TestFormatVLStatsTimestamp covers the reusable helper that normalises any
// Loki/Grafana timestamp representation to the Unix-seconds string required by
// VL's stats_query and stats_query_range endpoints.
func TestFormatVLStatsTimestamp(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantMax int // max digits in result (≤12 = seconds, 19 = nanos)
		want    string
	}{
		{
			name:    "unix seconds passthrough",
			raw:     "1745904000",
			wantMax: 12,
			want:    "1745904000",
		},
		{
			name:    "unix nanoseconds normalised to seconds",
			raw:     strconv.FormatInt(1745904000*int64(time.Second), 10),
			wantMax: 12,
			want:    "1745904000",
		},
		{
			name:    "RFC3339 normalised to seconds",
			raw:     "2025-04-29T10:00:00Z",
			wantMax: 12,
			want:    strconv.FormatInt(time.Date(2025, 4, 29, 10, 0, 0, 0, time.UTC).Unix(), 10),
		},
		{
			name:    "large modern nanosecond timestamp normalised to seconds",
			raw:     strconv.FormatInt(2_000_000_000*int64(time.Second), 10),
			wantMax: 12,
			want:    "2000000000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatVLStatsTimestamp(tc.raw)
			if tc.want != "" && got != tc.want {
				t.Errorf("formatVLStatsTimestamp(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			if len(got) > tc.wantMax {
				t.Errorf("formatVLStatsTimestamp(%q) = %q — looks like nanoseconds (>%d digits)", tc.raw, got, tc.wantMax)
			}
		})
	}
}

// TestFormatVLStatsTimestampNeverNanoseconds is a property guard: for any
// realistic timestamp, the output must always be ≤12 digits (seconds).
func TestFormatVLStatsTimestampNeverNanoseconds(t *testing.T) {
	inputs := []string{
		strconv.FormatInt(time.Now().Unix(), 10),
		strconv.FormatInt(time.Now().UnixNano(), 10),
		time.Now().UTC().Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339Nano),
	}
	for _, raw := range inputs {
		got := formatVLStatsTimestamp(raw)
		if len(got) > 12 {
			t.Errorf("formatVLStatsTimestamp(%q) = %q — must be ≤12 digits (seconds)", raw, got)
		}
	}
}

// TestFormatVLTimestamp_MillisecondRegression is a regression guard for the
// year-58366 bug: Grafana and tests using UnixMilli() send 13-digit millisecond
// timestamps. Before the fix, formatVLTimestamp passed them through unchanged
// and VL's log-query endpoint interpreted them as seconds → year 58366.
func TestFormatVLTimestamp_MillisecondRegression(t *testing.T) {
	anchor := time.Date(2025, 5, 25, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "milliseconds (13-digit) must not produce year 58366",
			input: strconv.FormatInt(anchor.UnixMilli(), 10),
			want:  strconv.FormatInt(anchor.UnixNano(), 10),
		},
		{
			name:  "nanoseconds (19-digit) pass through unchanged",
			input: strconv.FormatInt(anchor.UnixNano(), 10),
			want:  strconv.FormatInt(anchor.UnixNano(), 10),
		},
		{
			name:  "seconds (10-digit) normalize to nanoseconds",
			input: strconv.FormatInt(anchor.Unix(), 10),
			want:  strconv.FormatInt(anchor.UnixNano(), 10),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatVLTimestamp(tc.input)
			if got != tc.want {
				t.Errorf("formatVLTimestamp(%q) = %q, want %q", tc.input, got, tc.want)
			}
			// Guard: output must never be in the millisecond band (13 digits)
			// which VL interprets as seconds → year 58366.
			if len(got) >= 13 && len(got) <= 16 {
				t.Errorf("formatVLTimestamp(%q) returned %q — millisecond/microsecond magnitude causes VL year-overflow", tc.input, got)
			}
		})
	}
}

// TestFormatVLTimestamp_NeverMilliseconds is a property guard: formatVLTimestamp
// must never return a 13-16 digit value (millisecond/microsecond range) because
// VL's log-query endpoint interprets such values as seconds, producing far-future dates.
func TestFormatVLTimestamp_NeverMilliseconds(t *testing.T) {
	now := time.Now()
	inputs := []string{
		strconv.FormatInt(now.Unix(), 10),
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(now.UnixNano(), 10),
		now.UTC().Format(time.RFC3339),
		now.UTC().Format(time.RFC3339Nano),
	}
	for _, raw := range inputs {
		got := formatVLTimestamp(raw)
		if len(got) >= 13 && len(got) <= 16 {
			t.Errorf("formatVLTimestamp(%q) = %q — 13-16 digit output causes VL year-overflow bug", raw, got)
		}
	}
}
