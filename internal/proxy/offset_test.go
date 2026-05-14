package proxy

import (
	"testing"
	"time"
)

func TestExtractLogQLOffset(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantOffset time.Duration
		wantQuery  string
		wantErr    bool
	}{
		{
			name:       "no offset",
			input:      `rate({app="nginx"}[5m])`,
			wantOffset: 0,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
		{
			name:       "simple 1h offset",
			input:      `rate({app="nginx"}[5m] offset 1h)`,
			wantOffset: time.Hour,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
		{
			name:       "30m offset on count_over_time",
			input:      `count_over_time({app="nginx"}[5m] offset 30m)`,
			wantOffset: 30 * time.Minute,
			wantQuery:  `count_over_time({app="nginx"}[5m])`,
		},
		{
			name:       "outer aggregation with offset",
			input:      `sum by (level) (count_over_time({app="api"}[5m] offset 1h))`,
			wantOffset: time.Hour,
			wantQuery:  `sum by (level) (count_over_time({app="api"}[5m]))`,
		},
		{
			name:       "negative offset",
			input:      `rate({app="nginx"}[5m] offset -30m)`,
			wantOffset: -30 * time.Minute,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
		{
			name:       "1d offset",
			input:      `count_over_time({app="nginx"}[1h] offset 1d)`,
			wantOffset: 24 * time.Hour,
			wantQuery:  `count_over_time({app="nginx"}[1h])`,
		},
		{
			name:    "multiple different offsets error",
			input:   `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 2h)`,
			wantErr: true,
		},
		{
			name:       "same offset repeated is ok",
			input:      `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 1h)`,
			wantOffset: time.Hour,
			wantQuery:  `rate({app="a"}[5m]) + rate({app="b"}[5m])`,
		},
		{
			name:    "mixed: one vector with offset, one without — must error",
			input:   `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m])`,
			wantErr: true,
		},
		{
			name:    "mixed: offset on inner, bare outer — must error",
			input:   `sum(count_over_time({app="a"}[5m] offset 1h)) / sum(count_over_time({app="b"}[1h]))`,
			wantErr: true,
		},
		{
			name:    "mixed: three vectors, only two with offset — must error",
			input:   `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 1h) + rate({app="c"}[5m])`,
			wantErr: true,
		},
		{
			name:       "no space before offset keyword",
			input:      `rate({app="nginx"}[5m]offset 1h)`,
			wantOffset: time.Hour,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, stripped, err := extractLogQLOffset(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if offset != tt.wantOffset {
				t.Errorf("offset: got %v, want %v", offset, tt.wantOffset)
			}
			if stripped != tt.wantQuery {
				t.Errorf("stripped query:\n  got  %q\n  want %q", stripped, tt.wantQuery)
			}
		})
	}
}
