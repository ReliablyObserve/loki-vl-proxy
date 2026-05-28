package proxy

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestExtractCommonBase(t *testing.T) {
	tests := []struct {
		query     string
		wantBase  string
		wantField string
		wantOK    bool
	}{
		{
			query:     `{app="foo"} | filter trace_id != ""`,
			wantBase:  `{app="foo"}`,
			wantField: "trace_id",
			wantOK:    true,
		},
		{
			query:     `{app="foo"} | json | filter trace_id != ""`,
			wantBase:  `{app="foo"}`,
			wantField: "trace_id",
			wantOK:    true,
		},
		{
			query:     `{app="foo"} | unpack_json | filter span_id != ""`,
			wantBase:  `{app="foo"}`,
			wantField: "span_id",
			wantOK:    true,
		},
		{
			query:     `{app="foo"} | logfmt | filter level != ""`,
			wantBase:  `{app="foo"}`,
			wantField: "level",
			wantOK:    true,
		},
		{
			query:  `{app="foo"}`,
			wantOK: false,
		},
		{
			query:  `{app="foo"} | stats by (level) count() as c`,
			wantOK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			base, field, ok := extractCommonBase(tc.query)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if base != tc.wantBase {
				t.Errorf("base=%q want %q", base, tc.wantBase)
			}
			if field != tc.wantField {
				t.Errorf("field=%q want %q", field, tc.wantField)
			}
		})
	}
}

func TestDrilldownBurstCoalescer_CoalescesTwoFields(t *testing.T) {
	callCount := 0
	fireFn := func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
		callCount++
		result := make(map[string]fieldResult, len(fields))
		for _, f := range fields {
			result[f] = fieldResult{
				series: map[string]manualSeriesSamples{
					"": {Metric: map[string]string{}, Samples: nil},
				},
			}
		}
		return result, nil
	}

	c := newDrilldownBurstCoalescer(50, 30)
	key := burstKey{orgID: "default", base: `{app="foo"}`, startSec: 1000, endSec: 2000, stepNs: int64(time.Minute)}

	var wg sync.WaitGroup
	for _, f := range []string{"trace_id", "span_id"} {
		wg.Add(1)
		go func(field string) {
			defer wg.Done()
			_, err := c.Submit(context.Background(), key, field, fireFn)
			if err != nil {
				t.Errorf("Submit(%s): %v", field, err)
			}
		}(f)
	}
	wg.Wait()

	if callCount != 1 {
		t.Errorf("fireFn called %d times, want 1", callCount)
	}
}

func TestDrilldownBurstCoalescer_MaxFieldsSplitsGroups(t *testing.T) {
	callCount := 0
	var mu sync.Mutex
	fireFn := func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		result := make(map[string]fieldResult, len(fields))
		for _, f := range fields {
			result[f] = fieldResult{series: map[string]manualSeriesSamples{"": {}}}
		}
		return result, nil
	}

	c := newDrilldownBurstCoalescer(200, 2)
	key := burstKey{orgID: "default", base: `{app="foo"}`, startSec: 1000, endSec: 2000, stepNs: int64(time.Minute)}

	var wg sync.WaitGroup
	for _, f := range []string{"f1", "f2", "f3"} {
		wg.Add(1)
		go func(field string) {
			defer wg.Done()
			c.Submit(context.Background(), key, field, fireFn) //nolint:errcheck
		}(f)
	}
	wg.Wait()

	mu.Lock()
	count := callCount
	mu.Unlock()
	if count < 2 {
		t.Errorf("fireFn called %d times with maxFields=2, 3 fields — want ≥2", count)
	}
}

func TestDrilldownBurstCoalescer_ContextCancellation(t *testing.T) {
	fireFn := func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
		time.Sleep(200 * time.Millisecond)
		return map[string]fieldResult{}, nil
	}

	c := newDrilldownBurstCoalescer(100, 30)
	key := burstKey{orgID: "default", base: `{app="foo"}`, startSec: 1000, endSec: 2000, stepNs: int64(time.Minute)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Submit(ctx, key, "trace_id", fireFn)
	if err == nil {
		t.Error("expected context cancellation error, got nil")
	}
}
