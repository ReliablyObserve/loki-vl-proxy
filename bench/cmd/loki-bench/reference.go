package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/dataspan"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

// aliasFlag lets a second flag name write to the same string value.
type aliasFlag struct{ target *string }

func (a aliasFlag) String() string {
	if a.target == nil {
		return ""
	}
	return *a.target
}

func (a aliasFlag) Set(v string) error {
	*a.target = v
	return nil
}

// detectFunc returns the seeded data span; it is dataspan.DetectVL in production.
type detectFunc func(ctx context.Context, vlURL string) (dataspan.Span, error)

var detectSpan detectFunc = dataspan.DetectVL

// resolveReference turns --data-end / --data-start into the workload reference
// time and the known data span. With no --data-end the wall clock is used and
// the span is unknown (zero) unless --data-start is given.
func resolveReference(ctx context.Context, dataEnd, dataStart, vlURL string, wallNow time.Time) (dataspan.Span, time.Time, error) {
	var span dataspan.Span
	var detected *dataspan.Span
	detect := func() (dataspan.Span, error) {
		if detected != nil {
			return *detected, nil
		}
		if vlURL == "" {
			return dataspan.Span{}, fmt.Errorf("\"auto\" needs --vl or --vl-direct to query VictoriaLogs")
		}
		s, err := detectSpan(ctx, vlURL)
		if err != nil {
			return dataspan.Span{}, fmt.Errorf("detect seeded data span from VictoriaLogs: %w", err)
		}
		detected = &s
		return s, nil
	}

	ref := wallNow
	switch v := strings.TrimSpace(dataEnd); {
	case v == "":
	case strings.EqualFold(v, dataspan.Auto):
		s, err := detect()
		if err != nil {
			return span, time.Time{}, fmt.Errorf("--data-end=auto: %w", err)
		}
		span = s
		ref = s.End
	default:
		t, err := dataspan.ParseTime(v)
		if err != nil {
			return span, time.Time{}, fmt.Errorf("--data-end: %w", err)
		}
		span.End = t
		ref = t
	}

	switch v := strings.TrimSpace(dataStart); {
	case v == "":
	case strings.EqualFold(v, dataspan.Auto):
		s, err := detect()
		if err != nil {
			return span, time.Time{}, fmt.Errorf("--data-start=auto: %w", err)
		}
		span.Start = s.Start
		if span.End.IsZero() {
			span.End = s.End
		}
	default:
		t, err := dataspan.ParseTime(v)
		if err != nil {
			return span, time.Time{}, fmt.Errorf("--data-start: %w", err)
		}
		span.Start = t
	}

	if !span.Start.IsZero() && span.Start.After(ref) {
		return span, time.Time{}, fmt.Errorf("data start %s is after the reference time %s", span.Start.Format(time.RFC3339), ref.Format(time.RFC3339))
	}
	return span, ref, nil
}

// windowsBeforeData lists queries whose evaluated window (start or time minus
// the range-vector lookback) begins before the seeded data, which would time a
// partially empty range on every backend. Each entry names the workload, the
// query and how far before the data start it reads.
func windowsBeforeData(workloads []workload.Workload, span dataspan.Span) []string {
	if span.Start.IsZero() {
		return nil
	}
	minNs := span.Start.UnixNano()
	var out []string
	for _, wl := range workloads {
		for _, q := range wl.Queries {
			if earliest, ok := workload.EarliestEvaluated(q.Params); ok && earliest < minNs {
				out = append(out, fmt.Sprintf("%s/%s reads %s before the seeded data start",
					wl.Name, q.Name, time.Duration(minNs-earliest).Truncate(time.Second)))
			}
		}
	}
	return out
}

// jitterWarning explains why --jitter cannot be bounded to the data, or returns "".
func jitterWarning(jitter time.Duration, span dataspan.Span) string {
	if jitter <= 0 || !span.Start.IsZero() {
		return ""
	}
	return "--jitter without a data start: shifted windows may read before the seeded data; pass --data-start (or --data-end=auto)"
}
