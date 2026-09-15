package main

import (
	"context"
	"errors"
	"flag"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/dataspan"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func withDetect(t *testing.T, span dataspan.Span, err error) *int {
	t.Helper()
	calls := 0
	orig := detectSpan
	detectSpan = func(ctx context.Context, vlURL string) (dataspan.Span, error) {
		calls++
		return span, err
	}
	t.Cleanup(func() { detectSpan = orig })
	return &calls
}

func TestResolveReference(t *testing.T) {
	wall := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	seeded := dataspan.Span{
		Start: time.Date(2026, 9, 8, 19, 48, 23, 0, time.UTC),
		End:   time.Date(2026, 9, 14, 19, 47, 54, 100889666, time.UTC),
	}

	t.Run("wall clock by default", func(t *testing.T) {
		calls := withDetect(t, seeded, nil)
		span, ref, err := resolveReference(context.Background(), "", "", "http://vl", wall)
		if err != nil || !ref.Equal(wall) || !span.Start.IsZero() || *calls != 0 {
			t.Fatalf("span=%+v ref=%s err=%v calls=%d", span, ref, err, *calls)
		}
	})
	t.Run("auto pins to data end and start", func(t *testing.T) {
		calls := withDetect(t, seeded, nil)
		span, ref, err := resolveReference(context.Background(), "auto", "", "http://vl", wall)
		if err != nil || !ref.Equal(seeded.End) || !span.Start.Equal(seeded.Start) || *calls != 1 {
			t.Fatalf("span=%+v ref=%s err=%v calls=%d", span, ref, err, *calls)
		}
	})
	t.Run("auto start and end detect once", func(t *testing.T) {
		calls := withDetect(t, seeded, nil)
		if _, _, err := resolveReference(context.Background(), "AUTO", "auto", "http://vl", wall); err != nil || *calls != 1 {
			t.Fatalf("err=%v calls=%d", err, *calls)
		}
	})
	t.Run("explicit RFC3339", func(t *testing.T) {
		withDetect(t, seeded, nil)
		span, ref, err := resolveReference(context.Background(), "2026-09-14T19:00:00Z", "2026-09-12T00:00:00Z", "", wall)
		if err != nil || !ref.Equal(time.Date(2026, 9, 14, 19, 0, 0, 0, time.UTC)) || span.Start.Day() != 12 {
			t.Fatalf("span=%+v ref=%s err=%v", span, ref, err)
		}
	})
	t.Run("auto without VictoriaLogs URL", func(t *testing.T) {
		withDetect(t, seeded, nil)
		if _, _, err := resolveReference(context.Background(), "auto", "", "", wall); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("detection failure", func(t *testing.T) {
		withDetect(t, dataspan.Span{}, errors.New("boom"))
		if _, _, err := resolveReference(context.Background(), "auto", "", "http://vl", wall); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("start after reference", func(t *testing.T) {
		withDetect(t, seeded, nil)
		if _, _, err := resolveReference(context.Background(), "2026-09-10T00:00:00Z", "2026-09-11T00:00:00Z", "", wall); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("invalid value", func(t *testing.T) {
		withDetect(t, seeded, nil)
		if _, _, err := resolveReference(context.Background(), "soon", "", "", wall); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestWindowsBeforeDataIncludesLookback(t *testing.T) {
	end := time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC)
	ns := func(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }
	wl := []workload.Workload{{Name: "long_range", Queries: []workload.Query{
		{Name: "log_72h", Params: url.Values{"query": {`{a="1"}`}, "start": {ns(end.Add(-72 * time.Hour))}, "end": {ns(end)}}},
		{Name: "bytes_rate_72h", Params: url.Values{"query": {`sum(bytes_rate({a="1"}[1h]))`}, "start": {ns(end.Add(-72 * time.Hour))}, "end": {ns(end)}, "step": {"3600"}}},
	}}}
	span := dataspan.Span{Start: end.Add(-72*time.Hour - 30*time.Minute), End: end}
	got := windowsBeforeData(wl, span)
	if len(got) != 1 || !strings.Contains(got[0], "long_range/bytes_rate_72h reads 30m0s before") {
		t.Fatalf("got %v", got)
	}
	if windowsBeforeData(wl, dataspan.Span{Start: end.Add(-96 * time.Hour), End: end}) != nil {
		t.Fatal("a 4-day span must contain both windows")
	}
	if windowsBeforeData(wl, dataspan.Span{End: end}) != nil {
		t.Fatal("unknown data start must not report windows")
	}
}

func TestJitterWarning(t *testing.T) {
	span := dataspan.Span{End: time.Now()}
	if jitterWarning(time.Hour, span) == "" {
		t.Fatal("expected a warning when jitter has no data start")
	}
	if jitterWarning(0, span) != "" {
		t.Fatal("no jitter, no warning")
	}
	span.Start = span.End.Add(-time.Hour)
	if jitterWarning(time.Hour, span) != "" {
		t.Fatal("bounded jitter must not warn")
	}
}

func TestNowIsAliasForDataEnd(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	dataEnd := fs.String("data-end", "", "")
	fs.Var(aliasFlag{dataEnd}, "now", "")
	if err := fs.Parse([]string{"--now=auto"}); err != nil {
		t.Fatal(err)
	}
	if *dataEnd != "auto" {
		t.Fatalf("data-end = %q", *dataEnd)
	}
}
