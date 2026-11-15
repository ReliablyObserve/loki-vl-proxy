package casefixtures

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

// Config is one run of the seed command.
type Config struct {
	Target Target
	// Span and Every are the backlog's reach and the tick's interval.
	Span, Every time.Duration
	// Wait bounds the wait for both sides to count the same lines of a stream.
	Wait time.Duration
	// Follow keeps writing one tick per Every after the backlog until the context ends; Lag is how long past a
	// tick's time it is written (the clock must not be behind the timestamp).
	Follow bool
	Lag    time.Duration
}

// Choose keeps the streams whose names are listed (comma separated); an empty list keeps every stream.
func Choose(all []Stream, only string) []Stream {
	if strings.TrimSpace(only) == "" {
		return all
	}
	want := map[string]bool{}
	for _, n := range strings.Split(only, ",") {
		want[strings.TrimSpace(n)] = true
	}
	var out []Stream
	for _, s := range all {
		if want[s.Name] {
			out = append(out, s)
		}
	}
	return out
}

// Seed writes the backlog of every stream to both sides at the instant at, then waits for each stream to be counted
// equally by both. It keeps going after a failure and returns the streams that failed, each with the reason: a
// stream written to one side only is named, because lines on one side only turn every case selecting it into
// data_differs.
func Seed(ctx context.Context, cfg Config, streams []Stream, at time.Time, out io.Writer) []string {
	ticks := Ticks(at, cfg.Span, cfg.Every)
	var failed []string
	pushed := map[string]bool{}
	for _, s := range streams {
		if err := cfg.Target.Push(ctx, s, ticks); err != nil {
			failed = append(failed, err.Error())
			continue
		}
		pushed[s.Name] = true
		_, _ = fmt.Fprintf(out, "pushed %s: %d ticks of %d lines\n", s.Name, len(ticks), len(s.Entries))
	}
	cfg.Target.Flush(ctx)
	for _, s := range streams {
		if !pushed[s.Name] {
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, cfg.Wait)
		l, v, err := cfg.Target.WaitReady(wctx, s, at, cfg.Span+time.Minute, float64(len(ticks)*len(s.Entries)))
		cancel()
		if err != nil {
			failed = append(failed, fmt.Sprintf("stream %s is not ready: %v", s.Name, err))
			continue
		}
		_, _ = fmt.Fprintf(out, "ready %s: Loki %.0f, VictoriaLogs %.0f lines\n", s.Name, l, v)
	}
	return failed
}

// Follow writes one tick per interval after the backlog that ended at last, until the context ends.
func Follow(ctx context.Context, cfg Config, streams []Stream, last time.Time, errOut io.Writer) {
	next := last.Add(cfg.Every)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next.Add(cfg.Lag))):
		}
		for _, s := range streams {
			if err := cfg.Target.Push(ctx, s, []time.Time{next}); err != nil && ctx.Err() == nil {
				_, _ = fmt.Fprintln(errOut, "seed:", err)
			}
		}
		next = next.Add(cfg.Every)
	}
}

// Run seeds the backlog and, with Follow, keeps seeding; it returns the process exit code.
func Run(ctx context.Context, cfg Config, streams []Stream, out, errOut io.Writer) int {
	at := time.Now()
	failed := Seed(ctx, cfg, streams, at, out)
	for _, f := range failed {
		_, _ = fmt.Fprintln(errOut, "seed:", f)
	}
	if len(failed) > 0 {
		return 1
	}
	if cfg.Follow {
		ticks := Ticks(at, cfg.Span, cfg.Every)
		_, _ = fmt.Fprintln(out, "following: one tick per", cfg.Every, "until interrupted")
		Follow(ctx, cfg, streams, ticks[len(ticks)-1], errOut)
	}
	return 0
}
