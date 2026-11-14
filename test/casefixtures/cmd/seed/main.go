// Command seed writes the log streams the per-case live verifier needs (test/casefixtures) to a Loki and a
// VictoriaLogs, with the same bytes on both, and waits until both sides count the same lines for every stream.
//
//	go run ./test/casefixtures/cmd/seed -loki http://127.0.0.1:13101 -vl http://127.0.0.1:19428 [-follow]
//
// The backlog covers -span before the start at one tick per -every. With -follow the command then keeps writing
// one tick per -every until it is interrupted, so a verifier run started at any time finds data in its window (the
// verifier reads a window that ends -settle, five minutes, before its start). Without -follow the command exits
// after the backlog and the verifier must start within about five minutes of it.
//
// The command writes only to the two URLs it is given; it never reads a registry or a checkout.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/casefixtures"
)

func main() {
	var (
		loki   = flag.String("loki", "", "Loki base URL")
		vl     = flag.String("vl", "", "VictoriaLogs base URL")
		tenant = flag.String("tenant", "0", "X-Scope-OrgID of the Loki writes (VictoriaLogs' default account is 0)")
		span   = flag.Duration("span", casefixtures.DefaultSpan, "how far back the backlog reaches")
		every  = flag.Duration("every", casefixtures.DefaultEvery, "the interval of the ticks")
		follow = flag.Bool("follow", false, "after the backlog, keep writing one tick per interval until interrupted")
		wait   = flag.Duration("wait", 5*time.Minute, "how long to wait for both sides to count the same lines")
		only   = flag.String("only", "", "comma-separated stream names (default: every stream)")
		list   = flag.Bool("list", false, "list the streams and the cases that read them, and exit")
	)
	flag.Parse()
	streams := choose(casefixtures.Streams(), *only)
	if *list {
		for _, s := range streams {
			fmt.Printf("%s\t%s\t%s\t%s\n", s.Name, s.Route, selectorText(s), strings.Join(s.Cases, ","))
		}
		return
	}
	if *loki == "" || *vl == "" {
		fmt.Fprintln(os.Stderr, "seed: -loki and -vl are required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	target := casefixtures.Target{Loki: *loki, VL: *vl, Tenant: *tenant}
	now := time.Now()
	ticks := casefixtures.Ticks(now, *span, *every)
	for _, s := range streams {
		if err := target.Push(ctx, s, ticks); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
		fmt.Printf("pushed %s: %d ticks of %d lines\n", s.Name, len(ticks), len(s.Entries))
	}
	target.Flush(ctx)
	failed := false
	for _, s := range streams {
		wctx, cancel := context.WithTimeout(ctx, *wait)
		want := float64(len(ticks) * len(s.Entries))
		l, v, err := target.WaitReady(wctx, s, now, *span+time.Minute, want)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "seed: stream %s is not ready: %v\n", s.Name, err)
			failed = true
			continue
		}
		fmt.Printf("ready %s: Loki %.0f, VictoriaLogs %.0f lines\n", s.Name, l, v)
	}
	if failed {
		os.Exit(1)
	}
	if !*follow {
		return
	}
	fmt.Println("following: one tick per", *every, "until interrupted")
	next := ticks[len(ticks)-1].Add(*every)
	for {
		wait := time.Until(next.Add(2 * time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		for _, s := range streams {
			if err := target.Push(ctx, s, []time.Time{next}); err != nil {
				fmt.Fprintln(os.Stderr, "seed:", err)
			}
		}
		next = next.Add(*every)
	}
}

func choose(all []casefixtures.Stream, only string) []casefixtures.Stream {
	if only == "" {
		return all
	}
	want := map[string]bool{}
	for _, n := range strings.Split(only, ",") {
		want[strings.TrimSpace(n)] = true
	}
	var out []casefixtures.Stream
	for _, s := range all {
		if want[s.Name] {
			out = append(out, s)
		}
	}
	return out
}

func selectorText(s casefixtures.Stream) string {
	parts := make([]string, 0, len(s.Labels))
	for k, v := range s.Labels {
		parts = append(parts, fmt.Sprintf("%s=%q", k, v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
