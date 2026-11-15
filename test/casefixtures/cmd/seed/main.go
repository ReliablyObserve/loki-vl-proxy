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
// The command writes only to the two URLs it is given; it never reads a registry or a checkout. A stream that
// failed on either side is named on stderr and the exit code is 1 (the others are still written).
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
	streams := casefixtures.Choose(casefixtures.Streams(), *only)
	if *list {
		for _, s := range streams {
			fmt.Printf("%s\t%s\t%v\t%s\n", s.Name, s.Route, s.Labels, strings.Join(s.Cases, ","))
		}
		return
	}
	if *loki == "" || *vl == "" {
		fmt.Fprintln(os.Stderr, "seed: -loki and -vl are required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	cfg := casefixtures.Config{
		Target: casefixtures.Target{Loki: *loki, VL: *vl, Tenant: *tenant},
		Span:   *span, Every: *every, Wait: *wait, Follow: *follow, Lag: 2 * time.Second,
	}
	if code := casefixtures.Run(ctx, cfg, streams, os.Stdout, os.Stderr); code != 0 {
		stop()
		os.Exit(code)
	}
}
