package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// The query complexity flags default to what never refuses a query Loki accepts (the node and depth limits derived
// from the query length limit, the planning budget at its measured default), a set value reaches the proxy's limits,
// 0 turns the budget off, and a negative value is refused at start naming the flag.
//
// conformance: query-complexity-limits
func TestQueryComplexityFlags(t *testing.T) {
	capture := func(t *testing.T, args ...string) (proxy.Config, error) {
		t.Helper()
		sentinel := errors.New("captured config")
		var got runtimeOptions
		err := run(append([]string{"-server.admin-auth-token=test-token"}, args...), func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} }, func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
			got = opts
			return nil, sentinel
		}, nil, nil)
		if err != nil && !errors.Is(err, sentinel) {
			t.Fatalf("%v: %v", args, err)
		}
		return buildProxyConfig(got.proxyCfg)
	}
	must := func(t *testing.T, args ...string) proxy.Config {
		t.Helper()
		cfg, err := capture(t, args...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return cfg
	}
	cfg := must(t)
	if lim := cfg.ExecutionLimits; lim.MaxQueryASTNodes != 0 || lim.MaxQueryNestingDepth != 0 || lim.MaxQueryPlanTime != proxy.DefaultMaxQueryPlanTime {
		t.Fatalf("defaults %+v, want nodes and depth derived (0) and a planning budget of %s", lim, proxy.DefaultMaxQueryPlanTime)
	}
	if proxy.DefaultMaxQueryPlanTime != 10*time.Second {
		t.Errorf("the default budget is %s", proxy.DefaultMaxQueryPlanTime)
	}
	cfg = must(t, "-max-query-ast-nodes=500", "-max-query-nesting-depth=12", "-max-query-plan-time=2s")
	if lim := cfg.ExecutionLimits; lim.MaxQueryASTNodes != 500 || lim.MaxQueryNestingDepth != 12 || lim.MaxQueryPlanTime != 2*time.Second {
		t.Errorf("set values %+v", lim)
	}
	if cfg = must(t, "-max-query-plan-time=0"); cfg.ExecutionLimits.MaxQueryPlanTime >= 0 {
		t.Errorf("-max-query-plan-time=0 reaches the proxy as %s, want the explicit off value (negative)", cfg.ExecutionLimits.MaxQueryPlanTime)
	}
	for _, arg := range []string{"-max-query-ast-nodes=-1", "-max-query-nesting-depth=-1", "-max-query-plan-time=-1s"} {
		if _, err := capture(t, arg); err == nil || !strings.Contains(err.Error(), strings.SplitN(arg, "=", 2)[0]) {
			t.Errorf("%s: %v", arg, err)
		}
	}
}
