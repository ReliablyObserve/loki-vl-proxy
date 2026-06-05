# Tuning loki-vl-proxy

This document collects operator-facing tuning guidance for knobs whose
"right" value depends on workload shape (Grafana fleet size, dashboard
fanout, backend headroom).

## Rate limits and concurrency

Defaults changed in v1.56.0 from unlimited to conservative values to
protect against single-client request storms. A single noisy Grafana
instance opening many dashboards in parallel could previously drive an
unbounded number of concurrent fanout queries into VictoriaLogs.

The three knobs below are all exposed as `extraArgs.*` in the Helm chart
and as `-<flag>` on the proxy binary.

| Setting                              | Default | Disabled when | Notes                                  |
|--------------------------------------|---------|---------------|----------------------------------------|
| `extraArgs.max-concurrent`           | 64      | 0             | Per-replica in-flight cap. Excess requests are queued briefly, then 429'd. |
| `extraArgs.rate-limit-per-second`    | 50      | 0             | Per-client token bucket refill rate (req/s). Identifies clients by source IP. |
| `extraArgs.rate-limit-burst`         | 100     | 0             | Per-client burst size. Lets dashboard loads burst above the steady-state rate briefly. |

The binary's defaults are 100 / 50 / 100; the chart ships 64 / 50 / 100
(one tighter in-flight cap to leave backend headroom for a small Grafana
fleet). The chart historically shipped `0/0/0` (unlimited), which is why
this is a chart-only BREAKING change.

### Tuning guidance

Tune based on:

- **Number of Grafana / dashboard replicas calling the proxy.** Each
  Grafana instance counts as a distinct client for the per-client rate
  limit. A fleet of 4 Grafanas, each opening a 12-panel dashboard, can
  briefly demand 48 concurrent queries before the in-flight cap kicks
  in.
- **Average and p99 backend query latency.** Higher latency means each
  in-flight slot is held longer; raise `max-concurrent` to maintain
  throughput, or accept queuing.
- **Backend CPU/memory headroom for fanout.** `max-concurrent` is the
  upper bound on simultaneous load you pass through to VictoriaLogs from
  one proxy replica. Multiply by replica count to size the backend.
- **Proxy replica count.** `max-concurrent` is per-replica. Two replicas
  at 64 each = 128 concurrent in-flight in the worst case.

### Disabling limits

To restore the previous unlimited behavior (NOT recommended):

```bash
helm upgrade loki-vl-proxy charts/loki-vl-proxy \
  --set extraArgs.max-concurrent=0 \
  --set extraArgs.rate-limit-per-second=0 \
  --set extraArgs.rate-limit-burst=0
```

Prefer raising the caps over disabling them. Unlimited fanout interacts
badly with the per-window cache and singleflight coalescer when a
multi-tenant Grafana fleet hits the proxy at once.
