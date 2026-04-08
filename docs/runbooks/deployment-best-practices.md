# Deployment And Scaling Best Practices

Use these defaults to reduce incident frequency and keep recovery time short.

## Topology

- Run at least `2` replicas in production.
- Use `podDisruptionBudget.minAvailable: 1`.
- Keep backend and proxy in low-latency network zones when possible.
- Enable `peerCache.enabled=true` for multi-replica fleets.

## Resource Sizing

- Start with:
  - requests: `200m CPU`, `256Mi memory`
  - limits: `1000m CPU`, `1Gi memory`
- Increase memory first when cache miss spikes follow OOM pressure.
- Keep `goMemLimitPercent` enabled and set explicit `goMemLimit` in tightly constrained clusters.

## Cache Strategy

- Keep short TTLs for live query paths (`query`, `query_range`) where freshness matters.
- Use longer TTLs for metadata paths (`labels`, `detected_fields`, `detected_field_values`).
- For larger working sets, run `StatefulSet` + persistent `disk-cache-path`.
- Monitor cache hit ratio and tune `cache-max` before scaling backend blindly.

## Autoscaling

- Enable HPA with CPU target around `65-75%`.
- Scale on request rate or latency if custom metrics are available.
- Avoid aggressive downscale windows that cause cache churn.

## Health And Probes

- Keep liveness/readiness on `/ready`.
- Add synthetic e2e probes that execute a real lightweight query path, not only `/ready`.
- Alert separately on backend p95 latency and backend-unreachable signals.

## Rollout Safety

- Use rolling updates with max unavailable `0` where possible.
- Gate rollout by:
  - error-rate increase
  - p95/p99 latency increase
  - backend `502` burst
- Roll back quickly if all three regress simultaneously.

## Multi-Tenant Hardening

- Keep tenant map explicit and audited.
- Cap multi-tenant fanout and merged payload limits.
- Watch tenant-level error and latency histograms to catch noisy tenants early.

## Recommended SRE Checklist

1. `replicaCount >= 2`, PDB enabled.
2. HPA enabled with conservative downscale.
3. Cache policy reviewed for query vs metadata endpoints.
4. Backend p95 and proxy p99 alerts enabled.
5. Synthetic e2e probe running every 1-5 minutes.
6. Runbooks linked in alert annotations and tested in game days.
