# LokiVLProxyHighLatency

- Signal: `query_range` p99 above 10s for 5m.
- Likely causes: backend slowness, cache collapse, high-cardinality queries, tenant spikes.

## Triage

1. Compare end-to-end and backend-only latency metrics to isolate bottleneck.
2. `rate(loki_vl_proxy_cache_hits_total[5m])`
3. `rate(loki_vl_proxy_cache_misses_total[5m])`
4. Check top tenants/clients by request duration and query size.

## Mitigation

- Increase replicas/resources for proxy and backend.
- Tune cache TTL/max where freshness allows.
- Limit or reshape expensive query patterns.

## Recovery Criteria

- p99 returns under SLO and stays stable for at least 15m.
- User-facing dashboard and Explore latency normalizes.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for cache policy, autoscaling, and backend-aware capacity planning.
