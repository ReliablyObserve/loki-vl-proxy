# LokiVLProxyBackendHighLatency

- Signal: backend p95 latency above 5s for 5m.
- Likely causes: VictoriaLogs saturation (CPU/IO), heavy scans, storage contention.

## Triage

1. `histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket[5m])) by (le, endpoint))`
2. Correlate slow endpoints with request rate:
   - `sum(rate(loki_vl_proxy_requests_total[5m])) by (endpoint, status)`
3. Check backend resource pressure (CPU, disk IO, memory, queue depth).

## Mitigation

- Reduce expensive query load and tenant bursts.
- Scale backend resources and check storage performance.
- Increase metadata cache TTLs if repeated metadata scans are dominant.

## Recovery Criteria

- backend p95 below threshold for at least 15m.
- user-facing query latency and error rates return to baseline.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for cache tuning and backend-aware autoscaling.
