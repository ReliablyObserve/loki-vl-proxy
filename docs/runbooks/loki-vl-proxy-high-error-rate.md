# LokiVLProxyHighErrorRate

- Signal: `5xx` ratio above 5% for 5m.
- Likely causes: backend instability, translation failures, timeout pressure, bad rollout.
- Execution limits also produce `5xx`: raw metric row/series limits return `502`,
  the series cap while building a raw metric matrix and `max-concurrent`
  admission rejections return `503`, and a failed binary
  expression join (including output budgets) returns `500`. These are
  intentional rejections of oversized work, not backend faults.

## Triage

1. `sum(rate(loki_vl_proxy_requests_total{status=~"5.."}[5m])) by (endpoint)`
2. `histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket[5m])) by (le, endpoint))`
3. `rate(loki_vl_proxy_translation_errors_total[5m])`
4. Review proxy logs for failing paths and backend status codes. Error messages
   containing `limit exceeded` or `budget exceeded` (and `503` responses with
   `too many concurrent queries`) point to execution-limit rejections; identify
   the client or tenant sending those queries.

## Mitigation

- Stabilize backend or rollback recent proxy/backend config change.
- Throttle abusive traffic if a specific client/tenant is triggering failures.
- Increase backend/proxy capacity if incident is load-driven.

## Recovery Criteria

- Error ratio below 1% for at least 15m.
- No sustained endpoint-level `5xx` burst remains.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for capacity buffers, rollout gates, and query/load controls.
