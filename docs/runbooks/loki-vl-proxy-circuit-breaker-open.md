# LokiVLProxyCircuitBreakerOpen

- Signal: `loki_vl_proxy_circuit_breaker_state == 1`.
- Likely causes: repeated upstream failures, timeout storms, backend instability.

## Triage

1. Confirm current backend health and `5xx` trend.
2. Check timeout configuration and recent deployment changes.
3. Confirm no dependent service is flapping.

## Mitigation

- Stabilize backend first; do not only tune breaker values.
- Roll back unsafe change if incident started after rollout.
- Temporarily increase backoff/open duration only to protect backend during recovery.

## Recovery Criteria

- Circuit breaker state returns to `0` (closed).
- No repeated open/close flapping for at least 15m.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for dependency buffering, rollout control, and stable timeout/backoff policies.
