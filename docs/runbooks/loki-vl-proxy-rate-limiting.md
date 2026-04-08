# LokiVLProxyRateLimiting

- Signal: sustained `reason="rate_limited"` client errors.
- Likely causes: burst traffic, low client limits, retry storms.

## Triage

1. `sum(rate(loki_vl_proxy_client_errors_total{reason="rate_limited"}[5m])) by (client, endpoint)`
2. Check per-client request volume and retry behavior.
3. Validate configured `rate-per-second` and `rate-burst` values.

## Mitigation

- Tune rate limits to expected client traffic profile.
- Add client-side backoff/jitter and reduce retries.
- Isolate abusive client traffic if needed.

## Recovery Criteria

- Rate-limited errors return to expected baseline.
- Client-facing SLO returns to normal.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for client shaping, limit sizing, and retry-control practices.
