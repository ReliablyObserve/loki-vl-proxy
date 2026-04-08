# LokiVLProxyClientBadRequestBurst

- Signal: `bad_request` client errors above 2 req/s on an endpoint for 10m.
- Likely causes: invalid query syntax, incompatible client behavior, automated retries with malformed requests.

## Triage

1. `sum(rate(loki_vl_proxy_client_errors_total{reason="bad_request"}[5m])) by (client, endpoint)`
2. Check query-length outliers and endpoint-specific request spikes.
3. Inspect logs for parser/translation errors and request normalization rejects.

## Mitigation

- Contact top offending client owners with example failing queries.
- Add client-side validation and exponential backoff.
- Apply temporary traffic controls for abusive clients.

## Recovery Criteria

- bad request rate drops below alert threshold.
- retry-induced traffic amplification is no longer observed.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for rate-limit policy and client isolation strategy.
