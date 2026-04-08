# LokiVLProxyTenantHighErrorRate

- Signal: tenant-level `5xx` ratio above 10% for 5m.
- Likely causes: tenant-specific query patterns, auth/routing drift, per-tenant burst load.

## Triage

1. `sum(rate(loki_vl_proxy_tenant_requests_total{tenant="<tenant>",status=~"5.."}[5m])) by (endpoint)`
2. Inspect tenant query profile and query-length outliers.
3. Validate tenant mapping/fanout configuration.
4. Check whether incident is isolated to one tenant or multiple.

## Mitigation

- Throttle tenant bursts or adjust tenant limits.
- Fix tenant routing/auth mapping.
- Work with tenant to correct expensive or invalid queries.

## Recovery Criteria

- Tenant error ratio clears threshold.
- Other tenants remain stable during and after mitigation.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for tenant isolation, fanout limits, and noisy-tenant controls.
