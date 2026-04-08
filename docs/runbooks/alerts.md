# Alert Runbooks

These runbooks are written for on-call SRE operation of Loki-VL-proxy in front of VictoriaLogs.

Use this document together with:

- dashboard asset: [`dashboard/loki-vl-proxy.json`](../../dashboard/loki-vl-proxy.json)
- alert rule assets: [`alerting/loki-vl-proxy-prometheusrule.yaml`](../../alerting/loki-vl-proxy-prometheusrule.yaml) and [`alerting/loki-vl-proxy-alerting-rules.yaml`](../../alerting/loki-vl-proxy-alerting-rules.yaml)
- operations guide: [`docs/operations.md`](../operations.md)

## Shared Incident Workflow

1. Confirm blast radius (`single pod`, `single tenant`, or `fleet-wide`).
2. Check readiness and backend health:
   - `curl -fsS http://<proxy>:3100/ready`
   - `curl -fsS http://<victorialogs>:9428/health`
3. Check request status mix from metrics:
   - `loki_vl_proxy_requests_total{status=~"5.."}`
   - `loki_vl_proxy_request_duration_seconds_bucket`
4. Check recent proxy logs for translation, backend, and timeout errors.
5. Apply mitigation, then verify the alert condition clears.

## LokiVLProxyDown

- Signal: `up{job="<proxy-job>"} == 0` for 1m.
- Likely causes: pod crash, readiness failure, network policy, service misrouting.
- First actions:
  - `kubectl -n <ns> get pods -l app.kubernetes.io/name=loki-vl-proxy -o wide`
  - `kubectl -n <ns> describe pod <pod>`
  - `kubectl -n <ns> logs <pod> --since=10m`
  - `kubectl -n <ns> get endpoints <service-name>`
- Mitigation:
  - restart failed pod, roll back recent deployment, or scale replicas.
  - if readiness fails due to backend dependency, treat as backend incident.
- Recovery check:
  - `up == 1`, `/ready` returns `200`, dashboard traffic recovers.

## LokiVLProxyHighErrorRate

- Signal: `5xx` ratio above 5% for 5m.
- Likely causes: backend instability, query translation failures, oversized queries, timeout pressure.
- First actions:
  - inspect top failing endpoints:
    - `sum(rate(loki_vl_proxy_requests_total{status=~"5.."}[5m])) by (endpoint)`
  - inspect backend latency:
    - `histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket[5m])) by (le, endpoint))`
  - inspect translation failures:
    - `rate(loki_vl_proxy_translation_errors_total[5m])`
- Mitigation:
  - increase backend capacity or query limits.
  - reduce abusive client/tenant traffic.
  - if a recent rollout caused change, roll back.
- Recovery check:
  - error ratio below 1% for 15m, no sustained `5xx` bursts.

## LokiVLProxyHighLatency

- Signal: `query_range` p99 above 10s for 5m.
- Likely causes: backend slowness, cache collapse, high-cardinality queries, noisy tenants.
- First actions:
  - compare request vs backend latency to isolate proxy vs backend bottleneck.
  - check cache efficiency:
    - `rate(loki_vl_proxy_cache_hits_total[5m])`
    - `rate(loki_vl_proxy_cache_misses_total[5m])`
  - identify top tenants/clients by latency.
- Mitigation:
  - increase cache TTL/max where acceptable.
  - raise replicas and tune resource limits.
  - constrain expensive query patterns.
- Recovery check:
  - p99 trend returns below SLO and user-facing dashboard latency stabilizes.

## LokiVLProxyBackendUnreachable

- Signal: sustained proxy `502` responses.
- Likely causes: VictoriaLogs outage, DNS/network path failure, auth mismatch.
- First actions:
  - validate backend endpoint and DNS from proxy pod:
    - `kubectl -n <ns> exec <proxy-pod> -- wget -qO- http://<victorialogs>:9428/health`
  - check backend availability and resource saturation.
  - check proxy `-backend` and auth flags/headers.
- Mitigation:
  - restore backend availability or routing.
  - fix auth headers/credentials.
  - fail over to healthy backend if available.
- Recovery check:
  - `502` rate returns to baseline, readiness stable, circuit breaker closes.

## LokiVLProxyCircuitBreakerOpen

- Signal: `loki_vl_proxy_circuit_breaker_state == 1`.
- Likely causes: repeated upstream failures or elevated timeout/error bursts.
- First actions:
  - check backend health and current `5xx` rate.
  - review timeout settings and recent deployment/config changes.
  - verify no downstream dependency is intermittently failing.
- Mitigation:
  - stabilize backend dependency first.
  - temporarily increase backoff/open duration only if needed to protect backend.
  - roll back risky changes that increased failure rate.
- Recovery check:
  - breaker returns to closed (`0`) and remains stable for 15m.

## LokiVLProxyTenantHighErrorRate

- Signal: tenant-level `5xx` ratio above 10% for 5m.
- Likely causes: one tenant issuing incompatible or expensive queries, tenant routing/auth drift.
- First actions:
  - isolate tenant endpoint errors:
    - `sum(rate(loki_vl_proxy_tenant_requests_total{tenant="<tenant>",status=~"5.."}[5m])) by (endpoint)`
  - inspect tenant query profile and query length outliers.
  - verify tenant map/fanout configuration.
- Mitigation:
  - throttle tenant bursts, tune tenant-specific limits, or help tenant fix query patterns.
  - validate tenant auth and mapping correctness.
- Recovery check:
  - tenant error ratio returns under threshold without affecting other tenants.

## LokiVLProxyRateLimiting

- Signal: sustained `reason="rate_limited"` client errors.
- Likely causes: bursty clients, too-low per-client limits, unexpected retry storms.
- First actions:
  - identify top clients:
    - `sum(rate(loki_vl_proxy_client_errors_total{reason="rate_limited"}[5m])) by (client, endpoint)`
  - verify request burst characteristics and retry behavior.
  - check configured `rate-per-second` and `rate-burst`.
- Mitigation:
  - tune rate limits to expected load profile.
  - add client-side backoff/jitter.
  - isolate abusive clients if needed.
- Recovery check:
  - rate-limited errors fall to expected baseline and client SLO recovers.
