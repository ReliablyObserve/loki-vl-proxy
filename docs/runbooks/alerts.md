# Alert Runbooks

Alert runbooks are split per alert under this directory so each alert annotation links to a dedicated procedure.

## Shared Incident Workflow

1. Confirm blast radius (`single pod`, `single tenant`, or `fleet-wide`).
2. Validate health checks:
   - `curl -fsS http://<proxy>:3100/ready`
   - `curl -fsS http://<victorialogs>:9428/health`
3. Check request failures and latency from metrics. `/metrics` is served on the
   chart's dedicated metrics port (`:9091` by default); the binary serves it only
   with `-server.register-instrumentation=true` (on `-listen`, or on
   `-metrics-listen` when set).
4. Review proxy logs for translation, backend, timeout, or auth errors.
5. Apply mitigation, then verify alert recovery criteria.

## Runbooks

- [Deployment And Scaling Best Practices](deployment-best-practices.md)
- [LokiVLProxyDown](loki-vl-proxy-down.md)
- [LokiVLProxyHighErrorRate](loki-vl-proxy-high-error-rate.md)
- [LokiVLProxyHighLatency](loki-vl-proxy-high-latency.md)
- [LokiVLProxyBackendHighLatency](loki-vl-proxy-backend-high-latency.md)
- [LokiVLProxyBackendUnreachable](loki-vl-proxy-backend-unreachable.md)
- [LokiVLProxyCircuitBreakerOpen](loki-vl-proxy-circuit-breaker-open.md)
- [LokiVLProxyTenantHighErrorRate](loki-vl-proxy-tenant-high-error-rate.md)
- [LokiVLProxyRateLimiting](loki-vl-proxy-rate-limiting.md)
- [LokiVLProxyClientBadRequestBurst](loki-vl-proxy-client-bad-request-burst.md)
- [LokiVLProxyUnexpectedTupleMode and LokiVLProxyDefault2TupleMissing](loki-vl-proxy-grafana-tuple-contract.md)
- [LokiVLProxySystemMetricsMissing, LokiVLProxySystemMemoryHigh, LokiVLProxySystemCPUPressureHigh and LokiVLProxySystemIOPressureHigh](loki-vl-proxy-system-resources.md)

These names match the Helm `PrometheusRule` alerts in
`alerting/loki-vl-proxy-prometheusrule.yaml`, whose `runbook_url` annotations
link here. The standalone rule file `alerting/loki-vl-proxy-alerting-rules.yaml`
(for Prometheus, vmalert or Grafana alerting) uses a partly different alert set
and carries no runbook links.
