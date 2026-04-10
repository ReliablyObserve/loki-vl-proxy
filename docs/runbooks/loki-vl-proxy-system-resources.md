# LokiVLProxy System Resources

## Alerts Covered

- `LokiVLProxySystemMetricsMissing`
- `LokiVLProxySystemMemoryHigh`
- `LokiVLProxySystemCPUPressureHigh`
- `LokiVLProxySystemIOPressureHigh`

## Symptoms

- System metrics disappear from `/metrics` or dashboards.
- Memory usage ratio remains above `90%`.
- PSI `cpu` or `io` pressure (60s window) remains elevated for 10+ minutes.
- User-facing query latency and timeout/error rates increase during pressure windows.

## Immediate Checks

1. Confirm proxy health:
   - `curl -fsS http://<proxy>:3100/ready`
2. Confirm metrics endpoint includes system families:
   - `curl -fsS http://<proxy>:3100/metrics | rg "node_memory_usage_ratio|node_cpu_usage_ratio|node_pressure_"`
3. Inspect startup diagnostics in proxy logs for system-metrics check output.
4. Check dashboard section `System Resources` for memory, PSI, disk, and network trends.

## Kubernetes-Specific Checks

1. Ensure host `/proc` is mounted when node-level visibility is expected:
   - `systemMetrics.hostProc.enabled: true`
   - chart auto-sets `-proc-root=/host/proc`
2. Verify container has read access to mounted proc path.
3. If scraping is disabled (`server.register-instrumentation=false`), ensure OTLP pipeline is healthy and metrics are queryable in your backend.

## Mitigation

1. Reduce expensive query pressure:
   - identify top endpoints/tenants/clients from proxy metrics dashboards
   - temporarily tighten query ranges or concurrency
2. Scale capacity:
   - increase proxy replicas for CPU-bound contention
   - move to nodes with higher memory/IO capacity when sustained pressure persists
3. Validate backend health:
   - correlate with VictoriaLogs latency and error metrics
   - inspect backend disk/network saturation

## Recovery Criteria

- `node_memory_usage_ratio < 0.85` sustained.
- PSI `cpu` and `io` some-ratio drops below alert thresholds.
- Query latency/error alerts recover and remain stable for at least one alert interval.
