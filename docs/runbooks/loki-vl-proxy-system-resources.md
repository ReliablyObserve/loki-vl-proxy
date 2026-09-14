# LokiVLProxy Operational Resources

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
   - `curl -fsS http://<proxy>:9091/metrics | rg "loki_vl_proxy_process_memory_usage_ratio|loki_vl_proxy_process_cpu_usage_ratio|loki_vl_proxy_process_pressure_|loki_vl_proxy_process_disk_(read|write)_operations_total"`
   - `9091` is the chart's dedicated metrics listener (`extraArgs.metrics-listen`). A binary started without `-metrics-listen` serves `/metrics` on its `-listen` port, and only when `-server.register-instrumentation=true`.
3. Inspect startup diagnostics in proxy logs for system-metrics check output.
4. Check dashboard section `Operational Resources` for memory, CPU, PSI, disk IOPS/throughput, and network trends.

## Kubernetes-Specific Checks

1. Confirm which scope each family reports. Host-scope reads (`stat`, `meminfo`, `pressure/{cpu,memory,io}`) use `-host-proc-root`; self/container-scope reads (`self/*`, `net/dev`) use `-proc-root` (default `/proc`):
   - `systemMetrics.hostProc.enabled: true` (chart default)
   - the chart mounts only the five host files `/proc/stat`, `/proc/meminfo`, `/proc/pressure/cpu`, `/proc/pressure/memory` and `/proc/pressure/io` (read-only `hostPath` of type `File`) under `systemMetrics.hostProc.mountPath` (default `/host/proc`) and auto-sets `-host-proc-root=/host/proc`
2. Verify the container has read access to the mounted files; PSI files are absent on kernels without `/proc/pressure/*`.
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
   - inspect backend disk/network saturation and compare with proxy-side disk/network direction graphs

## Recovery Criteria

- `process_memory_usage_ratio < 0.85` sustained.
- PSI `cpu` and `io` some-ratio drops below alert thresholds.
- Query latency/error alerts recover and remain stable for at least one alert interval.
