# LokiVLProxy Grafana Tuple Contract

## Alerts Covered

- `LokiVLProxyGrafanaDefault3TupleUnexpected`
- `LokiVLProxyGrafanaDefault2TupleMissing`

## Symptoms

- Grafana Explore or Logs Drilldown shows tuple decode errors (for example `ReadArray`).
- `loki_vl_proxy_response_tuple_mode_total` shows `grafana_default_3tuple` increments.
- `grafana_default_2tuple` drops to zero while Grafana traffic still exists.

## Immediate Checks

1. Confirm proxy health:
   - `curl -fsS http://<proxy>:3100/ready`
2. Run tuple smoke canary with Grafana-style headers:
   - `PROXY_URL=http://<proxy>:3100 ./scripts/smoke-test.sh`
3. Check tuple mode counters:
   - `curl -fsS http://<proxy>:3100/metrics | rg "loki_vl_proxy_response_tuple_mode_total"`

## Typical Root Causes

1. Response tuple shape regressed to 3-tuple for default Grafana callers.
2. A global config or proxy path unintentionally forces structured metadata emission.
3. Recent query-path changes bypassed strict tuple-shape handling in merge/stream code.

## Mitigation

1. Temporarily force canonical shape for Grafana callers:
   - keep `-emit-structured-metadata=true` if needed for non-Grafana clients
   - do not force `structured_metadata=true` globally for Grafana traffic
2. Roll back to the previous known-good proxy version if smoke checks fail.
3. Validate multi-tenant and stream-response paths with the tuple contract tests.

## Recovery Criteria

- `./scripts/smoke-test.sh` passes for both `/query_range` and `/query`.
- `grafana_default_3tuple` rate returns to `0`.
- `grafana_default_2tuple` resumes for active Grafana requests.
