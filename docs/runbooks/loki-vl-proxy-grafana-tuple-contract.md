# LokiVLProxy Tuple Contract

## Alerts Covered

- `LokiVLProxyUnexpectedTupleMode`
- `LokiVLProxyDefault2TupleMissing`

## Symptoms

- Grafana Explore or Logs Drilldown shows tuple decode errors (for example `ReadArray`).
- `loki_vl_proxy_response_tuple_mode_total` contains unexpected mode labels (anything except `default_2tuple` and `categorize_labels_3tuple`).
- `default_2tuple` drops to zero while tuple traffic still exists.

## Immediate Checks

1. Confirm proxy health:
   - `curl -fsS http://<proxy>:3100/ready`
2. Run tuple smoke canary (validates both default 2-tuple and categorize-labels 3-tuple paths):
   - `PROXY_URL=http://<proxy>:3100 ./scripts/smoke-test.sh`
3. Check tuple mode counters:
   - `curl -fsS http://<proxy>:3100/metrics | rg "loki_vl_proxy_response_tuple_mode_total"`

## Typical Root Causes

1. Response tuple shape handling regressed in query/stream/merge paths.
2. Callers are forcing `categorize-labels` on all traffic, leaving no default 2-tuple compatibility coverage.
3. Proxy emitted non-contract mode labels after code changes.

## Mitigation

1. Keep strict contract behavior:
   - no flag => `[ts, line]`
   - `X-Loki-Response-Encoding-Flags: categorize-labels` => `[ts, line, metadata]`
2. Do not inject `categorize-labels` globally unless all clients support 3-tuples.
2. Roll back to the previous known-good proxy version if smoke checks fail.
3. Validate multi-tenant and stream-response paths with the tuple contract tests.

## Recovery Criteria

- `./scripts/smoke-test.sh` passes for both `/query_range` and `/query`.
- `loki_vl_proxy_response_tuple_mode_total` only shows `default_2tuple` and `categorize_labels_3tuple`.
- `default_2tuple` is present for no-flag traffic.
