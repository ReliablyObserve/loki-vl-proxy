---
sidebar_label: Cache troubleshooting
description: Diagnose Drilldown cache reuse and backend work in production.
---

# Diagnose slow Drilldown refreshes

Start with one reproducible request: keep the selector, tenant, forwarded auth,
client profile, step and evaluation phase fixed. Record the requested time
bounds and status. Compare a cold request, an identical repeat, then a window
moved by exactly one step. Compare successful answers against Loki where a
reference is available; an empty or partial response is not a successful speedup.

## Distinguish response hits, fragments and exclusions

An exact response hit can return before evaluation-point lookups. A moved
window cannot use that exact response; eligible requests reuse older settled
points and evaluate missing ranges. The Loki-facing response/point cache and
the VictoriaLogs-facing backend cache have separate identities and counters.

```promql
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="query_steps_cache"}[5m])
)
```

Request outcomes are `hit`, `partial`, `miss`, `fallback` and `error`.
`stored` and `unreadable` describe background admission, not extra requests;
exclude them from request hit-rate denominators. `partial` means missing
evaluation ranges were recomputed, not that a partial answer was returned.
These counts do not include requests already served by an exact response cache.

```promql
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="query_steps_eligibility"}[5m])
)
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="query_steps_admission"}[5m])
)
```

Eligibility exclusions are `disabled`, `drilldown_profile`, `mixed_parsers`,
`shape`, `bounds` and `phase`. An exclusion is not a lookup miss. Admission
rejections are `canceled`, `error`, `oversize`, `headers` or `evaluator`.
`evaluator` means the route did not certify a complete per-step answer: native
stats/field routes and truncated Drilldown results can use other caches, but
must not be treated as complete retained points. These fixed labels are
exported through the existing Prometheus and OTLP internal-operation families.

For parser-dependent metrics, inspect
`loki_vl_proxy_parser_metric_evaluations_total{evaluator,reason}`. `raw_rows`
with `probe` means a parse-risk check required exact row evaluation;
`ineligible` means the pipeline cannot use stats pushdown. A moving raw query
with settled-point reuse should transfer only missing edges, while an exact
response hit should avoid evaluator/backend calls entirely. Request counts
alone cannot distinguish a one-minute read from a two-day scan.

## Explain an individual decision

Temporarily enable the existing debug log level for a controlled investigation.
The structured `query_steps_cache` event contains `outcome`, `total_steps`,
`missing_steps`, `missing_ranges` and `response_bytes`. Outcomes include
`miss`, `hit`, `partial`, `gap_not_mergeable` and `merge_rejected`. A
`merge_rejected` event means the whole-window route is used to preserve its
series ordering, limits, envelope or evaluator contract. It is not permission
to raise limits without measuring the cost.

`backend_body_read` records each actual decoded upstream body at caller
close, before response transformations or coalesced replay. Its existing
internal-operation counter/histogram use fixed outcomes `eof`, `closed`,
`error`, `canceled` and `timeout`. The structured event adds `backend.route`,
`decoded_bytes`, `read_span_seconds` when both requested bounds are known,
`elapsed_seconds` and `eof_observed`. Normal completions are debug events;
read/close failures and deadlines are warnings without raw error or query text.
`loki_vl_proxy_backend_read_bytes_total{kind}` exposes consumed decoded bytes
at body close in both Prometheus and OTLP. The fixed kinds are `raw`, `stats`,
`metadata` and `other`; these describe VL HTTP reads, not cache occupancy.
Use `sum by (kind) (rate(loki_vl_proxy_backend_read_bytes_total[5m]))` to
verify reduced transfer without enabling debug logs. In-flight bodies have
not yet contributed their closing snapshot.

```promql
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="backend_body_read"}[5m])
)
histogram_quantile(0.95, sum by (le, outcome) (
  rate(loki_vl_proxy_internal_operation_duration_seconds_bucket{operation="backend_body_read"}[5m])
))
```

Compare byte rates, completion outcomes and duration with request volume.
Lower transfer with more `error`/`timeout` completions is not a successful
optimization. A stable backend request rate can still hide a large reduction
in the requested range or bytes; inspect these signals together.

The span describes requested backend bounds, including any lookback, rather
than rows actually scanned. Decoded bytes are consumed application bytes,
not compressed network traffic. Duration runs from HTTP start until caller
close and includes consumer processing/backpressure. `closed` only means
the consumer did not observe EOF; it does not imply a bad API answer. On a
concurrent abort, bytes are a snapshot at close: an interrupted read can
finish afterward. Existing HTTP/header observations and request-level error
metrics remain separate.

These events contain no query, selector, cache key, tenant or forwarded
credentials. Use normal request/trace correlation and the existing
`upstream_request` events to inspect backend latency/status. Expected cache
misses and exclusions remain debug diagnostics; failed backend requests retain
the existing error handling. Do not turn on raw-query logging to diagnose reuse.

## Measure cost by semantic layer and storage tier

Use the [layer metric definitions and caveats](observability.md#semantic-cache-layers)
alongside backend request/duration metrics and process CPU/RSS. Useful queries:

```promql
sum by (layer) (loki_vl_proxy_cache_layer_bytes{tier="l1_memory"})
sum by (layer, tier) (rate(loki_vl_proxy_cache_layer_read_bytes_total[5m]))
sum by (layer) (rate(loki_vl_proxy_cache_layer_disk_encoded_write_bytes_total[5m]))
sum by (layer, tier) (rate(loki_vl_proxy_cache_layer_evictions_total[5m]))
```

RAM occupancy is a logical cache charge, not process RSS. Encoded disk bytes
are not device IOPS, throughput or a physical bbolt file-size bound. Background
flush/replication may complete after the request. Keep priming, measured reads
and delayed writes separate; use filesystem/device telemetry for actual I/O.
Increasing a cache hit ratio without reducing backend ranges/bytes or latency
does not prove an improvement.

On Linux, process memory uses `/proc` RSS. When `/proc` is unavailable (including
macOS), the legacy `loki_vl_proxy_process_resident_memory_bytes` gauge falls
back to Go `MemStats.Sys`, despite its name. Use container/kernel memory
telemetry for RSS in that environment; do not relabel the fallback as RSS.

## Safety boundaries

Raw JSON `count_over_time` and `rate` can retain complete settled answers on
their original evaluation grid, including range equal to step. Native
Drilldown field histograms, transforms and mixed-parser routes retain their
existing admission rules. Any truncated source/gap, changed route or merged
series/byte/sample limit causes full evaluation rather than merging unsafe
points. New raw JSON step keys are separated from older binaries' step keys.

Points newer than `now - query-range-freshness` are recomputed. Reusing older
points preserves their original read age and extent TTL; it does not make late
backfills immediately visible after the settle horizon. This is not distributed
deletion invalidation. Check restart and peer-loss behavior separately from
ordinary warm requests; RF=1 and RF=2 have different recovery costs.
