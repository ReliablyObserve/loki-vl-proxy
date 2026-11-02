---
sidebar_label: Log Patterns
description: How loki-vl-proxy implements Loki-compatible log pattern clustering for Grafana Logs Drilldown.
---

# Patterns

This document covers Loki-compatible patterns behavior in Loki-VL-proxy, including persistence and sizing guidance.

## Compatibility Goal

Loki-VL-proxy exposes `GET /loki/api/v1/patterns` for Grafana Explore/Drilldown compatibility.

- API route and payload shape follow Loki patterns expectations.
- Endpoint can be gated via `-patterns-enabled` for strict environment control.
- Proxy-side response cache keeps repeated Drilldown pattern loads fast.

## How Detection Works

Proxy behavior:

- The proxy fetches matching logs and runs Drain-like token clustering.
- Patterns are derived from live query results, not from a static dictionary.
- Optional global autodetect mode (`-patterns-autodetect-from-queries=true`) additionally warms pattern cache from successful `query` and `query_range` responses.
- Maximum patterns returned per request are clamped to `1000`.
- Pattern extraction reads VictoriaLogs raw logs in bounded, aligned windows
  and mines samples for the requested step across the selected range.

### Cache reuse on moving ranges

An exact `/patterns` repeat uses the existing response cache. Autodetection
from a prior `query` or `query_range` can also populate that same response key
when its selector, range, step, tenant and forwarded authorization match.
These query-derived results are samples; a limited log query is not proof that
it covered all lines in a long selected range.

When `start` and `end` move, the response key changes. The proxy also stores
mined results for aligned source windows under `patterns_window:`
keys in the existing **Loki semantic cache layer**. The first and last windows
are fetched again because their bounds move. Interior windows use the same
`-query-range-freshness`, `-query-range-recent-cache-ttl` and
`-query-range-history-cache-ttl` rules as log-window fragments: by default,
recent windows are not cached and settled windows use the history TTL. Keys
include the backend/query, tenant and forwarded-auth scope, step, sample and
pattern limits, exact source bounds and level derivation settings. Cache
entries are bounded by the configured L1/L2/L3 cache budgets; they are not
written into the separate pattern snapshot file.
An incomplete VL stream is discarded instead of being persisted as a mined
window. When samples hit the source-line limit, the bounded second pass picks
stable windows across the range so a refresh can reuse settled samples.

Inspect `loki_vl_proxy_internal_operation_total{operation="patterns_window_cache"}`
with fixed outcomes `hit`, `miss`, `edge`, `recent_disabled` and `unreadable`.
The existing `loki_vl_proxy_cache_layer_*{layer="loki"}` series report tier
occupancy, bytes and evictions. Compare these with VL request counts, read
bytes and duration; a cache hit alone does not prove a faster page load.
Like the existing historical log-window cache, retained pattern windows may
remain stale until TTL expiry after a late backfill or deletion.

The e2e Compose stack used by CI and the local Grafana playground mounts a
named volume at `/cache` for the patterns datasource and enables a 64 MiB
logical L2 budget at `/cache/proxy-l2.bolt`. Its settled pattern windows survive
container restarts and can be read back into a fresh L1. The main proxy and
two fleet peers have separate named volumes and 128 MiB logical L2 budgets
each. These are test-stack budgets, not production defaults. L2 records keep
their normal window TTL (24 hours for historical windows by default), and
LRU eviction may remove them sooner. A named volume does not turn cached data
into permanent storage. `TestDrilldown_LongRangePatterns_ReturnContiguousGroupedSamples`
verifies a shifted Grafana request uses L2 after restarting the patterns proxy.
The same test also checks that ordinary label values, metric and log ranges,
series and instant metrics return identical answers and read L2 after restart.

## Static Custom Patterns

You can prepend static patterns to every `/loki/api/v1/patterns` response:

- `-patterns-custom` for inline config
- `-patterns-custom-file` for file-based config (for example ConfigMap-mounted files)

Accepted formats:

- JSON string array
- newline-separated text (`#` comments and empty lines are ignored)

Custom patterns are prepended before autodetected patterns and de-duplicated by exact pattern string.

Helm chart wiring:

- `patternsCustom.inline` -> `-patterns-custom`
- `patternsCustom.file.enabled=true` + ConfigMap mount -> `-patterns-custom-file`

Loki behavior reference:

- Loki also treats patterns as dynamic detection from ingested/queryable logs.
- There is no built-in static predefined pattern catalog shipped for all tenants.

References:

- Loki HTTP API patterns endpoint: https://grafana.com/docs/loki/latest/reference/loki-http-api/#patterns-detection
- Grafana Logs patterns UX: https://grafana.com/docs/grafana/latest/visualizations/simplified-exploration/logs/patterns/

## Grafana Logs Drilldown Compatibility

The Patterns tab in Grafana Logs Drilldown is controlled by the `pattern_ingester_enabled` field in `/loki/api/v1/drilldown-limits`. The proxy returns `true` for this field when both `-patterns-enabled` and `-patterns-autodetect-from-queries` flags are set.

### Known Issue: Patterns Tab in Drilldown 2.0.4 through 2.6.0

Drilldown 2.0.4 through 2.6.0 contain a bug where the Patterns tab does not re-appear after switching from a datasource that returned `pattern_ingester_enabled=false`. The initialization guard (`void 0 === null`, always false) prevents the tab from being re-enabled once `$patternsData` is set to `null`.

**Workaround**: Use the `-patterns-enabled=true -patterns-autodetect-from-queries=true` proxy variant as the Grafana **default datasource**. With this configuration, the first Drilldown load returns `true`, correctly initializing `$patternsData`, and the Patterns tab appears.

In the e2e-compat compose stack, `loki-vl-proxy-patterns-autodetect` serves as the default Grafana datasource for this reason.

## Persistence Model

When `-patterns-persist-path` is set:

- Every detected pattern response is tracked in an in-memory snapshot map.
- Snapshot is written to disk periodically (`-patterns-persist-interval`) and on graceful shutdown.
- On startup, proxy restores disk snapshot first, then merges peer snapshots (newest entry wins per cache key).
- Readiness remains `503` until startup warm completes.

Retention model:

- Pattern cache entries are kept with effectively long-lived TTL (`100y`) and updated in-place.
- Snapshot state is append/update oriented and does not run periodic pruning by age.

## Disk And Startup Flags

- `-patterns-enabled`
- `-patterns-autodetect-from-queries`
- `-patterns-custom`
- `-patterns-custom-file`
- `-patterns-persist-path`
- `-patterns-persist-interval`
- `-patterns-startup-stale-threshold`
- `-patterns-startup-peer-warm-timeout`

If persistence path is invalid/unwritable, startup fails fast with a clear error.

## Deployment Recommendation

For restart-safe pattern persistence:

- run as `StatefulSet`
- mount a PVC
- set `-patterns-persist-path` to a file on that volume

If persistence path is not set, patterns endpoint still works, but warm state is lost on restart.

## Sizing Guidance

Upper bound characteristics:

- response pattern cap: `1000` patterns per request
- each pattern carries:
  - `pattern` string
  - optional `level`
  - `samples` time/count buckets

Approximation:

- `payload_bytes ~= sum(pattern_entry_bytes)` for each cached `(tenant, rawQuery)` key
- rough pattern entry size:
  - `~120 bytes base + len(pattern) + ~24 bytes per sample bucket`

Total snapshot footprint depends on:

- number of unique cached pattern queries
- average pattern string length
- sample bucket density (`step`, query range)
