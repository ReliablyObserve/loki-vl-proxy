# Drilldown Field Histogram Quality

This document describes how the proxy produces drilldown field histograms,
how quality is measured, and the strategies used to match Loki's behaviour.

## Overview

Grafana Logs Drilldown displays per-field histograms: for each detected
field (e.g. `level`, `http_method`, `duration_ms`), it shows how many log
lines matched each value over time. These charts are the main "fields panel"
in the Drilldown view. The proxy translates Loki's `count_over_time` +
`sum by (field)` LogQL into VictoriaLogs `stats_query_range`, merges the
result with `field_values` data for coverage, and zero-fills missing time
steps to match Loki's continuous-line behaviour.

## Path Selection

The proxy selects one of three code paths based on the query range:

```
request range ≤ 12 h  →  fieldBatcher (drilldown_field_batcher.go)
                              Per-field parallel stats_query_range
                              Up to 120 buckets (maxDrilldownStatsBucketsShort)
                              Zero-filled by zerofillStatsMatrix

request range > 12 h  →  proxyStatsQueryRangeDrilldownHybrid (metric_binary.go)
                              field_values (O(column-index), ~30 ms, any range)
                              + stats_query_range at full range, ≤ 30 buckets
                              Zero-filled by zerofillStatsMatrix
                              Merged by mergeDrilldownWithFieldValues
```

The `X-Query-Tags: Source=grafana-lokiexplore-app` header (set by Grafana
Logs Drilldown) gates both paths. Without it, requests fall through to the
standard `proxyStatsQueryRangeDirect` path.

## Cardinality Tiers

The proxy classifies each field by name before issuing any VL query:

| Tier | Detection | Strategy | Examples |
|------|-----------|----------|---------|
| **Ultra-high** | `isHighCardinalityFieldName` — suffix `_id`, `_uid` | `field_values` stubs only; no stats histogram | `trace_id`, `span_id`, `request_id` |
| **High** | `isLikelyHighCardinalityField` — exact names or suffix `_token`, `_hash`, `_key`, `_uuid` | `field_values` stubs only; no stats histogram | `session_id`, `api_key` |
| **All others** | none | Full stats histogram + `field_values` merge | `level`, `http_method`, `duration_ms` |

High-cardinality fields skip `stats_query_range` entirely because:
- Millions of unique values × 30–120 buckets = VL CPU spike
- The cross-product limit (2000 combinations) truncates real values
- `field_values` already provides the correct total hit counts

## Zero-fill: Why and How

VictoriaLogs `stats_query_range` omits time buckets where count = 0.
Loki's `count_over_time` aggregation emits every step in the query window,
including zero-count steps.

Without zero-fill:
- VL returns points at t=100, t=300 (skipping t=200)
- Grafana connects those points with a line, drawing incorrect trends
- The chart appears "spiky" even for smooth traffic

With zero-fill (`zerofillStatsMatrix` in `drilldown_quality.go`):
- The proxy builds the complete time axis from startSec to endSec at stepSec
- For each existing series, missing steps are filled with `"0"`
- No new series are introduced; only existing series are zero-filled
- FV-only stub series (values in `field_values` but not in top-N stats)
  keep their averaged stub counts — they have no per-step data from VL

`zerofillStatsMatrix` is applied in two places:
1. `proxyStatsQueryRangeDrilldownHybrid` (long-range path, >12 h)
2. `fieldBatch.fire()` goroutine (short-range batcher, ≤12 h)

## Loki Parity

The e2e test `TestDrilldown_LokiCompare_FieldQuality` seeds identical log
streams into both Loki and VL, then compares proxy vs Loki responses.

Acceptance thresholds (enforced for low/medium cardinality fields):

| Metric | Threshold |
|--------|-----------|
| Series count | proxy ≥ loki |
| Total count per series | within ±15% |
| Non-zero bucket coverage | proxy ≥ 90% of Loki |

The ±15% count threshold accounts for the ~6% dual-push timing skew
observed in the e2e environment (sequential Loki + VL pushes, occasional
push failures). The 90% bucket coverage threshold ensures the proxy does not
lose data points that Loki shows.

Comparison is limited to 1 h, 3 h, and 6 h ranges because the e2e Loki
instance retains only ~12 h of data. For 24 h+ ranges only VL proxy output
is verified (via the quality matrix test).

## Quality Matrix Measurement

`TestDrilldown_QualityMatrix` seeds 1200 entries over 2 h into VL and
measures the proxy at 7 ranges × 5 field types:

**Ranges:** 1 h, 3 h, 6 h, 12 h, 24 h, 2 d, 7 d

**Fields:**

| Field | Cardinality | Type |
|-------|-------------|------|
| `level` | 3 values | stream label |
| `http_method` | 5 values | JSON field |
| `http_status` | 10 values | JSON field |
| `duration_ms` | ~50 values | JSON field |
| `trace_id` | unique/entry | JSON field (ultra-high) |

**Hard failures** (block CI):
- Empty result for `level` or `detected_level`
- Proxy HTTP 5xx

**Quality metrics** (logged, never fail CI):
- Series count
- Non-zero bucket count / total buckets (density %)
- Total log count
- End-to-end proxy latency (ms)
- `X-Proxy-Drilldown-Path` header value

## Reference

| Symbol | File | Purpose |
|--------|------|---------|
| `zerofillStatsMatrix` | `internal/proxy/drilldown_quality.go` | Fill missing VL time steps with 0 |
| `proxyStatsQueryRangeDrilldownHybrid` | `internal/proxy/metric_binary.go` | Long-range (>12 h) drilldown path |
| `fieldBatch.fire` | `internal/proxy/drilldown_field_batcher.go` | Short-range parallel per-field stats |
| `mergeDrilldownWithFieldValues` | `internal/proxy/metric_binary.go` | Merge stats histogram + FV stubs |
| `synthesizeDrilldownMatrix` | `internal/proxy/metric_binary.go` | FV-only stub matrix (no stats) |
| `isHighCardinalityFieldName` | `internal/proxy/metric_binary.go` | `_id`/`_uid` suffix detection |
| `isLikelyHighCardinalityField` | `internal/proxy/metric_binary.go` | Broad HC name detection |
| `coarsenDrilldownStep` | `internal/proxy/metric_binary.go` | Cap bucket count to ≤30/120 |
| `drilldownHybridThreshold` | `internal/proxy/metric_binary.go` | 12 h path split point |
| `TestDrilldown_QualityMatrix` | `test/e2e-compat/drilldown_quality_report_test.go` | Quality measurement (non-blocking) |
| `TestDrilldown_LokiCompare_FieldQuality` | `test/e2e-compat/drilldown_loki_compare_test.go` | Loki parity assertions |
