# Drilldown Field Histogram Quality

This document describes how the proxy produces drilldown field histograms,
how quality is measured, and the strategies used to match Loki's behaviour.

## Overview

Grafana Logs Drilldown displays per-field histograms: for each detected
field (e.g. `level`, `http_method`, `duration_ms`), it shows how many log
lines matched each value over time. These charts are the main "fields panel"
in the Drilldown view. The proxy answers Loki's `count_over_time` +
`sum by (field)` LogQL with VictoriaLogs `/select/logsql/hits` top-N values
first. When `/hits` cannot serve the query it falls back to
`stats_query_range` merged with `field_values` data, and zero-fills missing
time steps to match Loki's continuous-line behaviour.

## Path Selection

Drilldown-shaped single-field count queries (an existence filter on the
grouped field) are routed the same way for every client and every parseable
range:

```
proxyStatsQueryRangeDrilldown (metric_binary.go)
  → proxyStatsQueryRangeDrilldownHybrid
      1. proxyStatsQueryRangeDrilldownHits
           /select/logsql/hits, top 20 values (drilldownHitsFieldsLimit)
           range ≥ 6 h: 8 windows sampled in parallel (hitsWindowSampleThreshold,
                        hitsWindowCount)
           Drilldown-tagged residual chunk (end - start < step): empty matrix
           remainder bucket dropped; shared timestamp axis
      2. on /hits failure (X-Proxy-Drilldown-Hits-Fallback: 1)
           field_values over the full range (limit 500)
           + stats_query_range over the full range, | limit 500
           step capped to ≤ 120 buckets (maxDrilldownStatsBuckets)
           ≤ 30 buckets for likely high-cardinality fields
           ≤ 50 distinct values: requested step, floor range / 1000 buckets
           Zero-filled by zerofillStatsMatrix
           Merged by mergeDrilldownWithFieldValues

Parser-stage variant (| json / | logfmt before the filter)
  → proxyStatsQueryRangeDrilldownParserDirect
      /hits first, then stats_query_range with | limit 500
```

The per-field batcher (`drilldown_field_batcher.go`) remains behind the hybrid
path and only runs when `start`/`end` cannot be parsed.

Non-Drilldown-shaped `count() by (field)` queries use
`proxyStatsQueryRangeDirect`. They reach the window-sampled `/hits` path only
for ranges of 2 h or more when the request is Drilldown-tagged, or comes from
another Grafana client and groups by a likely high-cardinality field; otherwise
they get `stats_query_range` capped to the busiest `-max-stats-query-series`
(default 500) series.

## Cardinality Tiers

On the stats fallback, the proxy classifies each field by name and by the distinct-value count returned by `field_values`:

| Tier | Detection | Stats fallback strategy | Examples |
|------|-----------|----------|---------|
| **High** | `isLikelyHighCardinalityField` (exact names such as `trace_id`, `session_id`, or suffix `_id`, `.id`, `_uuid`, `_token`, `_hash`, `_key`) or `isHighCardinalityFieldName` (`_id`, `_uid`) | `stats_query_range` with step floored to ≤ 30 buckets (`drilldownHighCardStatsBuckets`); `field_values` synthesis only as last resort | `trace_id`, `span_id`, `api_key` |
| **Low** | ≤ 50 distinct values in `field_values` (`drilldownLowCardThreshold`) | requested step, floored to ≤ 1000 buckets (`drilldownLowCardStatsBuckets`) | `level`, `http_method` |
| **All others** | none | step coarsened to ≤ 120 buckets + `field_values` merge | `duration_ms` |

High-cardinality fields get a tighter bucket cap because VictoriaLogs' stats
pipe materializes one entry per (bucket × distinct value) before the top-N
limit applies. These tiers only apply when `/hits` fails; the `/hits` path
itself returns the top 20 values.

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

`zerofillStatsMatrix` is applied on the stats fallback paths:
1. `proxyStatsQueryRangeDrilldownHybrid` (stats tier after a `/hits` failure)
2. `proxyStatsQueryRangeDrilldownParserDirect` (parser-stage stats subpath)
3. `fieldBatch.fire()` goroutine (per-field batcher)

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
| `proxyStatsQueryRangeDrilldownHits` | `internal/proxy/metric_binary.go` | `/hits` top-N path, windowed sampling, residual suppression |
| `proxyStatsQueryRangeDrilldownHybrid` | `internal/proxy/metric_binary.go` | Drilldown path for all parseable ranges: `/hits`, then stats + `field_values` |
| `fieldBatch.fire` | `internal/proxy/drilldown_field_batcher.go` | Batched per-field stats (reached only without parseable `start`/`end`) |
| `mergeDrilldownWithFieldValues` | `internal/proxy/metric_binary.go` | Merge stats histogram + FV stubs |
| `synthesizeDrilldownMatrix` | `internal/proxy/metric_binary.go` | FV-only stub matrix (no stats) |
| `isHighCardinalityFieldName` | `internal/proxy/metric_binary.go` | `_id`/`_uid` suffix detection |
| `isLikelyHighCardinalityField` | `internal/proxy/metric_binary.go` | Broad HC name detection |
| `coarsenDrilldownStep` | `internal/proxy/metric_binary.go` | Cap bucket count to ≤ 120 |
| `highCardStepFloor` | `internal/proxy/metric_binary.go` | Cap high-cardinality fallback to ≤ 30 buckets |
| `isQuerySplitResidual` | `internal/proxy/metric_binary.go` | Drilldown-tagged sub-step residual chunk detection |
| `TestDrilldown_QualityMatrix` | `test/e2e-compat/drilldown_quality_report_test.go` | Quality measurement (non-blocking) |
| `TestDrilldown_LokiCompare_FieldQuality` | `test/e2e-compat/drilldown_loki_compare_test.go` | Loki parity assertions |
