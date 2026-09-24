# Drilldown Label And Field Breakdowns

This document describes how the proxy answers the label and field breakdowns
of Grafana Logs Drilldown, and how their parity with Loki is measured.

## Overview

Grafana Logs Drilldown shows, for each label and each detected field (for
example `pod`, `level`, `http_method`, `trace_id`), how many log lines carry
each value over time. It asks for one metric range query per label or field:

```
sum(count_over_time({env="production" ,pod != ""} [5m])) by (pod)
sum by (user_id) (count_over_time({env="production"} | json user_id="[\"user_id\"]" | drop __error__, __error_details__ | user_id!="" [5m]))
```

`step` equals the range (`$__auto`), and ranges of 24h or more reach the proxy
as Grafana's 24h query-split chunks. The plugin does not cap or sample the
series it receives; it sorts them and draws them.

Loki (v3.7.7) answers these like any other metric query: every series, exact
values on its evaluation timestamps, no sample for a step whose window holds no
line. Above `max_query_series` (default 500) it returns a partial result with
the warning `maximum number of series (N) reached for a single query; returning
partial results` for Drilldown (`JoinSampleVector` in `pkg/logql/engine.go`, the
`seriesLimiter` in `pkg/querier/queryrange/limits.go`) and a `400` for every
other client.

## How The Proxy Answers

The breakdown takes the same path as any other client's grouped count
(`proxyStatsQueryRange` in `internal/proxy/metric_binary.go`):

```
proxyStatsQueryRange
  Drilldown sub-step residual chunk (end - start < step): empty matrix
  range == step: fetch from start - range on buckets anchored to the request
                 start, relabel each bucket onto Loki's evaluation timestamp
  proxyStatsQueryRangeDirectAnchored
      limit = tenant's max_query_series
              (-tenant-limits → -tenant-default-limits → -max-stats-query-series → 500)
      Drilldown: take a -stats-query-range-concurrency slot
      one stats_query_range call, read until limit+1 series
      under the limit: every series (Drilldown and plain clients alike)
      plain client over the limit: Loki's 400
      Drilldown single-field breakdown over the limit: rankedSingleFieldQuery
          <base> | filter f:in(<base> | stats by (f) count() as __lvp_rank
                               | sort by (__lvp_rank desc, f) | limit <limit+1>
                               | fields f)
                 | stats by (f) count()
          (remembered for 5 minutes: later requests rank straight away)
      Drilldown over the limit: keep the limit busiest series + Loki's warning
      response above -backend-max-buffered-response-bytes → 502 naming the flag
```

The ranking subquery inherits the call's time range, so the ranked response
holds at most `limit + 1` series however many values the field has, and
`limit + 1` series means the limit was passed. VictoriaLogs still reads the
lines twice for the ranked call (the ranking, then the buckets). Each returned
series is the exact per-step count. A breakdown that cannot be ranked (the
underscore label style groups a dotted field by both spellings) is read whole
and capped the same way, bounded by `-backend-max-buffered-response-bytes`.

### Deviation From Loki

Over the limit, Loki keeps the first series it meets while evaluating; the
proxy keeps the busiest. For series of equal volume the kept set can differ;
the values of every kept series match Loki's. The residual-chunk suppression
exists for Grafana's merge of split chunks (see
[Drilldown compatibility](../compatibility-drilldown.md#long-range-histograms-and-grafana-querysplitting)).

## Loki Parity

`internal/proxy/drilldown_breakdown_exact_test.go` pins the contract against a
Loki reference evaluator: the labels and fields breakdown request shapes above,
under the limit (every series, Loki's values, no warning, one
`stats_query_range` call, no `/hits` call, no raw scan) and over a per-tenant
limit (the busiest series with Loki's warning for Drilldown, Loki's `400` for a
plain client, another tenant unaffected).

The e2e test `TestDrilldown_LokiCompare_FieldQuality` seeds identical log
streams into Loki and VictoriaLogs and compares proxy and Loki responses per
field and range.

`TestDrilldown_QualityMatrix` seeds entries into VictoriaLogs and logs, per
range (1h to 7d) and field type (`level`, `http_method`, `http_status`,
`duration_ms`, `trace_id`), the series count, bucket density, total count and
proxy latency. An empty result for `level` or `detected_level`, or a proxy 5xx,
fails CI.

## Reference

| Symbol | File | Purpose |
|--------|------|---------|
| `proxyStatsQueryRange` | `internal/proxy/metric_binary.go` | Entry: residual suppression, range == step relabel |
| `proxyStatsQueryRangeDirectAnchored` | `internal/proxy/metric_binary.go` | One stats call, series limit, warning or 400 |
| `rankedSingleFieldQuery` | `internal/proxy/metric_binary.go` | `in()` subquery keeping the `limit + 1` busiest values |
| `isQuerySplitResidual` | `internal/proxy/metric_binary.go` | Drilldown-tagged sub-step residual chunk detection |
| `TestDrilldownBreakdown_*` | `internal/proxy/drilldown_breakdown_exact_test.go` | Loki parity under and over the series limit |
| `TestDrilldown_QualityMatrix` | `test/e2e-compat/drilldown_quality_report_test.go` | Quality measurement |
| `TestDrilldown_LokiCompare_FieldQuality` | `test/e2e-compat/drilldown_loki_compare_test.go` | Loki parity assertions |
