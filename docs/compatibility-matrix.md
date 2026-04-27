---
sidebar_label: Compatibility Matrix
description: Side-by-side feature support table across Loki, VictoriaLogs, and loki-vl-proxy for every Grafana-facing capability.
---

# Compatibility Matrix

This project tracks compatibility on four separate layers. They are intentionally split because each layer has different contracts, failure modes, and supported version ranges.

## Tracks

| Track | What it answers | Workflow | Score test | Doc |
|---|---|---|---|---|
| Loki | Does the proxy behave like Loki for Loki clients and LogQL? | `compat-loki.yaml` | `TestLokiTrackScore` | [compatibility-loki.md](compatibility-loki.md) |
| Logs Drilldown | Does Grafana Logs Drilldown still work through the Loki datasource proxy path? | `compat-drilldown.yaml` | `TestDrilldownTrackScore` | [compatibility-drilldown.md](compatibility-drilldown.md) |
| Grafana Loki Datasource | Does the built-in Grafana Loki datasource contract stay stable across supported Grafana runtime families? | `compat-drilldown.yaml` | `TestGrafanaDatasourceCatalogAndHealth` | [compatibility-grafana-datasource.md](compatibility-grafana-datasource.md) |
| VictoriaLogs | Does the proxy keep VictoriaLogs backend behavior stable while translating to Loki semantics? | `compat-vl.yaml` | `TestVLTrackScore` | [compatibility-victorialogs.md](compatibility-victorialogs.md) |

The tracked versions live in [compatibility-matrix.json](../test/e2e-compat/compatibility-matrix.json).
The GitHub Actions compatibility workflows read their matrix lists directly from that manifest, so the repo has a single source of truth for supported versions.
VictoriaLogs capability profiles (used by runtime version sensing in proxy code) are tracked in the same manifest under `stack.victorialogs.capability_profiles`.
Grafana runtime and client-surface capability profiles are tracked under `stack.grafana_loki_datasource_contract.capability_profiles` and `stack.logs_drilldown_contract.capability_profiles`.

## Query Semantics Matrix

Version coverage and query semantics are tracked separately on purpose.

- [compatibility-matrix.json](../test/e2e-compat/compatibility-matrix.json) answers which runtime families we support in CI.
- [query-semantics-matrix.json](../test/e2e-compat/query-semantics-matrix.json) answers which Loki-facing query combinations must behave the same as real Loki.
- [query-semantics-operations.json](../test/e2e-compat/query-semantics-operations.json) answers which Loki operations are explicitly tracked by the required semantics gate.

The query semantics matrix is manifest-driven and runs against a live Docker Compose stack with:

- real Loki as the oracle
- Loki-VL-proxy against VictoriaLogs as the system under test
- the required `compat-loki` GitHub Actions workflow as the enforcement gate on pull requests
- the `loki-pinned` PR job as the stable required runtime check, with wider Loki-family coverage in the scheduled matrix

Each case declares:

- query family such as selector, parser pipeline, metric aggregation, binary op, or invalid syntax
- endpoint shape: `query` or `query_range`
- expected outcome: `success`, `client_error`, or `server_error`
- expected `resultType` when the query is valid
- comparison mode such as exact line-count parity, exact series-count parity, or exact metric-label-set parity

The operation inventory is machine-checked alongside the matrix so the repo does not quietly drift into “tested cases” without a visible coverage model.

Representative covered combinations now include:

- selector + regex / negative-regex combinations
- chained line filters including exclusion cases
- parser pipelines with exact, numeric, regex, and pattern field filters
- parser-inside-range metric queries and bare `unwrap` range functions
- `absent_over_time(...)` semantics for missing selectors
- scalar `bool` comparisons and vector set operators like `or` / `unless`
- instant post-aggregations like `topk`, `bottomk`, `sort`, and `sort_desc`
- invalid log-query aggregations that must fail the same way as Loki

This keeps the repo explicit about what must match Loki exactly versus what is a proxy-only Grafana contract.

## Proxy-Only Contract Matrix

Some important behaviors are intentionally not judged against Loki because they only exist in the proxy compatibility layer.

Examples:

- synthetic label translation such as `service_name`
- Grafana Drilldown helper endpoints like detected labels, detected fields, field values, volume, and patterns
- stale-on-error helper behavior
- bounded fallback and recovery behavior when VictoriaLogs discovery endpoints fail

Those cases belong in the proxy contract suite, not the strict Loki semantics parity matrix.

## Support Window Policy

The matrix is intentionally not open-ended. For every upstream we support a moving window that advances as new releases land:

- Loki: current minor family plus one minor family behind, including patch releases in those two families
- VictoriaLogs: transitional support across `v1.3x.x` through `v1.5x.x`
- Logs Drilldown: current release family plus one family behind, including patch releases in those two families
- Grafana runtime (for built-in Loki datasource): current runtime family plus one family behind

When a new upstream family becomes current, the oldest family drops out of the matrix. VictoriaLogs is the one exception right now: we intentionally keep the broader `v1.3x.x` through `v1.5x.x` band because backend upgrades lag more often during migrations. This keeps the compatibility budget focused on versions we can realistically support.

## Matrix Summary

| Component family | Versions tracked | Coverage mode |
|---|---|---|
| Loki | `3.6.x` and `3.7.x` | Real runtime matrix in GitHub Actions |
| Grafana Loki datasource runtime | `11.x` and `12.x` | Runtime contracts through Grafana datasource API in GitHub Actions |
| Logs Drilldown | `1.0.x` and `2.0.x` | Pinned runtime e2e plus source-contract matrix |
| VictoriaLogs | `v1.30.x` through `v1.50.x` | Real runtime matrix in GitHub Actions |

## Grafana Version Sensing Model

Proxy-side client sensing is intentionally conservative:

- Drilldown client detection: `X-Query-Tags: Source=grafana-lokiexplore-app`
- Grafana runtime version detection: `User-Agent` (`Grafana/<version>`)
- Built-in Loki datasource plugin exact semver: not exposed on wire by default

Because datasource semver is not emitted in requests, proxy behavior should gate by:

1. deterministic request signals (`X-Query-Tags`, endpoint family)
2. Grafana runtime family (`11.x`, `12.x`, future `13.x`)
3. compatibility matrix and e2e contracts, not guessed plugin build strings

## Why The Tracks Are Separate

- Loki compatibility is about frontend semantics: query results, labels, streams, and LogQL behavior.
- Drilldown compatibility is about Grafana app contracts: datasource resources, log frame labels, level coloring, and breakdown panels.
- Grafana datasource compatibility is about Grafana-to-proxy request shape and resource/API contract stability across runtime families.
- VictoriaLogs compatibility is about backend assumptions: how the proxy uses `field_names`, `field_values`, `hits`, `_stream`, and structured fields.

Blending those into one percentage hides which layer actually regressed. The repo now treats them as separate compatibility products.