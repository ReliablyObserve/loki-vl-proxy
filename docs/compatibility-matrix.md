# Compatibility Matrix

This project tracks compatibility on three separate layers. They are intentionally split because each layer has different contracts, failure modes, and supported version ranges.

## Tracks

| Track | What it answers | Workflow | Score test | Doc |
|---|---|---|---|---|
| Loki | Does the proxy behave like Loki for Loki clients and LogQL? | `compat-loki.yaml` | `TestLokiTrackScore` | [compatibility-loki.md](/tmp/Loki-VL-proxy/docs/compatibility-loki.md) |
| Logs Drilldown | Does Grafana Logs Drilldown still work through the Loki datasource proxy path? | `compat-drilldown.yaml` | `TestDrilldownTrackScore` | [compatibility-drilldown.md](/tmp/Loki-VL-proxy/docs/compatibility-drilldown.md) |
| VictoriaLogs | Does the proxy keep VictoriaLogs backend behavior stable while translating to Loki semantics? | `compat-vl.yaml` | `TestVLTrackScore` | [compatibility-victorialogs.md](/tmp/Loki-VL-proxy/docs/compatibility-victorialogs.md) |

The tracked versions live in [compatibility-matrix.json](/tmp/Loki-VL-proxy/test/e2e-compat/compatibility-matrix.json).
The GitHub Actions compatibility workflows read their matrix lists directly from that manifest, so the repo has a single source of truth for supported versions.

For normal pull requests, these tracks run as PR checks. Generated release PRs intentionally use a lighter validation path and do not rerun the full compatibility matrix, because the release branch only carries release metadata changes.

## Support Window Policy

The matrix is intentionally not open-ended. For every upstream we support a moving window that advances as new releases land:

- Loki: current minor family plus one minor family behind, including patch releases in those two families
- VictoriaLogs: the `v1.3x.x` and `v1.4x.x` support bands
- Logs Drilldown: current release family plus one family behind, including patch releases in those two families

When a new upstream family becomes current, the oldest family drops out of the matrix. VictoriaLogs is the one exception right now: we intentionally keep the broader `v1.3x.x` and `v1.4x.x` band because backend upgrades lag more often during migrations. This keeps the compatibility budget focused on versions we can realistically support.

## Matrix Summary

| Component family | Versions tracked | Coverage mode |
|---|---|---|
| Loki | `3.6.x` and `3.7.x` | Real runtime matrix in GitHub Actions |
| Logs Drilldown | `1.0.x` and `2.0.x` | Pinned runtime e2e plus source-contract matrix |
| VictoriaLogs | `v1.30.x` through `v1.49.x` | Real runtime matrix in GitHub Actions |

## Why The Tracks Are Separate

- Loki compatibility is about frontend semantics: query results, labels, streams, and LogQL behavior.
- Drilldown compatibility is about Grafana app contracts: datasource resources, log frame labels, level coloring, and breakdown panels.
- VictoriaLogs compatibility is about backend assumptions: how the proxy uses `field_names`, `field_values`, `hits`, `_stream`, and structured fields.

Blending those into one percentage hides which layer actually regressed. The repo now treats them as separate compatibility products.
