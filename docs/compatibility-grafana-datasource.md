# Grafana Loki Datasource Compatibility

This track focuses on the built-in Grafana Loki datasource contract (`/api/datasources/.../resources` and datasource proxy calls), separate from app-specific Drilldown behavior.

## Scope

- datasource catalog and health checks
- datasource resource endpoints (`labels`, `label/<name>/values`, `detected_*`, `index/volume*`, `patterns`, `drilldown-limits`)
- datasource proxy API path shape (`/api/datasources/proxy/uid/<uid>/loki/api/v1/...`)

## CI Coverage

- workflow: `compat-drilldown.yaml` (runtime smoke path)
- primary runtime contract test: `TestGrafanaDatasourceCatalogAndHealth`
- additional resource contract tests: `TestDrilldown_GrafanaResourceContracts`

## Runtime Version Matrix

| Grafana runtime version | Coverage path | Focus |
|---|---|---|
| `12.4.2` | pinned runtime CI | current-family datasource + Drilldown runtime contract |
| `12.4.1` | PR smoke + scheduled/manual matrix | current-family datasource contract drift detection |
| `11.6.6` | PR smoke + scheduled/manual matrix | previous-family datasource contract drift detection |

## Client Detection Signals

Runtime detection is signal-based, not guess-based:

- Grafana runtime version: parsed from `User-Agent` (`Grafana/<version>`)
- Drilldown caller surface: `X-Query-Tags: Source=grafana-lokiexplore-app`
- Loki datasource caller surface: inferred from Grafana runtime request shape when no Drilldown source tag is present

On-wire limitation:

- Built-in Loki datasource plugin exact semver is not emitted on requests by default.
- Proxy can reliably detect runtime family and client surface, but not the datasource plugin build number.

## Version Handling Policy

Use runtime-family capability profiles (like backend capability profiles for VictoriaLogs):

- `grafana-runtime-v12-plus`
  - treat as current-family behavior
  - enable current contract assumptions verified by runtime-family tests
- `grafana-runtime-v11`
  - keep previous-family compatibility behavior
  - preserve contract assumptions verified by runtime-family tests

When a new Grafana family becomes current:

1. add runtime to matrix,
2. run datasource + Drilldown runtime contract tests,
3. add/update profile row and focused edge cases,
4. then slide support window forward.

## What Can Differ By Family

The proxy should keep Loki API behavior stable, but client request patterns can differ by runtime family:

- default scene/query mixes and request ordering
- Drilldown resource call density and timing
- field/value follow-up query patterns
- request retries and cancellation timing

These are handled by strict contract tests and conservative runtime-family gating, not by ad-hoc heuristics.

## Release Watchlist

Potential next runtime family move:

- current: `12.x`
- previous supported: `11.x`
- next expected family to evaluate: `13.x`

Promotion criteria for runtime family updates:

1. add runtime versions in `test/e2e-compat/compatibility-matrix.json`,
2. pass `TestGrafanaDatasourceCatalogAndHealth` and `TestDrilldown_RuntimeFamilyContracts`,
3. verify proxy request-surface detection logs stay stable (`grafana.version`, `grafana.client.surface`, `grafana.client.source_tag`),
4. then slide support window and drop oldest family.
