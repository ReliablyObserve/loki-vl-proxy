---
sidebar_label: Grafana Datasource Compatibility
description: Grafana Loki datasource version compatibility, response format support, and known rendering behaviors.
---

# Grafana Loki Datasource Compatibility

This track focuses on the built-in Grafana Loki datasource contract (`/api/datasources/.../resources` and datasource proxy calls), separate from app-specific Drilldown behavior.

## Scope

- datasource catalog and health checks
- datasource resource endpoints (`labels`, `label/<name>/values`, `detected_*`, `index/volume*`, `patterns`, `drilldown-limits`)
- datasource proxy API path shape (`/api/datasources/proxy/uid/<uid>/loki/api/v1/...`)

## Response Format

**Response Format**: The proxy returns log stream values as Loki 3-tuples `[timestamp, line, {metadata}]` when the datasource sends `categorize-labels=true` or `X-Grafana-Meta: categorize-labels`. This is controlled by `-emit-structured-metadata` (default: `true`). The third element carries structured metadata (parser-extracted fields, OTel attributes) as a key-value map that Grafana displays in the Event Details panel.

`-emit-structured-metadata=false` disables this and returns 2-tuples `[timestamp, line]`. Only needed for very old Grafana versions that cannot handle the 3-tuple format.

## CI Coverage

- workflow: `compat-drilldown.yaml` (runtime smoke path)
- primary runtime contract test: `TestGrafanaDatasourceCatalogAndHealth`
- additional resource contract tests: `TestDrilldown_GrafanaResourceContracts`

## Runtime Version Matrix

| Grafana runtime version | Coverage path | Focus |
|---|---|---|
| `13.3.0` | pinned runtime CI | current-family datasource + Drilldown runtime contract; React 19; Explore auto-run quirk below |
| `13.2.5` | datasource contract matrix | previous pinned runtime |
| `13.2.3` | datasource contract matrix | previous pinned runtime |
| `13.2.1` | datasource contract matrix | previous pinned runtime |
| `13.0.1` | datasource contract matrix | previous current-family pin |
| `12.4.14` | PR smoke + scheduled/manual matrix | previous-family datasource contract drift detection |
| `12.4.12` | datasource contract matrix | previous previous-family smoke pin |
| `12.4.10` | datasource contract matrix | previous previous-family smoke pin |
| `12.4.2` | datasource contract matrix | previous-family datasource contract coverage |
| `12.4.1` | Scheduled and manual matrix | previous-family datasource contract coverage |
| `11.6.6` | Scheduled and manual matrix | lts-family datasource contract coverage |

### Grafana 13.3.0: Explore auto-run stays on `Cancel`

Grafana 13.3.0 Explore never leaves the loading state of the query it auto-runs from the URL on page load: the run button keeps reading `Cancel` with a spinner although every request answered (none in flight) and the logs rendered. It reproduces on the Loki (direct) datasource and on every time range (`now-1h` to `now-7d`), so the proxy is not involved. The next run, by the run button after `Cancel` or by `Shift+Enter` in the editor, goes idle normally. Grafana 13.3.0 bundles the Loki datasource plugin `13.2.5` (`loki/module.js?_cache=13.2.5`), and no `13.3.x` patch is published yet.

The e2e-ui helper `runQuery` copes with it without weakening any assertion: it clears the stuck auto-run with `Cancel` once, and only when the page's `/api/ds/query` requests were tracked from before the navigation, at least one answered, none is in flight and none moved for three seconds. The explicit run that follows must send a new query request that answers, and the toolbar must go idle again without any clearing, so a query that hangs still fails and the rows asserted are the ones of that run. A page that stays on `Cancel` with a query in flight fails with the loading state named.

## Client Detection Signals

Runtime detection is signal-based, not guess-based. The signals below were captured on the wire for every Explore
and Logs Drilldown request kind with Grafana 13.3.1 + Drilldown 3.2.0, 13.3.0 + 3.1.0 and 12.4.14 + 2.6.0 (scrubbed
header sets in `internal/clientprofile/testdata`), and `internal/clientprofile` reads them into the request's typed
client profile:

| Signal | Grafana 13.3.x | Grafana 12.4.x | Profile field |
|---|---|---|---|
| `User-Agent` | `Grafana/13.3.1 (linux; arm64) loki/13.2.5` | `Grafana/12.4.14` | Grafana version; Loki datasource plugin version (13.x only) |
| `X-Query-Tags: Source=grafana-lokiexplore-app` | every Drilldown request, its resource calls included | the same | surface `drilldown` |
| `X-Query-Tags: Source=grafana-explore` | an Explore query | absent | surface `explore` |
| `X-Query-Tags: Source=logvolhist` | Explore's log volume query | the same | surface `explore` |
| `X-Grafana-Referer` (path class only) | on resource calls (labels, index/stats, detected_*, patterns, volume) | the same | surface of an untagged resource call |

- A Grafana request with none of these is `datasource` (dashboards, alerting); a request with no Grafana header is
  `api`. Grafana 12's Explore query carries no Source tag, so it reads as `datasource`.
- No request names the Logs Drilldown app's version on any captured release; its profile field stays unknown.
- The request log lines (`client.surface`, `grafana.version`, `grafana.datasource.version`) and `/debug/queries`
  show the profile; see [observability](observability.md). Only closed values and normalised versions leave the
  proxy, never header text; the referer's URL is never kept.

## Version Handling Policy

Use runtime-family capability profiles (like backend capability profiles for VictoriaLogs):

- `grafana-runtime-v13-plus`
  - treat as current-family behavior
  - enable current contract assumptions verified by runtime-family tests
- `grafana-runtime-v12-plus`
  - keep previous-family compatibility behavior
  - preserve contract assumptions verified by runtime-family tests
- `grafana-runtime-v11`
  - lts compatibility behavior
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

Current runtime family state:

- current: `13.x` (pinned: `13.3.0`)
- previous supported: `12.x`
- lts supported: `11.x`
- next expected family to evaluate: `14.x` when released

Promotion criteria for runtime family updates:

1. add runtime versions in `test/e2e-compat/compatibility-matrix.json`,
2. pass `TestGrafanaDatasourceCatalogAndHealth` and `TestDrilldown_RuntimeFamilyContracts`,
3. verify proxy request-surface detection logs stay stable (`grafana.version`, `grafana.client.surface`, `grafana.client.source_tag`),
4. then slide support window and drop oldest family.