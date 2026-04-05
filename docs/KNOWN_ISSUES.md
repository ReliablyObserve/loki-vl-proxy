# Known Issues & VL Compatibility Gaps

Last updated: v0.26.0

## Remaining Behavioral Differences

All LogQL features are handled. No errors, no silent failures. Minor behavioral differences:

| Feature | Proxy Behavior | Loki Behavior |
|---|---|---|
| `without()` grouping | Proxy strips excluded labels from VL results | Native complement grouping |
| `on()`/`ignoring()` | Proxy-side label-subset matching | Native label-subset matching |
| `group_left()`/`group_right()` | Proxy-side one-to-many join via on() | Native one-to-many join validation |
| Subquery `rate(...)[1h:5m]` | Proxy-side: runs inner query at sub-steps, aggregates | Native nested sub-step evaluation |

## Grafana Datasource Gaps

The proxy now supports datasource-facing headers, cookie forwarding, backend timeout control, and optional HTTPS client-certificate verification. Two areas are still intentionally partial:

| Area | Current State |
|---|---|
| Alerting / ruler APIs | `/rules` and `/alerts` are compatibility stubs only, not a full Loki ruler implementation |
| Browser-origin tailing | `/loki/api/v1/tail` rejects browser `Origin` headers unless explicitly allowlisted via `-tail.allowed-origins` |

## Release Automation Gaps

The project now uses PR-based release automation:

`main merge -> Auto Release -> release/vX.Y.Z PR -> auto-merge -> tag -> GitHub release`

The remaining limitation is repository signature policy:

| Area | Current State |
|---|---|
| Automated release commit signatures | Release PR commits created by `github-actions[bot]` are still unsigned unless workflow signing is configured |
| Fully automatic release PR merge under required signatures | Requires either workflow commit signing or a repo ruleset exception for release automation |

The workflow already handles:

- changelog-backed release PR bodies
- changelog-backed GitHub release notes
- lightweight release-PR validation dispatch
- Helm chart version bumps
- binary, Helm package, and container-image publishing after tag creation

## Data Model Differences

### Stream Filter vs Field Filter Performance

VL stream selectors `{label="value"}` only match `_stream_fields`. By default, the proxy converts ALL Loki stream matchers to field filters for correctness. Use `-stream-fields=app,env,namespace` to enable VL native stream selectors for known `_stream_fields` (faster index path).

### Structured Metadata (Loki 3.x)

Loki 3.x has stream labels vs structured metadata vs parsed labels. VL treats all fields equally. The mapping is natural but not identical -- Grafana Explore handles both transparently.

### Labels vs OTel Dotted Fields

When `-label-style=underscores` and `-metadata-field-mode=hybrid` are used together, the proxy intentionally exposes:

- Loki-compatible underscore labels on the label surface
- both dotted OTel names and translated aliases on field-oriented APIs

This is the expected compatibility model for Drilldown and Explore. It is not a bug if `service_name` appears in label pickers while `service.name` appears in fields/details.

### Large Body Fields

VL may silently drop log records with very large body fields (50KB+). See [VL Issue #91](https://github.com/VictoriaMetrics/victorialogs-datasource/issues/91).

## Previously Fixed (for reference)

These were previously listed as gaps and have been resolved:

- ~~Substring vs word matching~~ -> Fixed: `|= "text"` -> VL `~"text"` (substring)
- ~~Volume API missing~~ -> Fixed: implemented via VL `/select/logsql/hits`
- ~~Tail WebSocket~~ -> Fixed: WebSocket->NDJSON bridge
- ~~Multitenancy header mismatch~~ -> Fixed: `-tenant-map` string->int mapping
- ~~`| decolorize`~~ -> Fixed: proxy-side ANSI stripping
- ~~`absent_over_time()`~~ -> Fixed: mapped to `count()`
- ~~Binary metric expressions~~ -> Fixed: proxy-side evaluation
- ~~`quantile_over_time()`~~ -> Fixed: mapped to VL `quantile(phi, field)`
- ~~Admin endpoints (`/rules`, `/alerts`)~~ -> Partially fixed: datasource-compatible stubs exist, but full ruler semantics remain a roadmap item
- ~~Coalescer cross-tenant data leak~~ -> Fixed: tenant included in coalescing key
- ~~Stats detection false-positive~~ -> Fixed: quote-aware parsing
- ~~Metrics always recording 200~~ -> Fixed: actual status code captured
- ~~`without()` clause silent wrong behavior~~ -> Fixed: returns clear error (v0.17.0)
- ~~Binary ops missing `%`, `^`, comparisons~~ -> Fixed: all operators implemented (v0.17.0)
- ~~Tenant map reload data race~~ -> Fixed: RLock on read path (v0.17.0)
- ~~CB metrics half_open mismatch~~ -> Fixed: accepts both underscore and hyphen (v0.17.0)
- ~~`targetLabels` missing on volume_range~~ -> Fixed: field param forwarded (v0.17.0)
- ~~`IsScalar` rejects negatives~~ -> Fixed: uses strconv.ParseFloat (v0.17.0)
- ~~No delete API~~ -> Fixed: `/loki/api/v1/delete` with safeguards (v0.17.0)
- ~~`X-Forwarded-For` spoofable~~ -> Fixed: proxy headers are ignored for client metrics unless `-metrics.trust-proxy-headers=true` (v0.24.0)
- ~~`offset` and `@` modifiers~~ -> Fixed: stripped during translation, time range unaffected (v0.19.0)
- ~~`bool` modifier~~ -> Fixed: stripped at translation, comparisons return 1/0 (v0.19.0)
- ~~Field-specific parser `| json f1, f2`~~ -> Fixed: maps to full unpack (v0.19.0)
- ~~Backslash quotes in selectors~~ -> Fixed: findMatchingBrace handles `\"` (v0.19.0)
- ~~No system metrics~~ -> Fixed: process + Linux runtime metrics exposed, with container-scoped cgroup metrics replacing old node-style naming in the latest observability docs
- ~~Cache random eviction~~ -> Fixed: LRU eviction via container/list (v0.21.0)
- ~~`unwrap duration()/bytes()` conversion~~ -> Fixed: proxy-side parsers for duration/byte strings (v0.21.0)
- ~~`on()`/`ignoring()`/`group_left()`/`group_right()`~~ -> Fixed: stripped at translation, binary uses exact key match (v0.22.0)
- ~~Subquery `rate(...)[1h:5m]`~~ -> Fixed: proxy-side evaluation — runs inner query at sub-steps, aggregates with outer function (v0.23.0)
- ~~golangci-lint v2 config~~ -> Fixed: added version: "2" to .golangci.yml (v0.22.0)
