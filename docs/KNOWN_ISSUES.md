# Known Issues & VL Compatibility Gaps

Last updated: v0.23.0

## Remaining Behavioral Differences

All LogQL features are handled. No errors, no silent failures. Minor behavioral differences:

| Feature | Proxy Behavior | Loki Behavior |
|---|---|---|
| `without()` grouping | Converted to `by()` (labels inverted) | Native complement grouping |
| `on()`/`ignoring()` | Stripped; exact metric key match | Label-subset matching |
| `group_left()`/`group_right()` | Stripped; no cardinality enforcement | One-to-many join validation |
| Subquery `rate(...)[1h:5m]` | Proxy-side: runs inner query at sub-steps, aggregates | Native nested sub-step evaluation |

## Data Model Differences

### Stream Filter vs Field Filter Performance

VL stream selectors `{label="value"}` only match `_stream_fields`. By default, the proxy converts ALL Loki stream matchers to field filters for correctness. Use `-stream-fields=app,env,namespace` to enable VL native stream selectors for known `_stream_fields` (faster index path).

### Structured Metadata (Loki 3.x)

Loki 3.x has stream labels vs structured metadata vs parsed labels. VL treats all fields equally. The mapping is natural but not identical -- Grafana Explore handles both transparently.

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
- ~~Admin endpoints (`/rules`, `/alerts`)~~ -> Fixed: stubs for Grafana Alerting compatibility
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
- ~~`X-Forwarded-For` spoofable~~ -> Fixed: ClientID uses RemoteAddr (v0.18.0)
- ~~`offset` and `@` modifiers~~ -> Fixed: stripped during translation, time range unaffected (v0.19.0)
- ~~`bool` modifier~~ -> Fixed: stripped at translation, comparisons return 1/0 (v0.19.0)
- ~~Field-specific parser `| json f1, f2`~~ -> Fixed: maps to full unpack (v0.19.0)
- ~~Backslash quotes in selectors~~ -> Fixed: findMatchingBrace handles `\"` (v0.19.0)
- ~~No system metrics~~ -> Fixed: /proc CPU, mem, IO, net, PSI exposed in /metrics (v0.19.0)
- ~~Cache random eviction~~ -> Fixed: LRU eviction via container/list (v0.21.0)
- ~~`unwrap duration()/bytes()` conversion~~ -> Fixed: proxy-side parsers for duration/byte strings (v0.21.0)
- ~~`on()`/`ignoring()`/`group_left()`/`group_right()`~~ -> Fixed: stripped at translation, binary uses exact key match (v0.22.0)
- ~~Subquery `rate(...)[1h:5m]`~~ -> Fixed: proxy-side evaluation — runs inner query at sub-steps, aggregates with outer function (v0.23.0)
- ~~golangci-lint v2 config~~ -> Fixed: added version: "2" to .golangci.yml (v0.22.0)
