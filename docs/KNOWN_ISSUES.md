# Known Issues & VL Compatibility Gaps

Last updated: v0.17.0

## Remaining Gaps (P3 -- Rare Edge Cases)

| Feature | Status | Impact |
|---|---|---|
| Subquery syntax `rate(...)[1h:5m]` | Not supported | No VL equivalent; rare in Grafana |
| Cache random eviction (not LRU) | Known limitation | Hot entries may be evicted under pressure |
| `on()`/`ignoring()`/`group_left()`/`group_right()` | Not supported | Complex dashboard joins fail |
| `offset` and `@` modifiers | Not supported | Week-over-week queries fail |
| `X-Forwarded-For` spoofable for rate limiting | Known limitation | Security edge case behind trusted proxy |
| `unwrap duration()/bytes()` unit conversion | Wrapper stripped | Raw field value used (no unit conversion at proxy) |

## Data Model Differences

### Stream Filter vs Field Filter Performance

VL stream selectors `{label="value"}` only match `_stream_fields`. The proxy converts ALL Loki stream matchers to field filters for correctness. For known stream fields, stream selectors would be faster.

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
