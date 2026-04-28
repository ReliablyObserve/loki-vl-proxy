---
sidebar_label: LogQL → LogsQL Reference
description: Complete mapping of LogQL operators, parsers, metric functions, and filter expressions to VictoriaLogs LogsQL.
---

# LogQL to LogsQL Translation Reference

For label/field exposure mode behavior (`label-style`, `metadata-field-mode`, `emit-structured-metadata`, and custom mappings), see [Translation Modes Guide](translation-modes.md).

## Stream Selectors

| LogQL | LogsQL | Notes |
|---|---|---|
| `{app="nginx"}` | `app:=nginx` | Field filter (not stream filter) |
| `{app!="debug"}` | `-app:=debug` | Negative equality |
| `{app=~"ng.*"}` | `app:~"ng.*"` | Regex match |
| `{app!~"test.*"}` | `-app:~"test.*"` | Negative regex |

All stream matchers are converted to field filters (not VL `{...}` stream selectors) because VL stream filters only match declared `_stream_fields`.

## Line Filters

| LogQL | LogsQL | Notes |
|---|---|---|
| `\|= "error"` | `~"error"` | Substring match (not word-only) |
| `!= "debug"` | `NOT ~"debug"` | Negative substring |
| `\|~ "err.*"` | `~"err.*"` | Regex match |
| `!~ "debug.*"` | `NOT ~"debug.*"` | Negative regex |

## Parser Stages

| LogQL | LogsQL |
|---|---|
| `\| json` | `\| unpack_json` |
| `\| unpack` | `\| unpack_json` |
| `\| logfmt` | `\| unpack_logfmt` |
| `\| pattern "<ip> ..."` | `\| extract "<ip> ..."` |
| `\| regexp "..."` | `\| extract_regexp "..."` |
| `\|> "pattern"` | Pattern match line filter (Loki 3.7+) |

## Label Filters

| LogQL | LogsQL |
|---|---|
| `\| label == "val"` | `label:=val` |
| `\| label != "val"` | `-label:=val` |
| `\| label =~ "5.."` | `label:~"5.."` |
| `\| label !~ "GET\|HEAD"` | `-label:~"GET\|HEAD"` |
| `\| label > 500` | `label:>500` |
| `\| label >= 500` | `label:>=500` |
| `\| label < 200` | `label:<200` |
| `\| label <= 200` | `label:<=200` |

After a parser stage, label filters are wrapped as `| filter <expr>`. For example `| json | status >= 400` is correctly supported; earlier proxy versions incorrectly rejected such filters at the syntax validation stage.

## Formatting Stages

| LogQL | LogsQL |
|---|---|
| `\| line_format "{{.x}}"` | `\| format "<x>"` |
| `\| label_format x="{{.y}}"` | `\| format "<y>" as x` |
| `\| label_format a="{{.x}}", b="{{.y}}"` | `\| format "<x>" as a \| format "<y>" as b` |
| `\| drop a, b` | `\| delete a, b` |
| `\| keep a, b` | `\| fields _time, _msg, _stream, a, b` |

## Proxy-Side Stages

These stages are executed at the proxy level (VL has no native equivalents):

| LogQL | Implementation |
|---|---|
| `\| decolorize` | ANSI escape sequence stripping via regex |
| `\| ip("10.0.0.0/8")` | IP CIDR range filtering |
| `\| line_format` (templates) | Full Go `text/template` (ToUpper, ToLower, default, etc.) |

## Metric Queries

### Range Vector Functions

| LogQL | LogsQL |
|---|---|
| `rate({...}[5m])` | `stats count()` + `math` normalization by window seconds, then `stats sum(...)` per grouping |
| `count_over_time({...}[5m])` | `... \| stats count()` |
| `bytes_over_time({...}[5m])` | `... \| stats sum_len(_msg)` |
| `bytes_rate({...}[5m])` | `stats sum_len(_msg)` + `math` normalization by window seconds, then `stats sum(...)` per grouping |
| `sum_over_time({...} \| unwrap f [5m])` | `... \| stats sum(f)` |
| `avg_over_time({...} \| unwrap f [5m])` | `... \| stats avg(f)` |
| `max_over_time({...} \| unwrap f [5m])` | `... \| stats max(f)` |
| `min_over_time({...} \| unwrap f [5m])` | `... \| stats min(f)` |
| `first_over_time({...} \| unwrap f [5m])` | `... \| stats first(f)` |
| `last_over_time({...} \| unwrap f [5m])` | `... \| stats last(f)` |
| `stddev_over_time({...} \| unwrap f [5m])` | `... \| stats stddev(f)` |
| `stdvar_over_time({...} \| unwrap f [5m])` | proxy binary expression: `(... \| stats stddev(f)) ^ 2` |
| `quantile_over_time(0.95, {...} \| unwrap f [5m])` | `... \| stats quantile(0.95, f)` |
| `rate_counter({...} \| unwrap f [5m])` | `... \| stats __rate_counter__(f)` |
| `absent_over_time({...}[5m])` | `... \| stats count()` |

### Outer Aggregations

| LogQL | LogsQL |
|---|---|
| `sum(rate({...}[5m]))` | normalized per-stream/per-group rate, then proxy applies outer aggregation where supported |
| `sum(rate({...}[5m])) by (x)` | `... \| stats by (x) count() as __lvp_inner \| math __lvp_inner/window as __lvp_rate \| stats by (x) sum(__lvp_rate)` |
| `avg(rate({...}[5m])) by (x)` | same normalized-rate path grouped by `(x)` |
| `topk(10, rate({...}[5m]))` | normalized-rate path with stream grouping; top-level selection remains simplified |

Supported: `sum`, `avg`, `max`, `min`, `count`, `topk`, `bottomk`, `stddev`, `stdvar`, `sort`, `sort_desc`, `group`, `label_replace`, `label_join`.

`group()` returns `1` for every series that has data. The inner metric is translated normally for grouping/presence detection; the proxy normalises all result values to `1` in post-processing.

`label_replace(v, dst, repl, src, regex)` and `label_join(v, dst, sep, src1, ...)` are handled via a marker-suffix pattern: the inner expression is translated to VL, the spec is base64-encoded and embedded as a query suffix, the proxy strips the marker before sending to VL and applies the label operation to the matrix response.

`count_values(label, v)` is not translatable — VictoriaLogs has no primitive that groups by metric values. Queries using `count_values` return a descriptive error.

### Binary Expressions

| LogQL | Proxy Behavior |
|---|---|
| `rate({...}[5m]) / rate({...}[5m])` | Both sides evaluated independently, combined at proxy |
| `rate({...}[5m]) * 100` | Scalar applied to each data point |
| `100 / rate({...}[5m])` | Reverse scalar operation |

Supported operators: `+`, `-`, `*`, `/`, `%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=`.

## Proxy Compatibility Layer

The following Loki semantics are implemented in the proxy to bridge gaps where VictoriaLogs primitives do not directly match Loki behavior.

### Time and Subquery Semantics

| LogQL feature | Proxy behavior |
|---|---|
| `offset 1h` on range vectors | NOT YET IMPLEMENTED -- time-window shifting gap; offset is silently stripped |
| `@ <timestamp>` modifier | Normalized/stripped in translation for VictoriaLogs backend requests |
| Subquery `rate(...)[1h:5m]` | Proxy runs inner query across sub-steps and applies outer aggregation |
| Range-vector metric windows (`*_over_time`, `rate`, `count_over_time`, `bytes_*`, `rate_counter`) | Proxy applies Loki-compatible sliding-window evaluation over step-aligned timestamps and emits matrix/vector responses |
| `label_replace(expr, dst, repl, src, regex)` | Proxy post-processing: inner expr translated to VL, spec embedded as marker, applied to matrix response (Prometheus semantics: no-match leaves dst unchanged) |
| `label_join(v, dst, sep, src1, ...)` | Proxy post-processing: same marker pattern as `label_replace`; missing src labels are skipped |

### Parser-Stage Metric Compatibility Path

For metric queries that include parser stages after translation (`unpack_*` or `extract*`), the proxy can switch from direct `stats_query(_range)` execution to proxy-side range evaluation so Loki behavior is preserved:

- parser-derived labels remain available in metric output cardinality
- unwrap-required functions keep Loki unwrap/error semantics
- `rate_counter` uses the proxy compatibility path by default (including reset-aware handling)

For non-parser metric queries, the default path remains single-shot `stats_query` / `stats_query_range` against VictoriaLogs.

#### Path Selection Rules

| Query shape | Selected execution path | Why |
|---|---|---|
| range metric family with parser stages (`unpack_*`, `extract*`) | proxy-side compatibility evaluation | preserves Loki parser-derived labelsets and unwrap semantics |
| `rate_counter(... \| unwrap ...)` | proxy-side compatibility evaluation | keeps counter-reset handling and behavior stable regardless of backend parser support |
| range metric family without parser stages | direct `stats_query_range` | fastest path when backend semantics match Loki expectations |
| instant metric family without parser stages | direct `stats_query` | keeps instant-path behavior aligned with existing VL fast path |

#### Compatibility Guarantees For This Path

- parser labels produced in the query pipeline remain visible in resulting series labels
- unwrap-required functions fail with Loki-style `invalid aggregation <func> without unwrap` errors when unwrap is omitted
- manual compatibility execution is scoped to affected metric families and does not replace direct backend stats execution globally

### Formatting and Normalization

| LogQL feature | Proxy behavior |
|---|---|
| `line_format` / `label_format` templates | Go-template based compatibility formatting in response pipeline |
| `decolorize` | ANSI escape sequence stripping |
| `\| unwrap duration(field)` | Unwrap with duration string conversion (proxy-side) |
| `\| unwrap bytes(field)` | Unwrap with byte size conversion (proxy-side) |
| `\| unwrap` (no field name) | Silently stripped; no translation error |
| Missing unwrap on unwrap-required functions | Proxy returns Loki-style `invalid aggregation <func> without unwrap` error |
| `bool` modifier on comparisons | Compatibility normalization to Loki-style boolean vector output |
| `without()` grouping | Compatibility label projection after backend aggregation |
| `on()` / `ignoring()` / `group_left()` / `group_right()` | Loki-style vector matching and join cardinality handling in proxy evaluation |

## VictoriaLogs References

- LogQL to LogsQL mapping: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/
- LogSQL reference: https://docs.victoriametrics.com/victorialogs/logsql/
- Querying guide: https://docs.victoriametrics.com/victorialogs/querying/