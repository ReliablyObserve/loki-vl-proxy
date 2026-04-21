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

After a parser stage, label filters are wrapped as `| filter <expr>`.

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
| `absent_over_time({...}[5m])` | `... \| stats count()` |

### Outer Aggregations

| LogQL | LogsQL |
|---|---|
| `sum(rate({...}[5m]))` | normalized per-stream/per-group rate, then proxy applies outer aggregation where supported |
| `sum(rate({...}[5m])) by (x)` | `... \| stats by (x) count() as __lvp_inner \| math __lvp_inner/window as __lvp_rate \| stats by (x) sum(__lvp_rate)` |
| `avg(rate({...}[5m])) by (x)` | same normalized-rate path grouped by `(x)` |
| `topk(10, rate({...}[5m]))` | normalized-rate path with stream grouping; top-level selection remains simplified |

Supported: `sum`, `avg`, `max`, `min`, `count`, `topk`, `bottomk`, `stddev`, `stdvar`, `sort`, `sort_desc`.

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
| `offset` modifier | Normalized in translation; query time bounds handled by the proxy request path |
| `@ <timestamp>` modifier | Normalized/stripped in translation for VictoriaLogs backend requests |
| Subquery `rate(...)[1h:5m]` | Proxy runs inner query across sub-steps and applies outer aggregation |

### Formatting and Normalization

| LogQL feature | Proxy behavior |
|---|---|
| `line_format` / `label_format` templates | Go-template based compatibility formatting in response pipeline |
| `decolorize` | ANSI escape sequence stripping |
| `unwrap duration()/bytes()` helpers | Unit-aware unwrap compatibility logic before aggregation |
| `bool` modifier on comparisons | Compatibility normalization to Loki-style boolean vector output |
| `without()` grouping | Compatibility label projection after backend aggregation |
| `on()` / `ignoring()` / `group_left()` / `group_right()` | Loki-style vector matching and join cardinality handling in proxy evaluation |

## VictoriaLogs References

- LogQL to LogsQL mapping: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/
- LogSQL reference: https://docs.victoriametrics.com/victorialogs/logsql/
- Querying guide: https://docs.victoriametrics.com/victorialogs/querying/
