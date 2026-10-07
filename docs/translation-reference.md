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
| `\| line_format "{{.x}}"` | `\| format "<x>"`; in the Loki-compatible profile a log query's `line_format` that no later line filter, parser or `decolorize` reads is not sent: the proxy renders it from the labels the entry carries, and VictoriaLogs returns the stored line; one that stays is preceded by `\| copy _msg as _lvp_line`, so the response carries the stored line too |
| `\| label_format x="{{.y}}"` | `\| format "<y>" as x` |
| `\| label_format a="{{.x}}", b="{{.y}}"` | `\| format "<x>" as a \| format "<y>" as b` |
| `\| label_format dst=src` | `\| format if (src:*) "<src>" as dst skip_empty_results \| delete src` — dst is kept when src is missing, as in Loki |
| `\| drop a, b` | `\| delete a, b` — bare field names, unconditional |
| `\| drop level="debug"` | proxy post-processes each entry: removes `level` from stream labels, structured metadata and parsed fields when value matches |
| `\| drop status=~"5.."` | proxy regex match: removes `status` when value matches regex |
| `\| keep a, b` | `\| fields _time, _msg, _stream, a, b` — bare field names |
| `\| keep method="GET"` | proxy post-processes: strips `method` (including a stream label) from entries where value ≠ `"GET"` |
| `\| keep status!~"2.."` | proxy negative-regex: removes `status` from entries where value does not match |
| `\| drop status!~"2.."` | proxy negative-regex: removes `status` when value does not match `2..` (the `!~` matcher form is recognised since v1.61.0) |

:::note Drop/keep scope
Both forms mutate stream labels as well as structured metadata and parsed fields. Matcher conditions (`=`, `!=`, `=~`, `!~`) remove a stream label only on entries whose value matches (drop) or does not match (keep); the response stream key is recomputed from the remaining labels. Under `label-style=underscores` the Loki name (`service_name`) is also matched against the stored dotted field (`service.name`).
:::

**Stream label exposure**: `| keep app, level` instructs VL to project only `_time, _msg, _stream, app, level`. The proxy then removes stream labels outside the keep list from the Loki response label set, so only `app` and `level` remain.

## Proxy-Side Stages

These stages are executed at the proxy level (VL has no native equivalents):

| LogQL | Implementation |
|---|---|
| `\| decolorize` | ANSI escape sequence stripping via regex |
| `\| ip("10.0.0.0/8")` | IP address, CIDR or range filtering. Arguments are validated at parse time; line filters accept only `\|= ip(...)` and `!= ip(...)`. Line filters translate to regular-expression approximations, so non-octet IPv4 prefixes, IPv4 ranges and IPv6 forms are not matched exactly. Label filters (`addr = ip("cidr")`) use VictoriaLogs `ipv4_range()` for IPv4 CIDRs when the backend supports it, otherwise a regular expression |
| `\| line_format` (templates) | Go `text/template` (ToUpper, ToLower, default, etc.) with bounded work: 64 KiB output per line and 16 MiB per response, plus template depth, input and execution limits. Exceeding a limit returns HTTP 400 |

## Metric Queries

### Series identity

Loki names the series of a metric query with the stream labels plus every label the pipeline extracted, so one stream can answer with many series.

- `| regexp` and `| pattern` name their captures in the query, so the proxy adds them to the grouping VictoriaLogs already computes (`... | stats by (_stream, level, <captures>) count()`) and the aggregation stays in the backend.
- `| json` and `| logfmt` produce a key set that is known only once a line is read, which VictoriaLogs cannot group by. The proxy keeps the stream identity for them, which returns fewer series than Loki; `-exact-parser-series-identity=true` names those series with the parsed labels instead, at the cost of evaluating the query from rows. See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for the measured difference.

### Range Vector Functions

| LogQL | LogsQL |
|---|---|
| `rate({...}[5m])` | `stats count()` + `math` normalization by window seconds, then `stats sum(...)` per grouping |
| `count_over_time({...}[5m])` | `... \| stats count()` |
| `bytes_over_time({...}[5m])` | `... \| stats sum_len(_msg)` |
| `bytes_rate({...}[5m])` | `stats sum_len(_msg)` + `math` normalization by window seconds, then `stats sum(...)` per grouping |
| `rate({...} \| unwrap f [5m])` | the sum of `f` per second: `... <gate> \| stats sum(__lvp_v)` + `math` normalization by window seconds (not a line rate) |
| `sum_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats sum(__lvp_v)` |
| `avg_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats avg(__lvp_v)` |
| `max_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats max(__lvp_v)` |
| `min_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats min(__lvp_v)` |
| `first_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats first(__lvp_v)` |
| `last_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats last(__lvp_v)` |
| `stddev_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats stddev(__lvp_v)` |
| `stdvar_over_time({...} \| unwrap f [5m])` | proxy binary expression: `(... <gate> \| stats stddev(__lvp_v)) ^ 2` |
| `quantile_over_time(0.95, {...} \| unwrap f [5m])` | proxy exact raw-sample evaluator for instant and range queries (Loki interpolates between ranked samples; VL `quantile` uses a different rank selection and tumbling buckets). Grouping is preserved and samples at the evaluation timestamp are included |
| `rate_counter({...} \| unwrap f [5m])` | `... <gate> \| stats __rate_counter__(__lvp_v)` (answered by the proxy's raw-sample evaluator) |
| `absent_over_time({...}[5m])` | `... \| stats count()` |

#### Unwrap sample validity

Loki makes a sample of a line only when its unwrapped label is present and converts (`pkg/logql/log/metrics_extraction.go`): a line without the label (or with it empty) makes no sample, and a value `strconv.ParseFloat` rejects is marked `__error__="SampleExtractionErr"`, which `| __error__=""` drops. VictoriaLogs' stats functions parse leniently (`sum` reads "86282s" as a number of nanoseconds and "1KiB" as 1024, `max` and `min` compare strings, a group without a value answers an empty string or `NaN`), so every stats pipe over a plain unwrapped label reads only the rows that make a sample. In the table above `<gate>` is, right before the stats pipe (after the pipes that resolve a parsed key),

```
| filter "f":~"<decimal number syntax of strconv.ParseFloat, plus inf>" | math "f" as __lvp_v
```

The filter keeps the rows whose value converts in Loki's syntax (a missing or empty label never matches), and the `math` pipe converts each kept row on its own, which the stats functions' value parsing does not do reliably (a group of one row reads `+5`, `.5` or `1e3` as `NaN`). The field is quoted because the `math` pipe reads a bare `max`, `abs` or `rand` as a function. The syntax is the one of `strconv.ParseFloat` for a plain value (digit separators, an exponent up to 307, `inf`), of `time.ParseDuration` for `unwrap duration(f)` (compound terms of `ns`, `us`, `µs`, `ms`, `s`, `m`, `h`, converted to seconds) and of `humanize.ParseBytes` for `unwrap bytes(f)` (case-insensitive SI and IEC units, an optional space, commas in the number, floored to whole bytes). Nothing is read row by row. Values the filter treats differently from Loki are registered in `semantics/unwrap-gate-parsefloat-divergences`: the filter drops hexadecimal floats, `infinity`, `nan`, a plain exponent of 308 and, for `bytes()`, a number that starts with a comma or a no-break space before the unit; it keeps a duration beyond int64 nanoseconds and a byte size at or above 2^64, which Loki rejects. The functions VictoriaLogs has no exact stats for (`quantile_over_time`, `rate_counter`, `first_over_time`, `last_over_time`) are answered by the bounded raw-sample evaluator, which converts with the same Go functions.

#### Unwrap conversion errors

A value its conversion rejects fails the query in Loki with HTTP 400 and `pipeline error: 'SampleExtractionErr' for series: '{...}'` (written as `text/plain`), unless a label filter after the unwrap drops the error (`| unwrap f | __error__=""`, `__error__!="SampleExtractionErr"`, `__error_details__!=""`). A filter or `drop __error__` before the unwrap, and `by (__error__)`, do not drop it; a missing or empty label makes no sample and no error. The value is the one Loki's parser gives: `| json` skips arrays and `null` and keeps booleans, `| unpack` adds the strings of a line holding `_entry` only, and a key named like stored structured metadata becomes `name_extracted`.

The proxy detects it in the metric's own VictoriaLogs scan. When no filter after the unwrap drops the error, the gate counts instead of dropping:

```
| format if ("f":* -"f":~"<every form the conversion may accept>") "1" as __lvp_bad
| filter "f":~"<gate>" or __lvp_bad:="1" | <the gate's conversion pipes> | format if (__lvp_bad:="1") "0" as __lvp_v
| stats by (<grouping>, __lvp_bad) ...           (every stats pipe of the query)
```

The rejected rows form groups of their own, so every other group aggregates exactly the rows it did; the flagged groups are taken out of the answer before it is read (a clean answer only loses the empty `__lvp_bad` label), and the buckets they held are remembered. The raw-row evaluators (quantile, first, last, rate_counter, bare parsers) report the rows they cannot convert. Only after such a hit, at most 4 `| limit 3` lookups restricted to the flagged buckets and Loki's windows read rows (the first confirmed row stops them), each row's stored line is re-read by stream id, time and exact `_msg` in parallel, and Loki's value is re-derived with Loki's parsers (`| logfmt` keeps the first non-empty value of a key, maps U+FFFD to a space and unquotes as Loki's decoder; `| unpack` keeps the last): only a value Loki's conversion rejects, on a line in an evaluated window that passes Loki's label filters, answers Loki's text over the line's labels (stream labels, structured metadata, parsed labels; only the labels Loki's parser hints keep when a `sum` pushes its grouping down, `x_extracted` hinting `x` as well). A clean query reads no row; a query VictoriaLogs flags but Loki converts pays the lookups (about 0.01 s and 16 small queries over 1 h). Shapes left out (a post filter that keeps only the error or compares a label, `absent_over_time`, `label_replace`, a value Loki rejects hidden by a later duplicate logfmt key, `unwrap duration_seconds`, pipelines the confirmation does not re-derive) answer as before and are registered as open cases.

### Outer Aggregations

| LogQL | LogsQL |
|---|---|
| `sum(rate({...}[5m]))` | normalized per-stream/per-group rate, then proxy applies outer aggregation where supported |
| `sum(rate({...}[5m])) by (x)` | `... \| stats by (x) count() as __lvp_inner \| math __lvp_inner/window as __lvp_rate \| stats by (x) sum(__lvp_rate)` |
| `avg(rate({...}[5m])) by (x)` | same normalized-rate path grouped by `(x)` |
| `topk(10, rate({...}[5m]))` | normalized-rate path with stream grouping; on range queries the proxy ranks series independently at each timestamp, so a range can return more than `k` distinct series, each with only its winning samples |

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

Binary expression notes:

- Each operand is executed through the normal query handlers, so trailing range windows, parser errors and extraction aliases are preserved. Operator precedence, parentheses and comparison filtering versus `bool` follow Loki.
- Vector-vector operands are evaluated on the step-aligned grid, as Loki's query frontend does with `align_queries_with_step` enabled. Against a default Loki, results for an unaligned `start` can differ by less than one step.
- Implicit many-to-one matches (without `group_left`/`group_right`) are rejected with HTTP 500 and Loki's `multiple matches for labels` error; cardinality is checked at each timestamp.
- Evaluation is bounded: 64 nesting levels, 1,024 child evaluations, 256 MiB of captured child responses, two million decoded arrays, one million output samples and 64 MiB of label work, with each encoded result capped at 64 MiB. A valid expression exceeding these limits fails explicitly.

## Proxy Compatibility Layer

The following Loki semantics are implemented in the proxy to bridge gaps where VictoriaLogs primitives do not directly match Loki behavior.

### Time Semantics

| LogQL feature | Proxy behavior |
|---|---|
| `offset 1h` on range vectors | Supported: proxy strips the offset clause and shifts `start`/`end` (or `time` for instant queries) backward by the offset duration before backend dispatch; multiple distinct offsets in the same query return HTTP 400 |
| `@ <timestamp>` modifier | Normalized/stripped in translation for VictoriaLogs backend requests |
| Subquery `max_over_time(rate(...)[1h:5m])` | Not LogQL: Loki's grammar has no `[range:step]` form. Rejected with Loki's HTTP 400 parse error (`syntax error: unexpected RATE, expecting NUMBER or { or (`) before any backend call |
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