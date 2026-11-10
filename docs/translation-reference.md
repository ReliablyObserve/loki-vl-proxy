---
sidebar_label: LogQL → LogsQL Reference
description: Complete mapping of LogQL operators, parsers, metric functions, and filter expressions to VictoriaLogs LogsQL.
---

# LogQL to LogsQL Translation Reference

For label/field exposure mode behavior (`label-style`, `metadata-field-mode`, `emit-structured-metadata`, and custom mappings), see [Translation Modes Guide](translation-modes.md).

The queries in this reference are translated by one of two paths that write the same VictoriaLogs query, byte for byte: log queries with a stream selector, line filters, pattern filters and label filters and no parser stage come from the typed query-plan lowering (`-query-plan-lowering`, on by default, `false` returns them to the string translator); every other query, such as parsers, formatting stages and metric queries, comes from the string translator. See [LogQL Parser](logql-parser.md#query-plan-lowering-which-translator-answers).

## Stream Selectors

| LogQL | LogsQL | Notes |
|---|---|---|
| `{app="nginx"}` | `app:=nginx` | Field filter (not stream filter) |
| `{app!="debug"}` | `-app:=debug` | Negative equality |
| `{app=~"ng.*"}` | `app:~"^(?s:ng.*)$"` | Regex match, anchored to the whole value as Prometheus compiles it |
| `{app!~"test.*"}` | `-app:~"^(?s:test.*)$"` | Negative regex, anchored |

Selector regexps (`=~`, `!~`) match the whole label value, as Prometheus' `labels.NewMatcher` compiles them (`^(?s:…)$`), because VictoriaLogs' `field:~"re"` is a substring match. Where an equivalent filter is cheaper, the proxy sends that instead: a whole-value literal as an exact filter (`{app=~"api"}` → `app:="api"`), `.*` and `.+` as they are (VictoriaLogs' match-all and non-empty filters), and `.*literal.*` as the substring regexp `app:~"literal"`. Line filters (`|~`, `!~`) are substring regexps in Loki and are passed through unchanged.

All stream matchers are converted to field filters (not VL `{...}` stream selectors) because VL stream filters only match declared `_stream_fields`.

## Line Filters

| LogQL | LogsQL | Notes |
|---|---|---|
| `\|= "error"` | `~"error"` | Substring match (not word-only) |
| `!= "debug"` | `NOT ~"debug"` | Negative substring |
| `\|~ "err.*"` | `~"err.*"` | Regex match |
| `!~ "debug.*"` | `NOT ~"debug.*"` | Negative regex |
| `\|= "\xff\xfe"` (text that is not UTF-8) | `_stream_id:in()` | No stored line holds such bytes, so the filter keeps no line, as in Loki; `!=` keeps every line (no filter) and an `or` chain keeps its valid alternatives |

## Parser Stages

| LogQL | LogsQL |
|---|---|
| `\| json` | `\| unpack_json` |
| `\| unpack` | `\| unpack_json` |
| `\| logfmt` | `\| unpack_logfmt` |
| `\| pattern "<ip> ..."` | `\| extract "<ip> ..."` |
| `\| regexp "..."` | `\| extract_regexp "..."` |
| `\|> "pattern"` | Pattern match line filter (Loki 3.7+). Two captures in a row (`<_><_>`) or a named capture (`<foo>`) is Loki's 400 with Loki's text; only `<_>` is a capture here |

`| json` and `| logfmt` write the same pipe whatever their flags and extraction list: `| logfmt --strict --keep-empty a, b` is `| unpack_logfmt`, and `| json a, b="x.y"` is `| unpack_json` followed by `| copy "x.y" as b` for each label defined by an expression. A json expression written with escapes, as a raw string, with a comma, with a leading or trailing space or as a bracket path is read from the stage's text by the string translator, and the query plan leaves such a query to it.

The query plan lowers a log query with these parsers, `| regexp`, `| pattern`, `| decolorize`, `| drop`, `| keep`, `| line_format` and `| label_format` with a template of text and plain `{{.name}}` fields, and the label filters after a parser (below), to the same LogsQL, under every translator profile (a regexp or pattern argument written with escapes that Go spells otherwise, such as `\x41`, is written in the canonical spelling: the same query to VictoriaLogs, not the same bytes). A template with a function, a pipe, a condition, a dotted field, a trim marker or an angle bracket in its text stays with the string translator, which copies it into the query as it is; so does `| unpack` (`conformance/registry/query-plan.json` lists the rows each construct holds).

## Label Filters

| LogQL | LogsQL |
|---|---|
| `\| label == "val"` | `label:=val` |
| `\| label != "val"` | `-label:=val` |
| `\| label =~ "5.."` | `label:~"^(?-s:5..)$"` |
| `\| label !~ "GET\|HEAD"` | `-(label:="GET" OR label:="HEAD")` |
| `\| label > 500` | `label:>500` |
| `\| label >= 500` | `label:>=500` |
| `\| label < 200` | `label:<200` |
| `\| label <= 200` | `label:<=200` |

Every label filter of a query applies where it stands, as in Loki: `| a="1" | a="2"` keeps no line and `| a="1" | a!="1"` keeps none (the translator used to keep only the latest filter on a label). After a parser stage, label filters are wrapped as `| filter <expr>`. For example `| json | status >= 400` is correctly supported; earlier proxy versions incorrectly rejected such filters at the syntax validation stage.

A label filter after a `| json` or `| logfmt` parser is a filter pipe of its own (`| unpack_json | filter status:>=400`), a line filter that follows joining the previous one, and it names every key the parser can have stored the label under: Loki sanitizes `http.status` and `http-status` to the label `http_status`, VictoriaLogs keeps the key, so `| json | http_status="5"` is `filter (http_status:="5" OR "http.status":="5" OR "http-status":="5")` (a filter that accepts the empty value, such as `=""`, holds when every key does, and `!=""` keeps the label's own key). A label a json stage defines (`| json st="status" | st>=500`) is read from the key it names, `__error__` and other labels that start with an underscore name themselves, and a filter on a stream label after a parser (profiles that know the stream labels) reads the stream's value from a copy made ahead of the parser (`__lxsv_*`, deleted at the end); a filter on `x_extracted` reads the parsed key `x` where the stream has the label `x`, from the second unpack of the keys the parsers do into `__lxp*` scratch fields. The query plan writes all of these as the translator does, byte for byte, for a `| json` or `| logfmt` that extracts every key. It leaves to the string translator a numeric, duration or bytes value compared with `=`, `==` or `!=` (the translator writes the string form of the number, a known difference from Loki that the plan will fix as a recorded divergence), a filter on `detected_level` or `service_name` (derived labels), a filter on a stream label inside an `and`/`or` tree, a label a json extraction list defines in a form the translator reads from the stage text, a label named `extract`, and `x_extracted` filters in a query whose parsers have flags or extraction lists.

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
| `\| ip("10.0.0.0/8")` | IP address, CIDR or range filtering. Arguments are validated at parse time; line filters accept only `\|= ip(...)` and `!= ip(...)`. IPv4 line filters keep exactly the lines Loki keeps: the address set is written as exact octet ranges with Loki's context around them (a digit or a dot right after the address makes it another address), after a literal of the octets every address fixes, which VictoriaLogs searches first. IPv6 line filters stay a regular-expression approximation. An IPv4 label filter (`addr = ip("cidr")`, `!=`) is `ipv4_range()` on a block VictoriaLogs stores as IPv4 addresses (`value_type(ipv4)`) and, on a block that holds text, the regexp of the `ip()` line filter after a substring prefilter (the fixed octets, an alternation of a few first octets, or the dot of an address), which reads an address inside other text as Loki does; an IPv6 label filter is `ipv6_range()` on a whole address (a subset of Loki's lines) |
| `\| line_format` (templates) | Go `text/template` (ToUpper, ToLower, default, etc.) with bounded work: 64 KiB output per line and 16 MiB per response, plus template depth, input and execution limits. Exceeding a limit returns HTTP 400 |

## Metric Queries

### Series identity

Loki names the series of a metric query with the stream labels plus every label the pipeline extracted, so one stream can answer with many series.

- `| regexp` and `| pattern` name their captures in the query, so the proxy adds them to the grouping VictoriaLogs already computes (`... | stats by (_stream, level, <captures>) count()`) and the aggregation stays in the backend.
- `| json` and `| logfmt` produce a key set that is known only once a line is read, which VictoriaLogs cannot group by. The proxy keeps the stream identity for them, which returns fewer series than Loki; `-exact-parser-series-identity=true` names those series with the parsed labels instead, at the cost of evaluating the query from rows. See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for the measured difference.

### Range Vector Functions

The query plan (held to the string translator by the shadow; requests are still the translator's) writes `count_over_time`, `bytes_over_time` and `absent_over_time` over a log query with line, pattern and label filters and no parser as one `stats` pipe with no grouping over the log query (written with the label names alone: the capabilities and stream fields of the request do not change it), so VictoriaLogs counts or sums per step bucket and the proxy never reads rows. `rate` and `bytes_rate` over the same log queries are written as the log query's filters, the chain of pipes that derives Loki's `detected_level` for every row (once, before the query's first pipe), `stats by (_stream, level, detected_level)` of the count (or of the line bytes), the division by the range's seconds, and the sum per series. The offset and the range stay out of the query, except the range's seconds in that division; the proxy aligns the window by them. With the tenant's stream labels the query restores their stream values after the chain. A range aggregation with a parser, an unwrapped value, a grouping, a `| drop` or `| keep`, a `detected_level` label filter or a stage the log query lowering does not write is still written by the string translator. `sum`, `count`, `min`, `max` and `avg`, with or without a `by` or `without` clause, over those five functions are written the same way: a `sum` groups the stats by the labels of the clause (a `sum` of a `rate` first counts per `by` label set and divides by the range's seconds); `count`, `min`, `max` and `avg` read one value per Loki series, so an inner `stats` groups by the clause's labels and then the stream identity (`_stream, level`, with `detected_level` appended), and the outer `stats` aggregates those values; a `without` clause writes no grouping and the proxy folds the series itself (the `__without__:<op>:<labels>` marker, held as a `Fold` around the query). `service_name` and `detected_level` in a grouping add the pipes that derive them before the log query's first pipe. A vector aggregation over other ones (two or more levels of `sum`, `count`, `min`, `max` and `avg`, each with its own `by` or `without` clause or none, over those five functions) is written as the innermost level's query followed by one `stats` pipe per level above it over the rows of the one below (the value of each stats named `__lvp_inner` for the next); a level keeps exactly the labels of its own clause that the level below kept (a `by` list is cut to them, a `without` list is subtracted from them, no clause keeps none and gets a `stats` with no grouping), and when the innermost level is a `without` the query is that level's series rows and the proxy folds every level over them (the marker carries the innermost labels and the top operator, held as a `Fold`). `stddev` and `stdvar` at any level (the proxy folds the operand's series itself and never sends the translator's text for them), a ranked aggregation, a binary or `label_replace` under a nest, a nest over an unwrapped label, a binary and the shapes above stay with the string translator, as does a query whose text the translator reads as syntax inside a quoted value (a `without (...)`, a ` bool `, a `| stats ` in a line filter) and an aggregation with a blank between its operand's parenthesis and the range function (`sum( rate(...) )`, `rate (`), which the translator returns as a phrase search (the lowering checks the query text for it: `lower.ScreenText`). A range aggregation spelled with a blank, tab or newline between its function and its parenthesis (`rate ({app="a"}[5m])`), at the top of a query, as an operand of a binary operation or as the first argument of a `label_replace` call, is a phrase search to the translator as well, where Loki reads the range aggregation (`semantics/translator-reads-blank-before-range-call-as-text`); the lowering leaves it to the translator (`RangeAgg spelling`).

The query plan writes the range functions over an unwrapped label (`sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `first_over_time`, `last_over_time`, `stddev_over_time`, `stdvar_over_time`, `quantile_over_time`, `rate_counter`, and `rate` over a label), with a plain, `bytes()`, `duration()` or `duration_seconds()` value and a `by` clause on the range, and `sum`, `count`, `min`, `max` and `avg` over them, as the translator does: the log query of the metric row, the unwrap gate (the rows whose value converts as Loki converts it, the converted value in `__lvp_v`) and the stats pipes. A function alone aggregates the rows of its group; `first_over_time` and `last_over_time` keep the row of the earliest (`row_min`) or latest (`row_max`) time per series, unpack its value and take the largest per group; under a vector aggregation the series identity is the stream (the stream and its level for a `rate`), a `sum` aggregates the rows, `count`, `min`, `max` and `avg` read a value per series, and a `without` clause leaves the fold to the proxy; `stdvar_over_time` is the deviation squared by the proxy (a binary expression). A `quantile_over_time` under a vector aggregation takes the quantile of each group of rows, as the translator does. A query with a parser, a range-level `without`, a quantile parameter written other than as its shortest form (`0.50`), an unwrap stage not spelled with one blank on each side, or quoted text the translator reads as syntax (`| unwrap`, `| json`, `| stats`) stays with the string translator.

The same functions over a log query with a `| json` or `| logfmt` parser (flags, extraction lists and aliases, label filters behind the parser) are written the same way, with the series the translator takes them to have: the stream (a `rate` groups by it and derives no `detected_level`; `count`, `min`, `max` and `avg` add it to the clause's labels), and for an unwrapped label each line of the stream (`_stream, _msg`) unless a clause groups it. A tenant with stream labels gets every stream value copied aside before the first parser, put back behind each parser and deleted at the end; a logfmt unpack that ends the query reads only the unwrapped label and the clause's labels. A grouping or unwrapped label with an underscore (which a parser may have stored under another spelling), a `without` clause over a parser, `| regexp`, `| pattern`, `| unpack` and the stages the log query lowering does not write stay with the string translator.

| LogQL | LogsQL |
|---|---|
| `rate({...}[5m])` | `stats count()` + `math` normalization by window seconds, then `stats sum(...)` per grouping |
| `count_over_time({...}[5m])` | `... \| stats count()` |
| `bytes_over_time({...}[5m])` | `... \| stats sum_len(_msg)` |
| `absent_over_time({...}[5m])` | `... \| stats count()`; the proxy routes `absent_over_time` before it translates and asks the count path for the presence of samples |
| `bytes_rate({...}[5m])` | `stats sum_len(_msg)` + `math` normalization by window seconds, then `stats sum(...)` per grouping |
| `rate({...} \| unwrap f [5m])` | the sum of `f` per second: `... <gate> \| stats sum(__lvp_v)` + `math` normalization by window seconds (not a line rate) |
| `sum_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats sum(__lvp_v)` |
| `avg_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats avg(__lvp_v)` |
| `max_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats max(__lvp_v)` |
| `min_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats min(__lvp_v)` |
| `first_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats by (<series>) row_min(_time, __lvp_v) as __lvp_inner \| unpack_json from __lvp_inner fields (__lvp_v) \| math __lvp_v as __lvp_inner \| stats by (<series>) max(__lvp_inner)` (VictoriaLogs has no `first`; the earliest row per series) |
| `last_over_time({...} \| unwrap f [5m])` | the same with `row_max` (the latest row per series) |
| `stddev_over_time({...} \| unwrap f [5m])` | `... <gate> \| stats stddev(__lvp_v)` |
| `stdvar_over_time({...} \| unwrap f [5m])` | proxy binary expression: `(... <gate> \| stats stddev(__lvp_v)) ^ 2` |
| `quantile_over_time(0.95, {...} \| unwrap f [5m])` | proxy exact raw-sample evaluator for instant and range queries (Loki interpolates between ranked samples; VL `quantile` uses a different rank selection and tumbling buckets). Grouping is preserved and samples at the evaluation timestamp are included |
| `rate_counter({...} \| unwrap f [5m])` | `... <gate> \| stats __rate_counter__(__lvp_v)` (answered by the proxy's raw-sample evaluator) |
| `absent_over_time({...}[5m])` | `... \| stats count()` |

#### Unwrap sample validity

Loki makes a sample of a line only when its unwrapped label is present and converts (`pkg/logql/log/metrics_extraction.go`): a line without the label (or with it empty) makes no sample, and a value `strconv.ParseFloat` rejects is marked `__error__="SampleExtractionErr"`, which `| __error__=""` drops. VictoriaLogs' stats functions parse leniently (`sum` reads "86282s" as a number of nanoseconds and "1KiB" as 1024, `max` and `min` compare strings, a group without a value answers an empty string or `NaN`), so every stats pipe over a plain unwrapped label reads only the rows that make a sample. In the table above `<gate>` is, right before the stats pipe (after the pipes that resolve a parsed key),

```
| filter "f":~"<syntax of strconv.ParseFloat>" | copy "f" as __lvp_n | replace_regexp ... at __lvp_n | math __lvp_n as __lvp_v | filter __lvp_v:<=inf
```

The filter keeps the rows whose value converts in Loki's syntax (a missing or empty label never matches), and the `math` pipe converts each kept row on its own, which the stats functions' value parsing does not do reliably (a group of one row reads `+5`, `.5` or `1e3` as `NaN`). The field is quoted because the `math` pipe reads a bare `max`, `abs` or `rand` as a function. The syntax is the one of `strconv.ParseFloat` for a plain value (decimal and hexadecimal floats, digit separators, `inf` and `infinity`, which is shortened to `inf` on a copy because the `math` pipe does not read it), of `time.ParseDuration` for `unwrap duration(f)` (compound terms of `ns`, `us`, `µs`, `ms`, `s`, `m`, `h`, converted to seconds) and of `humanize.ParseBytes` for `unwrap bytes(f)` (case-insensitive SI and IEC units, an optional space, commas in the number, floored to whole bytes). Nothing is read row by row. A value that is a number by syntax and overflows (`1e309`, `1000e307`, `0x1p2000`, `2562048h`, `19EB`) passes the filter and is dropped by the range filter after the `math` pipe (VictoriaLogs answers `NaN` for an overflowing float, saturates a duration at int64 nanoseconds and computes a size in float64, whose limit is 2^64), and the counting gate of the next section flags it so the query fails like Loki's. A `nan` value is a sample for Loki (`sum`, `avg`, `stddev` and `rate` over a window that holds one are `NaN`, `max` and `min` skip it) and VictoriaLogs' `sum` restarts at a NaN, so the counting gate keeps the rows apart and the proxy applies them to the answer. Values the gate treats differently from Loki are registered in `semantics/unwrap-gate-parsefloat-divergences`: a duration within about a microsecond of int64 nanoseconds (292 years) is dropped, and a `nan` is dropped by `max`/`min` windows made only of nan values and by a `max`, `min` or `count` of series that a nan made NaN. The functions VictoriaLogs has no exact stats for (`quantile_over_time`, `rate_counter`, `first_over_time`, `last_over_time`) are answered by the bounded raw-sample evaluator, which converts with the same Go functions.

#### Unwrap conversion errors

A value its conversion rejects fails the query in Loki with HTTP 400 and `pipeline error: 'SampleExtractionErr' for series: '{...}'` (written as `text/plain`), unless a label filter after the unwrap drops the error (`| unwrap f | __error__=""`, `__error__!="SampleExtractionErr"`, `__error_details__!=""`). A filter or `drop __error__` before the unwrap, and `by (__error__)`, do not drop it; a missing or empty label makes no sample and no error. The value is the one Loki's parser gives: `| json` skips arrays and `null` and keeps booleans, `| unpack` adds the strings of a line holding `_entry` only, and a key named like stored structured metadata becomes `name_extracted`.

The proxy detects it in the metric's own VictoriaLogs scan. When no filter after the unwrap drops the error, the gate counts instead of dropping:

```
| format if ("f":* -"f":~"<every form the conversion may accept>") "1" as __lvp_bad
| filter "f":~"<gate>" or __lvp_bad:="1" | <the gate's conversion pipes> | format if (__lvp_bad:="1") "0" as __lvp_v
| stats by (<grouping>, __lvp_bad) ...           (every stats pipe of the query)
```

The rejected rows form groups of their own, so every other group aggregates exactly the rows it did; the flagged groups are taken out of the answer before it is read (a clean answer only loses the empty `__lvp_bad` label), and the buckets they held are remembered. The raw-row evaluators (quantile, first, last, rate_counter, bare parsers) report the rows they cannot convert. Only after such a hit, at most 4 `| limit 3` lookups restricted to the flagged buckets and Loki's windows read rows (the first confirmed row stops them), each row's stored line is re-read by stream id, time and exact `_msg` in parallel, and Loki's value is re-derived with Loki's parsers (`| logfmt` keeps the first non-empty value of a key, maps U+FFFD to a space and unquotes as Loki's decoder; `| unpack` keeps the last): only a value Loki's conversion rejects, on a line in an evaluated window that passes Loki's label filters, answers Loki's text over the line's labels (stream labels, structured metadata, parsed labels; only the labels Loki's parser hints keep when a `sum` pushes its grouping down, `x_extracted` hinting `x` as well). A clean query reads no row; a query VictoriaLogs flags but Loki converts pays the lookups (about 0.01 s and 16 small queries over 1 h). Shapes left out (a post filter that keeps only the error or compares a label, `label_replace`, a value Loki rejects hidden by a later duplicate logfmt key, pipelines the confirmation does not re-derive) answer as before and are registered as open cases.

### Outer Aggregations

| LogQL | LogsQL |
|---|---|
| `sum(rate({...}[5m]))` | normalized per-stream/per-group rate, then proxy applies outer aggregation where supported |
| `sum(rate({...}[5m])) by (x)` | `... \| stats by (x) count() as __lvp_inner \| math __lvp_inner/window as __lvp_rate \| stats by (x) sum(__lvp_rate)` |
| `avg(rate({...}[5m])) by (x)` | same normalized-rate path grouped by `(x)` |
| `topk(10, rate({...}[5m]))` | normalized-rate path with stream grouping; on range queries the proxy ranks series independently at each timestamp, so a range can return more than `k` distinct series, each with only its winning samples |

Supported: `sum`, `avg`, `max`, `min`, `count`, `topk`, `bottomk`, `stddev`, `stdvar`, `sort`, `sort_desc`, `label_replace`.

Loki has no `group()` and no `label_join()` (nor the PromQL math and time functions): any name outside Loki's function tokens is a 400 `parse error at line 1, col N: syntax error: unexpected IDENTIFIER` on both query endpoints. Keywords are read in any case, as Loki's lexer does (`TOPK(3, ...)`, `SUM BY (a) (...)`).

`label_replace(v, dst, repl, src, regex)` is handled via a marker-suffix pattern: the inner expression is translated to VL, the spec is base64-encoded and embedded as a query suffix, the proxy strips the marker before sending to VL and applies the label operation to the matrix response.

`count_values(label, v)` is not LogQL — Loki v3.7.7 has no such aggregation. Queries using `count_values` get Loki's parse error (`parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`, at the position of the name).

### Binary Expressions

| LogQL | Proxy Behavior |
|---|---|
| `rate({...}[5m]) / rate({...}[5m])` | Both sides evaluated independently, combined at proxy |
| `rate({...}[5m]) * 100` | Scalar applied to each data point |
| `100 / rate({...}[5m])` | Reverse scalar operation |

Supported operators: `+`, `-`, `*`, `/`, `%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=`.

Binary expression notes:

- Each operand is executed through the normal query handlers, so trailing range windows, parser errors and extraction aliases are preserved. Operator precedence, parentheses and comparison filtering versus `bool` follow Loki.
- Vector-vector operands are evaluated on the step-aligned grid, as Loki's query frontend does with `align_queries_with_step` enabled. The proxy aligns metric `query_range` bounds to the step by default (`-align-queries-with-step`); a Loki with its defaults floors the start in its metric splitter too.
- Implicit many-to-one matches (without `group_left`/`group_right`) are rejected with HTTP 500 and Loki's `multiple matches for labels` error; cardinality is checked at each timestamp.
- Evaluation is bounded: 64 nesting levels, 1,024 child evaluations, 256 MiB of captured child responses, two million decoded arrays, one million output samples and 64 MiB of label work, with each encoded result capped at 64 MiB. A valid expression exceeding these limits fails explicitly.

The query plan (held to the string translator by the shadow; requests are still the translator's) writes a binary operation between two operands it writes natively (a range aggregation, a vector aggregation without a `without` clause, or a number; not two numbers) as the translator's text, `__binary__:<op>:<left>|||<right>` with the `on`, `ignoring`, `group_left` and `group_right` modifiers after it, held as a `physical.Binary` of the two operands' own queries; `label_replace` calls, nested ones too, around a range aggregation, a vector aggregation (a `without` clause included) or such a binary operation are written as the expression's query followed by one marker per call, innermost first (`physical.LabelMap`). The proxy still evaluates a binary operation by running each operand through its own handler. `and`, `or`, `unless`, `bool`, nested binary operations, `vector()`, rankings, a `without` operand, a `label_replace` operand and the operands the range and vector aggregation lowering leaves out stay with the string translator, as do the spellings the translator reads unlike Loki (an operator without a blank on each side, a number not written as Go prints it (`1e3`, `2.50`), a modifier label list not written with `, `, a parenthesized operand, a quoted string holding a parenthesis, two blanks or `bool`, `label_replace (` with a blank) and a `label_replace` around a `service_name` grouping, which the translator derives twice.

The query plan (held to the string translator by the shadow; requests are still the translator's) writes `topk`, `bottomk`, `sort` and `sort_desc` without a `by` or `without` clause over `count_over_time`, `bytes_over_time`, `absent_over_time`, `rate`, `bytes_rate` and the unwrap functions behind a `json` or `logfmt` parser as the operand's own query: the translator writes no ranking text (no marker, no pipe, `k` is not written), the proxy reads the ranking from the LogQL, asks VictoriaLogs for the operand alone and ranks each step of the answer itself. A `by` or `without` clause (the translator groups the operand's stats by it, where Loki ranks the operand's series `k` per group), an operand that is a vector aggregation, a ranking, a binary operation or `label_replace` (the translator writes the whole query as a phrase search), an unwrapped label with no parser (the translator groups it by the stream under a ranking only), a label filter after a parser and `rate` or `bytes_rate` over a parser (the lowering would allocate more than the translation), a `label_replace` or a binary operation around a ranking, and the spellings the translator reads unlike Loki (a blank before the operand's parenthesis, a parenthesized operand or ranking) stay with the string translator.

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