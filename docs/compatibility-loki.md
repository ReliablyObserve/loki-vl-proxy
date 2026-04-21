# Loki Compatibility

This track measures how closely the proxy behaves like Loki on Loki-facing APIs and LogQL behavior.

## Scope

- `/labels`, `/label/<name>/values`, `/series`, `/query`, `/query_range`
- LogQL parser, filter, metric, and OTel label compatibility
- Synthetic compatibility labels the proxy must expose to Loki clients, such as `service_name`
- Manifest-driven query semantics parity against real Loki in [query-semantics-matrix.json](../test/e2e-compat/query-semantics-matrix.json)
- Tracked operation inventory in [query-semantics-operations.json](../test/e2e-compat/query-semantics-operations.json)

## CI And Score

- Workflow: `compat-loki.yaml`
- Score test: `TestLokiTrackScore`
- Required PR gate: `loki-pinned`, which runs the `TestQuerySemantics*` inventory + matrix suite
- Runtime matrix: real Loki images
- Support window: current Loki minor family plus one minor family behind

The Loki matrix is a moving window. When a new Loki minor becomes current, the matrix advances to that family and the immediately previous minor family. Older minors drop out of support.

## Version Matrix

| Loki version | Coverage path | Version-specific focus |
|---|---|---|
| `3.7.1` | PR and main CI pinned runtime | Primary supported reference |
| `3.7.0` | Scheduled and manual matrix | `detected_level` metric grouping, OTel label parity |
| `3.6.10` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.9` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.8` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.7` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.6` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.5` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.4` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.3` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.2` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.1` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.0` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |

## Edge Cases Covered

- JSON and logfmt parser chains followed by field filters
- `detected_level` grouped metric queries used by Grafana log volume panels
- OTel dotted and underscore label parity through the underscore proxy
- Series and label-value parity for labels synthesized by the proxy

## Query Families In The Loki Semantics Matrix

The Loki semantics matrix focuses on query combinations where the proxy should match Loki as closely as possible on:

- HTTP status
- payload `status`
- `errorType`
- `resultType`
- line-count parity for log streams
- series-count parity for vectors and matrices
- exact metric-label-set parity for label-sensitive metric families such as bare parser metrics

The tracked operation inventory in [query-semantics-operations.json](../test/e2e-compat/query-semantics-operations.json) is machine-checked in CI. Every matrix case must belong to at least one inventory operation, and every inventory operation must reference live matrix cases.

Covered valid families:

| Family | Representative cases |
|---|---|
| Stream selectors | exact match, multi-label match, regex, negative match |
| Line filters | `|=`, `!=`, `|~`, `!~`, chained filter pipelines, negative-regex exclusions |
| Parser pipelines | `json`, `logfmt`, `regexp`, `pattern`, parser plus exact/regex/numeric field filter, `label_format` |
| Metric range queries | `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, `absent_over_time`, grouped range aggregations, parser-inside-range filters, bare unwrap range functions |
| Aggregations | `sum by(...)`, `without(...)`, `topk(...)`, `bottomk(...)`, `sort(...)`, `sort_desc(...)` |
| Binary operations | scalar comparisons/math, `bool` comparisons, and vector-to-vector operations such as `/`, `and`, `or`, and `unless` over valid metric expressions |

Detailed operation inventory:

| Category | Operations currently enforced in CI |
|---|---|
| Selectors | exact selectors, multi-label regex selectors, negative-regex selectors |
| Line filters | `|=`, `!=`, `|~`, `!~`, mixed chained filters |
| Parsers | `json`, `logfmt`, `regexp`, `pattern` plus parsed/extracted field filters |
| Formatting | `line_format`, `label_format`, `keep`, `drop` |
| Metric functions | `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, `absent_over_time`, `sum/avg/max/min/first/last/stddev/stdvar_over_time` with `unwrap`, `quantile_over_time` with `unwrap` |
| Aggregations | `sum by(...)`, `sum without(...)`, `topk`, `bottomk`, `sort`, `sort_desc` |
| Binary operators | scalar math/comparison, scalar `bool` comparison, vector arithmetic, `on(...)`, `group_left(...)`, logical `and`, `or`, `unless` |
| Invalid shapes | log-query aggregation misuse, missing metric range, malformed selector/parser syntax, invalid log-query binary ops |

Explicit invalid families:

| Family | Representative cases |
|---|---|
| Metric aggregation over a log query | `sum by(job) ({selector})` |
| Post-aggregation over a log query | `topk(2, {selector})`, `sort({selector})` |
| Missing range on a metric function | `rate({selector})` |
| Malformed selector / syntax | broken braces, parser syntax errors |
| Invalid binary shape | log-query to scalar/vector binary expressions |

These are intentionally called out because they are easy to regress while changing translation, shaping, or query planning.

## What Stays Outside Loki Parity

Some important compatibility behavior is still tested, but it is not part of the strict Loki parity matrix:

- synthetic `service_name` recovery when the backend only has structured metadata
- Drilldown helper endpoints like detected labels, detected fields, field values, volume, and patterns
- stale-on-error helper behavior under VictoriaLogs failures

Those cases live in the proxy contract suite because Loki itself is not the source of truth for them.

## Parity Rule

Valid Loki behavior is not an allowed exclusion category.

If a query shape works in real Loki and the proxy does not match it, that is treated as a parity bug and should be fixed or tracked with an explicit regression case. The bare parser-metric and bare `unwrap` metric shapes now live inside the required matrix for that reason.

## Detailed Edge Cases Now Gated

The required matrix is intentionally not limited to happy-path selectors. It now includes:

- parser-derived metric labelsets, not just result counts
- bare `unwrap` range functions where Loki keeps parsed labels but not the unwrap target field itself
- `pattern` parser extraction semantics
- set-style binary operators such as `or` and `unless`
- `bool` comparison semantics on metric expressions
- metric aggregations that do not group by service labels must not receive synthetic `service_name="unknown_service"` in query/query_range responses
- invalid log/metric shape rejections that must fail with the same class of error as Loki

When a new LogQL family is implemented or fixed in the proxy, the expectation is to add:

1. a runtime matrix case in `query-semantics-matrix.json`
2. an inventory entry in `query-semantics-operations.json`
3. a local or unit regression if the fix needed proxy-side translation or shaping changes
