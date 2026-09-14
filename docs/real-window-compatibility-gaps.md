---
sidebar_label: Real-window compatibility gaps
description: Measured compatibility findings using isolated populated fixtures.
---

# Real-window compatibility findings

PR #526 remains a draft. Parser-error regressions are merge blockers. The security integration in PR #525 does not establish full LogQL parity.

## Timestamp defect

The exhaustive helper sent millisecond integers to Loki, which interprets integers as nanoseconds. Loki therefore queried 1970 while the proxy queried current data. RFC3339Nano timestamps now address the same populated window. A literal canary also requires both freshly ingested lines from both backends. See the [Loki timestamp contract](https://grafana.com/docs/loki/latest/reference/loki-http-api/#timestamps).

## Isolated baseline

A fresh Compose project with Loki 3.7.7, VictoriaLogs 1.50.0 and proxy 76063b3, without the UI generator, produced **283/284 query checks** and **64/70 error checks**. The query failure was implicit many-to-one matching. The six error failures were four invalid IP cases and a duplicated parser-error case.

The earlier 255/284 query result came from a long-running UI generator stack with repeated ingestion. Its reference timeouts, sort-memory failures and empty-result findings are not isolated reproductions. The regexp capture/filter query returns populated data on the clean stack.

The exhaustive checks compare status, result type and non-emptiness. Strict quantile canaries demonstrated wrong grouping and values even when those checks passed. They are insufficient proof of labels, samples or timestamps.

## Corrections under validation

- IP line filters reject invalid single addresses, prefixes, ranges and unsupported operators. Substring filters preserve literal regex metacharacters, including text resembling `ip(...)`.
- Grouped quantiles retain `by (labels)` and `by ()`, interpolate over trailing windows and include samples at the evaluation timestamp. The exact adapter uses bounded raw samples; it does not establish native quantile performance.
- Binary matching checks cardinality independently at each timestamp. Empty grouping modifiers retain their meaning, disjoint streams do not conflict, and set operations retain their many-to-many exemption.

These corrections reject previously accepted invalid queries and change incorrect numeric results. They do not imply unchanged behavior for all clients. IP validation is eager: Loki can bypass invalid pipeline construction for historical empty ranges, while the proxy rejects the invalid expression.

## Remaining blockers

### Parser error state

VictoriaLogs JSON unpacking does not produce Loki error labels. Filtering those labels after pushdown can lose malformed lines or include lines that Loki rejects. Stage order and aggregation hints affect the result:

| Pipeline inside a metric | Measured Loki behavior |
| --- | --- |
| JSON with surviving malformed input | Pipeline error in ordinary grouped/raw evaluation |
| JSON then empty-error filter | Valid parsed samples only |
| JSON then nonempty-error filter | Pipeline error |
| JSON then drop error | Valid and malformed samples accepted |
| JSON, nonempty-error filter, then drop error | Malformed samples accepted |
| Drop error before nonempty-error filter | Empty result |
| Drop error details only | Error state survives |

Aggregation can suppress parsing or retain error labels. A blanket syntax rejection or malformed-JSON preflight does not implement this contract. The new strict unique-fixture canary has **24/24 Loki checks passing** and **5/24 proxy checks passing, 19 failing** on the current execution semantics. These are failing coverage, not waived tests.

Sources: [Loki parser](https://github.com/grafana/loki/blob/v3.7.7/pkg/logql/log/parser.go), [parser hints](https://github.com/grafana/loki/blob/v3.7.7/pkg/logql/log/parser_hints.go), [evaluator](https://github.com/grafana/loki/blob/v3.7.7/pkg/logql/evaluator.go), [VictoriaLogs JSON unpacking](https://github.com/VictoriaMetrics/VictoriaLogs/blob/v1.50.0/lib/logstorage/pipe_unpack_json.go).

### Other limits

Valid label `ip()` syntax and exact matching of all IPv6/non-octet CIDR/range forms need further compatibility work; argument validation does not fix the existing approximate translation. Cardinality validation alone does not establish correct output labels or sample shapes for every valid grouped matrix join. Wide raw-sample fallback resource limits remain relevant.

## Reproduce

Use a fresh isolated Compose project for each parity shard. Do not enable the UI generator or repeatedly ingest shared fixtures into the same volumes. Wait for Loki and the proxy to report ready.

```sh
LOKI_URL=http://127.0.0.1:33101 PROXY_URL=http://127.0.0.1:33100 VL_URL=http://127.0.0.1:49428 \
go test -v -tags=e2e ./test/e2e-compat -run '^TestLogQL_Exhaustive_' -count=1
```

Unique-fixture `TestHardeningLive_` canaries are selected by the existing security regression CI script. Record failures separately from passes; HTTP 200 or a nonempty chart is insufficient proof of parity.
