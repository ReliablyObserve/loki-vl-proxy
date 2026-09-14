---
sidebar_label: Real-window compatibility gaps
description: Blocking findings exposed by correcting the exhaustive Loki test timestamps.
---

# Real-window compatibility follow-up

Status: investigation and failing regression coverage; **not ready to merge or
claim full production compatibility**. This follows the security hardening
integration in PR #525. Its new seeded API tests and visible browser tests use
correct timestamps. The older exhaustive helper did not.

## Test defect and measured effect

The exhaustive helper sent `UnixMilli()` integers to Loki's `start` and `end`.
Loki interprets integer timestamps as nanoseconds, so those requests addressed
1970 while the proxy's permissive timestamp conversion addressed current data.
This made empty results and skipped runtime validation look like compatibility.
The change in this draft uses RFC3339 timestamps for the actual ingested range.
See the [Loki timestamp contract](https://grafana.com/docs/loki/latest/reference/loki-http-api/#timestamps).

Measured locally with Loki 3.7.7, VictoriaLogs 1.50.0 and proxy revision 5c33387:

| Finding | Evidence | Required solution and regression |
| --- | --- | --- |
| Invalid IP patterns silently succeed | Loki returns 400 for invalid IPv4, text, CIDR prefix and IPv6 patterns; proxy returns 200. Four error-parity cases fail. | Validate supported single-address, CIDR and range forms, including IPv6, at the appropriate execution boundary. Cover valid/invalid forms, empty/populated windows and line/label filters. Do not replace errors with an empty regex match. |
| Parser errors disappear inside metrics | `rate({env="production"} \| json \| __error__!="" [5m])` returns a Loki `JSONParserErr` pipeline error but proxy 200. The registry contains this case twice. | Preserve parser-error state through filters/drop stages and fail metric evaluation when surviving samples contain errors. A blanket syntax rejection would also reject valid empty/error-filtered results. Cover raw malformed input, `__error__=""`, nonempty filters, `drop __error__`, range/instant queries and alerting behavior. |
| Grouped quantile loses data | A populated `quantile_over_time(... unwrap duration_ms [5m]) by (level)` request returns three Loki series and no proxy series. | Preserve original grouping and parsed numeric fields through fallback dispatch. Add deterministic quantile fixtures with exact per-group values and timestamps, plus cache-hit repeats. |
| Regexp capture then filter loses data | `... \| regexp "(?P<http_method>[A-Z]+)" \| http_method="GET"` returns four Loki streams and no proxy streams. | Preserve the capture alias and pipeline order in translation; compare actual selected lines and categorized fields, then verify a Grafana filter click/reload. |
| Aggregate rate fallback can return a backend error | `sum(rate({env="production"}[5m]))` succeeds in Loki and returns proxy 502 from a rejected VL query in the corrected suite. | Reduce the generated query, fix aggregate-all fallback construction, and verify one scalar series with both parser-free and parsed inputs. Keep tenant constraints and backend error propagation intact. |
| Some reference queries time out | Several unwrapped range functions and `without` aggregations exceeded the local reference client's 20-second deadline; a whole-second RFC3339 probe also timed out. Loki stayed running without OOM/restarts. | Reproduce with a fresh minimal fixture and isolated reference backend before attributing these failures to proxy semantics. Distinguish upstream runtime/timeout failures from valid contract responses; do not make the proxy mimic reference infrastructure failures. |

The observed error-parity result was 64/70; six failures represent four IP cases
and the duplicated parser-error case. This is not an exhaustive count of product
defects. The broader corrected run also exposed the populated-result gaps above.
Previous all-green exhaustive results must not be used as proof of execution parity.

## Reproduce and acceptance

Start the review override from `docs/security-hardening-manual-acceptance.md`,
then use the stack's actual endpoints:

```sh
LOKI_URL=http://127.0.0.1:13101 PROXY_URL=http://127.0.0.1:13100 \
VL_URL=http://127.0.0.1:19428 \
go test -v -tags=e2e ./test/e2e-compat -run '^TestLogQL_Exhaustive_' -count=1
```

For the isolated review project, replace ports with 23101, 23100 and 29428.
The suite seeds rich data through `ensureDataIngested`; retain those fixtures
and record exact request windows and full error bodies when minimizing failures.

Before merging this follow-up, resolve or explicitly classify every failure
using real populated fixtures and upstream evidence. Add a timestamp canary
that requires a newly ingested marker from both backends, so a future unit
regression cannot pass by querying an empty epoch. Make assertions compare
actual lines/values and identities instead of only status and result shape.

For each fix, document compatibility changes, run its focused real-API check,
and then repeat Explore/Drilldown query-inspector and visible-row checks after
rebuilding the actual approved merge. Keep this follow-up separate from the
already measured security gates; do not hide failures or label them as passes.
