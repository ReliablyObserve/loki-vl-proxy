---
sidebar_label: Hardening validation
description: Security findings, compatibility evidence, and remaining production-readiness limits.
---

# Security hardening validation — September 2026

Baseline: `32dff5b1b7bf520eb748dfd6857bfc7cb4cb28b8` (v1.66.1).
This review followed the architecture, security, compatibility, fleet-cache and
peer-cache documentation, plus the historical tenant-label and secure-defaults
designs. The objective is to preserve Grafana visibility, tenant boundaries,
progressive metadata loading and historical cache reuse while making execution
bounded and failures visible. It is not a certification of every deployment.

## Findings and review units

| Findings | Fix and regression coverage | Review |
| --- | --- | --- |
| Label routing admitted unmapped wildcard tenants; tail allocated unbounded client messages; security lane skipped intended tests | Deny unsafe wildcard before label-mode shortcut; 4 KiB fragmented-message bound preserving control frames; enumerate security test selection | [Policy and tail](https://github.com/ReliablyObserve/loki-vl-proxy/pull/520) |
| Cold reads lost native tenant headers; reload reused old-scope cached data; implicit trusted identity was absent from cache keys; field batching lost request context | Immutable admission scope across hot/cold, caches, coalescers and background work; consistent identity forwarding/fingerprints; scope-aware label index | [Tenant isolation](https://github.com/ReliablyObserve/loki-vl-proxy/pull/521) |
| Appended label constraint broke LogsQL pipelines | Use backend-enforced JSON `extra_stream_filters`; verify enforcement with foreign data present | [Tenant isolation](https://github.com/ReliablyObserve/loki-vl-proxy/pull/521) |
| Rounded final-response keys mixed disjoint time windows and tuple profiles; cold benchmark mostly measured hits | Exact response contract keys; preserve metadata/aligned-window caches; verify one upstream call per benchmark operation | [Cache contracts](https://github.com/ReliablyObserve/loki-vl-proxy/pull/522) |
| Unbounded subquery work, backend fanout and template expansion; upstream failures became successful empty results | Total evaluation/byte/sample limits, fixed worker pool, body-lifetime backend permits, bounded template execution and error propagation | [Execution limits](https://github.com/ReliablyObserve/loki-vl-proxy/pull/523) |
| Expired unread disk entries blocked admission; oversized coalesced responses were truncated | Incremental expiry sweep even without writes; explicit response-size errors and bounded buffer retention | [Execution limits](https://github.com/ReliablyObserve/loki-vl-proxy/pull/523) |
| Vulnerable documentation image parser dependency | Integrity-checked CJS/ESM patch and timeout-isolated tests; reduced build permissions/timeouts | [Docs hardening](https://github.com/ReliablyObserve/loki-vl-proxy/pull/524) |
| Live parity exposed backtick formatting, skipped production tuple evaluation, and range-wide topk ranking | Decode template literals, format real tuple shapes, preserve categorized fields, enforce budgets before streaming, rank at each timestamp | [Integration #525](https://github.com/ReliablyObserve/loki-vl-proxy/pull/525); production-route unit tests and live API/browser checks |

## Measured compatibility

All live checks used isolated local Compose projects and dedicated ports/data.
Existing development containers and production deployments were not modified.

| Stack/check | Observed result |
| --- | --- |
| VictoriaLogs v1.30.0, v1.50.0, v1.52.0 | Native and label tenants isolated across hot/cold reads with caches enabled and disabled; repeated requests return own data and exclude foreign/default data. Nine tests per version including subtests. |
| Loki 3.6.0 and 3.7.1, VL v1.50.0 | Identical visible lines for disjoint one-second windows in the same former cache bucket, repeated requests, backtick formatting and escaped printf literals. Changing topk/bottomk winners match at each timestamp for rate and count_over_time. |
| Grafana 13.0.1 / Logs Drilldown 2.0.4 | Three strict browser tests pass across direct Loki, normal proxy and native-metadata proxy: 18 page states verify exact marker presence/absence and formatted text. Plugin version verified through Grafana API. |
| Grafana synthetic and ingress tail | Two tests require newly ingested markers in visible live log rows, beyond merely opening WebSockets. Both pass. |
| Broader Explore / Drilldown browser suite | 107 passed and 12 existing explicit skips across eight suites. After the final scope/cache performance change, all 18 focused Explore/Drilldown/visibility tests passed again. Skips remain gaps, not passes. |
| Go unit suite and race detector | Full final suite: 4,816 tests passed across 14 packages with `go test -race ./... -count=1` on revision `5c33387`, including empty-window ranking and scope/cache performance fixes. CI status is recorded in the integration PR. |
| Static/runtime dependency checks | `go vet ./...` passed; `govulncheck@latest ./...` reported no reachable Go vulnerabilities at review time. |
| Website | Fresh `npm ci` verifies all 20 patched bundles; two CJS/ESM subprocess regression groups pass; production build passed. Registry audit still flags 18 affected dependency-tree entries from two image-size advisories. |

The requested current-version review additionally ran on **Loki 3.7.7,
Grafana 13.2.1 and Logs Drilldown 2.5.2**, with runtime versions verified through
their APIs. All 107 selected browser tests passed with the same 12 existing
skips, both before and after the final empty-window ranking fix. Live tenant,
exact-window and formatting tests pass; topk/bottomk tests now also cover empty
leading/trailing windows and byte-based aggregations. The Compose review
override pins these versions for reproducible manual acceptance.

The final scope/cache implementation avoids repeated configuration serialization
on cache hits. Local Apple M5 measurements (`GOMAXPROCS=1`, three one-second runs)
were 1.73–2.01 µs / 872 B / 14 allocations for query-range hits and 497–517 ns /
48 B / 3 allocations for label hits. These are local microbenchmarks, not a
production latency guarantee; the integration PR also runs the CI comparison.

The live version sample covers the oldest VL support band, pinned runtime and
newer packaging, and both supported Loki minor families. It does not replace
the complete patch-version matrix in `test/e2e-compat/compatibility-matrix.json`.

## Reproduce the new gates

Start the existing compatibility stack, then supply its actual endpoints:

```sh
LOKI_URL=http://127.0.0.1:13101 PROXY_URL=http://127.0.0.1:13100 \
VL_URL=http://127.0.0.1:19428 \
go test -tags=e2e ./test/e2e-compat -run '^TestHardeningLive_' -count=1

cd test/e2e-ui
GRAFANA_URL=http://127.0.0.1:3002 VL_URL=http://127.0.0.1:19428 \
LOKI_URL=http://127.0.0.1:13101 \
npx playwright test tests/security-hardening-visibility.spec.ts tests/explore.spec.ts
```

The security CI script selects `TestHardeningLive_*`; the browser additions carry
the existing `@explore-core` and `@explore-tail` tags. Test ingestion URLs now
honor environment overrides so custom-port runs cannot seed another local stack.
The [manual acceptance guide](security-hardening-manual-acceptance.md) includes
per-PR changes, expected results and the rebuild procedure after every approved merge.

## Compatibility and remaining readiness work

These fixes are not wholly free of behavior changes. Unsafe wildcard requests,
oversized client tail messages and excessive query work are rejected. Exact
cache keys invalidate old entries and can increase misses. Formatting buffers
before responding. Range topk may expose more than k changing winners. Read the
[migration guide](security-hardening-migration.md) before rollout.

The opt-in delete adapter is still unsupported against the verified VL endpoint;
keep deletion disabled pending a separately designed asynchronous adapter. The
existing browser suite also contains explicit known-gap skips and permissive
legacy assertions. The new strict tests do not turn those gaps into guarantees.

A separate correction of the legacy exhaustive helper exposed a serious test
limitation: it sends millisecond integers, which Loki treats as nanoseconds,
and therefore often compares an empty epoch window with current proxy data.
An isolated run without the UI generator confirmed invalid-IP and parser-error
mismatches and invalid binary matching. Strict value checks also exposed
quantile errors and a warmed-field-mapping regexp capture defect. Earlier
reference timeouts and aggregate resource failures on a long-running generator
stack are not isolated reproductions. These compatibility issues are tracked
in the separate real-window draft; the legacy suite's green status is not
execution-parity evidence. The malformed-template hardening regression uses
seeded data and correct timestamps directly.

The pre-merge review additionally reproduced a hot/cold merge stall with one
backend permit: 502 at a 250 ms request deadline. Consuming bounded response
bodies inside their workers restored a successful two-backend response. The
new forward/backward and body-failure regressions and the full uncached race
suite pass with this correction.

Before broad shared production rollout, run the full supported-version matrix,
a representative concurrent/tenant workload soak, backend outage and disk-full
fault injection, and a canary watching rejected queries, queue wait, backend
calls, cache misses, memory and query latency. Remaining test priorities are
strict parsed-field filter interactions, multi-stage formatting order,
alerting behavior on partial backend failure, and tenant-scoped peer/persisted
cache restarts under configuration reload. Retain the local image-size patch
only until a verified upstream fix can replace it; see `website/SECURITY.md`.

Upstream contracts checked: [Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/),
[Loki template functions](https://grafana.com/docs/loki/latest/query/template_functions/),
[VL query constraints](https://docs.victoriametrics.com/victorialogs/querying/),
[VL security model](https://docs.victoriametrics.com/victorialogs/security-and-lb/).
