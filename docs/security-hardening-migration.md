---
sidebar_label: Security hardening migration
description: Compatibility and rollout notes for security review fixes.
---

# Security hardening migration

These changes preserve the documented Loki query response contracts. Security
policy corrections can still affect deployments that depended on previously
accepted unsafe behavior; assess the relevant configuration before rollout.

## Tenant wildcard policy and tail input

- `tenant.allow-global=false` now rejects an **unmapped** `X-Scope-OrgID: *`
  in label-routing mode as well as native tenant mode. An explicit tenant-map
  entry still takes precedence. Deliberately shared/global access must be
  enabled explicitly; requiring a tenant header does not authenticate its value.
- Default aliases (`0`, `fake`, `default`) retain their existing single-tenant
  behavior. Wildcards in a multi-tenant header remain invalid.
- Tail accepts at most 4 KiB per client application message, including fragmented
  messages, and closes larger messages with WebSocket code 1009. This limit is
  **not** a limit on log frames sent to Grafana. Ping, pong, close, and small
  legacy client messages remain supported. Loki's tail API describes a stream
  of server log results; Explore does not need large client data messages.
- The dedicated security lane now selects every hardening, tenant-scoping, and
  tail-hardening test. A selection inventory test fails if any is omitted.

Upstream contracts: [Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/)
and [Gorilla message read limit](https://pkg.go.dev/github.com/gorilla/websocket#Conn.SetReadLimit).

## Tenant scope across caches and backends

Hot/cold merge workers consume their responses before waiting for each other,
so a shared backend concurrency limit of one remains usable. Successful hot
and forward-cold responses are buffered up to 64 MiB each; overflow or read
failure returns an error. Backend permits remain held until each response is
consumed or closed.

Tenant routing is snapshotted at admission and retained for child queries and
background work. Cache and coalescer identities now include the resolved tenant
mapping, backend identity, configured field mappings, and forwarded identity.
Trusted identity headers use the same forwarding policy for dispatch, caching,
and background snapshots. Label-value indexes are also scoped by this identity.

This deliberately invalidates old response, window, pattern, and metadata cache
keys, including persisted and peer entries. Expect a temporary increase in
backend reads while caches warm. Old entries may occupy storage until expiry or
reclamation; they are not accepted into the new namespace. Mixed-version fleets
cannot share these entries, and old replicas retain the old security behavior.
The configuration namespace is computed at startup/reload instead of hashing
configuration on every read. Any tenant-map change invalidates the namespace
for all tenants; already admitted requests retain their original namespace.

Label tenancy now uses VictoriaLogs' `extra_stream_filters` parameter instead of
appending text after query pipelines. The configured tenant field must be a
**stream field**, preserving the original stream-selector contract. Request
parameters cannot override the internally generated constraint. See the
[upstream extra-filter contract](https://docs.victoriametrics.com/victorialogs/querying/#extra-filters).

Cold query dispatch now sends the resolved AccountID/ProjectID plus the external
X-Scope-OrgID. It carries the stream constraint in label mode. It does not copy
hot-backend service credentials to the cold host. A shared cold deployment that
previously relied on unscoped default data will now receive correctly scoped
requests and may show less data; verify its ingest tenant matches query routing.
The existing hot-backend `forward-tenant-header` precedence is preserved.

The fallback field batcher and active Drilldown burst coalescer retain request
identity when decoupling their cancellation lifetime, and cannot combine work
from different authorization scopes. This does not change which Drilldown path
is selected or remove progressive metadata/window caching.

## Exact response cache keys

Final query responses now vary by exact time bounds, step, limit, direction,
interval, and negotiated tuple/Grafana profile. Moving a time range inside a
five-minute bucket can now trigger a new final-response lookup instead of
returning another range's logs or evaluation grid. Metadata's progressive policy
and aligned-window reuse remain available. This corrects Loki semantics but can
increase backend work for drifting historical metric windows; size and observe
the rollout using actual backend-call counts. Tier0 entries use a new version.

The delayed cold-miss benchmark disables caches/coalescing and asserts both
successful content and one upstream call per timed operation. Its numbers are
not comparable to older runs that mostly measured primary-cache hits.

## Execution and storage limits

- `max-concurrent` now also bounds actual hot/cold/alerting backend operations,
  including fanout, until their response bodies are consumed or closed. Waiting
  children respect cancellation. The existing outer HTTP admission limit remains
  separate; zero still means unlimited. Native tail streams retain their
  dedicated streaming client, and readiness/peer control requests remain separate.
- `line_format` permits 64 KiB output per line and 16 MiB per response, with
  bounded intermediate formatting, input, template depth and execution work.
  Exceeding these limits returns HTTP 400. Normal printf, control flow, string
  operations and both quoted/backtick template literals remain supported.
- Raw metric evaluation (v1.68.0), including bare-parser `unwrap` queries,
  rejects scans beyond `-manual-range-metric-row-limit` (default 1,000,000 rows)
  and results beyond `-max-stats-query-series` (default 500 series), with 64 MiB
  input/output and one million output samples. Overflow returns an error
  (usually HTTP 502) instead of a truncated success. Native `stats_query_range`
  paths keep their existing top-series cap at the same series limit.
- Binary expressions (v1.68.0) allow nesting depth 64 and 1,024 child
  evaluations, sharing 256 MiB of captured child responses, two million decoded
  arrays, one million constructed samples and 64 MiB of label work; each encoded
  result is capped at 64 MiB.
- Coalesced bodies exceeding 256 MiB now return an error instead of being
  silently truncated. Scratch buffers above 1 MiB are not retained in the pool.
- Disk expiry reclamation runs incrementally even with no new writes. This
  restores admission during sliding-window churn. Disk limits still describe
  logical stored content, not a promise that the bbolt file physically shrinks.

These protective limits are intentional behavior changes for unusually large
workloads. Validate representative queries and watch rejection rates before
rollout; do not interpret a rejected query as absence of logs or healthy alerts.

Formatting queries now use the buffered evaluator even when response streaming
is enabled, so errors are reported before partial success is written. Raw log
queries retain streaming. Both legacy and categorized tuples are formatted;
categorized metadata stays attached and its fields are available to templates.
Backtick and escaped-quote literals are decoded correctly before translation.

Range-query `topk`/`bottomk` now select winners independently at each timestamp,
following [Prometheus range semantics](https://prometheus.io/blog/2021/02/18/introducing-the-%40-modifier/).
A range can therefore return more than `k` distinct series as winners change;
each series contains only its winning samples. Dashboards relying on the old
whole-range cap will display additional legitimate series.
Ranking excludes absent windows instead of admitting chart-fill zeros as
candidates. Regular Drilldown chart filling retains its existing time-axis
behavior. Byte-based ranking requests byte sums and presence counts together,
so a real empty log line remains eligible without sorting/scanning raw logs.
Count/rate ranking continues to use pre-aggregated buckets as well.

## Remaining delete API gap

The Loki delete handler (`/loki/api/v1/delete`) is always registered. It checks
the confirmation header, selector, time bounds and tenant scope, then forwards to
`/select/logsql/delete`, which VictoriaLogs v1.52.0 rejects as an unsupported
path. Its documented API is the asynchronous `/delete/run_task?filter=...` API.
This hardening series does not call that destructive API or claim delete
compatibility: treat delete as unsupported. A working adapter would need time and
tenant constraints, task status/cancellation, authorization, and cache
invalidation with live tests.
See [VictoriaLogs deletion](https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs).
