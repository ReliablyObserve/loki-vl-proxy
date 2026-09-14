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
