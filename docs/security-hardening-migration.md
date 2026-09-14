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
