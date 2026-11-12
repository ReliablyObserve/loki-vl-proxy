---
sidebar_label: Observability Guide
description: Metrics, dashboards, alert rules, and structured logging for loki-vl-proxy. Covers Prometheus, OTLP, and Grafana dashboard.
---

# Observability Guide

Loki-VL-proxy emits the same core operational signals in two forms:

| Signal | Transport | Format | Best use |
|---|---|---|---|
| Metrics | Pull | Prometheus text at `/metrics` | Prometheus, Grafana Agent, Alloy, VictoriaMetrics, kube scraping |
| Metrics | Push | OTLP HTTP JSON to `/v1/metrics` | OpenTelemetry Collector, vendor OTLP gateways |
| Logs | Stream | Structured JSON on stdout/stderr | Fluent Bit, Vector, OpenTelemetry Collector, Docker/Kubernetes log agents |

The intent is parity, not two separate products. Prometheus scrape and OTLP push carry the same proxy-centric metric families, units, and low-cardinality request dimensions for the important operational paths. Prometheus uses label keys such as `system`, `direction`, `endpoint`, `route`, and `status`; OTLP exports the same dimensions as semantically aligned attributes such as `loki.api.system`, `proxy.direction`, `loki.request.type`, `http.route`, and `http.response.status_code`.

That shared model is what makes the packaged dashboard portable across scrape-backed and OTLP-backed setups without rewriting the operator view:

- request volume and latency
- backend latency
- cache hit/miss behavior
- translation failures
- tenant and client hot spots
- client status and query-length outliers
- process/runtime and host-level health

The proxy also now keeps more expensive metadata paths deliberately warmer than live log queries:

- live query and tail paths stay on short TTLs so fresh log visibility is not stretched unnecessarily
- slower-changing metadata such as labels, field lists, field values, and patterns are cached more aggressively
- Drilldown prefers backend-native metadata discovery where it is safe, which reduces proxy-side rescans and lowers CPU pressure on repeated field/label browsing

## Observability Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /ready` | Readiness probe (checks backend `/health` and circuit-breaker state) |
| `GET /metrics` | Prometheus text exposition. Off by default in the binary (`-server.register-instrumentation=false`); when enabled it is served on `-listen`, or on `-metrics-listen` when set. The Helm chart enables it on a dedicated `:9091` listener. Bounded by `-server.metrics-max-concurrency` |
| `GET /debug/queries` | Query analytics endpoint (disabled by default, `-server.enable-query-analytics`) |
| `GET /debug/pprof/` | Go pprof profiling endpoints (disabled by default, `-server.enable-pprof`) |

Admin and debug routes (`/admin/*`, `/debug/*`) are served on the loopback `-admin-listen` address (default `127.0.0.1:3101`) unless `-server.admin-auth-token` is set, in which case they move to the main listener and require the token.

## Logs

### JSON Log Shape

Default logs are emitted as JSON and already use OTel-friendly top-level keys:

```json
{
  "timestamp": "2026-04-05T18:03:27.214918Z",
  "severity": {
    "text": "INFO",
    "number": 9
  },
  "body": "request",
  "component": "proxy",
  "http.route": "/loki/api/v1/query_range",
  "url.path": "/loki/api/v1/query_range",
  "http.request.method": "GET",
  "http.response.status_code": 200,
  "loki.request.type": "query_range",
  "loki.api.system": "loki",
  "proxy.direction": "downstream",
  "event.duration": 42000000,
  "loki.tenant.id": "team-a",
  "loki.query": "{service_name=\"api\"} |= \"error\"",
  "client.address": "10.0.0.12:51884",
  "enduser.id": "grafana-user@example.com",
  "enduser.source": "grafana_user",
  "cache.result": "miss",
  "proxy.duration_ms": 42,
  "upstream.calls": 1,
  "upstream.calls_by_type": {
    "vl:select_logsql_query": 1
  },
  "upstream.status_code": 200,
  "upstream.duration_ms": 31,
  "upstream.duration_ms_by_type": {
    "vl:select_logsql_query": 31
  },
  "proxy.operations_by_type": {
    "translate_query:translated": 1
  },
  "proxy.operation_duration_ms_by_type": {
    "translate_query:translated": 4
  },
  "proxy.overhead_ms": 11,
  "flow.cache.lookups": 3,
  "flow.cache.misses": 3,
  "flow.vl.calls": 1,
  "flow.vl.ms": 33,
  "flow.proxy.ms": 9
}
```

The example is shortened; the Log line reference below lists every field.

That makes the log stream usable in two ways:

- plain JSON ingestion with no transformation
- low-friction mapping into the OpenTelemetry log data model

### Log line reference

Every line is one JSON object. The proxy writes these lines about client
traffic (the rest of the log, such as `upstream_request` for each call to
VictoriaLogs or the cache and tail internals, is described under Log Sources):

| Line (`msg`) | Level | When |
|---|---|---|
| `request` | `INFO`; `WARN` for 4xx, `ERROR` for 5xx | once per client request, subject to Adaptive Log Sampling |
| `request_digest` | `INFO` | a periodic summary (`requests.total`, `requests.ok`, `requests.errors`, `requests.rate_per_sec`, `latency.avg_ms`, `latency.max_ms`, `cache.hit_pct`, an error breakdown) replacing the OK `request` lines above `-log-rate-threshold` |
| `slow_request` | `WARN` | a client request at or over `-slow-query-threshold`, whatever the sampling |
| `slow_upstream_request` | `WARN` | one VictoriaLogs call at or over `-slow-upstream-threshold`, whatever the sampling |

#### The `request` line

```json
{
  "timestamp": "2026-11-07T10:15:02.411903Z",
  "severity": {"text": "INFO", "number": 9},
  "body": "request",
  "http.route": "/loki/api/v1/query_range",
  "url.path": "/loki/api/v1/query_range",
  "http.request.method": "GET",
  "http.response.status_code": 200,
  "loki.request.type": "query_range",
  "loki.api.system": "loki",
  "proxy.direction": "downstream",
  "event.duration": 412000000,
  "loki.tenant.id": "team-a",
  "loki.query": "sum by (level) (count_over_time({service_name=\"api\"}[5m]))",
  "enduser.id": "grafana-user@example.com",
  "enduser.source": "grafana_user",
  "cache.result": "miss",
  "proxy.duration_ms": 412,
  "proxy.overhead_ms": 61,
  "upstream.calls": 2,
  "upstream.duration_ms": 351,
  "upstream.status_code": 200,
  "upstream.error": false,
  "flow.parse.count": 1,
  "flow.parse.ms": 0,
  "flow.translate.count": 2,
  "flow.translate.ms": 1,
  "flow.cache.lookups": 3,
  "flow.cache.hits": 0,
  "flow.cache.misses": 3,
  "flow.cache.stale": 0,
  "flow.cache.ms": 0,
  "flow.cache.bytes": 0,
  "flow.cache.by_tier": {
    "unknown": {"hits": 0, "misses": 3, "stale": 0}
  },
  "flow.vl.calls": 2,
  "flow.vl.ms": 366,
  "flow.vl.bytes": 18234,
  "flow.proxy.ms": 46,
  "upstream.call_types": 1,
  "upstream.duration_types": 1,
  "proxy.operation_types": 3,
  "proxy.operation_duration_types": 3,
  "client.address": "10.0.0.12",
  "network.peer.address": "10.0.0.7",
  "user_agent.original": "Grafana/12.2.0",
  "grafana.version": "12.2.0",
  "grafana.client.surface": "grafana_loki_datasource",
  "enduser.name": "grafana-user@example.com"
}
```

| Field | Meaning |
|---|---|
| `http.route`, `url.path`, `http.request.method`, `http.response.status_code` | the Loki route (the normalized label used by the metrics), the requested path, the method and the status sent |
| `loki.request.type` | the Loki endpoint name (`query_range`, `labels`, `detected_fields`, ...) |
| `loki.api.system`, `proxy.direction` | always `loki` and `downstream` on these lines |
| `event.duration` | request duration in nanoseconds; `proxy.duration_ms` is the same in milliseconds |
| `loki.tenant.id` | the `X-Scope-OrgID` of the request |
| `loki.query` | the LogQL, cut at 200 bytes |
| `enduser.id`, `enduser.source`, `enduser.name`, `auth.source` | who sent the request and how the proxy knows: the user named by `-log-user-header` or the trusted headers of `-metrics.trust-proxy-headers`, otherwise the tenant (`enduser.source` `tenant`) or the client address; capped at 128 bytes, control characters escaped. `auth.source` is only the mechanism (for example `basic_auth`), never the credential. See [Who is loading the proxy or VictoriaLogs](#who-is-loading-the-proxy-or-victorialogs) |
| `client.address`, `network.peer.address`, `user_agent.original` | the forwarded client address, the connection peer and the user agent |
| `grafana.*` | the Grafana version, surface (Explore, Drilldown), source tag and profiles read from the user agent and `X-Query-Tags`, and `grafana.query_tags`, the `X-Query-Tags` header as sent (capped at 256 bytes) when it holds more than the Source tag |
| `cache.result` | the answer-cache outcome of the request: `hit`, `miss` or `bypass` (cache not used) |
| `proxy.overhead_ms` | the request's duration minus `upstream.duration_ms` (never below 0) |
| `upstream.calls`, `upstream.duration_ms`, `upstream.status_code`, `upstream.error` | the requests sent to VictoriaLogs and Loki-compatible backends for this request: how many, their summed time to the response headers, the last status and whether any failed |
| `upstream.call_types`, `upstream.duration_types`, `proxy.operation_types`, `proxy.operation_duration_types` | how many distinct VictoriaLogs request types and proxy-side operations the request used; at `DEBUG` level the maps themselves follow (`upstream.calls_by_type`, `upstream.duration_ms_by_type`, `proxy.operations_by_type`, `proxy.operation_duration_ms_by_type`, keyed `system:type` and `operation:outcome`) |
| `flow.*` | what the request caused, see Query Flow below |

#### Query Flow (`flow.*`)

The `flow` fields answer "how much work did this request cause, and where did
the time go". They are on every `request` and `slow_request` line, and feed
the `loki_vl_proxy_request_phase_seconds` and `loki_vl_proxy_request_fanout`
metrics, which see every request whatever the log sampling.

| Field | Meaning |
|---|---|
| `flow.parse.count`, `flow.parse.ms` | LogQL parses made with the request's context, and their summed time. A lower bound: parses made by helpers that have no request context (the shape probes deeper in a handler) are not counted, and a hit in the parsed-query cache is not a parse |
| `flow.translate.count`, `flow.translate.ms` | LogQL to LogsQL translations (every `translate_query` outcome: `translated`, `cache_hit`, `passthrough`) and their summed time |
| `flow.cache.lookups` | physical cache reads: the answer, window, extent, label and inventory caches and the compatibility cache, each read counted once under the tier that answered. In-process translation-memo lookups are part of `flow.translate.*`, not counted here |
| `flow.cache.hits`, `flow.cache.misses`, `flow.cache.stale` | the lookups by result. `stale` is a read that asks for expired entries (the last known-good answer, used when VictoriaLogs fails) and found one; it counts as stale whatever the entry's remaining TTL, because it is only used after the fresh read missed |
| `flow.cache.ms`, `flow.cache.bytes` | summed lookup time, and the bytes the cache returned on hits and stale reads |
| `flow.cache.by_tier` | per tier (`l1_memory`, `l2_disk`, `l3_peer`, `unknown`) the `hits`, `misses` and `stale` counts. A read through every tier that misses is `unknown` (it consulted all of them); a memory-only read is `l1_memory` |
| `flow.vl.calls`, `flow.vl.ms`, `flow.vl.bytes` | VictoriaLogs calls, their summed time from sending to the end of the response body (the slow-query definition, so body reading is included), and the decoded response bytes. `upstream.calls` and `upstream.duration_ms` keep their earlier meaning (time to the response headers) |
| `flow.vl.rows` | on `slow_request` only: log rows read from VictoriaLogs raw query bodies (0 for stats and metadata calls, which return aggregates) |
| `flow.proxy.ms` | the request's own time: its duration minus `flow.vl.ms` and `flow.cache.ms`, never below 0. It includes the parse and translate time, which are not subtracted |

Times are sums of the calls' own times. When a request runs calls in parallel
(a split query, a metadata fan-out) `flow.vl.ms` and `flow.cache.ms` add up to
more than the request lasted, and `flow.proxy.ms` is then an estimate (0 when
the calls outlast the request).

How to read it:

- A refresh answered from the cache shows `flow.cache.hits` above 0 and
  `flow.vl.calls` 0, with `flow.cache.bytes` the size of the answer served.
- A cold request shows `flow.cache.misses` and then `flow.vl.calls`; if
  `flow.vl.ms` is most of `proxy.duration_ms` the time is VictoriaLogs', if
  `flow.proxy.ms` is, the proxy's (merging, post-processing).
- Many `flow.cache.lookups` for one request with `l2_disk` or `l3_peer` hits
  mean the answer was assembled from stored pieces (extents, windows) and
  that the disk or the fleet answered it.
- `flow.parse.count` is small by construction (see its row): use it to see
  whether a handler parses a query again, not as a total of the parser's work.

#### The `slow_request` line

```json
{
  "timestamp": "2026-11-07T10:20:41.007120Z",
  "severity": {"text": "WARN", "number": 13},
  "body": "slow_request",
  "http.route": "/loki/api/v1/query_range",
  "loki.request.type": "query_range",
  "loki.api.system": "loki",
  "proxy.direction": "downstream",
  "http.response.status_code": 200,
  "event.duration": 4210000000,
  "slow.threshold_ms": 3000,
  "loki.tenant.id": "team-a",
  "loki.query": "sum by (level) (count_over_time({service_name=\"api\"} | json [5m]))",
  "cache.result": "miss",
  "upstream.calls": 2,
  "upstream.duration_ms": 4102,
  "flow.parse.count": 1,
  "flow.parse.ms": 0,
  "flow.translate.count": 1,
  "flow.translate.ms": 1,
  "flow.cache.lookups": 3,
  "flow.cache.hits": 1,
  "flow.cache.misses": 2,
  "flow.cache.stale": 0,
  "flow.cache.ms": 1,
  "flow.cache.bytes": 2048,
  "flow.cache.by_tier": {
    "l1_memory": {"hits": 1, "misses": 0, "stale": 0},
    "unknown": {"hits": 0, "misses": 2, "stale": 0}
  },
  "flow.vl.calls": 2,
  "flow.vl.ms": 4180,
  "flow.vl.bytes": 912340,
  "flow.vl.rows": 0,
  "flow.proxy.ms": 29,
  "vl.account_id": "10",
  "vl.project_id": "0",
  "enduser.id": "alice@example.com",
  "enduser.source": "grafana_user",
  "enduser.name": "alice@example.com",
  "grafana.client.source_tag": "grafana-lokiexplore-app",
  "grafana.client.surface": "grafana_drilldown",
  "proxy.steps": [
    {"kind": "cache", "at_ms": 0, "tier": "unknown", "result": "miss", "bytes": 0, "duration_ms": 0},
    {"kind": "rewrite", "at_ms": 0, "from": "sum by (level) (count_over_time({service_name=\"api\"} | json [5m]))",
     "to": "sum by (level) (count_over_time({service_name=\"api\"} | json | drop __error__ [5m]))"},
    {"kind": "operation", "at_ms": 1, "operation": "translate_query", "outcome": "translated", "duration_ms": 1},
    {"kind": "upstream", "at_ms": 2, "route": "/select/logsql/stats_query_range",
     "query": "{service_name=\"api\"} | unpack_json | stats by (_time:5m, level) count() c",
     "window": "start=1794000000 end=1794003600 step=300", "status": 200, "bytes": 640112, "outcome": "eof", "duration_ms": 2301},
    {"kind": "upstream", "at_ms": 2305, "route": "/select/logsql/stats_query_range",
     "query": {"same_as_step": 3}, "window": "start=1793996400 end=1794000000 step=300",
     "status": 200, "bytes": 272228, "outcome": "eof", "duration_ms": 1879}
  ]
}
```

| Field | Meaning |
|---|---|
| `http.route`, `loki.request.type`, `http.response.status_code`, `event.duration`, `loki.tenant.id`, `cache.result`, `upstream.calls`, `upstream.duration_ms` | as on the `request` line |
| `slow.threshold_ms` | the `-slow-query-threshold` that was crossed |
| `loki.query` | the LogQL as received: its text up to 8 KB, or its `sha256:` hash and length with `-slow-query-log-raw-queries=false` |
| `flow.*` | as on the `request` line: what the request cost, with `flow.vl.calls`, `flow.vl.ms`, `flow.vl.bytes` and `flow.vl.rows` its VictoriaLogs cost |
| `enduser.*`, `grafana.*` | who ran it and from which Grafana app, as on the `request` line |
| `vl.account_id`, `vl.project_id` | the VictoriaLogs tenant the tenant's queries go to (its `-tenant-map` entry, the numeric org itself, or `0`/`0` for the default tenant); `vl.tenant_label` instead names the field of `-tenant-label` routing. Absent for a multi-tenant request (`a\|b`, whose `slow_upstream_request` lines carry each tenant's) and a refused one |
| `proxy.steps` | the request's steps, listed as they started, each with `at_ms` (the offset from the request's start); at most 64 |
| `proxy.steps_dropped` | steps beyond the limit that were counted, not kept (only present when above 0); at most 16 of the 64 are cache steps, so a request that reads the cache many times keeps room for its VictoriaLogs calls |

Each step has a `kind`:

| `kind` | Fields |
|---|---|
| `upstream` | one VictoriaLogs call: `route` (the VictoriaLogs path), `query` (the LogsQL), `window` (`start`, `end`, `step` as sent), `status`, `bytes` (decoded body bytes), `outcome` (`eof`, `closed`, `canceled`, `timeout`, `error`), `duration_ms` (to the end of the body) |
| `operation` | a proxy-side operation or route decision: `operation`, `outcome`, `duration_ms` (for example `patterns_state`, `json_metric_evaluator` with `vl_stats_buckets/pushdown` or `raw_rows/probe`, `translate_query`) |
| `rewrite` | a handler rewrote the request's LogQL: `from`, `to` |
| `cache` | one cache lookup: `tier` (`l1_memory`, `l2_disk`, `l3_peer` or `unknown`), `result` (`hit`, `miss`, `stale`), `bytes` returned, `duration_ms` |

A query appears once on a line: a step repeating an earlier query holds
`{"same_as_step": N}` naming the step that has it, and past a line budget of
32 KB a query is its `sha256:` hash. `proxy.steps_dropped` is the number of
steps (of any kind) not kept.

#### The `slow_upstream_request` line

```json
{
  "timestamp": "2026-11-07T10:20:40.998774Z",
  "severity": {"text": "WARN", "number": 13},
  "body": "slow_upstream_request",
  "http.route": "/select/logsql/stats_query_range",
  "loki.api.system": "vl",
  "proxy.direction": "upstream",
  "http.response.status_code": 200,
  "event.duration": 2301000000,
  "slow.threshold_ms": 2000,
  "loki.tenant.id": "team-a",
  "vl.query": "{service_name=\"api\"} | unpack_json | stats by (_time:5m, level) count() c",
  "vl.window": "start=1794000000 end=1794003600 step=300",
  "vl.body_bytes": 640112,
  "vl.body_outcome": "eof",
  "vl.account_id": "10",
  "vl.project_id": "0",
  "enduser.id": "alice@example.com",
  "enduser.source": "grafana_user",
  "enduser.name": "alice@example.com",
  "grafana.client.source_tag": "grafana-lokiexplore-app",
  "grafana.client.surface": "grafana_drilldown",
  "http.parent_route": "/loki/api/v1/query_range"
}
```

| Field | Meaning |
|---|---|
| `http.route` | the VictoriaLogs path |
| `http.response.status_code`, `event.duration` | the VictoriaLogs status and the call's duration in nanoseconds, to the end of its body |
| `slow.threshold_ms` | the `-slow-upstream-threshold` that was crossed |
| `vl.query`, `vl.window` | the LogsQL (text up to 8 KB, or its hash) and the `start`, `end`, `step` it was sent with |
| `vl.body_bytes`, `vl.body_outcome` | decoded body bytes and how the body ended (`eof`, `closed`, `canceled`, `timeout`, `error`) |
| `http.parent_route` | the client route the call served |
| `loki.tenant.id`, `vl.account_id`, `vl.project_id`, `vl.tenant_label` | the tenant the call ran for and its VictoriaLogs tenant, as on `slow_request` |
| `enduser.*`, `grafana.*` | the caller of the client request the call served. A call shared by coalesced identical requests, or started by one and reused by others, runs in the context of the request that started it and names that caller |
| `proxy.background` | `true` when the call outlived its request (a background build); such a call is in no request's trace |

### Log Sources

The proxy writes structured logs for:

- request lifecycle and status
- query translation and backend request flow
- tail/WebSocket behavior
- delete audit events
- cache warmer and disk cache internals
- OTLP export failures

### Adaptive Log Sampling

At low traffic (below `-log-rate-threshold`, default 10 req/s), every request is
logged individually for easy debugging. When traffic exceeds the threshold, the
proxy switches to a summary mode:

- **OK traffic (2xx/3xx)**: individual request logs are suppressed. A periodic
  summary is printed every `-log-stats-interval` (default 10s) with total
  requests, error count, average/max latency, and cache hit rate.
- **Errors (4xx/5xx)**: always logged. At high error rates, repeated status codes
  are collapsed into digest lines (e.g., "429 x312 in last 10s") instead of one
  line per request.

This keeps logs useful for troubleshooting without flooding pipelines during
normal traffic.

| Flag | Default | Effect |
|---|---|---|
| `-log-buffered` | `true` | Write logs in the background for lower request latency |
| `-log-rate-threshold` | `10` | Req/s above which OK logs become periodic summaries |
| `-log-stats-interval` | `10s` | How often to print the request statistics summary |

### Slow Queries

A client request taking at least `-slow-query-threshold` (default `3s`) and a
single VictoriaLogs call taking at least `-slow-upstream-threshold` (default
`2s`, to the end of its response body) are logged at `WARN` whatever the
sampling above, and counted in `loki_vl_proxy_slow_requests_total`:

- `slow_request`: the route, status, duration, tenant, the LogQL, cache
  result, the `flow.*` fields (what the request caused) and `proxy.steps`, the request's steps in order, each with its
  offset from the request's start (`at_ms`): every VictoriaLogs call
  (`upstream`: route, LogsQL, window, status, bytes, outcome, duration), every
  proxy-side operation or route decision (`operation`: e.g. `patterns_state`,
  `json_metric_evaluator` with `vl_stats_buckets/pushdown` or
  `raw_rows/probe`, with its outcome and duration), and every rewrite of the
  LogQL (`rewrite`: from, to), and every cache lookup (`cache`: tier, result,
  bytes, duration; at most 16, so they leave room for the calls). At most 64
  steps; `proxy.steps_dropped` counts the rest. Live tail is never a slow
  request. The Log line reference above lists every field with an example.
- `slow_upstream_request`: the VictoriaLogs route, LogsQL, window, body bytes
  and outcome, duration and the client route it served (`http.parent_route`);
  `proxy.background=true` marks a call that outlived its request (a background
  build).

The defaults come from measured timings: 3s is the 99th percentile of the
weekly A/B benchmark's cold first loads, 2s rarer than 1 in 50 VictoriaLogs
calls on the compose stack (98th percentile over 24h of heavy Drilldown use,
timed to headers; above the 99.9th timed to the end of their bodies).

| Flag | Default | Effect |
|---|---|---|
| `-slow-query-threshold` | `3s` | Client request duration at or over which it is slow; `0` turns it off |
| `-slow-upstream-threshold` | `2s` | VictoriaLogs call duration (to the end of its body) at or over which it is slow; `0` turns it off |
| `-slow-query-log-raw-queries` | `true` | Slow lines carry the queries' text (capped at 8 KB); `false` carries their sha256 and length |
| `-slow-query-log-level` | `warn` | Level of the slow lines: `warn`, `info`, `debug` (written only when `-log-level` lets it through), or `off` (no lines and no trace; the metric still counts) |

### Who is loading the proxy or VictoriaLogs

Every line that reports a query (`request`, `upstream_request`, `slow_request`,
`slow_upstream_request`) names its caller: the user (`enduser.id`,
`enduser.source`, `enduser.name`), the tenant (`loki.tenant.id`, and on the slow
lines the VictoriaLogs `vl.account_id`/`vl.project_id` or `vl.tenant_label`)
and the Grafana app (`grafana.client.source_tag`, `grafana.client.surface`, and
`grafana.query_tags`, all of `X-Query-Tags`, when it holds more than the Source
tag). The slow lines also carry what the request cost: `flow.vl.calls`,
`flow.vl.ms`, `flow.vl.bytes` and `flow.vl.rows` on `slow_request`;
`event.duration` and `vl.body_bytes` on `slow_upstream_request`. One line
answers "who did this and what did it cost".

The caller is resolved once per request, by the first line that names it (or
by the callers view), and shared by the request's later lines, such as one
`upstream_request` per VictoriaLogs call.

#### Grafana user identity

Every option that decides who a request is attributed to, with its default:

| Option | Default | Effect |
|---|---|---|
| Grafana `[dataproxy] send_user_header` (`GF_DATAPROXY_SEND_USER_HEADER`) | `false` | Grafana adds `X-Grafana-User: <login>` to the datasource requests it proxies. Only signed-in users get the header: requests of anonymous users carry none, and are logged under their tenant |
| `-log-user-header` | (empty: off) | The header named here (for example `X-Grafana-User`) is the user on the log lines and in `/debug/queries`. Never a metric label, never sent to VictoriaLogs, never part of a cache key |
| `-log-user-header-trusted-cidrs` | (empty: every peer) | The TCP peers (CIDRs or addresses) allowed to send that header; other peers' requests are logged under their tenant |
| `-metrics.trust-proxy-headers` | `false` | Trusts `X-Grafana-User`, `X-Forwarded-User`, `X-Webauth-User` and `X-Auth-Request-User` (first one set wins) as the user, and `X-Forwarded-For` as the client address. It also labels the per-client metrics with that user (below), forwards those headers and `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Real-Ip`, `Forwarded` to VictoriaLogs, and makes them part of every cache key |
| `-metrics.export-sensitive-labels` | `false` | Exports the per-tenant and per-client metrics at all |
| `-metrics.max-tenants`, `-metrics.max-clients` | `256`, `256` | Distinct tenant and client label values kept before the rest become `__overflow__` |
| `-server.enable-query-analytics` | `false` | Serves `/debug/queries`, with the callers view |

Which identity a line names, in order: the `-log-user-header` value from a
trusted peer; else, with `-metrics.trust-proxy-headers`, the first trusted user
header (`enduser.source` `grafana_user`, `forwarded_user`, `webauth_user`,
`auth_request_user`); else the tenant (`X-Scope-OrgID`, `enduser.source`
`tenant`); else the client address (`remote_addr`, or `forwarded_for` with
`-metrics.trust-proxy-headers`).

Where the user appears:

- the `request`, `upstream_request`, `slow_request` and `slow_upstream_request`
  lines (`enduser.id`, `enduser.source`, `enduser.name`);
- `/debug/queries` (`callers`: top callers and recent requests).

Where it never appears: metric labels under `-log-user-header`, and no
`loki_vl_proxy_tenant_*` metric at all. The one exception is the older
`-metrics.trust-proxy-headers`: with `-metrics.export-sensitive-labels` also on,
the `client` label of the `loki_vl_proxy_client_*` metrics is the trusted user
(at most `-metrics.max-clients` values, then `__overflow__`). Leave that pair
off when users must stay out of metrics.

Spoofing: every one of these headers is written by the client. Trust them only
when the proxy is reachable from Grafana or an authenticating gateway alone
(a network policy, or `-log-user-header-trusted-cidrs` set to their addresses):
any other client can put any name in them. `-metrics.trust-proxy-headers` has
no address check of its own.

#### Who ran this query

Without a user header the caller is the tenant (`enduser.source` `tenant`).
To name the Grafana user:

1. Grafana sends the logged-in user in `X-Grafana-User` when its data proxy is
   told to:

   ```ini
   [dataproxy]
   send_user_header = true
   ```

   (`GF_DATAPROXY_SEND_USER_HEADER=true` in a container). The Loki datasource
   must use `access: proxy` (the default).

2. The proxy logs that header as the user:

   ```bash
   -log-user-header=X-Grafana-User
   -log-user-header-trusted-cidrs=10.0.0.0/8   # where Grafana, or the load balancer in front of the proxy, connects from
   ```

   With Helm:

   ```yaml
   observability:
     queryUser:
       header: X-Grafana-User
       trustedCIDRs: "10.0.0.0/8"
   ```

`-log-user-header` names the user in the logs and `/debug/queries` only. It
never labels a metric (users are unbounded), is never sent to VictoriaLogs and
is never part of a cache key, so two users opening the same dashboard share its
cached answers. `-metrics.trust-proxy-headers=true` also names the user, but it
additionally labels the per-client metrics with it (with
`-metrics.export-sensitive-labels`), forwards the user headers to VictoriaLogs,
and makes the user part of every cache key, so each user's requests are cached
and sent to VictoriaLogs separately. Prefer `-log-user-header` unless
VictoriaLogs needs the user.

The header is sent by the client, so anyone who can reach the proxy can write
any name in it. `-log-user-header-trusted-cidrs` accepts it only from the
listed peers. The peer is the TCP connection's address (Grafana itself, or the
last load balancer in front of the proxy), never `X-Forwarded-For`, which the
client writes. A request from another peer, or without the header, is logged
under its tenant. Either way the value is capped at 128 bytes (`...` marks the
cut) and its control characters (C0, DEL, C1, U+2028, U+2029) and invalid UTF-8
are escaped as `\uNNNN` / `\xNN`, so a name cannot forge a log line or a field.

A slow Drilldown request by a Grafana user, and the VictoriaLogs call it made:

```json
{"severity": {"text": "WARN"}, "body": "slow_request", "http.route": "/loki/api/v1/query_range",
 "http.response.status_code": 200, "event.duration": 4210000000, "slow.threshold_ms": 3000,
 "loki.tenant.id": "team-a", "vl.account_id": "10", "vl.project_id": "0",
 "enduser.id": "alice@example.com", "enduser.source": "grafana_user", "enduser.name": "alice@example.com",
 "grafana.client.source_tag": "grafana-lokiexplore-app", "grafana.client.surface": "grafana_drilldown",
 "loki.query": "sum by (level) (count_over_time({service_name=\"api\"} | json [5m]))",
 "flow.vl.calls": 2, "flow.vl.ms": 4180, "flow.vl.bytes": 912340, "flow.vl.rows": 0, "proxy.steps": ["..."]}
{"severity": {"text": "WARN"}, "body": "slow_upstream_request", "http.route": "/select/logsql/stats_query_range",
 "event.duration": 2301000000, "vl.body_bytes": 640112, "loki.tenant.id": "team-a", "vl.account_id": "10",
 "vl.project_id": "0", "enduser.id": "alice@example.com", "enduser.source": "grafana_user",
 "grafana.client.source_tag": "grafana-lokiexplore-app", "http.parent_route": "/loki/api/v1/query_range",
 "vl.query": "{service_name=\"api\"} | unpack_json | stats by (_time:5m, level) count() c"}
```

The same request without a user header names the tenant:
`"enduser.id": "team-a", "enduser.source": "tenant"`.

#### The heaviest callers: `/debug/queries`

With `-server.enable-query-analytics`, `/debug/queries` (an admin route: on
the loopback `-admin-listen` listener, or behind `-server.admin-auth-token`)
adds a `callers` object next to the top queries:

```json
"callers": {
  "window_seconds": 1800,
  "since": 1794000000000,
  "callers": 2,
  "overflowed": false,
  "top_by_vl_ms": [
    {"tenant": "team-a", "user": "alice@example.com", "user_source": "grafana_user",
     "last_grafana_source": "grafana-lokiexplore-app", "last_seen": 1794000912000,
     "requests": 41, "errors": 0, "slow": 3, "duration_ms": 52310,
     "vl_calls": 96, "vl_ms": 48712, "vl_bytes": 182334112, "vl_rows": 0}
  ],
  "top_by_vl_bytes": ["..."],
  "recent_requests": [
    {"time": 1794000912000, "tenant": "team-a", "user": "alice@example.com", "user_source": "grafana_user",
     "grafana_source": "grafana-lokiexplore-app", "endpoint": "query_range", "route": "/loki/api/v1/query_range",
     "status": 200, "query": "sum by (level) (count_over_time({service_name=\"api\"} | json [5m]))",
     "requests": 1, "errors": 0, "slow": 1, "duration_ms": 4210, "vl_calls": 2, "vl_ms": 4180,
     "vl_bytes": 912340, "vl_rows": 0}
  ]
}
```

- `top_by_vl_ms` and `top_by_vl_bytes`: the 20 callers (tenant and user) that
  cost VictoriaLogs the most time and bytes over the window, with their
  requests, errors (status 400 and above), slow requests, time and VictoriaLogs
  calls, time, bytes and rows.
- `recent_requests`: the 256 most recent client requests, newest first, each
  with its caller, route, status, query (first 512 bytes) and cost.
- `?tenant=team-a` and `?user=alice@example.com` keep the callers and requests
  of that tenant or user (exact match; both may be given). The top-query lists
  above them are not filtered.

The window is two halves of 15 minutes: the lists cover between 15 and 30
minutes of traffic (`since` is the oldest). Memory is bounded: at most 1000
callers per half (the rest are summed under one `__overflow__` caller, and
`overflowed` turns true) and 256 recent requests. The view is per replica and
in memory; query each replica, or use the per-tenant metrics below for a
fleet-wide view. Live tail is left out (it lasts as long as the client keeps
it open).

#### Per-tenant cost metrics

With `-metrics.export-sensitive-labels`, like the other tenant metrics,
`loki_vl_proxy_tenant_vl_calls_total`, `loki_vl_proxy_tenant_vl_duration_seconds_total`,
`loki_vl_proxy_tenant_vl_response_bytes_total` and `loki_vl_proxy_tenant_vl_rows_total`
count what each tenant's client requests cost VictoriaLogs (`tenant` label,
bounded by `-metrics.max-tenants`). The heaviest tenant over the last hour:

```promql
topk(5, sum by (tenant) (rate(loki_vl_proxy_tenant_vl_duration_seconds_total[1h])))
```

No metric is labelled by user: users stay in the logs and `/debug/queries`.

| Flag | Default | Effect |
|---|---|---|
| `-log-user-header` | (empty) | Header naming the user who ran a query, for the logs and `/debug/queries` only (`X-Grafana-User` with Grafana's `send_user_header`) |
| `-log-user-header-trusted-cidrs` | (empty) | Peers (CIDRs or addresses) allowed to send it; empty trusts every peer |
| `-server.enable-query-analytics` | `false` | Serve `/debug/queries`, with the callers view |

### OpenTelemetry Fields Used in Logs

| Field | Meaning |
|---|---|
| `timestamp` | event time |
| `severity.text` / `severity.number` | log severity |
| `body` | message body |
| `component` | internal subsystem (`proxy`, `disk_cache`, `cache_warmer`, `otlp_metrics`) |
| `http.*` / `url.path` | request semantics and normalized route vs actual request path |
| `http.parent_route` | parent downstream route template on upstream child-call logs |
| `event.duration` | request or upstream call duration in nanoseconds |
| `client.address` | remote address |
| `enduser.id` | stable trusted user/client identity when available |
| `enduser.name` | display/login user name from trusted user headers when available |
| `enduser.source` | trusted header source for end-user attribution (`grafana_user`, `forwarded_user`, etc.) |
| `auth.source` | datasource auth mechanism when the request carries Basic-Auth credentials (the principal itself is never logged; separate from `enduser.id`) |
| `cache.result` | compatibility cache result (`hit`, `miss`, `bypass`) |
| `proxy.*` | proxy-facing convenience fields such as total request duration and measured proxy overhead |
| `upstream.*` | backend call count, status, and latency |
| `loki.*` | Loki/proxy-specific attributes |

Additional request-scope aggregate fields used for fanout visibility:

| Field | Meaning |
|---|---|
| `loki.parent_request.type` | parent downstream request type on upstream child-call logs |
| `upstream.calls_by_type` | per-parent aggregate map keyed by `<system>:<request_type>` |
| `upstream.duration_ms_by_type` | per-parent aggregate latency map keyed by `<system>:<request_type>` |
| `proxy.operations_by_type` | per-parent aggregate map keyed by `<operation>:<outcome>` for proxy-only work |
| `proxy.operation_duration_ms_by_type` | per-parent aggregate latency map keyed by `<operation>:<outcome>` |

These aggregate map keys are intentionally bounded by route templates and hardcoded operation/outcome enums. They are log fields, not metric labels.

## Metrics

### Export Modes

#### Prometheus Scrape

`/metrics` is not served unless `-server.register-instrumentation=true`. The Helm chart sets it and serves `/metrics` on a dedicated metrics listener (`extraArgs.metrics-listen: ":9091"`, exposed as the `metrics` Service port). A binary without `-metrics-listen` serves `/metrics` on its `-listen` port.

```yaml
scrape_configs:
  - job_name: loki-vl-proxy
    scrape_interval: 15s
    static_configs:
      - targets:
          - loki-vl-proxy:9091
```

#### OTLP Push

```bash
./loki-vl-proxy \
  -backend=http://victorialogs:9428 \
  -otlp-endpoint=http://otel-collector:4318/v1/metrics \
  -otlp-interval=30s \
  -otlp-compression=gzip \
  -otlp-headers='Authorization=Bearer example-token'
```

If the OTLP endpoint is passed as a collector base URL like `http://collector:4318` or `http://collector:4318/v1`, the proxy normalizes it to `/v1/metrics`.

### OpenTelemetry Resource Attributes for Metrics and Logs

These flags shape OTLP metric resource attributes. Structured logs intentionally do
not duplicate resource attributes per line; keep service identity in collector/OTLP
resource metadata to avoid `message.service.*` duplication in storage.

| Flag | Meaning |
|---|---|
| `-otel-service-name` | `service.name` |
| `-otel-service-namespace` | `service.namespace` |
| `-otel-service-instance-id` | `service.instance.id` |
| `-deployment-environment` | `deployment.environment.name` |

### Request Dimensions

Request-oriented metrics use stable low-cardinality dimensions so dashboards can slice by user-visible API shape without leaking raw paths or query content.

| Dimension | Prometheus scrape | OTLP push | Example |
|---|---|---|---|
| API system | `system` | `loki.api.system` | `loki`, `vl` |
| Direction | `direction` | `proxy.direction` | `downstream`, `upstream` |
| Request type | `endpoint` | `loki.request.type` | `query_range`, `labels`, `patterns` |
| Route template | `route` | `http.route` | `/loki/api/v1/query_range`, `/select/logsql/query` |
| Final status | `status` | `http.response.status_code` | `200`, `429`, `500` |

Downstream routes are the normalized Loki API templates registered by the proxy. Upstream routes are the stable VictoriaLogs or rules/alerts backend path templates used by the proxy itself. Raw request paths and query strings stay in logs, not in metric labels.

Tenant and client metric families are the only intentionally high-cardinality families, and even those are bounded with `-metrics.max-tenants` and `-metrics.max-clients`; excess identities collapse to `__overflow__`.

Histogram helper series (`_bucket`, `_sum`, `_count`) inherit the same label set and cardinality as the parent metric family.

### Cardinality Levels

| Level | Meaning |
|---|---|
| `Low` | no labels or only fixed route templates / small enums (`status`, `direction`, `mode`, `reason`) |
| `Medium` | bounded internal enums that may grow slowly with feature surface but not with traffic shape |
| `High (capped)` | user or tenant identity dimensions; bounded by `-metrics.max-tenants` / `-metrics.max-clients` with `__overflow__` fallback |

### Core Proxy Metrics

All rows below are exposed through Prometheus scrape and OTLP push unless noted otherwise.

| Metric | Type | Labels | Cardinality | Description |
|---|---|---|---|---|
| `loki_vl_proxy_requests_total` | counter | `system`, `direction`, `endpoint`, `route`, `status` | `Low` | all proxied requests, sliced by downstream Loki path or upstream backend path |
| `loki_vl_proxy_request_duration_seconds` | histogram | `system`, `direction`, `endpoint`, `route` | `Low` | end-to-end request latency |
| `loki_vl_proxy_backend_duration_seconds` | histogram | `system`, `direction`, `endpoint`, `route` | `Low` | upstream backend latency only (`system="vl"`, `direction="upstream"`) |
| `loki_vl_proxy_upstream_calls_per_request` | histogram | `system`, `direction`, `endpoint`, `route` | `Low` | number of upstream child requests fanned out under a single downstream request |
| `loki_vl_proxy_cache_hits_total` | counter | none | `Low` | global cache hits |
| `loki_vl_proxy_cache_misses_total` | counter | none | `Low` | global cache misses |
| `loki_vl_proxy_cache_hits_by_endpoint` | counter | `system`, `direction`, `endpoint`, `route` | `Low` | cache hits per normalized route |
| `loki_vl_proxy_cache_misses_by_endpoint` | counter | `system`, `direction`, `endpoint`, `route` | `Low` | cache misses per normalized route |
| `loki_vl_proxy_cache_tier_hits_total` | counter | `tier` | `Low` | cache hits per tier (`l0`, `l1_memory`, `l2_disk`, `l3_peer`) |
| `loki_vl_proxy_cache_tier_misses_total` | counter | `tier` | `Low` | cache misses per tier (`l0`, `l1_memory`, `l2_disk`, `l3_peer`) |
| `loki_vl_proxy_cache_tier_requests_total` | counter | `tier` | `Low` | total lookup attempts per tier (`l0`, `l1_memory`, `l2_disk`, `l3_peer`) |
| `loki_vl_proxy_cache_tier_stale_hits_total` | counter | `tier` | `Low` | responses served from an expired TTL entry per tier (`l1_memory`, `l2_disk`, `l3_peer`) |
| `loki_vl_proxy_cache_bytes` | gauge | `tier` | `Low` | stored bytes per cache tier (`l1_memory`, `l2_disk`) |
| `loki_vl_proxy_cache_objects` | gauge | `tier` | `Low` | object count per cache tier (`l0`, `l1_memory`, `l2_disk`) |
| `loki_vl_proxy_cache_backend_fallthrough_total` | counter | none | `Low` | requests that missed every cache tier and fell through to the backend |
| `loki_vl_proxy_translations_total` | counter | none | `Low` | successful LogQL to LogsQL translations |
| `loki_vl_proxy_translation_errors_total` | counter | none | `Low` | failed translations |
| `loki_vl_proxy_internal_operation_total` | counter | `operation`, `outcome` | `Medium` | named internal operations including translation, parser preference, rewrites and upstream body completion |
| `loki_vl_proxy_lowering_shadow_total` | counter | `result`, `construct` | `Low` | comparisons of the typed query-plan lowering with the string translator on live translation cache misses (opt-in, `-translate-shadow-sample`), and the queries the lowering translated instead of the translator (`-query-plan-lowering`, on by default): `result` is `match`, `mismatch`, `new_error`, `old_error`, `not_lowered`, `skipped`, `panic`, `budget`, `native_proxy_evaluated` (a ranking, `stddev` or `stdvar` the proxy evaluates itself whose lowering equals what the proxy runs: the operand's request and the levels applied to its answer), `known_divergence` (such a query whose plan holds a level the proxy's route drops, registry case `semantics/stddev-stdvar-nested-or-binary`: nothing compared) or `flipped` (a query translated by the lowering, nothing compared); `construct` is the node kind at the top of the plan (`LogSelect`, `RangeAgg`, `VectorAgg`, `Rank`, `BinOp`, `LabelReplace`, `VectorLit`, `Scalar`, `Opaque`), optionally followed by the part the lowering does not cover yet (or, for a root the proxy evaluates, `VectorAgg stddev` and `VectorAgg stdvar`), or `none` |
| `loki_vl_proxy_internal_operation_duration_seconds` | histogram | `operation`, `outcome` | `Medium` | named-operation duration; backend_body_read includes HTTP and consumer time |
| `loki_vl_proxy_request_phase_seconds` | histogram | `route`, `phase` | `Low` | time one client request spent per phase: `parse`, `translate`, `cache`, `vl`, `proxy` (the request's time minus `vl` and `cache`, so it includes `parse` and `translate`; an estimate when calls ran in parallel); a phase that did not run is not observed, `proxy` always is; Prometheus scrape only |
| `loki_vl_proxy_request_fanout` | histogram | `route`, `kind` | `Low` | work one client request caused, observed for every request, zero included: `parse`, `translate`, `cache_lookup`, `cache_hit`, `vl_call` counts; buckets 0, 1, 2, 3, 5, 8, 13, 21, 34, 55; Prometheus scrape only |
| `loki_vl_proxy_cache_lookup_seconds` | histogram | `tier`, `result` | `Low` | time of one cache lookup by the tier that answered (`l1_memory`, `l2_disk`, `l3_peer`, `unknown` for a miss of the full stack) and `result` (`hit`, `miss`, `stale`); buckets from 10 microseconds; Prometheus scrape only |
| `loki_vl_proxy_backend_read_bytes_total` | counter | `kind` | `Low` | consumed decoded VL HTTP body bytes recorded at close; fixed `raw`, `stats`, `metadata`, `other` kinds; not encoded network traffic or cache bytes; both Prometheus and OTLP |
| `loki_vl_proxy_coalesced_total` | counter | none | `Low` | requests served from coalesced results |
| `loki_vl_proxy_coalesced_saved_total` | counter | none | `Low` | backend requests saved by coalescing |
| `loki_vl_proxy_response_tuple_mode_total` | counter | `mode` | `Low` | emitted log tuple contract mode by client behavior (Prometheus scrape only today) |
| `loki_vl_proxy_uptime_seconds` | gauge | none | `Low` | process uptime |
| `loki_vl_proxy_active_requests` | gauge | none | `Low` | current in-flight requests |
| `loki_vl_proxy_circuit_breaker_state` | gauge | none | `Low` | `0=closed`, `1=open`, `2=half-open` |
| `loki_vl_proxy_http_connections` | gauge | `state` | `Low` | current downstream HTTP server connections by state |
| `loki_vl_proxy_http_connection_transitions_total` | counter | `state` | `Low` | downstream HTTP server connection state transitions |
| `loki_vl_proxy_http_connection_rotations_total` | counter | `reason` | `Low` | downstream HTTP/1.x connection rotations triggered by the proxy |
| `loki_vl_proxy_parser_metric_evaluations_total` | counter | `evaluator`, `reason` | `Low` | metric queries over a `\| json` or `\| logfmt` pipeline by the evaluator that answered them (`vl_stats_buckets`: VictoriaLogs stats over unpacked fields, no raw rows; `raw_rows`: the exact row evaluator, bounded by `-manual-range-metric-row-limit`) and why: `pushdown`; `probe` (a `\| limit 1` probe found a line the two parsers read differently, so the line shapes of that stream are worth a look); `ineligible` (a pipeline the pushdown does not cover: `keep`, a second parser, an `_extracted` or `__error__` label, a filter accepting the empty value without `drop __error__`); `grid` (no bucket grid for the step and range) |
| `loki_vl_proxy_slow_requests_total` | counter | `system`, `route` | `Low` | requests at or over their slow threshold: `system="loki"` a client request over `-slow-query-threshold`, `system="vl"` one VictoriaLogs call over `-slow-upstream-threshold` (see Slow Queries) |

`tier="l0"` is the hot-key index that sits alongside the lookup tiers, not a lookup tier itself: `requests` counts every cache request that touched it, `hits` counts requests whose key was already hot, `misses` counts first observations of a key, and `cache_objects{tier="l0"}` is the current bounded index population.

Operational notes for these hot paths:

- `query_range` and `labels` benchmarks in CI track both cache-hit and cache-bypass behavior
- multi-tenant read fanout and merged response bodies are capped to keep a single request from exhausting proxy memory
- synthetic tail keeps bounded dedup state so long-running websocket sessions do not grow without limit

### Semantic cache layers

For production Drilldown investigations, see the
[cache troubleshooting guide](cache-troubleshooting.md), including settled-step
eligibility/admission reasons and structured debug decision logs.

Prometheus scrape and OTLP push expose the same `loki_vl_proxy_cache_layer_*`
families. `layer` is `loki`, `vl` or `unknown`; `component` is `primary`,
`compat`, `translation` or `field_names`. These are fixed labels, without
tenant, credential, query or cache-key values. Layers describe payload
semantics; tiers describe where a lookup runs.

| Suffix after `loki_vl_proxy_cache_layer_` | Type | Meaning |
|---|---|---|
| `requests_total`, `hits_total`, `misses_total`, `stale_hits_total` | counter | Tier lookup outcomes; hits return a candidate which a caller may still reject during semantic validation |
| `read_bytes_total` | counter | Decoded value bytes returned by tier lookups |
| `write_bytes_total`, `admissions_total`, `rejections_total` | counter | L1 admission or L2 queue admission; replacements are new accepted writes |
| `evictions_total` | counter | Actual L1 entries or committed L2 records removed by capacity/expiry; excludes explicit invalidation, replacements and rejected incoming values |
| `objects`, `bytes` | gauge | L1 resident entries and key/value bytes; L2 committed records and key/encoded-record bytes |
| `lookup_seconds_total` | counter | Elapsed logical L2/L3 lookup time, including misses; not isolated mutex wait time |
| `disk_index_bytes`, `disk_queue_bytes` | gauge | Accounted RAM for the L2 index and pending-plus-flushing writes |
| `disk_encoded_read_bytes_total`, `disk_encoded_write_bytes_total` | counter | Encoded records copied from bbolt or written in successful transactions, excluding key and filesystem overhead |
| `coalesced_total`, `coalesced_saved_total`, `coalesced_wait_total`, `coalesced_wait_seconds_total` | counter | Successful shared requests, actual saved backend calls, follower waits and cumulative wait time; labels contain `layer` only |

Tier-labelled series use `l1_memory`, `l2_disk` and `l3_peer`. L3 exports
lookup outcomes, decoded reads and lookup duration. Peer occupancy and
write/admission/eviction series are omitted because these snapshots do not
measure them. L1 lookup time is also omitted. L2 can return a pending/flushing
RAM value: an L2 hit or decoded read is not proof of an SSD read. Encoded
record traffic is application traffic, not physical device throughput or IOPS.
Value-byte counters refer to the bytes passed to/from the cache after any
storage-codec decoding. A typed binary payload or an HTTP-compressed response
still counts its own representation; these counters do not decode application
payloads or estimate uncompressed log volume.
Encoded write bytes count record `Put` operations in successful transactions,
including records removed later in the same batch; they are not net retained
growth. Deletes, page writes, journal/filesystem amplification and failed
transactions are not estimated by that counter.

The mixed primary store attributes audited namespaces explicitly: Loki
responses, resolved log windows and `query-steps:v1:` are `loki`;
`stats-extent:v1:`, `vlinv-v1:` and backend field-name results are `vl`.
Unrecognized or ambiguous families remain `unknown`. Translation and
field-name auxiliary caches have fixed instance attribution because their
keys need not identify a semantic family. Compatibility responses are the
`compat` component, separate from the legacy `l0` hotness index. Distinct
cache instances are counted once even when profile/configuration pointers
alias; an aliased L2 store is attributed to its first component only.

RAM/index/queue bytes are application accounting charges. They exclude cache
object/map overhead, bbolt engine state, codec scratch and general in-flight
work; queue and L1 charges may reference shared payload backing memory.
They cannot be summed into measured unique heap or RSS. L2 logical bytes do
not divide the shared database file between layers; the legacy
`cache_bytes{tier="l2_disk"}` remains whole-file length. LRU eviction does not
necessarily shrink that file. Gauges include expired entries until removal,
and counters may advance during a scrape.

Backend coalescer observations belong to `vl`: its integrated call sites
wrap VL HTTP calls. `coalesced_total` retains the existing successful-shared
request definition, which includes a shared leader. `coalesced_saved_total`
and waits count actual followers, including shared failures. Disabled or
guard-rejected calls do not invent saved work. These are not peer transport
coalescer or resident cache statistics.

Examples:

```promql
# Accounted resident key/value bytes by semantic layer.
sum by (layer) (loki_vl_proxy_cache_layer_bytes{tier="l1_memory"})
# Decoded cache delivery bytes/second, not SSD/network bandwidth.
sum by (layer, tier) (rate(loki_vl_proxy_cache_layer_read_bytes_total[5m]))
# Committed encoded record traffic/second and actual expiry/capacity removals.
sum by (layer) (rate(loki_vl_proxy_cache_layer_disk_encoded_write_bytes_total[5m]))
sum by (layer, tier) (rate(loki_vl_proxy_cache_layer_evictions_total[5m]))
```

### Query-Range Windowing Metrics

These are the primary signals for long-range query performance and backend protection:

| Metric | Type | Labels | Cardinality | Description |
|---|---|---|---|---|
| `loki_vl_proxy_window_cache_hit_total` | counter | none | `Low` | cached split windows served without backend scan |
| `loki_vl_proxy_window_cache_miss_total` | counter | none | `Low` | split windows requiring backend scan |
| `loki_vl_proxy_window_fetch_seconds` | histogram | none | `Low` | backend fetch duration per split window |
| `loki_vl_proxy_window_merge_seconds` | histogram | none | `Low` | merge duration for split-window responses |
| `loki_vl_proxy_window_count` | histogram | none | `Low` | split windows per `query_range` request |
| `loki_vl_proxy_window_prefilter_attempt_total` | counter | none | `Low` | prefilter runs against `/select/logsql/hits` |
| `loki_vl_proxy_window_prefilter_error_total` | counter | none | `Low` | prefilter failures (proxy safely falls back to full window fanout) |
| `loki_vl_proxy_window_prefilter_kept_total` | counter | none | `Low` | split windows retained for real log fanout |
| `loki_vl_proxy_window_prefilter_skipped_total` | counter | none | `Low` | split windows skipped as empty by prefilter |
| `loki_vl_proxy_window_prefilter_hit_ratio` | gauge | none | `Low` | current prefilter kept/total ratio (0-1) |
| `loki_vl_proxy_window_retry_total` | counter | none | `Low` | per-window retry attempts after retryable backend failures |
| `loki_vl_proxy_window_degraded_batch_total` | counter | none | `Low` | batches that were downgraded to lower parallelism |
| `loki_vl_proxy_window_partial_response_total` | counter | none | `Low` | partial query-range responses returned when slow windows exceed budget |
| `loki_vl_proxy_window_prefilter_duration_seconds` | histogram | none | `Low` | prefilter latency |
| `loki_vl_proxy_window_adaptive_parallel_current` | gauge | none | `Low` | current adaptive split-window parallelism |
| `loki_vl_proxy_window_adaptive_latency_ewma_seconds` | gauge | none | `Low` | adaptive EWMA latency |
| `loki_vl_proxy_window_adaptive_error_ewma` | gauge | none | `Low` | adaptive EWMA backend error ratio |

### Patterns Snapshot Metrics

These metrics track the proxy-side pattern cache and snapshot lifecycle.

| Metric | Type | Labels | Cardinality | Description |
|---|---|---|---|---|
| `loki_vl_proxy_patterns_detected_total` | counter | none | `Low` | unique patterns detected from pattern mining |
| `loki_vl_proxy_patterns_stored_total` | counter | none | `Low` | pattern entries stored in proxy cache or snapshot updates |
| `loki_vl_proxy_patterns_restored_from_disk_total` | counter | none | `Low` | pattern entries restored from on-disk snapshots |
| `loki_vl_proxy_patterns_restored_from_peers_total` | counter | none | `Low` | pattern entries restored from peer snapshots |
| `loki_vl_proxy_patterns_restored_disk_entries_total` | counter | none | `Low` | snapshot cache keys restored from disk |
| `loki_vl_proxy_patterns_restored_peer_entries_total` | counter | none | `Low` | snapshot cache keys restored from peers |
| `loki_vl_proxy_patterns_deduplicated_total` | counter | `source` | `Low` | duplicate pattern snapshot entries removed by source (`mem`, `disk`, `peer`) |
| `loki_vl_proxy_patterns_in_memory` | gauge | none | `Low` | current number of patterns held in in-memory snapshot state |
| `loki_vl_proxy_patterns_cache_keys` | gauge | none | `Low` | current number of pattern cache keys held in memory |
| `loki_vl_proxy_patterns_in_memory_bytes` | gauge | none | `Low` | current bytes used by in-memory pattern snapshot payloads |
| `loki_vl_proxy_patterns_last_response_patterns` | gauge | none | `Low` | pattern entries returned in the most recent `/patterns` response |
| `loki_vl_proxy_patterns_last_response_bytes` | gauge | none | `Low` | encoded size of the most recent `/patterns` response |
| `loki_vl_proxy_patterns_persisted_disk_entries` | gauge | none | `Low` | snapshot cache keys present in the last persisted disk snapshot |
| `loki_vl_proxy_patterns_persisted_disk_patterns` | gauge | none | `Low` | pattern entries present in the last persisted disk snapshot |
| `loki_vl_proxy_patterns_persisted_disk_bytes` | gauge | none | `Low` | last persisted pattern snapshot size on disk |
| `loki_vl_proxy_patterns_persist_writes_total` | counter | none | `Low` | completed pattern snapshot writes to disk |
| `loki_vl_proxy_patterns_persist_write_bytes_total` | counter | none | `Low` | cumulative bytes written by pattern snapshot persistence |
| `loki_vl_proxy_patterns_restored_disk_bytes_total` | counter | none | `Low` | cumulative bytes restored from on-disk pattern snapshots |
| `loki_vl_proxy_patterns_restored_peer_bytes_total` | counter | none | `Low` | cumulative bytes restored from peer snapshot warmup |
| `loki_vl_proxy_patterns_source_lines_requested_total` | counter | none | `Low` | source lines requested from backend pattern fetches |
| `loki_vl_proxy_patterns_source_lines_scanned_total` | counter | none | `Low` | source lines scanned from backend responses |
| `loki_vl_proxy_patterns_source_lines_observed_total` | counter | none | `Low` | source lines accepted into the pattern miner |
| `loki_vl_proxy_patterns_windows_attempted_total` | counter | none | `Low` | pattern fetch windows attempted |
| `loki_vl_proxy_patterns_windows_accepted_total` | counter | none | `Low` | pattern fetch windows accepted into the merged response |
| `loki_vl_proxy_patterns_windows_capped_total` | counter | none | `Low` | pattern fetch windows that hit the per-window source line cap |
| `loki_vl_proxy_patterns_second_pass_windows_total` | counter | none | `Low` | pattern fetch windows retried with a higher line limit |
| `loki_vl_proxy_patterns_mined_pre_merge_total` | counter | none | `Low` | pattern entries mined before cross-window merge |
| `loki_vl_proxy_patterns_mined_post_merge_total` | counter | none | `Low` | pattern entries after cross-window merge |
| `loki_vl_proxy_patterns_snapshot_hits_total` | counter | none | `Low` | pattern snapshot fallback lookups that found cached payloads |
| `loki_vl_proxy_patterns_snapshot_misses_total` | counter | none | `Low` | pattern snapshot fallback lookups that missed |
| `loki_vl_proxy_patterns_snapshot_reused_total` | counter | none | `Low` | cached snapshot payloads actually reused in `/patterns` responses |
| `loki_vl_proxy_patterns_low_coverage_responses_total` | counter | none | `Low` | responses flagged as likely degraded by capped or incomplete mining coverage |

### Peer Cache Metrics

When the peer cache is enabled, `/metrics` appends these fleet-cache families (Prometheus scrape only). They mirror the counters in `PeerCache.Stats()`:

| Metric | Type | Labels | Cardinality | Description |
|---|---|---|---|---|
| `loki_vl_proxy_peer_cache_peers` | gauge | none | `Low` | remote peers currently in the fleet-cache ring, excluding self |
| `loki_vl_proxy_peer_cache_cluster_members` | gauge | none | `Low` | total ring members, including this instance |
| `loki_vl_proxy_peer_cache_replication_factor` | gauge | none | `Low` | replicas of every key, owner included (`-peer-replication-factor`) |
| `loki_vl_proxy_peer_cache_hits_total` | counter | none | `Low` | successful peer-cache fetches, from the owner or another replica |
| `loki_vl_proxy_peer_cache_backup_hits_total` | counter | none | `Low` | peer-cache fetches served by a replica other than the key's owner (owner down or without the key, or this instance is the owner); included in hits |
| `loki_vl_proxy_peer_cache_misses_total` | counter | none | `Low` | peer-cache lookups that missed on every replica asked |
| `loki_vl_proxy_peer_cache_fetch_duration_seconds` | histogram | `replica`, `outcome` | `Low` | duration of each peer-cache fetch by the replica asked (`owner`, `backup`) and its outcome (`hit`, `miss`, `error`); a fetch from a paused peer ends at `-peer-timeout`, or earlier when another fetch's timeout suspends that peer |
| `loki_vl_proxy_peer_cache_errors_total` | counter | none | `Low` | peer-cache fetch errors |
| `loki_vl_proxy_peer_cache_error_reason_total` | counter | `reason` | `Low` | peer-cache fetch errors by reason (emitted once any error has been recorded) |
| `loki_vl_proxy_peer_cache_write_through_pushes_total` | counter | none | `Low` | successful pushes to a key's replicas (one per replica) |
| `loki_vl_proxy_peer_cache_write_through_errors_total` | counter | none | `Low` | replica push errors |
| `loki_vl_proxy_peer_cache_push_too_large_total` | counter | none | `Low` | pushes skipped because the value is above the peers' accepted body size (8 MiB) |
| `loki_vl_proxy_peer_cache_peer_not_up_total` | counter | none | `Low` | refused connections to peers that had not answered since this process started: a peer still starting is skipped for 250 ms and tried again, without counting towards its breaker (reason `peer_not_up` in `peer_cache_error_reason_total` for lookups) |
| `loki_vl_proxy_peer_cache_push_peer_not_up_total` | counter | none | `Low` | pushes to a peer still starting that were dropped: above 256 kept pushes or 8 MiB per peer, or the peer did not answer within 30 s (the others are sent once it answers) |
| `loki_vl_proxy_peer_cache_push_throttled_total` | counter | none | `Low` | pushes dropped because the push queue was full: at most 64 pushes run at once (a slow replica holds its pushes for up to `-peer-timeout`) and 4,096 pushes or 64 MiB wait behind them; the replica that misses a copy fetches it from another replica on its first lookup |
| `loki_vl_proxy_peer_cache_hot_index_requests_total` | counter | none | `Low` | peer hot-index requests |
| `loki_vl_proxy_peer_cache_hot_index_errors_total` | counter | none | `Low` | peer hot-index request errors |
| `loki_vl_proxy_peer_cache_read_ahead_prefetches_total` | counter | none | `Low` | successful hot read-ahead prefetches |
| `loki_vl_proxy_peer_cache_read_ahead_prefetch_bytes_total` | counter | none | `Low` | bytes prefetched by hot read-ahead |
| `loki_vl_proxy_peer_cache_read_ahead_budget_drops_total` | counter | none | `Low` | read-ahead candidates dropped by budget or size filters |
| `loki_vl_proxy_peer_cache_read_ahead_tenant_skips_total` | counter | none | `Low` | read-ahead candidates skipped by tenant fairness |

Cross-tier peer lookups are also visible as `loki_vl_proxy_cache_tier_*{tier="l3_peer"}`. `GET /_cache/peers` returns the current ring membership as JSON (see [Fleet Cache](fleet-cache.md)).

### Tenant and Client Metrics

These are the metrics to use when you want to identify the users or tenants actually causing backend load.

| Metric | Type | Labels | Cardinality | Description |
|---|---|---|---|---|
| `loki_vl_proxy_tenant_requests_total` | counter | `system`, `direction`, `tenant`, `endpoint`, `route`, `status` | `High (capped)` | request volume by tenant |
| `loki_vl_proxy_tenant_request_duration_seconds` | histogram | `system`, `direction`, `tenant`, `endpoint`, `route` | `High (capped)` | latency by tenant |
| `loki_vl_proxy_tenant_vl_calls_total` | counter | `tenant` | `High (capped)` | VictoriaLogs calls made for the tenant's client requests |
| `loki_vl_proxy_tenant_vl_duration_seconds_total` | counter | `tenant` | `High (capped)` | their summed time, to the end of the body (calls in parallel add up) |
| `loki_vl_proxy_tenant_vl_response_bytes_total` | counter | `tenant` | `High (capped)` | bytes VictoriaLogs returned for them |
| `loki_vl_proxy_tenant_vl_rows_total` | counter | `tenant` | `High (capped)` | log rows read from VictoriaLogs for them |
| `loki_vl_proxy_client_requests_total` | counter | `system`, `direction`, `client`, `endpoint`, `route` | `High (capped)` | request volume by client identity |
| `loki_vl_proxy_client_response_bytes_total` | counter | `client` | `High (capped)` | response bytes by client |
| `loki_vl_proxy_client_status_total` | counter | `system`, `direction`, `client`, `endpoint`, `route`, `status` | `High (capped)` | final status breakdown by client |
| `loki_vl_proxy_client_inflight_requests` | gauge | `client` | `High (capped)` | current parallelism by client |
| `loki_vl_proxy_client_request_duration_seconds` | histogram | `system`, `direction`, `client`, `endpoint`, `route` | `High (capped)` | request latency by client |
| `loki_vl_proxy_client_query_length_chars` | histogram | `system`, `direction`, `client`, `endpoint`, `route` | `High (capped)` | query size outliers by client |
| `loki_vl_proxy_client_errors_total` | counter | `system`, `direction`, `endpoint`, `route`, `reason` | `Low` | categorized downstream client errors |

This is one of the main advantages of putting an explicit proxy between the
Grafana Loki datasource and VictoriaLogs: the read path becomes attributable.

Instead of only seeing aggregate datasource traffic, operators can see:

- which Grafana user or trusted client identity is generating load
- which tenant is hot
- which route is expensive for that client or tenant
- which client is producing the largest responses, longest queries, or most bad requests

### Grafana Client Visibility, Offenders, and User Patterns

When `-metrics.trust-proxy-headers=true` is enabled behind a trusted Grafana or
auth proxy, the proxy can turn northbound identity into durable read-path
signals without using raw datasource credentials as the end-user key.

That gives you:

- per-client request rate by route via `loki_vl_proxy_client_requests_total`
- per-client latency by route via `loki_vl_proxy_client_request_duration_seconds`
- per-client response-volume visibility via `loki_vl_proxy_client_response_bytes_total`
- per-client query-size outlier visibility via `loki_vl_proxy_client_query_length_chars`
- per-client bad-request and error clustering via `loki_vl_proxy_client_status_total` and `loki_vl_proxy_client_errors_total`
- per-tenant volume and latency visibility via `loki_vl_proxy_tenant_*`, and the VictoriaLogs cost each tenant causes via `loki_vl_proxy_tenant_vl_*_total`

Those tenant and client identity series are opt-in. Set `-metrics.export-sensitive-labels=true` only on trusted scrape or OTLP paths where exposing identity labels is acceptable.

At log level, the same request can also carry:

- `enduser.id`
- `enduser.name`
- `enduser.source`
- `auth.source`
- `loki.tenant.id`
- `http.route`

Per-request by-type breakdown maps such as `upstream.calls_by_type` and
`proxy.operations_by_type` are emitted only at debug level. The default info-level
request logs keep aggregate counts while the detailed per-type visibility stays in
Prometheus/OTLP metrics. This avoids log-body field explosion in pipelines that
flatten structured JSON bodies into discoverable `message.*` fields.

That separation matters:

- `enduser.*` answers "which Grafana user or trusted client triggered this?"
- `auth.source` answers "which datasource auth mechanism was used on the request path?" (for example `basic_auth`; the Basic-Auth username is credential material and is never logged)
- `loki.tenant.id` answers "which tenant boundary did the request execute in?"

This is what makes offender analysis practical on the read path instead of only
looking at coarse IP-level traffic.

### Northbound and Southbound Auth Boundaries

The same proxy layer also improves trust separation between components.

| Boundary | Main controls | Why it matters operationally |
|---|---|---|
| Grafana or client -> proxy | `-auth.enabled`, `-tls-client-ca-file`, `-tls-require-client-cert`, trusted user headers with `-metrics.trust-proxy-headers` | Lets the proxy require tenant context, optionally require client certs, and attribute read traffic to the actual Grafana user or trusted upstream identity when sensitive metrics export is explicitly enabled. |
| Proxy -> VictoriaLogs | `-backend-basic-auth`, `-forward-authorization`, `-forward-headers` | Lets the lower layer keep its own auth boundary while the proxy preserves full Loki-client compatibility on the northbound side. |
| Proxy -> peer cache | `-peer-auth-token` | Prevents peer-cache reuse from becoming an unauthenticated east-west path. Required whenever peer discovery is configured; the proxy refuses to start without it unless `-peer-insecure-ip-allowlist=true` restores the legacy IP-only check. |
| Operator -> admin/debug endpoints | `-server.admin-auth-token`, `-admin-listen` | Protects admin and troubleshooting surfaces without weakening the main read path. Without a token, `/admin/*` and `/debug/*` live on the loopback `-admin-listen` address (default `127.0.0.1:3101`); when instrumentation, pprof or query analytics is enabled, the proxy refuses to start if those routes would be exposed on a non-loopback address without the token. |

The proxy sends derived context headers to VictoriaLogs on every request: the
trusted user when trusted proxy headers are enabled, otherwise the tenant
(`X-Scope-OrgID`) or the client address:

- `X-Loki-VL-Client-ID`
- `X-Loki-VL-Client-Source`

That gives the lower layer better context about who is really behind the read
traffic while still preserving datasource compatibility at the Grafana edge.

### Runtime and Process Metrics

The proxy also exports a lightweight built-in set of runtime and process/container health metrics.
App-scoped aliases are emitted with the `loki_vl_proxy_` prefix, while legacy `go_*` and `process_*` families remain for compatibility:

Grouped family rows below mean every concrete metric name in that family shares the same cardinality profile.

| Metric family | Labels | Cardinality | Description |
|---|---|---|---|
| `loki_vl_proxy_go_memstats_*`, `loki_vl_proxy_go_goroutines`, `loki_vl_proxy_go_gc_cycles_total`, `loki_vl_proxy_go_gc_duration_seconds` | none | `Low` | Go runtime health |
| `loki_vl_proxy_process_resident_memory_bytes`, `loki_vl_proxy_process_open_fds` | none | `Low` | process resource usage |
| `loki_vl_proxy_process_cpu_seconds_total` | none | `Low` | total user and system CPU time of the proxy process, in seconds |
| `loki_vl_proxy_process_cpu_usage_ratio` | `mode` | `Low` | CPU pressure split by `user`, `system`, `iowait` |
| `loki_vl_proxy_process_memory_*` | none | `Low` | total, free, available, usage ratio |
| `loki_vl_proxy_process_disk_*_bytes_total` | none | `Low` | disk I/O byte counters |
| `loki_vl_proxy_process_disk_*_operations_total` | none | `Low` | disk read/write operation counters |
| `loki_vl_proxy_process_network_*_bytes_total` | none | `Low` | network I/O counters |
| `loki_vl_proxy_process_pressure_*_{some,full}_ratio` | `window` | `Low` | Linux PSI gauges when available (`10s`, `60s`, `300s`) |

Legacy unprefixed compatibility aliases (`go_*`, `process_*`) follow the same label sets and cardinality profile as their `loki_vl_proxy_*` counterparts.

Kubernetes notes:
- These runtime/system metrics are read from `/proc` and do not require Kubernetes RBAC permissions.
- PSI metrics (`process_pressure_*`) depend on kernel support and may be absent on nodes without `/proc/pressure/*`.
- On startup, the proxy logs a system-metrics readiness check with missing families and remediation hints instead of failing silently.
- Host-scope reads (`stat`, `meminfo`, `pressure/{cpu,memory,io}`) use `-host-proc-root`; self/container-scope reads (`self/*`, `net/dev`) use `-proc-root` (both default to `/proc`). The chart leaves `systemMetrics.hostProc.enabled` off by default (no `hostPath` volume, so the pod passes Pod Security `restricted`), and the proxy then reads the host-scope files from the container's own `/proc`. Set `systemMetrics.hostProc.enabled=true` to mount only those five host files read-only under `/host/proc` and set `-host-proc-root=/host/proc`; this needs a namespace that allows `hostPath` volumes, and the chart never mounts the whole host `/proc`.
- For per-pod attribution in OTLP backends, set `OTEL_SERVICE_INSTANCE_ID` from pod name and `OTEL_SERVICE_NAMESPACE` from pod namespace (the upstream chart now injects these by default).
- CI includes a metric-name guard so new app metrics must stay under the `loki_vl_proxy_*` prefix unless explicitly allowlisted for compatibility.

### PromQL Drilldowns For Slowness And Client Errors

Use these queries to quickly isolate downstream client pain, upstream slowness, and route-specific cache efficiency:

| Goal | Query |
|---|---|
| Downstream p95 latency by route | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_request_duration_seconds_bucket{system="loki",direction="downstream"}[5m])) by (le, endpoint, route))` |
| Upstream p95 latency by route | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket{system="vl",direction="upstream"}[5m])) by (le, endpoint, route))` |
| Downstream 5xx rate by route | `sum(rate(loki_vl_proxy_requests_total{system="loki",direction="downstream",status=~"5.."}[5m])) by (endpoint, route)` |
| Tenant p99 latency by route | `histogram_quantile(0.99, sum(rate(loki_vl_proxy_tenant_request_duration_seconds_bucket{system="loki",direction="downstream"}[5m])) by (le, tenant, endpoint, route))` |
| Route cache hit ratio | `sum(rate(loki_vl_proxy_cache_hits_by_endpoint{system="loki",direction="downstream"}[5m])) by (endpoint, route) / clamp_min(sum(rate(loki_vl_proxy_cache_hits_by_endpoint{system="loki",direction="downstream"}[5m])) by (endpoint, route) + sum(rate(loki_vl_proxy_cache_misses_by_endpoint{system="loki",direction="downstream"}[5m])) by (endpoint, route), 1)` |
| Client bad_request by route | `sum(rate(loki_vl_proxy_client_errors_total{system="loki",direction="downstream",reason="bad_request"}[5m])) by (endpoint, route)` |
| VictoriaLogs calls per request, p95 by route | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_request_fanout_bucket{kind="vl_call"}[5m])) by (le, route))` |
| Requests answered with no VictoriaLogs call | `sum(rate(loki_vl_proxy_request_fanout_bucket{kind="vl_call",le="0"}[5m])) by (route) / sum(rate(loki_vl_proxy_request_fanout_count{kind="vl_call"}[5m])) by (route)` |
| Cache lookups per request, p95 by route | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_request_fanout_bucket{kind="cache_lookup"}[5m])) by (le, route))` |
| Share of request time in cache vs VictoriaLogs, by route | `sum(rate(loki_vl_proxy_request_phase_seconds_sum{phase="cache"}[5m])) by (route) / sum(rate(loki_vl_proxy_request_phase_seconds_sum{phase=~"cache\|vl\|proxy"}[5m])) by (route)` and the same with `phase="vl"` (`parse` and `translate` run inside `proxy`, so leave them out of the denominator) |
| Where the time goes (`cache`, `vl`, `proxy` add up to the request time; `parse` and `translate` are part of `proxy`) | `sum(rate(loki_vl_proxy_request_phase_seconds_sum{phase=~"cache\|vl\|proxy"}[5m])) by (phase)` |
| p99 cache lookup time by tier | `histogram_quantile(0.99, sum(rate(loki_vl_proxy_cache_lookup_seconds_bucket{result="hit"}[5m])) by (le, tier))` |

For latency histograms, keep dashboards on `p50`, `p95`, and `p99` rather than averages. Averages hide tail latency incidents. For exact proxy-only overhead, use structured logs (`proxy.overhead_ms`) alongside the latency histograms; subtracting histogram quantiles is not mathematically reliable.

The packaged `Loki-VL-Proxy` dashboard includes a `Process Resources` row (Section 2) with:

- memory saturation and memory footprint/headroom
- CPU usage split by mode
- disk IOPS up/down and disk throughput up/down
- network up/down
- PSI pressure (cpu/memory/io)
- process RSS and open file descriptors by pod

The dashboard is organized into three collapsible sections:

**Section 1: SLO / SLI + Health** — SLO stat strip and SLI time-series for request rate, error rate, latency, cache hit ratio, and circuit breaker state.

**Section 2: Client → Proxy → VL + Resources (Operational)** — four rows covering all operational perspectives end-to-end:

- `Client-Side Loki API Visibility` — downstream request rate, errors by reason, query length
- `Proxy Internal Visibility` — proxy internals, internal operations by type and latency
- `Backend — VictoriaLogs Visibility` — upstream backend latency, duration by endpoint, upstream fanout
- `Process Resources` — memory, CPU, disk I/O, network, PSI pressure, RSS, open FDs

**Section 3: Deep Proxy Internals (Tuning & Visibility)** — six rows for deep investigation and tuning:

- `Cache Tiers (T0/L1/L2/L3)` — per-tier hit/miss/stale rates, cache size (objects and bytes), backend fallthrough
- `Peer Cache Fleet Distribution` — active peers, peer hit/miss, peer error breakdown
- `Query-Range Windowing & Prefilter` — window cache hit/miss, prefilter efficiency, retries, partial responses
- `Patterns Engine (Grafana Logs Drilldown)` — patterns in memory, cache keys, memory bytes, mining rate, persistence
- `HTTP Connection Lifecycle` — downstream connection state, transitions, rotations
- `Tenant Deep Dive` — per-tenant request rate, P99 latency, error rate

Suggested triage path:

1. Start on Section 1 (SLO / SLI) to confirm user-visible impact and overall health.
2. Move to Section 2, `Backend — VictoriaLogs Visibility` to separate upstream pressure from proxy-side issues.
3. Use Section 2, `Client-Side Loki API Visibility` to identify offending routes, clients, or tenants.
4. Drill into Section 3 to isolate cache tier behavior, windowing, peer distribution, and runtime resource bottlenecks.

The `Query-Range Windowing & Prefilter` row (Section 3) covers both windowing cache/tuning signals and long-range resilience KPIs:

- window fetch p50/p95 latency
- window merge p50/p95 latency
- window cache hit ratio
- adaptive window parallelism + EWMA latency/error
- prefilter kept/skipped rate
- retry/degraded-batch/partial-response rate
- prefilter hit ratio

Dashboard datasource notes:

- datasource variable regex is intentionally permissive (`/.*/`) so the dashboard works with scrape-backed and OTLP-backed metric datasources without renaming
- key stat panels use explicit zero fallbacks so dashboards remain readable during cold starts and low-traffic windows

### Active Backend E2E Healthchecks

`/ready` confirms backend reachability, but production health should also include synthetic end-to-end probes with real query traffic shape.

Recommended pattern:

1. Probe `/ready` every 15-30s for hard availability.
2. Run a lightweight synthetic `query_range` every 1-5m from inside the cluster.
3. Alert when synthetic query latency or error ratio breaches SLO even if `/ready` is green.

This catches backend partial degradation (slow scans, storage pressure, auth drift) earlier than readiness alone.

## Choosing Client Identity

Per-client metrics and request logs can use trusted upstream identity instead of only remote IP:

```bash
-metrics.trust-proxy-headers=true
```

When enabled, the proxy prefers:

1. Trusted user headers (`X-Grafana-User`, `X-Forwarded-User`, `X-Webauth-User`, `X-Auth-Request-User`)
2. tenant
3. trusted forwarded client IP (`X-Forwarded-For`)
4. remote IP

Datasource/basic-auth credentials are not used as end-user identity; request logs record only the auth mechanism as `auth.source`.
Only enable trusted proxy headers when the proxy sits behind a trusted auth proxy or Grafana instance.

## Integration Examples

### OpenTelemetry Collector: scrape `/metrics` and export OTLP

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: loki-vl-proxy
          scrape_interval: 15s
          static_configs:
            - targets: ["loki-vl-proxy:9091"]  # chart metrics port; use the -listen port if -metrics-listen is unset

processors:
  batch: {}

exporters:
  otlphttp:
    endpoint: https://otel-gateway.example.com
    headers:
      Authorization: Bearer ${OTLP_TOKEN}

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [otlphttp]
```

### OpenTelemetry Collector: collect JSON logs from container stdout

```yaml
receivers:
  filelog:
    include:
      - /var/log/containers/*loki-vl-proxy*.log
    operators:
      - type: json_parser

processors:
  batch: {}

exporters:
  otlphttp:
    endpoint: https://otel-gateway.example.com

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [otlphttp]
```

### Vector: ship structured JSON logs

```toml
[sources.proxy_logs]
type = "kubernetes_logs"

[transforms.proxy_json]
type = "remap"
inputs = ["proxy_logs"]
source = '''
. = parse_json!(string!(.message))
'''

[sinks.proxy_otlp]
type = "opentelemetry"
inputs = ["proxy_json"]
protocol.type = "http"
protocol.uri = "https://otel-gateway.example.com/v1/logs"
```

### Fluent Bit: tail container logs and keep JSON structure

```ini
[INPUT]
    Name              tail
    Path              /var/log/containers/*loki-vl-proxy*.log
    Parser            docker
    Tag               loki_vl_proxy

[FILTER]
    Name              parser
    Match             loki_vl_proxy
    Key_Name          log
    Parser            json

[OUTPUT]
    Name              opentelemetry
    Match             loki_vl_proxy
    Host              otel-collector
    Port              4318
    Logs_uri          /v1/logs
```

## Recommended Dashboards and Alerts

Start with:

- request rate and error rate by `endpoint`
- backend latency p95/p99 by `endpoint`
- cache hit ratio overall and by `endpoint`
- top `client` by request rate, bytes, and query length
- top `tenant` by request volume and latency
- circuit breaker state
- process RSS and open file descriptors

### Dashboard Catalog

| Dashboard | Source | Primary use |
|---|---|---|
| [`dashboard/loki-vl-proxy.json`](../dashboard/loki-vl-proxy.json) | Prometheus metrics | Service health, SLOs, cache and endpoint latency trends |

#### Metrics Dashboard Setup (Scrape and OTLP Push)

The metrics dashboard includes a `Datasource` variable and works with either metric transport mode:

- Prometheus scrape (`/metrics` + `ServiceMonitor`)
- OTLP push (`-otlp-endpoint=...`) into a Prometheus-compatible backend

Recommended setup:

1. Point `Datasource` to any Prometheus-compatible datasource that contains `loki_vl_proxy_*` metrics.
2. For scrape mode, use the datasource fed by your `ServiceMonitor`/Prometheus scrape pipeline.
3. For OTLP push mode, use the datasource fed by your OTLP metrics pipeline.
4. VictoriaMetrics can be used for both modes when it receives both scrape and OTLP streams.

Transport checklist:

- Scrape mode:
  - `-server.register-instrumentation=true` (binary default is `false`; the chart sets `true`)
  - Helm: `serviceMonitor.enabled=true`, scraping the `metrics` Service port (`extraArgs.metrics-listen`, default `:9091`)
  - Helm with the default `networkPolicy.enabled=true`: also set `networkPolicy.monitoringNamespace` (or `networkPolicy.monitoringFrom`, or an explicit `networkPolicy.monitoringAllowAll=true`), otherwise templating fails
- OTLP push mode:
  - `-otlp-endpoint` configured
  - push-only (optional): `-server.register-instrumentation=false`; with the chart also set `service.metrics.enabled=false` and clear `extraArgs.metrics-listen`, because the chart and the binary both refuse a metrics listener without instrumentation

Quick validation in Grafana Explore against the selected datasource:

```promql
loki_vl_proxy_uptime_seconds
```

If this query has data, the `Loki-VL-Proxy Metrics` dashboard should populate out of the box.

High-signal alert ideas:

- `5xx` rate rising on query endpoints
- cache hit ratio collapsing
- backend latency p95 breaching SLO
- a single `client` dominating bytes or query length
- circuit breaker opening repeatedly

The packaged alert set and incident procedures live in:

- [`alerting/loki-vl-proxy-prometheusrule.yaml`](../alerting/loki-vl-proxy-prometheusrule.yaml) — 15 alerts rendered by the Helm chart as a `PrometheusRule` (`prometheusRule.enabled=true`), each with a `runbook_url`
- [`alerting/loki-vl-proxy-alerting-rules.yaml`](../alerting/loki-vl-proxy-alerting-rules.yaml) — 16 alerts as a plain rule-group file for Prometheus, vmalert or Grafana alerting (thresholds to adjust; no runbook links, and the per-tenant alerts need `-metrics.export-sensitive-labels=true`)
- [`docs/runbooks/alerts.md`](runbooks/alerts.md)

## Notes

- OTLP push and Prometheus scrape share the same important proxy metrics and metric names.
- The OTLP export is intentionally lightweight and does not pull in the full OpenTelemetry Go SDK.
- Structured logs are already safe for JSON ingestion; agents can forward them directly or transform them into OTLP logs.
