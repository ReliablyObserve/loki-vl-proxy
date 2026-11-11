---
sidebar_label: Tenancy and Authentication
description: Every tenancy and authentication option with its default, and setups that follow Loki, follow VictoriaLogs, or mix them.
---

# Tenancy and Authentication

This is the operator guide for who can read what through Loki-VL-proxy. It lists every option and its default, so you can pick a setup that follows Loki, follows VictoriaLogs, or mixes them. Each flag's type, default and Helm key is also listed in the generated [configuration reference](reference/configuration-reference.md).

## 1. Defaults at a glance

| Surface | Default | Turn it on or open it with |
|---|---|---|
| Tenancy | **Off.** `X-Scope-OrgID` is ignored, every request reads VictoriaLogs account `0:0`, and every Grafana user sees all data, as Loki with `auth_enabled: false`. | Any one of: `-auth.enabled`, `-require-tenant-header`, `-tenant-map`, `-tenant-map-file`, `-tenant-label`, `-tenant.numeric-passthrough=true` (see [Choose a setup](#2-choose-a-setup)) |
| Caller authentication | None. The proxy does not authenticate callers, and neither does Loki. | A gateway in front of the proxy, or client certificates (`-tls-client-ca-file`, `-tls-require-client-cert`) |
| Write path `/loki/api/v1/push` | Blocked: `405` | Not available; send logs to VictoriaLogs |
| Delete API `/loki/api/v1/delete` | Not registered: `404`, as Loki with deletion disabled | `-delete-api-enabled=true` plus its own token `-delete-api-auth-token` (Helm `deleteAuth.existingSecret`); the proxy refuses to start without the token. Every request must carry it in `X-Delete-Token` or `Authorization: Bearer`. |
| Admin and debug (`/admin/cache/flush`, `/debug/pprof/*`, `/debug/queries`) | Off. When enabled without a token they listen on the loopback `-admin-listen` (`127.0.0.1:3101`) only. | `-server.register-instrumentation`, `-server.enable-pprof`, `-server.enable-query-analytics`; `-server.admin-auth-token` (Helm `adminAuth.existingSecret`) moves them to the main listener behind the token. The proxy refuses to expose them on a non-loopback address without the token. |
| `/metrics` | Not served | `-server.register-instrumentation=true`, optionally on its own port with `-metrics-listen`. It shows no tokens, credentials, tenant-map values or addresses, and no tenant or user names unless `-metrics.export-sensitive-labels=true`. |
| `/config` | A one-line stub on the main listener | Never shows the configuration |
| Peer cache (`/_cache/*`) | Off. With peers configured, the proxy refuses to start without `-peer-auth-token` | `-peer-auth-token` (or the explicit `-peer-insecure-ip-allowlist=true`) |
| Client credentials toward VictoriaLogs | Nothing a client sends is forwarded | `-forward-authorization`, `-forward-headers`, `-forward-cookies`, `-metrics.trust-proxy-headers` (see [Behind a gateway](#e-behind-a-gateway-vmauth-an-oauth-proxy)) |

## 2. Choose a setup

| You want | Setup | Key flags |
|---|---|---|
| One shared log store, everyone sees everything (Loki `auth_enabled: false`) | [(a) Loki single-tenant](#a-loki-single-tenant-the-default) | none |
| Tenants named by `X-Scope-OrgID`, Loki's `auth_enabled: true` behaviour, `a\|b` queries | [(b) Loki multi-tenant](#b-loki-multi-tenant) | `-auth.enabled` + `-tenant-map` (or numeric passthrough) |
| Clients send VictoriaLogs account numbers | [(c) VictoriaLogs-native accounts](#c-victorialogs-native-accounts) | `-tenant.numeric-passthrough=true` |
| Named tenants and numeric accounts together, or one VictoriaLogs tenant split by a field | [(d) Hybrid](#d-hybrid-tenant-map-and-numeric-passthrough-or-tenant-label) | `-tenant-map` / `-tenant-map-file` (+ passthrough), or `-tenant-label` |
| An identity-aware gateway in front | [(e) Behind a gateway](#e-behind-a-gateway-vmauth-an-oauth-proxy) | one of (b)–(d), plus forwarding flags as needed |

### Every setup against Loki and VictoriaLogs

| Setup | Loki `auth_enabled: false` | Loki `auth_enabled: true` | VictoriaLogs `AccountID`/`ProjectID` headers | VictoriaLogs behind vmauth |
|---|---|---|---|---|
| (a) single-tenant (default) | **same**: header ignored, one tenant for everyone | differs: no tenants, no `401` | every request on `0:0` | vmauth sees no tenant header from the proxy |
| (b) Loki multi-tenant | differs: header required (`-auth.enabled`) | **same**: `401 no org id`, Loki's `400` texts, `a\|b` with `__tenant_id__`; an unmapped name gets the proxy's `403` (Loki would serve an empty tenant) | each org on its mapped account | the proxy sends `AccountID`/`ProjectID` (and `X-Scope-OrgID` with `-forward-tenant-header`) |
| (c) native accounts | differs: header selects an account | close: numbers only; `007` gets `403` | **same**: canonical `N` → `AccountID: N`, `ProjectID: 0` | as (b) |
| (d) hybrid / tenant label | differs | as (b) for mapped names; label mode filters one VictoriaLogs tenant by a stream field | mapped names and numbers each on their account; label mode stays on `0:0` with `extra_stream_filters` | as (b) |
| (e) behind a gateway | the gateway decides | the gateway sets `X-Scope-OrgID` | as the chosen setup | vmauth authenticates the proxy (`-backend-basic-auth`) |

### Refusals with tenancy on

Once tenancy is on, the proxy refuses a header as Loki 3.7.9 does with `auth_enabled: true`, before any cache lookup or VictoriaLogs call. Loki's answers are recorded in `internal/proxy/testdata/tenant_auth_loki_answers.jsonl` and replayed by `TestTenantAuth_AnswersLikeLoki`. The full per-route table is in the [API reference](api-reference.md#tenant-header-x-scope-orgid).

| Header | Read routes |
|---|---|
| Missing, empty or blank, with `-auth.enabled` or `-require-tenant-header` | `401` `no org id` |
| `.` or `..`, a byte outside `A-Z a-z 0-9 ! ' ( ) * - . : _`, or longer than 150 bytes | `400` with Loki's text (for example `tenant ID 'a b' contains unsupported character ' '`) |
| A valid name the proxy cannot route (not in the map, a non-canonical number, `*` without `-tenant.allow-global`) | `403` `unknown tenant ...`. This refusal is the proxy's own: VictoriaLogs has numeric accounts only, so an unmapped name is never served from another account. |

### (a) Loki single-tenant (the default)

- **Flags:** none. Tenancy is off while `-tenant.numeric-passthrough` is unset (or `false`) and none of `-auth.enabled`, `-require-tenant-header`, `-tenant-map`, `-tenant-map-file` and `-tenant-label` is set (including their `TENANT_MAP`, `TENANT_MAP_FILE` and `TENANT_LABEL` environment forms).
- **Helm:** leave the tenant keys out of `extraArgs`.
- **Grafana:** no `X-Scope-OrgID` is needed; one that is sent is ignored. Use Basic auth or a bearer token only if a gateway in front checks them.
- **What users see:** every user reads VictoriaLogs account `0:0`, whatever header they send: a number, a name such as `team-a`, `a|b` or nothing.
- **Isolation:** none, by design, as in Loki with `auth_enabled: false`.
- **Missing or invalid tenant:** no error. Every header is answered as if absent (recorded from Loki with `auth_enabled: false` in `testdata/tenant_noauth_loki_answers.jsonl`, replayed by `TestTenantNoAuth_HeaderIgnoredLikeLoki`). The header is never validated, routed on, forwarded or used in a cache key.

### (b) Loki multi-tenant

- **Flags:** `-auth.enabled=true`, plus `-tenant-map` / `-tenant-map-file` for named tenants, or `-tenant.numeric-passthrough=true` for numeric ones. `-multi-tenant-max-fanout` bounds `a|b` requests.
- **Helm:** `extraArgs: {auth.enabled: "true", tenant-map: '{"team-a":{"account_id":"1","project_id":"0"}}'}`, or `tenant-map-file` with a mounted ConfigMap.
- **Grafana:** set the custom HTTP header `X-Scope-OrgID` on the data source (`team-a`, or `team-a|team-b` for a multi-tenant data source).
- **What users see:** each data source sees its tenant. `a|b` queries every listed tenant and adds `__tenant_id__` to every series and stream, as Loki does; `a|a` is the tenant `a`.
- **Isolation:** each tenant reads only its own VictoriaLogs account. Cache keys on every tier (memory, disk, peer) carry the tenant, which `TestTenantIsolation_NoLeakOnAnyReadRouteOrCacheTier` proves on every read route. The header is not authenticated: anyone who can reach the proxy can name any routed tenant, as with Loki. Put a gateway in front (setup e) or use client certificates.
- **Missing or invalid tenant:** Loki's `401 no org id` and `400` texts (see [Refusals with tenancy on](#refusals-with-tenancy-on)). Tail and delete accept one tenant only: `400 multiple org IDs present`.

### (c) VictoriaLogs-native accounts

- **Flags:** `-tenant.numeric-passthrough=true`.
- **Helm:** `extraArgs: {tenant.numeric-passthrough: "true"}`.
- **Grafana:** `X-Scope-OrgID: 42`.
- **What users see:** a canonical decimal `X-Scope-OrgID` (digits only, no sign, no leading zero, at most `4294967295`) reads VictoriaLogs account `N`, project `0`. The aliases `0`, `fake` and `default` read `0:0`. `007`, `00`, `-1` and numbers above `4294967295` are refused with `403`, because Loki keeps `007` and `7` as two tenants. An `AccountID:ProjectID` pair other than project `0` is reached only through a tenant-map entry (setup d).
- **Isolation:** per account, as in (b). Add `-auth.enabled` to require the header.
- **Missing or invalid tenant:** a missing header reads `0:0` unless `-auth.enabled` or `-require-tenant-header` is set (then `401 no org id`). Invalid IDs get Loki's `400`.

### (d) Hybrid: tenant map and numeric passthrough, or tenant label

- **Tenant map:** `-tenant-map` (JSON) or `-tenant-map-file` (YAML/JSON, reloaded on SIGHUP and every `-tenant-map-reload-interval`) maps names to `account_id`/`project_id`. With a map and `-tenant.numeric-passthrough` unset, canonical numbers are still routed to their accounts, so named and numeric tenants mix. With `-tenant.numeric-passthrough=false` set explicitly, routing is strict: only orgs in the map are served, and the aliases and a missing header need a map entry or `-tenant.allow-global=true`.
- **Tenant label:** `-tenant-label=<stream field>` keeps one VictoriaLogs tenant and confines each org to the streams whose field equals the org name, through VictoriaLogs `extra_stream_filters`, which a query cannot override. The aliases, `*` and a missing org are refused unless `-tenant.allow-global=true` or a map entry admits them.
- **Helm:** `extraArgs.tenant-map` or `tenant-map-file`, and/or `tenant.numeric-passthrough`, or `tenant-label`.
- **Grafana:** `X-Scope-OrgID` with the name or number.
- **Isolation and errors:** as in (b).

### (e) Behind a gateway (vmauth, an OAuth proxy)

The proxy trusts nothing about the caller unless you configure it:

- **Tenant:** `X-Scope-OrgID` is taken as sent. In production, let the gateway set it from the authenticated identity and drop the client's value, make the gateway the only network path to the proxy, and cover the `/loki/api/v1/tail` WebSocket upgrade too.
- **Credentials toward VictoriaLogs:**
  - **Grafana Basic auth or a bearer token** (`Authorization`): not forwarded unless `-forward-authorization=true` or `-forward-headers=Authorization`.
  - **Forward OAuth Identity** (`Authorization` + `X-Id-Token`): forward with `-forward-headers=Authorization,X-Id-Token`.
  - **Custom headers:** `-forward-headers=<names>`.
  - **Cookies:** `-forward-cookies=<names>`.

  A request that forwards a credential gets cache entries keyed by a hash of it, and those entries are never pushed to peers. Credentials never appear in logs, metrics or the disk cache (`TestCredentialPassThrough_AsConfigured`).
- **User identity headers** (`X-Grafana-User`, `X-Forwarded-User`, `X-Webauth-User`, `X-Auth-Request-User`, `X-Forwarded-For`): used and forwarded only with `-metrics.trust-proxy-headers=true` (see [Grafana user identity](#5-grafana-user-identity)).
- **vmauth between the proxy and VictoriaLogs:** `-backend-basic-auth` (Helm `backendAuth.existingSecret`) authenticates the proxy to it. `-forward-tenant-header` (default `true`) forwards the routed `X-Scope-OrgID` for a backend that routes on it; with tenancy off the header is never forwarded.
- **TLS at the proxy:** `-tls-cert-file`, `-tls-key-file`, `-tls-client-ca-file`, `-tls-require-client-cert`. A client certificate proves which client connected, not which tenant it may read.

## 3. Every option

| Flag | Helm | Default | Turns tenancy on | Loki equivalent |
|---|---|---|---|---|
| `-auth.enabled` | `extraArgs.auth.enabled` | `false` | yes | `auth_enabled` |
| `-require-tenant-header` | `extraArgs.require-tenant-header` | `false` | yes | `auth_enabled: true` (presence only) |
| `-tenant-map` / `TENANT_MAP` | `extraArgs.tenant-map` | empty | yes | none (Loki tenants are names) |
| `-tenant-map-file` / `TENANT_MAP_FILE` | `extraArgs.tenant-map-file` | empty | yes, even while the file is empty | none |
| `-tenant-map-reload-interval` | `extraArgs.tenant-map-reload-interval` | `30s` | no | none |
| `-tenant-label` / `TENANT_LABEL` | `extraArgs.tenant-label` | empty | yes | none |
| `-tenant.numeric-passthrough` | `extraArgs.tenant.numeric-passthrough` | `false` (unset: on next to another tenant setting) | `true` turns it on | none |
| `-tenant.allow-global` | `extraArgs.tenant.allow-global` | `false` | no | none (`*` is an ordinary name to Loki) |
| `-tenant-limits` / `TENANT_LIMITS` | `extraArgs.tenant-limits` | empty | no; **requires** tenancy (the proxy and the chart refuse it with tenancy off) | per-tenant `limits_config` overrides |
| `-tenant-default-limits` / `TENANT_DEFAULT_LIMITS` | `extraArgs.tenant-default-limits` | empty | no; allowed with tenancy off (applies to every request) | `limits_config` |
| `-forward-tenant-header` / `FORWARD_TENANT_HEADER` | `extraArgs.forward-tenant-header` | `true` | no | none |
| `-multi-tenant-max-fanout` | `extraArgs.multi-tenant-max-fanout` | `64` | no | `multi_tenant_queries_enabled` (Loki has no count cap) |
| `-delete-api-enabled` | `extraArgs.delete-api-enabled` | `false` | no | compactor `retention_enabled` + deletion |
| `-delete-api-auth-token` | `deleteAuth.existingSecret` / `extraArgs.delete-api-auth-token` | empty, required by the delete API | no | none |
| `-server.admin-auth-token` | `adminAuth.existingSecret` / `extraArgs.server.admin-auth-token` | empty | no | none |
| `-admin-listen` | `extraArgs.admin-listen` | `127.0.0.1:3101` | no | none |
| `-peer-auth-token` | `peerCache.authToken` / `peerCache.existingSecret` | empty, required with peers | no | none |
| `-forward-authorization`, `-forward-headers`, `-forward-cookies` | `extraArgs.*` | off / empty | no | none |
| `-metrics.trust-proxy-headers` | `observability.metrics.trustProxyHeaders` | `false` | no | none |
| `-metrics.export-sensitive-labels` | `observability.metrics.exportSensitiveLabels` | `false` | no | none |
| `-backend-basic-auth` | `backendAuth.existingSecret` / `extraArgs.backend-basic-auth` | empty | no | none |

`proxy.Config`, for library users, is unchanged: its zero value keeps numeric passthrough on (`StrictTenantRouting: false`), so tenancy is on unless `StrictTenantRouting` is set with no other tenant setting.

## 4. Upgrading

**Breaking:** tenancy is now off by default. A deployment that separated tenants only by numeric `X-Scope-OrgID` (no tenant map, map file, tenant label, `-auth.enabled` or `-require-tenant-header`) now serves account `0:0` to everyone after the upgrade. To keep the old behaviour, add one line:

```
-tenant.numeric-passthrough=true
```

(Helm: `extraArgs: {tenant.numeric-passthrough: "true"}`). Deployments with any other tenant setting keep routing as before.

Per-tenant limits (`-tenant-limits`) are keyed by `X-Scope-OrgID`, so with tenancy off the proxy refuses to start with them; turn tenancy on, or move them to `-tenant-default-limits`. If a reload adds tenant-map entries to a proxy that started with tenancy off, it routes strictly (only mapped orgs) until it is restarted, which fails closed.

The delete API now needs its own token: set `-delete-api-auth-token` (Helm `deleteAuth.existingSecret`) and send it with every delete request. The [CHANGELOG](../CHANGELOG.md) lists the other changed answers, such as Loki's `401 no org id` and `400` tenant texts.

## 5. Grafana user identity

- Grafana sends `X-Grafana-User` to data sources only with `[dataproxy] send_user_header = true`, and only for signed-in users; anonymous requests carry no user. Keep the data source in proxy mode (`access: proxy`).
- With `-metrics.trust-proxy-headers=true` the proxy takes the caller's identity from the first of `X-Grafana-User`, `X-Forwarded-User`, `X-Webauth-User` and `X-Auth-Request-User`, then `X-Scope-OrgID`, then `X-Forwarded-For`, then the remote address. Without the flag it is `X-Scope-OrgID`, then the remote address. The request log writes it as `enduser.id` / `enduser.source`. Per-client metric labels carry it only with `-metrics.export-sensitive-labels=true`.
- With the same flag the proxy forwards those user and proxy-chain headers (`X-Forwarded-*`, `X-Real-Ip`, `Forwarded`) to VictoriaLogs, and the user name of a data source's basic authentication as `X-Loki-VL-Auth-User` (logged only as `auth.source`, never the name). Every backend request carries `X-Loki-VL-Client-ID` / `X-Loki-VL-Client-Source` with the identity above.
- These headers are plain text the caller controls. Trust them only when the proxy is reachable solely through Grafana or a gateway that sets them and strips any value the client sent. If Grafana sits behind an auth proxy, keep `X-Forwarded-User`, `X-Grafana-User`, `X-Forwarded-*` and `Forwarded` end to end.
- Identity never selects a tenant: the tenant comes from `X-Scope-OrgID` (setups b–d).

| Grafana data source setting | Proxy side |
|---|---|
| No authentication | supported |
| Basic authentication | not checked by the proxy; forwarded only with `-forward-authorization` |
| Forward OAuth Identity | not checked; forwarded with `-forward-headers=Authorization,X-Id-Token` |
| TLS Client Auth | `-tls-client-ca-file`, `-tls-require-client-cert` |
| Custom HTTP headers (`X-Scope-OrgID`, others) | tenant as in setups b–d; others forwarded only when listed in `-forward-headers` |
| Allowed cookies / With credentials | forwarded only when listed in `-forward-cookies` |

## 6. Resolution details

### Tenant resolution order (tenancy on)

1. **Loki's tenant ID check:** every member of the header is checked as Loki checks it (see [Refusals](#refusals-with-tenancy-on)); `a|a` is the tenant `a`.
2. **No header:** VictoriaLogs default tenant `0:0` unless `-auth.enabled` or `-require-tenant-header` is set (`401 no org id`). With `-tenant-label` or strict routing it is refused with `403` unless `-tenant.allow-global=true`; no map entry admits a missing org.
3. **Tenant map:** an org listed in the map (static or file) uses its `account_id`/`project_id`. An exact entry also overrides the aliases and `*`.
4. **Default-tenant aliases** `0`, `fake`, `default`: read `0:0`. With `-tenant-label` or strict routing they are treated like `*`.
5. **Wildcard** `*`: a proxy-only convenience. It reads `0:0` only with `-tenant.allow-global=true` (or a `"*"` map entry); otherwise `403`. It is not allowed inside `a|b`.
6. **Label routing:** with `-tenant-label`, any other org is confined by the stream-field filter.
7. **Numeric passthrough:** a canonical number `N` reads account `N`, project `0`. Not with strict routing (`-tenant.numeric-passthrough=false` set explicitly).
8. **Fail closed:** every other org gets `403 unknown tenant`.

One resolver decides both admission and the `AccountID`/`ProjectID` headers of every backend request: queries, tail, rules and alerts, the cold tier, and each part of `a|b`. An admitted org is therefore never served from `0:0` by accident. The cold tier also receives `X-Scope-OrgID`; mirror tenant-map entries in its own alias table.

### Numeric tenants and strict routing

Loki keeps `7` and `007` as two tenants, while VictoriaLogs has one account per number. With numeric passthrough on, only the canonical spelling routes: digits only, no sign, no leading zero, at most `4294967295`.

| `X-Scope-OrgID` (tenancy on) | numeric passthrough on | strict (`-tenant.numeric-passthrough=false` explicit) |
|---|---|---|
| `7` | account `7` | `403` unless mapped |
| `007`, `00`, `-1`, `4294967296` | `403 unknown tenant` unless mapped | `403` unless mapped |
| `+7`, `a b`, `..`, over 150 bytes | Loki's `400` | Loki's `400` |
| `team-a` in the tenant map | its account and project | its account and project |
| `team-a` not in the map | `403 unknown tenant` | `403` |
| `0`, `fake`, `default`, no header | `0:0` | `403` unless `-tenant.allow-global=true` or mapped |
| `*` | `403` unless `-tenant.allow-global=true` or mapped | same |

A refused non-canonical number is logged once per org every ten minutes as `tenant refused`, with the reason and the fix. At startup one `tenant routing` line states the active mode. Tenant-map values are unaffected: `"007"` as an `account_id` still means account 7.

### Multi-tenant queries (`a|b`)

- Served on `/query`, `/query_range`, `/series`, `/labels`, `/label/{name}/values`, `/index/*`, `/patterns` and the Drilldown metadata routes. Tail and delete answer `400 multiple org IDs present`; the ruler views answer in their JSON.
- Every series and stream carries `__tenant_id__`. A `__tenant_id__` matcher in the leading selector narrows the tenants before any backend call. Use it in LogQL as `{app="api", __tenant_id__="team-b"}`.
- `detected_fields` and `detected_labels` count the exact union of the values each tenant returns. When a tenant reports a larger cardinality than the values it returned, that count is the floor, so a merged number can still undercount a true cross-tenant union.
- Like Loki, the request fails as a whole when any tenant's sub-request fails (`400` for a rejected query, `504` on a timeout, `500` otherwise), and nothing is merged or cached.
- Fan-out is capped by `-multi-tenant-max-fanout`, and the merged body is size-capped.

### Tenant map format and reload

`-tenant-map` (inline JSON, or `TENANT_MAP`) and `-tenant-map-file` (YAML or JSON; `.json` is parsed as JSON, anything else as YAML) map each `X-Scope-OrgID` string to a VictoriaLogs account and project. Both fields are required non-negative integers given as strings (`0`–`4294967295`). An invalid map stops startup. File entries override inline entries with the same key.

```yaml
# /etc/loki-vl-proxy/tenant-map.yaml
team-alpha: {account_id: "1", project_id: "1"}
team-beta:  {account_id: "1", project_id: "2"}
ops-prod:   {account_id: "42", project_id: "0"}
```

```bash
./loki-vl-proxy -backend=http://victorialogs:9428 \
  -tenant-map-file=/etc/loki-vl-proxy/tenant-map.yaml -tenant-map-reload-interval=30s
```

The file is re-read on SIGHUP (`kill -HUP <pid>`) and whenever its modification time changes (polled every `-tenant-map-reload-interval`; `0` turns polling off). A Kubernetes ConfigMap update is therefore picked up without a restart; see [k8s-tenant-map-configmap.yaml](k8s-tenant-map-configmap.yaml). An inline `-tenant-map` needs a restart, except that a `TENANT_MAP` environment value is re-read on SIGHUP when no file is set. A configured file turns tenancy on even while it is empty.

### Tenant label routing

With `-tenant-label=tenant`, an unmapped org (not an alias, empty or `*`) is sent as the VictoriaLogs `extra_stream_filters` constraint `{"tenant":"<org>"}`, which request parameters cannot override. The field must be a VictoriaLogs stream field. Map entries take priority. The rules and alerts read routes are confined the same way: an org sees only the rules and alerts whose `labels` carry `tenant=<org>`. A mapped org is not filtered by the proxy; it relies on the backend scoping by `AccountID`/`ProjectID`, which vmalert and the Loki ruler do not do.

### Grafana data source examples

```yaml
datasources:
  - name: Logs (team-alpha)            # one tenant
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100
    jsonData: {httpHeaderName1: X-Scope-OrgID}
    secureJsonData: {httpHeaderValue1: team-alpha}
  - name: Logs (team-a + team-b)       # multi-tenant, with __tenant_id__
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100
    jsonData: {httpHeaderName1: X-Scope-OrgID}
    secureJsonData: {httpHeaderValue1: team-a|team-b}
  - name: Logs (single-tenant)         # setup (a): no header needed
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100
```
