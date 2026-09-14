---
sidebar_label: Hardening manual acceptance
description: Verify the merged hardening changes in the local E2E Compose stack.
---

# Manual acceptance after merge

Run this pass after the review PRs are merged, using the resulting `main` commit.
Record the commit, backend versions, Grafana version and Drilldown plugin version
with the results. Automated checks on a proposed branch do not replace this pass.

## Build the merged commit

From a clean checkout of the merged `main`, rebuild the E2E images and recreate
the stack with its UI profile. Preserve volumes; no data reset is required.
The review override pins Loki 3.7.7, Grafana 13.2.1 and Logs Drilldown 2.5.2,
the current stable review baseline verified on September 14, 2026. The original
Compose defaults remain available for testing the older compatibility baseline.

```sh
git rev-parse HEAD
export COMPOSE_FILE=test/e2e-compat/docker-compose.yml:test/e2e-compat/docker-compose.review.yml
docker compose --profile ui pull loki grafana
docker compose --profile ui build \
  --build-arg REVISION="$(git rev-parse HEAD)"
docker compose --profile ui up -d --no-build
docker compose --profile ui ps
docker compose logs loki-vl-proxy \
  | grep 'proxy build info'
curl -fsS http://127.0.0.1:3002/api/health
curl -fsS http://127.0.0.1:3002/api/plugins/grafana-lokiexplore-app/settings
curl -fsS http://127.0.0.1:13101/loki/api/v1/status/buildinfo
```

The startup log's `revision` must match the checked-out commit. The Loki-compatible
`/loki/api/v1/status/buildinfo` advertises the emulated Loki version; it does not
identify the proxy build. Confirm the merge exists on `main` before calling a
build a merged version.

The standard stack uses Grafana at `http://127.0.0.1:3002`, proxy at port 13100,
Loki at 13101 and VictoriaLogs at 19428. For an isolated review stack, use its
Compose file/project and assigned ports consistently for both build and tests.
Do not mix its test ingestion endpoints with another running stack.

## Handoff for every review phase

The focused PRs below are stacked review slices. [Integration PR #525](https://github.com/ReliablyObserve/loki-vl-proxy/pull/525)
contains all slices plus the final compatibility/performance fixes and runs the
full CI against `main`. If #525 is merged together, run every row against that
merge. If slices are merged separately, rebuild after each approved merge and
run its row plus the earlier rows. Tests introduced in a later slice require the
integration checkout; do not report them as run on an earlier commit.

| Phase / review | Change and compatibility impact | Manual check and expected result |
| --- | --- | --- |
| [#520 Policy and tail](https://github.com/ReliablyObserve/loki-vl-proxy/pull/520) | Reject unmapped global tenant access and client tail messages over 4 KiB. Normal Grafana control messages remain supported. | Start synthetic and ingress tail, ingest a unique marker, and see its new row. On a disposable proxy configured with label tenancy and global access disabled, an unmapped `*` tenant must receive 403. Explicitly mapped tenants still work. |
| [#521 Tenant isolation](https://github.com/ReliablyObserve/loki-vl-proxy/pull/521) | Scope hot/cold reads, caches and background work consistently; invalidate old scope keys. Cold reads now use the correct tenant. | Seed distinct tenant markers, prime both caches, switch tenants repeatedly, and inspect service/field lists and historical cold reads. Only the selected tenant's markers appear. Repeat after tenant-map reload; admitted work retains its original scope. |
| [#522 Exact cache contracts](https://github.com/ReliablyObserve/loki-vl-proxy/pull/522) | Keep exact range and response shape in final-response keys; nearby windows can cause more misses. | Move an explicit range by seconds within one five-minute interval, then return to the first range. Compare direct Loki and proxy on every visit. Only in-range markers appear, including on cache hits; native metadata stays attached. |
| [#523 Execution and storage](https://github.com/ReliablyObserve/loki-vl-proxy/pull/523) | Bound query/template work and backend concurrency; surface size/failure errors; reclaim expired disk entries. Large workloads may now be rejected. | Run representative long-range charts and formatted logs, then the limit regressions below. Normal queries succeed, excessive work returns an error, and subsequent small queries still work. Check errors in Grafana's query inspector. |
| [#524 Documentation build](https://github.com/ReliablyObserve/loki-vl-proxy/pull/524) | Patch the vulnerable build-time image parser and constrain CI permissions/time. Registry audit metadata still flags the dependency. | Run the website commands below; malformed-image tests terminate and pass, then the website builds. Browse the generated migration and validation pages. |
| [#525 Integration](https://github.com/ReliablyObserve/loki-vl-proxy/pull/525) | Preserve real log tuples and template fields; select topk/bottomk winners at each timestamp; avoid per-request scope hashing overhead. Changing winners can produce more than k series over a range. | Compare quoted/backtick formatting and changing-winner charts with Loki. Run the strict browser checks below and the full user-visible checklist. Check both chart edges and actual log rows. |

Run these focused checks from the repository root on the integrated revision:

```sh
# Policy, tail, tenant scope, formatting and execution budgets.
go test -race ./internal/proxy \
  -run '^(TestHardening_|TestTailHardening_|TestTenantScoping_)' -count=1
# Exact final responses and idle disk expiry reclamation.
go test ./internal/proxy -run '^TestResponseCache_' -count=1
go test ./internal/cache -run '^TestDiskCache(UnreadExpiry|ExpirySweep)' -count=1
# Seed dedicated fixtures in the disposable real backends and compare results.
LOKI_URL=http://127.0.0.1:13101 PROXY_URL=http://127.0.0.1:13100 \
VL_URL=http://127.0.0.1:19428 \
go test -tags=e2e ./test/e2e-compat -run '^TestHardeningLive_' -count=1
```

From `test/e2e-ui`, install the locked dependencies and run visible UI checks:

```sh
npm ci
npx playwright install chromium
GRAFANA_URL=http://127.0.0.1:3002 VL_URL=http://127.0.0.1:19428 \
LOKI_URL=http://127.0.0.1:13101 PROXY_NATIVE_METADATA_URL=http://127.0.0.1:13106 \
WORKERS=1 npx playwright test tests/security-hardening-visibility.spec.ts \
  tests/explore.spec.ts tests/logs-drilldown.spec.ts
```

From `website`, run `npm ci`, `npm run test:security`, and `npm run build`.

For each handoff, record: PR and merge SHA; actual running revision from startup
logs; Compose project/file and URLs; backend/Grafana/plugin versions; automated
passes, failures and skips; manual steps with expected results; and outstanding
limitations. Keep a failed check open with its evidence and reproduction until
it is resolved. The review stack can be used before merge, but label its results
as testing the proposed revision.

## User-visible checklist

- Open Explore with **Loki (via VL proxy)** and **Loki (direct)**. Compare the same
  explicit range and query. Confirm actual log lines, timestamps, direction and
  line limits; repeat each request to exercise cache hits.
- Move the start/end a few seconds inside one five-minute interval. Verify logs
  enter and leave the view at the expected boundaries. Repeat with a saved URL.
- Expand a JSON log row. Check stream labels, parsed fields, structured metadata
  and trace ID. Filter for and exclude a field value; confirm the query changes
  and the visible results obey it. Repeat on the native-metadata datasource.
- Try backtick and escaped-quote `line_format`, including `printf`. Confirm the
  formatted line and field details remain visible; no literal template remains.
- Open Drilldown. Check service buckets, service logs, labels, fields, cardinality
  badges and field-value filters. Reload a filtered URL and switch tabs.
- Compare 30m, 1h, 6h, 24h, 2d and 7d charts with direct Loki. Check both edges,
  gaps, totals, changing topk winners, and the final partial time interval.
- Verify Patterns is shown only on enabled datasources and that enabled patterns
  contain useful entries. Confirm empty selections show empty results clearly.
- Use the multi-tenant datasource and tenant selectors. Switch tenants repeatedly
  after priming caches; check expected service/field inventories and that foreign
  markers never appear. Repeat a historical range routed to cold storage.
- Start synthetic and native live tail, switch to the ingress datasource, ingest
  a unique marker, and confirm the new marker appears in a visible row. Pause,
  resume and stop; check reconnection and browser errors.
- Confirm backend failures and query-limit errors are visible errors rather than
  successful empty charts. Use only the disposable E2E services for fault injection.

Record pass/fail and a screenshot or query/response for each discrepancy. Keep
delete disabled: the existing optional adapter is not verified against VL's
asynchronous deletion API. Known compatibility gaps and migration behavior are
listed in the [validation record](security-hardening-validation.md) and
[migration guide](security-hardening-migration.md).
