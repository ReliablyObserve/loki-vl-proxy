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

```sh
git rev-parse HEAD
docker compose -f test/e2e-compat/docker-compose.yml --profile ui build \
  --build-arg REVISION="$(git rev-parse HEAD)"
docker compose -f test/e2e-compat/docker-compose.yml --profile ui up -d --no-build
docker compose -f test/e2e-compat/docker-compose.yml --profile ui ps
```

The standard stack uses Grafana at `http://127.0.0.1:3002`, proxy at port 13100,
Loki at 13101 and VictoriaLogs at 19428. For an isolated review stack, use its
Compose file/project and assigned ports consistently for both build and tests.
Do not mix its test ingestion endpoints with another running stack.

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
